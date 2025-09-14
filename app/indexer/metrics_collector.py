"""
Metrics collection and health check system for the indexer service.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    try:
        from aiohttp import web
    except ImportError:
        pass

from .config import MetricsConfig

logger = logging.getLogger(__name__)


@dataclass
class IndexerMetrics:
    """Container for indexer metrics."""

    # Processing metrics
    messages_processed: int = 0
    messages_failed: int = 0
    documents_indexed: int = 0
    embeddings_generated: int = 0

    # Performance metrics
    total_processing_time: float = 0.0
    average_processing_time: float = 0.0

    # Error metrics
    opensearch_errors: int = 0
    bedrock_errors: int = 0
    s3_errors: int = 0
    dlq_messages: int = 0

    # Queue metrics
    queue_depth: int = 0
    messages_in_flight: int = 0

    # Health metrics
    last_successful_processing: Optional[datetime] = None
    service_start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def get_success_rate(self) -> float:
        """Calculate success rate percentage."""
        total = self.messages_processed + self.messages_failed
        if total == 0:
            return 0.0
        return (self.messages_processed / total) * 100

    def get_uptime_seconds(self) -> float:
        """Get service uptime in seconds."""
        return (datetime.now(timezone.utc) - self.service_start_time).total_seconds()


class MetricsCollector:
    """Collects and reports metrics for the indexer service."""

    def __init__(self, config: MetricsConfig):
        self.config = config
        self.metrics = IndexerMetrics()
        self._processing_start_times: Dict[str, float] = {}

    def start_processing_timer(self, message_id: str) -> None:
        """Start timing a message processing operation."""
        self._processing_start_times[message_id] = time.time()

    def end_processing_timer(self, message_id: str, success: bool = True) -> float:
        """End timing a message processing operation."""
        start_time = self._processing_start_times.pop(message_id, None)
        if start_time is None:
            return 0.0

        processing_time = time.time() - start_time
        self.metrics.total_processing_time += processing_time

        # Update average processing time
        total_messages = self.metrics.messages_processed + self.metrics.messages_failed
        if total_messages > 0:
            self.metrics.average_processing_time = self.metrics.total_processing_time / total_messages

        if success:
            self.metrics.messages_processed += 1
            self.metrics.last_successful_processing = datetime.now(timezone.utc)
        else:
            self.metrics.messages_failed += 1

        return processing_time

    def record_document_indexed(self) -> None:
        """Record that a document was successfully indexed."""
        self.metrics.documents_indexed += 1

    def record_embedding_generated(self) -> None:
        """Record that an embedding was generated."""
        self.metrics.embeddings_generated += 1

    def record_error(self, error_type: str) -> None:
        """Record an error by type."""
        if error_type == "opensearch":
            self.metrics.opensearch_errors += 1
        elif error_type == "bedrock":
            self.metrics.bedrock_errors += 1
        elif error_type == "s3":
            self.metrics.s3_errors += 1
        elif error_type == "dlq":
            self.metrics.dlq_messages += 1

    def update_queue_metrics(self, queue_depth: int, messages_in_flight: int) -> None:
        """Update queue-related metrics."""
        self.metrics.queue_depth = queue_depth
        self.metrics.messages_in_flight = messages_in_flight

    def get_metrics_dict(self) -> Dict[str, Any]:
        """Get metrics as a dictionary for JSON serialization."""
        return {
            "processing": {
                "messages_processed": self.metrics.messages_processed,
                "messages_failed": self.metrics.messages_failed,
                "documents_indexed": self.metrics.documents_indexed,
                "embeddings_generated": self.metrics.embeddings_generated,
                "success_rate": self.metrics.get_success_rate(),
            },
            "performance": {
                "total_processing_time": self.metrics.total_processing_time,
                "average_processing_time": self.metrics.average_processing_time,
                "uptime_seconds": self.metrics.get_uptime_seconds(),
            },
            "errors": {
                "opensearch_errors": self.metrics.opensearch_errors,
                "bedrock_errors": self.metrics.bedrock_errors,
                "s3_errors": self.metrics.s3_errors,
                "dlq_messages": self.metrics.dlq_messages,
            },
            "queue": {
                "queue_depth": self.metrics.queue_depth,
                "messages_in_flight": self.metrics.messages_in_flight,
            },
            "health": {
                "last_successful_processing": self.metrics.last_successful_processing.isoformat()
                if self.metrics.last_successful_processing
                else None,
                "service_start_time": self.metrics.service_start_time.isoformat(),
            },
        }

    def get_health_status(self) -> Dict[str, Any]:
        """Get health status for health check endpoint."""
        now = datetime.now(timezone.utc)
        uptime = self.metrics.get_uptime_seconds()

        # Determine health status
        is_healthy = True
        issues: List[str] = []

        # Check if we've processed messages recently (within last 10 minutes)
        if self.metrics.last_successful_processing:
            time_since_last_success = (now - self.metrics.last_successful_processing).total_seconds()
            if time_since_last_success > 600 and self.metrics.messages_processed > 0:  # 10 minutes
                is_healthy = False
                issues.append(f"No successful processing in {time_since_last_success:.0f} seconds")

        # Check error rate
        if (
            self.metrics.get_success_rate() < 80
            and (self.metrics.messages_processed + self.metrics.messages_failed) > 10
        ):
            is_healthy = False
            issues.append(f"Low success rate: {self.metrics.get_success_rate():.1f}%")

        # Check if service just started
        if uptime < 60:  # Less than 1 minute
            status = "starting"
        elif is_healthy:
            status = "healthy"
        else:
            status = "unhealthy"

        return {
            "status": status,
            "uptime_seconds": uptime,
            "issues": issues,
            "timestamp": now.isoformat(),
            "metrics_summary": {
                "messages_processed": self.metrics.messages_processed,
                "success_rate": self.metrics.get_success_rate(),
                "queue_depth": self.metrics.queue_depth,
            },
        }

    async def start_metrics_server(self) -> None:
        """Start HTTP server for metrics and health check endpoints."""
        if not self.config.enable_metrics:
            return

        try:
            from aiohttp import web, web_runner

            app = web.Application()
            app.router.add_get(self.config.metrics_path, self._metrics_handler)
            app.router.add_get(self.config.health_check_path, self._health_handler)

            runner = web_runner.AppRunner(app)
            await runner.setup()

            site = web_runner.TCPSite(runner, "0.0.0.0", self.config.metrics_port)
            await site.start()

            logger.info(f"Metrics server started on port {self.config.metrics_port}")
            logger.info(f"Metrics endpoint: http://localhost:{self.config.metrics_port}{self.config.metrics_path}")
            logger.info(
                f"Health check endpoint: http://localhost:{self.config.metrics_port}{self.config.health_check_path}"
            )

        except ImportError:
            logger.warning("aiohttp not available, metrics server disabled")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")

    async def _metrics_handler(self, request: "web.Request") -> "web.Response":
        """Handle metrics endpoint requests."""
        from aiohttp import web

        metrics = self.get_metrics_dict()
        return web.json_response(metrics)

    async def _health_handler(self, request: "web.Request") -> "web.Response":
        """Handle health check endpoint requests."""
        from aiohttp import web

        health = self.get_health_status()
        status_code = 200 if health["status"] in ["healthy", "starting"] else 503

        return web.json_response(health, status=status_code)
