"""
Main crawler worker implementation for distributed web crawling.

Provides the core worker loop that processes crawl queue messages from SQS,
implements distributed locking with DynamoDB, performs rate limiting checks,
executes HTTP crawling, and updates state and saves results.
"""

import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import ValidationError

from ...schema.crawl import CrawlResult
from ..config.aws_init import initialize_aws_services, update_dynamodb_table_name
from ..config.settings import CrawlerSettings, get_cached_settings
from ..core.types import CrawlerStatus, URLStateEnum
from ..discovery.queue_manager import CrawlMessage, SQSQueueManager
from ..http_client.client import CrawlerHTTPClient, initialize_http_client
from ..state import create_lock_manager, create_state_manager
from ..storage.pipeline import DataPipeline
from ..storage.s3_client import S3StorageClient
from ..utils.logging import setup_crawler_logger
from ..utils.url import generate_url_hash
from .concurrent_manager import ConcurrentCrawlManager
from .error_handler import CrawlErrorHandler

logger = logging.getLogger(__name__)


class CrawlerWorkerStats(dict[str, Any]):
    """Extended dict for worker statistics with type hints"""

    def __init__(self):
        super().__init__(
            {
                "worker_started_at": datetime.now(timezone.utc),
                "messages_received": 0,
                "messages_processed": 0,
                "messages_failed": 0,
                "urls_crawled": 0,
                "urls_successful": 0,
                "urls_failed": 0,
                "locks_acquired": 0,
                "locks_failed": 0,
                "retries_scheduled": 0,
                "processing_time_total": 0.0,
                "errors_by_type": {},
                "domains_processed": set(),
            }
        )

    def record_message_received(self):
        self["messages_received"] += 1

    def record_message_processed(self, success: bool, processing_time: float):
        if success:
            self["messages_processed"] += 1
        else:
            self["messages_failed"] += 1
        self["processing_time_total"] += processing_time

    def record_url_crawled(self, success: bool, domain: str):
        self["urls_crawled"] += 1
        if success:
            self["urls_successful"] += 1
        else:
            self["urls_failed"] += 1
        self["domains_processed"].add(domain)  # type: ignore

    def record_lock_attempt(self, success: bool):
        if success:
            self["locks_acquired"] += 1
        else:
            self["locks_failed"] += 1

    def record_error(self, error_type: str):
        if error_type not in self["errors_by_type"]:
            self["errors_by_type"][error_type] = 0
        self["errors_by_type"][error_type] += 1

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics for reporting"""
        uptime = datetime.now(timezone.utc) - self["worker_started_at"]  # type: ignore

        return {
            "uptime_seconds": uptime.total_seconds(),  # type: ignore
            "messages_received": self["messages_received"],
            "messages_processed": self["messages_processed"],
            "messages_failed": self["messages_failed"],
            "urls_crawled": self["urls_crawled"],
            "urls_successful": self["urls_successful"],
            "urls_failed": self["urls_failed"],
            "success_rate": (self["urls_successful"] / max(1, self["urls_crawled"])),  # type: ignore
            "locks_acquired": self["locks_acquired"],
            "locks_failed": self["locks_failed"],
            "lock_success_rate": (self["locks_acquired"] / max(1, self["locks_acquired"] + self["locks_failed"])),  # type: ignore
            "domains_processed_count": len(self["domains_processed"]),
            "average_processing_time": (
                self["processing_time_total"] / max(1, self["messages_processed"] + self["messages_failed"])  # type: ignore
            ),
            "errors_by_type": dict(self["errors_by_type"]),
        }


class CrawlerWorker:
    """
    Main crawler worker that processes crawl queue messages.

    Integrates all crawler components including SQS queue management,
    distributed locking, rate limiting, HTTP client, error handling,
    and concurrency control to provide a complete crawling solution.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None, crawler_id: Optional[str] = None):
        """
        Initialize the crawler worker.

        Args:
            settings: Optional crawler settings (uses cached settings if None)
            crawler_id: Optional crawler instance ID (generates random if None)
        """
        self.settings = settings or get_cached_settings()
        self.crawler_id = crawler_id or self.settings.crawler_id or f"worker-{uuid4().hex[:8]}"

        # Worker state
        self.status = CrawlerStatus.STARTING
        self._shutdown_requested = False
        self._main_task: Optional[asyncio.Task[None]] = None

        # Component initialization (will be done in initialize())
        self.queue_manager: Optional[SQSQueueManager] = None
        self.http_client: Optional[CrawlerHTTPClient] = None
        self.state_manager: Optional[Any] = None  # URLStateManager or LocalStackURLStateManager
        self.lock_manager: Optional[Any] = None  # DistributedLockManager or LocalStackDistributedLockManager
        self.concurrent_manager: Optional[ConcurrentCrawlManager] = None
        self.error_handler: Optional[CrawlErrorHandler] = None
        self.storage_client: Optional[S3StorageClient] = None
        self.data_pipeline: Optional[DataPipeline] = None

        # Statistics and monitoring
        self.stats = CrawlerWorkerStats()

        # Configuration
        self.polling_interval = 20  # SQS long polling
        self.max_empty_polls = 3  # Number of empty polls before brief sleep
        self.empty_poll_count = 0

        logger.info(f"Initialized crawler worker {self.crawler_id}", extra={"crawler_id": self.crawler_id})

    async def initialize(self):
        """Initialize all worker components"""
        try:
            logger.info(f"Initializing crawler worker {self.crawler_id}...")

            # Initialize AWS services (LocalStack support)
            initialize_aws_services(self.settings)
            update_dynamodb_table_name(self.settings)

            # Initialize SQS queue manager
            self.queue_manager = SQSQueueManager(self.settings)
            await self.queue_manager.initialize()

            # Initialize HTTP client
            self.http_client = await initialize_http_client(self.settings)

            # Initialize state manager and lock manager (environment-aware)
            self.lock_manager = create_lock_manager(self.crawler_id, self.settings)
            self.state_manager = create_state_manager(self.crawler_id, self.settings)

            # Initialize concurrent manager
            self.concurrent_manager = ConcurrentCrawlManager(
                max_concurrent=self.settings.max_concurrent_requests,
                max_concurrent_per_domain=2,  # Conservative default
                domain_concurrency_overrides={"example.com": 1},  # Example override
                task_timeout=self.settings.request_timeout + 60,  # HTTP timeout + buffer
            )
            await self.concurrent_manager.initialize()

            # Initialize error handler
            self.error_handler = CrawlErrorHandler(
                max_retries=self.settings.max_retries,
                base_backoff_seconds=self.settings.base_backoff_seconds,
                max_backoff_seconds=self.settings.max_backoff_seconds,
            )

            # Initialize S3 storage client
            self.storage_client = S3StorageClient(self.settings)

            # Initialize data pipeline for indexing queue integration
            self.data_pipeline = DataPipeline(self.settings)

            self.status = CrawlerStatus.RUNNING
            logger.info(f"Crawler worker {self.crawler_id} initialized successfully")

        except Exception as e:
            self.status = CrawlerStatus.ERROR
            logger.error(f"Failed to initialize crawler worker: {e}")
            raise

    async def run(self):
        """
        Main worker loop that processes crawl queue messages.

        Runs continuously until shutdown is requested, processing SQS messages
        and coordinating crawling operations.
        """
        if self.status != CrawlerStatus.RUNNING:
            raise RuntimeError("Worker must be initialized before running")

        logger.info(f"Starting crawler worker main loop for {self.crawler_id}")

        try:
            while not self._shutdown_requested:
                try:
                    # Process one batch of crawl queue messages
                    await self.process_crawl_queue_batch()

                    # Brief pause between polling cycles
                    await asyncio.sleep(0.1)

                except asyncio.CancelledError:
                    logger.info("Main loop cancelled, shutting down...")
                    break

                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    self.stats.record_error("main_loop_error")

                    # Brief sleep before retrying
                    await asyncio.sleep(5)

        except Exception as e:
            self.status = CrawlerStatus.ERROR
            logger.error(f"Fatal error in worker main loop: {e}")
            raise
        finally:
            self.status = CrawlerStatus.STOPPING
            logger.info(f"Crawler worker {self.crawler_id} main loop stopped")

    async def process_crawl_queue_batch(self):
        """Process a batch of messages from the crawl queue"""
        # Receive messages from SQS (up to 10 messages per call)
        messages = await self._receive_crawl_messages(max_messages=10)

        if not messages:
            self.empty_poll_count += 1
            if self.empty_poll_count >= self.max_empty_polls:
                # Brief sleep when no messages available
                logger.debug("No messages available, brief sleep...")
                await asyncio.sleep(5)
                self.empty_poll_count = 0
            return

        self.empty_poll_count = 0
        logger.info(f"Processing {len(messages)} crawl messages")

        # Process messages concurrently
        tasks: List[asyncio.Task[None]] = []
        for message_data in messages:
            task = asyncio.create_task(self.process_single_message(message_data))
            tasks.append(task)

        # Wait for all message processing to complete
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _receive_crawl_messages(self, max_messages: int = 10) -> List[Dict[str, Any]]:
        """Receive crawl messages from SQS"""
        assert self.queue_manager is not None, "Worker not properly initialized"
        try:
            # Use the actual queue manager to receive messages
            messages = await self.queue_manager._receive_messages(self.settings.sqs_crawl_queue_url, max_messages)  # type: ignore
            return messages

        except Exception as e:
            logger.error(f"Error receiving crawl messages: {e}")
            self.stats.record_error("sqs_receive_error")
            return []

    async def process_single_message(self, message_data: Dict[str, Any]):
        """Process a single crawl message"""
        start_time = time.time()
        message_id = message_data.get("MessageId", "unknown")
        receipt_handle = message_data.get("ReceiptHandle")

        try:
            # Parse message body
            try:
                message_body = json.loads(message_data["Body"])
                crawl_message = CrawlMessage(**message_body)
            except (json.JSONDecodeError, ValidationError) as e:
                logger.error(f"Invalid message format: {e}")
                await self._handle_invalid_message(message_data, str(e))
                return

            self.stats.record_message_received()

            logger.info(
                "Processing crawl message",
                extra={
                    "message_id": message_id,
                    "url": crawl_message.url,
                    "domain": crawl_message.domain,
                    "retry_count": crawl_message.retry_count,
                },
            )

            # Process the crawl message
            success = await self.process_crawl_message(crawl_message)

            # Delete message from queue if successful
            if success and receipt_handle:
                assert self.queue_manager is not None, "Worker not properly initialized"
                await self.queue_manager.delete_message(self.settings.sqs_crawl_queue_url, receipt_handle)

            processing_time = time.time() - start_time
            self.stats.record_message_processed(success, processing_time)

        except Exception as e:
            logger.error(
                f"Error processing message {message_id}: {e}",
                extra={"message_id": message_id, "error": str(e)},
            )
            processing_time = time.time() - start_time
            self.stats.record_message_processed(False, processing_time)
            self.stats.record_error("message_processing_error")

    async def process_crawl_message(self, crawl_message: CrawlMessage) -> bool:
        """
        Process a single crawl message with full pipeline.

        Args:
            crawl_message: Parsed crawl message

        Returns:
            True if processing succeeded, False otherwise
        """
        url = crawl_message.url
        domain = crawl_message.domain
        retry_count = crawl_message.retry_count
        url_hash = generate_url_hash(url)

        try:
            # Step 1: Try to acquire distributed lock
            assert self.lock_manager is not None, "Worker not properly initialized"
            lock_acquired = await self.lock_manager.try_acquire_url(url=url, domain=domain)

            self.stats.record_lock_attempt(lock_acquired)

            if not lock_acquired:
                logger.info(f"Could not acquire lock for {url}, skipping...")
                return True  # Not our fault, message can be deleted

            try:
                # Step 2: Update state to in_progress
                assert self.state_manager is not None, "Worker not properly initialized"
                await self.state_manager.update_state(
                    url_hash=url_hash, new_state=URLStateEnum.IN_PROGRESS, crawler_id=self.crawler_id
                )

                # Step 3: Perform crawling with concurrency control
                async def crawl_function(crawl_url: str) -> CrawlResult:
                    return await self.crawl_single_url(crawl_url)

                assert self.concurrent_manager is not None, "Worker not properly initialized"
                result = await self.concurrent_manager.crawl_with_concurrency(crawl_function, url)

                # Step 4: Save content to S3 and trigger indexing pipeline
                if result.status_code == 200 and result.content:
                    try:
                        # Save content to S3 storage
                        assert self.storage_client is not None, "Worker not properly initialized"
                        raw_s3_key, parsed_s3_key = await self.storage_client.save_crawl_result(
                            crawl_result=result,
                            raw_content=result.content.encode("utf-8")
                            if isinstance(result.content, str)
                            else result.content,
                        )

                        # Update result with S3 keys
                        result.html_s3_key = raw_s3_key

                        # Trigger data pipeline for indexing
                        assert self.data_pipeline is not None, "Worker not properly initialized"
                        await self.data_pipeline.process_crawl_completion(
                            crawl_result=result,
                            raw_s3_key=raw_s3_key,
                            parsed_s3_key=parsed_s3_key,
                        )

                        logger.info(
                            "Saved crawl result to S3 and triggered indexing",
                            extra={
                                "url": url,
                                "raw_s3_key": raw_s3_key,
                                "parsed_s3_key": parsed_s3_key,
                            },
                        )

                    except Exception as storage_error:
                        logger.error(
                            f"Failed to save crawl result to S3: {storage_error}",
                            extra={"url": url, "error": str(storage_error)},
                        )
                        # Don't fail the crawl for storage errors, continue with state update

                # Step 5: Update state to completed
                assert self.state_manager is not None, "Worker not properly initialized"
                await self.state_manager.update_state(
                    url_hash=url_hash, new_state=URLStateEnum.DONE, crawler_id=self.crawler_id, result=result
                )

                self.stats.record_url_crawled(True, domain)

                logger.info(
                    f"Successfully crawled {url}",
                    extra={
                        "url": url,
                        "domain": domain,
                        "status_code": result.status_code,
                        "retry_count": retry_count,
                    },
                )

                return True

            except Exception as crawl_error:
                # Step 5: Handle crawl error
                return await self._handle_crawl_error(crawl_error, url, url_hash, domain, retry_count)

            finally:
                # Always release the lock
                assert self.lock_manager is not None, "Worker not properly initialized"
                await self.lock_manager.release_url(url)

        except Exception as e:
            logger.error(f"Unexpected error processing crawl message for {url}: {e}")
            self.stats.record_error("unexpected_error")
            return False

    async def crawl_single_url(self, url: str) -> CrawlResult:
        """
        Crawl a single URL using the HTTP client.

        Args:
            url: URL to crawl

        Returns:
            CrawlResult with crawling results
        """
        try:
            assert self.http_client is not None, "Worker not properly initialized"
            result = await self.http_client.fetch_url(url)
            return result

        except Exception as e:
            logger.error(f"HTTP client error for {url}: {e}")
            raise

    async def _handle_crawl_error(
        self, error: Exception, url: str, url_hash: str, domain: str, retry_count: int
    ) -> bool:
        """Handle crawl errors with retry logic"""
        try:
            # Use error handler to determine retry strategy
            assert self.error_handler is not None, "Worker not properly initialized"
            retry_decision = await self.error_handler.handle_crawl_error(error, url, retry_count, domain)

            self.stats.record_url_crawled(False, domain)
            self.stats.record_error(retry_decision.error_type.value)

            if retry_decision.should_retry:
                # Schedule retry
                assert self.state_manager is not None, "Worker not properly initialized"
                success = await self.state_manager.schedule_retry(
                    url_hash=url_hash, delay_seconds=retry_decision.delay_seconds, error_message=retry_decision.reason
                )

                if success:
                    self.stats["retries_scheduled"] += 1
                    logger.info(f"Scheduled retry for {url} in {retry_decision.delay_seconds}s")
                else:
                    logger.error(f"Failed to schedule retry for {url}")

            else:
                # Mark as permanently failed
                assert self.state_manager is not None, "Worker not properly initialized"
                await self.state_manager.update_state(
                    url_hash=url_hash,
                    new_state=URLStateEnum.FAILED,
                    crawler_id=self.crawler_id,
                    error=retry_decision.reason,
                )

                logger.warning(f"Marked {url} as permanently failed: {retry_decision.reason}")

            return True  # Error handled successfully

        except Exception as e:
            logger.error(f"Error handling crawl error for {url}: {e}")
            return False

    async def _handle_invalid_message(self, message_data: Dict[str, Any], error_reason: str):
        """Handle invalid message format"""
        logger.error(f"Invalid message format: {error_reason}")

        # Send to DLQ if available
        if hasattr(self.queue_manager, "_send_to_dlq") and self.queue_manager is not None:
            await self.queue_manager._send_to_dlq(message_data, error_reason)  # type: ignore

        # Delete invalid message from queue
        receipt_handle = message_data.get("ReceiptHandle")
        if receipt_handle and self.queue_manager is not None:
            await self.queue_manager.delete_message(self.settings.sqs_crawl_queue_url, receipt_handle)

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status"""
        return {
            "crawler_id": self.crawler_id,
            "status": self.status.value,
            "uptime_seconds": (datetime.now(timezone.utc) - self.stats["worker_started_at"]).total_seconds(),
            "shutdown_requested": self._shutdown_requested,
            "components_initialized": {
                "queue_manager": self.queue_manager is not None,
                "http_client": self.http_client is not None,
                "state_manager": self.state_manager is not None,
                "lock_manager": self.lock_manager is not None,
                "concurrent_manager": self.concurrent_manager is not None,
                "error_handler": self.error_handler is not None,
                "storage_client": self.storage_client is not None,
                "data_pipeline": self.data_pipeline is not None,
            },
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive worker statistics"""
        stats: Dict[str, Any] = self.stats.get_summary()

        # Add component statistics if available
        if self.http_client:
            stats["http_client"] = self.http_client.get_stats()
        if self.concurrent_manager:
            stats["concurrent_manager"] = self.concurrent_manager.get_stats()
        if self.error_handler:
            stats["error_handler"] = self.error_handler.get_stats()
        if self.state_manager:
            stats["state_manager"] = self.state_manager.get_stats()
        if self.storage_client:
            stats["storage_client"] = self.storage_client.get_stats()
        if self.data_pipeline:
            stats["data_pipeline"] = self.data_pipeline.get_stats()

        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health: Dict[str, Any] = {
            "status": "healthy",
            "worker_status": self.status.value,
            "crawler_id": self.crawler_id,
            "components": {},
        }

        # Check each component
        try:
            if self.http_client:
                health["components"]["http_client"] = await self.http_client.health_check()

            if self.concurrent_manager:
                health["components"]["concurrent_manager"] = await self.concurrent_manager.health_check()

            if self.queue_manager:
                health["components"]["queue_manager"] = await self.queue_manager.health_check()

            if self.state_manager:
                health["components"]["state_manager"] = await self.state_manager.health_check()

            if self.storage_client:
                health["components"]["storage_client"] = await self.storage_client.health_check()

            if self.data_pipeline:
                health["components"]["data_pipeline"] = await self.data_pipeline.health_check()

            # Determine overall health
            component_statuses = [comp.get("status", "unknown") for comp in health["components"].values()]

            if any(status == "unhealthy" for status in component_statuses):
                health["status"] = "unhealthy"
            elif any(status == "degraded" for status in component_statuses):
                health["status"] = "degraded"

        except Exception as e:
            health["status"] = "unhealthy"
            health["error"] = str(e)

        return health

    async def shutdown(self):
        """Gracefully shutdown the crawler worker"""
        logger.info(f"Shutting down crawler worker {self.crawler_id}...")
        self._shutdown_requested = True
        self.status = CrawlerStatus.STOPPING

        try:
            # Cancel main task if running
            if self._main_task and not self._main_task.done():
                self._main_task.cancel()
                try:
                    await self._main_task
                except asyncio.CancelledError:
                    pass

            # Shutdown components in reverse order of initialization
            if self.concurrent_manager:
                await self.concurrent_manager.shutdown()

            if self.http_client:
                await self.http_client.close()

            if self.queue_manager:
                await self.queue_manager.close()

            self.status = CrawlerStatus.STOPPED
            logger.info(f"Crawler worker {self.crawler_id} shutdown complete")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            self.status = CrawlerStatus.ERROR

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(sig: int, frame: Any):  # type: ignore
            logger.info(f"Received signal {sig}, requesting shutdown...")
            asyncio.create_task(self.shutdown())

        signal.signal(signal.SIGTERM, signal_handler)  # type: ignore
        signal.signal(signal.SIGINT, signal_handler)  # type: ignore


# Main entry point for running the worker
async def main():
    """Main entry point for crawler worker"""
    # Setup logging
    setup_crawler_logger("crawler.worker", level="INFO")

    # Load settings
    settings = get_cached_settings()

    # Create and initialize worker
    worker = CrawlerWorker(settings)
    worker.setup_signal_handlers()

    try:
        await worker.initialize()

        # Start main worker loop
        worker._main_task = asyncio.create_task(worker.run())  # type: ignore
        await worker._main_task  # type: ignore

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        await worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
