"""
Data pipeline integration for connecting storage operations to indexing queues.

Handles the flow from S3 storage completion to search index updates through
SQS messaging with proper error handling and dead letter queue management.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, HttpUrl

from ...schema.crawl import CrawlResult, ParsedContent
from ..config.settings import CrawlerSettings, get_cached_settings
from ..utils.url import extract_domain, generate_url_hash

logger = logging.getLogger(__name__)


class IndexingMessage(BaseModel):
    """
    Message format for indexing queue.

    Contains all information needed by the indexing service to process
    a crawled document and add it to the search index.
    """

    # Document identification
    url: HttpUrl
    url_hash: str
    domain: str

    # Content location
    raw_s3_key: str
    parsed_s3_key: Optional[str] = None

    # Crawl metadata
    status_code: int
    fetched_at: datetime
    crawl_error: Optional[str] = None

    # Content metadata (if parsed)
    title: Optional[str] = None
    language: Optional[str] = None
    published_at: Optional[datetime] = None
    content_length: Optional[int] = None

    # Processing metadata
    crawler_id: Optional[str] = None
    processing_priority: int = Field(default=0, description="Higher numbers = higher priority")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProcessingEvent(BaseModel):
    """Event message for processing pipeline coordination"""

    event_type: str  # "content_stored", "processing_complete", "error"
    url: HttpUrl
    url_hash: str

    # Event-specific data
    data: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = "crawler"


class PipelineClient:
    """
    Client for managing data pipeline messaging and coordination.

    Handles sending messages to indexing queues, processing events,
    and managing the flow from storage to search indexing.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None):
        self.settings = settings or get_cached_settings()
        self._sqs_client: Optional[Any] = None

        # Queue URLs
        self.indexing_queue_url = getattr(self.settings, "sqs_indexing_queue_url", None)
        self.processing_queue_url = getattr(self.settings, "sqs_processing_queue_url", None)
        self.dlq_url = getattr(self.settings, "sqs_dlq_url", None)

        # Statistics
        self.stats = {
            "messages_sent": 0,
            "messages_failed": 0,
            "indexing_messages": 0,
            "processing_events": 0,
            "dlq_messages": 0,
        }

        logger.info("Initialized pipeline client")

    def _ensure_sqs_client(self) -> Any:
        """Ensure SQS client is initialized"""
        if self._sqs_client is None:
            try:
                if self.settings.localstack_endpoint:
                    self._sqs_client = boto3.client(  # type: ignore
                        "sqs",
                        endpoint_url=self.settings.localstack_endpoint,
                        aws_access_key_id=self.settings.aws_access_key_id,
                        aws_secret_access_key=self.settings.aws_secret_access_key,
                        region_name=self.settings.aws_region,
                    )
                else:
                    self._sqs_client = boto3.client(  # type: ignore
                        "sqs",
                        region_name=self.settings.aws_region,
                    )

                logger.debug("Created SQS client for pipeline")

            except Exception as e:
                logger.error(f"Failed to create SQS client: {e}")
                raise

        return self._sqs_client

    async def send_for_indexing(
        self,
        crawl_result: CrawlResult,
        raw_s3_key: str,
        parsed_content: Optional[ParsedContent] = None,
        parsed_s3_key: Optional[str] = None,
        priority: int = 0,
    ) -> bool:
        """
        Send crawl result to indexing queue.

        Args:
            crawl_result: Crawl result metadata
            raw_s3_key: S3 key for raw HTML content
            parsed_content: Optional parsed content object
            parsed_s3_key: Optional S3 key for parsed content
            priority: Processing priority (higher = more important)

        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.indexing_queue_url:
            logger.warning("Indexing queue URL not configured, skipping indexing message")
            return False

        try:
            url = str(crawl_result.url)

            # Create indexing message
            message = IndexingMessage(
                url=crawl_result.url,
                url_hash=generate_url_hash(url),
                domain=extract_domain(url),
                raw_s3_key=raw_s3_key,
                parsed_s3_key=parsed_s3_key,
                status_code=crawl_result.status_code,
                fetched_at=crawl_result.fetched_at,
                crawl_error=crawl_result.error,
                crawler_id=self.settings.crawler_id,
                processing_priority=priority,
            )

            # Add parsed content metadata if available
            if parsed_content:
                message.title = parsed_content.title
                message.language = parsed_content.lang
                message.published_at = parsed_content.published_at
                message.content_length = len(parsed_content.body_text) if parsed_content.body_text else None

            # Send to queue
            success = await self._send_sqs_message(
                queue_url=self.indexing_queue_url,
                message_body=message.model_dump_json(),
                message_attributes={
                    "MessageType": {"StringValue": "IndexingRequest", "DataType": "String"},
                    "Domain": {"StringValue": extract_domain(url), "DataType": "String"},
                    "Priority": {"StringValue": str(priority), "DataType": "Number"},
                    "Status": {"StringValue": str(crawl_result.status_code), "DataType": "Number"},
                },
                message_group_id=extract_domain(url),  # For FIFO queues
            )

            if success:
                self.stats["indexing_messages"] += 1
                logger.info(
                    f"Sent indexing message for {url}",
                    extra={
                        "url": url,
                        "raw_s3_key": raw_s3_key,
                        "parsed_s3_key": parsed_s3_key,
                        "status_code": crawl_result.status_code,
                        "priority": priority,
                    },
                )

            return success

        except Exception as e:
            logger.error(f"Failed to send indexing message for {crawl_result.url}: {e}")
            return False

    async def send_processing_event(
        self,
        event_type: str,
        url: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Send processing event to pipeline coordination queue.

        Args:
            event_type: Type of event (e.g., "content_stored", "processing_complete")
            url: URL associated with the event
            data: Optional event-specific data

        Returns:
            True if event sent successfully, False otherwise
        """
        if not self.processing_queue_url:
            logger.debug("Processing queue URL not configured, skipping event")
            return True  # Not an error if not configured

        try:
            event = ProcessingEvent(
                event_type=event_type,
                url=HttpUrl(url),
                url_hash=generate_url_hash(url),
                data=data or {},
            )

            success = await self._send_sqs_message(
                queue_url=self.processing_queue_url,
                message_body=event.model_dump_json(),
                message_attributes={
                    "EventType": {"StringValue": event_type, "DataType": "String"},
                    "Domain": {"StringValue": extract_domain(url), "DataType": "String"},
                },
                message_group_id=extract_domain(url),
            )

            if success:
                self.stats["processing_events"] += 1
                logger.debug(f"Sent processing event: {event_type} for {url}")

            return success

        except Exception as e:
            logger.error(f"Failed to send processing event {event_type} for {url}: {e}")
            return False

    async def _send_sqs_message(
        self,
        queue_url: str,
        message_body: str,
        message_attributes: Optional[Dict[str, Any]] = None,
        message_group_id: Optional[str] = None,
        delay_seconds: int = 0,
    ) -> bool:
        """
        Send message to SQS queue with error handling.

        Args:
            queue_url: SQS queue URL
            message_body: Message body (JSON string)
            message_attributes: Optional message attributes
            message_group_id: Optional message group ID for FIFO queues
            delay_seconds: Optional delivery delay

        Returns:
            True if message sent successfully, False otherwise
        """
        try:
            sqs_client = self._ensure_sqs_client()

            # Prepare message parameters
            params = {
                "QueueUrl": queue_url,
                "MessageBody": message_body,
            }

            if message_attributes:
                params["MessageAttributes"] = message_attributes  # type: ignore

            # Only add FIFO-specific parameters if queue is FIFO
            if message_group_id and queue_url.endswith(".fifo"):
                params["MessageGroupId"] = message_group_id
                # Add deduplication ID to prevent duplicates
                params["MessageDeduplicationId"] = generate_url_hash(message_body + str(datetime.now(timezone.utc)))

            if delay_seconds > 0:
                params["DelaySeconds"] = delay_seconds  # type: ignore

            # Send message
            response = sqs_client.send_message(**params)

            self.stats["messages_sent"] += 1

            logger.debug(
                f"Sent SQS message: {response['MessageId']}",
                extra={
                    "queue_url": queue_url,
                    "message_id": response.get("MessageId"),
                    "message_group_id": message_group_id,
                },
            )

            return True

        except ClientError as e:
            self.stats["messages_failed"] += 1
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.error(f"SQS send failed with error {error_code}: {e}")

            # Try to send to DLQ if configured
            if self.dlq_url and queue_url != self.dlq_url:
                await self._send_to_dlq(message_body, f"SQS error: {error_code}")

            return False

        except Exception as e:
            self.stats["messages_failed"] += 1
            logger.error(f"Unexpected error sending SQS message: {e}")

            # Try to send to DLQ
            if self.dlq_url:
                await self._send_to_dlq(message_body, f"Unexpected error: {str(e)}")

            return False

    async def _send_to_dlq(self, original_message: str, error_reason: str) -> bool:
        """
        Send failed message to dead letter queue.

        Args:
            original_message: Original message that failed
            error_reason: Reason for failure

        Returns:
            True if sent to DLQ successfully, False otherwise
        """
        if not self.dlq_url:
            logger.warning("No DLQ configured, cannot send failed message")
            return False

        try:
            dlq_message = {
                "original_message": original_message,
                "error_reason": error_reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "source": "crawler-pipeline",
            }

            success = await self._send_sqs_message(
                queue_url=self.dlq_url,
                message_body=json.dumps(dlq_message),
                message_attributes={
                    "ErrorReason": {"StringValue": error_reason[:256], "DataType": "String"},
                    "Source": {"StringValue": "crawler-pipeline", "DataType": "String"},
                },
            )

            if success:
                self.stats["dlq_messages"] += 1
                logger.info(f"Sent failed message to DLQ: {error_reason}")

            return success

        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline client statistics"""
        stats = self.stats.copy()

        # Calculate derived metrics
        total_messages = stats["messages_sent"] + stats["messages_failed"]
        if total_messages > 0:
            stats["success_rate"] = stats["messages_sent"] / total_messages  # type: ignore
            stats["failure_rate"] = stats["messages_failed"] / total_messages  # type: ignore
        else:
            stats["success_rate"] = 0.0  # type: ignore
            stats["failure_rate"] = 0.0  # type: ignore

        # Add configuration info
        stats["configuration"] = {  # type: ignore
            "indexing_queue_configured": self.indexing_queue_url is not None,
            "processing_queue_configured": self.processing_queue_url is not None,
            "dlq_configured": self.dlq_url is not None,
        }

        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on pipeline client"""
        try:
            # Test SQS connectivity
            sqs_client = self._ensure_sqs_client()

            # Test queue access (if configured)
            queue_health = {}

            if self.indexing_queue_url:
                try:
                    sqs_client.get_queue_attributes(
                        QueueUrl=self.indexing_queue_url, AttributeNames=["ApproximateNumberOfMessages"]
                    )
                    queue_health["indexing_queue"] = "healthy"
                except Exception as e:
                    queue_health["indexing_queue"] = f"unhealthy: {e}"

            return {
                "status": "healthy",
                "sqs_client_active": sqs_client is not None,
                "queue_health": queue_health,
                "stats": self.get_stats(),
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
            }


class DataPipeline:
    """
    Complete data pipeline coordinator for crawl-to-index workflow.

    Orchestrates the flow from crawling completion through S3 storage
    to search index updates with proper error handling and monitoring.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None):
        self.settings = settings or get_cached_settings()
        self.pipeline_client = PipelineClient(settings)

        # Pipeline configuration
        self.auto_index_threshold = 200  # Auto-index if status_code == 200
        self.priority_domains = {"example.com": 10}  # Domain-specific priorities

    async def process_crawl_completion(
        self,
        crawl_result: CrawlResult,
        raw_s3_key: str,
        parsed_content: Optional[ParsedContent] = None,
        parsed_s3_key: Optional[str] = None,
    ) -> bool:
        """
        Process complete crawl result through the data pipeline.

        Args:
            crawl_result: Crawl result metadata
            raw_s3_key: S3 key for raw content
            parsed_content: Optional parsed content
            parsed_s3_key: Optional S3 key for parsed content

        Returns:
            True if pipeline processing succeeded, False otherwise
        """
        url = str(crawl_result.url)
        domain = extract_domain(url)

        try:
            # Send content stored event
            await self.pipeline_client.send_processing_event(
                event_type="content_stored",
                url=url,
                data={
                    "raw_s3_key": raw_s3_key,
                    "parsed_s3_key": parsed_s3_key,
                    "status_code": crawl_result.status_code,
                    "domain": domain,
                },
            )

            # Determine if content should be indexed
            should_index = self._should_index_content(crawl_result, parsed_content)
            priority = 0  # Initialize priority

            if should_index:
                # Calculate priority
                priority = self._calculate_priority(domain, crawl_result, parsed_content)

                # Send to indexing queue
                success = await self.pipeline_client.send_for_indexing(
                    crawl_result=crawl_result,
                    raw_s3_key=raw_s3_key,
                    parsed_content=parsed_content,
                    parsed_s3_key=parsed_s3_key,
                    priority=priority,
                )

                if success:
                    logger.info(
                        f"Sent for indexing: {url} (priority: {priority})",
                        extra={
                            "url": url,
                            "domain": domain,
                            "priority": priority,
                            "status_code": crawl_result.status_code,
                        },
                    )
                else:
                    logger.error(f"Failed to send for indexing: {url}")
                    return False

            else:
                logger.info(
                    f"Skipping indexing for {url} (status: {crawl_result.status_code})",
                    extra={
                        "url": url,
                        "status_code": crawl_result.status_code,
                        "error": crawl_result.error,
                    },
                )

            # Send processing complete event
            await self.pipeline_client.send_processing_event(
                event_type="processing_complete",
                url=url,
                data={
                    "indexed": should_index,
                    "priority": priority if should_index else None,
                },
            )

            return True

        except Exception as e:
            logger.error(f"Pipeline processing failed for {url}: {e}")

            # Send error event
            await self.pipeline_client.send_processing_event(event_type="error", url=url, data={"error": str(e)})

            return False

    def _should_index_content(
        self,
        crawl_result: CrawlResult,
        parsed_content: Optional[ParsedContent] = None,
    ) -> bool:
        """
        Determine if crawled content should be indexed.

        Args:
            crawl_result: Crawl result metadata
            parsed_content: Optional parsed content

        Returns:
            True if content should be indexed, False otherwise
        """
        # Don't index failed crawls
        if crawl_result.status_code != 200:
            return False

        # Don't index if crawl had errors
        if crawl_result.error:
            return False

        # Don't index if no meaningful content
        if parsed_content:
            if not parsed_content.body_text or len(parsed_content.body_text.strip()) < 100:
                return False

        return True

    def _calculate_priority(
        self,
        domain: str,
        crawl_result: CrawlResult,
        parsed_content: Optional[ParsedContent] = None,
    ) -> int:
        """
        Calculate indexing priority for content.

        Args:
            domain: Content domain
            crawl_result: Crawl result metadata
            parsed_content: Optional parsed content

        Returns:
            Priority score (higher = more important)
        """
        priority = 0

        # Base priority from domain
        priority += self.priority_domains.get(domain, 0)

        # Boost for recent content
        if parsed_content and parsed_content.published_at:
            age_days = (datetime.now(timezone.utc) - parsed_content.published_at).days
            if age_days < 1:
                priority += 5
            elif age_days < 7:
                priority += 3
            elif age_days < 30:
                priority += 1

        # Boost for content with good metadata
        if parsed_content:
            if parsed_content.title and len(parsed_content.title) > 10:
                priority += 2
            if parsed_content.description:
                priority += 1
            if parsed_content.lang and parsed_content.lang in ["ja", "en"]:
                priority += 1

        return max(0, priority)

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on data pipeline"""
        pipeline_health = await self.pipeline_client.health_check()

        return {
            "status": pipeline_health["status"],
            "pipeline_client": pipeline_health,
            "configuration": {
                "auto_index_threshold": self.auto_index_threshold,
                "priority_domains_count": len(self.priority_domains),
            },
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get data pipeline statistics"""
        return self.pipeline_client.get_stats()


if __name__ == "__main__":
    # CLI utility for testing pipeline
    import asyncio
    import sys

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python pipeline.py [health|stats|test] [args...]")
            sys.exit(1)

        command = sys.argv[1]

        # Initialize pipeline
        pipeline = DataPipeline()

        try:
            if command == "health":
                health = await pipeline.health_check()
                print(f"Pipeline Health: {health}")

            elif command == "stats":
                stats = pipeline.get_stats()
                print(f"Pipeline Stats: {stats}")

            elif command == "test":
                print("Testing data pipeline...")
                # Create test crawl result
                from ...schema.crawl import CrawlResult

                test_result = CrawlResult(
                    url=HttpUrl("https://example.com/test"),  # type: ignore
                    status_code=200,
                    fetched_at=datetime.now(timezone.utc),
                    html_s3_key="test-key",
                )

                success = await pipeline.process_crawl_completion(
                    crawl_result=test_result,
                    raw_s3_key="test-raw-key",
                )

                if success:
                    print("✅ Pipeline test successful!")
                else:
                    print("❌ Pipeline test failed")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    asyncio.run(main())
