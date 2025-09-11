"""
SQS Queue Manager for distributed crawler.

Handles SQS message operations for discovery queue (receiving) and
crawl queue (sending) with batch processing, error handling, and DLQ support.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import boto3
from pydantic import BaseModel, ValidationError

from ..config.settings import CrawlerSettings
from ..utils.retry import DATABASE_RETRY_CONFIG, AsyncRetrier

logger = logging.getLogger(__name__)


class DiscoveryMessage(BaseModel):
    """Discovery message format for SQS"""

    domain: str
    priority: int = 1
    max_urls: Optional[int] = None
    discovery_depth: int = 3
    requested_at: datetime = datetime.now(timezone.utc)
    requester_id: Optional[str] = None

    # SQS metadata (added dynamically when receiving messages)
    receipt_handle: Optional[str] = None
    message_id: Optional[str] = None


class CrawlMessage(BaseModel):
    """Crawl message format for SQS"""

    url: str
    domain: str
    priority: int = 1
    retry_count: int = 0
    enqueued_at: datetime = datetime.now(timezone.utc)
    discovery_source: Optional[str] = None  # sitemap, manual, etc.


class QueueStats(BaseModel):
    """Statistics for queue operations"""

    discovery_messages_received: int = 0
    discovery_messages_processed: int = 0
    discovery_messages_failed: int = 0
    crawl_messages_sent: int = 0
    crawl_messages_failed: int = 0
    batch_operations: int = 0
    dlq_messages: int = 0
    aws_api_errors: int = 0


class SQSQueueManager:
    """
    SQS Queue Manager for handling crawler queue operations.

    Manages both discovery queue (receiving messages to process domains)
    and crawl queue (sending messages for individual URLs to crawl).
    """

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.retrier = AsyncRetrier(DATABASE_RETRY_CONFIG)

        # Initialize AWS SQS clients
        self._sqs_client: Optional[Any] = None  # boto3.client('sqs')
        self._async_sqs_client = None  # Would use aioboto3 in production

        # Queue URLs from settings
        self.discovery_queue_url = getattr(settings, "sqs_discovery_queue_url", None)
        self.crawl_queue_url = settings.sqs_crawl_queue_url
        self.dlq_url = getattr(settings, "sqs_dlq_url", None)

        # Batch processing configuration
        self.max_batch_size = 10  # SQS limit
        self.receive_wait_time = 20  # Long polling
        self.visibility_timeout = 300  # 5 minutes
        self.message_retention_period = 1209600  # 14 days

        # Statistics
        self.stats = QueueStats()

        logger.info("SQS Queue Manager initialized")

    async def initialize(self):
        """Initialize SQS clients and validate queue access"""
        try:
            # Initialize SQS client with LocalStack support
            if hasattr(self.settings, "localstack_endpoint") and self.settings.localstack_endpoint:
                # LocalStack configuration
                self._sqs_client = boto3.client(  # type: ignore
                    "sqs",
                    endpoint_url=self.settings.localstack_endpoint,
                    aws_access_key_id=self.settings.aws_access_key_id,
                    aws_secret_access_key=self.settings.aws_secret_access_key,
                    region_name=self.settings.aws_region,
                )
                logger.debug(f"Using LocalStack SQS endpoint: {self.settings.localstack_endpoint}")
            else:
                # Production AWS configuration
                session = boto3.Session(
                    region_name=self.settings.aws_region,
                    # AWS credentials should be configured via IAM roles or environment
                )
                self._sqs_client = session.client("sqs")  # type: ignore

            # Validate queue access
            await self._validate_queue_access()

            logger.info("SQS Queue Manager initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize SQS Queue Manager: {e}")
            raise

    async def _validate_queue_access(self):
        """Validate access to configured queues"""
        try:
            # Test access to crawl queue (required)
            await self._get_queue_attributes(self.crawl_queue_url)
            logger.debug(f"Crawl queue access validated: {self.crawl_queue_url}")

            # Test access to discovery queue (optional)
            if self.discovery_queue_url:
                await self._get_queue_attributes(self.discovery_queue_url)
                logger.debug(f"Discovery queue access validated: {self.discovery_queue_url}")

            # Test access to DLQ (optional)
            if self.dlq_url:
                await self._get_queue_attributes(self.dlq_url)
                logger.debug(f"DLQ access validated: {self.dlq_url}")

        except Exception as e:
            logger.error(f"Queue access validation failed: {e}")
            raise

    async def _get_queue_attributes(self, queue_url: str) -> Dict[str, str]:
        """Get queue attributes (used for validation and monitoring)"""
        if not self._sqs_client:
            raise RuntimeError("SQS client not initialized")

        try:

            async def _async_get_attributes():
                assert self._sqs_client is not None
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None,
                    lambda: self._sqs_client.get_queue_attributes(  # type: ignore
                        QueueUrl=queue_url,
                        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
                    ),
                )

            response = await self.retrier.call(_async_get_attributes)
            return response.get("Attributes", {})

        except Exception as e:
            logger.error(f"Failed to get queue attributes for {queue_url}: {e}")
            self.stats.aws_api_errors += 1
            raise

    async def receive_discovery_message(self) -> Optional[DiscoveryMessage]:
        """
        Receive a single discovery message from the discovery queue.

        Returns:
            DiscoveryMessage if available, None if no messages
        """
        if not self.discovery_queue_url:
            logger.debug("No discovery queue configured")
            return None

        try:
            messages = await self._receive_messages(self.discovery_queue_url, max_messages=1)

            if not messages:
                return None

            message = messages[0]

            # Parse message body
            try:
                message_data = json.loads(message["Body"])
                discovery_message = DiscoveryMessage(**message_data)

                # Store receipt handle for later deletion
                discovery_message.receipt_handle = message["ReceiptHandle"]
                discovery_message.message_id = message["MessageId"]

                self.stats.discovery_messages_received += 1

                logger.debug(
                    f"Received discovery message for domain: {discovery_message.domain}",
                    extra={
                        "domain": discovery_message.domain,
                        "message_id": message["MessageId"],
                        "priority": discovery_message.priority,
                    },
                )

                return discovery_message

            except (ValidationError, json.JSONDecodeError) as e:
                logger.error(f"Invalid discovery message format: {e}")
                # Send malformed message to DLQ
                await self._send_to_dlq(message, f"Invalid message format: {e}")
                await self.delete_message(self.discovery_queue_url, message["ReceiptHandle"])
                self.stats.discovery_messages_failed += 1
                return None

        except Exception as e:
            logger.error(f"Error receiving discovery message: {e}")
            self.stats.aws_api_errors += 1
            return None

    async def _receive_messages(self, queue_url: str, max_messages: int = 10) -> List[Dict[str, Any]]:
        """Receive messages from SQS queue"""
        if not self._sqs_client:
            raise RuntimeError("SQS client not initialized")

        try:

            async def _async_receive():
                assert self._sqs_client is not None
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None,
                    lambda: self._sqs_client.receive_message(  # type: ignore
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=min(max_messages, self.max_batch_size),
                        WaitTimeSeconds=self.receive_wait_time,
                        VisibilityTimeout=self.visibility_timeout,
                        AttributeNames=["SentTimestamp", "ApproximateReceiveCount"],
                    ),
                )

            response = await self.retrier.call(_async_receive)
            return response.get("Messages", [])

        except Exception as e:
            logger.error(f"Failed to receive messages from {queue_url}: {e}")
            self.stats.aws_api_errors += 1
            return []

    async def send_crawl_messages(self, urls: List[str], discovery_source: str = "sitemap"):
        """
        Send crawl messages for URLs to the crawl queue in batches.

        Args:
            urls: List of URLs to enqueue for crawling
            discovery_source: Source of URL discovery (sitemap, manual, etc.)
        """
        if not urls:
            return

        try:
            # Create crawl messages
            crawl_messages: List[CrawlMessage] = []
            for url in urls:
                from ..utils.url import extract_domain  # Import here to avoid circular imports

                message = CrawlMessage(url=url, domain=extract_domain(url), discovery_source=discovery_source)
                crawl_messages.append(message)

            # Send in batches
            batch_count = 0
            for i in range(0, len(crawl_messages), self.max_batch_size):
                batch = crawl_messages[i : i + self.max_batch_size]
                await self._send_message_batch(self.crawl_queue_url, batch)
                batch_count += 1

                # Brief delay between batches to avoid rate limiting
                if batch_count > 1:
                    await asyncio.sleep(0.1)

            self.stats.crawl_messages_sent += len(crawl_messages)
            self.stats.batch_operations += batch_count

            logger.info(
                f"Sent {len(crawl_messages)} crawl messages in {batch_count} batches",
                extra={
                    "messages_sent": len(crawl_messages),
                    "batches": batch_count,
                    "discovery_source": discovery_source,
                },
            )

        except Exception as e:
            logger.error(f"Error sending crawl messages: {e}")
            self.stats.crawl_messages_failed += len(urls)

    async def _send_message_batch(self, queue_url: str, messages: List[CrawlMessage]):
        """Send a batch of messages to SQS"""
        if not self._sqs_client:
            raise RuntimeError("SQS client not initialized")

        try:
            entries: List[Dict[str, Any]] = []
            for i, message in enumerate(messages):
                entries.append(
                    {
                        "Id": str(i),
                        "MessageBody": message.model_dump_json(),
                        "MessageAttributes": {
                            "domain": {"StringValue": message.domain, "DataType": "String"},
                            "priority": {"StringValue": str(message.priority), "DataType": "Number"},
                            "discovery_source": {
                                "StringValue": message.discovery_source or "unknown",
                                "DataType": "String",
                            },
                        },
                    }
                )

            def _sync_send_batch():
                assert self._sqs_client is not None
                return self._sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)  # type: ignore

            response = await self.retrier.call(_sync_send_batch)

            # Handle partial failures
            failed_messages = response.get("Failed", [])
            if failed_messages:
                logger.warning(f"Some messages failed to send: {len(failed_messages)} failures")
                for failure in failed_messages:
                    logger.error(f"Message send failure: {failure}")

            successful_count = len(messages) - len(failed_messages)
            logger.debug(f"Successfully sent {successful_count}/{len(messages)} messages")

        except Exception as e:
            logger.error(f"Failed to send message batch: {e}")
            self.stats.aws_api_errors += 1
            raise

    async def delete_message(self, queue_url: str, receipt_handle: str):
        """Delete a processed message from the queue"""
        if not self._sqs_client:
            raise RuntimeError("SQS client not initialized")

        try:

            def _sync_delete():
                assert self._sqs_client is not None
                return self._sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)  # type: ignore

            await self.retrier.call(_sync_delete)
            logger.debug("Message deleted successfully")

        except Exception as e:
            logger.error(f"Failed to delete message: {e}")
            self.stats.aws_api_errors += 1

    async def _send_to_dlq(self, message: Dict[str, Any], error_reason: str):
        """Send a problematic message to the Dead Letter Queue"""
        if not self._sqs_client:
            raise RuntimeError("SQS client not initialized")

        if not self.dlq_url:
            logger.debug("No DLQ configured, skipping DLQ send")
            return

        try:
            dlq_message = {
                "original_message": message,
                "error_reason": error_reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "crawler_id": self.settings.crawler_id,
            }

            def _sync_send_dlq():
                assert self._sqs_client is not None
                return self._sqs_client.send_message(  # type: ignore
                    QueueUrl=self.dlq_url,
                    MessageBody=json.dumps(dlq_message),
                    MessageAttributes={
                        "error_reason": {"StringValue": error_reason, "DataType": "String"},
                        "original_queue": {"StringValue": "discovery", "DataType": "String"},
                    },
                )

            await self.retrier.call(_sync_send_dlq)
            self.stats.dlq_messages += 1

            logger.info(f"Sent message to DLQ: {error_reason}")

        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
            self.stats.aws_api_errors += 1

    async def get_queue_depths(self) -> Dict[str, Any]:
        """Get approximate message counts for all queues"""
        queue_depths: Dict[str, Any] = {}

        queues = [
            ("crawl_queue", self.crawl_queue_url),
            ("discovery_queue", self.discovery_queue_url),
            ("dlq", self.dlq_url),
        ]

        for queue_name, queue_url in queues:
            if not queue_url:
                continue

            try:
                attributes = await self._get_queue_attributes(queue_url)
                visible_messages = int(attributes.get("ApproximateNumberOfMessages", 0))
                in_flight_messages = int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0))

                queue_depths[queue_name] = {
                    "visible": visible_messages,
                    "in_flight": in_flight_messages,
                    "total": visible_messages + in_flight_messages,
                }

            except Exception as e:
                logger.error(f"Failed to get depth for {queue_name}: {e}")
                queue_depths[queue_name] = {"error": str(e)}

        return queue_depths

    def get_stats(self) -> Dict[str, Any]:
        """Get queue manager statistics"""
        return {
            **self.stats.model_dump(),
            "configuration": {
                "crawl_queue_url": self.crawl_queue_url,
                "discovery_queue_url": self.discovery_queue_url,
                "dlq_url": self.dlq_url,
                "max_batch_size": self.max_batch_size,
                "receive_wait_time": self.receive_wait_time,
                "visibility_timeout": self.visibility_timeout,
            },
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on queue manager"""
        try:
            health_status: Dict[str, Any] = {
                "status": "healthy",
                "sqs_client_initialized": self._sqs_client is not None,
                "queue_access": {},
            }

            # Test queue access
            if self.crawl_queue_url:
                try:
                    await self._get_queue_attributes(self.crawl_queue_url)
                    health_status["queue_access"]["crawl_queue"] = "accessible"
                except Exception as e:
                    health_status["queue_access"]["crawl_queue"] = f"error: {e}"
                    health_status["status"] = "degraded"

            if self.discovery_queue_url:
                try:
                    await self._get_queue_attributes(self.discovery_queue_url)
                    health_status["queue_access"]["discovery_queue"] = "accessible"
                except Exception as e:
                    health_status["queue_access"]["discovery_queue"] = f"error: {e}"
                    health_status["status"] = "degraded"

            # Get queue depths for monitoring
            try:
                health_status["queue_depths"] = await self.get_queue_depths()
            except Exception as e:
                health_status["queue_depths_error"] = str(e)
                health_status["status"] = "degraded"

            health_status["stats"] = self.get_stats()

            return health_status

        except Exception as e:
            logger.error(f"Queue manager health check failed: {e}")
            return {"status": "unhealthy", "error": str(e)}

    async def close(self):
        """Close SQS clients and cleanup resources"""
        # boto3 clients don't need explicit closing
        # but we can clean up any resources here
        logger.info("Queue manager closed")


# Utility functions for message processing


def create_discovery_message(
    domain: str,
    priority: int = 1,
    max_urls: Optional[int] = None,
    discovery_depth: int = 3,
    requester_id: Optional[str] = None,
) -> DiscoveryMessage:
    """Create a discovery message for a domain"""
    return DiscoveryMessage(
        domain=domain,
        priority=priority,
        max_urls=max_urls,
        discovery_depth=discovery_depth,
        requester_id=requester_id or str(uuid4()),
    )


def create_crawl_message(url: str, priority: int = 1, discovery_source: str = "manual") -> CrawlMessage:
    """Create a crawl message for a URL"""
    from ..utils.url import extract_domain

    return CrawlMessage(url=url, domain=extract_domain(url), priority=priority, discovery_source=discovery_source)


if __name__ == "__main__":
    # CLI utility for testing queue manager
    import sys

    from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python queue_manager.py <command> [args...]")
            print("Commands:")
            print("  health - Check queue manager health")
            print("  stats - Show queue manager statistics")
            print("  depths - Show queue depths")
            print("  send-test <url> - Send test crawl message")
            print("  receive-test - Try to receive discovery message")
            sys.exit(1)

        command = sys.argv[1]
        settings = load_settings()
        queue_manager = SQSQueueManager(settings)

        try:
            await queue_manager.initialize()

            if command == "health":
                health = await queue_manager.health_check()
                print("Queue manager health:")
                print(json.dumps(health, indent=2, default=str))

            elif command == "stats":
                stats = queue_manager.get_stats()
                print("Queue manager statistics:")
                print(json.dumps(stats, indent=2, default=str))

            elif command == "depths":
                depths = await queue_manager.get_queue_depths()
                print("Queue depths:")
                print(json.dumps(depths, indent=2, default=str))

            elif command == "send-test" and len(sys.argv) >= 3:
                test_url = sys.argv[2]
                print(f"Sending test crawl message for: {test_url}")
                await queue_manager.send_crawl_messages([test_url], "test")
                print("Test message sent successfully")

            elif command == "receive-test":
                print("Trying to receive discovery message...")
                message = await queue_manager.receive_discovery_message()
                if message:
                    print(f"Received message: {message}")
                else:
                    print("No messages available")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
        finally:
            await queue_manager.close()

    asyncio.run(main())
