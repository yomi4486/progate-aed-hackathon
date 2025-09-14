"""
Dead Letter Queue handler for failed indexing operations.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError

from .config import DLQConfig

logger = logging.getLogger(__name__)


class DLQHandler:
    """Handles Dead Letter Queue operations for failed indexing messages."""

    def __init__(self, config: DLQConfig, aws_region: str = "us-east-1"):
        self.config = config
        self.sqs = boto3.client("sqs", region_name=aws_region)  # type: ignore
        self.failed_messages: Dict[str, Any] = {}  # Track failed messages and retry counts

    async def handle_failed_message(
        self, original_message: Dict[str, Any], error: Exception, retry_count: int = 0
    ) -> bool:
        """
        Handle a failed indexing message.

        Args:
            original_message: The original SQS message
            error: The error that caused the failure
            retry_count: Current retry count

        Returns:
            True if message should be deleted from original queue, False otherwise
        """
        if not self.config.enable_dlq:
            logger.warning("DLQ is disabled, discarding failed message")
            return True

        message_id = original_message.get("MessageId")
        if not message_id:
            logger.error("Message has no MessageId, cannot track retries")
            return True

        # Check if we should retry or send to DLQ
        if retry_count < self.config.max_retry_attempts:
            await self._schedule_retry(original_message, error, retry_count)
            return False  # Don't delete from original queue yet
        else:
            await self._send_to_dlq(original_message, error, retry_count)
            return True  # Delete from original queue

    async def _schedule_retry(self, message: Dict[str, Any], error: Exception, retry_count: int) -> None:
        """Schedule a message for retry with exponential backoff."""
        message_id = message.get("MessageId")

        # Calculate backoff delay
        delay = min(self.config.retry_backoff_base**retry_count, self.config.retry_backoff_max_delay)

        logger.info(
            f"Scheduling retry for message {message_id} "
            f"(attempt {retry_count + 1}/{self.config.max_retry_attempts}) "
            f"with {delay}s delay"
        )

        # Store retry information
        if message_id is None:
            logger.error("MessageId is None, cannot schedule retry")
            return

        self.failed_messages[message_id] = {
            "retry_count": retry_count + 1,
            "last_error": str(error),
            "next_retry_time": time.time() + delay,
            "original_message": message,
        }

        # In a production system, you might want to use SQS message delay
        # or a separate retry queue. For now, we'll use in-memory tracking.

    async def _send_to_dlq(self, original_message: Dict[str, Any], error: Exception, retry_count: int) -> None:
        """Send a failed message to the Dead Letter Queue."""
        if not self.config.dlq_url:
            logger.error("DLQ URL not configured, cannot send failed message")
            return

        message_id = original_message.get("MessageId")

        # Create DLQ message with failure metadata
        dlq_message = {
            "original_message": original_message,
            "failure_metadata": {
                "error": str(error),
                "error_type": type(error).__name__,
                "retry_count": retry_count,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "failure_reason": "max_retries_exceeded",
            },
        }

        try:
            response = self.sqs.send_message(
                QueueUrl=self.config.dlq_url,
                MessageBody=json.dumps(dlq_message),
                MessageAttributes={
                    "FailureReason": {"StringValue": "max_retries_exceeded", "DataType": "String"},
                    "OriginalMessageId": {"StringValue": message_id or "unknown", "DataType": "String"},
                    "RetryCount": {"StringValue": str(retry_count), "DataType": "Number"},
                },
            )

            logger.info(f"Sent failed message {message_id} to DLQ: {response['MessageId']}")

            # Clean up tracking
            if message_id in self.failed_messages:
                del self.failed_messages[message_id]

        except ClientError as e:
            logger.error(f"Failed to send message to DLQ: {e}")

    async def process_retries(self) -> None:
        """Process messages that are ready for retry."""
        current_time = time.time()
        retry_messages: List[Tuple[str, Dict[str, Any]]] = []

        for message_id, retry_info in list(self.failed_messages.items()):
            if current_time >= retry_info["next_retry_time"]:
                retry_messages.append((message_id, retry_info))

        if not retry_messages:
            return

        logger.info(f"Processing {len(retry_messages)} retry messages")

        for message_id, retry_info in retry_messages:
            # This would typically re-queue the message or return it for processing
            # For now, we'll just log it. In a full implementation, you'd integrate
            # this with your main message processing loop.
            logger.info(f"Message {message_id} ready for retry (attempt {retry_info['retry_count']})")

    def get_retry_statistics(self) -> Dict[str, Any]:
        """Get current retry statistics."""
        current_time = time.time()

        pending_retries = len(self.failed_messages)
        ready_retries = sum(1 for info in self.failed_messages.values() if current_time >= info["next_retry_time"])

        return {
            "pending_retries": pending_retries,
            "ready_retries": ready_retries,
            "failed_messages": len(self.failed_messages),
        }

    async def drain_retry_queue(self) -> None:
        """Drain all pending retry messages to DLQ (for shutdown)."""
        if not self.failed_messages:
            return

        logger.info(f"Draining {len(self.failed_messages)} pending retry messages to DLQ")

        for _, retry_info in list(self.failed_messages.items()):
            await self._send_to_dlq(
                retry_info["original_message"], Exception(retry_info["last_error"]), retry_info["retry_count"]
            )


class RetryableException(Exception):
    """Exception that indicates a message should be retried."""

    pass


class NonRetryableException(Exception):
    """Exception that indicates a message should not be retried."""

    pass
