"""
Local Queue Manager for distributed crawler.

File-based queue implementation for local development that mimics SQS behavior
without requiring AWS services.
"""

import asyncio
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel

from ..config.settings import CrawlerSettings
from .queue_manager import CrawlMessage, DiscoveryMessage, QueueStats

logger = logging.getLogger(__name__)


class LocalMessage(BaseModel):
    """Local message wrapper with metadata"""
    
    message_id: str
    receipt_handle: str
    body: str
    enqueued_at: datetime
    receive_count: int = 0
    visibility_timeout_until: Optional[datetime] = None


class LocalQueueManager:
    """
    Local file-based Queue Manager for crawler operations.
    
    Provides the same interface as SQSQueueManager but stores messages
    in local JSON files for development and testing.
    """
    
    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        
        # Queue file paths
        self.queue_file = settings.local_queue_file
        self.dlq_file = settings.local_queue_file.parent / "dlq.json"
        self.processing_file = settings.local_queue_file.parent / "processing.json"
        
        # Ensure queue directory exists
        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Queue configuration
        self.max_batch_size = 10
        self.receive_wait_time = 1  # Shorter for local development
        self.visibility_timeout = 300  # 5 minutes
        self.max_receive_count = 3
        
        # Thread lock for file operations
        self._lock = threading.Lock()
        
        # Statistics
        self.stats = QueueStats()
        
        logger.info(f"Local Queue Manager initialized with queue file: {self.queue_file}")
    
    async def initialize(self):
        """Initialize local queue files"""
        try:
            # Initialize queue files if they don't exist
            for queue_file in [self.queue_file, self.dlq_file, self.processing_file]:
                if not queue_file.exists():
                    self._write_queue_file(queue_file, [])
            
            logger.info("Local queue manager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize local queue manager: {e}")
            raise
    
    def _read_queue_file(self, file_path: Path) -> List[LocalMessage]:
        """Read messages from a queue file"""
        try:
            if not file_path.exists():
                return []
                
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return [LocalMessage(**msg) for msg in data]
                
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Error reading queue file {file_path}: {e}")
            return []
    
    def _write_queue_file(self, file_path: Path, messages: List[LocalMessage]):
        """Write messages to a queue file"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(
                    [msg.model_dump(mode='json') for msg in messages], 
                    f, 
                    indent=2, 
                    default=str
                )
        except Exception as e:
            logger.error(f"Error writing queue file {file_path}: {e}")
            raise
    
    async def send_crawl_message(self, message: CrawlMessage) -> bool:
        """Send a crawl message to the local queue"""
        try:
            # Create local message wrapper
            local_msg = LocalMessage(
                message_id=str(uuid4()),
                receipt_handle=str(uuid4()),
                body=message.model_dump_json(),
                enqueued_at=datetime.now(timezone.utc)
            )
            
            # Thread-safe file operation
            with self._lock:
                messages = self._read_queue_file(self.queue_file)
                messages.append(local_msg)
                self._write_queue_file(self.queue_file, messages)
            
            self.stats.crawl_messages_sent += 1
            logger.debug(f"Sent crawl message for URL: {message.url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send crawl message: {e}")
            self.stats.crawl_messages_failed += 1
            return False
    
    async def send_crawl_messages(self, messages: List[CrawlMessage]) -> int:
        """Send multiple crawl messages to the local queue"""
        success_count = 0
        
        try:
            local_messages = []
            for message in messages:
                local_msg = LocalMessage(
                    message_id=str(uuid4()),
                    receipt_handle=str(uuid4()),
                    body=message.model_dump_json(),
                    enqueued_at=datetime.now(timezone.utc)
                )
                local_messages.append(local_msg)
            
            # Thread-safe batch operation
            with self._lock:
                existing_messages = self._read_queue_file(self.queue_file)
                existing_messages.extend(local_messages)
                self._write_queue_file(self.queue_file, existing_messages)
            
            success_count = len(messages)
            self.stats.crawl_messages_sent += success_count
            self.stats.batch_operations += 1
            
            logger.debug(f"Sent batch of {success_count} crawl messages")
            
        except Exception as e:
            logger.error(f"Failed to send batch of crawl messages: {e}")
            self.stats.crawl_messages_failed += len(messages) - success_count
        
        return success_count
    
    async def _receive_messages(self, queue_url: str, max_messages: int = 10) -> List[Dict[str, Any]]:
        """Receive messages from local queue (SQS-compatible format)"""
        try:
            # Clean up expired visibility timeouts first
            await self._cleanup_expired_messages()
            
            with self._lock:
                messages = self._read_queue_file(self.queue_file)
                processing_messages = self._read_queue_file(self.processing_file)
                
                # Get available messages (not in processing)
                available_messages = [
                    msg for msg in messages 
                    if msg.visibility_timeout_until is None or 
                    msg.visibility_timeout_until <= datetime.now(timezone.utc)
                ]
                
                # Limit to max_messages
                selected_messages = available_messages[:max_messages]
                
                if not selected_messages:
                    return []
                
                # Move selected messages to processing with visibility timeout
                now = datetime.now(timezone.utc)
                for msg in selected_messages:
                    msg.visibility_timeout_until = now.total_seconds() + self.visibility_timeout
                    msg.receive_count += 1
                    processing_messages.append(msg)
                
                # Remove from main queue
                remaining_messages = [
                    msg for msg in messages 
                    if msg.message_id not in [m.message_id for m in selected_messages]
                ]
                
                # Update files
                self._write_queue_file(self.queue_file, remaining_messages)
                self._write_queue_file(self.processing_file, processing_messages)
            
            # Convert to SQS-compatible format
            sqs_messages = []
            for msg in selected_messages:
                sqs_messages.append({
                    "MessageId": msg.message_id,
                    "ReceiptHandle": msg.receipt_handle,
                    "Body": msg.body,
                    "Attributes": {
                        "ApproximateReceiveCount": str(msg.receive_count)
                    }
                })
            
            logger.debug(f"Received {len(sqs_messages)} messages from local queue")
            return sqs_messages
            
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            return []
    
    async def delete_message(self, queue_url: str, receipt_handle: str):
        """Delete a message from the processing queue"""
        try:
            with self._lock:
                processing_messages = self._read_queue_file(self.processing_file)
                
                # Remove message with matching receipt handle
                updated_messages = [
                    msg for msg in processing_messages 
                    if msg.receipt_handle != receipt_handle
                ]
                
                if len(updated_messages) != len(processing_messages):
                    self._write_queue_file(self.processing_file, updated_messages)
                    logger.debug(f"Deleted message with receipt handle: {receipt_handle}")
                else:
                    logger.warning(f"Message not found for deletion: {receipt_handle}")
            
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
    
    async def _send_to_dlq(self, message_data: Dict[str, Any], error_reason: str):
        """Send failed message to dead letter queue"""
        try:
            dlq_message = LocalMessage(
                message_id=str(uuid4()),
                receipt_handle=str(uuid4()),
                body=json.dumps({
                    "original_message": message_data,
                    "error_reason": error_reason,
                    "failed_at": datetime.now(timezone.utc).isoformat()
                }),
                enqueued_at=datetime.now(timezone.utc)
            )
            
            with self._lock:
                dlq_messages = self._read_queue_file(self.dlq_file)
                dlq_messages.append(dlq_message)
                self._write_queue_file(self.dlq_file, dlq_messages)
            
            self.stats.dlq_messages += 1
            logger.warning(f"Sent message to DLQ: {error_reason}")
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    async def _cleanup_expired_messages(self):
        """Move expired messages back to main queue"""
        try:
            now = datetime.now(timezone.utc)
            
            with self._lock:
                processing_messages = self._read_queue_file(self.processing_file)
                main_messages = self._read_queue_file(self.queue_file)
                
                expired_messages = []
                still_processing = []
                
                for msg in processing_messages:
                    if (msg.visibility_timeout_until and 
                        msg.visibility_timeout_until <= now.timestamp()):
                        # Reset visibility timeout
                        msg.visibility_timeout_until = None
                        
                        # Check if message should go to DLQ
                        if msg.receive_count >= self.max_receive_count:
                            await self._send_to_dlq(
                                {"MessageId": msg.message_id, "Body": msg.body},
                                f"Max receive count exceeded: {msg.receive_count}"
                            )
                        else:
                            expired_messages.append(msg)
                    else:
                        still_processing.append(msg)
                
                # Update files
                if expired_messages:
                    main_messages.extend(expired_messages)
                    self._write_queue_file(self.queue_file, main_messages)
                
                if len(still_processing) != len(processing_messages):
                    self._write_queue_file(self.processing_file, still_processing)
            
        except Exception as e:
            logger.error(f"Error cleaning up expired messages: {e}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on local queue manager"""
        try:
            # Check if queue files are accessible
            queue_accessible = self.queue_file.exists() and self.queue_file.is_file()
            dlq_accessible = self.dlq_file.exists() and self.dlq_file.is_file()
            processing_accessible = self.processing_file.exists() and self.processing_file.is_file()
            
            # Count messages
            with self._lock:
                queue_count = len(self._read_queue_file(self.queue_file))
                processing_count = len(self._read_queue_file(self.processing_file))
                dlq_count = len(self._read_queue_file(self.dlq_file))
            
            return {
                "status": "healthy" if all([queue_accessible, dlq_accessible, processing_accessible]) else "unhealthy",
                "queue_file_accessible": queue_accessible,
                "dlq_file_accessible": dlq_accessible,
                "processing_file_accessible": processing_accessible,
                "queue_depth": queue_count,
                "processing_depth": processing_count,
                "dlq_depth": dlq_count,
                "stats": self.stats.model_dump()
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def close(self):
        """Clean up resources"""
        try:
            # Move processing messages back to main queue
            await self._cleanup_expired_messages()
            logger.info("Local queue manager closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing local queue manager: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        return self.stats.model_dump()


# Convenience functions for creating messages (compatible with SQS version)
def create_discovery_message(
    domain: str,
    priority: int = 1,
    max_urls: Optional[int] = None,
    discovery_depth: int = 3,
    requester_id: Optional[str] = None
) -> DiscoveryMessage:
    """Create a discovery message for the queue"""
    return DiscoveryMessage(
        domain=domain,
        priority=priority,
        max_urls=max_urls,
        discovery_depth=discovery_depth,
        requester_id=requester_id
    )


def create_crawl_message(
    url: str,
    domain: str,
    priority: int = 1,
    retry_count: int = 0,
    discovery_source: Optional[str] = None
) -> CrawlMessage:
    """Create a crawl message for the queue"""
    return CrawlMessage(
        url=url,
        domain=domain,
        priority=priority,
        retry_count=retry_count,
        discovery_source=discovery_source
    )