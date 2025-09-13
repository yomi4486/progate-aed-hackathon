"""
LocalStack-compatible distributed lock manager.

Provides the same interface as DistributedLockManager but uses the LocalStack
DynamoDB client for better endpoint compatibility.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from ..core.types import URLStateEnum
from .localstack_client import LocalStackDynamoDBClient

logger = logging.getLogger(__name__)


class LocalStackDistributedLockManager:
    """
    Distributed lock manager using LocalStack DynamoDB client.

    Provides URL-level locking for distributed crawler coordination
    using DynamoDB conditional writes.
    """

    def __init__(self, crawler_id: str, client: LocalStackDynamoDBClient):
        self.crawler_id = crawler_id
        self.client = client

        # Lock statistics
        self.stats = {
            "locks_acquired": 0,
            "locks_released": 0,
            "lock_failures": 0,
            "lock_timeouts": 0,
        }

        logger.info(f"LocalStack distributed lock manager initialized for crawler {crawler_id}")

    async def try_acquire_lock(self, url_hash: str, timeout_seconds: Optional[int] = None) -> bool:
        """
        Try to acquire a distributed lock on a URL.

        Args:
            url_hash: Hash of the URL to lock
            timeout_seconds: Lock timeout (defaults to settings)

        Returns:
            True if lock was acquired, False otherwise
        """
        if timeout_seconds is None:
            timeout_seconds = self.client.settings.acquisition_ttl_seconds

        now = datetime.now(timezone.utc)
        ttl_timestamp = int((now + timedelta(seconds=timeout_seconds)).timestamp())

        try:
            # Try to acquire lock by updating from PENDING to IN_PROGRESS
            # with condition that state is PENDING and no active crawler
            updates = {
                "state": URLStateEnum.IN_PROGRESS.value,
                "crawler_id": self.crawler_id,
                "acquired_at": now,
                "updated_at": now,
                "ttl": ttl_timestamp,
            }

            # Condition: state must be PENDING and either no crawler_id or expired acquisition
            now_timestamp = int(now.timestamp())
            condition = "attribute_exists(#state) AND #state = :pending AND (attribute_not_exists(crawler_id) OR attribute_not_exists(acquired_at) OR #ttl < :now)"

            success = await self._update_item_with_condition(
                url_hash, updates, condition, {":pending": URLStateEnum.PENDING.value, ":now": now_timestamp}
            )

            if success:
                self.stats["locks_acquired"] += 1
                logger.debug(f"Acquired lock on URL {url_hash}")
                return True
            else:
                self.stats["lock_failures"] += 1
                logger.debug(f"Failed to acquire lock on URL {url_hash} (contention)")
                return False

        except Exception as e:
            self.stats["lock_failures"] += 1
            logger.error(f"Error acquiring lock on URL {url_hash}: {e}")
            return False

    async def release_lock(self, url_hash: str, new_state: URLStateEnum) -> bool:
        """
        Release a distributed lock and update URL state.

        Args:
            url_hash: Hash of the URL to unlock
            new_state: New state to set (DONE, FAILED, or PENDING)

        Returns:
            True if lock was released, False otherwise
        """
        now = datetime.now(timezone.utc)

        try:
            updates = {
                "state": new_state.value,
                "updated_at": now,
                "crawler_id": None,  # Clear the lock
                "acquired_at": None,
            }

            # Add state-specific fields
            if new_state == URLStateEnum.DONE:
                updates["last_crawled"] = now
            elif new_state == URLStateEnum.FAILED:
                # Keep retry-related fields if they exist
                pass
            elif new_state == URLStateEnum.PENDING:
                # Clear error message for retry
                updates["error_message"] = None

            # Condition: must be held by this crawler
            # condition = f"crawler_id = :crawler_id"  # type: ignore  # Unused variable

            # Build condition expression manually since we need attribute values
            success = await self._update_with_crawler_condition(url_hash, updates, self.crawler_id)

            if success:
                self.stats["locks_released"] += 1
                logger.debug(f"Released lock on URL {url_hash}, new state: {new_state.value}")
                return True
            else:
                logger.warning(f"Failed to release lock on URL {url_hash} (not held by this crawler)")
                return False

        except Exception as e:
            logger.error(f"Error releasing lock on URL {url_hash}: {e}")
            return False

    async def _update_item_with_condition(
        self, url_hash: str, updates: Dict[str, Any], condition_expression: str, condition_values: Dict[str, Any]
    ) -> bool:
        """Update item with custom condition expression."""
        try:
            # Build update expression manually
            set_parts: list[str] = []
            remove_parts: list[str] = []
            expression_values: Dict[str, Any] = {**condition_values}
            expression_names: Dict[str, str] = {}

            for key, value in updates.items():
                if value is None:
                    remove_parts.append(f"#{key}")
                    expression_names[f"#{key}"] = key
                else:
                    set_parts.append(f"#{key} = :{key}")
                    expression_names[f"#{key}"] = key

                    if isinstance(value, str):
                        expression_values[f":{key}"] = {"S": value}
                    elif isinstance(value, int):
                        expression_values[f":{key}"] = {"N": str(value)}
                    elif isinstance(value, datetime):
                        expression_values[f":{key}"] = {"S": value.isoformat()}
                    else:
                        expression_values[f":{key}"] = {"S": str(value)}

            # Convert condition values to DynamoDB format
            dynamodb_condition_values: Dict[str, Any] = {}
            for key, value in condition_values.items():
                if isinstance(value, str):
                    dynamodb_condition_values[key] = {"S": value}
                elif isinstance(value, int):
                    dynamodb_condition_values[key] = {"N": str(value)}
                else:
                    dynamodb_condition_values[key] = {"S": str(value)}

            expression_values.update(dynamodb_condition_values)  # type: ignore

            # Add attribute names for condition
            expression_names["#state"] = "state"
            expression_names["#ttl"] = "ttl"

            # Build proper UpdateExpression
            update_expression_parts: list[str] = []
            if set_parts:
                update_expression_parts.append(f"SET {', '.join(set_parts)}")
            if remove_parts:
                update_expression_parts.append(f"REMOVE {', '.join(remove_parts)}")

            if not update_expression_parts:
                return True

            update_args = {
                "TableName": self.client.table_name,
                "Key": {"url_hash": {"S": url_hash}},
                "UpdateExpression": " ".join(update_expression_parts),
                "ConditionExpression": condition_expression,
                "ExpressionAttributeNames": expression_names,
                "ExpressionAttributeValues": expression_values,
            }

            self.client.client.update_item(**update_args)  # type: ignore
            return True

        except Exception as e:
            if "ConditionalCheckFailedException" in str(e):
                return False
            raise

    async def _update_with_crawler_condition(
        self, url_hash: str, updates: Dict[str, Any], expected_crawler_id: str
    ) -> bool:
        """Update item with condition that crawler_id matches expected value."""
        try:
            # Build update expression manually
            set_parts: list[str] = []
            remove_parts: list[str] = []
            expression_values: Dict[str, Any] = {":crawler_id": {"S": expected_crawler_id}}
            expression_names: Dict[str, str] = {}

            for key, value in updates.items():
                if value is None:
                    remove_parts.append(f"#{key}")
                    expression_names[f"#{key}"] = key
                else:
                    set_parts.append(f"#{key} = :{key}")
                    expression_names[f"#{key}"] = key

                    if isinstance(value, str):
                        expression_values[f":{key}"] = {"S": value}
                    elif isinstance(value, int):
                        expression_values[f":{key}"] = {"N": str(value)}
                    elif isinstance(value, datetime):
                        expression_values[f":{key}"] = {"S": value.isoformat()}
                    else:
                        expression_values[f":{key}"] = {"S": str(value)}

            # Build proper UpdateExpression
            update_expression_parts: list[str] = []
            if set_parts:
                update_expression_parts.append(f"SET {', '.join(set_parts)}")
            if remove_parts:
                update_expression_parts.append(f"REMOVE {', '.join(remove_parts)}")

            if not update_expression_parts:
                return True

            update_args = {
                "TableName": self.client.table_name,
                "Key": {"url_hash": {"S": url_hash}},
                "UpdateExpression": " ".join(update_expression_parts),
                "ConditionExpression": "crawler_id = :crawler_id",
                "ExpressionAttributeNames": expression_names,
                "ExpressionAttributeValues": expression_values,
            }

            self.client.client.update_item(**update_args)  # type: ignore
            return True

        except Exception as e:
            if "ConditionalCheckFailedException" in str(e):
                return False
            raise

    async def check_lock_status(self, url_hash: str) -> Dict[str, Any]:
        """
        Check the current lock status of a URL.

        Args:
            url_hash: Hash of the URL to check

        Returns:
            Dictionary with lock status information
        """
        try:
            item = await self.client.get_item(url_hash)

            if not item:
                return {"locked": False, "exists": False}

            now = datetime.now(timezone.utc)

            # Check if URL is locked
            crawler_id = item.get("crawler_id")
            acquired_at_str = item.get("acquired_at")
            state = item.get("state")
            ttl = item.get("ttl", 0)

            is_locked = (
                crawler_id is not None
                and acquired_at_str is not None
                and state == URLStateEnum.IN_PROGRESS.value
                and ttl > int(now.timestamp())
            )

            return {
                "locked": is_locked,
                "exists": True,
                "state": state,
                "crawler_id": crawler_id,
                "acquired_at": acquired_at_str,
                "ttl": ttl,
                "owned_by_us": crawler_id == self.crawler_id if crawler_id else False,
            }

        except Exception as e:
            logger.error(f"Error checking lock status for URL {url_hash}: {e}")
            return {"locked": False, "exists": False, "error": str(e)}

    async def cleanup_expired_locks(self, max_cleanup: int = 100) -> int:
        """
        Clean up expired locks by resetting them to PENDING state.

        Args:
            max_cleanup: Maximum number of locks to clean up in one call

        Returns:
            Number of locks cleaned up
        """
        try:
            # now_timestamp = int(datetime.now(timezone.utc).timestamp())  # Unused

            # Scan for items with expired locks
            items = await self.client.scan_table(
                filter_expression="#state = :in_progress AND #ttl < :now", limit=max_cleanup
            )

            if not items:
                return 0

            cleaned_count = 0

            for item in items:
                url_hash = item["url_hash"]

                # Reset to PENDING state
                updates = {
                    "state": URLStateEnum.PENDING.value,
                    "updated_at": datetime.now(timezone.utc),
                    "crawler_id": None,
                    "acquired_at": None,
                }

                success = await self.client.update_item(url_hash, updates)
                if success:
                    cleaned_count += 1
                    logger.debug(f"Cleaned up expired lock on URL {url_hash}")

            logger.info(f"Cleaned up {cleaned_count} expired locks")
            return cleaned_count

        except Exception as e:
            logger.error(f"Error cleaning up expired locks: {e}")
            return 0

    @asynccontextmanager
    async def acquire_lock_context(self, url_hash: str, timeout_seconds: Optional[int] = None):
        """
        Context manager for acquiring and automatically releasing locks.

        Args:
            url_hash: Hash of the URL to lock
            timeout_seconds: Lock timeout

        Yields:
            True if lock was acquired, False otherwise

        Example:
            async with lock_manager.acquire_lock_context(url_hash) as acquired:
                if acquired:
                    # Perform crawling
                    await crawl_url(url)
                    # Lock will be automatically released as DONE
        """
        acquired = await self.try_acquire_lock(url_hash, timeout_seconds)

        try:
            yield acquired
        finally:
            if acquired:
                # Default to DONE state on successful context exit
                # This can be overridden by calling release_lock explicitly
                lock_status = await self.check_lock_status(url_hash)
                if lock_status.get("owned_by_us", False):
                    await self.release_lock(url_hash, URLStateEnum.DONE)

    def get_stats(self) -> Dict[str, Any]:
        """Get lock manager statistics."""
        return {**self.stats, "crawler_id": self.crawler_id}

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on lock manager."""
        try:
            # Test basic lock operations
            test_url_hash = f"health_check_{self.crawler_id}"

            # Try to acquire and immediately release a test lock
            acquired = await self.try_acquire_lock(test_url_hash, 60)

            if acquired:
                released = await self.release_lock(test_url_hash, URLStateEnum.PENDING)
                return {"status": "healthy", "test_lock_acquired": True, "test_lock_released": released}
            else:
                return {"status": "degraded", "test_lock_acquired": False, "message": "Could not acquire test lock"}

        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
