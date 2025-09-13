"""
Distributed lock manager for URL crawling coordination.

Uses DynamoDB conditional writes to implement a distributed locking mechanism
that prevents multiple crawlers from processing the same URL simultaneously.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from pynamodb.expressions.condition import And, NotExists, Or

from ..core.types import URLStateEnum
from ..utils.url import generate_url_hash
from .client import ConditionalCheckFailedError, DynamoDBClient, get_dynamodb_client
from .models import URLStateModel

logger = logging.getLogger(__name__)


class LockAcquisitionError(Exception):
    """Raised when URL lock acquisition fails"""

    pass


class LockNotHeldError(Exception):
    """Raised when trying to operate on a lock not held by the caller"""

    pass


class DistributedLockManager:
    """
    Distributed lock manager for coordinating URL crawling across multiple crawler instances.

    Uses DynamoDB conditional writes to ensure atomic lock acquisition/release operations.
    Each URL can only be locked by one crawler at a time, preventing duplicate crawling.
    """

    def __init__(self, crawler_id: str, client: Optional[DynamoDBClient] = None):
        """
        Initialize the lock manager.

        Args:
            crawler_id: Unique identifier for this crawler instance
            client: Optional DynamoDB client (uses default if None)
        """
        self.crawler_id = crawler_id
        self.client = client or get_dynamodb_client()
        self.held_locks: Set[str] = set()

        logger.info(f"Initialized distributed lock manager for crawler {crawler_id}")

    async def try_acquire_url(self, url: str, domain: str, ttl_seconds: Optional[int] = None) -> bool:
        """
        Attempt to acquire a distributed lock on a URL for crawling.

        Uses DynamoDB conditional write to atomically transition URL from
        "pending" or "failed" state to "in_progress" with this crawler's ID.

        Args:
            url: URL to lock
            domain: Domain of the URL
            ttl_seconds: Lock TTL in seconds (defaults to settings value)

        Returns:
            True if lock was acquired, False if URL is already locked

        Raises:
            LockAcquisitionError: If acquisition fails due to unexpected error
        """
        url_hash = generate_url_hash(url)
        now = datetime.now(timezone.utc)

        if ttl_seconds is None:
            ttl_seconds = self.client.settings.acquisition_ttl_seconds

        ttl_timestamp = now + timedelta(seconds=ttl_seconds)

        try:
            # First, try to get existing item
            existing_item = await self.client.get_item(URLStateModel, url_hash)

            if existing_item is None:
                # Create new item in pending state, then acquire it
                new_item = URLStateModel(
                    url_hash=url_hash,
                    url=url,
                    domain=domain,
                    state=URLStateEnum.PENDING.value,
                    ttl=ttl_timestamp,
                )

                # Save with condition that item doesn't exist
                await self.client.put_item(new_item, condition=NotExists(URLStateModel.url_hash))

                # Now try to acquire the newly created item
                return await self._acquire_existing_url(url_hash, ttl_timestamp)

            else:
                # Try to acquire existing item
                return await self._acquire_existing_url(url_hash, ttl_timestamp)

        except ConditionalCheckFailedError:
            # Someone else beat us to it
            logger.debug(f"Failed to acquire lock on {url_hash}: conditional check failed")
            return False

        except Exception as e:
            logger.error(f"Unexpected error acquiring lock on {url_hash}: {e}")
            raise LockAcquisitionError(f"Failed to acquire URL lock: {e}") from e

    async def _acquire_existing_url(self, url_hash: str, ttl_timestamp: datetime) -> bool:
        """
        Try to acquire lock on an existing URL record.

        Args:
            url_hash: Hash of the URL
            ttl_timestamp: TTL timestamp for the lock

        Returns:
            True if acquired, False if already locked
        """
        now = datetime.now(timezone.utc)

        try:
            # Update with condition that URL is in acquirable state
            # (pending, failed, or expired in_progress)
            # Or は2引数のみ受け取るため、3条件を入れ子にする
            condition = Or(
                # URL is in pending state
                URLStateModel.state == URLStateEnum.PENDING.value,
                Or(
                    # URL is in failed state
                    URLStateModel.state == URLStateEnum.FAILED.value,
                    # URL is in_progress but TTL has expired
                    And(
                        URLStateModel.state == URLStateEnum.IN_PROGRESS.value,
                        URLStateModel.ttl <= now,
                    ),
                ),
            )

            # Prepare update actions
            actions = [
                URLStateModel.state.set(URLStateEnum.IN_PROGRESS.value),
                URLStateModel.crawler_id.set(self.crawler_id),
                URLStateModel.acquired_at.set(now),
                URLStateModel.ttl.set(ttl_timestamp),
                URLStateModel.updated_at.set(now),
            ]

            # Get the item to update
            item = await self.client.get_item(URLStateModel, url_hash)
            if item is None:
                return False

            # Perform conditional update
            await self.client.update_item(item, actions, condition=condition)

            # Track that we hold this lock
            self.held_locks.add(url_hash)

            logger.info(f"Acquired lock on URL {url_hash} for crawler {self.crawler_id}")
            return True

        except ConditionalCheckFailedError:
            # Lock is not available
            logger.debug(f"URL {url_hash} is not available for acquisition")
            return False

    async def release_url(
        self,
        url_hash: str,
        final_state: URLStateEnum,
        error_message: Optional[str] = None,
        s3_key: Optional[str] = None,
        retries: Optional[int] = None,
    ) -> None:
        """
        Release a URL lock and update its final state.

        Args:
            url_hash: Hash of the URL to release
            final_state: Final state (done or failed)
            error_message: Error message if state is failed
            s3_key: S3 key for stored content if successful
            retries: Number of retries if failed

        Raises:
            LockNotHeldError: If this crawler doesn't hold the lock
        """
        if url_hash not in self.held_locks:
            raise LockNotHeldError(f"Crawler {self.crawler_id} doesn't hold lock for {url_hash}")

        now = datetime.now(timezone.utc)

        try:
            # Get the current item
            item = await self.client.get_item(URLStateModel, url_hash)
            if item is None:
                logger.warning(f"URL {url_hash} not found during release")
                self.held_locks.discard(url_hash)
                return

            # Verify we own the lock
            if item.crawler_id != self.crawler_id:
                raise LockNotHeldError(f"Lock for {url_hash} is held by {item.crawler_id}, not {self.crawler_id}")

            # Prepare update actions
            actions = [
                URLStateModel.state.set(final_state.value),
                URLStateModel.crawler_id.remove(),  # Clear crawler_id
                URLStateModel.acquired_at.remove(),  # Clear acquired_at
                URLStateModel.updated_at.set(now),
            ]

            # Set optional fields based on final state
            if final_state == URLStateEnum.DONE:
                actions.append(URLStateModel.last_crawled.set(now))
                if s3_key:
                    actions.append(URLStateModel.s3_key.set(s3_key))
            elif final_state == URLStateEnum.FAILED:
                if error_message:
                    actions.append(URLStateModel.error_message.set(error_message))
                if retries is not None:
                    actions.append(URLStateModel.retries.set(retries))

            # Condition: we must still own the lock
            condition = And(
                URLStateModel.state == URLStateEnum.IN_PROGRESS.value, URLStateModel.crawler_id == self.crawler_id
            )

            await self.client.update_item(item, actions, condition=condition)

            # Remove from held locks
            self.held_locks.discard(url_hash)

            logger.info(f"Released lock on URL {url_hash} with state {final_state.value}")

        except ConditionalCheckFailedError as e:
            logger.error(f"Failed to release lock for {url_hash}: lock not held by this crawler")
            self.held_locks.discard(url_hash)
            raise LockNotHeldError(f"Lock for {url_hash} is no longer held by this crawler") from e
        except Exception as e:
            logger.error(f"Unexpected error releasing lock for {url_hash}: {e}")
            raise

    async def extend_lock(self, url_hash: str, additional_seconds: Optional[int] = None) -> bool:
        """
        Extend the TTL of a held lock (heartbeat mechanism).

        Args:
            url_hash: Hash of the URL whose lock to extend
            additional_seconds: Additional seconds to add (defaults to settings value)

        Returns:
            True if lock was extended, False if lock is no longer held

        Raises:
            LockNotHeldError: If this crawler doesn't hold the lock
        """
        if url_hash not in self.held_locks:
            raise LockNotHeldError(f"Crawler {self.crawler_id} doesn't hold lock for {url_hash}")

        if additional_seconds is None:
            additional_seconds = self.client.settings.acquisition_ttl_seconds

        now = datetime.now(timezone.utc)
        new_ttl = now + timedelta(seconds=additional_seconds)

        try:
            # Get current item
            item = await self.client.get_item(URLStateModel, url_hash)
            if item is None:
                logger.warning(f"URL {url_hash} not found during lock extension")
                self.held_locks.discard(url_hash)
                return False

            # Verify ownership and state
            if item.crawler_id != self.crawler_id:
                logger.warning(f"Lock for {url_hash} no longer held by {self.crawler_id}")
                self.held_locks.discard(url_hash)
                return False

            # Update TTL
            actions = [
                URLStateModel.ttl.set(new_ttl),
                URLStateModel.updated_at.set(now),
            ]

            # Condition: we must still own an active lock
            condition = And(
                URLStateModel.state == URLStateEnum.IN_PROGRESS.value, URLStateModel.crawler_id == self.crawler_id
            )

            await self.client.update_item(item, actions, condition=condition)

            logger.debug(f"Extended lock on URL {url_hash} until {new_ttl}")
            return True

        except ConditionalCheckFailedError:
            logger.warning(f"Failed to extend lock for {url_hash}: lock no longer held")
            self.held_locks.discard(url_hash)
            return False
        except Exception as e:
            logger.error(f"Unexpected error extending lock for {url_hash}: {e}")
            return False

    async def cleanup_expired_locks(self, batch_size: int = 100) -> int:
        """
        Clean up expired locks by moving them back to pending state.

        This should be run periodically to recover from crashed crawlers
        that didn't properly release their locks.

        Args:
            batch_size: Maximum number of items to process per batch

        Returns:
            Number of expired locks cleaned up
        """
        now = datetime.now(timezone.utc)
        cleaned_count = 0

        try:
            # Find expired in_progress items
            expired_items = await self.client.scan_items(
                URLStateModel,
                filter_condition=And(URLStateModel.state == URLStateEnum.IN_PROGRESS.value, URLStateModel.ttl <= now),
                limit=batch_size,
            )

            logger.info(f"Found {len(expired_items)} expired locks to clean up")

            for item in expired_items:
                try:
                    # Reset to pending state
                    actions = [
                        URLStateModel.state.set(URLStateEnum.PENDING.value),
                        URLStateModel.crawler_id.remove(),
                        URLStateModel.acquired_at.remove(),
                        URLStateModel.updated_at.set(now),
                        # Keep TTL for rate limiting purposes
                    ]

                    # Condition: item is still expired and in_progress
                    condition = And(URLStateModel.state == URLStateEnum.IN_PROGRESS.value, URLStateModel.ttl <= now)

                    await self.client.update_item(item, actions, condition=condition)
                    cleaned_count += 1

                    logger.debug(f"Cleaned up expired lock for URL {item.url_hash}")

                except ConditionalCheckFailedError:
                    # Item was modified by another process, skip
                    continue
                except Exception as e:
                    logger.error(f"Error cleaning up expired lock {item.url_hash}: {e}")
                    continue

            logger.info(f"Cleaned up {cleaned_count} expired locks")
            return cleaned_count

        except Exception as e:
            logger.error(f"Error during expired lock cleanup: {e}")
            return cleaned_count

    async def get_held_locks(self) -> List[Dict[str, Optional[str]]]:
        """
        Get information about locks currently held by this crawler.

        Returns:
            List of dictionaries with lock information
        """
        held_lock_info: List[Dict[str, Optional[str]]] = []

        for url_hash in list(self.held_locks):
            try:
                item = await self.client.get_item(URLStateModel, url_hash)
                if item and item.crawler_id == self.crawler_id:
                    held_lock_info.append(
                        {
                            "url_hash": url_hash,
                            "url": item.url,
                            "domain": item.domain,
                            "acquired_at": item.acquired_at.isoformat() if item.acquired_at else None,
                            "ttl": item.ttl.isoformat() if item.ttl else None,
                        }
                    )
                else:
                    # Lock is no longer held, clean up
                    self.held_locks.discard(url_hash)

            except Exception as e:
                logger.error(f"Error checking held lock {url_hash}: {e}")
                self.held_locks.discard(url_hash)

        return held_lock_info

    async def force_release_all_locks(self) -> int:
        """
        Force release all locks held by this crawler.

        This should be called during graceful shutdown.

        Returns:
            Number of locks released
        """
        logger.info(f"Force releasing {len(self.held_locks)} locks for crawler {self.crawler_id}")

        released_count = 0
        held_locks_copy = list(self.held_locks)

        for url_hash in held_locks_copy:
            try:
                await self.release_url(
                    url_hash, URLStateEnum.FAILED, error_message=f"Crawler {self.crawler_id} shutdown"
                )
                released_count += 1

            except Exception as e:
                logger.error(f"Error force-releasing lock {url_hash}: {e}")
                # Remove from held_locks anyway
                self.held_locks.discard(url_hash)

        logger.info(f"Force released {released_count} locks")
        return released_count

    def get_stats(self) -> Dict[str, Any]:
        """Get lock manager statistics"""
        return {
            "held_locks_count": len(self.held_locks),
            "crawler_id": self.crawler_id,
        }


# Helper functions for common lock management patterns


async def with_url_lock(
    lock_manager: DistributedLockManager,
    url: str,
    domain: str,
    operation: Callable[..., Awaitable[Any]],
    *args: Any,
    **kwargs: Any,
) -> Optional[Any]:
    """
    Context manager-like function for URL lock acquisition and release.

    Args:
        lock_manager: Lock manager instance
        url: URL to lock
        domain: Domain of the URL
        operation: Async function to execute while holding the lock
        *args: Arguments for the operation
        **kwargs: Keyword arguments for the operation

    Returns:
        Result of the operation or None if lock couldn't be acquired
    """
    url_hash = generate_url_hash(url)

    # Try to acquire lock
    acquired = await lock_manager.try_acquire_url(url, domain)
    if not acquired:
        logger.debug(f"Could not acquire lock for URL: {url}")
        return None

    try:
        # Execute operation while holding lock
        result: Any = await operation(*args, **kwargs)

        # Release with success state
        await lock_manager.release_url(url_hash, URLStateEnum.DONE, s3_key=getattr(result, "s3_key", None))

        return result

    except Exception as e:
        # Release with failure state
        await lock_manager.release_url(url_hash, URLStateEnum.FAILED, error_message=str(e))
        raise


if __name__ == "__main__":
    # CLI utility for lock management operations
    import asyncio
    import sys

    # from ..config.settings import load_settings  # CLI では未使用

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python lock_manager.py [cleanup|stats] [crawler_id]")
            sys.exit(1)

        command = sys.argv[1]
        crawler_id = sys.argv[2] if len(sys.argv) > 2 else "cli-manager"

        # settings = load_settings()  # 未使用のためコメントアウト
        lock_manager = DistributedLockManager(crawler_id)

        if command == "cleanup":
            print("Cleaning up expired locks...")
            cleaned = await lock_manager.cleanup_expired_locks()
            print(f"Cleaned up {cleaned} expired locks")

        elif command == "stats":
            stats = lock_manager.get_stats()
            held_locks = await lock_manager.get_held_locks()

            print("Lock Manager Stats:")
            print(f"  Crawler ID: {stats['crawler_id']}")
            print(f"  Held locks: {stats['held_locks_count']}")

            if held_locks:
                print("\nHeld locks details:")
                for lock_info in held_locks:
                    print(f"  - {lock_info['url']} (expires: {lock_info['ttl']})")

        else:
            print(f"Unknown command: {command}")
            sys.exit(1)

    asyncio.run(main())
