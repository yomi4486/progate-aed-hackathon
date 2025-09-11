"""
URL state management for the distributed crawler.

Provides high-level state transition operations on top of the distributed
lock manager, with support for crawl results, retry scheduling, and
batch operations.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pynamodb.expressions.condition import And, Or

from ...schema.crawl import CrawlResult
from ..core.types import URLStateEnum
from ..utils.url import extract_domain, generate_url_hash
from .client import DynamoDBClient, get_dynamodb_client
from .lock_manager import DistributedLockManager
from .models import URLStateModel

logger = logging.getLogger(__name__)


class StateTransitionError(Exception):
    """Raised when state transition is invalid or fails"""

    pass


class URLStateManager:
    """
    High-level URL state management with support for crawl results,
    retry scheduling, and efficient batch operations.

    Works with DistributedLockManager to provide complete URL lifecycle
    management from discovery through successful crawling or failure.
    """

    def __init__(
        self,
        crawler_id: str,
        client: Optional[DynamoDBClient] = None,
        lock_manager: Optional[DistributedLockManager] = None,
    ):
        """
        Initialize the URL state manager.

        Args:
            crawler_id: Unique identifier for this crawler instance
            client: Optional DynamoDB client
            lock_manager: Optional lock manager (creates new one if None)
        """
        self.crawler_id = crawler_id
        self.client = client or get_dynamodb_client()
        self.lock_manager = lock_manager or DistributedLockManager(crawler_id, client)

        # Statistics tracking
        self.stats = {
            "states_updated": 0,
            "retries_scheduled": 0,
            "batch_operations": 0,
            "pending_urls_fetched": 0,
            "errors_encountered": 0,
        }

        logger.info(f"Initialized URL state manager for crawler {crawler_id}")

    async def add_url(
        self, url: str, domain: Optional[str] = None, initial_state: URLStateEnum = URLStateEnum.PENDING
    ) -> str:
        """
        Add a new URL to the system.

        Args:
            url: URL to add
            domain: Domain (extracted from URL if not provided)
            initial_state: Initial state for the URL

        Returns:
            URL hash of the added URL

        Raises:
            StateTransitionError: If URL addition fails
        """
        url_hash = generate_url_hash(url)

        if domain is None:
            domain = extract_domain(url)

        now = datetime.now(timezone.utc)
        ttl_timestamp = now + timedelta(seconds=self.client.settings.acquisition_ttl_seconds)

        try:
            # Check if URL already exists
            existing_item = await self.client.get_item(URLStateModel, url_hash)
            if existing_item is not None:
                logger.debug(f"URL {url} already exists with state {existing_item.state}")
                return url_hash

            # Create new URL state record
            new_item = URLStateModel(
                url_hash=url_hash,
                url=url,
                domain=domain,
                state=initial_state.value,
                ttl=ttl_timestamp,
                created_at=now,
                updated_at=now,
            )

            await self.client.put_item(new_item)

            logger.info(f"Added new URL {url} with state {initial_state.value}")
            return url_hash

        except Exception as e:
            self.stats["errors_encountered"] += 1
            raise StateTransitionError(f"Failed to add URL {url}: {e}") from e

    async def update_state(
        self,
        url_hash: str,
        new_state: URLStateEnum,
        crawler_id: Optional[str] = None,
        result: Optional[CrawlResult] = None,
        error: Optional[str] = None,
        retries: Optional[int] = None,
    ) -> bool:
        """
        Update URL state with optional crawl result data.

        Args:
            url_hash: Hash of URL to update
            new_state: New state to set
            crawler_id: Crawler ID (defaults to this manager's crawler_id)
            result: Optional crawl result data
            error: Optional error message
            retries: Optional retry count

        Returns:
            True if state was updated successfully

        Raises:
            StateTransitionError: If state update fails
        """
        if crawler_id is None:
            crawler_id = self.crawler_id

        now = datetime.now(timezone.utc)

        try:
            # Get current item
            current_item = await self.client.get_item(URLStateModel, url_hash)
            if current_item is None:
                raise StateTransitionError(f"URL {url_hash} not found")

            # Validate state transition
            self._validate_state_transition(current_item.state, new_state.value, crawler_id)

            # Prepare update actions
            actions = [
                URLStateModel.state.set(new_state.value),
                URLStateModel.updated_at.set(now),
            ]

            # Update fields based on new state
            if new_state == URLStateEnum.IN_PROGRESS:
                actions.extend(
                    [
                        URLStateModel.crawler_id.set(crawler_id),
                        URLStateModel.acquired_at.set(now),
                        URLStateModel.ttl.set(now + timedelta(seconds=self.client.settings.acquisition_ttl_seconds)),
                    ]
                )

            elif new_state == URLStateEnum.DONE:
                actions.append(URLStateModel.last_crawled.set(now))
                if result:
                    if result.html_s3_key:
                        actions.append(URLStateModel.s3_key.set(result.html_s3_key))
                # Clear lock-related fields
                if current_item.crawler_id:
                    actions.append(URLStateModel.crawler_id.remove())
                if current_item.acquired_at:
                    actions.append(URLStateModel.acquired_at.remove())

            elif new_state == URLStateEnum.FAILED:
                if error:
                    actions.append(URLStateModel.error_message.set(error))
                if retries is not None:
                    actions.append(URLStateModel.retries.set(retries))
                # Clear lock-related fields
                if current_item.crawler_id:
                    actions.append(URLStateModel.crawler_id.remove())
                if current_item.acquired_at:
                    actions.append(URLStateModel.acquired_at.remove())

            elif new_state == URLStateEnum.PENDING:
                # Clear all processing-related fields
                if current_item.crawler_id:
                    actions.append(URLStateModel.crawler_id.remove())
                if current_item.acquired_at:
                    actions.append(URLStateModel.acquired_at.remove())
                if current_item.error_message:
                    actions.append(URLStateModel.error_message.remove())

            # Perform the update
            await self.client.update_item(current_item, actions)

            self.stats["states_updated"] += 1
            logger.info(f"Updated URL {url_hash} state to {new_state.value}")
            return True

        except Exception as e:
            self.stats["errors_encountered"] += 1
            raise StateTransitionError(f"Failed to update URL {url_hash} state: {e}") from e

    def _validate_state_transition(self, current_state: str, new_state: str, crawler_id: str) -> None:
        """
        Validate that a state transition is allowed.

        Args:
            current_state: Current state
            new_state: Requested new state
            crawler_id: Crawler requesting the transition

        Raises:
            StateTransitionError: If transition is not allowed
        """
        # Define valid state transitions
        valid_transitions = {
            URLStateEnum.PENDING.value: [URLStateEnum.IN_PROGRESS.value],
            URLStateEnum.IN_PROGRESS.value: [
                URLStateEnum.DONE.value,
                URLStateEnum.FAILED.value,
                URLStateEnum.PENDING.value,  # For timeout recovery
            ],
            URLStateEnum.FAILED.value: [
                URLStateEnum.PENDING.value,  # For retry
                URLStateEnum.IN_PROGRESS.value,  # Direct retry
            ],
            URLStateEnum.DONE.value: [
                URLStateEnum.PENDING.value  # For re-crawling
            ],
        }

        if new_state not in valid_transitions.get(current_state, []):
            raise StateTransitionError(f"Invalid state transition from {current_state} to {new_state}")

    async def schedule_retry(
        self, url_hash: str, delay_seconds: int, max_retries: Optional[int] = None, error_message: Optional[str] = None
    ) -> bool:
        """
        Schedule a URL for retry after a specified delay.

        Args:
            url_hash: Hash of URL to retry
            delay_seconds: Delay before retry becomes available
            max_retries: Maximum retry count (defaults to settings)
            error_message: Optional error message

        Returns:
            True if retry was scheduled, False if max retries exceeded
        """
        if max_retries is None:
            max_retries = self.client.settings.max_retries

        try:
            # Get current item
            current_item = await self.client.get_item(URLStateModel, url_hash)
            if current_item is None:
                logger.error(f"Cannot schedule retry for non-existent URL {url_hash}")
                return False

            # Check retry limit
            current_retries = current_item.retries or 0
            if current_retries >= max_retries:
                logger.info(f"URL {url_hash} has exceeded max retries ({max_retries})")
                return False

            # Calculate retry time
            now = datetime.now(timezone.utc)
            retry_time = now + timedelta(seconds=delay_seconds)

            # Update state to failed with retry scheduling
            actions = [
                URLStateModel.state.set(URLStateEnum.FAILED.value),
                URLStateModel.retries.set(current_retries + 1),
                URLStateModel.ttl.set(retry_time),  # TTL acts as retry delay
                URLStateModel.updated_at.set(now),
            ]

            if error_message:
                actions.append(URLStateModel.error_message.set(error_message))

            # Clear lock fields
            if current_item.crawler_id:
                actions.append(URLStateModel.crawler_id.remove())
            if current_item.acquired_at:
                actions.append(URLStateModel.acquired_at.remove())

            await self.client.update_item(current_item, actions)

            self.stats["retries_scheduled"] += 1
            logger.info(
                f"Scheduled retry for URL {url_hash} in {delay_seconds}s (attempt {current_retries + 1}/{max_retries})"
            )
            return True

        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to schedule retry for URL {url_hash}: {e}")
            return False

    async def get_pending_urls_for_domain(
        self, domain: str, limit: int = 100, exclude_recently_failed: bool = True
    ) -> List[str]:
        """
        Get pending URLs for a specific domain.

        Args:
            domain: Domain to query
            limit: Maximum number of URLs to return
            exclude_recently_failed: Whether to exclude recently failed URLs

        Returns:
            List of URL hashes ready for crawling
        """
        try:
            now = datetime.now(timezone.utc)

            # Query using domain GSI
            filter_conditions: List[Any] = []

            if exclude_recently_failed:
                # Exclude URLs with unexpired TTL (recently failed)
                filter_conditions.append(Or(URLStateModel.ttl <= now, URLStateModel.ttl.does_not_exist()))

            # Combine filter conditions
            filter_condition: Any = None
            if filter_conditions:
                filter_condition = And(*filter_conditions) if len(filter_conditions) > 1 else filter_conditions[0]

            # Query pending URLs for the domain
            items: List[URLStateModel] = await self.client.query_items(
                URLStateModel,
                hash_key=domain,
                range_key_condition=URLStateModel.state == URLStateEnum.PENDING.value,
                filter_condition=filter_condition,
                limit=limit,
                index="domain-state-index",
            )

            url_hashes: List[str] = [item.url_hash for item in items]

            self.stats["pending_urls_fetched"] += len(url_hashes)
            logger.debug(f"Retrieved {len(url_hashes)} pending URLs for domain {domain}")

            return url_hashes

        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to get pending URLs for domain {domain}: {e}")
            return []

    async def get_urls_by_state(
        self, state: URLStateEnum, limit: int = 100, domain: Optional[str] = None
    ) -> List[URLStateModel]:
        """
        Get URLs by state, optionally filtered by domain.

        Args:
            state: State to filter by
            limit: Maximum number of URLs to return
            domain: Optional domain filter

        Returns:
            List of URLStateModel instances
        """
        try:
            if domain:
                # Use domain GSI
                items = await self.client.query_items(
                    URLStateModel,
                    hash_key=domain,
                    range_key_condition=URLStateModel.state == state.value,
                    limit=limit,
                    index="DomainStateIndex",
                )
            else:
                # Scan entire table
                items = await self.client.scan_items(
                    URLStateModel, filter_condition=URLStateModel.state == state.value, limit=limit
                )

            logger.debug(f"Retrieved {len(items)} URLs with state {state.value}")
            return items

        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to get URLs by state {state.value}: {e}")
            return []

    async def batch_add_urls(self, urls: List[str], initial_state: URLStateEnum = URLStateEnum.PENDING) -> List[str]:
        """
        Add multiple URLs in batch for better performance.

        Args:
            urls: List of URLs to add
            initial_state: Initial state for all URLs

        Returns:
            List of URL hashes for successfully added URLs
        """
        if not urls:
            return []

        now = datetime.now(timezone.utc)
        ttl_timestamp = now + timedelta(seconds=self.client.settings.acquisition_ttl_seconds)

        # Prepare items for batch insert
        items_to_save: List[URLStateModel] = []
        url_hashes: List[str] = []

        for url in urls:
            try:
                url_hash = generate_url_hash(url)
                domain = extract_domain(url)

                item = URLStateModel(
                    url_hash=url_hash,
                    url=url,
                    domain=domain,
                    state=initial_state.value,
                    ttl=ttl_timestamp,
                    created_at=now,
                    updated_at=now,
                )

                items_to_save.append(item)
                url_hashes.append(url_hash)

            except Exception as e:
                logger.error(f"Error preparing URL {url} for batch insert: {e}")
                continue

        if not items_to_save:
            return []

        try:
            # Batch write to DynamoDB
            await self.client.batch_write_items(items_to_save)  # type: ignore[arg-type]

            self.stats["batch_operations"] += 1
            logger.info(f"Batch added {len(items_to_save)} URLs")

            return url_hashes

        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Batch add URLs failed: {e}")
            return []

    async def get_domain_statistics(self, domain: str) -> Dict[str, int]:
        """
        Get statistics for a specific domain.

        Args:
            domain: Domain to get statistics for

        Returns:
            Dictionary with state counts
        """
        stats = {state.value: 0 for state in URLStateEnum}

        try:
            # Query all states for the domain using GSI
            for state in URLStateEnum:
                items = await self.client.query_items(
                    URLStateModel,
                    hash_key=domain,
                    range_key_condition=URLStateModel.state == state.value,
                    limit=1000,  # Reasonable limit for counting
                    index="domain-state-index",
                )
                stats[state.value] = len(items)

            logger.debug(f"Retrieved statistics for domain {domain}: {stats}")
            return stats

        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Failed to get domain statistics for {domain}: {e}")
            return stats

    async def batch_create_url_states(self, url_states: List[Dict[str, Any]]) -> List[str]:
        """
        Create multiple URL state entries from prepared state data.

        Args:
            url_states: List of dictionaries containing URL state data

        Returns:
            List of URL hashes for successfully created URLs
        """
        if not url_states:
            return []

        # Prepare items for batch insert
        items_to_save: List[URLStateModel] = []
        url_hashes: List[str] = []

        now = datetime.now(timezone.utc)
        ttl_timestamp = now + timedelta(seconds=self.client.settings.acquisition_ttl_seconds)

        for state_data in url_states:
            try:
                # Extract state from URLStateEnum if it's provided as a string
                state_value = state_data.get("state", URLStateEnum.PENDING.value)
                if isinstance(state_value, str):
                    state_value = state_value
                else:
                    state_value = state_value.value if hasattr(state_value, "value") else str(state_value)

                item = URLStateModel(
                    url_hash=state_data["url_hash"],
                    url=state_data["url"],
                    domain=state_data["domain"],
                    state=state_value,
                    ttl=ttl_timestamp,
                    created_at=state_data.get("created_at", now),
                    updated_at=state_data.get("updated_at", now),
                )

                items_to_save.append(item)
                url_hashes.append(state_data["url_hash"])

            except Exception as e:
                logger.error(f"Error preparing URL state {state_data.get('url_hash', 'unknown')} for batch insert: {e}")
                continue

        if not items_to_save:
            return []

        try:
            # Batch write to DynamoDB
            await self.client.batch_write_items(items_to_save)  # type: ignore[arg-type]

            self.stats["batch_operations"] += 1
            logger.info(f"Batch created {len(items_to_save)} URL states")

            return url_hashes

        except Exception as e:
            self.stats["errors_encountered"] += 1
            logger.error(f"Batch create URL states failed: {e}")
            return []

    async def get_url_state(self, url: str) -> Optional[URLStateModel]:
        """
        Get URL state for a single URL.

        Args:
            url: URL to check

        Returns:
            URLStateModel if exists, None otherwise
        """
        try:
            url_hash = generate_url_hash(url)
            item = await self.client.get_item(URLStateModel, url_hash)
            return item

        except Exception as e:
            logger.error(f"Error getting URL state for {url}: {e}")
            return None

    async def batch_get_url_states(self, urls: List[str]) -> Dict[str, URLStateModel]:
        """
        Get URL states for multiple URLs in batch.

        Args:
            urls: List of URLs to check

        Returns:
            Dictionary mapping url_hash to URLStateModel for existing URLs
        """
        if not urls:
            return {}

        try:
            # Convert URLs to hashes
            url_hashes = [generate_url_hash(url) for url in urls]

            # Use batch_get_item to fetch multiple states efficiently
            # PynamoDB batch_get expects just hash keys for hash-only tables
            keys = url_hashes
            items = await self.client.batch_get_items(URLStateModel, keys)

            # Convert to dictionary mapping url_hash to model
            result: Dict[str, Any] = {}
            for item in items:
                result[item.url_hash] = item  # type: ignore

            logger.debug(f"Batch fetched states for {len(items)}/{len(urls)} URLs")
            return result

        except Exception as e:
            logger.error(f"Error batch getting URL states: {e}")
            # Return empty dict on error - assume all URLs are new
            return {}

    def get_stats(self) -> Dict[str, Any]:
        """Get URL state manager statistics"""
        return {
            **self.stats,
            "crawler_id": self.crawler_id,
        }

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on state manager.

        Returns:
            Health status information
        """
        try:
            # Test basic operations
            await self.get_domain_statistics("example.com")

            return {
                "status": "healthy",
                "operations_completed": sum(self.stats.values()),
                "error_rate": (self.stats["errors_encountered"] / max(1, sum(self.stats.values()))),
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
            }


if __name__ == "__main__":
    # CLI utility for state management operations
    import asyncio
    import sys

    # from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python state_manager.py [stats|test|domain] [args...]")
            sys.exit(1)

        command = sys.argv[1]
        crawler_id = "cli-state-manager"

        state_manager = URLStateManager(crawler_id)

        if command == "stats":
            stats = state_manager.get_stats()
            health = await state_manager.health_check()
            print(f"Stats: {stats}")
            print(f"Health: {health}")

        elif command == "test":
            print("Testing state manager...")

            # Add test URL
            test_url = "https://example.com/test"
            url_hash = await state_manager.add_url(test_url)
            print(f"Added URL: {url_hash}")

            # Update state
            success = await state_manager.update_state(url_hash, URLStateEnum.IN_PROGRESS)
            print(f"Updated to in_progress: {success}")

            # Schedule retry
            retry_success = await state_manager.schedule_retry(url_hash, 60, error_message="Test retry")
            print(f"Scheduled retry: {retry_success}")

            print("State manager test completed!")

        elif command == "domain" and len(sys.argv) > 2:
            domain = sys.argv[2]
            print(f"Getting statistics for domain: {domain}")

            stats = await state_manager.get_domain_statistics(domain)
            print(f"Domain statistics: {stats}")

            pending_urls = await state_manager.get_pending_urls_for_domain(domain)
            print(f"Pending URLs: {len(pending_urls)}")

        else:
            print(f"Unknown command: {command}")
            sys.exit(1)

    asyncio.run(main())
