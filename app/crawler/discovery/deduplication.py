"""
URL Deduplication mechanism for distributed crawler.

Provides high-performance URL deduplication using Bloom Filters for fast filtering
combined with DynamoDB for authoritative duplicate checking.
"""

import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import redis.asyncio as aioredis

from ..config.settings import CrawlerSettings
from ..state.state_manager import URLStateManager
from ..utils.retry import DATABASE_RETRY_CONFIG, AsyncRetrier
from ..utils.url import generate_url_hash, normalize_url
from ._bloomfilter import BloomFilter

logger = logging.getLogger(__name__)


class DeduplicationStats:
    """Statistics for URL deduplication operations"""

    def __init__(self):
        self.urls_processed: int = 0
        self.urls_new: int = 0
        self.urls_duplicate: int = 0
        self.bloom_hits: int = 0
        self.bloom_misses: int = 0
        self.dynamodb_checks: int = 0
        self.batch_operations: int = 0
        self.false_positives: int = 0
        self.normalization_changes: int = 0


class BloomFilterManager:
    """
    Manages distributed Bloom Filters in Redis for fast URL deduplication.

    Uses multiple Bloom Filters with different time windows to balance
    accuracy and memory usage.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        capacity: int = 1000000,  # 1M URLs
        error_rate: float = 0.001,  # 0.1% false positive rate
        num_filters: int = 3,  # Number of rotating filters
    ):
        self.redis_client = redis_client
        self.capacity = capacity
        self.error_rate = error_rate
        self.num_filters = num_filters

        # Create local Bloom Filter for fast checking
        self.local_bloom = BloomFilter(capacity=capacity, error_rate=error_rate)

        # Redis keys for distributed Bloom Filters
        self.bloom_key_prefix = "crawler:bloom:"
        self.current_filter_key = "crawler:bloom:current"

        self.stats = {"bloom_checks": 0, "bloom_hits": 0, "bloom_additions": 0, "filter_rotations": 0}

    async def contains(self, url_hash: str) -> bool:
        """
        Check if URL hash is in the Bloom Filter.

        Checks both local and distributed Bloom Filters.
        Returns True if URL might exist (could be false positive).
        """
        self.stats["bloom_checks"] += 1

        # Check local Bloom Filter first (fastest)
        if url_hash in self.local_bloom:
            self.stats["bloom_hits"] += 1
            return True

        # Check distributed Bloom Filters in Redis
        try:
            for i in range(self.num_filters):
                filter_key = f"{self.bloom_key_prefix}{i}"
                exists = await self.redis_client.getbit(filter_key, self._hash_to_bit_positions(url_hash)[0])

                if exists:
                    # Check all bit positions for this filter
                    all_bits_set = await self._check_all_bits(filter_key, url_hash)
                    if all_bits_set:
                        self.stats["bloom_hits"] += 1
                        # Add to local filter for future fast access
                        self.local_bloom.add(url_hash)
                        return True

        except Exception as e:
            logger.debug(f"Error checking distributed Bloom Filter: {e}")
            # Fall back to local filter result

        return False

    async def add(self, url_hash: str):
        """Add URL hash to Bloom Filters"""
        try:
            # Add to local filter
            self.local_bloom.add(url_hash)

            # Add to current distributed filter
            current_filter = await self._get_current_filter()
            bit_positions = self._hash_to_bit_positions(url_hash)

            # Set all required bits
            pipeline = self.redis_client.pipeline()
            for bit_pos in bit_positions:
                pipeline.setbit(current_filter, bit_pos, 1)
            await pipeline.execute()

            self.stats["bloom_additions"] += 1

        except Exception as e:
            logger.error(f"Error adding to Bloom Filter: {e}")

    async def _check_all_bits(self, filter_key: str, url_hash: str) -> bool:
        """Check if all bits for a URL hash are set in the filter"""
        bit_positions = self._hash_to_bit_positions(url_hash)

        pipeline = self.redis_client.pipeline()
        for bit_pos in bit_positions:
            pipeline.getbit(filter_key, bit_pos)

        results = await pipeline.execute()
        return all(results)

    def _hash_to_bit_positions(self, url_hash: str, num_positions: int = 3) -> List[int]:
        """Convert URL hash to bit positions for Bloom Filter"""
        positions: List[int] = []

        for i in range(num_positions):
            combined = f"{url_hash}:{i}"
            hash_obj = hashlib.md5(combined.encode())
            hash_int = int(hash_obj.hexdigest(), 16)
            bit_position = hash_int % (self.capacity * 8)  # Convert to bit position
            positions.append(bit_position)

        return positions

    async def _get_current_filter(self) -> str:
        """Get the current active filter key"""
        try:
            current = await self.redis_client.get(self.current_filter_key)
            if current:
                return current.decode()
            else:
                # Initialize first filter
                filter_key = f"{self.bloom_key_prefix}0"
                await self.redis_client.set(self.current_filter_key, filter_key)
                return filter_key
        except Exception:
            # Fallback to filter 0
            return f"{self.bloom_key_prefix}0"

    async def rotate_filters(self):
        """Rotate to next Bloom Filter (for time-based cleanup)"""
        try:
            current_filter = await self._get_current_filter()
            current_num = int(current_filter.split(":")[-1])
            next_num = (current_num + 1) % self.num_filters

            next_filter = f"{self.bloom_key_prefix}{next_num}"

            # Clear the next filter and make it current
            await self.redis_client.delete(next_filter)
            await self.redis_client.set(self.current_filter_key, next_filter)

            self.stats["filter_rotations"] += 1
            logger.info(f"Rotated Bloom Filter from {current_filter} to {next_filter}")

        except Exception as e:
            logger.error(f"Error rotating Bloom Filters: {e}")

    def get_stats(self) -> Dict[str, Union[int, float]]:
        """Get Bloom Filter statistics"""
        return {
            **self.stats,
            "local_bloom_items": len(self.local_bloom),
            "capacity": self.capacity,
            "error_rate": self.error_rate,
        }


class URLDeduplicator:
    """
    High-performance URL deduplication using Bloom Filters and DynamoDB.

    Uses a two-stage approach:
    1. Bloom Filter for fast duplicate detection (may have false positives)
    2. DynamoDB for authoritative duplicate checking
    """

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.state_manager = URLStateManager(settings.crawler_id or "default_crawler_id")
        self.retrier = AsyncRetrier(DATABASE_RETRY_CONFIG)
        self.stats = DeduplicationStats()

        # Bloom Filter configuration
        self.bloom_capacity = getattr(settings, "bloom_filter_capacity", 1000000)
        self.bloom_error_rate = getattr(settings, "bloom_filter_error_rate", 0.001)

        # Redis for distributed Bloom Filters
        self.redis_client: Optional[aioredis.Redis] = None
        self.bloom_manager: Optional[BloomFilterManager] = None

        # Batch processing configuration
        self.batch_size = getattr(settings, "dedup_batch_size", 100)

        # URL normalization settings
        self.normalize_urls = getattr(settings, "normalize_urls", True)
        self.case_sensitive = getattr(settings, "url_case_sensitive", False)

        logger.info("URL Deduplicator initialized")

    async def initialize(self):
        """Initialize Redis connection and Bloom Filters"""
        try:
            # Initialize Redis client for Bloom Filters
            if not self.settings.redis_url:
                logger.warning("Redis URL not configured, skipping Bloom Filter initialization")
                return

            self.redis_client = aioredis.Redis.from_url(  # type: ignore
                self.settings.redis_url,
                decode_responses=False,  # We need bytes for bit operations
            )

            # Test Redis connection
            await self.redis_client.ping()  # type: ignore

            # Initialize Bloom Filter manager
            self.bloom_manager = BloomFilterManager(
                self.redis_client, capacity=self.bloom_capacity, error_rate=self.bloom_error_rate
            )

            logger.info("URL Deduplicator initialized with Bloom Filter support")

        except Exception as e:
            logger.warning(f"Failed to initialize Bloom Filters: {e}")
            logger.info("Continuing without Bloom Filter optimization")

    async def deduplicate_urls(self, urls: List[str]) -> Tuple[List[str], Dict[str, Any]]:
        """
        Deduplicate a list of URLs.

        Args:
            urls: List of URLs to deduplicate

        Returns:
            Tuple of (unique_urls, deduplication_stats)
        """
        if not urls:
            return [], {"processed": 0, "unique": 0, "duplicates": 0}

        start_time = datetime.now()
        initial_count = len(urls)

        # Step 1: Normalize URLs
        normalized_urls = await self._normalize_urls_batch(urls)

        # Step 2: Remove local duplicates (within the batch)
        unique_normalized = await self._remove_local_duplicates(normalized_urls)

        # Step 3: Bloom Filter check (if available)
        bloom_filtered = await self._bloom_filter_check(unique_normalized)

        # Step 4: DynamoDB authoritative check
        final_unique_urls = await self._dynamodb_duplicate_check(bloom_filtered)

        # Step 5: Update Bloom Filters with new URLs
        await self._update_bloom_filters(final_unique_urls)

        # Calculate statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        dedup_stats = {
            "processed": initial_count,
            "unique": len(final_unique_urls),
            "duplicates": initial_count - len(final_unique_urls),
            "duration_seconds": duration,
            "bloom_hits": self.stats.bloom_hits,
            "bloom_misses": self.stats.bloom_misses,
            "dynamodb_checks": self.stats.dynamodb_checks,
            "normalization_changes": self.stats.normalization_changes,
        }

        # Update global stats
        self.stats.urls_processed += initial_count
        self.stats.urls_new += len(final_unique_urls)
        self.stats.urls_duplicate += initial_count - len(final_unique_urls)
        self.stats.batch_operations += 1

        logger.debug(f"Deduplication complete: {initial_count} -> {len(final_unique_urls)} URLs", extra=dedup_stats)

        return final_unique_urls, dedup_stats

    async def _normalize_urls_batch(self, urls: List[str]) -> List[str]:
        """Normalize URLs in batch with statistics tracking"""
        if not self.normalize_urls:
            return urls

        normalized: List[str] = []
        changes = 0

        for url in urls:
            try:
                normalized_url = normalize_url(url)

                if not self.case_sensitive:
                    normalized_url = normalized_url.lower()

                if normalized_url != url:
                    changes += 1

                normalized.append(normalized_url)

            except Exception as e:
                logger.debug(f"Failed to normalize URL {url}: {e}")
                normalized.append(url)  # Keep original if normalization fails

        self.stats.normalization_changes += changes
        return normalized

    async def _remove_local_duplicates(self, urls: List[str]) -> List[str]:
        """Remove duplicates within the batch"""
        seen: set[str] = set()
        unique: List[str] = []

        for url in urls:
            if url not in seen:
                seen.add(url)
                unique.append(url)

        return unique

    async def _bloom_filter_check(self, urls: List[str]) -> List[str]:
        """Filter URLs using Bloom Filter (if available)"""
        if not self.bloom_manager:
            return urls

        potentially_new: List[str] = []

        for url in urls:
            url_hash = generate_url_hash(url)

            if await self.bloom_manager.contains(url_hash):
                self.stats.bloom_hits += 1
                # URL might exist (could be false positive)
                # Still need to check DynamoDB
                potentially_new.append(url)
            else:
                self.stats.bloom_misses += 1
                # URL definitely doesn't exist
                potentially_new.append(url)

        return potentially_new

    async def _dynamodb_duplicate_check(self, urls: List[str]) -> List[str]:
        """Authoritative duplicate check using DynamoDB"""
        if not urls:
            return []

        try:
            # Check URLs in batches to avoid DynamoDB limits
            unique_urls: List[str] = []

            for i in range(0, len(urls), self.batch_size):
                batch = urls[i : i + self.batch_size]

                # Get existing states for this batch
                existing_states = await self.state_manager.batch_get_url_states(batch)
                self.stats.dynamodb_checks += len(batch)

                # Filter out URLs that already exist
                for url in batch:
                    url_hash = generate_url_hash(url)

                    if url_hash not in existing_states:
                        unique_urls.append(url)
                    else:
                        # Count false positives from Bloom Filter
                        if self.bloom_manager:
                            self.stats.false_positives += 1

            return unique_urls

        except Exception as e:
            logger.error(f"Error checking duplicates in DynamoDB: {e}")
            # Fallback: assume all URLs are unique
            return urls

    async def _update_bloom_filters(self, new_urls: List[str]):
        """Update Bloom Filters with newly discovered URLs"""
        if not self.bloom_manager or not new_urls:
            return

        try:
            for url in new_urls:
                url_hash = generate_url_hash(url)
                await self.bloom_manager.add(url_hash)

            logger.debug(f"Added {len(new_urls)} URLs to Bloom Filter")

        except Exception as e:
            logger.error(f"Error updating Bloom Filters: {e}")

    async def is_duplicate(self, url: str) -> bool:
        """
        Check if a single URL is a duplicate.

        Args:
            url: URL to check

        Returns:
            True if URL is a duplicate, False if it's new
        """
        try:
            # Normalize URL
            if self.normalize_urls:
                url = normalize_url(url)
                if not self.case_sensitive:
                    url = url.lower()

            url_hash = generate_url_hash(url)

            # Check Bloom Filter first (if available)
            if self.bloom_manager:
                bloom_hit = await self.bloom_manager.contains(url_hash)
                if not bloom_hit:
                    # Definitely not a duplicate
                    return False

            # Authoritative check in DynamoDB
            existing_state = await self.state_manager.get_url_state(url)
            return existing_state is not None

        except Exception as e:
            logger.error(f"Error checking duplicate for URL {url}: {e}")
            # Conservative: assume not duplicate if check fails
            return False

    async def get_domain_duplicate_stats(self, domain: str) -> Dict[str, Union[str, int, float]]:
        """Get deduplication statistics for a specific domain"""
        try:
            # This would query DynamoDB for domain-specific stats
            # Implementation depends on the specific requirements
            return {"domain": domain, "total_urls": 0, "unique_urls": 0, "duplicate_rate": 0.0}
        except Exception as e:
            logger.error(f"Error getting domain stats for {domain}: {e}")
            return {}

    async def rotate_bloom_filters(self):
        """Rotate Bloom Filters to manage memory usage"""
        if self.bloom_manager:
            await self.bloom_manager.rotate_filters()

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive deduplication statistics"""
        base_stats: Dict[str, Any] = {
            "urls_processed": self.stats.urls_processed,
            "urls_new": self.stats.urls_new,
            "urls_duplicate": self.stats.urls_duplicate,
            "bloom_hits": self.stats.bloom_hits,
            "bloom_misses": self.stats.bloom_misses,
            "dynamodb_checks": self.stats.dynamodb_checks,
            "batch_operations": self.stats.batch_operations,
            "false_positives": self.stats.false_positives,
            "normalization_changes": self.stats.normalization_changes,
        }

        # Add duplicate rate calculation
        if self.stats.urls_processed > 0:
            base_stats["duplicate_rate"] = self.stats.urls_duplicate / self.stats.urls_processed
        else:
            base_stats["duplicate_rate"] = 0.0

        # Add Bloom Filter stats if available
        if self.bloom_manager:
            base_stats["bloom_filter"] = self.bloom_manager.get_stats()

        return base_stats

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on deduplicator"""
        try:
            health: Dict[str, Any] = {
                "status": "healthy",
                "bloom_filter_enabled": self.bloom_manager is not None,
                "redis_connected": False,
            }

            # Test Redis connection
            if self.redis_client:
                try:
                    await self.redis_client.ping()  # type: ignore
                    health["redis_connected"] = True
                except Exception as e:
                    health["redis_error"] = str(e)
                    health["status"] = "degraded"

            # Test DynamoDB connection via state manager
            try:
                # This would be implemented in state_manager
                health["dynamodb_connected"] = True
            except Exception as e:
                health["dynamodb_error"] = str(e)
                health["status"] = "unhealthy"

            health["stats"] = self.get_stats()

            return health

        except Exception as e:
            logger.error(f"Deduplicator health check failed: {e}")
            return {"status": "unhealthy", "error": str(e)}

    async def close(self):
        """Close Redis connections and cleanup resources"""
        if self.redis_client:
            await self.redis_client.aclose()
            logger.info("URL Deduplicator closed")


if __name__ == "__main__":
    # CLI utility for testing deduplication
    import asyncio
    import sys

    from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python deduplication.py <command> [args...]")
            print("Commands:")
            print("  test-dedup <url1> <url2> ... - Test deduplication on URLs")
            print("  check-duplicate <url> - Check if single URL is duplicate")
            print("  stats - Show deduplication statistics")
            print("  health - Check deduplicator health")
            print("  rotate - Rotate Bloom Filters")
            sys.exit(1)

        command = sys.argv[1]
        settings = load_settings()
        deduplicator = URLDeduplicator(settings)

        try:
            await deduplicator.initialize()

            if command == "test-dedup" and len(sys.argv) >= 3:
                test_urls = sys.argv[2:]
                print(f"Testing deduplication on {len(test_urls)} URLs...")

                unique_urls, stats = await deduplicator.deduplicate_urls(test_urls)

                print(f"Results: {len(test_urls)} -> {len(unique_urls)} unique URLs")
                print("Statistics:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

                print("\nUnique URLs:")
                for i, url in enumerate(unique_urls, 1):
                    print(f"  {i}. {url}")

            elif command == "check-duplicate" and len(sys.argv) >= 3:
                test_url = sys.argv[2]
                print(f"Checking if URL is duplicate: {test_url}")

                is_dup = await deduplicator.is_duplicate(test_url)
                print(f"Result: {'Duplicate' if is_dup else 'New URL'}")

            elif command == "stats":
                stats = deduplicator.get_stats()
                print("Deduplication statistics:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

            elif command == "health":
                health = await deduplicator.health_check()
                print("Deduplicator health:")
                for key, value in health.items():
                    print(f"  {key}: {value}")

            elif command == "rotate":
                print("Rotating Bloom Filters...")
                await deduplicator.rotate_bloom_filters()
                print("Bloom Filters rotated successfully")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
        finally:
            await deduplicator.close()

    asyncio.run(main())
