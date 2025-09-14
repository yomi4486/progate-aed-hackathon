"""
Distributed rate limiter implementation using Redis sliding window algorithm.

Provides domain-based rate limiting that works across multiple crawler instances
using Redis for state coordination.
"""

import logging
import time
from typing import Any, Dict, Optional

from ..config.settings import CrawlerSettings
from .redis_client import RedisClient, get_redis_client

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded"""

    def __init__(self, domain: str, retry_after: float):
        self.domain = domain
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded for domain {domain}, retry after {retry_after:.2f}s")


class SlidingWindowRateLimiter:
    """
    Distributed sliding window rate limiter using Redis.

    Uses sliding window counter algorithm to provide accurate rate limiting
    across multiple crawler instances. Each domain has its own rate limit
    that can be configured independently.
    """

    def __init__(self, redis_client: Optional[RedisClient] = None, settings: Optional[CrawlerSettings] = None):
        self.redis_client = redis_client or get_redis_client(settings)
        self.settings = settings

        # Sliding window parameters
        self.window_size_seconds = 60  # 1 minute sliding window
        self.bucket_size_seconds = 1  # 1 second buckets for precision
        self.buckets_per_window = self.window_size_seconds // self.bucket_size_seconds

        # Key prefixes for Redis
        self.request_count_prefix = "rate_limit:requests:"
        self.last_request_prefix = "rate_limit:last_request:"
        self.domain_config_prefix = "rate_limit:config:"

        logger.info("Sliding window rate limiter initialized")

    def _get_current_bucket_key(self, domain: str, timestamp: Optional[float] = None) -> str:
        """
        Get the Redis key for the current time bucket.

        Args:
            domain: Domain name
            timestamp: Unix timestamp (defaults to current time)

        Returns:
            Redis key for the current bucket
        """
        if timestamp is None:
            timestamp = time.time()

        bucket_id = int(timestamp) // self.bucket_size_seconds
        return f"{self.request_count_prefix}{domain}:{bucket_id}"

    def _get_window_bucket_keys(self, domain: str, timestamp: Optional[float] = None) -> list[str]:
        """
        Get all Redis keys for the current sliding window.

        Args:
            domain: Domain name
            timestamp: Unix timestamp (defaults to current time)

        Returns:
            List of Redis keys covering the sliding window
        """
        if timestamp is None:
            timestamp = time.time()

        current_bucket_id = int(timestamp) // self.bucket_size_seconds
        keys: list[str] = []

        for i in range(self.buckets_per_window):
            bucket_id = current_bucket_id - i
            keys.append(f"{self.request_count_prefix}{domain}:{bucket_id}")

        return keys

    async def _get_domain_qps_limit(self, domain: str) -> int:
        """
        Get QPS limit for a domain (from settings or Redis cache).

        Args:
            domain: Domain name

        Returns:
            QPS limit for the domain
        """
        # Check Redis cache first
        cache_key = f"{self.domain_config_prefix}{domain}"
        cached_limit = await self.redis_client.get(cache_key)

        if cached_limit is not None:
            try:
                return int(cached_limit)
            except ValueError:
                pass

        # Fall back to settings
        if self.settings:
            # Check domain-specific overrides first
            if domain in self.settings.domain_qps_overrides:
                limit = self.settings.domain_qps_overrides[domain]
            else:
                limit = self.settings.default_qps_per_domain

            # Cache the limit in Redis for 5 minutes
            await self.redis_client.set(cache_key, str(limit), expire=300)
            return limit

        # Default fallback
        return 1

    async def check_domain_limit(self, domain: str, qps_limit: Optional[int] = None) -> bool:
        """
        Check if a request to the domain would exceed the rate limit.

        Args:
            domain: Domain name
            qps_limit: Optional QPS limit override

        Returns:
            True if request is allowed, False if rate limit exceeded
        """
        try:
            if qps_limit is None:
                qps_limit = await self._get_domain_qps_limit(domain)

            current_time = time.time()
            window_keys = self._get_window_bucket_keys(domain, current_time)

            # Get request counts for all buckets in the sliding window
            bucket_counts = await self.redis_client.mget(window_keys)

            # Calculate total requests in the window
            total_requests = 0
            for count_str in bucket_counts or []:
                if count_str is not None:
                    try:
                        total_requests += int(count_str)
                    except ValueError:
                        continue

            # Check if adding one more request would exceed the limit
            requests_per_minute = qps_limit * self.window_size_seconds
            allowed = total_requests < requests_per_minute

            if not allowed:
                logger.debug(f"Rate limit exceeded for {domain}: {total_requests} >= {requests_per_minute}")

            return allowed

        except Exception as e:
            logger.error(f"Error checking rate limit for {domain}: {e}")
            # Fail open - allow request if Redis is down
            return True

    async def record_request(self, domain: str) -> None:
        """
        Record a request for the domain in the current time bucket.

        Args:
            domain: Domain name
        """
        try:
            current_time = time.time()
            bucket_key = self._get_current_bucket_key(domain, current_time)
            last_request_key = f"{self.last_request_prefix}{domain}"

            # Use pipeline for atomic operations
            pipeline = await self.redis_client.pipeline()
            if pipeline is not None:
                # Increment counter for current bucket
                await pipeline.incr(bucket_key)
                # Set expiration for bucket (window size + buffer)
                await pipeline.expire(bucket_key, self.window_size_seconds + 60)
                # Update last request timestamp
                await pipeline.set(last_request_key, str(current_time), ex=3600)
                # Execute pipeline
                await pipeline.execute()
            else:
                # Fallback without pipeline if Redis is degraded
                await self.redis_client.incr(bucket_key)
                await self.redis_client.expire(bucket_key, self.window_size_seconds + 60)
                await self.redis_client.set(last_request_key, str(current_time), expire=3600)

            logger.debug(f"Recorded request for domain {domain}")

        except Exception as e:
            logger.error(f"Error recording request for {domain}: {e}")
            # Continue execution - recording failure shouldn't block crawling

    async def get_next_allowed_time(self, domain: str) -> float:
        """
        Calculate when the next request to this domain would be allowed.

        Args:
            domain: Domain name

        Returns:
            Unix timestamp when next request is allowed
        """
        try:
            qps_limit = await self._get_domain_qps_limit(domain)
            current_time = time.time()

            # If rate limit check passes, request is allowed now
            if await self.check_domain_limit(domain, qps_limit):
                return current_time

            # Find the oldest bucket that still has requests
            window_keys = self._get_window_bucket_keys(domain, current_time)
            bucket_counts = await self.redis_client.mget(window_keys)

            # Calculate when the oldest request will expire
            requests_per_minute = qps_limit * self.window_size_seconds
            total_requests = sum(int(count or 0) for count in (bucket_counts or []))

            if total_requests >= requests_per_minute:
                # Calculate minimum wait time
                # This is a simplified approach - wait for oldest bucket to expire
                _ = (len(window_keys) - 1) * self.bucket_size_seconds
                next_allowed = current_time - (current_time % self.bucket_size_seconds) + self.bucket_size_seconds
                return next_allowed
            else:
                return current_time

        except Exception as e:
            logger.error(f"Error calculating next allowed time for {domain}: {e}")
            # Fail safe - allow request now
            return time.time()

    async def get_domain_stats(self, domain: str) -> Dict[str, Any]:
        """
        Get statistics for a domain's rate limiting.

        Args:
            domain: Domain name

        Returns:
            Dictionary with rate limiting statistics
        """
        try:
            current_time = time.time()
            qps_limit = await self._get_domain_qps_limit(domain)
            window_keys = self._get_window_bucket_keys(domain, current_time)
            bucket_counts = await self.redis_client.mget(window_keys)

            total_requests = sum(int(count or 0) for count in (bucket_counts or []))
            requests_per_minute_limit = qps_limit * self.window_size_seconds

            # Get last request timestamp
            last_request_key = f"{self.last_request_prefix}{domain}"
            last_request_str = await self.redis_client.get(last_request_key)
            last_request_time = None
            if last_request_str:
                try:
                    last_request_time = float(last_request_str)
                except ValueError:
                    pass

            return {
                "domain": domain,
                "qps_limit": qps_limit,
                "requests_per_minute_limit": requests_per_minute_limit,
                "current_requests_in_window": total_requests,
                "utilization_percent": (total_requests / requests_per_minute_limit) * 100
                if requests_per_minute_limit > 0
                else 0,
                "is_rate_limited": total_requests >= requests_per_minute_limit,
                "last_request_time": last_request_time,
                "window_size_seconds": self.window_size_seconds,
            }

        except Exception as e:
            logger.error(f"Error getting stats for {domain}: {e}")
            return {
                "domain": domain,
                "error": str(e),
            }

    async def reset_domain_limit(self, domain: str) -> bool:
        """
        Reset rate limiting data for a domain (for testing/admin purposes).

        Args:
            domain: Domain name

        Returns:
            True if reset was successful
        """
        try:
            current_time = time.time()
            window_keys = self._get_window_bucket_keys(domain, current_time)

            # Also include keys for future buckets to be thorough
            future_keys: list[str] = []
            current_bucket_id = int(current_time) // self.bucket_size_seconds
            for i in range(self.buckets_per_window):
                bucket_id = current_bucket_id + i + 1
                future_keys.append(f"{self.request_count_prefix}{domain}:{bucket_id}")

            all_keys = window_keys + future_keys
            all_keys.append(f"{self.last_request_prefix}{domain}")
            all_keys.append(f"{self.domain_config_prefix}{domain}")

            deleted = await self.redis_client.delete(*all_keys)
            logger.info(f"Reset rate limit data for {domain}, deleted {deleted} keys")
            return True

        except Exception as e:
            logger.error(f"Error resetting rate limit for {domain}: {e}")
            return False

    async def set_domain_qps_limit(self, domain: str, qps_limit: int, expire_seconds: int = 3600) -> bool:
        """
        Set a custom QPS limit for a domain.

        Args:
            domain: Domain name
            qps_limit: New QPS limit
            expire_seconds: How long to cache this setting

        Returns:
            True if setting was successful
        """
        try:
            cache_key = f"{self.domain_config_prefix}{domain}"
            result = await self.redis_client.set(cache_key, str(qps_limit), expire=expire_seconds)

            if result:
                logger.info(f"Set QPS limit for {domain} to {qps_limit}")
            return bool(result)

        except Exception as e:
            logger.error(f"Error setting QPS limit for {domain}: {e}")
            return False


class RateLimitManager:
    """
    High-level rate limiting manager that combines sliding window limiting
    with domain-specific configuration and fallback strategies.
    """

    def __init__(self, redis_client: Optional[RedisClient] = None, settings: Optional[CrawlerSettings] = None):
        self.rate_limiter = SlidingWindowRateLimiter(redis_client, settings)
        self.settings = settings or CrawlerSettings(
            environment="default",
            aws_region="us-east-1",
            localstack_endpoint=None,
            dynamodb_table="crawler_table",
            sqs_crawl_queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/crawl-queue",
            sqs_discovery_queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/discovery-queue",
            sqs_indexing_queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/indexing-queue",
            s3_raw_bucket="crawler-raw-bucket",
            s3_parsed_bucket="crawler-parsed-bucket",
            redis_url="redis://localhost:6379",
            redis_db=0,
            crawler_id="default_crawler",
            max_concurrent_requests=10,
            request_timeout=30,
            user_agent="MyCrawler/1.0",
            default_qps_per_domain=5,
            max_retries=3,
            base_backoff_seconds=1,
            max_backoff_seconds=32,
            acquisition_ttl_seconds=60,
            heartbeat_interval_seconds=30,
            max_content_length=1048576,
            default_language="en",
            language_detection_confidence=0.9,
            log_level="INFO",
            rate_limiter_enabled=True,
            json_logs=False,
            health_check_port=8080,
            health_check_enabled=True,
            metrics_enabled=True,
            metrics_interval_seconds=60,
        )

        # Fallback rate limiting when Redis is unavailable
        self._local_last_request: Dict[str, float] = {}
        self._fallback_active = False

    async def check_and_wait_if_needed(self, domain: str) -> float:
        """
        Check rate limit and wait if necessary before allowing request.

        Args:
            domain: Domain name

        Returns:
            Actual wait time (0 if no wait was needed)

        Raises:
            RateLimitExceeded: If rate limit is exceeded and should retry later
        """
        try:
            # Check if request is allowed
            if await self.rate_limiter.check_domain_limit(domain):
                # Record the request
                await self.rate_limiter.record_request(domain)
                return 0.0

            # Calculate wait time
            next_allowed_time = await self.rate_limiter.get_next_allowed_time(domain)
            current_time = time.time()
            wait_time = max(0, next_allowed_time - current_time)

            if wait_time > 0:
                raise RateLimitExceeded(domain, wait_time)

            # If no wait time, record request and proceed
            await self.rate_limiter.record_request(domain)
            return 0.0

        except RateLimitExceeded:
            raise
        except Exception as e:
            logger.error(f"Error in rate limiting for {domain}: {e}")
            # Fallback to local rate limiting
            return await self._fallback_rate_limit(domain)

    async def _fallback_rate_limit(self, domain: str) -> float:
        """
        Fallback rate limiting when Redis is unavailable.
        Uses simple local timestamp tracking.
        """
        if not self._fallback_active:
            logger.warning("Redis unavailable, falling back to local rate limiting")
            self._fallback_active = True

        current_time = time.time()
        last_request_time = self._local_last_request.get(domain, 0)

        # Simple 1-second minimum delay between requests as fallback
        time_since_last = current_time - last_request_time
        if time_since_last < 1.0:
            wait_time = 1.0 - time_since_last
            raise RateLimitExceeded(domain, wait_time)

        self._local_last_request[domain] = current_time
        return 0.0

    async def get_domain_stats(self, domain: str) -> Dict[str, Any]:
        """Get rate limiting statistics for a domain"""
        return await self.rate_limiter.get_domain_stats(domain)

    async def reset_domain_limit(self, domain: str) -> bool:
        """Reset rate limiting data for a domain"""
        # Also reset local fallback data
        self._local_last_request.pop(domain, None)
        return await self.rate_limiter.reset_domain_limit(domain)


if __name__ == "__main__":
    # CLI utility for testing rate limiter
    import asyncio
    import sys

    from ..config.settings import load_settings
    from .redis_client import initialize_redis_client

    async def main():
        if len(sys.argv) < 3:
            print("Usage: python limiter.py [check|record|stats|reset] <domain> [qps_limit]")
            sys.exit(1)

        command = sys.argv[1]
        domain = sys.argv[2]
        qps_limit = int(sys.argv[3]) if len(sys.argv) > 3 else None

        settings = load_settings()
        redis_client = await initialize_redis_client(settings)

        try:
            limiter = SlidingWindowRateLimiter(redis_client, settings)

            if command == "check":
                allowed = await limiter.check_domain_limit(domain, qps_limit)
                print(f"Domain {domain} request allowed: {allowed}")

            elif command == "record":
                await limiter.record_request(domain)
                print(f"Recorded request for domain {domain}")

            elif command == "stats":
                stats = await limiter.get_domain_stats(domain)
                print(f"Stats for {domain}: {stats}")

            elif command == "reset":
                reset_ok = await limiter.reset_domain_limit(domain)
                print(f"Reset {domain}: {reset_ok}")

            elif command == "test":
                print(f"Testing rate limiter for {domain}...")

                # Test multiple requests
                for i in range(5):
                    allowed = await limiter.check_domain_limit(domain, qps_limit or 2)
                    print(f"Request {i + 1}: {'ALLOWED' if allowed else 'BLOCKED'}")

                    if allowed:
                        await limiter.record_request(domain)

                    await asyncio.sleep(0.1)

                # Show final stats
                stats = await limiter.get_domain_stats(domain)
                print(f"Final stats: {stats}")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        finally:
            await redis_client.close()

    asyncio.run(main())
