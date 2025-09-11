"""
Redis client initialization and management for distributed rate limiting.

Provides centralized Redis configuration, connection pooling, error handling,
and failover support for all rate limiting operations.
"""

import logging
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import AuthenticationError, ConnectionError, RedisClusterException, RedisError, TimeoutError

from ..config.settings import CrawlerSettings
from ..utils.retry import DATABASE_RETRY_CONFIG, AsyncRetrier, RetryError

logger = logging.getLogger(__name__)


class RedisConnectionError(RedisError):
    """Raised when Redis connection fails"""

    pass


class RedisTimeoutError(RedisError):
    """Raised when Redis operation times out"""

    pass


class RedisClient:
    """
    Enhanced Redis client with connection pooling, error handling,
    and retry logic specifically designed for distributed rate limiting.
    """

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.retrier = AsyncRetrier(DATABASE_RETRY_CONFIG)

        # Connection pools for different Redis instances
        self._connection_pool: Optional[aioredis.ConnectionPool] = None
        self._failover_pools: List[aioredis.ConnectionPool] = []
        self._redis_client: Optional[aioredis.Redis] = None

        # Circuit breaker state
        self._circuit_breaker_open = False
        self._circuit_breaker_last_failure = 0
        self._circuit_breaker_timeout = 30  # seconds

        logger.info(f"Redis client initialized for URL: {settings.redis_url}")

    async def initialize(self) -> None:
        """Initialize Redis connection pools"""
        try:
            # Parse Redis URL to extract connection parameters
            assert self.settings.redis_url is not None, "Redis URL is required"
            redis_params = self._parse_redis_url(self.settings.redis_url)

            # Create main connection pool
            self._connection_pool = aioredis.ConnectionPool(
                host=redis_params["host"],
                port=redis_params["port"],
                db=redis_params["db"],
                password=redis_params.get("password"),
                username=redis_params.get("username"),
                max_connections=20,  # Pool size
                retry_on_timeout=True,
                retry_on_error=[ConnectionError, TimeoutError],
                retry=Retry(ExponentialBackoff(), 3),
                socket_connect_timeout=5,
                socket_timeout=5,
                health_check_interval=30,  # Health check every 30 seconds
            )

            # Create Redis client
            self._redis_client = aioredis.Redis(
                connection_pool=self._connection_pool,
                decode_responses=True,  # Automatically decode responses to strings
            )

            # Test connection
            await self._redis_client.ping()  # type: ignore

            logger.info("Redis connection established successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {e}")
            raise RedisConnectionError(f"Failed to connect to Redis: {e}") from e

    def _parse_redis_url(self, redis_url: str) -> Dict[str, Any]:
        """Parse Redis URL into connection parameters"""
        try:
            from urllib.parse import urlparse

            parsed = urlparse(redis_url)

            return {
                "host": parsed.hostname or "localhost",
                "port": parsed.port or 6379,
                "db": int(parsed.path.lstrip("/")) if parsed.path and parsed.path != "/" else 0,
                "password": parsed.password,
                "username": parsed.username,
            }

        except Exception as e:
            logger.error(f"Failed to parse Redis URL {redis_url}: {e}")
            # Return defaults
            return {
                "host": "localhost",
                "port": 6379,
                "db": 0,
            }

    async def close(self) -> None:
        """Close all Redis connections"""
        if self._redis_client:
            await self._redis_client.aclose()
            logger.info("Redis connections closed")

    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker is open"""
        if not self._circuit_breaker_open:
            return False

        # Check if timeout has passed
        if time.time() - self._circuit_breaker_last_failure > self._circuit_breaker_timeout:
            self._circuit_breaker_open = False
            logger.info("Circuit breaker closed - attempting Redis operations")
            return False

        return True

    def _trip_circuit_breaker(self) -> None:
        """Trip the circuit breaker"""
        self._circuit_breaker_open = True
        self._circuit_breaker_last_failure = time.time()
        logger.warning("Circuit breaker opened - Redis operations will be bypassed temporarily")

    async def handle_error(self, error: Exception, operation: str) -> None:
        """
        Centralized error handling for Redis operations.

        Args:
            error: The exception that occurred
            operation: Description of the operation that failed

        Raises:
            Appropriate RedisError subclass based on the original error
        """
        if isinstance(error, (ConnectionError, RedisClusterException)):
            self._trip_circuit_breaker()
            raise RedisConnectionError(f"Redis connection error during {operation}: {str(error)}", error)
        elif isinstance(error, TimeoutError):
            raise RedisTimeoutError(f"Redis timeout during {operation}: {str(error)}", error)
        elif isinstance(error, AuthenticationError):
            raise RedisError(f"Redis authentication error during {operation}: {str(error)}", error)
        else:
            raise RedisError(f"Redis error during {operation}: {str(error)}", error)

    async def execute_with_retry(
        self,
        operation_func: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a Redis operation with retry logic and circuit breaker.

        Args:
            operation_func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the operation or None if circuit breaker is open

        Raises:
            RedisError: If operation fails after all retries
        """
        # Check circuit breaker
        if self._check_circuit_breaker():
            logger.debug("Circuit breaker is open - skipping Redis operation")
            return None

        operation_name = getattr(operation_func, "__name__", "unknown_operation")

        try:
            exceptions_tuple = (ConnectionError, TimeoutError, RedisError, RedisClusterException)
            return await self.retrier.call(
                operation_func,
                *args,
                exceptions=exceptions_tuple,
                **kwargs,
            )
        except RetryError as e:
            await self.handle_error(e.last_exception, operation_name)
        except Exception as e:
            await self.handle_error(e, operation_name)

    async def set(
        self,
        key: str,
        value: Union[str, int, float],
        expire: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Optional[bool]:
        """
        Set a key-value pair in Redis.

        Args:
            key: Redis key
            value: Value to store
            expire: Expiration time in seconds
            nx: Only set if key doesn't exist
            xx: Only set if key exists

        Returns:
            True if successful, None if circuit breaker is open
        """

        async def _set():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.set(key, value, ex=expire, nx=nx, xx=xx)

        return await self.execute_with_retry(_set)

    async def get(self, key: str) -> Optional[str]:
        """
        Get a value from Redis.

        Args:
            key: Redis key

        Returns:
            Value or None if not found or circuit breaker is open
        """

        async def _get():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.get(key)

        return await self.execute_with_retry(_get)

    async def incr(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment a counter in Redis.

        Args:
            key: Redis key
            amount: Amount to increment by

        Returns:
            New value or None if circuit breaker is open
        """

        async def _incr():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.incr(key, amount)

        return await self.execute_with_retry(_incr)

    async def expire(self, key: str, seconds: int) -> Optional[bool]:
        """
        Set expiration time for a key.

        Args:
            key: Redis key
            seconds: Expiration time in seconds

        Returns:
            True if successful, None if circuit breaker is open
        """

        async def _expire():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.expire(key, seconds)

        return await self.execute_with_retry(_expire)

    async def delete(self, *keys: str) -> Optional[int]:
        """
        Delete keys from Redis.

        Args:
            *keys: Redis keys to delete

        Returns:
            Number of keys deleted or None if circuit breaker is open
        """

        async def _delete():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.delete(*keys)

        return await self.execute_with_retry(_delete)

    async def exists(self, *keys: str) -> Optional[int]:
        """
        Check if keys exist in Redis.

        Args:
            *keys: Redis keys to check

        Returns:
            Number of existing keys or None if circuit breaker is open
        """

        async def _exists():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.exists(*keys)

        return await self.execute_with_retry(_exists)

    async def mget(self, keys: List[str]) -> Optional[List[Optional[str]]]:
        """
        Get multiple values from Redis.

        Args:
            keys: List of Redis keys

        Returns:
            List of values (None for missing keys) or None if circuit breaker is open
        """

        async def _mget():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.mget(keys)

        return await self.execute_with_retry(_mget)

    async def mset(self, mapping: Dict[str, Union[str, int, float]]) -> Optional[bool]:
        """
        Set multiple key-value pairs in Redis.

        Args:
            mapping: Dictionary of key-value pairs

        Returns:
            True if successful, None if circuit breaker is open
        """

        async def _mset():
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.mset(mapping)

        return await self.execute_with_retry(_mset)

    async def eval(
        self,
        script: str,
        keys: List[str],
        args: List[Union[str, int, float]],
    ) -> Any:
        """
        Execute a Lua script on Redis.

        Args:
            script: Lua script to execute
            keys: Redis keys the script will access
            args: Arguments to pass to the script

        Returns:
            Script result or None if circuit breaker is open
        """

        async def _eval() -> Any:
            if not self._redis_client:
                raise RedisConnectionError("Redis client not initialized")
            return await self._redis_client.eval(script, len(keys), *keys, *args)  # type: ignore

        return await self.execute_with_retry(_eval)

    async def pipeline(self):
        """
        Create a Redis pipeline for batch operations.

        Returns:
            Redis pipeline object or None if circuit breaker is open
        """
        if self._check_circuit_breaker():
            logger.debug("Circuit breaker is open - skipping pipeline creation")
            return None

        if not self._redis_client:
            raise RedisConnectionError("Redis client not initialized")

        return self._redis_client.pipeline()

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on Redis connectivity.

        Returns:
            Health status dictionary
        """
        try:
            if not self._redis_client:
                return {
                    "status": "unhealthy",
                    "error": "Redis client not initialized",
                    "circuit_breaker_open": self._circuit_breaker_open,
                }

            async def _ping():
                return await self._redis_client.ping()  # type: ignore

            ping_result = await self.execute_with_retry(_ping)

            return {
                "status": "healthy" if ping_result else "degraded",
                "ping_successful": bool(ping_result),
                "circuit_breaker_open": self._circuit_breaker_open,
                "redis_url": self.settings.redis_url,
            }

        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "circuit_breaker_open": self._circuit_breaker_open,
                "redis_url": self.settings.redis_url,
            }

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        # retrier.get_stats() は数値系のみの辞書として型推論されることがあるため、
        # ここでは拡張可能な Dict[str, Any] として扱う
        stats: Dict[str, Any] = dict(self.retrier.get_stats())
        stats["redis_url"] = self.settings.redis_url
        stats["circuit_breaker_open"] = self._circuit_breaker_open
        stats["circuit_breaker_last_failure"] = self._circuit_breaker_last_failure
        stats["connection_pool_created"] = self._connection_pool is not None
        return stats


# Global client instance
_client: Optional[RedisClient] = None


def get_redis_client(settings: Optional[CrawlerSettings] = None) -> RedisClient:
    """
    Get the global Redis client instance.

    Args:
        settings: Optional settings override

    Returns:
        Redis client instance
    """
    global _client

    if _client is None or settings is not None:
        from ..config.settings import get_cached_settings

        if settings is None:
            settings = get_cached_settings()

        _client = RedisClient(settings)

    return _client


async def initialize_redis_client(settings: Optional[CrawlerSettings] = None) -> RedisClient:
    """
    Initialize and return the global Redis client.

    Args:
        settings: Optional settings override

    Returns:
        Initialized Redis client instance
    """
    client = get_redis_client(settings)
    await client.initialize()
    return client


def reset_client() -> None:
    """Reset the global client instance (useful for testing)"""
    global _client
    _client = None


if __name__ == "__main__":
    # CLI utility for testing Redis functionality
    import asyncio
    import sys

    from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python redis_client.py [health|stats|test]")
            sys.exit(1)

        command = sys.argv[1]

        settings = load_settings()
        client = await initialize_redis_client(settings)

        try:
            if command == "health":
                health = await client.health_check()
                print(f"Health status: {health}")

            elif command == "stats":
                stats = client.get_stats()
                print(f"Client stats: {stats}")

            elif command == "test":
                # Test basic operations
                print("Testing Redis client...")

                # Test set/get
                await client.set("test_key", "test_value", expire=60)
                value = await client.get("test_key")
                print(f"Set/Get test: {value}")

                # Test increment
                counter = await client.incr("test_counter")
                print(f"Increment test: {counter}")

                # Test batch operations
                await client.mset({"key1": "value1", "key2": "value2"})
                values = await client.mget(["key1", "key2"])
                print(f"Batch test: {values}")

                # Cleanup
                await client.delete("test_key", "test_counter", "key1", "key2")

                print("Redis client test completed!")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        finally:
            await client.close()

    asyncio.run(main())
