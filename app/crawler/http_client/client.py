"""
HTTP client for the distributed crawler system.

Provides high-level HTTP operations with built-in rate limiting, robots.txt
checking, error handling, and content processing capabilities.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, cast

import aiohttp
from aiohttp import ClientError, ClientTimeout, TCPConnector
from pydantic import HttpUrl

from ...schema.crawl import CrawlResult
from ..config.settings import CrawlerSettings, get_cached_settings
from ..core.types import CrawlErrorType, Headers
from ..rate_limiter.limiter import SlidingWindowRateLimiter
from ..rate_limiter.robots_cache import RobotsCacheManager
from ..utils.retry import NETWORK_RETRY_CONFIG, AsyncRetrier
from ..utils.url import extract_domain, normalize_url

logger = logging.getLogger(__name__)


class CrawlError(Exception):
    """Base exception for crawling errors"""

    def __init__(
        self,
        message: str,
        error_type: CrawlErrorType = CrawlErrorType.UNKNOWN,
        original_error: Optional[Exception] = None,
    ):
        self.error_type = error_type
        self.original_error = original_error
        super().__init__(message)


class RateLimitExceededError(CrawlError):
    """Raised when rate limit is exceeded"""

    def __init__(self, domain: str, retry_after: Optional[float] = None):
        self.domain = domain
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded for domain {domain}", CrawlErrorType.RATE_LIMITED)


class RobotsBlockedError(CrawlError):
    """Raised when URL is blocked by robots.txt"""

    def __init__(self, url: str, user_agent: str):
        self.url = url
        self.user_agent = user_agent
        super().__init__(f"URL {url} blocked by robots.txt for user agent {user_agent}", CrawlErrorType.ROBOTS_BLOCKED)


class HTTPError(CrawlError):
    """Raised for HTTP-related errors"""

    def __init__(self, message: str, status_code: Optional[int] = None, original_error: Optional[Exception] = None):
        self.status_code = status_code
        super().__init__(message, CrawlErrorType.HTTP_ERROR, original_error)


class ContentTooLargeError(CrawlError):
    """Raised when content exceeds size limits"""

    def __init__(self, url: str, content_length: int, max_length: int):
        self.url = url
        self.content_length = content_length
        self.max_length = max_length
        super().__init__(
            f"Content too large: {content_length} bytes > {max_length} bytes for {url}", CrawlErrorType.HTTP_ERROR
        )


class CrawlerHTTPClient:
    """
    HTTP client for distributed web crawling with built-in rate limiting,
    robots.txt checking, and comprehensive error handling.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None):
        """
        Initialize the HTTP client.

        Args:
            settings: Optional crawler settings (uses cached settings if None)
        """
        self.settings = settings or get_cached_settings()
        self.retrier = AsyncRetrier(NETWORK_RETRY_CONFIG)

        # Initialize session (will be created on first use)
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_created_at = 0.0

        # Initialize rate limiter and robots cache
        self.rate_limiter = SlidingWindowRateLimiter(settings=self.settings)
        self.robots_cache = RobotsCacheManager(settings=self.settings)

        # Statistics tracking
        self.stats = {
            "requests_made": 0,
            "requests_successful": 0,
            "requests_failed": 0,
            "rate_limit_hits": 0,
            "robots_blocked": 0,
            "bytes_downloaded": 0,
            "total_response_time": 0.0,
        }

        logger.info(f"Initialized HTTP client with max_concurrent={self.settings.max_concurrent_requests}")

    async def initialize(self) -> None:
        """Initialize the HTTP client and its dependencies"""
        await self._ensure_session()

    async def close(self) -> None:
        """Close the HTTP client and clean up resources"""
        if self._session:
            await self._session.close()
            self._session = None

        logger.info("HTTP client closed")

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure HTTP session is created and valid"""
        # Recreate session every 30 minutes to prevent connection staleness
        if self._session is None or time.time() - self._session_created_at > 1800:
            if self._session:
                await self._session.close()

            # Create connector with optimized settings
            connector = TCPConnector(
                limit=self.settings.max_concurrent_requests,
                limit_per_host=min(10, self.settings.max_concurrent_requests // 2),
                ttl_dns_cache=300,  # DNS cache TTL
                use_dns_cache=True,
                keepalive_timeout=30,
                enable_cleanup_closed=True,
            )

            # Create timeout configuration
            timeout = ClientTimeout(
                total=self.settings.request_timeout,
                connect=10,  # Connection timeout
                sock_read=self.settings.request_timeout - 5,  # Socket read timeout
            )

            # Default headers
            headers = {
                "User-Agent": self.settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja,en;q=0.9,*;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
            }

            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers,
                raise_for_status=False,  # We'll handle status codes manually
            )

            self._session_created_at = time.time()
            logger.debug("Created new HTTP session")

        return self._session

    async def fetch_url(
        self, url: str, check_robots: bool = True, custom_headers: Optional[Headers] = None
    ) -> CrawlResult:
        """
        Fetch a single URL with full crawling pipeline.

        Args:
            url: URL to fetch
            check_robots: Whether to check robots.txt before fetching
            custom_headers: Optional custom headers to add

        Returns:
            CrawlResult with fetched content and metadata

        Raises:
            CrawlError: If crawling fails for any reason
        """
        start_time = time.time()
        normalized_url = normalize_url(url)
        domain = extract_domain(normalized_url)

        try:
            # Step 1: Check rate limits
            await self._check_rate_limits(domain)

            # Step 2: Check robots.txt if requested
            if check_robots:
                await self._check_robots_permission(normalized_url, domain)

            # Step 3: Record request attempt for rate limiting
            await self.rate_limiter.record_request(domain)

            # Step 4: Perform HTTP request
            response_data = await self._perform_request(normalized_url, custom_headers)

            # Step 5: Validate response
            await self._validate_response(response_data, normalized_url)

            # Step 6: Create crawl result
            result = await self._create_crawl_result(normalized_url, response_data)

            # Update statistics
            response_time = time.time() - start_time
            self.stats["requests_successful"] += 1
            self.stats["total_response_time"] += response_time
            self.stats["bytes_downloaded"] += len(response_data["content"])

            logger.info(
                f"Successfully crawled {normalized_url}",
                extra={
                    "url": normalized_url,
                    "domain": domain,
                    "status_code": response_data["status_code"],
                    "response_time": response_time,
                    "content_length": len(response_data["content"]),
                },
            )

            return result

        except CrawlError:
            # Re-raise crawl errors as-is
            self.stats["requests_failed"] += 1
            raise

        except Exception as e:
            self.stats["requests_failed"] += 1
            logger.error(f"Unexpected error crawling {normalized_url}: {e}")
            raise CrawlError(f"Unexpected error: {str(e)}", CrawlErrorType.UNKNOWN, e) from e

        finally:
            self.stats["requests_made"] += 1

    async def fetch_robots_txt(self, domain: str) -> Optional[str]:
        """
        Fetch robots.txt for a domain.

        Args:
            domain: Domain to fetch robots.txt for

        Returns:
            robots.txt content or None if not available
        """
        robots_url = f"https://{domain}/robots.txt"

        try:
            # Use minimal request for robots.txt (no robots checking for robots.txt itself)
            session = await self._ensure_session()

            async with session.get(robots_url) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.debug(f"Fetched robots.txt for {domain}")
                    return content
                else:
                    logger.debug(f"No robots.txt found for {domain} (status {response.status})")
                    return None

        except Exception as e:
            logger.warning(f"Error fetching robots.txt for {domain}: {e}")
            return None

    async def _check_rate_limits(self, domain: str) -> None:
        """Check if request is allowed under rate limits"""
        # Get domain-specific QPS limit
        qps_limit = self.settings.domain_qps_overrides.get(domain, self.settings.default_qps_per_domain)

        # Check rate limit
        allowed = await self.rate_limiter.check_domain_limit(domain, qps_limit)
        if not allowed:
            self.stats["rate_limit_hits"] += 1
            # Calculate next allowed time (型安全に計算)
            next_time = await self.rate_limiter.get_next_allowed_time(domain)
            retry_after = max(0.0, float(next_time) - time.time())

            logger.warning(f"Rate limit exceeded for {domain}, retry after {retry_after:.1f}s")
            raise RateLimitExceededError(domain, retry_after)

    async def _check_robots_permission(self, url: str, domain: str) -> None:
        """Check if URL is allowed by robots.txt"""
        # First try to get cached robots info
        robots_parser = await self.robots_cache.get_robots_parser(domain)

        if robots_parser is None:
            # Fetch and cache robots.txt
            robots_content = await self.fetch_robots_txt(domain)
            if robots_content:
                await self.robots_cache.cache_robots_parser(domain, robots_content)
                robots_parser = await self.robots_cache.get_robots_parser(domain)

        # Check permission
        if robots_parser:
            user_agent = self.settings.user_agent
            if not robots_parser.can_fetch(user_agent, url):
                self.stats["robots_blocked"] += 1
                logger.warning(f"URL {url} blocked by robots.txt for user agent {user_agent}")
                raise RobotsBlockedError(url, user_agent)

    async def _perform_request(self, url: str, custom_headers: Optional[Headers] = None) -> Dict[str, Any]:
        """Perform the actual HTTP request with retries"""
        session = await self._ensure_session()

        # Merge custom headers
        headers: Dict[str, str] = {}
        if custom_headers:
            headers.update(custom_headers)

        async def _request() -> Dict[str, Any]:
            async with session.get(url, headers=headers or None) as response:
                # Read content with size limit
                content = await self._read_content_safely(response, url)

                return {
                    "status_code": response.status,
                    "headers": dict(response.headers),
                    "content": content,
                    "final_url": str(response.url),
                    "content_type": response.headers.get("content-type", ""),
                }

        try:
            return await self.retrier.call(_request, exceptions=(ClientError, TimeoutError))

        except Exception as e:
            if isinstance(e, (ClientError, TimeoutError)):
                raise HTTPError(f"HTTP request failed: {str(e)}", original_error=e) from e
            else:
                raise CrawlError(f"Request error: {str(e)}", CrawlErrorType.CONNECTION_ERROR, e) from e

    async def _read_content_safely(self, response: aiohttp.ClientResponse, url: str) -> bytes:
        """Read response content with size limits"""
        content_length = response.headers.get("content-length")
        if content_length:
            try:
                length = int(content_length)
                if length > self.settings.max_content_length:
                    raise ContentTooLargeError(url, length, self.settings.max_content_length)
            except ValueError:
                pass  # Invalid content-length header, continue

        # Read content in chunks to respect size limits
        content = bytearray()
        chunk_size = 8192

        async for chunk in response.content.iter_chunked(chunk_size):
            content.extend(chunk)
            if len(content) > self.settings.max_content_length:
                raise ContentTooLargeError(url, len(content), self.settings.max_content_length)

        return bytes(content)

    async def _validate_response(self, response_data: Dict[str, Any], url: str) -> None:
        """Validate HTTP response"""
        status_code = response_data["status_code"]

        # Check for client/server errors
        if status_code >= 400:
            error_msg = f"HTTP {status_code} error for {url}"

            # Classify error type based on status code
            if 400 <= status_code < 500:
                if status_code == 404:
                    # 404 is not always an error for crawling purposes
                    logger.info(f"URL not found: {url} (404)")
                else:
                    raise HTTPError(error_msg, status_code)
            else:  # 500+ server errors
                raise HTTPError(error_msg, status_code)

        # Validate content type (should be HTML or similar)
        content_type = response_data.get("content_type", "").lower()
        if content_type and not any(ct in content_type for ct in ["text/html", "application/xhtml", "text/plain"]):
            logger.warning(f"Unexpected content type {content_type} for {url}")

    async def _create_crawl_result(self, url: str, response_data: Dict[str, Any]) -> CrawlResult:
        """Create CrawlResult from response data"""
        # For now, we'll create a simple result without S3 storage
        # S3 storage will be handled by the storage layer

        return CrawlResult(
            url=cast(HttpUrl, url),
            status_code=response_data["status_code"],
            fetched_at=datetime.now(timezone.utc),
            html_s3_key="",  # Will be populated by storage layer
            error=None,
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get HTTP client statistics"""
        stats = self.stats.copy()

        # Calculate derived metrics
        if stats["requests_made"] > 0:
            stats["success_rate"] = stats["requests_successful"] / stats["requests_made"]
            stats["average_response_time"] = (
                stats["total_response_time"] / stats["requests_successful"] if stats["requests_successful"] > 0 else 0
            )
        else:
            stats["success_rate"] = 0
            stats["average_response_time"] = 0

        # Add session info
        stats["session_active"] = self._session is not None and not self._session.closed
        stats["session_age"] = time.time() - self._session_created_at if self._session else 0

        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on HTTP client"""
        try:
            # Test basic connectivity
            session = await self._ensure_session()

            return {
                "status": "healthy",
                "session_active": not session.closed,
                "rate_limiter_healthy": True,  # Could add actual health check
                "robots_cache_healthy": True,  # Could add actual health check
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
            }


# Global client instance
_client: Optional[CrawlerHTTPClient] = None


def get_http_client(settings: Optional[CrawlerSettings] = None) -> CrawlerHTTPClient:
    """
    Get the global HTTP client instance.

    Args:
        settings: Optional settings override

    Returns:
        HTTP client instance
    """
    global _client

    if _client is None or settings is not None:
        if settings is None:
            settings = get_cached_settings()
        _client = CrawlerHTTPClient(settings)

    return _client


async def initialize_http_client(settings: Optional[CrawlerSettings] = None) -> CrawlerHTTPClient:
    """
    Initialize and return the global HTTP client.

    Args:
        settings: Optional settings override

    Returns:
        Initialized HTTP client instance
    """
    client = get_http_client(settings)
    await client.initialize()
    return client


def reset_client() -> None:
    """Reset the global client instance (useful for testing)"""
    global _client
    _client = None


if __name__ == "__main__":
    # CLI utility for testing HTTP client functionality
    import asyncio
    import sys

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python client.py [health|stats|test] [url]")
            sys.exit(1)

        command = sys.argv[1]

        # Initialize client
        client = await initialize_http_client()

        try:
            if command == "health":
                health = await client.health_check()
                print(f"Health status: {health}")

            elif command == "stats":
                stats = client.get_stats()
                print(f"Client stats: {stats}")

            elif command == "test" and len(sys.argv) > 2:
                url = sys.argv[2]
                print(f"Testing HTTP client with URL: {url}")

                try:
                    result = await client.fetch_url(url)
                    print(f"Success: {result.status_code} - {result.fetched_at}")
                except CrawlError as e:
                    print(f"Crawl error: {e.error_type.value} - {e}")
                except Exception as e:
                    print(f"Unexpected error: {e}")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        finally:
            await client.close()

    asyncio.run(main())
