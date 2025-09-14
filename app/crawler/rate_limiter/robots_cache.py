"""
robots.txt caching system using Redis for distributed crawler coordination.

Provides efficient caching of robots.txt files and parsed RobotsParser objects
to avoid repeated downloads and parsing across multiple crawler instances.
"""

import json
import logging
import pickle
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from ..config.settings import CrawlerSettings
from .redis_client import RedisClient, get_redis_client

logger = logging.getLogger(__name__)


class RobotsParseError(Exception):
    """Raised when robots.txt parsing fails"""

    pass


class RobotsCacheManager:
    """
    Redis-based caching system for robots.txt files.

    Caches both raw robots.txt content and parsed RobotFileParser objects
    to minimize network requests and CPU usage across distributed crawlers.
    """

    def __init__(self, redis_client: Optional[RedisClient] = None, settings: Optional[CrawlerSettings] = None):
        self.redis_client = redis_client or get_redis_client(settings)
        self.settings = settings or CrawlerSettings(
            environment="default",
            aws_region="us-east-1",
            localstack_endpoint=None,
            dynamodb_table="default_table",
            sqs_crawl_queue_url="",
            sqs_discovery_queue_url=None,
            sqs_indexing_queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/indexing-queue",
            s3_raw_bucket="default_bucket",
            s3_parsed_bucket="crawler-parsed-bucket",
            redis_url="redis://localhost:6379",
            redis_db=0,
            crawler_id="default_crawler",
            max_concurrent_requests=10,
            request_timeout=30,
            user_agent="DefaultUserAgent",
            default_qps_per_domain=1,
            max_retries=3,
            base_backoff_seconds=1,
            max_backoff_seconds=32,
            acquisition_ttl_seconds=300,
            heartbeat_interval_seconds=60,
            max_content_length=1048576,
            default_language="en",
            language_detection_confidence=0.9,
            log_level="INFO",
            json_logs=False,
            health_check_port=8080,
            health_check_enabled=True,
            metrics_enabled=True,
            metrics_interval_seconds=60,
            rate_limiter_enabled=True,
        )

        # Redis key prefixes
        self.robots_content_prefix = "robots:content:"
        self.robots_parser_prefix = "robots:parser:"
        self.robots_metadata_prefix = "robots:meta:"
        self.robots_error_prefix = "robots:error:"

        # Default TTL values (in seconds)
        self.default_robots_ttl = 3600  # 1 hour
        self.error_cache_ttl = 300  # 5 minutes for errors
        self.metadata_ttl = 7200  # 2 hours for metadata

        logger.info("Robots cache manager initialized")

    def _get_robots_url(self, domain: str) -> str:
        """
        Get the robots.txt URL for a domain.

        Args:
            domain: Domain name

        Returns:
            robots.txt URL
        """
        # Ensure domain doesn't have protocol
        if domain.startswith(("http://", "https://")):
            parsed = urlparse(domain)
            domain = parsed.netloc

        # Always use HTTPS first, fallback to HTTP handled at request level
        return f"https://{domain}/robots.txt"

    async def get_robots_parser(self, domain: str) -> Optional[RobotFileParser]:
        """
        Get cached robots.txt parser for a domain.

        Args:
            domain: Domain name

        Returns:
            RobotFileParser instance or None if not cached/available
        """
        try:
            parser_key = f"{self.robots_parser_prefix}{domain}"

            # Try to get pickled parser from cache
            cached_parser_data = await self.redis_client.get(parser_key)

            if cached_parser_data is not None:
                try:
                    # Unpickle the parser object
                    parser = pickle.loads(cached_parser_data.encode("latin-1"))
                    logger.debug(f"Retrieved cached robots parser for {domain}")
                    return parser
                except (pickle.PickleError, UnicodeDecodeError) as e:
                    logger.warning(f"Failed to unpickle robots parser for {domain}: {e}")
                    # Remove corrupted cache entry
                    await self.redis_client.delete(parser_key)

            # No cached parser found
            logger.debug(f"No cached robots parser found for {domain}")
            return None

        except Exception as e:
            logger.error(f"Error retrieving robots parser for {domain}: {e}")
            return None

    async def cache_robots_parser(
        self,
        domain: str,
        robots_content: str,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Cache robots.txt content and create a parsed RobotFileParser.

        Args:
            domain: Domain name
            robots_content: Raw robots.txt content
            ttl: Time to live in seconds (defaults to class default)

        Returns:
            True if caching was successful
        """
        try:
            if ttl is None:
                ttl = self.default_robots_ttl

            # Parse the robots.txt content
            parser = self._parse_robots_content(domain, robots_content)

            # Cache raw content
            content_key = f"{self.robots_content_prefix}{domain}"
            content_cached = await self.redis_client.set(content_key, robots_content, expire=ttl)

            # Cache pickled parser
            parser_key = f"{self.robots_parser_prefix}{domain}"
            try:
                pickled_parser = pickle.dumps(parser).decode("latin-1")
                parser_cached = await self.redis_client.set(parser_key, pickled_parser, expire=ttl)
            except pickle.PickleError as e:
                logger.warning(f"Failed to pickle robots parser for {domain}: {e}")
                parser_cached = False

            # Cache metadata
            await self._cache_robots_metadata(domain, parser, robots_content, ttl)

            success = bool(content_cached and parser_cached)
            if success:
                logger.info(f"Cached robots.txt for {domain} (TTL: {ttl}s)")
            else:
                logger.warning(f"Partial failure caching robots.txt for {domain}")

            return success

        except Exception as e:
            logger.error(f"Error caching robots.txt for {domain}: {e}")
            return False

    def _parse_robots_content(self, domain: str, robots_content: str) -> RobotFileParser:
        """
        Parse robots.txt content into a RobotFileParser.

        Args:
            domain: Domain name
            robots_content: Raw robots.txt content

        Returns:
            Parsed RobotFileParser instance

        Raises:
            RobotsParseError: If parsing fails
        """
        try:
            robots_url = self._get_robots_url(domain)
            parser = RobotFileParser()
            parser.set_url(robots_url)

            # Parse the content by splitting into lines
            lines = robots_content.split("\n")
            parser.parse(lines)

            logger.debug(f"Successfully parsed robots.txt for {domain}")
            return parser

        except Exception as e:
            logger.error(f"Failed to parse robots.txt for {domain}: {e}")
            raise RobotsParseError(f"Failed to parse robots.txt for {domain}: {e}") from e

    async def _cache_robots_metadata(
        self,
        domain: str,
        parser: RobotFileParser,
        robots_content: str,
        ttl: int,
    ) -> None:
        """
        Cache metadata about the robots.txt file.

        Args:
            domain: Domain name
            parser: Parsed RobotFileParser
            robots_content: Raw robots.txt content
            ttl: Time to live in seconds
        """
        try:
            # Extract useful metadata
            metadata = {
                "cached_at": time.time(),
                "content_length": len(robots_content),
                "crawl_delay": parser.crawl_delay("*"),
                "request_rate": parser.request_rate("*"),
                "has_sitemap": bool(self._extract_sitemaps_from_content(robots_content)),
                "disallowed_paths_count": len(self._extract_disallowed_paths(parser)),
                "allowed_paths_count": len(self._extract_allowed_paths(parser)),
            }

            metadata_key = f"{self.robots_metadata_prefix}{domain}"
            await self.redis_client.set(metadata_key, json.dumps(metadata), expire=min(ttl, self.metadata_ttl))

        except Exception as e:
            logger.warning(f"Failed to cache robots metadata for {domain}: {e}")

    def _extract_sitemaps_from_content(self, robots_content: str) -> List[str]:
        """Extract sitemap URLs from robots.txt content"""
        sitemaps: List[str] = []
        for line in robots_content.split("\n"):
            line = line.strip()
            if line.lower().startswith("sitemap:"):
                sitemap_url = line[8:].strip()  # Remove 'sitemap:' prefix
                if sitemap_url:
                    sitemaps.append(sitemap_url)
        return sitemaps

    def _extract_disallowed_paths(self, parser: RobotFileParser) -> Set[str]:
        """Extract disallowed paths from parsed robots.txt"""
        disallowed: Set[str] = set()

        # This is a simplified extraction - RobotFileParser doesn't expose
        # parsed rules directly, so we parse them from the entries
        if hasattr(parser, "entries"):
            for entry in parser.entries:  # type: ignore
                if hasattr(entry, "rulelines"):  # type: ignore
                    for rule in entry.rulelines:  # type: ignore
                        if hasattr(rule, "allowance") and rule.allowance is False:  # type: ignore
                            if hasattr(rule, "path"):  # type: ignore
                                disallowed.add(rule.path)  # type: ignore

        return disallowed

    def _extract_allowed_paths(self, parser: RobotFileParser) -> Set[str]:
        """Extract explicitly allowed paths from parsed robots.txt"""
        allowed: Set[str] = set()

        if hasattr(parser, "entries"):
            for entry in parser.entries:  # type: ignore
                if hasattr(entry, "rulelines"):  # type: ignore
                    for rule in entry.rulelines:  # type: ignore
                        if hasattr(rule, "allowance") and rule.allowance is True:  # type: ignore
                            if hasattr(rule, "path"):  # type: ignore
                                allowed.add(rule.path)  # type: ignore

        return allowed

    async def get_robots_content(self, domain: str) -> Optional[str]:
        """
        Get raw robots.txt content from cache.

        Args:
            domain: Domain name

        Returns:
            Raw robots.txt content or None if not cached
        """
        try:
            content_key = f"{self.robots_content_prefix}{domain}"
            content = await self.redis_client.get(content_key)

            if content is not None:
                logger.debug(f"Retrieved cached robots content for {domain}")

            return content

        except Exception as e:
            logger.error(f"Error retrieving robots content for {domain}: {e}")
            return None

    async def cache_robots_error(self, domain: str, error_message: str) -> bool:
        """
        Cache that robots.txt retrieval failed for a domain.

        Args:
            domain: Domain name
            error_message: Error message describing the failure

        Returns:
            True if error was cached successfully
        """
        try:
            error_key = f"{self.robots_error_prefix}{domain}"
            error_data = json.dumps(
                {
                    "error": error_message,
                    "cached_at": time.time(),
                }
            )

            result = await self.redis_client.set(error_key, error_data, expire=self.error_cache_ttl)

            if result:
                logger.debug(f"Cached robots.txt error for {domain}: {error_message}")

            return bool(result)

        except Exception as e:
            logger.error(f"Error caching robots error for {domain}: {e}")
            return False

    async def is_robots_error_cached(self, domain: str) -> bool:
        """
        Check if robots.txt retrieval error is cached for a domain.

        Args:
            domain: Domain name

        Returns:
            True if error is cached (indicating recent failure)
        """
        try:
            error_key = f"{self.robots_error_prefix}{domain}"
            error_data = await self.redis_client.get(error_key)
            return error_data is not None

        except Exception as e:
            logger.error(f"Error checking robots error cache for {domain}: {e}")
            return False

    async def get_robots_metadata(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Get cached robots.txt metadata for a domain.

        Args:
            domain: Domain name

        Returns:
            Metadata dictionary or None if not cached
        """
        try:
            metadata_key = f"{self.robots_metadata_prefix}{domain}"
            metadata_str = await self.redis_client.get(metadata_key)

            if metadata_str is not None:
                return json.loads(metadata_str)

            return None

        except Exception as e:
            logger.error(f"Error retrieving robots metadata for {domain}: {e}")
            return None

    async def clear_robots_cache(self, domain: str) -> int:
        """
        Clear all cached robots.txt data for a domain.

        Args:
            domain: Domain name

        Returns:
            Number of cache entries removed
        """
        try:
            keys_to_delete = [
                f"{self.robots_content_prefix}{domain}",
                f"{self.robots_parser_prefix}{domain}",
                f"{self.robots_metadata_prefix}{domain}",
                f"{self.robots_error_prefix}{domain}",
            ]

            deleted_count = await self.redis_client.delete(*keys_to_delete)

            if deleted_count and deleted_count > 0:
                logger.info(f"Cleared robots cache for {domain}, removed {deleted_count} entries")

            return deleted_count or 0

        except Exception as e:
            logger.error(f"Error clearing robots cache for {domain}: {e}")
            return 0

    async def get_cache_stats(self) -> Dict[str, int]:
        """
        Get statistics about the robots.txt cache.

        Returns:
            Dictionary with cache statistics
        """
        try:
            # This is a simple implementation - in production you might want
            # to use Redis SCAN to count keys by prefix
            stats = {
                "content_entries": 0,
                "parser_entries": 0,
                "metadata_entries": 0,
                "error_entries": 0,
            }

            # Note: Getting exact counts would require scanning all keys
            # which could be expensive. This is a placeholder implementation.
            logger.debug("Cache stats requested - returning placeholder data")

            return stats

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}

    async def preload_robots_for_domains(
        self,
        domains: List[str],
        robots_fetcher_func: Callable[[str], Awaitable[Optional[str]]],
        max_concurrent: int = 5,
    ) -> Dict[str, bool]:
        """
        Preload robots.txt for multiple domains.

        Args:
            domains: List of domain names
            robots_fetcher_func: Async function that fetches robots.txt content
            max_concurrent: Maximum concurrent requests

        Returns:
            Dictionary mapping domain to success status
        """
        import asyncio

        results: Dict[str, bool] = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_and_cache_robots(domain: str) -> bool:
            async with semaphore:
                try:
                    # Check if already cached
                    if await self.get_robots_parser(domain) is not None:
                        logger.debug(f"Robots.txt already cached for {domain}")
                        return True

                    # Fetch robots.txt content
                    robots_content = await robots_fetcher_func(domain)

                    if robots_content is not None:
                        # Cache the content
                        return await self.cache_robots_parser(domain, robots_content)
                    else:
                        await self.cache_robots_error(domain, "Failed to fetch robots.txt")
                        return False

                except Exception as e:
                    logger.error(f"Error preloading robots.txt for {domain}: {e}")
                    await self.cache_robots_error(domain, str(e))
                    return False

        # Run all tasks concurrently
        tasks = [fetch_and_cache_robots(domain) for domain in domains]
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        for domain, result in zip(domains, task_results):
            if isinstance(result, Exception):
                results[domain] = False
                logger.error(f"Exception preloading robots.txt for {domain}: {result}")
            else:
                results[domain] = bool(result)

        successful = sum(1 for success in results.values() if isinstance(success, bool) and success is True)  # type: ignore
        logger.info(f"Preloaded robots.txt for {successful}/{len(domains)} domains")

        return results


if __name__ == "__main__":
    # CLI utility for testing robots cache
    import asyncio
    import sys

    from ..config.settings import load_settings
    from .redis_client import initialize_redis_client

    async def main():
        if len(sys.argv) < 3:
            print("Usage: python robots_cache.py [get|cache|clear|stats] <domain> [content_file]")
            sys.exit(1)

        command = sys.argv[1]
        domain = sys.argv[2]

        settings = load_settings()
        redis_client = await initialize_redis_client(settings)

        try:
            cache_manager = RobotsCacheManager(redis_client, settings)

            if command == "get":
                parser = await cache_manager.get_robots_parser(domain)
                if parser:
                    print(f"Found cached robots parser for {domain}")
                    print(f"Crawl delay: {parser.crawl_delay('*')}")
                    print(f"Can fetch /: {parser.can_fetch('*', '/')}")
                else:
                    print(f"No cached robots parser found for {domain}")

            elif command == "cache":
                if len(sys.argv) < 4:
                    print("Usage: python robots_cache.py cache <domain> <content_file>")
                    sys.exit(1)

                content_file = sys.argv[3]
                try:
                    with open(content_file, "r", encoding="utf-8") as f:
                        robots_content = f.read()

                    success = await cache_manager.cache_robots_parser(domain, robots_content)
                    print(f"Cached robots.txt for {domain}: {'SUCCESS' if success else 'FAILED'}")

                except FileNotFoundError:
                    print(f"File not found: {content_file}")
                except Exception as e:
                    print(f"Error reading file: {e}")

            elif command == "clear":
                cleared = await cache_manager.clear_robots_cache(domain)
                print(f"Cleared {cleared} cache entries for {domain}")

            elif command == "stats":
                stats = await cache_manager.get_cache_stats()
                print(f"Cache statistics: {stats}")

                metadata = await cache_manager.get_robots_metadata(domain)
                if metadata:
                    print(f"Metadata for {domain}: {metadata}")
                else:
                    print(f"No metadata cached for {domain}")

            elif command == "test":
                print(f"Testing robots cache for {domain}...")

                # Test with sample robots.txt content
                sample_robots = """User-agent: *
Disallow: /admin/
Disallow: /private/
Allow: /public/

Crawl-delay: 1

Sitemap: https://example.com/sitemap.xml
"""

                # Cache the sample content
                cached = await cache_manager.cache_robots_parser(domain, sample_robots)
                print(f"Cached sample robots.txt: {cached}")

                # Retrieve and test
                parser = await cache_manager.get_robots_parser(domain)
                if parser:
                    print(f"Retrieved parser - crawl delay: {parser.crawl_delay('*')}")
                    print(f"Can fetch /admin/: {parser.can_fetch('*', '/admin/')}")
                    print(f"Can fetch /public/: {parser.can_fetch('*', '/public/')}")

                # Check metadata
                metadata = await cache_manager.get_robots_metadata(domain)
                print(f"Metadata: {metadata}")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        finally:
            await redis_client.close()

    asyncio.run(main())
