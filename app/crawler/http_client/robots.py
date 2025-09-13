"""
Robots.txt checking and processing for the distributed crawler.

Provides high-level robots.txt operations with caching, crawl delay detection,
and comprehensive permission checking functionality.
"""

import logging
from typing import Any, Dict, Optional
from urllib.robotparser import RobotFileParser

from ..config.settings import CrawlerSettings, get_cached_settings
from ..core.types import RobotsInfo
from ..rate_limiter.robots_cache import RobotsCacheManager
from ..utils.url import extract_domain

logger = logging.getLogger(__name__)


class RobotsError(Exception):
    """Base exception for robots.txt processing errors"""

    pass


class RobotsChecker:
    """
    High-level robots.txt checker with caching and comprehensive URL permission validation.

    Integrates with the RobotsCacheManager from the rate limiting system to provide
    efficient robots.txt processing across distributed crawler instances.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None):
        """
        Initialize the robots checker.

        Args:
            settings: Optional crawler settings (uses cached settings if None)
        """
        self.settings = settings or get_cached_settings()
        self.robots_cache = RobotsCacheManager(settings=self.settings)

        # Statistics tracking
        self.stats = {
            "checks_performed": 0,
            "urls_allowed": 0,
            "urls_blocked": 0,
            "robots_fetched": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "parse_errors": 0,
        }

        logger.info("Initialized robots checker")

    async def check_url_allowed(self, url: str, user_agent: Optional[str] = None) -> bool:
        """
        Check if a URL is allowed by robots.txt rules.

        Args:
            url: URL to check
            user_agent: User agent to check for (defaults to settings user agent)

        Returns:
            True if URL is allowed, False if blocked

        Raises:
            RobotsError: If robots.txt processing fails
        """
        if user_agent is None:
            user_agent = self.settings.user_agent

        domain = extract_domain(url)

        try:
            # Get robots parser from cache or fetch/parse if needed
            robots_parser = await self._get_robots_parser(domain)

            self.stats["checks_performed"] += 1

            if robots_parser is None:
                # No robots.txt found or accessible, default to allowed
                self.stats["urls_allowed"] += 1
                logger.debug(f"No robots.txt for {domain}, allowing {url}")
                return True

            # Check permission using robots parser
            allowed = robots_parser.can_fetch(user_agent, url)

            if allowed:
                self.stats["urls_allowed"] += 1
                logger.debug(f"URL {url} allowed for user agent {user_agent}")
            else:
                self.stats["urls_blocked"] += 1
                logger.info(f"URL {url} blocked by robots.txt for user agent {user_agent}")

            return allowed

        except Exception as e:
            logger.error(f"Error checking robots.txt permission for {url}: {e}")
            # Default to allowing on error to avoid blocking crawling
            self.stats["urls_allowed"] += 1
            return True

    async def get_crawl_delay(self, domain: str, user_agent: Optional[str] = None) -> Optional[int]:
        """
        Get crawl delay for a domain from robots.txt.

        Args:
            domain: Domain to get crawl delay for
            user_agent: User agent to check for (defaults to settings user agent)

        Returns:
            Crawl delay in seconds, or None if not specified
        """
        if user_agent is None:
            user_agent = self.settings.user_agent

        try:
            robots_parser = await self._get_robots_parser(domain)

            if robots_parser is None:
                logger.debug(f"No robots.txt for {domain}, no crawl delay")
                return None

            # Get crawl delay for the user agent
            crawl_delay = robots_parser.crawl_delay(user_agent)

            if crawl_delay is not None:
                # Convert to integer seconds
                delay_seconds = int(crawl_delay)
                logger.debug(f"Crawl delay for {domain}: {delay_seconds}s")
                return delay_seconds
            else:
                logger.debug(f"No crawl delay specified for {domain}")
                return None

        except Exception as e:
            logger.error(f"Error getting crawl delay for {domain}: {e}")
            return None

    async def get_robots_info(self, domain: str, user_agent: Optional[str] = None) -> RobotsInfo:
        """
        Get comprehensive robots.txt information for a domain.

        Args:
            domain: Domain to get robots info for
            user_agent: User agent to check for (defaults to settings user agent)

        Returns:
            RobotsInfo with permission and crawl delay information
        """
        if user_agent is None:
            user_agent = self.settings.user_agent

        try:
            robots_parser = await self._get_robots_parser(domain)

            if robots_parser is None:
                return RobotsInfo(
                    domain=domain,
                    allowed=True,
                    crawl_delay=None,
                )

            # For general permission checking, test the domain root
            test_url = f"https://{domain}/"
            allowed = robots_parser.can_fetch(user_agent, test_url)
            crawl_delay = robots_parser.crawl_delay(user_agent)

            return RobotsInfo(
                domain=domain,
                allowed=allowed,
                crawl_delay=int(crawl_delay) if crawl_delay is not None else None,
            )

        except Exception as e:
            logger.error(f"Error getting robots info for {domain}: {e}")
            # Default to allowed on error
            return RobotsInfo(
                domain=domain,
                allowed=True,
                crawl_delay=None,
            )

    async def parse_robots_txt(self, content: str, domain: str) -> Optional[RobotFileParser]:
        """
        Parse robots.txt content into a RobotFileParser.

        Args:
            content: robots.txt content
            domain: Domain the robots.txt belongs to

        Returns:
            Parsed RobotFileParser or None if parsing fails
        """
        try:
            parser = RobotFileParser()
            parser.set_url(f"https://{domain}/robots.txt")

            # Parse the content
            parser.parse(content.split("\n"))

            logger.debug(f"Successfully parsed robots.txt for {domain}")
            return parser

        except Exception as e:
            self.stats["parse_errors"] += 1
            logger.error(f"Error parsing robots.txt for {domain}: {e}")
            return None

    async def fetch_and_cache_robots(self, domain: str) -> Optional[RobotFileParser]:
        """
        Fetch robots.txt for a domain and cache it.

        Args:
            domain: Domain to fetch robots.txt for

        Returns:
            Parsed RobotFileParser or None if not available
        """
        try:
            # Import here to avoid circular imports
            from .client import CrawlerHTTPClient

            # Create a temporary HTTP client for fetching robots.txt
            http_client = CrawlerHTTPClient(self.settings)
            await http_client.initialize()

            try:
                robots_content = await http_client.fetch_robots_txt(domain)

                if robots_content:
                    # Parse and cache the robots.txt
                    await self.robots_cache.cache_robots_parser(domain, robots_content)
                    self.stats["robots_fetched"] += 1

                    # Return the parsed version
                    return await self.robots_cache.get_robots_parser(domain)
                else:
                    logger.debug(f"No robots.txt found for {domain}")
                    return None

            finally:
                await http_client.close()

        except Exception as e:
            logger.error(f"Error fetching robots.txt for {domain}: {e}")
            return None

    async def _get_robots_parser(self, domain: str) -> Optional[RobotFileParser]:
        """
        Get robots parser for a domain, fetching if not cached.

        Args:
            domain: Domain to get robots parser for

        Returns:
            RobotFileParser or None if not available
        """
        # Try to get from cache first
        robots_parser = await self.robots_cache.get_robots_parser(domain)

        if robots_parser is not None:
            self.stats["cache_hits"] += 1
            return robots_parser

        # Cache miss - fetch and cache robots.txt
        self.stats["cache_misses"] += 1
        logger.debug(f"Robots.txt cache miss for {domain}, fetching...")

        return await self.fetch_and_cache_robots(domain)

    def get_stats(self) -> Dict[str, Any]:
        """Get robots checker statistics"""
        stats = self.stats.copy()

        # Calculate derived metrics
        if stats["checks_performed"] > 0:
            stats["block_rate"] = int(stats["urls_blocked"] / stats["checks_performed"])
            stats["allow_rate"] = int(stats["urls_allowed"] / stats["checks_performed"])
        else:
            stats["block_rate"] = 0
            stats["allow_rate"] = 0

        cache_requests = stats["cache_hits"] + stats["cache_misses"]
        if cache_requests > 0:
            stats["cache_hit_rate"] = int(stats["cache_hits"] / cache_requests)
        else:
            stats["cache_hit_rate"] = 0

        return stats

    async def health_check(self) -> Dict[str, Any]:
        raise NotImplementedError("Health check not implemented")


# Global checker instance
_checker: Optional[RobotsChecker] = None


def get_robots_checker(settings: Optional[CrawlerSettings] = None) -> RobotsChecker:
    """
    Get the global robots checker instance.

    Args:
        settings: Optional settings override

    Returns:
        Robots checker instance
    """
    global _checker

    if _checker is None or settings is not None:
        if settings is None:
            settings = get_cached_settings()
        _checker = RobotsChecker(settings)

    return _checker


async def initialize_robots_checker(settings: Optional[CrawlerSettings] = None) -> RobotsChecker:
    """
    Initialize and return the global robots checker.

    Args:
        settings: Optional settings override

    Returns:
        Initialized robots checker instance
    """
    checker = get_robots_checker(settings)
    return checker


def reset_checker() -> None:
    """Reset the global checker instance (useful for testing)"""
    global _checker
    _checker = None


if __name__ == "__main__":
    # CLI utility for testing robots checker functionality
    import asyncio
    import sys

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python robots.py [health|stats|check|delay|info] [domain/url]")
            sys.exit(1)

        command = sys.argv[1]

        # Initialize checker
        checker = await initialize_robots_checker()

        try:
            if command == "health":
                health = await checker.health_check()
                print(f"Health status: {health}")

            elif command == "stats":
                stats = checker.get_stats()
                print(f"Checker stats: {stats}")

            elif command == "check" and len(sys.argv) > 2:
                url = sys.argv[2]
                print(f"Checking robots.txt permission for: {url}")

                allowed = await checker.check_url_allowed(url)
                print(f"Allowed: {allowed}")

            elif command == "delay" and len(sys.argv) > 2:
                domain = sys.argv[2]
                print(f"Getting crawl delay for domain: {domain}")

                delay = await checker.get_crawl_delay(domain)
                print(f"Crawl delay: {delay}s" if delay else "No crawl delay specified")

            elif command == "info" and len(sys.argv) > 2:
                domain = sys.argv[2]
                print(f"Getting robots info for domain: {domain}")

                info = await checker.get_robots_info(domain)
                print(f"Domain: {info.domain}")
                print(f"Allowed: {info.allowed}")
                print(f"Crawl delay: {info.crawl_delay}s" if info.crawl_delay else "No crawl delay")

            else:
                print(f"Unknown command or missing arguments: {command}")
                sys.exit(1)

        finally:
            pass

    asyncio.run(main())
