"""
URL Discovery Coordinator for distributed crawler.

Orchestrates the discovery of URLs for domains through sitemap parsing,
robots.txt analysis, and integration with the crawling queue system.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

from ..config.settings import CrawlerSettings
from ..discovery.sitemap_parser import SitemapParser, URLInfo
from ..http_client.client import CrawlerHTTPClient
from ..http_client.robots import RobotsChecker
from ..state.state_manager import URLStateManager
from ..utils.retry import NETWORK_RETRY_CONFIG, AsyncRetrier
from ..utils.url import extract_domain, generate_url_hash, normalize_url

logger = logging.getLogger(__name__)


class DiscoveryMessage(BaseModel):
    """Message format for discovery queue"""

    domain: str
    priority: int = 1
    max_urls: Optional[int] = None
    discovery_depth: int = 3
    requested_at: datetime = datetime.now(timezone.utc)


class DiscoveryStats(BaseModel):
    """Statistics for discovery operations"""

    domains_processed: int = 0
    urls_discovered: int = 0
    urls_enqueued: int = 0
    urls_duplicates: int = 0
    robots_blocked_domains: int = 0
    sitemap_errors: int = 0
    queue_errors: int = 0


class URLDiscoveryCoordinator:
    """
    Coordinates URL discovery for domains through sitemap parsing and robots.txt analysis.

    Processes discovery queue messages, discovers URLs for domains using sitemaps,
    and enqueues discovered URLs for crawling with proper deduplication.
    """

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.running = False
        self.discovery_task: Optional[asyncio.Task[Any]] = None

        # Initialize components
        self.http_client = CrawlerHTTPClient(settings)
        self.sitemap_parser = SitemapParser(settings, self.http_client)
        self.robots_checker = RobotsChecker(settings)
        self.state_manager = URLStateManager(settings.crawler_id or f"crawler-{id(self)}")
        self.retrier = AsyncRetrier(NETWORK_RETRY_CONFIG)

        # Statistics
        self.stats = DiscoveryStats()

        # Discovery configuration
        self.batch_size = getattr(settings, "discovery_batch_size", 100)
        self.max_concurrent_discoveries = getattr(settings, "max_concurrent_discoveries", 5)
        self.discovery_interval = getattr(settings, "discovery_interval_seconds", 30)

        # Semaphore for concurrent discovery control
        self.discovery_semaphore = asyncio.Semaphore(self.max_concurrent_discoveries)

        logger.info(f"Discovery coordinator initialized for crawler {settings.crawler_id or 'unknown'}")

    async def start(self):
        """Start the discovery coordinator main loop"""
        if self.running:
            logger.warning("Discovery coordinator is already running")
            return

        self.running = True
        self.discovery_task = asyncio.create_task(self._main_loop())

        logger.info("Discovery coordinator started")

    async def stop(self):
        """Stop the discovery coordinator gracefully"""
        if not self.running:
            return

        self.running = False

        if self.discovery_task and not self.discovery_task.done():
            logger.info("Stopping discovery coordinator...")
            self.discovery_task.cancel()

            try:
                await self.discovery_task
            except asyncio.CancelledError:
                pass

        # Cleanup resources
        await self.sitemap_parser.close()
        await self.http_client.close()

        logger.info("Discovery coordinator stopped")

    async def _main_loop(self):
        """Main discovery coordinator loop"""
        logger.info("Discovery coordinator main loop started")

        while self.running:
            try:
                # Process discovery queue with timeout
                await asyncio.wait_for(self.process_discovery_queue(), timeout=self.discovery_interval)

            except asyncio.TimeoutError:
                # Normal timeout - continue loop
                continue
            except asyncio.CancelledError:
                logger.info("Discovery coordinator loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in discovery main loop: {e}")
                # Wait before retrying to avoid tight error loops
                await asyncio.sleep(min(self.discovery_interval, 10))

        logger.info("Discovery coordinator main loop ended")

    async def process_discovery_queue(self):
        """
        Process messages from the discovery queue.

        This is a placeholder implementation. In production, this would
        integrate with AWS SQS to receive discovery messages.
        """
        # Placeholder implementation - in production this would receive from SQS

        # For now, we'll simulate processing some domains
        # This would be replaced with actual SQS message reception
        test_domains: List[str] = []

        if test_domains:
            for domain in test_domains:
                if not self.running:
                    break

                message = DiscoveryMessage(domain=domain)
                await self._process_discovery_message(message)
        else:
            # No messages to process, wait briefly
            await asyncio.sleep(1)

    async def _process_discovery_message(self, message: DiscoveryMessage):
        """Process a single discovery message"""
        async with self.discovery_semaphore:
            try:
                logger.info(
                    f"Processing discovery for domain: {message.domain}",
                    extra={
                        "domain": message.domain,
                        "priority": message.priority,
                        "discovery_depth": message.discovery_depth,
                    },
                )

                await self.discover_domain_urls(
                    message.domain, max_urls=message.max_urls, discovery_depth=message.discovery_depth
                )

                self.stats.domains_processed += 1

            except Exception as e:
                logger.error(f"Error processing discovery message for {message.domain}: {e}")

    async def discover_domain_urls(self, domain: str, max_urls: Optional[int] = None, discovery_depth: int = 3):
        """
        Discover URLs for a domain through comprehensive analysis.

        Args:
            domain: Domain to discover URLs for
            max_urls: Maximum number of URLs to discover (None for no limit)
            discovery_depth: Maximum depth for recursive sitemap parsing
        """
        try:
            # 1. Check robots.txt and respect crawling permissions
            robots_allowed = await self._check_domain_robots_permission(domain)
            if not robots_allowed:
                logger.info(f"Domain {domain} blocks crawling via robots.txt")
                self.stats.robots_blocked_domains += 1
                return

            # 2. Discover and parse sitemaps
            discovered_urls = await self._discover_urls_from_sitemaps(domain, discovery_depth)

            # 3. Apply URL filtering and limits
            filtered_urls = await self._filter_and_limit_urls(discovered_urls, max_urls)

            # 4. Check for duplicates against existing state
            new_urls = await self._deduplicate_urls(filtered_urls)

            # 5. Enqueue new URLs for crawling
            if new_urls:
                await self.enqueue_urls_batch(new_urls)

                logger.info(
                    f"Discovery completed for {domain}: {len(new_urls)} new URLs enqueued",
                    extra={
                        "domain": domain,
                        "urls_discovered": len(discovered_urls),
                        "urls_filtered": len(filtered_urls),
                        "urls_new": len(new_urls),
                        "urls_duplicates": len(filtered_urls) - len(new_urls),
                    },
                )
            else:
                logger.info(f"No new URLs discovered for domain {domain}")

            # Update statistics
            self.stats.urls_discovered += len(discovered_urls)
            self.stats.urls_enqueued += len(new_urls)
            self.stats.urls_duplicates += len(filtered_urls) - len(new_urls)

        except Exception as e:
            logger.error(f"Error discovering URLs for domain {domain}: {e}")
            self.stats.sitemap_errors += 1

    async def _check_domain_robots_permission(self, domain: str) -> bool:
        """Check if domain allows crawling via robots.txt"""
        try:
            # Check if our user agent is allowed to crawl the domain root
            test_url = f"https://{domain}/"
            allowed = await self.robots_checker.check_url_allowed(test_url, self.settings.user_agent)

            return allowed

        except Exception as e:
            logger.debug(f"Could not check robots.txt for {domain}: {e}")
            # Default to allowed if robots.txt cannot be checked
            return True

    async def _discover_urls_from_sitemaps(self, domain: str, discovery_depth: int = 3) -> List[URLInfo]:
        """Discover URLs from domain sitemaps"""
        try:
            # 1. Discover sitemap URLs for the domain
            sitemap_urls = await self.sitemap_parser.discover_sitemaps(domain)

            if not sitemap_urls:
                logger.info(f"No sitemaps found for domain {domain}")
                return []

            # 2. Recursively extract URLs from all sitemaps
            discovered_urls = await self.sitemap_parser.extract_urls_recursive(sitemap_urls, max_depth=discovery_depth)

            return discovered_urls

        except Exception as e:
            logger.error(f"Error discovering URLs from sitemaps for {domain}: {e}")
            return []

    async def _filter_and_limit_urls(self, urls: List[URLInfo], max_urls: Optional[int] = None) -> List[URLInfo]:
        """Filter URLs and apply limits"""
        if not urls:
            return []

        # Filter out invalid or unwanted URLs
        filtered_urls: List[URLInfo] = []
        for url_info in urls:
            if await self._should_include_url(str(url_info.url)):
                filtered_urls.append(url_info)

        # Apply URL limit if specified
        if max_urls and len(filtered_urls) > max_urls:
            # Prioritize URLs by various factors
            filtered_urls = await self._prioritize_urls(filtered_urls)
            filtered_urls = filtered_urls[:max_urls]

        return filtered_urls

    async def _should_include_url(self, url: str) -> bool:
        """Determine if URL should be included in crawling"""
        try:
            # Basic URL validation
            if not url or len(url) > 2048:  # URL too long
                return False

            # Skip non-HTTP/HTTPS URLs
            if not url.lower().startswith(("http://", "https://")):
                return False

            # Skip common non-content file types
            excluded_extensions = {
                ".pdf",
                ".doc",
                ".docx",
                ".xls",
                ".xlsx",
                ".ppt",
                ".pptx",
                ".zip",
                ".rar",
                ".7z",
                ".tar",
                ".gz",
                ".jpg",
                ".jpeg",
                ".png",
                ".gif",
                ".bmp",
                ".svg",
                ".webp",
                ".mp3",
                ".mp4",
                ".avi",
                ".mov",
                ".wmv",
                ".flv",
                ".css",
                ".js",
                ".ico",
                ".woff",
                ".woff2",
                ".ttf",
                ".eot",
            }

            url_lower = url.lower()
            if any(url_lower.endswith(ext) for ext in excluded_extensions):
                return False

            # Additional filtering could be added here
            # (e.g., blacklist patterns, robots.txt per-URL checking)

            return True

        except Exception as e:
            logger.debug(f"Error filtering URL {url}: {e}")
            return False

    async def _prioritize_urls(self, urls: List[URLInfo]) -> List[URLInfo]:
        """Prioritize URLs based on sitemap metadata"""

        def priority_key(url_info: URLInfo) -> Tuple[float, float, int]:
            # Sort by: priority (desc), last_modified (desc), URL length (asc)
            priority = url_info.priority or 0.5
            last_mod = url_info.last_modified or datetime.min.replace(tzinfo=timezone.utc)
            url_length = len(str(url_info.url))

            return (-priority, -last_mod.timestamp(), url_length)

        return sorted(urls, key=priority_key)

    async def _deduplicate_urls(self, urls: List[URLInfo]) -> List[str]:
        """Remove duplicate URLs by checking against existing state"""
        if not urls:
            return []

        new_urls: List[str] = []
        url_strings = [str(url_info.url) for url_info in urls]

        try:
            # Check which URLs already exist in the state
            existing_states = await self.state_manager.batch_get_url_states(url_strings)

            for url in url_strings:
                url_hash = generate_url_hash(normalize_url(url))
                if url_hash not in existing_states:
                    new_urls.append(url)

        except Exception as e:
            logger.warning(f"Error checking for duplicate URLs: {e}")
            # Fallback: return all URLs if deduplication fails
            new_urls = url_strings

        return new_urls

    async def enqueue_urls_batch(self, urls: List[str]):
        """
        Enqueue discovered URLs to the crawl queue in batches.

        Args:
            urls: List of URLs to enqueue for crawling
        """
        if not urls:
            return

        try:
            # Create URL state entries for all new URLs
            await self._create_url_states_batch(urls)

            # In production, this would send messages to SQS crawl queue
            # For now, we'll log the operation
            logger.info(f"Enqueued {len(urls)} URLs for crawling")

            # Placeholder for SQS integration:
            # await self.queue_manager.send_crawl_messages(urls)

        except Exception as e:
            logger.error(f"Error enqueueing URLs batch: {e}")
            self.stats.queue_errors += 1

    async def _create_url_states_batch(self, urls: List[str]):
        """Create URL state entries for discovered URLs"""
        try:
            url_states: List[Dict[str, Any]] = []

            for url in urls:
                normalized_url = normalize_url(url)
                url_hash = generate_url_hash(normalized_url)
                domain = extract_domain(normalized_url)

                url_states.append(
                    {
                        "url_hash": url_hash,
                        "url": normalized_url,
                        "domain": domain,
                        "state": "pending",
                        "created_at": datetime.now(timezone.utc),
                        "updated_at": datetime.now(timezone.utc),
                    }
                )

            # Batch create URL states
            await self.state_manager.batch_create_url_states(url_states)

            logger.debug(f"Created {len(url_states)} URL state entries")

        except Exception as e:
            logger.error(f"Error creating URL state entries: {e}")

    async def process_manual_discovery(
        self, domain: str, max_urls: Optional[int] = None, discovery_depth: int = 3
    ) -> Dict[str, Any]:
        """
        Manually trigger discovery for a domain (useful for testing/debugging).

        Args:
            domain: Domain to discover URLs for
            max_urls: Maximum URLs to discover
            discovery_depth: Sitemap parsing depth

        Returns:
            Discovery results summary
        """
        start_time = datetime.now()
        initial_stats = self.stats.model_dump()

        try:
            await self.discover_domain_urls(domain, max_urls, discovery_depth)

            # Calculate delta statistics
            end_time = datetime.now()
            current_stats = self.stats.model_dump()

            result = {
                "status": "success",
                "domain": domain,
                "duration_seconds": (end_time - start_time).total_seconds(),
                "stats_delta": {key: current_stats[key] - initial_stats[key] for key in current_stats},
            }

            return result

        except Exception as e:
            logger.error(f"Manual discovery failed for {domain}: {e}")
            return {
                "status": "error",
                "domain": domain,
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }

    def get_stats(self) -> Dict[str, Any]:
        """Get discovery coordinator statistics"""
        return {
            **self.stats.model_dump(),
            "running": self.running,
            "sitemap_parser_stats": self.sitemap_parser.get_stats(),
            "robots_checker_stats": self.robots_checker.get_stats(),
            "http_client_stats": self.http_client.get_stats(),
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on discovery coordinator"""
        try:
            # Check component health
            sitemap_health = await self.sitemap_parser.health_check()
            http_health = await self.http_client.health_check()

            # Check if coordinator is running properly
            coordinator_healthy = self.running and (
                self.discovery_task is None or not self.discovery_task.done() or not self.discovery_task.exception()
            )

            overall_status = "healthy"
            if not coordinator_healthy or sitemap_health.get("status") != "healthy":
                overall_status = "degraded"
            if http_health.get("status") == "unhealthy":
                overall_status = "unhealthy"

            return {
                "status": overall_status,
                "coordinator_running": self.running,
                "sitemap_parser_status": sitemap_health.get("status", "unknown"),
                "http_client_status": http_health.get("status", "unknown"),
                "stats": self.get_stats(),
            }

        except Exception as e:
            logger.error(f"Discovery coordinator health check failed: {e}")
            return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    # CLI utility for testing discovery coordinator
    import sys

    from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python discovery_coordinator.py <command> [args...]")
            print("Commands:")
            print("  discover <domain> [max_urls] - Manually discover URLs for domain")
            print("  start - Start discovery coordinator daemon")
            print("  health - Check coordinator health")
            print("  stats - Show coordinator statistics")
            sys.exit(1)

        command = sys.argv[1]
        settings = load_settings()
        coordinator = URLDiscoveryCoordinator(settings)

        try:
            if command == "discover" and len(sys.argv) >= 3:
                domain = sys.argv[2]
                max_urls = int(sys.argv[3]) if len(sys.argv) > 3 else None

                print(f"Discovering URLs for domain: {domain}")
                result = await coordinator.process_manual_discovery(domain, max_urls)

                print(f"Discovery result: {result}")

            elif command == "start":
                print("Starting discovery coordinator...")
                await coordinator.start()

                # Keep running until interrupted
                try:
                    while coordinator.running:
                        await asyncio.sleep(1)
                except KeyboardInterrupt:
                    print("\nShutting down...")
                    await coordinator.stop()

            elif command == "health":
                health = await coordinator.health_check()
                print(f"Health status: {health}")

            elif command == "stats":
                stats = coordinator.get_stats()
                print("Discovery coordinator statistics:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
        finally:
            await coordinator.stop()

    asyncio.run(main())
