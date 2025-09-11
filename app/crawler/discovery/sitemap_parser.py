"""
Sitemap XML parsing and URL discovery for distributed crawler.

Handles sitemap.xml, sitemap index files, and recursive URL extraction
with proper error handling and rate limiting integration.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

from pydantic import BaseModel, HttpUrl

from ..config.settings import CrawlerSettings
from ..http_client.client import CrawlerHTTPClient
from ..utils.retry import NETWORK_RETRY_CONFIG, AsyncRetrier
from ..utils.url import is_valid_url, normalize_url

logger = logging.getLogger(__name__)


class URLInfo(BaseModel):
    """Information extracted from sitemap entries"""

    url: HttpUrl
    last_modified: Optional[datetime] = None
    change_frequency: Optional[str] = None
    priority: Optional[float] = None
    source_sitemap: Optional[str] = None


class SitemapStats(BaseModel):
    """Statistics for sitemap parsing operations"""

    sitemaps_processed: int = 0
    urls_discovered: int = 0
    urls_filtered: int = 0
    xml_parse_errors: int = 0
    http_errors: int = 0
    duplicate_urls: int = 0


class SitemapParser:
    """
    Sitemap XML parser with recursive discovery and URL extraction.

    Supports both sitemap.xml files and sitemap index files with
    proper error handling and integration with HTTP client.
    """

    def __init__(self, settings: CrawlerSettings, http_client: Optional[CrawlerHTTPClient] = None):
        self.settings = settings
        self.http_client = http_client or CrawlerHTTPClient(settings)
        self.retrier = AsyncRetrier(NETWORK_RETRY_CONFIG)

        # Statistics tracking
        self.stats = SitemapStats()

        # Common sitemap paths to check
        self.common_sitemap_paths = [
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemaps.xml",
            "/sitemap/sitemap.xml",
            "/sitemaps/sitemap.xml",
        ]

        logger.info("Sitemap parser initialized")

    async def discover_sitemaps(self, domain: str) -> List[str]:
        """
        Discover sitemap URLs for a domain.

        Checks robots.txt first, then falls back to common paths.

        Args:
            domain: Domain to discover sitemaps for

        Returns:
            List of sitemap URLs found
        """
        sitemap_urls: Set[str] = set()
        base_url = f"https://{domain}"

        try:
            # 1. Check robots.txt for sitemap declarations
            robots_sitemaps = await self._discover_sitemaps_from_robots(domain)
            sitemap_urls.update(robots_sitemaps)

            # 2. Check common sitemap paths
            common_sitemaps = await self._check_common_sitemap_paths(base_url)
            sitemap_urls.update(common_sitemaps)

            sitemap_list = list(sitemap_urls)

            logger.info(
                f"Discovered {len(sitemap_list)} sitemaps for domain {domain}",
                extra={
                    "domain": domain,
                    "sitemap_count": len(sitemap_list),
                    "sitemap_urls": sitemap_list[:5],  # Log first 5 for debugging
                },
            )

            return sitemap_list

        except Exception as e:
            logger.error(f"Error discovering sitemaps for domain {domain}: {e}")
            return []

    async def _discover_sitemaps_from_robots(self, domain: str) -> List[str]:
        """Extract sitemap URLs from robots.txt"""
        sitemap_urls: List[str] = []

        try:
            robots_url = f"https://{domain}/robots.txt"

            # Use HTTP client to fetch robots.txt
            result = await self.http_client.fetch_url(robots_url, check_robots=False)

            if result.status_code == 200 and result.html_s3_key:
                # For now, we'll need to fetch the content from S3 or store it temporarily
                # This is a simplification - in production we'd fetch from S3
                robots_content = "# This would be fetched from S3 in production"

                # Parse sitemap declarations from robots.txt
                sitemap_pattern = re.compile(r"^sitemap:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
                matches = sitemap_pattern.findall(robots_content)

                for match in matches:
                    sitemap_url = match.strip()
                    if is_valid_url(sitemap_url):
                        sitemap_urls.append(normalize_url(sitemap_url))  # type: ignore

        except Exception as e:
            logger.debug(f"Could not fetch robots.txt for {domain}: {e}")

        return sitemap_urls

    async def _check_common_sitemap_paths(self, base_url: str) -> List[str]:
        """Check common sitemap paths for a domain"""
        sitemap_urls: List[str] = []

        for path in self.common_sitemap_paths:
            sitemap_url = urljoin(base_url, path)

            try:
                # Use HTTP client with robots check disabled for sitemap discovery
                result = await self.http_client.fetch_url(sitemap_url, check_robots=False)

                if result.status_code == 200:
                    sitemap_urls.append(sitemap_url)  # type: ignore
                    logger.debug(f"Found sitemap at common path: {sitemap_url}")

            except Exception as e:
                logger.debug(f"No sitemap found at {sitemap_url}: {e}")
                continue

        return sitemap_urls

    async def parse_sitemap_xml(self, sitemap_url: str) -> List[URLInfo]:
        """
        Parse a sitemap XML file and extract URL information.

        Args:
            sitemap_url: URL of the sitemap to parse

        Returns:
            List of URLInfo objects extracted from sitemap
        """
        try:
            # Fetch sitemap content
            result = await self.http_client.fetch_url(sitemap_url, check_robots=False)

            if result.status_code != 200:
                logger.warning(f"Failed to fetch sitemap {sitemap_url}: HTTP {result.status_code}")
                self.stats.http_errors += 1
                return []

            # For now, we'll simulate XML content - in production this would be fetched from S3
            xml_content = await self._get_xml_content_from_result(result)

            # Parse XML content
            urls = await self._parse_xml_content(xml_content, sitemap_url)

            self.stats.sitemaps_processed += 1
            self.stats.urls_discovered += len(urls)

            logger.info(
                f"Parsed sitemap {sitemap_url}: found {len(urls)} URLs",
                extra={"sitemap_url": sitemap_url, "urls_found": len(urls)},
            )

            return urls

        except Exception as e:
            logger.error(f"Error parsing sitemap {sitemap_url}: {e}")
            self.stats.xml_parse_errors += 1
            return []

    async def _get_xml_content_from_result(self, result: Any) -> str:
        """
        Get XML content from crawl result.

        In production, this would fetch from S3 using the html_s3_key.
        For now, we'll return a sample XML for testing.
        """
        # This is a placeholder - in production we'd fetch from S3
        return """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url>
        <loc>https://example.com/page1</loc>
        <lastmod>2023-12-01T10:00:00Z</lastmod>
        <changefreq>weekly</changefreq>
        <priority>0.8</priority>
    </url>
</urlset>"""

    async def _parse_xml_content(self, xml_content: str, source_sitemap: str) -> List[URLInfo]:
        """Parse XML content and extract URL information"""
        urls: List[URLInfo] = []

        try:
            root = ET.fromstring(xml_content)

            # Handle regular sitemap format
            if self._is_regular_sitemap(root):
                urls = await self._parse_regular_sitemap(root, source_sitemap)
            # Handle sitemap index format
            elif self._is_sitemap_index(root):
                # For sitemap index, we return the nested sitemap URLs
                # They will be processed recursively by the coordinator
                nested_sitemaps = await self._parse_sitemap_index(root)
                # Convert sitemap URLs to URLInfo objects for consistency
                for sitemap_url in nested_sitemaps:
                    try:
                        urls.append(URLInfo(url=HttpUrl(sitemap_url), source_sitemap=source_sitemap))
                    except Exception as e:
                        logger.debug(f"Invalid sitemap URL {sitemap_url}: {e}")

        except ET.ParseError as e:
            logger.warning(f"XML parse error in sitemap {source_sitemap}: {e}")
            self.stats.xml_parse_errors += 1
        except Exception as e:
            logger.error(f"Unexpected error parsing XML content: {e}")
            self.stats.xml_parse_errors += 1

        return urls

    def _is_regular_sitemap(self, root: ET.Element) -> bool:
        """Check if XML root represents a regular sitemap"""
        return root.tag.endswith("}urlset") or root.tag == "urlset"

    def _is_sitemap_index(self, root: ET.Element) -> bool:
        """Check if XML root represents a sitemap index"""
        return root.tag.endswith("}sitemapindex") or root.tag == "sitemapindex"

    async def _parse_regular_sitemap(self, root: ET.Element, source_sitemap: str) -> List[URLInfo]:
        """Parse regular sitemap XML and extract URLs"""
        urls: List[URLInfo] = []

        # Handle both namespaced and non-namespaced XML
        url_elements = root.findall(".//url") or root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url")

        for url_element in url_elements:
            try:
                url_info = await self._extract_url_info(url_element, source_sitemap)
                if url_info:
                    urls.append(url_info)
            except Exception as e:
                logger.debug(f"Error extracting URL info: {e}")
                continue

        return urls

    async def _extract_url_info(self, url_element: ET.Element, source_sitemap: str) -> Optional[URLInfo]:
        """Extract URL information from a sitemap URL element"""
        try:
            # Extract URL (required field)
            loc_element = url_element.find("loc") or url_element.find(
                "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
            )

            if loc_element is None or not loc_element.text:
                return None

            url = normalize_url(loc_element.text.strip())

            if not is_valid_url(url):
                logger.debug(f"Invalid URL in sitemap: {url}")
                self.stats.urls_filtered += 1
                return None

            # Extract optional fields
            lastmod = self._extract_lastmod(url_element)
            changefreq = self._extract_text_field(url_element, "changefreq")
            priority = self._extract_priority(url_element)

            return URLInfo(
                url=HttpUrl(url),
                last_modified=lastmod,
                change_frequency=changefreq,
                priority=priority,
                source_sitemap=source_sitemap,
            )

        except Exception as e:
            logger.debug(f"Error extracting URL info from element: {e}")
            return None

    def _extract_lastmod(self, url_element: ET.Element) -> Optional[datetime]:
        """Extract and parse lastmod field"""
        lastmod_element = url_element.find("lastmod") or url_element.find(
            "{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod"
        )

        if lastmod_element is None or not lastmod_element.text:
            return None

        try:
            # Handle various datetime formats
            lastmod_text = lastmod_element.text.strip()

            # Try ISO format first
            try:
                return datetime.fromisoformat(lastmod_text.replace("Z", "+00:00"))
            except ValueError:
                pass

            # Try common date formats
            date_formats = ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ"]

            for date_format in date_formats:
                try:
                    dt = datetime.strptime(lastmod_text, date_format)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue

        except Exception as e:
            logger.debug(f"Could not parse lastmod '{lastmod_element.text}': {e}")

        return None

    def _extract_text_field(self, url_element: ET.Element, field_name: str) -> Optional[str]:
        """Extract text field from URL element"""
        element = url_element.find(field_name) or url_element.find(
            f"{{http://www.sitemaps.org/schemas/sitemap/0.9}}{field_name}"
        )

        if element is not None and element.text:
            return element.text.strip()
        return None

    def _extract_priority(self, url_element: ET.Element) -> Optional[float]:
        """Extract and parse priority field"""
        priority_text = self._extract_text_field(url_element, "priority")

        if priority_text:
            try:
                priority = float(priority_text)
                # Ensure priority is within valid range (0.0 to 1.0)
                return max(0.0, min(1.0, priority))
            except ValueError:
                logger.debug(f"Invalid priority value: {priority_text}")

        return None

    async def parse_sitemap_index(self, sitemap_url: str) -> List[str]:
        """
        Parse a sitemap index file and extract nested sitemap URLs.

        Args:
            sitemap_url: URL of the sitemap index to parse

        Returns:
            List of nested sitemap URLs
        """
        try:
            result = await self.http_client.fetch_url(sitemap_url, check_robots=False)

            if result.status_code != 200:
                logger.warning(f"Failed to fetch sitemap index {sitemap_url}: HTTP {result.status_code}")
                self.stats.http_errors += 1
                return []

            xml_content = await self._get_xml_content_from_result(result)
            nested_sitemaps = await self._parse_sitemap_index(ET.fromstring(xml_content))

            logger.info(
                f"Parsed sitemap index {sitemap_url}: found {len(nested_sitemaps)} nested sitemaps",
                extra={"sitemap_index_url": sitemap_url, "nested_sitemap_count": len(nested_sitemaps)},
            )

            return nested_sitemaps

        except Exception as e:
            logger.error(f"Error parsing sitemap index {sitemap_url}: {e}")
            self.stats.xml_parse_errors += 1
            return []

    async def _parse_sitemap_index(self, root: ET.Element) -> List[str]:
        """Parse sitemap index XML and extract nested sitemap URLs"""
        sitemap_urls: List[str] = []

        # Handle both namespaced and non-namespaced XML
        sitemap_elements = root.findall(".//sitemap") or root.findall(
            ".//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"
        )

        for sitemap_element in sitemap_elements:
            loc_element = sitemap_element.find("loc") or sitemap_element.find(
                "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
            )

            if loc_element is not None and loc_element.text:
                sitemap_url = normalize_url(loc_element.text.strip())
                if is_valid_url(sitemap_url):
                    sitemap_urls.append(sitemap_url)  # type: ignore

        return sitemap_urls

    async def extract_urls_recursive(self, sitemap_urls: List[str], max_depth: int = 3) -> List[URLInfo]:
        """
        Recursively extract URLs from sitemaps with depth control.

        Args:
            sitemap_urls: List of sitemap URLs to process
            max_depth: Maximum recursion depth for nested sitemaps

        Returns:
            List of all URLInfo objects found recursively
        """
        all_urls: List[URLInfo] = []
        processed_sitemaps: Set[str] = set()

        async def _process_sitemaps(urls: List[str], depth: int) -> List[URLInfo]:
            if depth >= max_depth:
                logger.info(f"Reached maximum sitemap recursion depth {max_depth}")
                return []

            batch_urls: List[URLInfo] = []

            for sitemap_url in urls:
                if sitemap_url in processed_sitemaps:
                    self.stats.duplicate_urls += 1
                    continue

                processed_sitemaps.add(sitemap_url)

                try:
                    # Parse the sitemap
                    urls_found = await self.parse_sitemap_xml(sitemap_url)

                    # Separate regular URLs from nested sitemaps
                    regular_urls: List[URLInfo] = []
                    nested_sitemaps: List[str] = []

                    for url_info in urls_found:
                        url_str = str(url_info.url)
                        if self._is_sitemap_url(url_str):
                            nested_sitemaps.append(url_str)  # type: ignore
                        else:
                            regular_urls.append(url_info)  # type: ignore

                    batch_urls.extend(regular_urls)

                    # Recursively process nested sitemaps
                    if nested_sitemaps and depth < max_depth - 1:
                        nested_urls = await _process_sitemaps(nested_sitemaps, depth + 1)
                        batch_urls.extend(nested_urls)

                except Exception as e:
                    logger.error(f"Error processing sitemap {sitemap_url} at depth {depth}: {e}")
                    continue

            return batch_urls

        try:
            all_urls = await _process_sitemaps(sitemap_urls, 0)

            logger.info(
                f"Recursive sitemap extraction complete: {len(all_urls)} URLs found",
                extra={
                    "total_urls": len(all_urls),
                    "processed_sitemaps": len(processed_sitemaps),
                    "max_depth": max_depth,
                },
            )

        except Exception as e:
            logger.error(f"Error in recursive URL extraction: {e}")

        return all_urls

    def _is_sitemap_url(self, url: str) -> bool:
        """Check if a URL appears to be a sitemap based on its path"""
        url_lower = url.lower()
        sitemap_indicators = ["sitemap", "sitemap.xml", "sitemaps.xml", "sitemap_index.xml", "sitemap-index.xml"]
        return any(indicator in url_lower for indicator in sitemap_indicators)

    def get_stats(self) -> Dict[str, int]:
        """Get sitemap parsing statistics"""
        return {
            "sitemaps_processed": self.stats.sitemaps_processed,
            "urls_discovered": self.stats.urls_discovered,
            "urls_filtered": self.stats.urls_filtered,
            "xml_parse_errors": self.stats.xml_parse_errors,
            "http_errors": self.stats.http_errors,
            "duplicate_urls": self.stats.duplicate_urls,
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on sitemap parser"""
        try:
            # Test HTTP client health
            http_health = await self.http_client.health_check()

            return {
                "status": "healthy" if http_health.get("status") == "healthy" else "degraded",
                "http_client_status": http_health.get("status", "unknown"),
                "stats": self.get_stats(),
            }

        except Exception as e:
            logger.error(f"Sitemap parser health check failed: {e}")
            return {"status": "unhealthy", "error": str(e), "stats": self.get_stats()}

    async def close(self):
        """Close HTTP client and cleanup resources"""
        if self.http_client:
            await self.http_client.close()
            logger.info("Sitemap parser closed")


if __name__ == "__main__":
    # CLI utility for testing sitemap parsing
    import asyncio
    import sys

    from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 3:
            print("Usage: python sitemap_parser.py [discover|parse] <domain_or_url>")
            sys.exit(1)

        command = sys.argv[1]
        target = sys.argv[2]

        settings = load_settings()
        parser = SitemapParser(settings)

        try:
            if command == "discover":
                print(f"Discovering sitemaps for domain: {target}")
                sitemaps = await parser.discover_sitemaps(target)
                print(f"Found {len(sitemaps)} sitemaps:")
                for sitemap in sitemaps:
                    print(f"  {sitemap}")

            elif command == "parse":
                print(f"Parsing sitemap: {target}")
                urls = await parser.parse_sitemap_xml(target)
                print(f"Found {len(urls)} URLs:")
                for i, url_info in enumerate(urls[:10]):  # Show first 10
                    print(f"  {i + 1}. {url_info.url}")
                if len(urls) > 10:
                    print(f"  ... and {len(urls) - 10} more")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

            print("\nStatistics:")
            stats = parser.get_stats()
            for key, value in stats.items():
                print(f"  {key}: {value}")

        finally:
            await parser.close()

    asyncio.run(main())
