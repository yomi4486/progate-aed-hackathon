"""
HTML content parsing and text extraction for the distributed crawler.

Provides comprehensive HTML parsing with metadata extraction, text cleaning,
language detection, and structured content processing.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString
from pydantic import HttpUrl

try:
    import langdetect
    from langdetect import detect_langs  # type: ignore
    from langdetect.lang_detect_exception import LangDetectException
except ImportError:
    langdetect = None
    LangDetectException = Exception

from ...schema.common import Lang
from ...schema.crawl import ParsedContent
from ..config.settings import CrawlerSettings, get_cached_settings
from ..utils.url import extract_domain

logger = logging.getLogger(__name__)


class ParseError(Exception):
    """Base exception for parsing errors"""

    pass


class ContentParser:
    """
    HTML content parser with comprehensive text extraction, metadata processing,
    and language detection capabilities.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None):
        """
        Initialize the content parser.

        Args:
            settings: Optional crawler settings (uses cached settings if None)
        """
        self.settings = settings or get_cached_settings()

        # Statistics tracking
        self.stats = {
            "documents_parsed": 0,
            "successful_parses": 0,
            "failed_parses": 0,
            "language_detections": 0,
            "metadata_extractions": 0,
            "text_extractions": 0,
        }

        logger.info("Initialized content parser")

    async def parse_html_content(self, html_content: bytes, url: str, content_type: str = "") -> ParsedContent:
        """
        Parse HTML content into structured ParsedContent.

        Args:
            html_content: Raw HTML content as bytes
            url: Source URL of the content
            content_type: Content type header

        Returns:
            ParsedContent with extracted text and metadata

        Raises:
            ParseError: If parsing fails
        """
        try:
            self.stats["documents_parsed"] += 1

            # Decode HTML content
            html_text = self._decode_html_content(html_content, content_type)

            # Parse with BeautifulSoup
            soup = BeautifulSoup(html_text, "html.parser")

            # Extract main text content
            body_text = await self._extract_text_content(soup, url)

            # Extract metadata
            metadata = await self._extract_metadata(soup, url)

            # Detect language
            detected_lang = await self._detect_language(body_text)

            # Extract title and description from metadata
            title = metadata.get("title")
            description = metadata.get("description")

            # Extract publish date if available
            published_at = self._parse_published_date(metadata)

            # Create ParsedContent result
            result = ParsedContent(
                url=HttpUrl(url),
                title=title,
                description=description,
                body_text=body_text,
                lang=detected_lang,
                published_at=published_at,
                metadata=metadata,
                parsed_s3_key="",  # Will be populated by storage layer
            )

            self.stats["successful_parses"] += 1
            self.stats["text_extractions"] += 1
            self.stats["metadata_extractions"] += 1
            self.stats["language_detections"] += 1

            logger.debug(
                f"Successfully parsed content from {url}",
                extra={
                    "url": url,
                    "title": title,
                    "body_length": len(body_text),
                    "language": detected_lang,
                    "metadata_keys": len(metadata),
                },
            )

            return result

        except Exception as e:
            self.stats["failed_parses"] += 1
            logger.error(f"Error parsing content from {url}: {e}")
            raise ParseError(f"Failed to parse HTML content: {str(e)}") from e

    def _decode_html_content(self, html_content: bytes, content_type: str) -> str:
        """
        Decode HTML content to text with proper encoding detection.

        Args:
            html_content: Raw HTML bytes
            content_type: Content-Type header value

        Returns:
            Decoded HTML text
        """
        # Extract charset from content-type header
        encoding = "utf-8"  # Default encoding

        if content_type:
            charset_match = re.search(r"charset=([^;]+)", content_type.lower())
            if charset_match:
                encoding = charset_match.group(1).strip()

        # Try to decode with detected/specified encoding
        try:
            return html_content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            # Fallback to common encodings
            for fallback_encoding in ["utf-8", "iso-8859-1", "cp1252"]:
                try:
                    return html_content.decode(fallback_encoding)
                except (UnicodeDecodeError, LookupError):
                    continue

            # Last resort: decode with errors ignored
            return html_content.decode("utf-8", errors="ignore")

    async def _extract_text_content(self, soup: BeautifulSoup, url: str) -> str:
        """
        Extract clean text content from HTML.

        Args:
            soup: BeautifulSoup parsed HTML
            url: Source URL for context

        Returns:
            Cleaned text content
        """
        # Remove unwanted elements
        unwanted_tags = [
            "script",
            "style",
            "nav",
            "header",
            "footer",
            "aside",
            "advertisement",
            "ads",
            "sidebar",
            "menu",
            "form",
            "button",
            "input",
            "select",
            "textarea",
        ]

        for tag_name in unwanted_tags:
            for element in soup.find_all(tag_name):
                element.decompose()

        # Remove elements by class/id (common patterns)
        unwanted_patterns = [
            "nav",
            "menu",
            "sidebar",
            "footer",
            "header",
            "advertisement",
            "ads",
            "social",
            "share",
            "comment",
            "popup",
            "modal",
            "breadcrumb",
            "pagination",
            "tags",
        ]

        for pattern in unwanted_patterns:
            # Remove by class
            for element in soup.find_all(class_=re.compile(pattern, re.I)):
                element.decompose()
            # Remove by ID
            for element in soup.find_all(id=re.compile(pattern, re.I)):
                element.decompose()

        # Try to find main content areas
        main_content = None
        content_selectors = [
            "main",
            "article",
            '[role="main"]',
            ".content",
            ".main-content",
            ".post-content",
            ".entry-content",
            ".article-content",
            ".page-content",
        ]

        for selector in content_selectors:
            element = soup.select_one(selector)
            if element and len(element.get_text().strip()) > 100:
                main_content = element
                break

        # If no main content found, use body
        if main_content is None:
            main_content = soup.find("body") or soup

        # Extract text with proper spacing
        text_parts: list[str] = []
        if isinstance(main_content, Tag):
            for element in main_content.descendants:
                if isinstance(element, NavigableString):
                    text = element.strip()
                    if text:
                        text_parts.append(text)
                elif isinstance(element, Tag):
                    # Add space after block elements
                    if element.name in ["p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "br"]:
                        text_parts.append("\n")

        # Join and clean up text
        full_text = " ".join(text_parts)

        # Clean up whitespace and normalize
        full_text = re.sub(r"\s+", " ", full_text)  # Multiple spaces to single
        full_text = re.sub(r"\n\s*\n", "\n\n", full_text)  # Multiple newlines to double
        full_text = full_text.strip()

        return full_text

    async def _extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Extract metadata from HTML document.

        Args:
            soup: BeautifulSoup parsed HTML
            url: Source URL for context

        Returns:
            Dictionary of extracted metadata
        """
        metadata: Dict[str, Any] = {}

        # Basic document metadata
        metadata["url"] = url
        metadata["domain"] = extract_domain(url)
        metadata["parsed_at"] = datetime.now(timezone.utc).isoformat()

        # Title extraction (multiple sources)
        title = None
        # Try meta og:title first
        og_title = soup.find("meta", property="og:title")
        if isinstance(og_title, Tag) and og_title.get("content"):
            title = og_title["content"]
        # Try regular title tag
        elif soup.title and soup.title.string:
            title = soup.title.string.strip()
        # Try h1 as fallback
        elif soup.h1 and soup.h1.get_text():
            title = soup.h1.get_text().strip()

        if title:
            metadata["title"] = title

        # Description extraction
        description = None
        # Try meta description
        desc_meta = soup.find("meta", attrs={"name": "description"})
        if isinstance(desc_meta, Tag) and desc_meta.get("content"):
            description = desc_meta["content"]
        # Try og:description
        elif soup.find("meta", property="og:description"):
            og_desc = soup.find("meta", property="og:description")
            if isinstance(og_desc, Tag) and og_desc.get("content"):
                description = og_desc["content"]

        if description:
            metadata["description"] = description

        # Extract Open Graph metadata
        og_metadata = {}
        for og_tag in soup.find_all("meta", property=re.compile(r"^og:")):
            property_name = og_tag.get("property") if isinstance(og_tag, Tag) else None
            content = og_tag.get("content") if isinstance(og_tag, Tag) else None
            if property_name and content:
                og_metadata[property_name] = content
        if og_metadata:
            metadata["opengraph"] = og_metadata

        # Extract Twitter Card metadata
        twitter_metadata = {}
        for twitter_tag in soup.find_all("meta", attrs={"name": re.compile(r"^twitter:")}):
            name = twitter_tag.get("name") if isinstance(twitter_tag, Tag) else None
            content = twitter_tag.get("content") if isinstance(twitter_tag, Tag) else None
            if name and content:
                twitter_metadata[name] = content
        if twitter_metadata:
            metadata["twitter"] = twitter_metadata

        # Extract schema.org structured data
        schema_data: list[dict[str, Any]] = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                script_string = getattr(script, "string", None)
                data: dict[str, Any] = {}  # Initialize data to avoid potential unbound error
                if script_string:
                    data = json.loads(script_string)
                schema_data.append(data)
            except (json.JSONDecodeError, AttributeError):
                continue
        if schema_data:
            metadata["schema"] = schema_data

        # Language detection from HTML
        lang_attr = soup.find("html", lang=True)
        if lang_attr:
            metadata["html_lang"] = (
                lang_attr["lang"] if isinstance(lang_attr, Tag) and "lang" in lang_attr.attrs else None
            )

        # Author information
        author = None
        author_meta = soup.find("meta", attrs={"name": "author"})
        if isinstance(author_meta, Tag) and author_meta.get("content"):
            author = author_meta["content"]
        elif soup.find("meta", property="article:author"):
            author_og = soup.find("meta", property="article:author")
            if isinstance(author_og, Tag) and author_og.get("content"):
                author = author_og["content"]

        if author:
            metadata["author"] = author

        # Publication date
        pub_dates: list[str] = []
        # Try various date meta tags
        date_selectors: list[str] = [
            'meta[name="date"]',
            'meta[name="publish-date"]',
            'meta[name="publication-date"]',
            'meta[property="article:published_time"]',
            'meta[property="article:modified_time"]',
            "time[datetime]",
            "time[pubdate]",
        ]

        for selector in date_selectors:
            elements = soup.select(selector)
            for element in elements:
                date_str = element.get("content") or element.get("datetime")
                if date_str:
                    pub_dates.append(str(date_str))

        if pub_dates:
            metadata["publication_dates"] = pub_dates

        # Word count and reading time estimation
        text_content = soup.get_text()
        words = len(text_content.split())
        metadata["word_count"] = words
        metadata["estimated_reading_time_minutes"] = max(1, words // 200)  # ~200 words per minute

        # Extract links
        links: list[dict[str, str]] = []
        for link in soup.find_all("a", href=True):
            href = link["href"] if isinstance(link, Tag) else None
            # Resolve relative URLs
            if isinstance(href, str) and href.startswith(("http://", "https://")):
                absolute_url = href
            else:
                absolute_url = urljoin(url, str(href))

            link_text = link.get_text().strip()
            links.append(
                {
                    "url": absolute_url,
                    "text": link_text,
                    **(
                        {"domain": extract_domain(absolute_url)}
                        if absolute_url.startswith(("http://", "https://"))
                        else {}
                    ),
                }
            )

        metadata["links"] = links[:50]  # Limit to first 50 links to avoid bloat
        metadata["total_links"] = len(links)

        # Extract images
        images: list[dict[str, str]] = []
        for img in soup.find_all("img", src=True):
            src = img["src"] if isinstance(img, Tag) and "src" in img.attrs else None
            # Resolve relative URLs
            if isinstance(src, str) and src.startswith(("http://", "https://")):
                absolute_url = src
            else:
                absolute_url = urljoin(url, str(src))

            if isinstance(img, Tag):
                images.append({"url": absolute_url, "alt": str(img.get("alt", "")), "title": str(img.get("title", ""))})

        metadata["images"] = images[:20]  # Limit to first 20 images
        metadata["total_images"] = len(images)

        return metadata

    async def _detect_language(self, text: str) -> Optional[Lang]:
        """
        Detect the language of text content.

        Args:
            text: Text to analyze

        Returns:
            Detected language code or None if detection fails
        """
        if not langdetect or not text.strip():
            return self.settings.default_language

        try:
            # Use only first 10k characters for language detection (performance)
            sample_text = text[:10000]

            # Get language probabilities
            if not detect_langs:  # type: ignore
                raise ParseError("Language detection is unavailable. Ensure 'langdetect' is installed.")
            lang_probs = detect_langs(sample_text)

            if lang_probs:
                # Get the most likely language
                top_lang = lang_probs[0]

                # Only accept if confidence is above threshold
                if (
                    hasattr(top_lang, "prob")
                    and isinstance(getattr(top_lang, "prob", None), (int, float))
                    and getattr(top_lang, "prob", 0) >= self.settings.language_detection_confidence
                ):
                    # Map langdetect codes to our Lang enum values
                    lang_code = getattr(top_lang, "lang", None)
                    if not lang_code:
                        raise ParseError(
                            "Language detection failed: 'lang' attribute is missing in the detected language object."
                        )

                    # Convert common langdetect codes to our Lang enum
                    lang_mapping = {
                        "ja": "ja",
                        "en": "en",
                        "zh-cn": "zh",
                        "zh-tw": "zh",
                        "ko": "ko",
                        "es": "es",
                        "fr": "fr",
                        "de": "de",
                        "it": "it",
                        "pt": "pt",
                        "ru": "ru",
                        "ar": "ar",
                        "hi": "hi",
                    }

                    mapped_lang = lang_mapping.get(lang_code)
                    if mapped_lang:
                        logger.debug(
                            f"Detected language: {mapped_lang} (confidence: {getattr(top_lang, 'prob', 0):.2f})"
                        )
                        if isinstance(Lang, type) and hasattr(Lang, mapped_lang):
                            return getattr(Lang, mapped_lang)
                        logger.warning(f"Invalid language mapping: {mapped_lang}")
                        return None

            # Fallback to default if detection failed or confidence too low
            logger.debug(
                f"Language detection failed or low confidence, using default: {self.settings.default_language}"
            )
            return self.settings.default_language

        except LangDetectException as e:
            logger.warning(f"Language detection error: {e}")
            return self.settings.default_language
        except Exception as e:
            logger.error(f"Unexpected error in language detection: {e}")
            return self.settings.default_language

    def _parse_published_date(self, metadata: Dict[str, Any]) -> Optional[datetime]:
        """
        Parse publication date from metadata.

        Args:
            metadata: Extracted metadata dictionary

        Returns:
            Parsed datetime or None if parsing fails
        """
        pub_dates = metadata.get("publication_dates", [])
        if not pub_dates:
            return None

        # Try to parse the first available date
        for date_str in pub_dates:
            try:
                # Handle various date formats
                date_formats = [
                    "%Y-%m-%dT%H:%M:%S%z",  # ISO format with timezone
                    "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO format with microseconds
                    "%Y-%m-%dT%H:%M:%SZ",  # ISO format UTC
                    "%Y-%m-%d %H:%M:%S",  # Standard datetime
                    "%Y-%m-%d",  # Date only
                    "%d/%m/%Y",  # DD/MM/YYYY
                    "%m/%d/%Y",  # MM/DD/YYYY
                ]

                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        # Ensure timezone awareness
                        if parsed_date.tzinfo is None:
                            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                        return parsed_date
                    except ValueError:
                        continue

            except Exception as e:
                logger.debug(f"Failed to parse date '{date_str}': {e}")
                continue

        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get content parser statistics"""
        stats = self.stats.copy()

        # Calculate derived metrics
        if stats["documents_parsed"] > 0:
            stats["success_rate"] = int(stats["successful_parses"] / stats["documents_parsed"])
            stats["failure_rate"] = int(stats["failed_parses"] / stats["documents_parsed"])
        else:
            stats["success_rate"] = 0
            stats["failure_rate"] = 0

        # Language detection availability
        stats["language_detection_available"] = langdetect is not None

        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on content parser"""
        try:
            # Test basic parsing with a minimal HTML document
            test_html = b"<html><head><title>Test</title></head><body><p>Test content</p></body></html>"
            test_url = "https://example.com/test"

            _ = await self.parse_html_content(test_html, test_url)

            return {
                "status": "healthy",
                "test_parse_successful": True,
                "language_detection_available": langdetect is not None,
                "documents_processed": self.stats["documents_parsed"],
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "language_detection_available": langdetect is not None,
            }


# Global parser instance
_parser: Optional[ContentParser] = None


def get_content_parser(settings: Optional[CrawlerSettings] = None) -> ContentParser:
    """
    Get the global content parser instance.

    Args:
        settings: Optional settings override

    Returns:
        Content parser instance
    """
    global _parser

    if _parser is None or settings is not None:
        if settings is None:
            settings = get_cached_settings()
        _parser = ContentParser(settings)

    return _parser


def reset_parser() -> None:
    """Reset the global parser instance (useful for testing)"""
    global _parser
    _parser = None


if __name__ == "__main__":
    # CLI utility for testing content parser functionality
    import asyncio
    import sys

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python parser.py [health|stats|test] [html_file]")
            sys.exit(1)

        command = sys.argv[1]

        # Initialize parser
        parser = get_content_parser()

        try:
            if command == "health":
                health = await parser.health_check()
                print(f"Health status: {health}")

            elif command == "stats":
                stats = parser.get_stats()
                print(f"Parser stats: {stats}")

            elif command == "test" and len(sys.argv) > 2:
                html_file = sys.argv[2]
                test_url = "https://example.com/test"

                print(f"Testing content parser with HTML file: {html_file}")

                try:
                    with open(html_file, "rb") as f:
                        html_content = f.read()

                    result = await parser.parse_html_content(html_content, test_url)
                    print("Parse successful!")
                    print(f"Title: {result.title}")
                    print(f"Language: {result.lang}")
                    print(f"Body length: {len(result.body_text)} chars")
                    print(f"Metadata keys: {len(result.metadata)}")

                except FileNotFoundError:
                    print(f"HTML file not found: {html_file}")
                except Exception as e:
                    print(f"Parse error: {e}")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    asyncio.run(main())
