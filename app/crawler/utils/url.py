"""
URL utilities for the distributed crawler.

Provides URL normalization, hashing, validation, and domain extraction.
"""

import hashlib
from typing import Optional, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from pydantic import HttpUrl, ValidationError


def normalize_url(url: str) -> str:
    """
    Normalize URL for consistent processing.

    This function:
    - Converts to lowercase (except path)
    - Removes fragment (#)
    - Sorts query parameters
    - Removes default ports (80, 443)
    - Removes trailing slash for paths
    - Converts punycode domains to unicode

    Args:
        url: Raw URL string

    Returns:
        Normalized URL string

    Raises:
        ValueError: If URL is malformed
    """

    # Parse the URL
    try:
        parsed = urlparse(url.strip())
    except Exception as e:
        raise ValueError(f"Invalid URL format: {e}")

    # Validate scheme
    if not parsed.scheme:
        raise ValueError("URL must include scheme (http/https)")

    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"Unsupported scheme: {scheme}")

    # Normalize netloc (domain and port)
    netloc = parsed.netloc.lower()
    if netloc == "":
        raise ValueError("URL must include domain")

    # Remove default ports
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    # Normalize path
    path = parsed.path
    if not path:
        path = "/"
    elif path != "/" and path.endswith("/"):
        # Remove trailing slash except for root
        path = path.rstrip("/")

    # Normalize query parameters
    query = ""
    if parsed.query:
        try:
            # Parse and sort query parameters
            query_params = parse_qs(parsed.query, keep_blank_values=True)
            sorted_params: list[tuple[str, str]] = []
            for key in sorted(query_params.keys()):
                for value in sorted(query_params[key]):
                    sorted_params.append((key, value))
            query = urlencode(sorted_params)
        except Exception:
            # If query parsing fails, keep original
            query = parsed.query

    # Reconstruct URL without fragment
    normalized = urlunparse((scheme, netloc, path, parsed.params, query, ""))

    return normalized


def generate_url_hash(url: str) -> str:
    """
    Generate a consistent hash for a URL.

    Uses SHA-256 hash of the normalized URL to create a unique identifier.

    Args:
        url: URL to hash

    Returns:
        Hexadecimal hash string (64 characters)
    """
    try:
        normalized = normalize_url(url)
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    except Exception:
        # Fallback: hash the original URL if normalization fails
        return hashlib.sha256(url.encode("utf-8")).hexdigest()


def extract_domain(url: str) -> str:
    """
    Extract domain from URL.

    Args:
        url: URL string

    Returns:
        Domain name (without port)

    Raises:
        ValueError: If URL is invalid or has no domain
    """
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError("URL has no domain")

        # Remove port if present
        domain = parsed.netloc.lower().split(":")[0]
        if not domain:
            raise ValueError("Invalid domain in URL")

        return domain
    except Exception as e:
        raise ValueError(f"Cannot extract domain from URL '{url}': {e}")


def extract_domain_and_port(url: str) -> Tuple[str, Optional[int]]:
    """
    Extract domain and port from URL.

    Args:
        url: URL string

    Returns:
        Tuple of (domain, port). Port is None if not specified.

    Raises:
        ValueError: If URL is invalid
    """
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError("URL has no domain")

        netloc = parsed.netloc.lower()
        if ":" in netloc:
            domain, port_str = netloc.rsplit(":", 1)
            try:
                port = int(port_str)
                return domain, port
            except ValueError:
                # Port is not a number, treat as part of domain
                return netloc, None
        else:
            return netloc, None
    except Exception as e:
        raise ValueError(f"Cannot extract domain and port from URL '{url}': {e}")


def is_valid_url(url: str) -> bool:
    """
    Check if URL is valid for crawling.

    Args:
        url: URL string to validate

    Returns:
        True if URL is valid, False otherwise
    """
    if not url or not isinstance(url, str):  # type: ignore
        return False

    try:
        # Use Pydantic's HttpUrl for validation
        HttpUrl(url)

        # Additional checks
        parsed = urlparse(url)

        # Must have http or https scheme
        if parsed.scheme not in ("http", "https"):
            return False

        # Must have domain
        if not parsed.netloc:
            return False

        # Domain should not be IP address (basic check)
        domain = extract_domain(url)
        if _is_ip_address(domain):
            # Allow IP addresses for development
            pass

        return True

    except (ValidationError, ValueError):
        return False


def is_same_domain(url1: str, url2: str) -> bool:
    """
    Check if two URLs belong to the same domain.

    Args:
        url1: First URL
        url2: Second URL

    Returns:
        True if domains match, False otherwise
    """
    try:
        domain1 = extract_domain(url1)
        domain2 = extract_domain(url2)
        return domain1 == domain2
    except ValueError:
        return False


def resolve_relative_url(base_url: str, relative_url: str) -> str:
    """
    Resolve relative URL against base URL.

    Args:
        base_url: Base URL
        relative_url: Relative URL or absolute URL

    Returns:
        Absolute URL

    Raises:
        ValueError: If resolution fails
    """
    try:
        resolved = urljoin(base_url, relative_url)
        # Validate the resolved URL
        if is_valid_url(resolved):
            return resolved
        else:
            raise ValueError(f"Resolved URL is invalid: {resolved}")
    except Exception as e:
        raise ValueError(f"Cannot resolve relative URL '{relative_url}' against '{base_url}': {e}")


def get_url_depth(url: str) -> int:
    """
    Get the depth of URL path (number of path segments).

    Args:
        url: URL string

    Returns:
        Number of path segments (0 for root)
    """
    try:
        parsed = urlparse(url)
        path = parsed.path.strip("/")
        if not path:
            return 0
        return len(path.split("/"))
    except Exception:
        return 0


def is_crawlable_file_type(url: str) -> bool:
    """
    Check if URL points to a crawlable file type.

    Args:
        url: URL string

    Returns:
        True if file type is crawlable (HTML, text, etc.)
    """
    # Extract file extension from path
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()

        # If no extension, assume it's a page
        if "." not in path.split("/")[-1]:
            return True

        # Define crawlable extensions
        crawlable_extensions = {
            ".html",
            ".htm",
            ".shtml",
            ".xhtml",
            ".php",
            ".asp",
            ".aspx",
            ".jsp",
            ".cfm",
            ".txt",
            ".xml",
            ".rss",
            ".atom",
            "",  # No extension
        }

        # Non-crawlable extensions
        non_crawlable_extensions = {
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".svg",
            ".webp",
            ".mp3",
            ".mp4",
            ".avi",
            ".mov",
            ".wmv",
            ".flv",
            ".zip",
            ".rar",
            ".tar",
            ".gz",
            ".7z",
            ".exe",
            ".dmg",
            ".pkg",
            ".deb",
            ".rpm",
            ".css",
            ".js",
            ".json",
            ".csv",
            ".sql",
        }

        # Get file extension
        if "." in path:
            extension = "." + path.split(".")[-1]

            if extension in non_crawlable_extensions:
                return False
            elif extension in crawlable_extensions:
                return True

        # Default to crawlable
        return True

    except Exception:
        return True  # Default to crawlable if parsing fails


def _is_ip_address(hostname: str) -> bool:
    """Check if hostname is an IP address (basic check)"""
    import socket

    try:
        socket.inet_aton(hostname)
        return True
    except socket.error:
        try:
            socket.inet_pton(socket.AF_INET6, hostname)
            return True
        except socket.error:
            return False


def get_robots_txt_url(domain: str, scheme: str = "https") -> str:
    """
    Get the robots.txt URL for a domain.

    Args:
        domain: Domain name
        scheme: URL scheme (http or https)

    Returns:
        robots.txt URL
    """
    if "://" in domain:
        # Domain already includes scheme
        parsed = urlparse(domain)
        scheme = parsed.scheme
        domain = parsed.netloc

    return f"{scheme}://{domain}/robots.txt"


def get_sitemap_urls(domain: str, scheme: str = "https") -> list[str]:
    """
    Get common sitemap URLs for a domain.

    Args:
        domain: Domain name
        scheme: URL scheme

    Returns:
        List of potential sitemap URLs
    """
    if "://" in domain:
        parsed = urlparse(domain)
        scheme = parsed.scheme
        domain = parsed.netloc

    base_url = f"{scheme}://{domain}"

    return [
        f"{base_url}/sitemap.xml",
        f"{base_url}/sitemap_index.xml",
        f"{base_url}/sitemaps.xml",
        f"{base_url}/sitemap/sitemap.xml",
        f"{base_url}/sitemap/index.xml",
    ]
