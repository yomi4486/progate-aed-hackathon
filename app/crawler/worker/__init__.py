"""
Crawler worker components for distributed web crawling.

This package provides the main worker implementation and supporting components
for the distributed crawler system, including:

- CrawlerWorker: Main worker that processes SQS messages and coordinates crawling
- ConcurrentCrawlManager: Manages concurrent crawling operations with resource limits
- CrawlErrorHandler: Handles errors and implements retry logic with exponential backoff

The worker integrates with all other crawler components including state management,
rate limiting, HTTP client, and queue management to provide a complete crawling
solution.
"""

from .concurrent_manager import ConcurrentCrawlManager
from .crawler_worker import CrawlerWorker
from .error_handler import CrawlErrorHandler

__all__ = [
    "CrawlerWorker",
    "ConcurrentCrawlManager",
    "CrawlErrorHandler",
]
