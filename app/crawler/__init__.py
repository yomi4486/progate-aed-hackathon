"""
Distributed Web Crawler for AED Hackathon

A distributed crawler system that uses DynamoDB for state management,
Redis for rate limiting, and SQS for task distribution.
"""

from .utils.logging import setup_crawler_logger

__version__ = "0.1.0"
__all__ = ["setup_crawler_logger"]

# Default logger setup
logger = setup_crawler_logger("crawler")
