"""
Logging utilities for the distributed crawler.

Provides structured logging with JSON output for better observability.
"""

import logging
import sys
from typing import Any

import structlog


def setup_crawler_logger(name: str, level: str = "INFO", json_logs: bool = True) -> structlog.BoundLogger:
    """
    Set up structured logging for the crawler.

    Args:
        name: Logger name
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        json_logs: Whether to output JSON format logs

    Returns:
        Configured structlog logger
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Configure structlog
    if json_logs:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.dev.ConsoleRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(name)


def log_crawl_event(logger: structlog.BoundLogger, event_type: str, url: str, **kwargs: Any) -> None:
    """
    Log a crawling event with structured data.

    Args:
        logger: Structured logger instance
        event_type: Type of event (crawl_started, crawl_completed, etc.)
        url: URL being processed
        **kwargs: Additional event data
    """
    event_data = {"event_type": event_type, "url": url, **kwargs}

    if event_type.endswith("_error") or event_type.endswith("_failed"):
        logger.error(event_type, **event_data)
    elif event_type.endswith("_warning"):
        logger.warning(event_type, **event_data)
    else:
        logger.info(event_type, **event_data)


def get_crawler_logger(name: str) -> structlog.BoundLogger:
    """
    Get a logger instance with the given name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return structlog.get_logger(name)


class CrawlerLoggerAdapter:
    """
    Logger adapter that adds crawler context to all log messages.
    """

    def __init__(self, logger: structlog.BoundLogger, crawler_id: str):
        self.logger = logger.bind(crawler_id=crawler_id)
        self.crawler_id = crawler_id

    def log_crawl_started(self, url: str, domain: str, **kwargs: Any) -> None:
        """Log crawl start event"""
        self.logger.info("crawl_started", url=url, domain=domain, **kwargs)

    def log_crawl_completed(
        self, url: str, domain: str, status_code: int, response_time_ms: float, content_length: int, **kwargs: Any
    ) -> None:
        """Log crawl completion event"""
        self.logger.info(
            "crawl_completed",
            url=url,
            domain=domain,
            status_code=status_code,
            response_time_ms=response_time_ms,
            content_length=content_length,
            **kwargs,
        )

    def log_crawl_failed(self, url: str, domain: str, error_type: str, error_message: str, **kwargs: Any) -> None:
        """Log crawl failure event"""
        self.logger.error(
            "crawl_failed", url=url, domain=domain, error_type=error_type, error_message=error_message, **kwargs
        )

    def log_lock_acquired(self, url_hash: str, domain: str, **kwargs: Any) -> None:
        """Log lock acquisition event"""
        self.logger.debug("lock_acquired", url_hash=url_hash, domain=domain, **kwargs)

    def log_lock_failed(self, url_hash: str, domain: str, **kwargs: Any) -> None:
        """Log lock acquisition failure"""
        self.logger.debug("lock_failed", url_hash=url_hash, domain=domain, **kwargs)

    def log_rate_limited(self, domain: str, wait_time: float, **kwargs: Any) -> None:
        """Log rate limiting event"""
        self.logger.warning("rate_limited", domain=domain, wait_time=wait_time, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message"""
        self.logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message"""
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message"""
        self.logger.error(message, **kwargs)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message"""
        self.logger.debug(message, **kwargs)
