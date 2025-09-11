"""
Error handling and retry logic for the distributed crawler worker.

Provides comprehensive error classification, retry decision logic, and exponential
backoff calculations with support for different error types and configurable
retry strategies.
"""

import asyncio
import logging
import random
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import ClientError, ServerTimeoutError
from pydantic import BaseModel

from ..core.types import CrawlErrorType, URLStateEnum
from ..http_client.client import ContentTooLargeError, CrawlError, HTTPError, RateLimitExceededError, RobotsBlockedError

logger = logging.getLogger(__name__)


class RetryDecision(BaseModel):
    """Decision about whether to retry a failed operation"""

    should_retry: bool
    delay_seconds: int
    new_state: URLStateEnum
    error_type: CrawlErrorType
    reason: str


class ErrorClassification(BaseModel):
    """Classification of an error for handling purposes"""

    error_type: CrawlErrorType
    is_retryable: bool
    is_permanent: bool
    suggested_delay: int
    description: str


class CrawlErrorHandler:
    """
    Comprehensive error handler for crawler operations.

    Provides error classification, retry decision logic, and exponential
    backoff calculations with configurable retry strategies based on
    error types and attempt counts.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_backoff_seconds: int = 60,
        max_backoff_seconds: int = 3600,
        backoff_multiplier: float = 2.0,
        jitter_factor: float = 0.1,
    ):
        """
        Initialize the error handler.

        Args:
            max_retries: Maximum number of retry attempts
            base_backoff_seconds: Base delay for exponential backoff
            max_backoff_seconds: Maximum delay for exponential backoff
            backoff_multiplier: Multiplier for exponential backoff
            jitter_factor: Factor for adding randomness to backoff delays
        """
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.backoff_multiplier = backoff_multiplier
        self.jitter_factor = jitter_factor

        # Statistics tracking
        self.stats: Dict[str, Any] = {
            "errors_handled": 0,
            "retries_scheduled": 0,
            "permanent_failures": 0,
            "errors_by_type": {},
        }

        # Error type configuration
        self._error_configs = self._initialize_error_configs()

        logger.info(
            f"Initialized error handler with max_retries={max_retries}, "
            f"base_backoff={base_backoff_seconds}s, max_backoff={max_backoff_seconds}s"
        )

    def _initialize_error_configs(self) -> Dict[CrawlErrorType, Dict[str, Any]]:
        """Initialize configuration for different error types"""
        return {
            # Network and connectivity errors - generally retryable
            CrawlErrorType.CONNECTION_ERROR: {
                "retryable": True,
                "permanent": False,
                "base_delay_multiplier": 1.0,
                "max_retries_override": None,
            },
            CrawlErrorType.TIMEOUT: {
                "retryable": True,
                "permanent": False,
                "base_delay_multiplier": 1.5,
                "max_retries_override": None,
            },
            # HTTP errors - depends on status code
            CrawlErrorType.HTTP_ERROR: {
                "retryable": True,  # Depends on status code
                "permanent": False,
                "base_delay_multiplier": 1.0,
                "max_retries_override": None,
            },
            # Rate limiting - retryable with longer delay
            CrawlErrorType.RATE_LIMITED: {
                "retryable": True,
                "permanent": False,
                "base_delay_multiplier": 2.0,
                "max_retries_override": 5,
            },
            # Robots.txt blocking - permanent failure
            CrawlErrorType.ROBOTS_BLOCKED: {
                "retryable": False,
                "permanent": True,
                "base_delay_multiplier": 1.0,
                "max_retries_override": 0,
            },
            # Parse errors - generally not retryable
            CrawlErrorType.PARSE_ERROR: {
                "retryable": False,
                "permanent": True,
                "base_delay_multiplier": 1.0,
                "max_retries_override": 1,  # Maybe retry once in case it was transient
            },
            # Unknown errors - retry with caution
            CrawlErrorType.UNKNOWN: {
                "retryable": True,
                "permanent": False,
                "base_delay_multiplier": 2.0,
                "max_retries_override": 2,
            },
        }

    async def handle_crawl_error(
        self, error: Exception, url: str, retry_count: int, domain: Optional[str] = None
    ) -> RetryDecision:
        """
        Handle a crawling error and determine retry strategy.

        Args:
            error: The exception that occurred
            url: URL that failed
            retry_count: Current retry count
            domain: Domain of the URL (for logging)

        Returns:
            RetryDecision with retry strategy
        """
        self.stats["errors_handled"] += 1

        # Classify the error
        classification = self.classify_error(error)

        # Update error type statistics
        error_type_str = classification.error_type.value
        errors_by_type = self.stats["errors_by_type"]  # type: ignore
        if error_type_str not in errors_by_type:
            errors_by_type[error_type_str] = 0
        errors_by_type[error_type_str] += 1

        # Determine if retry should be attempted
        should_retry = await self.should_retry(error, retry_count, classification)

        if should_retry:
            # Calculate backoff delay
            delay_seconds = self.calculate_backoff_delay(retry_count, classification)

            self.stats["retries_scheduled"] += 1

            decision = RetryDecision(
                should_retry=True,
                delay_seconds=delay_seconds,
                new_state=URLStateEnum.FAILED,  # Will be retried later
                error_type=classification.error_type,
                reason=f"Retryable error (attempt {retry_count + 1}): {classification.description}",
            )

            logger.info(
                f"Scheduling retry for {url}",
                extra={
                    "url": url,
                    "domain": domain,
                    "error_type": classification.error_type.value,
                    "retry_count": retry_count + 1,
                    "delay_seconds": delay_seconds,
                    "reason": str(error),
                },
            )

        else:
            # Permanent failure or max retries exceeded
            self.stats["permanent_failures"] += 1

            decision = RetryDecision(
                should_retry=False,
                delay_seconds=0,
                new_state=URLStateEnum.FAILED,
                error_type=classification.error_type,
                reason=f"Permanent failure: {classification.description}",
            )

            logger.warning(
                f"Marking {url} as permanently failed",
                extra={
                    "url": url,
                    "domain": domain,
                    "error_type": classification.error_type.value,
                    "retry_count": retry_count,
                    "reason": str(error),
                    "is_permanent": classification.is_permanent,
                    "max_retries_exceeded": retry_count >= self.max_retries,
                },
            )

        return decision

    def classify_error(self, error: Exception) -> ErrorClassification:
        """
        Classify an error to determine appropriate handling strategy.

        Args:
            error: Exception to classify

        Returns:
            ErrorClassification with handling details
        """
        # Handle CrawlError types (our custom exceptions)
        if isinstance(error, CrawlError):
            return self._classify_crawl_error(error)

        # Handle standard HTTP/network exceptions
        if isinstance(error, (ClientError, ConnectionError, OSError)):
            return ErrorClassification(
                error_type=CrawlErrorType.CONNECTION_ERROR,
                is_retryable=True,
                is_permanent=False,
                suggested_delay=self.base_backoff_seconds,
                description=f"Network/connection error: {str(error)}",
            )

        if isinstance(error, (TimeoutError, ServerTimeoutError, asyncio.TimeoutError)):
            return ErrorClassification(
                error_type=CrawlErrorType.TIMEOUT,
                is_retryable=True,
                is_permanent=False,
                suggested_delay=int(self.base_backoff_seconds * 1.5),
                description=f"Timeout error: {str(error)}",
            )

        # Unknown error - handle conservatively
        return ErrorClassification(
            error_type=CrawlErrorType.UNKNOWN,
            is_retryable=True,
            is_permanent=False,
            suggested_delay=self.base_backoff_seconds * 2,
            description=f"Unknown error: {type(error).__name__}: {str(error)}",
        )

    def _classify_crawl_error(self, error: CrawlError) -> ErrorClassification:
        """Classify a CrawlError based on its type"""
        config = self._error_configs.get(error.error_type, {})

        # Special handling for HTTP errors based on status code
        if isinstance(error, HTTPError) and error.status_code:
            return self._classify_http_error(error)

        # Special handling for rate limiting
        if isinstance(error, RateLimitExceededError):
            suggested_delay = int(error.retry_after or (self.base_backoff_seconds * 2))
            return ErrorClassification(
                error_type=CrawlErrorType.RATE_LIMITED,
                is_retryable=True,
                is_permanent=False,
                suggested_delay=suggested_delay,
                description=f"Rate limited for domain {error.domain}",
            )

        # Special handling for robots.txt blocking
        if isinstance(error, RobotsBlockedError):
            return ErrorClassification(
                error_type=CrawlErrorType.ROBOTS_BLOCKED,
                is_retryable=False,
                is_permanent=True,
                suggested_delay=0,
                description=f"Blocked by robots.txt: {error.url}",
            )

        # Special handling for content too large
        if isinstance(error, ContentTooLargeError):
            return ErrorClassification(
                error_type=CrawlErrorType.HTTP_ERROR,
                is_retryable=False,
                is_permanent=True,
                suggested_delay=0,
                description=f"Content too large: {error.content_length} bytes",
            )

        # Default handling based on error type
        base_delay_multiplier = config.get("base_delay_multiplier", 1.0)
        suggested_delay = int(self.base_backoff_seconds * base_delay_multiplier)

        return ErrorClassification(
            error_type=error.error_type,
            is_retryable=config.get("retryable", True),
            is_permanent=config.get("permanent", False),
            suggested_delay=suggested_delay,
            description=str(error),
        )

    def _classify_http_error(self, error: HTTPError) -> ErrorClassification:
        """Classify HTTP errors based on status code"""
        status_code = error.status_code

        if not status_code:
            # No status code available
            return ErrorClassification(
                error_type=CrawlErrorType.HTTP_ERROR,
                is_retryable=True,
                is_permanent=False,
                suggested_delay=self.base_backoff_seconds,
                description=f"HTTP error without status code: {str(error)}",
            )

        # 4xx client errors - generally not retryable
        if 400 <= status_code < 500:
            if status_code == 404:
                # 404 is common and generally permanent
                return ErrorClassification(
                    error_type=CrawlErrorType.HTTP_ERROR,
                    is_retryable=False,
                    is_permanent=True,
                    suggested_delay=0,
                    description="HTTP 404 Not Found: URL does not exist",
                )
            elif status_code == 403:
                # 403 might be temporary (rate limiting) or permanent (access denied)
                return ErrorClassification(
                    error_type=CrawlErrorType.HTTP_ERROR,
                    is_retryable=True,
                    is_permanent=False,
                    suggested_delay=self.base_backoff_seconds * 2,
                    description="HTTP 403 Forbidden: Access denied (might be temporary)",
                )
            elif status_code == 429:
                # 429 is rate limiting - definitely retryable
                return ErrorClassification(
                    error_type=CrawlErrorType.RATE_LIMITED,
                    is_retryable=True,
                    is_permanent=False,
                    suggested_delay=self.base_backoff_seconds * 3,
                    description="HTTP 429 Too Many Requests: Rate limited",
                )
            else:
                # Other 4xx errors - generally permanent
                return ErrorClassification(
                    error_type=CrawlErrorType.HTTP_ERROR,
                    is_retryable=False,
                    is_permanent=True,
                    suggested_delay=0,
                    description=f"HTTP {status_code} Client Error",
                )

        # 5xx server errors - generally retryable
        if status_code >= 500:
            return ErrorClassification(
                error_type=CrawlErrorType.HTTP_ERROR,
                is_retryable=True,
                is_permanent=False,
                suggested_delay=self.base_backoff_seconds,
                description=f"HTTP {status_code} Server Error",
            )

        # Other status codes (shouldn't happen with error cases, but just in case)
        return ErrorClassification(
            error_type=CrawlErrorType.HTTP_ERROR,
            is_retryable=True,
            is_permanent=False,
            suggested_delay=self.base_backoff_seconds,
            description=f"HTTP {status_code}: {str(error)}",
        )

    async def should_retry(
        self, error: Exception, retry_count: int, classification: Optional[ErrorClassification] = None
    ) -> bool:
        """
        Determine if an error should be retried.

        Args:
            error: The exception that occurred
            retry_count: Current retry count
            classification: Optional pre-computed classification

        Returns:
            True if error should be retried, False otherwise
        """
        if classification is None:
            classification = self.classify_error(error)

        # Check if error type is retryable
        if not classification.is_retryable:
            return False

        # Check if it's a permanent failure
        if classification.is_permanent:
            return False

        # Check retry count against max retries
        error_config = self._error_configs.get(classification.error_type, {})
        max_retries = error_config.get("max_retries_override", self.max_retries)

        if retry_count >= max_retries:
            logger.debug(f"Max retries exceeded for {classification.error_type.value}: {retry_count}/{max_retries}")
            return False

        return True

    def calculate_backoff_delay(
        self, retry_count: int, classification: Optional[ErrorClassification] = None, base_delay: Optional[int] = None
    ) -> int:
        """
        Calculate exponential backoff delay with jitter.

        Args:
            retry_count: Current retry attempt (0-based)
            classification: Optional error classification for custom delays
            base_delay: Optional base delay override

        Returns:
            Delay in seconds before next retry
        """
        if base_delay is None:
            if classification:
                base_delay = classification.suggested_delay
            else:
                base_delay = self.base_backoff_seconds

        # Calculate exponential backoff
        delay = base_delay * (self.backoff_multiplier**retry_count)

        # Add jitter to avoid thundering herd
        jitter_range = delay * self.jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        delay = max(1, int(delay + jitter))

        # Cap at maximum backoff
        delay = min(delay, self.max_backoff_seconds)

        logger.debug(
            f"Calculated backoff delay: {delay}s (base={base_delay}s, retry={retry_count}, jitter={jitter:.1f}s)"
        )

        return delay

    def get_retry_schedule(self, error: Exception, max_attempts: Optional[int] = None) -> List[Tuple[int, int]]:
        """
        Get the complete retry schedule for an error.

        Args:
            error: Exception to generate schedule for
            max_attempts: Maximum attempts (uses configured max if None)

        Returns:
            List of (attempt_number, delay_seconds) tuples
        """
        classification = self.classify_error(error)
        if not classification.is_retryable:
            return []

        max_attempts = max_attempts or self.max_retries
        schedule: List[Tuple[int, int]] = []

        for attempt in range(max_attempts):
            delay = self.calculate_backoff_delay(attempt, classification)
            schedule.append((attempt + 1, delay))

        return schedule

    def get_stats(self) -> Dict[str, Any]:
        """Get error handler statistics"""
        stats: Dict[str, Any] = self.stats.copy()

        # Calculate derived metrics
        if stats["errors_handled"] > 0:
            stats["retry_rate"] = stats["retries_scheduled"] / stats["errors_handled"]  # type: ignore
            stats["permanent_failure_rate"] = stats["permanent_failures"] / stats["errors_handled"]  # type: ignore
        else:
            stats["retry_rate"] = 0.0
            stats["permanent_failure_rate"] = 0.0

        # Add configuration info
        stats["config"] = {
            "max_retries": self.max_retries,
            "base_backoff_seconds": self.base_backoff_seconds,
            "max_backoff_seconds": self.max_backoff_seconds,
            "backoff_multiplier": self.backoff_multiplier,
            "jitter_factor": self.jitter_factor,
        }

        return stats

    def reset_stats(self):
        """Reset error handler statistics"""
        self.stats: Dict[str, Any] = {
            "errors_handled": 0,
            "retries_scheduled": 0,
            "permanent_failures": 0,
            "errors_by_type": {},
        }


if __name__ == "__main__":
    # CLI utility for testing error handler
    import sys

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python error_handler.py <command> [args...]")
            print("Commands:")
            print("  test - Test error classification")
            print("  schedule <error_type> - Show retry schedule")
            print("  stats - Show statistics")
            sys.exit(1)

        command = sys.argv[1]
        error_handler = CrawlErrorHandler()

        if command == "test":
            print("Testing error classification...")

            # Test different error types
            test_errors = [
                ConnectionError("Connection failed"),
                TimeoutError("Request timed out"),
                HTTPError("HTTP error", 404),
                HTTPError("Server error", 500),
                RateLimitExceededError("example.com", 60),
                RobotsBlockedError("https://example.com/test", "TestBot"),
                Exception("Unknown error"),
            ]

            for error in test_errors:
                classification = error_handler.classify_error(error)
                print(f"\nError: {error}")
                print(f"  Type: {classification.error_type.value}")
                print(f"  Retryable: {classification.is_retryable}")
                print(f"  Permanent: {classification.is_permanent}")
                print(f"  Suggested delay: {classification.suggested_delay}s")

        elif command == "schedule" and len(sys.argv) >= 3:
            error_type = sys.argv[2]
            print(f"Retry schedule for {error_type}:")

            # Create a mock error based on type
            if error_type == "http_404":
                test_error = HTTPError("Not found", 404)
            elif error_type == "http_500":
                test_error = HTTPError("Server error", 500)
            elif error_type == "timeout":
                test_error = TimeoutError("Request timed out")
            elif error_type == "rate_limit":
                test_error = RateLimitExceededError("example.com")
            else:
                test_error = Exception("Generic error")

            schedule = error_handler.get_retry_schedule(test_error)
            for attempt, delay in schedule:
                print(f"  Attempt {attempt}: {delay}s delay")

        elif command == "stats":
            stats = error_handler.get_stats()
            print("Error handler statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")

        else:
            print(f"Unknown command: {command}")
            sys.exit(1)

    asyncio.run(main())
