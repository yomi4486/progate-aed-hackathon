"""
Retry utilities for the distributed crawler.

Provides exponential backoff retry mechanism with jitter and customizable
exception handling.
"""

import asyncio
import logging
import random
from typing import Awaitable, Callable, Optional, ParamSpec, Tuple, Type, TypeVar, Union

logger = logging.getLogger(__name__)


# Generic param/return specs for call-through wrappers
P = ParamSpec("P")
R = TypeVar("R")


class RetryError(Exception):
    """Raised when all retry attempts have been exhausted"""

    def __init__(self, attempts: int, last_exception: Exception):
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(f"Failed after {attempts} attempts. Last error: {last_exception}")


class RetryConfig:
    """Configuration for retry behavior"""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        jitter_range: float = 0.1,
    ):
        """
        Initialize retry configuration.

        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential backoff calculation
            jitter: Whether to add random jitter to delays
            jitter_range: Range of jitter (0.0 to 1.0)
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.jitter_range = jitter_range

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for given attempt number.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        # Calculate exponential delay
        delay = self.base_delay * (self.exponential_base**attempt)

        # Cap at maximum delay
        delay = min(delay, self.max_delay)

        # Add jitter if enabled
        if self.jitter and self.jitter_range > 0:
            jitter_amount = delay * self.jitter_range
            jitter = random.uniform(-jitter_amount, jitter_amount)
            delay = max(0, delay + jitter)

        return delay


async def exponential_backoff_retry(
    func: Callable[P, Awaitable[R]],
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,),
    on_retry: Optional[Callable[[int, Exception, float], Awaitable[None]]] = None,
    *args: P.args,
    **kwargs: P.kwargs,
) -> R:
    """
    Retry an async function with exponential backoff.

    Args:
        func: Async function to retry
        *args: Positional arguments for func
        max_retries: Maximum number of retries
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exceptions: Exception types to catch and retry
        on_retry: Optional callback called on each retry attempt
        **kwargs: Keyword arguments for func

    Returns:
        Result of successful function call

    Raises:
        RetryError: If all retry attempts fail
        Exception: If function raises an exception not in the exceptions list
    """
    config = RetryConfig(
        max_attempts=max_retries + 1,  # +1 for initial attempt
        base_delay=base_delay,
        max_delay=max_delay,
    )

    return await retry_with_config(func, config, exceptions, on_retry, *args, **kwargs)


async def retry_with_config(
    func: Callable[P, Awaitable[R]],
    config: RetryConfig,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,),
    on_retry: Optional[Callable[[int, Exception, float], Awaitable[None]]] = None,
    *args: P.args,
    **kwargs: P.kwargs,
) -> R:
    """
    Retry an async function with custom retry configuration.

    Args:
        func: Async function to retry
        config: Retry configuration
        *args: Positional arguments for func
        exceptions: Exception types to catch and retry
        on_retry: Optional callback called on each retry attempt
        **kwargs: Keyword arguments for func

    Returns:
        Result of successful function call

    Raises:
        RetryError: If all retry attempts fail
        Exception: If function raises an exception not in the exceptions list
    """
    last_exception = None

    for attempt in range(config.max_attempts):
        try:
            result = await func(*args, **kwargs)

            # Success - log if this was a retry
            if attempt > 0:
                logger.info(
                    f"Function succeeded after {attempt + 1} attempts",
                    extra={
                        "function": func.__name__,
                        "attempts": attempt + 1,
                        "function_args": str(args)[:100],  # Truncate for logging
                    },
                )

            return result

        except exceptions as e:
            last_exception = e

            # Check if we have more attempts
            if attempt + 1 >= config.max_attempts:
                break

            # Calculate delay for next attempt
            delay = config.calculate_delay(attempt)

            # Log retry attempt
            logger.warning(
                f"Function failed, retrying in {delay:.2f}s",
                extra={
                    "function": func.__name__,
                    "attempt": attempt + 1,
                    "max_attempts": config.max_attempts,
                    "delay": delay,
                    "exception": str(e),
                    "exception_type": type(e).__name__,
                },
            )

            # Call retry callback if provided
            if on_retry:
                try:
                    await on_retry(attempt + 1, e, delay)
                except Exception as callback_error:
                    logger.error(f"Retry callback failed: {callback_error}")

            # Wait before retry
            if delay > 0:
                await asyncio.sleep(delay)

    # All attempts failed
    raise RetryError(config.max_attempts, last_exception or Exception("No exception captured during retries"))


class AsyncRetrier:
    """
    Reusable retry handler for consistent retry behavior.
    """

    def __init__(self, config: RetryConfig):
        """
        Initialize retrier with configuration.

        Args:
            config: Retry configuration
        """
        self.config = config
        self.stats: dict[str, Union[int, float]] = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "total_attempts": 0,
            "retry_attempts": 0,
        }

    async def call(
        self,
        func: Callable[P, Awaitable[R]],
        exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,),
        on_retry: Optional[Callable[[int, Exception, float], Awaitable[None]]] = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """
        Call function with retry logic.

        Args:
            func: Function to call
            *args: Positional arguments
            exceptions: Exceptions to catch and retry
            on_retry: Retry callback
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            RetryError: If all attempts fail
        """
        self.stats["total_calls"] += 1

        try:
            result = await retry_with_config(
                func,
                self.config,
                exceptions,
                await self._on_retry_wrapper(on_retry),
                *args,
                **kwargs,
            )
            self.stats["successful_calls"] += 1
            return result

        except RetryError as e:
            self.stats["failed_calls"] += 1
            self.stats["total_attempts"] += e.attempts
            self.stats["retry_attempts"] += e.attempts - 1
            raise

    async def _on_retry_wrapper(
        self, user_callback: Optional[Callable[[int, Exception, float], Awaitable[None]]]
    ) -> Callable[[int, Exception, float], Awaitable[None]]:
        """Internal wrapper for retry callback"""

        async def wrapper(attempt: int, exception: Exception, delay: float):
            # Update internal stats
            self.stats["retry_attempts"] += 1

            # Call user callback if provided
            if user_callback:
                await user_callback(attempt, exception, delay)

        return wrapper

    def get_stats(self) -> dict[str, Union[int, float]]:
        """Get retry statistics"""
        stats = self.stats.copy()
        if stats["total_calls"] > 0:
            stats["success_rate"] = stats["successful_calls"] / stats["total_calls"]
            stats["average_attempts"] = (
                stats["total_attempts"] / stats["total_calls"] if stats["total_attempts"] > 0 else 1.0
            )
        else:
            stats["success_rate"] = int(0.0)
            stats["average_attempts"] = 0

        return stats

    def reset_stats(self) -> None:
        """Reset statistics"""
        self.stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "total_attempts": 0,
            "retry_attempts": 0,
        }


# Pre-configured retry configurations for common scenarios
NETWORK_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=2.0,
    max_delay=30.0,
    exponential_base=2.0,
    jitter=True,
)

DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=0.5,
    max_delay=10.0,
    exponential_base=1.5,
    jitter=True,
)

QUICK_RETRY_CONFIG = RetryConfig(
    max_attempts=2,
    base_delay=0.1,
    max_delay=1.0,
    exponential_base=2.0,
    jitter=False,
)

AGGRESSIVE_RETRY_CONFIG = RetryConfig(
    max_attempts=10,
    base_delay=1.0,
    max_delay=120.0,
    exponential_base=2.0,
    jitter=True,
)


# Convenience functions for common retry patterns
async def retry_network_call(func: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
    """Retry with network-optimized settings"""
    return await retry_with_config(
        func,
        NETWORK_RETRY_CONFIG,
        exceptions=(Exception,),
        on_retry=None,
        *args,
        **kwargs,
    )


async def retry_database_call(func: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
    """Retry with database-optimized settings"""
    return await retry_with_config(
        func,
        DATABASE_RETRY_CONFIG,
        exceptions=(Exception,),
        on_retry=None,
        *args,
        **kwargs,
    )


async def retry_quick_call(func: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
    """Retry with quick/lightweight settings"""
    return await retry_with_config(
        func,
        QUICK_RETRY_CONFIG,
        exceptions=(Exception,),
        on_retry=None,
        *args,
        **kwargs,
    )


if __name__ == "__main__":
    # Simple demo without relying on function attributes (to keep type checkers happy)
    async def failing_function(*, fail_count: int = 2) -> str:
        if not hasattr(failing_function, "_counter"):
            setattr(failing_function, "_counter", 0)
        # mypy/pyright: ignore attribute on function object for demo purposes
        current = getattr(failing_function, "_counter") + 1  # type: ignore[attr-defined]
        setattr(failing_function, "_counter", current)  # type: ignore[attr-defined]
        if current <= fail_count:
            raise ValueError(f"Attempt {current} failed")
        return f"Success on attempt {current}"

    async def test_retry():
        try:
            result = await exponential_backoff_retry(
                failing_function, fail_count=2, max_retries=3, base_delay=0.1, max_delay=1.0
            )
            print(f"Result: {result}")
        except RetryError as e:
            print(f"All retries failed: {e}")

    asyncio.run(test_retry())
