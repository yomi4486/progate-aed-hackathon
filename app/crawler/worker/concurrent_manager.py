"""
Concurrent processing control for the distributed crawler worker.

Manages concurrent crawling operations with semaphore-based request limiting,
domain-specific concurrency control, and resource-efficient processing to
prevent overwhelming target servers and optimize crawler performance.
"""

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel

from ..utils.url import extract_domain

logger = logging.getLogger(__name__)


class ConcurrencyStats(BaseModel):
    """Statistics for concurrent operations"""

    total_tasks_started: int = 0
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    current_active_tasks: int = 0
    current_domain_tasks: Dict[str, int] = {}
    peak_concurrency: int = 0
    average_task_duration: float = 0.0
    semaphore_wait_times: List[float] = []


class TaskInfo(BaseModel):
    """Information about a running task"""

    task_id: str
    url: str
    domain: str
    started_at: datetime
    semaphore_acquired_at: Optional[datetime] = None


class ConcurrentCrawlManager:
    """
    Manages concurrent crawling operations with configurable limits.

    Provides semaphore-based global concurrency control, domain-specific
    limits to avoid overwhelming individual servers, and comprehensive
    statistics tracking for monitoring and optimization.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        max_concurrent_per_domain: int = 2,
        domain_concurrency_overrides: Optional[Dict[str, int]] = None,
        task_timeout: int = 300,  # 5 minutes
        cleanup_interval: int = 60,  # 1 minute
    ):
        """
        Initialize the concurrent crawl manager.

        Args:
            max_concurrent: Maximum total concurrent tasks
            max_concurrent_per_domain: Default maximum concurrent tasks per domain
            domain_concurrency_overrides: Per-domain concurrency overrides
            task_timeout: Maximum time for a single task (seconds)
            cleanup_interval: Interval for cleanup operations (seconds)
        """
        self.max_concurrent = max_concurrent
        self.max_concurrent_per_domain = max_concurrent_per_domain
        self.domain_concurrency_overrides = domain_concurrency_overrides or {}
        self.task_timeout = task_timeout
        self.cleanup_interval = cleanup_interval

        # Concurrency control
        self.global_semaphore = asyncio.Semaphore(max_concurrent)
        self.domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        self.domain_semaphore_lock = asyncio.Lock()

        # Task tracking
        self.active_tasks: Dict[str, TaskInfo] = {}
        self.domain_task_counts: Dict[str, int] = defaultdict(int)
        self.task_lock = asyncio.Lock()

        # Statistics
        self.stats = ConcurrencyStats()
        self.task_start_times: Dict[str, float] = {}

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task[None]] = None
        self._shutdown_event = asyncio.Event()

        logger.info(
            f"Initialized concurrent crawl manager: "
            f"max_concurrent={max_concurrent}, "
            f"max_per_domain={max_concurrent_per_domain}"
        )

    async def initialize(self):
        """Initialize the concurrent manager and start background tasks"""
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        logger.info("Concurrent crawl manager initialized")

    async def shutdown(self):
        """Shutdown the concurrent manager and cleanup resources"""
        self._shutdown_event.set()

        # Cancel cleanup task
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Wait for all active tasks to complete (with timeout)
        if self.active_tasks:
            logger.info(f"Waiting for {len(self.active_tasks)} active tasks to complete...")

            # Give tasks some time to complete naturally
            await asyncio.sleep(5)

            # Cancel remaining tasks if any
            remaining_tasks = list(self.active_tasks.values())
            if remaining_tasks:
                logger.warning(f"Force-cancelling {len(remaining_tasks)} remaining tasks")

        logger.info("Concurrent crawl manager shutdown complete")

    async def crawl_with_concurrency(
        self,
        crawl_func: Callable[[str], Awaitable[Any]],
        url: str,
        task_id: Optional[str] = None,
    ) -> Any:
        """
        Execute a crawl function with concurrency control.

        Args:
            crawl_func: Async function that performs the actual crawling
            url: URL to crawl
            task_id: Optional task identifier

        Returns:
            Result from crawl_func

        Raises:
            Exception: If crawling fails or times out
        """
        if task_id is None:
            task_id = str(uuid4())

        domain = extract_domain(url)

        # Create task info
        task_info = TaskInfo(
            task_id=task_id,
            url=url,
            domain=domain,
            started_at=datetime.now(timezone.utc),
        )

        start_time = time.time()

        try:
            # Register task
            await self._register_task(task_info)

            # Acquire semaphores with timeout
            await self._acquire_semaphores(task_info)

            # Execute the crawl function with timeout
            result = await asyncio.wait_for(crawl_func(url), timeout=self.task_timeout)

            # Update success statistics
            duration = time.time() - start_time
            await self._update_completion_stats(task_info, duration, success=True)

            logger.debug(
                "Task completed successfully",
                extra={
                    "task_id": task_id,
                    "url": url,
                    "domain": domain,
                    "duration": duration,
                },
            )

            return result

        except asyncio.TimeoutError:
            duration = time.time() - start_time
            await self._update_completion_stats(task_info, duration, success=False)

            logger.error(
                f"Task timed out after {duration:.1f}s",
                extra={
                    "task_id": task_id,
                    "url": url,
                    "domain": domain,
                    "timeout": self.task_timeout,
                },
            )
            raise

        except Exception as e:
            duration = time.time() - start_time
            await self._update_completion_stats(task_info, duration, success=False)

            logger.error(
                f"Task failed: {e}",
                extra={
                    "task_id": task_id,
                    "url": url,
                    "domain": domain,
                    "duration": duration,
                    "error": str(e),
                },
            )
            raise

        finally:
            # Always release semaphores and unregister task
            await self._release_semaphores(task_info)
            await self._unregister_task(task_info)

    async def crawl_batch_with_concurrency(
        self,
        crawl_func: Callable[[str], Awaitable[Any]],
        urls: List[str],
        max_concurrent_batch: Optional[int] = None,
    ) -> List[Any]:
        """
        Execute crawl function for multiple URLs with concurrency control.

        Args:
            crawl_func: Async function that performs the actual crawling
            urls: List of URLs to crawl
            max_concurrent_batch: Optional override for batch concurrency

        Returns:
            List of results (None for failed crawls)
        """
        if not urls:
            return []

        if max_concurrent_batch is None:
            max_concurrent_batch = min(self.max_concurrent, len(urls))

        logger.info(f"Starting batch crawl of {len(urls)} URLs with concurrency {max_concurrent_batch}")

        # Create semaphore for batch-level concurrency control
        batch_semaphore = asyncio.Semaphore(max_concurrent_batch)

        async def crawl_with_batch_limit(url: str) -> Optional[Any]:
            async with batch_semaphore:
                try:
                    return await self.crawl_with_concurrency(crawl_func, url)
                except Exception as e:
                    logger.error(f"Batch crawl failed for {url}: {e}")
                    return None

        # Execute all URLs concurrently with batch limit
        tasks = [crawl_with_batch_limit(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        successful = sum(1 for result in results if result is not None)
        logger.info(f"Batch crawl completed: {successful}/{len(urls)} successful")

        return results

    async def _register_task(self, task_info: TaskInfo):
        """Register a new task for tracking"""
        async with self.task_lock:
            self.active_tasks[task_info.task_id] = task_info
            self.domain_task_counts[task_info.domain] += 1
            self.stats.total_tasks_started += 1
            self.stats.current_active_tasks += 1
            self.stats.current_domain_tasks[task_info.domain] = self.domain_task_counts[task_info.domain]

            # Update peak concurrency
            if self.stats.current_active_tasks > self.stats.peak_concurrency:
                self.stats.peak_concurrency = self.stats.current_active_tasks

            self.task_start_times[task_info.task_id] = time.time()

    async def _unregister_task(self, task_info: TaskInfo):
        """Unregister a completed task"""
        async with self.task_lock:
            self.active_tasks.pop(task_info.task_id, None)
            self.domain_task_counts[task_info.domain] -= 1
            self.stats.current_active_tasks -= 1

            if self.domain_task_counts[task_info.domain] <= 0:
                del self.domain_task_counts[task_info.domain]
                self.stats.current_domain_tasks.pop(task_info.domain, None)
            else:
                self.stats.current_domain_tasks[task_info.domain] = self.domain_task_counts[task_info.domain]

            self.task_start_times.pop(task_info.task_id, None)

    async def _acquire_semaphores(self, task_info: TaskInfo):
        """Acquire both global and domain-specific semaphores"""
        # Acquire global semaphore first
        semaphore_wait_start = time.time()
        await self.global_semaphore.acquire()

        try:
            # Get or create domain semaphore
            domain_semaphore = await self._get_domain_semaphore(task_info.domain)

            # Acquire domain semaphore
            await domain_semaphore.acquire()

            # Record semaphore acquisition time
            wait_time = time.time() - semaphore_wait_start
            self.stats.semaphore_wait_times.append(wait_time)

            # Keep only recent wait times for statistics
            if len(self.stats.semaphore_wait_times) > 100:
                self.stats.semaphore_wait_times = self.stats.semaphore_wait_times[-50:]

            task_info.semaphore_acquired_at = datetime.now(timezone.utc)

            logger.debug(
                f"Acquired semaphores for {task_info.domain}",
                extra={
                    "task_id": task_info.task_id,
                    "domain": task_info.domain,
                    "wait_time": wait_time,
                },
            )

        except Exception:
            # If domain semaphore acquisition fails, release global semaphore
            self.global_semaphore.release()
            raise

    async def _release_semaphores(self, task_info: TaskInfo):
        """Release both global and domain-specific semaphores"""
        try:
            # Get domain semaphore
            domain_semaphore = await self._get_domain_semaphore(task_info.domain)
            domain_semaphore.release()

            logger.debug(
                f"Released semaphores for {task_info.domain}",
                extra={
                    "task_id": task_info.task_id,
                    "domain": task_info.domain,
                },
            )
        except Exception as e:
            logger.error(f"Error releasing domain semaphore: {e}")
        finally:
            # Always release global semaphore
            self.global_semaphore.release()

    async def _get_domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        """Get or create a semaphore for the specified domain"""
        async with self.domain_semaphore_lock:
            if domain not in self.domain_semaphores:
                # Check for domain-specific override
                limit = self.domain_concurrency_overrides.get(domain, self.max_concurrent_per_domain)
                self.domain_semaphores[domain] = asyncio.Semaphore(limit)

                logger.debug(f"Created domain semaphore for {domain} with limit {limit}")

            return self.domain_semaphores[domain]

    async def _update_completion_stats(self, task_info: TaskInfo, duration: float, success: bool):
        """Update completion statistics"""
        if success:
            self.stats.total_tasks_completed += 1
        else:
            self.stats.total_tasks_failed += 1

        # Update average duration (simple moving average)
        total_completed = self.stats.total_tasks_completed + self.stats.total_tasks_failed
        if total_completed > 0:
            self.stats.average_task_duration = (
                self.stats.average_task_duration * (total_completed - 1) + duration
            ) / total_completed

    async def _periodic_cleanup(self):
        """Periodic cleanup of stale tasks and statistics"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_stale_tasks()
                await self._cleanup_domain_semaphores()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")

    async def _cleanup_stale_tasks(self):
        """Clean up tasks that have been running too long"""
        current_time = datetime.now(timezone.utc)
        stale_tasks: List[TaskInfo] = []

        async with self.task_lock:
            for _, task_info in self.active_tasks.items():
                if current_time - task_info.started_at > timedelta(seconds=self.task_timeout * 2):
                    stale_tasks.append(task_info)

        if stale_tasks:
            logger.warning(f"Found {len(stale_tasks)} stale tasks for cleanup")

            for task_info in stale_tasks:
                logger.warning(f"Cleaning up stale task: {task_info.task_id} ({task_info.url})")
                await self._unregister_task(task_info)

    async def _cleanup_domain_semaphores(self):
        """Clean up unused domain semaphores"""
        async with self.domain_semaphore_lock:
            # Find domains with no active tasks
            unused_domains: List[str] = []
            for domain in list(self.domain_semaphores.keys()):
                if domain not in self.domain_task_counts or self.domain_task_counts[domain] == 0:
                    unused_domains.append(domain)

            # Remove unused semaphores
            for domain in unused_domains:
                del self.domain_semaphores[domain]
                logger.debug(f"Cleaned up unused semaphore for domain: {domain}")

    def get_concurrency_status(self) -> Dict[str, Any]:
        """Get current concurrency status"""
        return {
            "global_semaphore_available": self.global_semaphore._value,
            "global_semaphore_locked": self.max_concurrent - self.global_semaphore._value,
            "active_tasks": len(self.active_tasks),
            "active_domains": len(self.domain_task_counts),
            "domain_task_counts": dict(self.domain_task_counts),
            "domain_semaphore_count": len(self.domain_semaphores),
        }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        metrics = {
            "tasks_per_second": 0.0,
            "completion_rate": 0.0,
            "average_wait_time": 0.0,
            "peak_concurrency": self.stats.peak_concurrency,
            "average_task_duration": self.stats.average_task_duration,
        }

        # Calculate rates
        total_tasks = self.stats.total_tasks_started
        if total_tasks > 0:
            completed_tasks = self.stats.total_tasks_completed + self.stats.total_tasks_failed
            metrics["completion_rate"] = completed_tasks / total_tasks

        # Calculate average wait time
        if self.stats.semaphore_wait_times:
            metrics["average_wait_time"] = sum(self.stats.semaphore_wait_times) / len(self.stats.semaphore_wait_times)

        return metrics

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics"""
        return {
            **self.stats.model_dump(),
            "concurrency_status": self.get_concurrency_status(),
            "performance_metrics": self.get_performance_metrics(),
            "configuration": {
                "max_concurrent": self.max_concurrent,
                "max_concurrent_per_domain": self.max_concurrent_per_domain,
                "domain_concurrency_overrides": self.domain_concurrency_overrides,
                "task_timeout": self.task_timeout,
            },
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on concurrent manager"""
        try:
            status = self.get_concurrency_status()
            performance = self.get_performance_metrics()

            # Determine health status
            health_status = "healthy"

            # Check for potential issues
            if status["global_semaphore_locked"] >= self.max_concurrent * 0.9:
                health_status = "degraded"  # Near capacity

            if len(self.active_tasks) > 0 and performance["average_task_duration"] > self.task_timeout * 0.8:
                health_status = "degraded"  # Tasks taking too long

            return {
                "status": health_status,
                "active_tasks": len(self.active_tasks),
                "concurrency_utilization": status["global_semaphore_locked"] / self.max_concurrent,
                "performance_metrics": performance,
                "cleanup_task_running": self._cleanup_task is not None and not self._cleanup_task.done(),
            }

        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    # CLI utility for testing concurrent manager
    import sys

    async def mock_crawl_function(url: str) -> str:
        """Mock crawl function for testing"""
        await asyncio.sleep(random.uniform(0.5, 2.0))  # Simulate work
        return f"Crawled: {url}"

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python concurrent_manager.py <command> [args...]")
            print("Commands:")
            print("  test - Test concurrent manager")
            print("  batch-test <count> - Test batch crawling")
            print("  health - Check health status")
            print("  stats - Show statistics")
            sys.exit(1)

        command = sys.argv[1]
        manager = ConcurrentCrawlManager(max_concurrent=5, max_concurrent_per_domain=2)

        try:
            await manager.initialize()

            if command == "test":
                print("Testing concurrent manager...")

                # Test single crawl
                result = await manager.crawl_with_concurrency(mock_crawl_function, "https://example.com/test")
                print(f"Single crawl result: {result}")

            elif command == "batch-test" and len(sys.argv) >= 3:
                count = int(sys.argv[2])
                print(f"Testing batch crawl with {count} URLs...")

                # Generate test URLs
                test_urls = [f"https://example{i % 3}.com/page{i}" for i in range(count)]

                start_time = time.time()
                results = await manager.crawl_batch_with_concurrency(mock_crawl_function, test_urls)
                duration = time.time() - start_time

                successful = sum(1 for r in results if r is not None)
                print(f"Batch crawl completed in {duration:.1f}s: {successful}/{count} successful")

            elif command == "health":
                health = await manager.health_check()
                print("Health status:")
                for key, value in health.items():
                    print(f"  {key}: {value}")

            elif command == "stats":
                stats = manager.get_stats()
                print("Concurrent manager statistics:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        finally:
            await manager.shutdown()

    import random

    asyncio.run(main())
