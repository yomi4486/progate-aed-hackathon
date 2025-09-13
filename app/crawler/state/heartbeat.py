"""
Heartbeat mechanism for maintaining distributed locks.

Provides background tasks to periodically extend lock TTLs and handle
graceful shutdown to prevent orphaned locks.
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from types import FrameType, TracebackType
from typing import Any, Dict, Optional, Type, TypedDict

from ..config.settings import get_cached_settings
from .lock_manager import DistributedLockManager

logger = logging.getLogger(__name__)


class StatsType(TypedDict):
    heartbeats_sent: int
    heartbeat_failures: int
    cleanups_performed: int
    locks_cleaned: int
    startup_time: Optional[int]


class HeartbeatManager:
    """
    Manages periodic heartbeat operations to maintain distributed locks.

    Runs background tasks to extend lock TTLs and handles graceful shutdown
    to prevent orphaned locks when the crawler process terminates.
    """

    def __init__(
        self,
        lock_manager: DistributedLockManager,
        heartbeat_interval_seconds: Optional[int] = None,
        cleanup_interval_seconds: Optional[int] = None,
    ):
        """
        Initialize the heartbeat manager.

        Args:
            lock_manager: Distributed lock manager instance
            heartbeat_interval_seconds: Interval for heartbeat (defaults to settings)
            cleanup_interval_seconds: Interval for cleanup (defaults to 5 minutes)
        """
        self.lock_manager = lock_manager
        self.settings = get_cached_settings()

        self.heartbeat_interval = heartbeat_interval_seconds or self.settings.heartbeat_interval_seconds
        self.cleanup_interval = cleanup_interval_seconds or 300  # 5 minutes

        # Task management
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._cleanup_task: Optional[asyncio.Task[None]] = None
        self._shutdown_event = asyncio.Event()
        self._running = False

        # Statistics
        self.stats: StatsType = {
            "heartbeats_sent": 0,
            "heartbeat_failures": 0,
            "cleanups_performed": 0,
            "locks_cleaned": 0,
            "startup_time": None,
        }

        # Signal handlers
        self._setup_signal_handlers()

        logger.info(f"Initialized heartbeat manager with {self.heartbeat_interval}s interval")

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown"""
        if sys.platform != "win32":  # Unix-like systems
            for sig in [signal.SIGTERM, signal.SIGINT]:
                signal.signal(sig, self._handle_shutdown_signal)
        else:  # Windows
            signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signum: int, _: Optional[FrameType]) -> None:
        """Handle shutdown signals by triggering graceful shutdown"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")

        # Use asyncio.create_task if we're in an event loop
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.shutdown())
        except RuntimeError:
            # No event loop running, set shutdown event
            self._shutdown_event.set()

    async def start(self) -> None:
        """
        Start the heartbeat manager background tasks.
        """
        if self._running:
            logger.warning("Heartbeat manager is already running")
            return

        self._running = True
        self.stats["startup_time"] = int(datetime.now(timezone.utc).timestamp())

        logger.info("Starting heartbeat manager tasks")

        # Start heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        logger.info("Heartbeat manager started successfully")

    async def shutdown(self) -> None:
        """
        Gracefully shutdown the heartbeat manager.

        Cancels background tasks and releases all held locks.
        """
        if not self._running:
            logger.warning("Heartbeat manager is not running")
            return

        logger.info("Shutting down heartbeat manager")

        self._running = False
        self._shutdown_event.set()

        # Cancel background tasks
        tasks_to_cancel: list[asyncio.Task[Any]] = []
        if self._heartbeat_task and not self._heartbeat_task.done():
            tasks_to_cancel.append(self._heartbeat_task)
        if self._cleanup_task and not self._cleanup_task.done():
            tasks_to_cancel.append(self._cleanup_task)

        if tasks_to_cancel:
            logger.info(f"Cancelling {len(tasks_to_cancel)} background tasks")
            for task in tasks_to_cancel:
                task.cancel()

            # Wait for tasks to finish cancellation
            try:
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error during task cancellation: {e}")

        # Force release all locks
        try:
            released_count = await self.lock_manager.force_release_all_locks()
            logger.info(f"Released {released_count} locks during shutdown")
        except Exception as e:
            logger.error(f"Error releasing locks during shutdown: {e}")

        logger.info("Heartbeat manager shutdown complete")

    async def _heartbeat_loop(self) -> None:
        """
        Background task that sends periodic heartbeats to extend lock TTLs.
        """
        logger.info(f"Starting heartbeat loop with {self.heartbeat_interval}s interval")

        while self._running and not self._shutdown_event.is_set():
            try:
                await self._send_heartbeats()

                # Wait for next heartbeat interval or shutdown signal
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=self.heartbeat_interval)
                    # If we reach here, shutdown was signaled
                    break
                except asyncio.TimeoutError:
                    # Normal case - continue with next heartbeat
                    continue

            except asyncio.CancelledError:
                logger.info("Heartbeat loop cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in heartbeat loop: {e}")
                # Continue running despite errors
                await asyncio.sleep(min(self.heartbeat_interval, 10))

        logger.info("Heartbeat loop stopped")

    async def _send_heartbeats(self) -> None:
        """Send heartbeats to extend TTLs for all held locks"""
        held_locks = list(self.lock_manager.held_locks)

        if not held_locks:
            logger.debug("No locks held, skipping heartbeat")
            return

        logger.debug(f"Sending heartbeat for {len(held_locks)} locks")

        successful_heartbeats = 0
        failed_heartbeats = 0

        # Send heartbeats concurrently for better performance
        heartbeat_tasks = [self._send_single_heartbeat(url_hash) for url_hash in held_locks]

        results = await asyncio.gather(*heartbeat_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                failed_heartbeats += 1
                logger.error(f"Heartbeat failed: {result}")
            elif result:
                successful_heartbeats += 1
            else:
                failed_heartbeats += 1

        # Update statistics
        self.stats["heartbeats_sent"] += successful_heartbeats
        self.stats["heartbeat_failures"] += failed_heartbeats

        if failed_heartbeats > 0:
            logger.warning(f"Heartbeat summary: {successful_heartbeats} successful, {failed_heartbeats} failed")
        else:
            logger.debug(f"Sent {successful_heartbeats} heartbeats successfully")

    async def _send_single_heartbeat(self, url_hash: str) -> bool:
        """
        Send heartbeat for a single URL lock.

        Args:
            url_hash: Hash of the URL to send heartbeat for

        Returns:
            True if heartbeat was successful, False otherwise
        """
        try:
            success = await self.lock_manager.extend_lock(url_hash)
            if not success:
                logger.warning(f"Failed to extend lock for {url_hash}")
            return success
        except Exception as e:
            logger.error(f"Error sending heartbeat for {url_hash}: {e}")
            return False

    async def _cleanup_loop(self) -> None:
        """
        Background task that periodically cleans up expired locks.
        """
        logger.info(f"Starting cleanup loop with {self.cleanup_interval}s interval")

        while self._running and not self._shutdown_event.is_set():
            try:
                await self._perform_cleanup()

                # Wait for next cleanup interval or shutdown signal
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=self.cleanup_interval)
                    # If we reach here, shutdown was signaled
                    break
                except asyncio.TimeoutError:
                    # Normal case - continue with next cleanup
                    continue

            except asyncio.CancelledError:
                logger.info("Cleanup loop cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in cleanup loop: {e}")
                # Continue running despite errors
                await asyncio.sleep(min(self.cleanup_interval, 60))

        logger.info("Cleanup loop stopped")

    async def _perform_cleanup(self) -> None:
        """Perform cleanup of expired locks"""
        logger.debug("Performing expired lock cleanup")

        try:
            cleaned_count = await self.lock_manager.cleanup_expired_locks()

            # Update statistics
            self.stats["cleanups_performed"] += 1
            self.stats["locks_cleaned"] += cleaned_count

            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} expired locks")
            else:
                logger.debug("No expired locks found during cleanup")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get heartbeat manager statistics"""
        uptime_seconds = None
        if self.stats["startup_time"]:
            # startup_time is stored as a UNIX timestamp; compute uptime via timestamps
            now_ts = int(datetime.now(timezone.utc).timestamp())
            uptime_seconds = now_ts - int(self.stats["startup_time"])

        return {
            **self.stats,
            "running": self._running,
            "heartbeat_interval": self.heartbeat_interval,
            "cleanup_interval": self.cleanup_interval,
            "uptime_seconds": uptime_seconds,
            "held_locks_count": len(self.lock_manager.held_locks),
        }

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on heartbeat manager.

        Returns:
            Health status information
        """
        health_status: Dict[str, Any] = {
            "status": "healthy" if self._running else "stopped",
            "running": self._running,
            "tasks_running": {
                "heartbeat": (self._heartbeat_task is not None and not self._heartbeat_task.done()),
                "cleanup": (self._cleanup_task is not None and not self._cleanup_task.done()),
            },
        }

        # Check for task failures
        if self._heartbeat_task and self._heartbeat_task.done():
            try:
                self._heartbeat_task.result()
            except Exception as e:
                health_status["status"] = "degraded"
                health_status["heartbeat_error"] = str(e)

        if self._cleanup_task and self._cleanup_task.done():
            try:
                self._cleanup_task.result()
            except Exception as e:
                health_status["status"] = "degraded"
                health_status["cleanup_error"] = str(e)

        # Check failure rates
        total_heartbeats = self.stats["heartbeats_sent"] + self.stats["heartbeat_failures"]
        if total_heartbeats > 0:
            failure_rate = self.stats["heartbeat_failures"] / total_heartbeats
            if failure_rate > 0.1:  # More than 10% failure rate
                health_status["status"] = "degraded"
                health_status["heartbeat_failure_rate"] = failure_rate

        return health_status

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Async context manager exit"""
        await self.shutdown()


class HeartbeatManagerSingleton:
    """
    Singleton wrapper for HeartbeatManager to ensure only one instance
    per crawler process.
    """

    _instance: Optional[HeartbeatManager] = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls, lock_manager: Optional[DistributedLockManager] = None) -> Optional[HeartbeatManager]:
        """
        Get the singleton HeartbeatManager instance.

        Args:
            lock_manager: Lock manager (required for first call)

        Returns:
            HeartbeatManager instance or None if not initialized
        """
        async with cls._lock:
            if cls._instance is None and lock_manager is not None:
                cls._instance = HeartbeatManager(lock_manager)
                await cls._instance.start()
            return cls._instance

    @classmethod
    async def shutdown_instance(cls) -> None:
        """Shutdown the singleton instance"""
        async with cls._lock:
            if cls._instance is not None:
                await cls._instance.shutdown()
                cls._instance = None

    @classmethod
    def reset_instance(cls) -> None:
        """Reset instance (for testing)"""
        cls._instance = None


if __name__ == "__main__":
    # CLI utility for heartbeat management

    from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python heartbeat.py [test|monitor] [crawler_id]")
            sys.exit(1)

        command = sys.argv[1]
        crawler_id = sys.argv[2] if len(sys.argv) > 2 else "test-crawler"

        # Ensure settings can be loaded if needed by other components
        load_settings()
        lock_manager = DistributedLockManager(crawler_id)

        if command == "test":
            print(f"Testing heartbeat manager for crawler {crawler_id}")

            async with HeartbeatManager(lock_manager) as heartbeat_manager:
                print("Heartbeat manager started, waiting 30 seconds...")
                await asyncio.sleep(30)

                stats = heartbeat_manager.get_stats()
                health = await heartbeat_manager.health_check()

                print(f"\nStats: {stats}")
                print(f"Health: {health}")

            print("Heartbeat manager test completed")

        elif command == "monitor":
            print(f"Monitoring heartbeat manager for crawler {crawler_id}")
            print("Press Ctrl+C to stop")

            heartbeat_manager = HeartbeatManager(lock_manager)
            await heartbeat_manager.start()

            try:
                while True:
                    stats = heartbeat_manager.get_stats()
                    health = await heartbeat_manager.health_check()

                    print(f"\n[{datetime.now()}]")
                    print(f"Status: {health['status']}")
                    print(f"Heartbeats sent: {stats['heartbeats_sent']}")
                    print(f"Heartbeat failures: {stats['heartbeat_failures']}")
                    print(f"Cleanups performed: {stats['cleanups_performed']}")
                    print(f"Locks cleaned: {stats['locks_cleaned']}")
                    print(f"Held locks: {stats['held_locks_count']}")

                    await asyncio.sleep(10)  # Update every 10 seconds

            except KeyboardInterrupt:
                print("\nShutting down...")
            finally:
                await heartbeat_manager.shutdown()

        else:
            print(f"Unknown command: {command}")
            sys.exit(1)

    asyncio.run(main())
