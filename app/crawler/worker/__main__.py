"""
CLI entry point for running the crawler worker.

This module provides a command-line interface for starting and managing
crawler worker instances with various configuration options and commands.
"""

import argparse
import asyncio
import logging
import sys
from typing import Any, Dict, Optional

from ..config.settings import load_settings
from ..utils.logging import setup_crawler_logger
from .crawler_worker import CrawlerWorker


async def run_worker(
    environment: Optional[str] = None,
    crawler_id: Optional[str] = None,
    log_level: str = "INFO",
    config_overrides: Optional[Dict[str, Any]] = None,
):
    """
    Run the crawler worker with specified configuration.

    Args:
        environment: Environment name (dev/staging/prod)
        crawler_id: Custom crawler ID
        log_level: Logging level
        config_overrides: Configuration overrides
    """
    # Setup logging
    setup_crawler_logger("crawler.worker", level=log_level)
    logger = logging.getLogger(__name__)

    worker = None
    try:
        # Load settings
        settings = load_settings(environment=environment, **(config_overrides or {}))

        logger.info(f"Starting crawler worker with environment: {environment}")
        logger.info(
            f"Crawler configuration: max_concurrent={settings.max_concurrent_requests}, "
            f"timeout={settings.request_timeout}s"
        )

        # Create and run worker
        worker = CrawlerWorker(settings, crawler_id)
        worker.setup_signal_handlers()

        await worker.initialize()

        # Start main worker loop
        worker._main_task = asyncio.create_task(worker.run())  # type: ignore
        await worker._main_task  # type: ignore

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if worker is not None:
            await worker.shutdown()


async def health_check(
    environment: Optional[str] = None,
    crawler_id: Optional[str] = None,
):
    """
    Perform health check on worker components.

    Args:
        environment: Environment name
        crawler_id: Crawler ID for health check
    """
    worker = None
    try:
        settings = load_settings(environment=environment)
        worker = CrawlerWorker(settings, crawler_id)

        await worker.initialize()
        health = await worker.health_check()

        print("Worker Health Check Results:")
        print(f"Overall Status: {health['status']}")
        print(f"Worker Status: {health['worker_status']}")
        print(f"Crawler ID: {health['crawler_id']}")

        print("\nComponent Health:")
        for component, status in health.get("components", {}).items():
            component_status = status.get("status", "unknown")
            print(f"  {component}: {component_status}")
            if "error" in status:
                print(f"    Error: {status['error']}")

        if health["status"] != "healthy":
            sys.exit(1)

    except Exception as e:
        print(f"Health check failed: {e}")
        sys.exit(1)
    finally:
        if worker is not None:
            await worker.shutdown()


async def show_stats(
    environment: Optional[str] = None,
    crawler_id: Optional[str] = None,
):
    """
    Show worker statistics.

    Args:
        environment: Environment name
        crawler_id: Crawler ID
    """
    worker = None
    try:
        settings = load_settings(environment=environment)
        worker = CrawlerWorker(settings, crawler_id)

        await worker.initialize()

        # Brief run to collect some stats
        print("Collecting statistics (5 second sample)...")
        worker._main_task = asyncio.create_task(worker.run())  # type: ignore

        try:
            await asyncio.wait_for(worker._main_task, timeout=5)  # type: ignore
        except asyncio.TimeoutError:
            worker._shutdown_requested = True  # type: ignore
            worker._main_task.cancel()  # type: ignore

        stats = worker.get_stats()

        print("\nWorker Statistics:")
        print(f"  Uptime: {stats['uptime_seconds']:.1f}s")
        print(f"  Messages Received: {stats['messages_received']}")
        print(f"  Messages Processed: {stats['messages_processed']}")
        print(f"  URLs Crawled: {stats['urls_crawled']}")
        print(f"  Success Rate: {stats['success_rate']:.2%}")
        print(f"  Domains Processed: {stats['domains_processed_count']}")
        print(f"  Locks Acquired: {stats['locks_acquired']}")
        print(f"  Lock Success Rate: {stats['lock_success_rate']:.2%}")

        if stats["errors_by_type"]:
            print("\nErrors by Type:")
            for error_type, count in stats["errors_by_type"].items():
                print(f"  {error_type}: {count}")

    except Exception as e:
        print(f"Statistics collection failed: {e}")
        sys.exit(1)
    finally:
        if worker is not None:
            await worker.shutdown()


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Distributed crawler worker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m app.crawler.worker run --environment dev
  python -m app.crawler.worker health --environment prod
  python -m app.crawler.worker stats --crawler-id worker-123
  python -m app.crawler.worker run --log-level DEBUG
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the crawler worker")
    run_parser.add_argument("--environment", "-e", help="Environment (dev/staging/prod)")
    run_parser.add_argument("--crawler-id", help="Custom crawler ID")
    run_parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level"
    )
    run_parser.add_argument("--max-concurrent", type=int, help="Override max concurrent requests")
    run_parser.add_argument("--timeout", type=int, help="Override request timeout")

    # Health command
    health_parser = subparsers.add_parser("health", help="Perform health check")
    health_parser.add_argument("--environment", "-e", help="Environment")
    health_parser.add_argument("--crawler-id", help="Crawler ID")

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show worker statistics")
    stats_parser.add_argument("--environment", "-e", help="Environment")
    stats_parser.add_argument("--crawler-id", help="Crawler ID")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Build config overrides
    config_overrides: Dict[str, Any] = {}
    if hasattr(args, "max_concurrent") and args.max_concurrent:
        config_overrides["max_concurrent_requests"] = args.max_concurrent
    if hasattr(args, "timeout") and args.timeout:
        config_overrides["request_timeout"] = args.timeout

    # Run the appropriate command
    try:
        if args.command == "run":
            asyncio.run(
                run_worker(
                    environment=args.environment,
                    crawler_id=args.crawler_id,
                    log_level=args.log_level,
                    config_overrides=config_overrides,
                )
            )
        elif args.command == "health":
            asyncio.run(health_check(environment=args.environment, crawler_id=args.crawler_id))
        elif args.command == "stats":
            asyncio.run(show_stats(environment=args.environment, crawler_id=args.crawler_id))
        else:
            print(f"Unknown command: {args.command}")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
