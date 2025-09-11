"""
URL state management for the distributed crawler.

Provides distributed locking, state transitions, and batch operations
for URL crawl coordination across multiple crawler instances.
"""

from ..config.settings import CrawlerSettings
from .client import DynamoDBClient, get_dynamodb_client, reset_client

# LocalStack compatibility imports
from .localstack_client import LocalStackDynamoDBClient, LocalStackURLStateManager
from .localstack_lock_manager import LocalStackDistributedLockManager
from .lock_manager import DistributedLockManager
from .models import URLStateModel
from .state_manager import URLStateManager

__all__ = [
    "DynamoDBClient",
    "get_dynamodb_client",
    "reset_client",
    "DistributedLockManager",
    "URLStateModel",
    "URLStateManager",
    # LocalStack components
    "LocalStackDynamoDBClient",
    "LocalStackURLStateManager",
    "LocalStackDistributedLockManager",
    # Factory functions
    "create_state_manager",
    "create_lock_manager",
]


def create_state_manager(crawler_id: str, settings: CrawlerSettings):
    """
    Factory function to create appropriate state manager based on environment.

    Args:
        crawler_id: Unique crawler identifier
        settings: CrawlerSettings instance

    Returns:
        URLStateManager or LocalStackURLStateManager based on environment
    """
    if settings.environment == "devlocal" and settings.localstack_endpoint:
        # Use LocalStack-compatible implementations
        return LocalStackURLStateManager(crawler_id, settings)
    else:
        # Use standard PynamoDB implementations
        return URLStateManager(crawler_id)


def create_lock_manager(crawler_id: str, settings: CrawlerSettings):
    """
    Factory function to create appropriate lock manager based on environment.

    Args:
        crawler_id: Unique crawler identifier
        settings: CrawlerSettings instance

    Returns:
        DistributedLockManager or LocalStackDistributedLockManager
    """
    if settings.environment == "devlocal" and settings.localstack_endpoint:
        # Use LocalStack-compatible implementation
        client = LocalStackDynamoDBClient(settings)
        return LocalStackDistributedLockManager(crawler_id, client)
    else:
        # Use standard PynamoDB implementation
        return DistributedLockManager(crawler_id)
