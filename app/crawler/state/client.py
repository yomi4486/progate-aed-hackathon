"""
DynamoDB client initialization and management for the distributed crawler.

Provides centralized configuration, connection pooling, error handling,
and LocalStack support for all DynamoDB operations.
"""

import logging
import os
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type, TypeVar

from botocore.exceptions import (
    ClientError,
    ConnectionError,
    EndpointConnectionError,
    NoCredentialsError,
    ReadTimeoutError,
)
from pynamodb.exceptions import DoesNotExist, TableError
from pynamodb.models import Model

from ..config.settings import CrawlerSettings
from ..utils.retry import DATABASE_RETRY_CONFIG, AsyncRetrier, RetryError

logger = logging.getLogger(__name__)

# Type variable for PynamoDB models
ModelType = TypeVar("ModelType", bound=Model)


class DynamoDBError(Exception):
    """Base exception for DynamoDB operations"""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        self.original_error = original_error
        super().__init__(message)


class ThrottlingError(DynamoDBError):
    """Raised when DynamoDB requests are being throttled"""

    pass


class CapacityExceededError(DynamoDBError):
    """Raised when table capacity is exceeded"""

    pass


class ConditionalCheckFailedError(DynamoDBError):
    """Raised when conditional write fails (used for distributed locking)"""

    pass


class DynamoDBClient:
    """
    Enhanced DynamoDB client with connection pooling, error handling,
    and retry logic specifically designed for the distributed crawler.
    """

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.retrier = AsyncRetrier(DATABASE_RETRY_CONFIG)

        # Configure PynamoDB settings
        self._configure_pynamodb()

        # Initialize connection pool
        self._init_connection_pool()

        logger.info(f"DynamoDB client initialized for region {settings.aws_region}")

    def _configure_pynamodb(self) -> None:
        """Configure PynamoDB global settings"""
        # Region to environment so botocore can resolve
        os.environ.setdefault("AWS_DEFAULT_REGION", self.settings.aws_region)

        # LocalStack endpoint is handled at Model.Meta.host; see initialize_models()
        # Set credentials via environment if provided (botocore default chain)
        if self.settings.aws_access_key_id:
            os.environ["AWS_ACCESS_KEY_ID"] = self.settings.aws_access_key_id
        if self.settings.aws_secret_access_key:
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.settings.aws_secret_access_key
        if self.settings.aws_session_token:
            os.environ["AWS_SESSION_TOKEN"] = self.settings.aws_session_token

        # Ensure models are configured with current settings (table, region, host, tags)
        from .models import initialize_models

        initialize_models()

    def _init_connection_pool(self) -> None:
        """Initialize connection pool settings"""
        # PynamoDB handles connection pooling internally
        # We just ensure proper configuration is applied
        pass

    async def handle_error(self, error: Exception, operation: str) -> None:
        """
        Centralized error handling for DynamoDB operations.

        Args:
            error: The exception that occurred
            operation: Description of the operation that failed

        Raises:
            Appropriate DynamoDBError subclass based on the original error
        """
        if isinstance(error, ClientError):
            error_code = error.response.get("Error", {}).get("Code", "Unknown")
            error_message = error.response.get("Error", {}).get("Message", str(error))

            if error_code == "ProvisionedThroughputExceededException":
                raise ThrottlingError(f"DynamoDB throughput exceeded during {operation}: {error_message}", error)
            elif error_code == "ConditionalCheckFailedException":
                raise ConditionalCheckFailedError(
                    f"Conditional check failed during {operation}: {error_message}", error
                )
            elif error_code == "LimitExceededException":
                raise CapacityExceededError(
                    f"DynamoDB capacity limit exceeded during {operation}: {error_message}", error
                )
            elif error_code == "ResourceNotFoundException":
                raise DynamoDBError(f"DynamoDB resource not found during {operation}: {error_message}", error)
            else:
                raise DynamoDBError(f"DynamoDB error during {operation}: {error_message}", error)

        elif isinstance(error, (ConnectionError, EndpointConnectionError, ReadTimeoutError)):
            raise DynamoDBError(f"DynamoDB connection error during {operation}: {str(error)}", error)

        elif isinstance(error, NoCredentialsError):
            raise DynamoDBError(f"AWS credentials not configured for DynamoDB {operation}", error)

        elif isinstance(error, TableError):
            raise DynamoDBError(f"PynamoDB error during {operation}: {str(error)}", error)

        else:
            raise DynamoDBError(f"Unexpected error during {operation}: {str(error)}", error)

    async def execute_with_retry(
        self,
        operation_func: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a DynamoDB operation with retry logic.

        Args:
            operation_func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the operation

        Raises:
            DynamoDBError: If operation fails after all retries
        """
        operation_name = getattr(operation_func, "__name__", "unknown_operation")

        try:
            exceptions_tuple: tuple[type[Exception], ...] = (
                ClientError,
                ConnectionError,
                EndpointConnectionError,
                ReadTimeoutError,
                ThrottlingError,
            )
            retrier_any: Any = self.retrier
            return await retrier_any.call(
                operation_func,
                *args,
                exceptions=exceptions_tuple,
                **kwargs,
            )
        except RetryError as e:
            await self.handle_error(e.last_exception, operation_name)
        except Exception as e:
            await self.handle_error(e, operation_name)

    async def get_item(
        self,
        model_class: Type[ModelType],
        hash_key: Any,
        range_key: Optional[Any] = None,
        **kwargs: Any,
    ) -> Optional[ModelType]:
        """
        Get a single item from DynamoDB.

        Args:
            model_class: PynamoDB model class
            hash_key: Hash key value
            range_key: Range key value (if applicable)
            **kwargs: Additional query parameters

        Returns:
            Model instance or None if not found
        """

        async def _get_item():
            try:
                if range_key is not None:
                    return model_class.get(hash_key, range_key, **kwargs)
                else:
                    return model_class.get(hash_key, **kwargs)
            except DoesNotExist:
                return None

        return await self.execute_with_retry(_get_item)

    async def put_item(self, item: Model, condition: Optional[Any] = None, **kwargs: Any) -> None:
        """
        Put an item to DynamoDB.

        Args:
            item: Model instance to save
            condition: Conditional expression
            **kwargs: Additional save parameters
        """

        async def _put_item():
            if condition is not None:
                item.save(condition=condition, **kwargs)
            else:
                item.save(**kwargs)

        await self.execute_with_retry(_put_item)

    async def update_item(
        self, item: Model, actions: List[Any], condition: Optional[Any] = None, **kwargs: Any
    ) -> None:
        """
        Update an item in DynamoDB.

        Args:
            item: Model instance to update
            actions: List of update actions
            condition: Conditional expression
            **kwargs: Additional update parameters
        """

        async def _update_item():
            if condition is not None:
                item.update(actions, condition=condition, **kwargs)
            else:
                item.update(actions, **kwargs)

        await self.execute_with_retry(_update_item)

    async def query_items(
        self,
        model_class: Type[ModelType],
        hash_key: Any,
        range_key_condition: Optional[Any] = None,
        filter_condition: Optional[Any] = None,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> List[ModelType]:
        """
        Query items from DynamoDB.

        Args:
            model_class: PynamoDB model class
            hash_key: Hash key value
            range_key_condition: Range key condition
            filter_condition: Filter condition
            limit: Maximum number of items to return
            **kwargs: Additional query parameters

        Returns:
            List of model instances
        """

        async def _query_items():
            query = model_class.query(
                hash_key,
                range_key_condition=range_key_condition,
                filter_condition=filter_condition,
                limit=limit,
                **kwargs,
            )
            return list(query)

        return await self.execute_with_retry(_query_items)

    async def scan_items(
        self,
        model_class: Type[ModelType],
        filter_condition: Optional[Any] = None,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> List[ModelType]:
        """
        Scan items from DynamoDB.

        Args:
            model_class: PynamoDB model class
            filter_condition: Filter condition
            limit: Maximum number of items to return
            **kwargs: Additional scan parameters

        Returns:
            List of model instances
        """

        async def _scan_items():
            scan = model_class.scan(filter_condition=filter_condition, limit=limit, **kwargs)
            return list(scan)

        return await self.execute_with_retry(_scan_items)

    async def batch_get_items(
        self,
        model_class: Type[ModelType],
        keys: List[Any],  # Changed to support hash-only tables
        **kwargs: Any,
    ) -> List[ModelType]:
        """
        Batch get items from DynamoDB.

        Args:
            model_class: PynamoDB model class
            keys: List of hash keys (for hash-only tables) or (hash_key, range_key) tuples
            **kwargs: Additional batch get parameters

        Returns:
            List of model instances
        """

        async def _batch_get_items() -> List[ModelType]:
            items: List[ModelType] = []
            # Process in chunks of 100 (DynamoDB limit)
            chunk_size = 100
            for i in range(0, len(keys), chunk_size):
                chunk_keys = keys[i : i + chunk_size]
                batch_items = list(model_class.batch_get(chunk_keys, **kwargs))
                items.extend(batch_items)
            return items

        return await self.execute_with_retry(_batch_get_items)

    async def batch_write_items(
        self, items_to_save: List[Model], items_to_delete: Optional[List[Model]] = None, **kwargs: Any
    ) -> None:
        """
        Batch write items to DynamoDB.

        Args:
            items_to_save: List of items to save
            items_to_delete: List of items to delete
            **kwargs: Additional batch write parameters
        """

        async def _batch_write_items():
            # Process saves in chunks of 25 (DynamoDB limit)
            chunk_size = 25

            # Handle saves
            if items_to_save:
                for i in range(0, len(items_to_save), chunk_size):
                    chunk_items = items_to_save[i : i + chunk_size]
                    with items_to_save[0].__class__.batch_write(**kwargs) as batch:
                        for item in chunk_items:
                            batch.save(item)

            # Handle deletes
            if items_to_delete:
                for i in range(0, len(items_to_delete), chunk_size):
                    chunk_items = items_to_delete[i : i + chunk_size]
                    with items_to_delete[0].__class__.batch_write(**kwargs) as batch:
                        for item in chunk_items:
                            batch.delete(item)

        await self.execute_with_retry(_batch_write_items)

    async def create_table_if_not_exists(
        self,
        model_class: Type[ModelType],
        read_capacity_units: int = 5,
        write_capacity_units: int = 5,
        **kwargs: Any,
    ) -> bool:
        """
        Create table if it doesn't exist.

        Args:
            model_class: PynamoDB model class
            read_capacity_units: Read capacity units
            write_capacity_units: Write capacity units
            **kwargs: Additional table creation parameters

        Returns:
            True if table was created, False if it already existed
        """

        async def _create_table():
            if not model_class.exists():
                model_class.create_table(
                    read_capacity_units=read_capacity_units,
                    write_capacity_units=write_capacity_units,
                    wait=True,
                    **kwargs,
                )
                logger.info(f"Created DynamoDB table: {model_class.Meta.table_name}")
                return True
            else:
                logger.info(f"DynamoDB table already exists: {model_class.Meta.table_name}")
                return False

        return await self.execute_with_retry(_create_table)

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        stats: Dict[str, Any] = dict(self.retrier.get_stats())  # type: ignore[call-overload]
        stats.update(
            {
                "region": self.settings.aws_region,
                "localstack_enabled": bool(self.settings.localstack_endpoint),
                "table_name": self.settings.dynamodb_table,
            }
        )
        return stats

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on DynamoDB connectivity.

        Returns:
            Health status dictionary
        """
        try:
            # Try to describe the main URL state table
            from .models import URLStateModel

            async def _health_check():
                return URLStateModel.exists()

            exists = await self.execute_with_retry(_health_check)

            return {
                "status": "healthy" if exists else "degraded",
                "table_exists": exists,
                "region": self.settings.aws_region,
                "localstack": bool(self.settings.localstack_endpoint),
            }

        except Exception as e:
            logger.error(f"DynamoDB health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "region": self.settings.aws_region,
                "localstack": bool(self.settings.localstack_endpoint),
            }


# Global client instance
_client: Optional[DynamoDBClient] = None


def get_dynamodb_client(settings: Optional[CrawlerSettings] = None) -> DynamoDBClient:
    """
    Get the global DynamoDB client instance.

    Args:
        settings: Optional settings override

    Returns:
        DynamoDB client instance
    """
    global _client

    if _client is None or settings is not None:
        from ..config.settings import get_cached_settings

        if settings is None:
            settings = get_cached_settings()

        _client = DynamoDBClient(settings)

    return _client


def reset_client() -> None:
    """Reset the global client instance (useful for testing)"""
    global _client
    _client = None


if __name__ == "__main__":
    # CLI utility for testing client functionality
    import asyncio
    import sys

    from ..config.settings import load_settings

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python client.py [health|stats|test]")
            sys.exit(1)

        command = sys.argv[1]

        settings = load_settings()
        client = DynamoDBClient(settings)

        if command == "health":
            health = await client.health_check()
            print(f"Health status: {health}")

        elif command == "stats":
            stats = client.get_stats()
            print(f"Client stats: {stats}")

        elif command == "test":
            # Test basic operations
            from .models import URLStateModel

            print("Testing DynamoDB client...")

            # Try to create table
            created = await client.create_table_if_not_exists(URLStateModel)
            print(f"Table created: {created}")

            # Try to query (should return empty list)
            items = await client.scan_items(URLStateModel, limit=1)
            print(f"Found {len(items)} items in table")

            print("DynamoDB client test completed!")

        else:
            print(f"Unknown command: {command}")
            sys.exit(1)

    asyncio.run(main())
