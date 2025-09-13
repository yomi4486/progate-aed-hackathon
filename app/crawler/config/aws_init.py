"""
AWS services initialization for LocalStack and production environments.

Provides centralized configuration for PynamoDB and other AWS services
to support both LocalStack development and production deployments.
"""

import logging
import os
from typing import Any, Callable

from .settings import CrawlerSettings

logger = logging.getLogger(__name__)


def initialize_pynamodb_for_localstack(settings: CrawlerSettings) -> None:
    """
    Configure PynamoDB to use LocalStack endpoint for development.

    This must be called before importing or using any PynamoDB models.

    Args:
        settings: Crawler settings with LocalStack configuration
    """
    if not settings.localstack_endpoint:
        return

    logger.info(f"Configuring PynamoDB for LocalStack: {settings.localstack_endpoint}")

    # Set environment variables for AWS SDK
    os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id or "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key or "test"
    os.environ["AWS_DEFAULT_REGION"] = settings.aws_region

    # Try multiple approaches to configure PynamoDB for LocalStack
    try:
        # Method 1: Patch the connection creation directly
        import pynamodb.connection

        original_get_connection: Callable[[Any], Any] = pynamodb.connection.Connection._get_connection  # type: ignore

        def localstack_get_connection(self: Any) -> Any:
            """Override connection creation to use LocalStack"""
            # Force endpoint URL in the session config
            if hasattr(self, "session"):
                self.session.set_config_variable("dynamodb", "endpoint_url", settings.localstack_endpoint)

            # Call original method
            connection = original_get_connection(self)  # type: ignore

            # Override endpoint after connection is created
            if hasattr(connection, "_endpoint") and settings.localstack_endpoint:  # type: ignore
                connection._endpoint.endpoint_url = settings.localstack_endpoint  # type: ignore
                localstack_host = settings.localstack_endpoint.replace("http://", "").replace("https://", "")
                connection._endpoint.host = localstack_host  # type: ignore
                logger.debug(f"Patched connection endpoint to: {settings.localstack_endpoint}")

            return connection  # type: ignore

        # Patch the method
        pynamodb.connection.Connection._get_connection = localstack_get_connection  # type: ignore
        logger.debug("PynamoDB _get_connection method patched for LocalStack")

    except Exception as e:
        logger.debug(f"Method 1 failed: {e}")

        # Method 2: Try client creation override
        try:
            import boto3
            from pynamodb.connection import Connection

            # Store original client creation (not used but kept for potential restoration)
            getattr(Connection, "_get_client", None)  # type: ignore

            def localstack_get_client(self: Any, _operation_name: Any = None) -> Any:
                """Create DynamoDB client with LocalStack endpoint"""
                return boto3.client(  # type: ignore
                    "dynamodb",
                    endpoint_url=settings.localstack_endpoint,
                    region_name=self.region,
                    aws_access_key_id=settings.aws_access_key_id,
                    aws_secret_access_key=settings.aws_secret_access_key,
                )

            # Patch client creation
            Connection._get_client = localstack_get_client  # type: ignore
            logger.debug("PynamoDB _get_client patched for LocalStack")

        except Exception as e2:
            logger.warning(f"PynamoDB LocalStack configuration failed: {e2}")
            # Fall back to environment variables only
            os.environ["DYNAMODB_ENDPOINT"] = settings.localstack_endpoint


def initialize_aws_services(settings: CrawlerSettings) -> None:
    """
    Initialize all AWS services based on the environment configuration.

    Args:
        settings: Crawler settings
    """
    if settings.environment == "devlocal" and settings.localstack_endpoint:
        # Configure for LocalStack development
        initialize_pynamodb_for_localstack(settings)
        logger.info("AWS services configured for LocalStack development")
    else:
        # Production configuration - rely on IAM roles or environment variables
        logger.info(f"AWS services configured for {settings.environment} environment")


def update_dynamodb_table_name(settings: CrawlerSettings) -> None:
    """
    Update PynamoDB model table names and host settings based on settings.

    This allows us to use different table names and endpoints for different environments.

    Args:
        settings: Crawler settings with table names
    """
    try:
        from ..state.models import URLStateModel

        # Update the model's table name
        URLStateModel.Meta.table_name = settings.dynamodb_table

        # If using LocalStack, update the host
        if settings.localstack_endpoint:
            # Remove protocol and use just hostname:port
            localstack_host = settings.localstack_endpoint.replace("http://", "").replace("https://", "")
            URLStateModel.Meta.host = localstack_host  # type: ignore
            logger.debug(f"Updated DynamoDB endpoint to: {localstack_host}")

        logger.debug(f"Updated DynamoDB table name to: {settings.dynamodb_table}")

    except ImportError:
        logger.warning("URLStateModel not available, skipping table name update")
