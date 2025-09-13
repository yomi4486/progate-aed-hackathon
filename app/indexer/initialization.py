"""
Indexer service initialization routines.

Handles startup initialization tasks like setting up OpenSearch templates,
checking connections, and preparing the service for operation.
"""

import asyncio
import logging
from typing import Optional, Tuple

from .bedrock_client import BedrockClient
from .config import IndexerConfig
from .index_templates import create_index_with_template
from .opensearch_client import OpenSearchClient

logger = logging.getLogger(__name__)


async def initialize_opensearch_templates(
    opensearch_client: OpenSearchClient, environment: str = "dev", embedding_dimension: int = 1536
) -> bool:
    """
    Initialize OpenSearch index templates if they don't exist.

    Args:
        opensearch_client: OpenSearch client instance
        environment: Environment name
        embedding_dimension: Vector embedding dimension

    Returns:
        True if templates are ready, False if failed
    """
    try:
        logger.info(f"Initializing OpenSearch templates for environment: {environment}")

        # Check if OpenSearch is healthy
        healthy = await opensearch_client.health_check()
        if not healthy:
            logger.warning("OpenSearch cluster is not healthy, but continuing...")

        # Create templates and initial index
        success = await create_index_with_template(
            opensearch_client, environment=environment, embedding_dimension=embedding_dimension
        )

        if success:
            logger.info("OpenSearch templates initialized successfully")
        else:
            logger.error("Failed to initialize OpenSearch templates")

        return success

    except Exception as e:
        logger.error(f"Error initializing OpenSearch templates: {e}")
        return False


async def check_service_dependencies(config: IndexerConfig) -> Tuple[bool, bool]:
    """
    Check if all service dependencies are available.

    Args:
        config: Indexer configuration

    Returns:
        Tuple of (opensearch_ready, bedrock_ready)
    """
    opensearch_ready = False
    bedrock_ready = False

    try:
        # Check OpenSearch
        logger.info("Checking OpenSearch connection...")
        opensearch_client = OpenSearchClient(config.opensearch_config)
        opensearch_ready = await opensearch_client.health_check()
        await opensearch_client.close()

        if opensearch_ready:
            logger.info("OpenSearch is ready")
        else:
            logger.warning("OpenSearch is not ready")

        # Check Bedrock if enabled
        if config.bedrock_config and config.enable_embeddings:
            logger.info("Checking Bedrock connection...")
            bedrock_client = BedrockClient(config.bedrock_config)
            bedrock_ready = await bedrock_client.test_connection()

            if bedrock_ready:
                logger.info("Bedrock is ready")
            else:
                logger.warning("Bedrock is not ready")
        else:
            logger.info("Bedrock embeddings disabled, skipping check")
            bedrock_ready = True  # Consider it ready if not needed

    except Exception as e:
        logger.error(f"Error checking service dependencies: {e}")

    return opensearch_ready, bedrock_ready


async def initialize_indexer_service(config: IndexerConfig) -> bool:
    """
    Complete initialization routine for the indexer service.

    Args:
        config: Indexer configuration

    Returns:
        True if initialization successful, False otherwise
    """
    logger.info("Starting indexer service initialization...")

    try:
        # Check dependencies
        opensearch_ready, bedrock_ready = await check_service_dependencies(config)

        if not opensearch_ready:
            logger.error("OpenSearch is not ready - indexer cannot start")
            return False

        if not bedrock_ready and config.enable_embeddings:
            logger.warning("Bedrock is not ready - embeddings will be disabled")

        # Initialize OpenSearch templates
        opensearch_client = OpenSearchClient(config.opensearch_config)

        # Determine embedding dimension
        embedding_dimension = 1536  # Default Titan v1
        if config.bedrock_config and bedrock_ready:
            try:
                bedrock_client = BedrockClient(config.bedrock_config)
                embedding_dimension = bedrock_client.get_embedding_dimension()
                logger.info(f"Using embedding dimension: {embedding_dimension}")
            except Exception as e:
                logger.warning(f"Could not get embedding dimension, using default: {e}")

        # Initialize templates
        template_success = await initialize_opensearch_templates(
            opensearch_client, environment=config.environment, embedding_dimension=embedding_dimension
        )

        await opensearch_client.close()

        if not template_success:
            logger.error("Failed to initialize OpenSearch templates")
            return False

        logger.info("Indexer service initialization completed successfully")
        return True

    except Exception as e:
        logger.error(f"Indexer service initialization failed: {e}")
        return False


async def graceful_shutdown():
    """
    Perform graceful shutdown tasks.
    """
    logger.info("Starting graceful shutdown...")

    # Give time for in-flight operations to complete
    await asyncio.sleep(2)

    logger.info("Graceful shutdown completed")


def setup_startup_hooks():
    """
    Set up startup hooks and signal handlers.
    """
    import signal

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        # Note: In a real service, this would trigger the main loop to exit cleanly

    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


# Helper function for environment-specific initialization
async def initialize_for_environment(
    environment: str,
    opensearch_endpoint: str,
    opensearch_username: Optional[str] = None,
    opensearch_password: Optional[str] = None,
    bedrock_region: str = "ap-northeast-1",
    enable_embeddings: bool = True,
) -> bool:
    """
    Initialize indexer service for a specific environment.

    Args:
        environment: Environment name (dev/staging/prod)
        opensearch_endpoint: OpenSearch endpoint URL
        opensearch_username: Optional username
        opensearch_password: Optional password
        bedrock_region: AWS region for Bedrock
        enable_embeddings: Whether to enable embedding generation

    Returns:
        True if initialization successful, False otherwise
    """
    from .config import BedrockConfig, OpenSearchConfig

    # Create configuration
    opensearch_config = OpenSearchConfig(
        endpoint=opensearch_endpoint,
        index_name=f"documents-{environment}",
        username=opensearch_username,
        password=opensearch_password,
        use_ssl=opensearch_endpoint.startswith("https://"),
        verify_certs=not opensearch_endpoint.startswith("http://localhost"),
    )

    bedrock_config = None
    if enable_embeddings:
        bedrock_config = BedrockConfig(region=bedrock_region, embedding_model="amazon.titan-embed-text-v1")

    indexer_config = IndexerConfig(
        environment=environment,
        opensearch_config=opensearch_config,
        bedrock_config=bedrock_config,
        enable_embeddings=enable_embeddings,
    )

    # Initialize service
    return await initialize_indexer_service(indexer_config)


if __name__ == "__main__":
    import os
    import sys

    # Simple CLI for testing initialization
    environment = os.getenv("INDEXER_ENVIRONMENT", "dev")
    opensearch_endpoint = os.getenv("INDEXER_OPENSEARCH_ENDPOINT")

    if not opensearch_endpoint:
        print("Error: INDEXER_OPENSEARCH_ENDPOINT environment variable required")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    async def run_init():
        success = await initialize_for_environment(
            environment=environment,
            opensearch_endpoint=opensearch_endpoint,
            opensearch_username=os.getenv("INDEXER_OPENSEARCH_USERNAME"),
            opensearch_password=os.getenv("INDEXER_OPENSEARCH_PASSWORD"),
        )

        if success:
            print("✅ Indexer initialization successful")
            sys.exit(0)
        else:
            print("❌ Indexer initialization failed")
            sys.exit(1)

    asyncio.run(run_init())
