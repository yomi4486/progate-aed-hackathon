"""
OpenSearch setup and initialization script.

Sets up index templates and initial indices for the search system.
Can be run as a CLI tool or imported as a module for initialization.
"""

import asyncio
import logging
import sys

from .config import OpenSearchConfig
from .index_templates import create_index_with_template
from .opensearch_client import OpenSearchClient, ProcessedDocument

logger = logging.getLogger(__name__)


async def setup_opensearch_for_environment(
    opensearch_config: OpenSearchConfig,
    environment: str = "dev",
    embedding_dimension: int = 1536,
    force_recreate: bool = False,
) -> bool:
    """
    Set up OpenSearch index templates and indices for an environment.

    Args:
        opensearch_config: OpenSearch configuration
        environment: Environment name (dev/staging/prod)
        embedding_dimension: Vector embedding dimension
        force_recreate: Whether to recreate existing templates/indices

    Returns:
        True if setup successful, False otherwise
    """
    try:
        # Initialize OpenSearch client
        client = OpenSearchClient(opensearch_config)

        # Check connection
        healthy = await client.health_check()
        if not healthy:
            logger.error("OpenSearch cluster is not healthy")
            return False

        logger.info(f"Setting up OpenSearch for environment: {environment}")

        # Create index template and initial index
        success = await create_index_with_template(
            client, environment=environment, embedding_dimension=embedding_dimension
        )

        if success:
            logger.info(f"Successfully set up OpenSearch for environment: {environment}")
        else:
            logger.error(f"Failed to set up OpenSearch for environment: {environment}")

        # Close client
        await client.close()

        return success

    except Exception as e:
        logger.error(f"Error setting up OpenSearch: {e}")
        return False


async def verify_opensearch_setup(opensearch_config: OpenSearchConfig, environment: str = "dev") -> bool:
    """
    Verify OpenSearch setup by testing basic operations.

    Args:
        opensearch_config: OpenSearch configuration
        environment: Environment name

    Returns:
        True if verification successful, False otherwise
    """
    try:
        client = OpenSearchClient(opensearch_config)

        # Test index operations
        from .index_templates import SAMPLE_DOCUMENT

        # Index a sample document
        doc_id = "test-doc-001"
        document = ProcessedDocument(**SAMPLE_DOCUMENT, id=doc_id)  # type: ignore
        success = await client.index_document(document)

        if not success:
            logger.error("Failed to index test document")
            return False

        # Wait for index refresh
        await asyncio.sleep(2)

        # Search for the document
        search_results = await client.search("サンプル", size=5)

        if not search_results.get("hits", {}).get("hits", []):
            logger.error("Failed to find test document in search results")
            return False

        logger.info("OpenSearch verification successful")

        # Clean up test document
        # Note: We could delete it here, but leaving it for debugging

        await client.close()
        return True

    except Exception as e:
        logger.error(f"Error verifying OpenSearch setup: {e}")
        return False


def main():
    """CLI entry point for OpenSearch setup."""
    import argparse

    parser = argparse.ArgumentParser(description="Set up OpenSearch index templates and indices")
    parser.add_argument(
        "--environment", "-e", default="dev", choices=["dev", "staging", "prod"], help="Environment to set up"
    )
    parser.add_argument("--endpoint", required=True, help="OpenSearch endpoint URL")
    parser.add_argument("--username", help="OpenSearch username (if required)")
    parser.add_argument("--password", help="OpenSearch password (if required)")
    parser.add_argument("--embedding-dimension", type=int, default=1536, help="Vector embedding dimension")
    parser.add_argument("--verify", action="store_true", help="Run verification tests after setup")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Create OpenSearch configuration
    opensearch_config = OpenSearchConfig(
        endpoint=args.endpoint,
        index_name=f"documents-{args.environment}",  # Default index name
        username=args.username,
        password=args.password,
        use_ssl=args.endpoint.startswith("https://"),
        verify_certs=not args.endpoint.startswith("http://localhost"),
    )

    async def run_setup():
        try:
            # Set up OpenSearch
            success = await setup_opensearch_for_environment(
                opensearch_config=opensearch_config,
                environment=args.environment,
                embedding_dimension=args.embedding_dimension,
            )

            if not success:
                logger.error("OpenSearch setup failed")
                return 1

            # Run verification if requested
            if args.verify:
                logger.info("Running verification tests...")
                verify_success = await verify_opensearch_setup(
                    opensearch_config=opensearch_config, environment=args.environment
                )

                if not verify_success:
                    logger.error("OpenSearch verification failed")
                    return 1

            logger.info("OpenSearch setup completed successfully")
            return 0

        except KeyboardInterrupt:
            logger.info("Setup interrupted by user")
            return 1
        except Exception as e:
            logger.error(f"Setup failed with error: {e}")
            return 1

    # Run async setup
    exit_code = asyncio.run(run_setup())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
