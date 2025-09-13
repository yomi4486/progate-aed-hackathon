"""
Main indexer service for processing SQS indexing queue.

This service:
1. Receives IndexingMessage from SQS queue
2. Downloads parsed content from S3
3. Generates embeddings via Bedrock
4. Indexes documents to OpenSearch
5. Deletes processed messages from SQS
"""

import asyncio
import json
import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from ..crawler.storage.pipeline import IndexingMessage
from .bedrock_client import BedrockClient
from .config import IndexerConfig
from .document_processor import DocumentProcessor
from .opensearch_client import OpenSearchClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class IndexerService:
    """
    Main indexer service that processes the indexing queue.
    """

    def __init__(self, config: IndexerConfig):
        self.config = config
        self.sqs = boto3.client("sqs", region_name=config.aws_region)
        self.s3 = boto3.client("s3", region_name=config.aws_region)

        # Initialize clients
        self.opensearch_client = OpenSearchClient(config.opensearch_config)
        self.bedrock_client = BedrockClient(config.bedrock_config) if config.enable_embeddings else None
        self.document_processor = DocumentProcessor(config)

        # Configure OpenSearch embedding dimension if Bedrock is enabled
        if self.bedrock_client:
            embedding_dim = self.bedrock_client.get_embedding_dimension()
            self.opensearch_client.set_embedding_dimension(embedding_dim)

        # Runtime stats
        self.processed_count = 0
        self.error_count = 0
        self.running = False

    async def start(self):
        """Start the indexer service."""
        logger.info("Starting indexer service...")
        logger.info(f"Queue URL: {self.config.sqs_indexing_queue_url}")
        logger.info(f"OpenSearch endpoint: {self.config.opensearch_config.endpoint}")
        logger.info(f"Embeddings enabled: {self.config.enable_embeddings}")

        # Initialize service dependencies
        from .initialization import initialize_indexer_service

        init_success = await initialize_indexer_service(self.config)
        if not init_success:
            logger.error("Failed to initialize indexer service")
            return

        self.running = True

        # Start processing loop
        while self.running:
            try:
                await self._process_queue_batch()
                await asyncio.sleep(self.config.poll_interval_seconds)
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Error in main processing loop: {e}")
                self.error_count += 1
                await asyncio.sleep(5)  # Wait before retrying

        logger.info(f"Indexer service stopped. Processed: {self.processed_count}, Errors: {self.error_count}")

    async def _process_queue_batch(self):
        """Process a batch of messages from the SQS queue."""
        try:
            # Receive messages from SQS
            response = self.sqs.receive_message(
                QueueUrl=self.config.sqs_indexing_queue_url,
                MaxNumberOfMessages=min(self.config.batch_size, 10),  # SQS max is 10
                WaitTimeSeconds=self.config.long_poll_seconds,
                VisibilityTimeoutSeconds=self.config.message_visibility_timeout,
            )

            messages = response.get("Messages", [])
            if not messages:
                return

            logger.info(f"Processing {len(messages)} indexing messages")

            # Process messages concurrently
            tasks = [self._process_message(msg) for msg in messages]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing message {i}: {result}")
                    self.error_count += 1
                else:
                    # Delete successfully processed message
                    try:
                        self.sqs.delete_message(
                            QueueUrl=self.config.sqs_indexing_queue_url, ReceiptHandle=messages[i]["ReceiptHandle"]
                        )
                        self.processed_count += 1
                    except ClientError as e:
                        logger.error(f"Error deleting message: {e}")

        except ClientError as e:
            logger.error(f"Error receiving messages from SQS: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in _process_queue_batch: {e}")

    async def _process_message(self, sqs_message: dict) -> bool:
        """Process a single indexing message."""
        try:
            # Parse the IndexingMessage
            body = json.loads(sqs_message["Body"])
            indexing_msg = IndexingMessage(**body)

            logger.info(f"Processing indexing message for URL: {indexing_msg.url}")

            # Download parsed content from S3
            parsed_content = await self._download_s3_content(self.config.s3_parsed_bucket, indexing_msg.parsed_s3_key)

            if not parsed_content:
                logger.warning(f"No parsed content found for {indexing_msg.url}")
                return True  # Delete message - nothing to process

            # Process the document
            document = await self.document_processor.process_document(indexing_msg, parsed_content)

            # Generate embeddings if enabled
            if self.bedrock_client and self.config.enable_embeddings:
                embeddings = await self.bedrock_client.generate_embeddings(document.content)
                document.embedding = embeddings

            # Index to OpenSearch
            success = await self.opensearch_client.index_document(document)

            if success:
                logger.info(f"Successfully indexed document: {indexing_msg.url}")
                return True
            else:
                logger.error(f"Failed to index document: {indexing_msg.url}")
                return False

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    async def _download_s3_content(self, bucket: str, key: str) -> Optional[dict]:
        """Download and parse content from S3."""
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read()

            # Handle gzipped content
            if key.endswith(".gz"):
                import gzip

                content = gzip.decompress(content)

            return json.loads(content.decode("utf-8"))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"S3 object not found: s3://{bucket}/{key}")
                return None
            logger.error(f"Error downloading from S3: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing S3 content: {e}")
            return None

    def stop(self):
        """Stop the indexer service gracefully."""
        self.running = False


async def main():
    """Main entry point for the indexer service."""
    # Load configuration
    config = IndexerConfig.from_environment()

    # Create and start indexer service
    indexer = IndexerService(config)

    try:
        await indexer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down indexer service...")
        indexer.stop()


if __name__ == "__main__":
    asyncio.run(main())
