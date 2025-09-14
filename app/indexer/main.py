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
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_sqs.type_defs import MessageTypeDef

from ..crawler.storage.pipeline import IndexingMessage
from .bedrock_client import BedrockClient
from .config import IndexerConfig
from .document_processor import DocumentProcessor, ProcessedDocument
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
        self.sqs = boto3.client("sqs", region_name=config.aws_region)  # type: ignore
        self.s3 = boto3.client("s3", region_name=config.aws_region)  # type: ignore

        # Initialize clients
        self.opensearch_client = OpenSearchClient(config.opensearch_config)
        self.bedrock_client = (
            BedrockClient(config.bedrock_config) if config.enable_embeddings and config.bedrock_config else None
        )
        self.document_processor = DocumentProcessor(config)

        # Initialize new components
        from .dlq_handler import DLQHandler
        from .metrics_collector import MetricsCollector
        from .text_chunker import TextChunker

        self.dlq_handler = DLQHandler(config.dlq_config, config.aws_region) if config.dlq_config else None
        self.metrics_collector = MetricsCollector(config.metrics_config) if config.metrics_config else None
        self.text_chunker = TextChunker(config.chunking_config) if config.chunking_config else None

        # Configure OpenSearch embedding dimension if Bedrock is enabled
        if self.bedrock_client:
            embedding_dim = self.bedrock_client.get_embedding_dimension()
            self.opensearch_client.set_embedding_dimension(embedding_dim)

        # Runtime stats
        self.processed_count = 0
        self.error_count = 0
        self.running = False
        self.start_time = time.time()

    async def start(self):
        """Start the indexer service."""
        logger.info("Starting indexer service...")
        logger.info(f"Queue URL: {self.config.sqs_indexing_queue_url}")
        logger.info(f"OpenSearch endpoint: {self.config.opensearch_config.endpoint}")
        logger.info(f"Embeddings enabled: {self.config.enable_embeddings}")
        logger.info(f"DLQ enabled: {self.dlq_handler is not None}")
        logger.info(f"Metrics enabled: {self.metrics_collector is not None}")
        logger.info(f"Text chunking enabled: {self.text_chunker is not None}")

        # Initialize service dependencies
        from .initialization import initialize_indexer_service

        init_success = await initialize_indexer_service(self.config)
        if not init_success:
            logger.error("Failed to initialize indexer service")
            return

        # Start metrics server if enabled
        if self.metrics_collector:
            await self.metrics_collector.start_metrics_server()

        self.running = True

        # Start processing loop
        while self.running:
            try:
                await self._process_queue_batch()

                # Process retries if DLQ is enabled
                if self.dlq_handler:
                    await self.dlq_handler.process_retries()

                await asyncio.sleep(self.config.poll_interval_seconds)
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Error in main processing loop: {e}")
                self.error_count += 1
                if self.metrics_collector:
                    self.metrics_collector.record_error("service")
                await asyncio.sleep(5)  # Wait before retrying

        # Graceful shutdown
        await self._graceful_shutdown()
        logger.info(f"Indexer service stopped. Processed: {self.processed_count}, Errors: {self.error_count}")

    async def _process_queue_batch(self):
        """Process a batch of messages from the SQS queue."""
        try:
            # Receive messages from SQS
            response = self.sqs.receive_message(
                QueueUrl=self.config.sqs_indexing_queue_url,
                MaxNumberOfMessages=min(self.config.batch_size, 10),  # SQS max is 10
                WaitTimeSeconds=self.config.long_poll_seconds,
                MessageAttributeNames=["All"],
            )

            messages: List[MessageTypeDef] = response.get("Messages", [])
            if not messages:
                return

            logger.info(f"Processing {len(messages)} indexing messages")

            # Update queue metrics
            if self.metrics_collector:
                self.metrics_collector.update_queue_metrics(queue_depth=len(messages), messages_in_flight=len(messages))

            # Process messages concurrently
            tasks = [self._process_message_with_metrics(msg) for msg in messages]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle results
            for i, result in enumerate(results):
                message = messages[i]
                if isinstance(result, Exception):
                    logger.error(f"Error processing message {i}: {result}")
                    self.error_count += 1

                    # Handle with DLQ if enabled
                    if self.dlq_handler:
                        should_delete = await self.dlq_handler.handle_failed_message(dict(message), result, 0)
                        if should_delete:
                            await self._delete_message(message)
                    else:
                        # Delete message anyway to prevent infinite reprocessing
                        await self._delete_message(message)

                else:
                    # Delete successfully processed message
                    await self._delete_message(message)
                    self.processed_count += 1

        except ClientError as e:
            logger.error(f"Error receiving messages from SQS: {e}")
            if self.metrics_collector:
                self.metrics_collector.record_error("sqs")
        except Exception as e:
            logger.error(f"Unexpected error in _process_queue_batch: {e}")
            if self.metrics_collector:
                self.metrics_collector.record_error("service")

    async def _process_message_with_metrics(self, sqs_message: MessageTypeDef) -> bool:
        """Process a single message with metrics tracking."""
        message_id = sqs_message.get("MessageId", "unknown")

        # Start metrics timer
        if self.metrics_collector:
            self.metrics_collector.start_processing_timer(message_id)

        try:
            result = await self._process_message(sqs_message)

            # End metrics timer with success
            if self.metrics_collector:
                self.metrics_collector.end_processing_timer(message_id, success=True)

            return result
        except Exception as e:
            # End metrics timer with failure
            if self.metrics_collector:
                self.metrics_collector.end_processing_timer(message_id, success=False)
            raise e

    async def _process_message(self, sqs_message: MessageTypeDef) -> bool:
        """Process a single indexing message."""
        try:
            # Parse the IndexingMessage
            body_content = sqs_message.get("Body")
            if not body_content:
                logger.error("Message has no body content")
                return False
            body = json.loads(body_content)
            indexing_msg = IndexingMessage(**body)

            logger.info(f"Processing indexing message for URL: {indexing_msg.url}")

            # Download parsed content from S3
            if not indexing_msg.parsed_s3_key:
                logger.error(f"No parsed S3 key found for {indexing_msg.url}")
                return False
            parsed_content = await self._download_s3_content(self.config.s3_parsed_bucket, indexing_msg.parsed_s3_key)

            if not parsed_content:
                logger.warning(f"No parsed content found for {indexing_msg.url}")
                return True  # Delete message - nothing to process

            # Process the document
            document = await self.document_processor.process_document(indexing_msg, parsed_content)

            # Check if document should be chunked
            if self.text_chunker and self.text_chunker.should_chunk(document.content):
                await self._process_chunked_document(document, indexing_msg)
            else:
                await self._process_single_document(document)

            logger.info(f"Successfully processed document: {indexing_msg.url}")
            return True

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    async def _process_single_document(self, document: ProcessedDocument) -> None:
        """Process a single document (no chunking)."""
        # Generate embeddings if enabled
        if self.bedrock_client and self.config.enable_embeddings:
            embeddings = await self.bedrock_client.generate_embeddings(document.content)
            if embeddings:
                document.embedding = embeddings
                if self.metrics_collector:
                    self.metrics_collector.record_embedding_generated()

        # Index to OpenSearch
        success = await self.opensearch_client.index_document(document)
        if success:
            if self.metrics_collector:
                self.metrics_collector.record_document_indexed()
        else:
            if self.metrics_collector:
                self.metrics_collector.record_error("opensearch")
            raise Exception("Failed to index document to OpenSearch")

    async def _process_chunked_document(self, document: ProcessedDocument, indexing_msg: IndexingMessage) -> None:
        """Process a document by chunking it into smaller pieces."""
        if not self.text_chunker:
            logger.error("Text chunker not available for chunked document processing")
            return

        chunks = self.text_chunker.chunk_text(
            document.content, metadata={"original_url": document.url, "original_document_id": document.document_id}
        )

        logger.info(f"Processing {len(chunks)} chunks for document: {document.url}")

        # Process each chunk
        for chunk in chunks:
            # Create a new document for each chunk
            chunk_document = ProcessedDocument(
                document_id=f"{document.document_id}_chunk_{chunk.chunk_index}",
                url=document.url,
                url_hash=document.url_hash,
                domain=document.domain,
                title=f"{document.title} (Part {chunk.chunk_index + 1})",
                content=chunk.content,
                content_type=document.content_type,
                language=chunk.language if chunk.language != "unknown" else document.language,
                fetched_at=document.fetched_at,
                indexed_at=document.indexed_at,
                content_length=len(chunk.content),
                processing_priority=document.processing_priority,
                status_code=document.status_code,
                keywords=document.keywords,  # Inherit from parent
                categories=document.categories,  # Inherit from parent
                raw_s3_key=document.raw_s3_key,
                parsed_s3_key=document.parsed_s3_key,
            )

            await self._process_single_document(chunk_document)

    async def _delete_message(self, sqs_message: MessageTypeDef) -> None:
        """Delete a message from SQS."""
        try:
            self.sqs.delete_message(
                QueueUrl=self.config.sqs_indexing_queue_url, ReceiptHandle=sqs_message.get("ReceiptHandle", "")
            )
        except ClientError as e:
            logger.error(f"Error deleting message: {e}")
            if self.metrics_collector:
                self.metrics_collector.record_error("sqs")

    async def _download_s3_content(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
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
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchKey":
                logger.warning(f"S3 object not found: s3://{bucket}/{key}")
                return None
            logger.error(f"Error downloading from S3: {e}")
            if self.metrics_collector:
                self.metrics_collector.record_error("s3")
            return None
        except Exception as e:
            logger.error(f"Error parsing S3 content: {e}")
            if self.metrics_collector:
                self.metrics_collector.record_error("s3")
            return None

    async def _graceful_shutdown(self):
        """Perform graceful shutdown operations."""
        logger.info("Starting graceful shutdown...")

        # Drain retry queue to DLQ if enabled
        if self.dlq_handler:
            await self.dlq_handler.drain_retry_queue()

        # Close OpenSearch client
        if self.opensearch_client:
            await self.opensearch_client.close()

        logger.info("Graceful shutdown completed")

    def stop(self):
        """Stop the indexer service gracefully."""
        self.running = False

    def get_service_status(self) -> Dict[str, Any]:
        """Get current service status."""
        status: Dict[str, Any] = {
            "running": self.running,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "uptime_seconds": time.time() - (self.start_time if hasattr(self, "start_time") else time.time()),
        }

        # Add metrics if available
        if self.metrics_collector:
            status["metrics"] = self.metrics_collector.get_metrics_dict()
            status["health"] = self.metrics_collector.get_health_status()

        # Add DLQ stats if available
        if self.dlq_handler:
            status["dlq_stats"] = self.dlq_handler.get_retry_statistics()

        return status


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
