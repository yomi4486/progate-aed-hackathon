"""
Configuration management for the indexer service.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class OpenSearchConfig:
    """OpenSearch client configuration."""

    endpoint: str
    index_name: str = "documents"
    username: Optional[str] = None
    password: Optional[str] = None
    use_ssl: bool = True
    verify_certs: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class BedrockConfig:
    """Bedrock client configuration."""

    region: str = "us-east-1"
    embedding_model: str = "amazon.titan-embed-text-v1"
    max_tokens: int = 8192
    timeout: int = 30


@dataclass
class IndexerConfig:
    """Main indexer service configuration."""

    # Required fields (no defaults)
    sqs_indexing_queue_url: str
    s3_parsed_bucket: str
    opensearch_config: OpenSearchConfig

    # Optional fields (with defaults)
    aws_region: str = "us-east-1"
    batch_size: int = 5
    poll_interval_seconds: int = 10
    long_poll_seconds: int = 20
    message_visibility_timeout: int = 300  # 5 minutes
    enable_embeddings: bool = True
    enable_content_preprocessing: bool = True
    bedrock_config: Optional[BedrockConfig] = None

    @classmethod
    def from_environment(cls) -> "IndexerConfig":
        """Create configuration from environment variables."""

        # Required environment variables
        sqs_indexing_queue_url = os.getenv("INDEXER_SQS_INDEXING_QUEUE_URL")
        if not sqs_indexing_queue_url:
            raise ValueError("INDEXER_SQS_INDEXING_QUEUE_URL environment variable is required")

        s3_parsed_bucket = os.getenv("INDEXER_S3_PARSED_BUCKET")
        if not s3_parsed_bucket:
            raise ValueError("INDEXER_S3_PARSED_BUCKET environment variable is required")

        opensearch_endpoint = os.getenv("INDEXER_OPENSEARCH_ENDPOINT")
        if not opensearch_endpoint:
            raise ValueError("INDEXER_OPENSEARCH_ENDPOINT environment variable is required")

        # OpenSearch configuration
        opensearch_config = OpenSearchConfig(
            endpoint=opensearch_endpoint,
            index_name=os.getenv("INDEXER_OPENSEARCH_INDEX", "documents"),
            username=os.getenv("INDEXER_OPENSEARCH_USERNAME"),
            password=os.getenv("INDEXER_OPENSEARCH_PASSWORD"),
            use_ssl=os.getenv("INDEXER_OPENSEARCH_USE_SSL", "true").lower() == "true",
            verify_certs=os.getenv("INDEXER_OPENSEARCH_VERIFY_CERTS", "true").lower() == "true",
        )

        # Bedrock configuration (optional)
        bedrock_config = None
        enable_embeddings = os.getenv("INDEXER_ENABLE_EMBEDDINGS", "true").lower() == "true"
        if enable_embeddings:
            bedrock_config = BedrockConfig(
                region=os.getenv("INDEXER_BEDROCK_REGION", "us-east-1"),
                embedding_model=os.getenv("INDEXER_BEDROCK_EMBEDDING_MODEL", "amazon.titan-embed-text-v1"),
            )

        return cls(
            aws_region=os.getenv("INDEXER_AWS_REGION", "us-east-1"),
            sqs_indexing_queue_url=sqs_indexing_queue_url,
            s3_parsed_bucket=s3_parsed_bucket,
            batch_size=int(os.getenv("INDEXER_BATCH_SIZE", "5")),
            poll_interval_seconds=int(os.getenv("INDEXER_POLL_INTERVAL_SECONDS", "10")),
            long_poll_seconds=int(os.getenv("INDEXER_LONG_POLL_SECONDS", "20")),
            message_visibility_timeout=int(os.getenv("INDEXER_MESSAGE_VISIBILITY_TIMEOUT", "300")),
            enable_embeddings=enable_embeddings,
            enable_content_preprocessing=os.getenv("INDEXER_ENABLE_CONTENT_PREPROCESSING", "true").lower() == "true",
            opensearch_config=opensearch_config,
            bedrock_config=bedrock_config,
        )
