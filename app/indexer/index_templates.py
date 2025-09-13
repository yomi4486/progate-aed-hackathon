"""
OpenSearch index templates for Japanese language analysis with MeCab and vector search.

Implements index templates optimized for Japanese content using MeCab tokenizer,
with proper mappings for hybrid BM25+vector search as specified in design.md.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def get_japanese_analysis_settings() -> Dict[str, Any]:
    """
    Get OpenSearch analysis settings optimized for Japanese content using MeCab.

    Returns:
        Analysis configuration with MeCab tokenizer and filters
    """
    return {
        "analysis": {
            "analyzer": {
                "japanese_analyzer": {
                    "type": "custom",
                    "tokenizer": "mecab_tokenizer",
                    "filter": ["lowercase", "stop"],
                },
                # Standard analyzer for English content
                "multilingual_standard": {"type": "standard", "stopwords": "_english_"},
            }
        }
    }


def get_document_index_mapping(embedding_dimension: int = 1536) -> Dict[str, Any]:
    """
    Get index mapping for document storage with Japanese analysis and vector search.

    Args:
        embedding_dimension: Vector embedding dimension (1536 for Titan v1, 1024 for v2)

    Returns:
        Complete index mapping configuration following design.md specifications
    """
    return {
        "mappings": {
            "properties": {
                # Core document fields as specified in design.md
                "title": {"type": "text", "analyzer": "japanese_analyzer", "fields": {"keyword": {"type": "keyword"}}},
                "body": {"type": "text", "analyzer": "japanese_analyzer"},
                "url": {"type": "keyword"},
                "site": {"type": "keyword"},
                "lang": {"type": "keyword"},
                "published_at": {"type": "date"},
                "crawled_at": {"type": "date"},
                "content_hash": {"type": "keyword"},
                "popularity_score": {"type": "float"},
                # Vector embeddings for semantic search
                "embedding": {
                    "type": "knn_vector",
                    "dimension": embedding_dimension,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesim",  # Updated from design.md typo
                        "engine": "lucene",  # Using lucene instead of nmslib for better compatibility
                    },
                },
                # Additional fields for indexer service
                "url_hash": {"type": "keyword"},
                "domain": {"type": "keyword"},
                "content": {  # Alias for body to match indexer service
                    "type": "text",
                    "analyzer": "japanese_analyzer",
                },
                "language": {  # Additional language field
                    "type": "keyword"
                },
                "fetched_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "indexed_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "content_type": {"type": "keyword"},
                "content_length": {"type": "integer"},
                "processing_priority": {"type": "integer"},
                "status_code": {"type": "integer"},
                "keywords": {
                    "type": "text",
                    "analyzer": "japanese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "categories": {"type": "keyword"},
                # S3 storage references (not indexed)
                "raw_s3_key": {"type": "keyword", "index": False},
                "parsed_s3_key": {"type": "keyword", "index": False},
            }
        },
        # Index settings
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,  # Start with 0 replicas for dev
                "refresh_interval": "5s",
                "max_result_window": 10000,
            },
            # Include Japanese analysis settings
            **get_japanese_analysis_settings(),
        },
    }


def get_index_template(
    template_name: str = "documents-template", index_pattern: str = "documents-*", embedding_dimension: int = 1536
) -> Dict[str, Any]:
    """
    Get complete index template for document indices.

    Args:
        template_name: Name of the index template
        index_pattern: Index pattern (e.g., "documents-*")
        embedding_dimension: Vector embedding dimension

    Returns:
        Complete index template configuration
    """
    mapping = get_document_index_mapping(embedding_dimension)

    return {
        "index_patterns": [index_pattern],
        "priority": 100,
        "version": 1,
        "template": {"settings": mapping["settings"], "mappings": mapping["mappings"]},
        "_meta": {
            "description": f"Template for {index_pattern} with MeCab Japanese analysis and vector search",
            "created_by": "indexer-service",
            "version": "1.0.0",
            "embedding_dimension": embedding_dimension,
        },
    }


def get_environment_template(environment: str, embedding_dimension: int = 1536) -> Dict[str, Any]:
    """
    Get environment-specific index template.

    Args:
        environment: Environment name (dev/staging/prod)
        embedding_dimension: Vector embedding dimension

    Returns:
        Environment-specific index template
    """
    # Configuration per environment
    config = {
        "dev": {"shards": 1, "replicas": 0, "refresh_interval": "1s"},
        "staging": {"shards": 2, "replicas": 1, "refresh_interval": "5s"},
        "prod": {"shards": 3, "replicas": 2, "refresh_interval": "10s"},
    }

    env_config = config.get(environment, config["dev"])
    index_pattern = f"documents-{environment}-*"

    template = get_index_template(
        template_name=f"documents-{environment}-template",
        index_pattern=index_pattern,
        embedding_dimension=embedding_dimension,
    )

    # Override settings for environment
    template["template"]["settings"]["index"]["number_of_shards"] = env_config["shards"]
    template["template"]["settings"]["index"]["number_of_replicas"] = env_config["replicas"]
    template["template"]["settings"]["index"]["refresh_interval"] = env_config["refresh_interval"]

    return template


def get_default_index_name(environment: str = "dev") -> str:
    """
    Get default index name for environment.

    Args:
        environment: Environment name

    Returns:
        Default index name
    """
    return f"documents-{environment}-001"


async def create_index_with_template(
    opensearch_client, environment: str = "dev", embedding_dimension: int = 1536
) -> bool:
    """
    Create index template and initial index for environment.

    Args:
        opensearch_client: OpenSearchClient instance
        environment: Environment name
        embedding_dimension: Vector embedding dimension

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create index template
        template = get_environment_template(environment, embedding_dimension)
        template_name = f"documents-{environment}-template"

        success = await opensearch_client.create_index_template(template_name, template)
        if not success:
            return False

        logger.info(f"Created index template: {template_name}")

        # Create initial index
        index_name = get_default_index_name(environment)
        success = await opensearch_client.create_index(index_name)
        if not success:
            return False

        logger.info(f"Created initial index: {index_name}")

        return True

    except Exception as e:
        logger.error(f"Failed to create index template and index: {e}")
        return False


# Template for testing/development with sample data
SAMPLE_DOCUMENT = {
    "title": "サンプル記事タイトル",
    "body": "これは日本語のサンプル記事本文です。MeCabで形態素解析されます。",
    "url": "https://example.com/sample-article",
    "site": "example.com",
    "lang": "ja",
    "published_at": "2024-01-01T00:00:00Z",
    "crawled_at": "2024-01-01T01:00:00Z",
    "content_hash": "abc123",
    "popularity_score": 0.85,
    "url_hash": "hash123",
    "domain": "example.com",
    "content": "これは日本語のサンプル記事本文です。MeCabで形態素解析されます。",
    "language": "ja",
    "fetched_at": "2024-01-01T01:00:00Z",
    "indexed_at": "2024-01-01T02:00:00Z",
    "content_type": "text/html",
    "content_length": 1024,
    "processing_priority": 1,
    "status_code": 200,
    "keywords": "サンプル,記事,日本語",
    "categories": ["news", "technology"],
}
