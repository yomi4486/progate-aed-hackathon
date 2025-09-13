"""
Configuration management for the search service.
"""

import os
from dataclasses import dataclass
from typing import Optional

from ..indexer.config import BedrockConfig, OpenSearchConfig


@dataclass
class SearchServiceConfig:
    """Main search service configuration."""

    # OpenSearch configuration (required - must be first)
    opensearch_config: OpenSearchConfig

    # Service configuration
    service_name: str = "search"
    service_version: str = "1.0.0"

    # Bedrock configuration (optional - for vector search)
    bedrock_config: Optional[BedrockConfig] = None

    # Search behavior configuration
    max_results_per_query: int = 100
    default_page_size: int = 10
    max_page_size: int = 50

    # RRF (Reciprocal Rank Fusion) configuration for hybrid search
    rrf_rank_constant: int = 60
    default_bm25_weight: float = 0.7
    default_vector_weight: float = 0.3

    # Performance configuration
    search_timeout_seconds: int = 30
    suggestion_cache_ttl: int = 300  # 5 minutes

    # Feature flags
    enable_vector_search: bool = True
    enable_suggestions: bool = True
    enable_highlights: bool = True
    enable_facets: bool = True

    @classmethod
    def from_environment(cls) -> "SearchServiceConfig":
        """Create configuration from environment variables."""

        # OpenSearch configuration (required)
        opensearch_endpoint = os.getenv("SEARCH_OPENSEARCH_ENDPOINT")
        if not opensearch_endpoint:
            raise ValueError("SEARCH_OPENSEARCH_ENDPOINT environment variable is required")

        opensearch_config = OpenSearchConfig(
            endpoint=opensearch_endpoint,
            index_name=os.getenv("SEARCH_OPENSEARCH_INDEX", "documents"),
            username=os.getenv("SEARCH_OPENSEARCH_USERNAME"),
            password=os.getenv("SEARCH_OPENSEARCH_PASSWORD"),
            use_ssl=os.getenv("SEARCH_OPENSEARCH_USE_SSL", "true").lower() == "true",
            verify_certs=os.getenv("SEARCH_OPENSEARCH_VERIFY_CERTS", "true").lower() == "true",
        )

        # Bedrock configuration (optional)
        bedrock_config = None
        enable_vector_search = os.getenv("SEARCH_ENABLE_VECTOR_SEARCH", "true").lower() == "true"
        if enable_vector_search:
            bedrock_config = BedrockConfig(
                region=os.getenv("SEARCH_BEDROCK_REGION", "us-east-1"),
                embedding_model=os.getenv("SEARCH_BEDROCK_EMBEDDING_MODEL", "amazon.titan-embed-text-v1"),
            )

        return cls(
            opensearch_config=opensearch_config,
            bedrock_config=bedrock_config,
            max_results_per_query=int(os.getenv("SEARCH_MAX_RESULTS_PER_QUERY", "100")),
            default_page_size=int(os.getenv("SEARCH_DEFAULT_PAGE_SIZE", "10")),
            max_page_size=int(os.getenv("SEARCH_MAX_PAGE_SIZE", "50")),
            rrf_rank_constant=int(os.getenv("SEARCH_RRF_RANK_CONSTANT", "60")),
            default_bm25_weight=float(os.getenv("SEARCH_DEFAULT_BM25_WEIGHT", "0.7")),
            default_vector_weight=float(os.getenv("SEARCH_DEFAULT_VECTOR_WEIGHT", "0.3")),
            search_timeout_seconds=int(os.getenv("SEARCH_TIMEOUT_SECONDS", "30")),
            suggestion_cache_ttl=int(os.getenv("SEARCH_SUGGESTION_CACHE_TTL", "300")),
            enable_vector_search=enable_vector_search,
            enable_suggestions=os.getenv("SEARCH_ENABLE_SUGGESTIONS", "true").lower() == "true",
            enable_highlights=os.getenv("SEARCH_ENABLE_HIGHLIGHTS", "true").lower() == "true",
            enable_facets=os.getenv("SEARCH_ENABLE_FACETS", "true").lower() == "true",
        )
