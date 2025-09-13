"""
Main search service implementing hybrid BM25 + vector search.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from ..indexer.bedrock_client import BedrockClient
from ..indexer.config import BedrockConfig, OpenSearchConfig
from ..indexer.opensearch_client import OpenSearchClient
from ..schema.common import Highlight
from ..schema.search import SearchHit, SearchQuery, SearchResponse, SuggestResponse

logger = logging.getLogger(__name__)


class SearchService:
    """
    Hybrid search service combining BM25 and vector search.
    """

    def __init__(self, opensearch_config: OpenSearchConfig, bedrock_config: Optional[BedrockConfig] = None):
        self.opensearch_client = OpenSearchClient(opensearch_config)
        self.bedrock_client = BedrockClient(bedrock_config) if bedrock_config else None
        self.opensearch_config = opensearch_config

        # Search configuration
        self.max_results = 100  # Maximum results per search
        self.rrf_rank_constant = 60  # RRF rank constant for score fusion

    async def search(self, query: SearchQuery) -> SearchResponse:
        """
        Perform hybrid search combining BM25 and vector search.

        Args:
            query: SearchQuery from existing schema

        Returns:
            SearchResponse from existing schema
        """
        asyncio.get_event_loop().time()

        try:
            # Convert page-based pagination to offset-based
            from_ = (query.page - 1) * query.size

            # Enable hybrid search if bedrock client is available
            enable_hybrid = self.bedrock_client is not None

            if enable_hybrid:
                # Hybrid search: BM25 + Vector
                response = await self._hybrid_search(query, from_)
            else:
                # BM25 only search
                response = await self._bm25_search(query, from_)

            # Process results to match existing SearchHit schema
            hits = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]

                # Convert highlights to existing schema format
                highlights_list = []
                if "highlight" in hit:
                    for field, snippets in hit["highlight"].items():
                        highlights_list.append(Highlight(field=field, snippets=snippets))

                # Create snippet from content
                snippet = source.get("content", "")
                if len(snippet) > 300:
                    snippet = snippet[:300] + "..."

                hit_obj = SearchHit(
                    id=hit["_id"],
                    title=source.get("title"),
                    url=source["url"],
                    site=source.get("domain", ""),  # Map domain to site
                    lang=source.get("language", "ja"),  # Map language to lang
                    score=hit["_score"],
                    snippet=snippet,
                    highlights=highlights_list,
                )
                hits.append(hit_obj)

            total_hits = response["hits"]["total"]["value"]

            return SearchResponse(total=total_hits, hits=hits, page=query.page, size=query.size)

        except Exception as e:
            logger.error(f"Search failed for query '{query.q}': {e}")
            return SearchResponse(total=0, hits=[], page=query.page, size=query.size)

    async def _hybrid_search(self, query: SearchQuery, from_: int) -> Dict[str, Any]:
        """Perform hybrid BM25 + vector search."""

        # Generate query embedding
        query_embedding = None
        if self.bedrock_client:
            query_embedding = await self.bedrock_client.generate_embeddings(query.q)

        if not query_embedding:
            logger.warning("Failed to generate query embedding, falling back to BM25")
            return await self._bm25_search(query, from_)

        # Build hybrid search query
        search_body = {
            "size": min(query.size, self.max_results),
            "from": from_,
            "query": {
                "hybrid": {
                    "queries": [
                        # BM25 query
                        {
                            "multi_match": {
                                "query": query.q,
                                "fields": ["title^2", "content", "keywords"],
                                "type": "best_fields",
                                "fuzziness": "AUTO",
                            }
                        },
                        # Vector similarity query
                        {
                            "knn": {
                                "embedding": {
                                    "vector": query_embedding,
                                    "k": min(query.size * 2, 20),  # Get more candidates for reranking
                                }
                            }
                        },
                    ]
                }
            },
        }

        # Add filters based on existing schema
        filters = []
        if query.lang:
            filters.append({"term": {"language": query.lang}})
        if query.site:
            filters.append({"term": {"domain": query.site}})

        if filters:
            search_body["query"] = {"bool": {"must": [search_body["query"]], "filter": filters}}

        # Add highlights
        search_body["highlight"] = {
            "fields": {"title": {}, "content": {"fragment_size": 150, "number_of_fragments": 3}}
        }

        # Add sorting if specified
        if query.sort:
            if query.sort == "_score":
                search_body["sort"] = [{"_score": {"order": "desc"}}]
            elif query.sort == "published_at":
                search_body["sort"] = [{"fetched_at": {"order": "desc"}}]  # Use fetched_at as proxy
            elif query.sort == "popularity_score":
                search_body["sort"] = [{"processing_priority": {"order": "desc"}}]  # Use priority as proxy

        # Execute search
        return await self.opensearch_client.search_raw(search_body)

    async def _bm25_search(self, query: SearchQuery, from_: int) -> Dict[str, Any]:
        """Perform BM25 text search only."""

        search_body = {
            "size": min(query.size, self.max_results),
            "from": from_,
            "query": {
                "bool": {
                    "should": [
                        # Exact title match (highest boost)
                        {"match": {"title": {"query": query.q, "boost": 3.0}}},
                        # Content match
                        {"match": {"content": {"query": query.q, "boost": 1.0}}},
                        # Keywords match
                        {"match": {"keywords": {"query": query.q, "boost": 2.0}}},
                        # Multi-field match with fuzziness
                        {
                            "multi_match": {
                                "query": query.q,
                                "fields": ["title^2", "content", "keywords"],
                                "type": "cross_fields",
                                "fuzziness": "AUTO",
                                "boost": 0.5,
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
        }

        # Add filters based on existing schema
        filters = []
        if query.lang:
            filters.append({"term": {"language": query.lang}})
        if query.site:
            filters.append({"term": {"domain": query.site}})

        if filters:
            search_body["query"]["bool"]["filter"] = filters

        # Add highlights
        search_body["highlight"] = {
            "fields": {"title": {}, "content": {"fragment_size": 150, "number_of_fragments": 3}}
        }

        # Add sorting if specified
        if query.sort:
            if query.sort == "_score":
                search_body["sort"] = [{"_score": {"order": "desc"}}]
            elif query.sort == "published_at":
                search_body["sort"] = [{"fetched_at": {"order": "desc"}}]  # Use fetched_at as proxy
            elif query.sort == "popularity_score":
                search_body["sort"] = [{"processing_priority": {"order": "desc"}}]  # Use priority as proxy

        # Execute search
        return await self.opensearch_client.search_raw(search_body)

    def _build_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build OpenSearch filter query from filter parameters."""
        filter_clauses = []

        # Domain filter
        if "domain" in filters:
            domains = filters["domain"] if isinstance(filters["domain"], list) else [filters["domain"]]
            filter_clauses.append({"terms": {"domain": domains}})

        # Language filter
        if "language" in filters:
            languages = filters["language"] if isinstance(filters["language"], list) else [filters["language"]]
            filter_clauses.append({"terms": {"language": languages}})

        # Categories filter
        if "categories" in filters:
            categories = filters["categories"] if isinstance(filters["categories"], list) else [filters["categories"]]
            filter_clauses.append({"terms": {"categories": categories}})

        # Date range filter
        if "date_from" in filters or "date_to" in filters:
            date_filter = {"range": {"fetched_at": {}}}
            if "date_from" in filters:
                date_filter["range"]["fetched_at"]["gte"] = filters["date_from"]
            if "date_to" in filters:
                date_filter["range"]["fetched_at"]["lte"] = filters["date_to"]
            filter_clauses.append(date_filter)

        # Content length filter
        if "min_content_length" in filters:
            filter_clauses.append({"range": {"content_length": {"gte": filters["min_content_length"]}}})

        if len(filter_clauses) == 1:
            return filter_clauses[0]
        elif len(filter_clauses) > 1:
            return {"bool": {"must": filter_clauses}}
        else:
            return {}

    async def suggest(self, query: str, size: int = 5) -> SuggestResponse:
        """Get search suggestions based on indexed content."""
        try:
            search_body = {
                "size": 0,  # Don't return documents
                "suggest": {
                    "title_suggest": {"text": query, "term": {"field": "title.keyword", "size": size}},
                    "content_suggest": {"text": query, "term": {"field": "keywords", "size": size}},
                },
            }

            response = await self.opensearch_client.search_raw(search_body)

            suggestions = set()

            # Extract title suggestions
            for suggestion in response.get("suggest", {}).get("title_suggest", []):
                for option in suggestion.get("options", []):
                    suggestions.add(option["text"])

            # Extract content suggestions
            for suggestion in response.get("suggest", {}).get("content_suggest", []):
                for option in suggestion.get("options", []):
                    suggestions.add(option["text"])

            return SuggestResponse(suggestions=list(suggestions)[:size])

        except Exception as e:
            logger.error(f"Suggestion failed for query '{query}': {e}")
            return SuggestResponse(suggestions=[])

    async def health_check(self) -> bool:
        """Check if search service is healthy."""
        try:
            # Check OpenSearch health
            opensearch_healthy = await self.opensearch_client.health_check()

            # Check Bedrock health (if enabled)
            bedrock_healthy = True
            if self.bedrock_client:
                bedrock_healthy = await self.bedrock_client.test_connection()

            return opensearch_healthy and bedrock_healthy

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def close(self):
        """Close the search service and clean up resources."""
        if self.opensearch_client:
            await self.opensearch_client.close()
