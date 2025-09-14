"""
Main search service implementing hybrid BM25 + vector search.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

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
            hits: List[SearchHit] = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]

                # Convert highlights to existing schema format
                highlights_list: List[Highlight] = []
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

    async def _execute_bm25_search(self, query: SearchQuery, size: int) -> List[Dict[str, Any]]:
        """Execute BM25 search and return raw hits."""
        # Detect if query contains Japanese characters
        is_japanese_query = self._is_japanese_query(query.q)

        # Choose appropriate fields based on language
        if is_japanese_query:
            title_field = "title"  # Uses japanese_analyzer
            content_field = "content"  # Uses japanese_analyzer
            keywords_field = "keywords"  # Uses japanese_analyzer
            multi_match_fields = ["title^2", "content", "keywords"]
        else:
            title_field = "title.standard"  # Uses standard analyzer for English
            content_field = "content.standard"  # Uses standard analyzer
            keywords_field = "keywords.standard"  # Uses standard analyzer
            multi_match_fields = ["title.standard^2", "content.standard", "keywords.standard"]

        search_body: Dict[str, Any] = {
            "size": size,
            "query": {
                "bool": {
                    "should": [
                        {"match": {title_field: {"query": query.q, "boost": 3.0}}},
                        {"match": {content_field: {"query": query.q, "boost": 1.0}}},
                        {"match": {keywords_field: {"query": query.q, "boost": 2.0}}},
                        {
                            "multi_match": {
                                "query": query.q,
                                "fields": multi_match_fields,
                                "type": "cross_fields",
                                "fuzziness": "AUTO" if not is_japanese_query else "0",  # No fuzziness for Japanese
                                "boost": 0.5,
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
            "highlight": {"fields": {"title": {}, "content": {"fragment_size": 150, "number_of_fragments": 3}}},
        }

        # Add filters
        filters: List[Dict[str, Any]] = []
        if query.lang:
            filters.append({"term": {"language": query.lang}})
        if query.site:
            filters.append({"term": {"domain": query.site}})

        if filters:
            search_body["query"]["bool"]["filter"] = filters

        response = await self.opensearch_client.search_raw(search_body)
        return response["hits"]["hits"]

    async def _execute_vector_search(
        self, query: SearchQuery, query_embedding: List[float], size: int
    ) -> List[Dict[str, Any]]:
        """Execute vector similarity search and return raw hits."""
        search_body: Dict[str, Any] = {
            "size": size,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_embedding,
                        "k": size,
                    }
                }
            },
            "highlight": {"fields": {"title": {}, "content": {"fragment_size": 150, "number_of_fragments": 3}}},
        }

        # Add filters
        filters: List[Dict[str, Any]] = []
        if query.lang:
            filters.append({"term": {"language": query.lang}})
        if query.site:
            filters.append({"term": {"domain": query.site}})

        if filters:
            search_body["query"] = {"bool": {"must": [search_body["query"]], "filter": filters}}

        response = await self.opensearch_client.search_raw(search_body)
        return response["hits"]["hits"]

    def _apply_rrf_fusion(
        self, bm25_hits: List[Dict[str, Any]], vector_hits: List[Dict[str, Any]], target_size: int
    ) -> List[Dict[str, Any]]:
        """Apply Reciprocal Rank Fusion to combine BM25 and vector search results."""
        # Create ranking maps
        bm25_ranks = {hit["_id"]: i + 1 for i, hit in enumerate(bm25_hits)}
        vector_ranks = {hit["_id"]: i + 1 for i, hit in enumerate(vector_hits)}

        # Collect all unique documents
        all_docs: Dict[str, Dict[str, Any]] = {}
        for hit in bm25_hits:
            all_docs[hit["_id"]] = hit
        for hit in vector_hits:
            all_docs[hit["_id"]] = hit

        # Calculate RRF scores
        rrf_scores: Dict[str, float] = {}
        for doc_id in all_docs:
            bm25_rank = bm25_ranks.get(doc_id, len(bm25_hits) + 1)
            vector_rank = vector_ranks.get(doc_id, len(vector_hits) + 1)

            # RRF formula: 1/(rank + k) where k is typically 60
            rrf_score: float = (1.0 / (bm25_rank + self.rrf_rank_constant)) + (
                1.0 / (vector_rank + self.rrf_rank_constant)
            )
            rrf_scores[doc_id] = rrf_score

        # Sort by RRF score (descending)
        sorted_docs = sorted(all_docs.items(), key=lambda x: rrf_scores[x[0]], reverse=True)

        # Return top results with RRF score as _score
        fused_results: List[Dict[str, Any]] = []
        for doc_id, hit in sorted_docs[: target_size * 2]:  # Get more for pagination
            hit["_score"] = rrf_scores[doc_id]
            fused_results.append(hit)

        return fused_results

    def _is_japanese_query(self, query_text: str) -> bool:
        """Check if the query contains Japanese characters."""
        import re

        japanese_pattern = re.compile(r"[ひらがなカタカナ一-龯]")
        japanese_chars = len(japanese_pattern.findall(query_text))
        total_chars = len(query_text.replace(" ", ""))  # Exclude spaces

        # Consider it Japanese if more than 30% of non-space characters are Japanese
        return japanese_chars > 0 and (japanese_chars / max(total_chars, 1)) > 0.3

    async def _hybrid_search(self, query: SearchQuery, from_: int) -> Dict[str, Any]:
        """Perform hybrid BM25 + vector search using manual RRF."""

        # Generate query embedding
        query_embedding = None
        if self.bedrock_client:
            query_embedding = await self.bedrock_client.generate_embeddings(query.q)

        if not query_embedding:
            logger.warning("Failed to generate query embedding, falling back to BM25")
            return await self._bm25_search(query, from_)

        # Execute BM25 and vector searches separately, then combine with RRF
        bm25_results = await self._execute_bm25_search(query, query.size * 2)  # Get more results for fusion
        vector_results = await self._execute_vector_search(query, query_embedding, query.size * 2)

        # Apply RRF fusion
        fused_results = self._apply_rrf_fusion(bm25_results, vector_results, query.size)

        # Apply pagination
        total_hits = len(fused_results)
        paginated_hits = fused_results[from_ : from_ + query.size]

        # Format as OpenSearch response
        return {"hits": {"total": {"value": total_hits, "relation": "eq"}, "hits": paginated_hits}}

    async def _bm25_search(self, query: SearchQuery, from_: int) -> Dict[str, Any]:
        """Perform BM25 text search only."""

        # Detect if query contains Japanese characters
        is_japanese_query = self._is_japanese_query(query.q)

        # Choose appropriate fields based on language
        if is_japanese_query:
            title_field = "title"  # Uses japanese_analyzer
            content_field = "content"  # Uses japanese_analyzer
            keywords_field = "keywords"  # Uses japanese_analyzer
            multi_match_fields = ["title^2", "content", "keywords"]
            fuzziness = "0"  # No fuzziness for Japanese
        else:
            title_field = "title.standard"  # Uses standard analyzer for English
            content_field = "content.standard"  # Uses standard analyzer
            keywords_field = "keywords.standard"  # Uses standard analyzer
            multi_match_fields = ["title.standard^2", "content.standard", "keywords.standard"]
            fuzziness = "AUTO"  # Use fuzziness for English

        search_body: Dict[str, Any] = {
            "size": min(query.size, self.max_results),
            "from": from_,
            "query": {
                "bool": {
                    "should": [
                        # Exact title match (highest boost)
                        {"match": {title_field: {"query": query.q, "boost": 3.0}}},
                        # Content match
                        {"match": {content_field: {"query": query.q, "boost": 1.0}}},
                        # Keywords match
                        {"match": {keywords_field: {"query": query.q, "boost": 2.0}}},
                        # Multi-field match with fuzziness
                        {
                            "multi_match": {
                                "query": query.q,
                                "fields": multi_match_fields,
                                "type": "cross_fields",
                                "fuzziness": fuzziness,
                                "boost": 0.5,
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
        }

        # Add filters based on existing schema
        filters: List[Dict[str, Any]] = []
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
            sort_config: List[Dict[str, Dict[str, str]]] = []
            if query.sort == "_score":
                sort_config = [{"_score": {"order": "desc"}}]
            elif query.sort == "published_at":
                sort_config = [{"fetched_at": {"order": "desc"}}]  # Use fetched_at as proxy
            elif query.sort == "popularity_score":
                sort_config = [{"processing_priority": {"order": "desc"}}]  # Use priority as proxy
            search_body["sort"] = sort_config

        # Execute search
        return await self.opensearch_client.search_raw(search_body)

    def _build_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build OpenSearch filter query from filter parameters."""
        filter_clauses: List[Dict[str, Any]] = []

        # Domain filter
        if "domain" in filters:
            domains: List[str] = (
                list(filters["domain"]) if isinstance(filters["domain"], list) else [str(filters["domain"])]  # type: ignore
            )  # type: ignore
            filter_clauses.append({"terms": {"domain": domains}})

        # Language filter
        if "language" in filters:
            languages: List[str] = (  # type: ignore
                filters["language"] if isinstance(filters["language"], list) else [filters["language"]]
            )  # type: ignore
            filter_clauses.append({"terms": {"language": languages}})

        # Categories filter
        if "categories" in filters:
            categories: List[str] = (  # type: ignore
                filters["categories"] if isinstance(filters["categories"], list) else [filters["categories"]]
            )  # type: ignore
            filter_clauses.append({"terms": {"categories": categories}})

        # Date range filter
        if "date_from" in filters or "date_to" in filters:
            date_filter: Dict[str, Dict[str, Dict[str, Any]]] = {"range": {"fetched_at": {}}}
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

            suggestions: set[str] = set()

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
