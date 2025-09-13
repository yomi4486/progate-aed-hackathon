"""
Search API router using existing schema.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ...schema.common import HealthStatus, Lang
from ...schema.search import SearchQuery, SearchResponse, SuggestResponse
from ...search.config import SearchServiceConfig
from ...search.search_service import SearchService

logger = logging.getLogger(__name__)

# Global search service instance (initialized on startup)
_search_service: Optional[SearchService] = None

# Create FastAPI router
router = APIRouter(prefix="/search", tags=["search"])


def get_search_service() -> SearchService:
    """Get search service instance."""
    if _search_service is None:
        raise HTTPException(status_code=503, detail="Search service not initialized")
    return _search_service


@router.get("/", response_model=SearchResponse)
async def search(
    q: str = Query(..., description="Search query", min_length=1, max_length=500),
    page: int = Query(1, description="Page number", ge=1),
    size: int = Query(10, description="Results per page", ge=1, le=100),
    lang: Optional[Lang] = Query(None, description="Filter by language"),
    site: Optional[str] = Query(None, description="Filter by site/domain"),
    sort: Optional[str] = Query(None, description="Sort by field", regex="^(_score|published_at|popularity_score)$"),
    service: SearchService = Depends(get_search_service),
) -> SearchResponse:
    """
    Search documents using hybrid BM25 + vector search.

    This endpoint provides powerful search capabilities combining:
    - Traditional BM25 text search for exact matches
    - Vector similarity search for semantic matches
    - Advanced filtering by language, site, etc.
    - Multiple sorting options
    """
    try:
        # Create search query using existing schema
        # Cast sort to proper literal type
        valid_sort = None
        if sort and sort in ["_score", "published_at", "popularity_score"]:
            valid_sort = sort

        search_query = SearchQuery(q=q, page=page, size=size, lang=lang, site=site, sort=valid_sort)

        # Execute search
        response = await service.search(search_query)

        return response

    except Exception as e:
        logger.error(f"Search failed for query '{q}': {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.get("/suggest", response_model=SuggestResponse)
async def suggest(
    q: str = Query(..., description="Query for suggestions", min_length=1, max_length=100),
    size: int = Query(5, description="Number of suggestions", ge=1, le=20),
    service: SearchService = Depends(get_search_service),
) -> SuggestResponse:
    """
    Get search suggestions based on indexed content.

    Returns relevant search suggestions that can help users refine their queries.
    """
    try:
        response = await service.suggest(q, size)
        return response

    except Exception as e:
        logger.error(f"Suggestions failed for query '{q}': {e}")
        raise HTTPException(status_code=500, detail=f"Suggestions failed: {str(e)}")


@router.get("/health", response_model=HealthStatus)
async def health_check(service: SearchService = Depends(get_search_service)) -> HealthStatus:
    """
    Health check endpoint for the search service.

    Checks connectivity to OpenSearch and Bedrock (if enabled).
    """
    try:
        is_healthy = await service.health_check()

        # Check individual components
        opensearch_healthy = await service.opensearch_client.health_check()
        opensearch_status = "ok" if opensearch_healthy else "down"

        # Determine overall status
        if is_healthy and opensearch_healthy:
            status = "ok"
        elif opensearch_healthy:
            status = "degraded"  # Bedrock might be down but OpenSearch works
        else:
            status = "down"

        return HealthStatus(status=status, version="1.0.0", opensearch=opensearch_status)

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthStatus(status="down", opensearch="down")


# Startup and shutdown functions
async def initialize_search_service():
    """Initialize the search service on startup."""
    global _search_service

    try:
        config = SearchServiceConfig.from_environment()
        _search_service = SearchService(
            opensearch_config=config.opensearch_config, bedrock_config=config.bedrock_config
        )
        logger.info("Search service initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize search service: {e}")
        # Don't raise exception on startup failure - let service start without search
        _search_service = None


async def shutdown_search_service():
    """Cleanup search service on shutdown."""
    global _search_service

    if _search_service:
        try:
            await _search_service.close()
            logger.info("Search service shutdown completed")
        except Exception as e:
            logger.error(f"Error during search service shutdown: {e}")
        finally:
            _search_service = None
