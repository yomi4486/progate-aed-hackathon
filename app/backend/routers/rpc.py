from fastapi import APIRouter, HTTPException

from ...schema import SearchHit, SearchResponse
from random import random

rpc_router = APIRouter()


@rpc_router.get("/")
async def read_root() -> str:
    raise HTTPException(status_code=404, detail="Not Found")


@rpc_router.get("/search")
async def search_items(query: str, page: int = 1, size: int = 30) -> SearchResponse:
    all_hits = [
        SearchHit(
            id=str(i),
            title=f"Random result {i} for '{query}'",
            url=f"https://example{i}.com/{hash(query)}",
            site=f"example{i}.com",
            lang="en",
            score=round(random(), 2),
        )
        for i in range(1, 101)
    ]

    start = (page - 1) * size
    end = start + size
    paginated_hits = all_hits[start:end]

    return SearchResponse(
        total=len(all_hits),
        hits=paginated_hits,
        page=page,
        size=size,
    )
