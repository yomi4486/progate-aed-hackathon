from fastapi import APIRouter, HTTPException

from ...schema import SearchHit, SearchResponse

rpc_router = APIRouter()


@rpc_router.get("/")
async def read_root() -> str:
    raise HTTPException(status_code=404, detail="Not Found")


@rpc_router.get("/search")
async def search_items(query: str) -> SearchResponse:
    return SearchResponse(
        total=1,
        hits=[
            SearchHit(
                id="1",
                title=f"Result for '{query}'",
                url="https://example.com",
                site="example.com",
                lang="en",
                score=1.0,
            )
        ],
        page=1,
        size=10,
    )
