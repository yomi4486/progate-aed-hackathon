from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from .common import Highlight, Lang


class SearchQuery(BaseModel):
    q: str
    page: int = Field(1, ge=1)
    size: int = Field(10, ge=1, le=100)
    lang: Optional[Lang] = None
    site: Optional[str] = None
    sort: Optional[Literal["_score", "published_at", "popularity_score"]] = None


class SearchHit(BaseModel):
    id: str
    title: Optional[str] = None
    url: str
    site: str
    lang: Lang
    score: float
    snippet: Optional[str] = None
    highlights: List[Highlight] = Field(default_factory=list[Highlight])


class SearchResponse(BaseModel):
    total: int
    hits: List[SearchHit]
    page: int
    size: int


class SuggestResponse(BaseModel):
    suggestions: List[str]
