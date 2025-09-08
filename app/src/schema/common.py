from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

Lang = Literal["ja", "en"]


class Pagination(BaseModel):
    page: int = Field(1, ge=1)
    size: int = Field(10, ge=1, le=100)


class TimeWindow(BaseModel):
    gte: Optional[datetime] = None
    lte: Optional[datetime] = None


class Highlight(BaseModel):
    field: str
    snippets: List[str] = Field(default_factory=list)


class Snippet(BaseModel):
    text: str
    offset: Optional[int] = None
    score: Optional[float] = None


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None


class HealthStatus(BaseModel):
    status: Literal["ok", "degraded", "down"] = "ok"
    version: Optional[str] = None
    opensearch: Optional[Literal["ok", "down"]] = None
    cache: Optional[Literal["ok", "down"]] = None
