from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, HttpUrl

from .common import Lang


class CrawlResult(BaseModel):
    url: HttpUrl
    status_code: int
    fetched_at: datetime
    html_s3_key: str
    error: Optional[str] = None


class ParsedContent(BaseModel):
    url: HttpUrl
    title: Optional[str] = None
    description: Optional[str] = None
    body_text: str
    lang: Optional[Lang] = None
    published_at: Optional[datetime] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    parsed_s3_key: str


class URLState(BaseModel):
    url_hash: str
    domain: str
    last_crawled: Optional[datetime] = None
    state: Literal["pending", "in_progress", "done", "failed"] = Field(
        "pending", description="pending|in_progress|done|failed"
    )
    retries: int = 0
    s3_key: Optional[str] = None
