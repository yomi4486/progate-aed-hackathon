from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl

from .common import Lang

EmbeddingVector = List[float]


class Document(BaseModel):
    id: str
    url: HttpUrl
    site: str
    lang: Lang
    title: Optional[str] = None
    body: Optional[str] = None
    published_at: Optional[datetime] = None
    crawled_at: Optional[datetime] = None
    content_hash: Optional[str] = None
    popularity_score: Optional[float] = Field(default=None, ge=0)
    s3_key: Optional[str] = None
    embedding: Optional[EmbeddingVector] = None


class IndexReadyDocument(BaseModel):
    id: str
    url: HttpUrl
    site: str
    lang: Lang
    title: Optional[str]
    snippet: Optional[str] = None
    published_at: Optional[datetime] = None
    crawled_at: Optional[datetime] = None
    content_hash: Optional[str] = None
    popularity_score: Optional[float] = Field(default=None, ge=0)
    embedding: Optional[EmbeddingVector] = None
