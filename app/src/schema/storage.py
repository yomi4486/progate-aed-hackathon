from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class S3ObjectRef(BaseModel):
    bucket: str
    key: str
    version_id: Optional[str] = None
    etag: Optional[str] = None
    content_type: Optional[str] = None
