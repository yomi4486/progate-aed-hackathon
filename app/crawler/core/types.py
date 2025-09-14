"""
Core types for the distributed crawler system.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

from pydantic import BaseModel, Field, HttpUrl

from ...schema.common import Lang
from ...schema.crawl import URLState as BaseURLState


class CrawlerStatus(str, Enum):
    """Crawler instance status"""

    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class URLStateEnum(str, Enum):
    """URL processing state"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


class CrawlErrorType(str, Enum):
    """Types of crawl errors"""

    HTTP_ERROR = "http_error"
    ROBOTS_BLOCKED = "robots_blocked"
    RATE_LIMITED = "rate_limited"
    PARSE_ERROR = "parse_error"
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    UNKNOWN = "unknown"


class ExtendedURLState(BaseURLState):
    """Extended URL state with crawler-specific fields"""

    url: HttpUrl
    crawler_id: Optional[str] = None
    acquired_at: Optional[datetime] = None
    ttl: Optional[datetime] = None
    error_message: Optional[str] = None
    error_type: Optional[CrawlErrorType] = None


class CrawlTask(BaseModel):
    """Individual crawl task"""

    url: HttpUrl
    priority: int = Field(1, ge=1, le=10)
    domain: str
    retry_count: int = 0
    scheduled_for: Optional[datetime] = None


class CrawlerConfig(BaseModel):
    """Configuration for a crawler instance"""

    # Identity
    crawler_id: str = Field(default_factory=lambda: str(uuid4()))

    # AWS Configuration
    aws_region: str = Field("us-east-1")
    dynamodb_table: str = Field("url-states")
    sqs_crawl_queue_url: str
    sqs_discovery_queue_url: Optional[str] = None
    sqs_indexing_queue_url: Optional[str] = None  # Queue for indexing tasks
    s3_raw_bucket: str
    s3_parsed_bucket: Optional[str] = None  # Bucket for parsed content (for indexing)
    redis_url: Optional[str] = None

    # HTTP Configuration
    max_concurrent_requests: int = Field(10, ge=1, le=100)
    request_timeout: int = Field(30, ge=5, le=300)
    user_agent: str = Field("AEDHack-Crawler/1.0")

    # Rate Limiting
    default_qps_per_domain: int = Field(1, ge=1, le=100)
    domain_qps_overrides: Dict[str, int] = Field(default_factory=dict)

    # Retry Configuration
    max_retries: int = Field(3, ge=0, le=10)
    base_backoff_seconds: int = Field(60, ge=1)
    max_backoff_seconds: int = Field(3600, ge=60)

    # Locking Configuration
    acquisition_ttl_seconds: int = Field(3600, ge=300)  # 1 hour
    heartbeat_interval_seconds: int = Field(30, ge=10)  # 30 seconds

    # Parsing Configuration
    max_content_length: int = Field(50 * 1024 * 1024, ge=1024)  # 50MB
    default_language: Lang = Field("ja")
    language_detection_confidence: float = Field(0.7, ge=0.0, le=1.0)


class CrawlMetrics(BaseModel):
    """Metrics for crawling operations"""

    urls_processed: int = 0
    urls_succeeded: int = 0
    urls_failed: int = 0
    total_response_time: float = 0.0  # in seconds
    last_activity: Optional[datetime] = None
    errors_by_type: Dict[CrawlErrorType, int] = Field(default_factory=Dict[CrawlErrorType, int])
    domains_processed: Dict[str, int] = Field(default_factory=dict)


class HealthStatus(BaseModel):
    """Health check response"""

    status: str = "healthy"
    timestamp: datetime = Field(default_factory=datetime.now)
    crawler_id: str
    uptime_seconds: float
    dependencies: Dict[str, bool] = Field(default_factory=dict)
    metrics: Optional[CrawlMetrics] = None


class RobotsInfo(BaseModel):
    """Information extracted from robots.txt"""

    domain: str
    allowed: bool = True
    crawl_delay: Optional[int] = None  # in seconds
    cached_at: datetime = Field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None


class DiscoveryMessage(BaseModel):
    """Message format for URL discovery queue"""

    domain: str
    priority: int = Field(1, ge=1, le=10)
    requested_at: datetime = Field(default_factory=datetime.now)
    max_urls: Optional[int] = None


class SitemapInfo(BaseModel):
    """Information about a sitemap"""

    url: HttpUrl
    last_modified: Optional[datetime] = None
    urls_found: int = 0
    processed_at: datetime = Field(default_factory=datetime.now)


class CrawlSession(BaseModel):
    """Information about a crawling session"""

    session_id: str = Field(default_factory=lambda: str(uuid4()))
    crawler_id: str
    started_at: datetime = Field(default_factory=datetime.now)
    ended_at: Optional[datetime] = None
    total_urls: int = 0
    successful_urls: int = 0
    failed_urls: int = 0
    domains: List[str] = Field(default_factory=list)


# Type aliases for convenience
URL = Union[str, HttpUrl]
Headers = Dict[str, str]
QueryParams = Dict[str, Any]
