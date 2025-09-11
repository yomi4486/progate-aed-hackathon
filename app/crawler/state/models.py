"""
DynamoDB models for distributed crawler state management.

These models contain crawler-internal state information not needed by the frontend.
They extend the basic URLState with distributed locking and heartbeat capabilities.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pynamodb.attributes import NumberAttribute, TTLAttribute, UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.models import Model

from ..config.settings import get_cached_settings


class DomainStateIndex(GlobalSecondaryIndex["URLStateModel"]):
    """
    GSI for querying URLs by domain and state.

    Used for efficient domain-based URL distribution to crawlers.
    """

    class Meta:
        index_name = "DomainStateIndex"
        projection = AllProjection()
        # Unsupported attributes like `read_capacity_units`, `write_capacity_units`, `host`, etc.,
        # must not be included to avoid overriding errors.

    # GSI partition key and sort key
    domain = UnicodeAttribute(hash_key=True)
    state = UnicodeAttribute(range_key=True)


class URLStateModel(Model):
    """
    DynamoDB model for URL state management with distributed locking.

    This model handles the internal crawler state including distributed locks,
    heartbeats, and TTL-based timeout recovery. Frontend APIs use the simpler
    URLState schema from app/schema/crawl.py.
    """

    class Meta:  # type: ignore[reportIncompatibleVariableOverride]
        # Table configuration
        table_name = "crawler-url-states"
        region = "ap-northeast-1"

        # LocalStack support - will be updated dynamically
        host = None  # Will be set to localhost:4566 for LocalStack

        # Billing mode and capacity
        billing_mode = "PAY_PER_REQUEST"
        # Unsupported attributes like `tags` have been removed to avoid overriding errors.

    url_hash = UnicodeAttribute(hash_key=True)

    # Core URL information
    url = UnicodeAttribute()
    domain = UnicodeAttribute()

    # State management
    state = UnicodeAttribute()  # pending/in_progress/done/failed

    # Distributed locking fields
    crawler_id = UnicodeAttribute(null=True)
    acquired_at = UTCDateTimeAttribute(null=True)
    # Allow None for TTL to simplify checks and avoid static analysis false-positives
    ttl = TTLAttribute(null=True)

    # Crawling metadata
    last_crawled = UTCDateTimeAttribute(null=True)
    retries = NumberAttribute(default=0)
    error_message = UnicodeAttribute(null=True)

    # Content reference
    s3_key = UnicodeAttribute(null=True)

    # Audit fields
    created_at = UTCDateTimeAttribute(default=lambda: datetime.now(timezone.utc))
    updated_at = UTCDateTimeAttribute(default=lambda: datetime.now(timezone.utc))

    # GSI for domain-based queries
    domain_state_index = DomainStateIndex()

    @classmethod
    def get_table_name(cls) -> str:
        """Get table name from settings"""
        settings = get_cached_settings()
        return settings.dynamodb_table

    # Note: We avoid overriding Model.save to prevent signature/type issues.
    # Callers should update timestamps explicitly or rely on business logic layers.

    def is_locked(self) -> bool:
        """Check if URL is currently locked by a crawler"""
        ttl_value: Any = getattr(self, "ttl", None)
        return (
            self.state == "in_progress"
            and bool(self.crawler_id)
            and bool(self.acquired_at)
            and ttl_value is not None
            and ttl_value.timestamp() > datetime.now(timezone.utc).timestamp()
        )

    def is_expired(self) -> bool:
        """Check if the lock has expired"""
        ttl_value: Any = getattr(self, "ttl", None)
        if ttl_value is None:
            return False
        return ttl_value.timestamp() <= datetime.now(timezone.utc).timestamp()

    def can_be_acquired(self) -> bool:
        """Check if URL can be acquired for crawling"""
        return self.state in ["pending", "failed"] or self.is_expired()

    def time_until_retry(self) -> Optional[float]:
        """Get seconds until this URL can be retried"""
        if self.state not in ["in_progress", "failed"]:
            return 0

        ttl_value: Any = getattr(self, "ttl", None)
        if ttl_value is None:
            return 0

        now = datetime.now(timezone.utc).timestamp()
        ttl_timestamp = ttl_value.timestamp()

        if ttl_timestamp <= now:
            return 0

        return ttl_timestamp - now

    def to_frontend_state(self) -> dict[str, object]:
        """Convert to frontend-compatible URLState format"""
        return {
            "url_hash": self.url_hash,
            "domain": self.domain,
            "last_crawled": self.last_crawled,
            "state": self.state,
            "retries": self.retries,
            "s3_key": self.s3_key,
        }


class CrawlerMetricsModel(Model):
    """
    DynamoDB model for crawler performance metrics.

    Stores crawler-specific metrics for monitoring and debugging.
    Frontend doesn't need access to these internal metrics.
    """

    class Meta:  # type: ignore[reportIncompatibleVariableOverride]
        table_name = "crawler-metrics"
        region = "ap-northeast-1"
        billing_mode = "PAY_PER_REQUEST"

    # Primary key: crawler_id + timestamp (sorted by time)
    crawler_id = UnicodeAttribute(hash_key=True)
    timestamp = UTCDateTimeAttribute(range_key=True, default=lambda: datetime.now(timezone.utc))

    # Metrics data
    urls_processed = NumberAttribute(default=0)
    urls_succeeded = NumberAttribute(default=0)
    urls_failed = NumberAttribute(default=0)
    avg_response_time_ms = NumberAttribute(default=0)

    # Rate limiting stats
    rate_limit_hits = NumberAttribute(default=0)
    rate_limit_waits_ms = NumberAttribute(default=0)

    # Lock contention stats
    lock_acquisitions_attempted = NumberAttribute(default=0)
    lock_acquisitions_succeeded = NumberAttribute(default=0)
    lock_contentions = NumberAttribute(default=0)

    # Error stats
    network_errors = NumberAttribute(default=0)
    parsing_errors = NumberAttribute(default=0)
    storage_errors = NumberAttribute(default=0)

    # Resource utilization
    memory_usage_mb = NumberAttribute(default=0)
    cpu_usage_percent = NumberAttribute(default=0)

    # TTL for automatic cleanup (keep metrics for 30 days)
    ttl = TTLAttribute(default=lambda: datetime.now(timezone.utc) + timedelta(days=30))

    @classmethod
    def get_table_name(cls) -> str:
        """Get table name - use fixed name for metrics"""
        return "crawler-metrics"


# Initialize models with proper table configuration
def initialize_models():
    """
    Initialize DynamoDB models with proper configuration.

    This function should be called during application startup to ensure
    proper table configuration and create tables if they don't exist.
    """
    settings = get_cached_settings()

    # Configure URLStateModel with settings
    URLStateModel.Meta.table_name = settings.dynamodb_table
    URLStateModel.Meta.region = settings.aws_region

    # Endpoint override for LocalStack can be supplied via environment or client config when instantiating clients.

    # Tags are not set via PynamoDB Meta to avoid unsupported attribute overrides.


def create_tables_if_not_exist():
    """
    Create DynamoDB tables if they don't exist.

    Should be called during application initialization.
    """
    # Create URL state table
    if not URLStateModel.exists():
        URLStateModel.create_table(read_capacity_units=5, write_capacity_units=5, wait=True)

    # Create metrics table
    if not CrawlerMetricsModel.exists():
        CrawlerMetricsModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)


if __name__ == "__main__":
    # CLI utility for table management
    import asyncio
    import sys

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python models.py [create|describe|delete]")
            sys.exit(1)

        command = sys.argv[1]

        initialize_models()

        if command == "create":
            print("Creating DynamoDB tables...")
            create_tables_if_not_exist()
            print("Tables created successfully!")

        elif command == "describe":
            print("URL State Table:")
            print(f"  Table name: {URLStateModel.Meta.table_name}")
            print(f"  Exists: {URLStateModel.exists()}")

            print("\nCrawler Metrics Table:")
            print(f"  Table name: {CrawlerMetricsModel.Meta.table_name}")
            print(f"  Exists: {CrawlerMetricsModel.exists()}")

        elif command == "delete":
            print("Deleting DynamoDB tables...")
            if URLStateModel.exists():
                URLStateModel.delete_table()
            if CrawlerMetricsModel.exists():
                CrawlerMetricsModel.delete_table()
            print("Tables deleted!")

        else:
            print(f"Unknown command: {command}")
            sys.exit(1)

    asyncio.run(main())
