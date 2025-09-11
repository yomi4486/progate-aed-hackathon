"""
S3 storage client for the distributed crawler.

Handles saving raw HTML content and parsed documents to S3 with
optimized key structures, metadata management, and error handling.
"""

import asyncio
import gzip
import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from ...schema.crawl import CrawlResult, ParsedContent
from ...schema.storage import S3ObjectRef
from ..config.settings import CrawlerSettings, get_cached_settings
from ..utils.url import extract_domain, generate_url_hash

logger = logging.getLogger(__name__)


class S3StorageError(Exception):
    """Base exception for S3 storage operations"""

    pass


class S3Client:
    """
    AWS S3 client wrapper with optimized configuration for crawler operations.

    Provides async-friendly interface for S3 operations with proper error handling,
    retry logic, and performance optimizations for high-throughput crawling.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None):
        self.settings = settings or get_cached_settings()
        self._client: Optional[Any] = None
        self._session_created_at = 0.0

        # Statistics tracking
        self.stats = {
            "uploads_attempted": 0,
            "uploads_successful": 0,
            "uploads_failed": 0,
            "bytes_uploaded": 0,
            "average_upload_time": 0.0,
            "total_upload_time": 0.0,
        }

    def _ensure_client(self) -> Any:
        """Ensure S3 client is initialized and fresh"""
        # Recreate client every hour to prevent credential staleness
        if self._client is None or time.time() - self._session_created_at > 3600:
            try:
                if self.settings.localstack_endpoint:
                    # LocalStack development
                    self._client = boto3.client(  # type: ignore
                        "s3",
                        endpoint_url=self.settings.localstack_endpoint,
                        aws_access_key_id=self.settings.aws_access_key_id,
                        aws_secret_access_key=self.settings.aws_secret_access_key,
                        region_name=self.settings.aws_region,
                    )
                else:
                    # Production AWS
                    self._client = boto3.client(  # type: ignore
                        "s3",
                        region_name=self.settings.aws_region,
                    )

                self._session_created_at = time.time()
                logger.debug("Created new S3 client")

            except Exception as e:
                logger.error(f"Failed to create S3 client: {e}")
                raise S3StorageError(f"S3 client initialization failed: {e}") from e

        return self._client

    async def put_object_async(
        self,
        bucket: str,
        key: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        compress: bool = True,
    ) -> S3ObjectRef:
        """
        Upload content to S3 asynchronously.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            content: Content to upload
            content_type: MIME type of content
            metadata: Optional metadata dict
            compress: Whether to gzip compress the content

        Returns:
            S3ObjectRef with upload details

        Raises:
            S3StorageError: If upload fails
        """
        start_time = time.time()
        self.stats["uploads_attempted"] += 1

        try:
            # Prepare content for upload
            upload_content = content
            upload_content_type = content_type
            upload_metadata = metadata or {}

            if compress and len(content) > 1024:  # Compress if > 1KB
                upload_content = gzip.compress(content)
                upload_content_type = content_type
                upload_metadata["Content-Encoding"] = "gzip"
                upload_metadata["Original-Content-Length"] = str(len(content))
                logger.debug(f"Compressed content from {len(content)} to {len(upload_content)} bytes")

            # Add standard metadata
            upload_metadata.update(
                {
                    "Crawler-Upload-Time": datetime.now(timezone.utc).isoformat(),
                    "Crawler-ID": self.settings.crawler_id or "unknown",
                    "Content-Hash": hashlib.md5(content).hexdigest(),
                }
            )

            # Perform upload in executor to avoid blocking
            def _upload():
                client = self._ensure_client()
                response = client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=upload_content,
                    ContentType=upload_content_type,
                    Metadata=upload_metadata,
                    ServerSideEncryption="AES256",  # Enable encryption
                )
                return response

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, _upload)

            # Update statistics
            upload_time = time.time() - start_time
            self.stats["uploads_successful"] += 1
            self.stats["bytes_uploaded"] += len(upload_content)
            self.stats["total_upload_time"] += upload_time
            self.stats["average_upload_time"] = self.stats["total_upload_time"] / self.stats["uploads_successful"]

            logger.info(
                f"Successfully uploaded to S3: s3://{bucket}/{key} ({len(upload_content)} bytes, {upload_time:.2f}s)",
                extra={
                    "bucket": bucket,
                    "key": key,
                    "size_bytes": len(upload_content),
                    "upload_time": upload_time,
                    "compressed": compress and len(content) > 1024,
                },
            )

            return S3ObjectRef(
                bucket=bucket,
                key=key,
                version_id=response.get("VersionId"),
                etag=response.get("ETag"),
                content_type=upload_content_type,
            )

        except ClientError as e:
            self.stats["uploads_failed"] += 1
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.error(f"S3 upload failed with error {error_code}: {e}")
            raise S3StorageError(f"S3 upload failed: {error_code}") from e

        except NoCredentialsError as e:
            self.stats["uploads_failed"] += 1
            logger.error("S3 upload failed due to missing credentials")
            raise S3StorageError("S3 credentials not configured") from e

        except Exception as e:
            self.stats["uploads_failed"] += 1
            logger.error(f"Unexpected error during S3 upload: {e}")
            raise S3StorageError(f"Unexpected upload error: {e}") from e

    async def get_object_async(self, bucket: str, key: str) -> bytes:
        """
        Download content from S3 asynchronously.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Object content as bytes

        Raises:
            S3StorageError: If download fails
        """
        try:

            def _download():
                client = self._ensure_client()
                response = client.get_object(Bucket=bucket, Key=key)
                content = response["Body"].read()

                # Handle gzip decompression
                if response.get("ContentEncoding") == "gzip":
                    content = gzip.decompress(content)

                return content

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _download)

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "NoSuchKey":
                logger.warning(f"S3 object not found: s3://{bucket}/{key}")
                raise S3StorageError(f"Object not found: s3://{bucket}/{key}") from e
            else:
                logger.error(f"S3 download failed with error {error_code}: {e}")
                raise S3StorageError(f"S3 download failed: {error_code}") from e

        except Exception as e:
            logger.error(f"Unexpected error during S3 download: {e}")
            raise S3StorageError(f"Unexpected download error: {e}") from e

    async def head_object_async(self, bucket: str, key: str) -> Dict[str, Any]:
        """
        Get object metadata from S3 asynchronously.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Object metadata dict

        Raises:
            S3StorageError: If metadata retrieval fails
        """
        try:

            def _head():
                client = self._ensure_client()
                return client.head_object(Bucket=bucket, Key=key)

            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _head)

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.error(f"S3 head object failed with error {error_code}: {e}")
            raise S3StorageError(f"Head object failed: {error_code}") from e

    def get_stats(self) -> Dict[str, Any]:
        """Get S3 client statistics"""
        stats = self.stats.copy()

        # Calculate derived metrics
        if stats["uploads_attempted"] > 0:
            stats["success_rate"] = stats["uploads_successful"] / stats["uploads_attempted"]
            stats["failure_rate"] = stats["uploads_failed"] / stats["uploads_attempted"]
        else:
            stats["success_rate"] = 0.0
            stats["failure_rate"] = 0.0

        # Add client info
        stats["client_active"] = self._client is not None
        stats["client_age"] = time.time() - self._session_created_at if self._client else 0

        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on S3 client"""
        try:
            # Test basic connectivity by listing buckets (limited access)
            def _health_check():
                self._ensure_client()
                # Just try to create the client - actual bucket access test would need permissions
                return True

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _health_check)

            return {
                "status": "healthy",
                "client_active": self._client is not None,
                "stats": self.get_stats(),
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
            }


class S3StorageClient:
    """
    High-level S3 storage client for crawler operations.

    Provides methods for storing raw crawl results and parsed content
    with optimized S3 key generation and metadata management.
    """

    def __init__(self, settings: Optional[CrawlerSettings] = None):
        self.settings = settings or get_cached_settings()
        self.s3_client = S3Client(settings)

        # Key generation settings
        self.raw_content_prefix = "raw-html"
        self.parsed_content_prefix = "parsed-content"
        self.date_partition = True  # Use YYYY/MM/DD partitioning

        logger.info(f"Initialized S3 storage client for bucket: {self.settings.s3_raw_bucket}")

    def generate_s3_key(
        self,
        url: str,
        content_type: str = "html",
        prefix: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """
        Generate optimized S3 key for content storage.

        Args:
            url: Source URL
            content_type: Type of content ("html", "parsed", etc.)
            prefix: Optional prefix override
            timestamp: Optional timestamp (defaults to now)

        Returns:
            Generated S3 key string
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        # Extract domain and generate URL hash
        domain = extract_domain(url)
        url_hash = generate_url_hash(url)

        # Build key components
        key_parts: List[str] = []

        # Add prefix
        if prefix:
            key_parts.append(prefix)
        elif content_type == "html":
            key_parts.append(self.raw_content_prefix)
        elif content_type == "parsed":
            key_parts.append(self.parsed_content_prefix)
        else:
            key_parts.append(content_type)

        # Add date partitioning
        if self.date_partition:
            key_parts.extend(
                [
                    timestamp.strftime("%Y"),
                    timestamp.strftime("%m"),
                    timestamp.strftime("%d"),
                ]
            )

        # Add domain partitioning
        key_parts.append(domain)

        # Add filename with hash and extension
        if content_type == "html":
            filename = f"{url_hash}.html.gz"
        elif content_type == "parsed":
            filename = f"{url_hash}.json.gz"
        else:
            filename = f"{url_hash}.{content_type}"

        key_parts.append(filename)

        return "/".join(key_parts)

    async def save_raw_content(
        self,
        url: str,
        content: bytes,
        content_type: str = "text/html",
        crawl_result: Optional[CrawlResult] = None,
    ) -> str:
        """
        Save raw HTML content to S3.

        Args:
            url: Source URL
            content: Raw HTML content
            content_type: MIME type of content
            crawl_result: Optional crawl result for metadata

        Returns:
            S3 key of saved content

        Raises:
            S3StorageError: If save operation fails
        """
        # Generate S3 key
        s3_key = self.generate_s3_key(url, "html")

        # Prepare metadata
        metadata = {
            "source-url": url,
            "content-type": content_type,
            "domain": extract_domain(url),
        }

        if crawl_result:
            metadata.update(
                {
                    "status-code": str(crawl_result.status_code),
                    "fetched-at": crawl_result.fetched_at.isoformat(),
                }
            )
            if crawl_result.error:
                metadata["crawl-error"] = crawl_result.error[:1000]  # Limit length

        # Upload to S3
        try:
            await self.s3_client.put_object_async(
                bucket=self.settings.s3_raw_bucket,
                key=s3_key,
                content=content,
                content_type="text/html",
                metadata=metadata,
                compress=True,
            )

            logger.info(
                f"Saved raw content to S3: {s3_key}",
                extra={
                    "url": url,
                    "s3_key": s3_key,
                    "content_size": len(content),
                },
            )

            return s3_key

        except Exception as e:
            logger.error(f"Failed to save raw content for {url}: {e}")
            raise

    async def save_parsed_content(
        self,
        url: str,
        parsed_content: ParsedContent,
    ) -> str:
        """
        Save parsed content to S3 as JSON.

        Args:
            url: Source URL
            parsed_content: Parsed content object

        Returns:
            S3 key of saved content

        Raises:
            S3StorageError: If save operation fails
        """
        # Generate S3 key
        s3_key = self.generate_s3_key(url, "parsed")

        # Serialize to JSON
        content_json = parsed_content.model_dump_json(indent=2)
        content_bytes = content_json.encode("utf-8")

        # Prepare metadata
        metadata = {
            "source-url": url,
            "content-type": "application/json",
            "domain": extract_domain(url),
            "language": parsed_content.lang or "unknown",
        }

        if parsed_content.title:
            metadata["title"] = parsed_content.title[:200]  # Limit length

        if parsed_content.published_at:
            metadata["published-at"] = parsed_content.published_at.isoformat()

        # Upload to S3
        try:
            await self.s3_client.put_object_async(
                bucket=self.settings.s3_raw_bucket,
                key=s3_key,
                content=content_bytes,
                content_type="application/json",
                metadata=metadata,
                compress=True,
            )

            logger.info(
                f"Saved parsed content to S3: {s3_key}",
                extra={
                    "url": url,
                    "s3_key": s3_key,
                    "content_size": len(content_bytes),
                    "title": parsed_content.title,
                    "language": parsed_content.lang,
                },
            )

            return s3_key

        except Exception as e:
            logger.error(f"Failed to save parsed content for {url}: {e}")
            raise

    async def save_crawl_result(
        self,
        crawl_result: CrawlResult,
        raw_content: bytes,
        parsed_content: Optional[ParsedContent] = None,
    ) -> Tuple[str, Optional[str]]:
        """
        Save complete crawl result (raw + parsed) to S3.

        Args:
            crawl_result: Crawl result metadata
            raw_content: Raw HTML content
            parsed_content: Optional parsed content

        Returns:
            Tuple of (raw_s3_key, parsed_s3_key)

        Raises:
            S3StorageError: If any save operation fails
        """
        url = str(crawl_result.url)

        # Save raw content
        raw_s3_key = await self.save_raw_content(
            url=url,
            content=raw_content,
            crawl_result=crawl_result,
        )

        # Save parsed content if available
        parsed_s3_key = None
        if parsed_content:
            parsed_s3_key = await self.save_parsed_content(
                url=url,
                parsed_content=parsed_content,
            )

        logger.info(
            "Saved complete crawl result to S3",
            extra={
                "url": url,
                "raw_s3_key": raw_s3_key,
                "parsed_s3_key": parsed_s3_key,
                "status_code": crawl_result.status_code,
            },
        )

        return raw_s3_key, parsed_s3_key

    async def get_raw_content(self, s3_key: str) -> bytes:
        """
        Retrieve raw content from S3.

        Args:
            s3_key: S3 key of content

        Returns:
            Raw content as bytes
        """
        return await self.s3_client.get_object_async(self.settings.s3_raw_bucket, s3_key)

    async def get_parsed_content(self, s3_key: str) -> ParsedContent:
        """
        Retrieve parsed content from S3.

        Args:
            s3_key: S3 key of content

        Returns:
            ParsedContent object
        """
        content_bytes = await self.s3_client.get_object_async(self.settings.s3_raw_bucket, s3_key)
        content_json = content_bytes.decode("utf-8")
        return ParsedContent.model_validate_json(content_json)

    def get_stats(self) -> Dict[str, Any]:
        """Get storage client statistics"""
        return self.s3_client.get_stats()

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on storage client"""
        return await self.s3_client.health_check()


if __name__ == "__main__":
    # CLI utility for testing S3 storage
    import asyncio
    import sys

    from ...schema.crawl import CrawlResult, ParsedContent

    async def main():
        if len(sys.argv) < 2:
            print("Usage: python s3_client.py [test|upload|download|health] [args...]")
            sys.exit(1)

        command = sys.argv[1]

        # Initialize storage client
        storage = S3StorageClient()

        try:
            if command == "health":
                health = await storage.health_check()
                print(f"S3 Storage Health: {health}")

            elif command == "stats":
                stats = storage.get_stats()
                print(f"S3 Storage Stats: {stats}")

            elif command == "test" and len(sys.argv) >= 3:
                url = sys.argv[2]
                print(f"Testing S3 storage with URL: {url}")

                # Test content
                test_content = b"<html><body><h1>Test Content</h1><p>This is a test.</p></body></html>"

                # Save raw content
                s3_key = await storage.save_raw_content(url, test_content)
                print(f"Saved raw content: {s3_key}")

                # Retrieve content
                retrieved = await storage.get_raw_content(s3_key)
                print(f"Retrieved {len(retrieved)} bytes")

                if retrieved == test_content:
                    print("✅ S3 storage test successful!")
                else:
                    print("❌ S3 storage test failed - content mismatch")

            elif command == "key" and len(sys.argv) >= 3:
                url = sys.argv[2]
                content_type = sys.argv[3] if len(sys.argv) > 3 else "html"

                s3_key = storage.generate_s3_key(url, content_type)
                print(f"Generated S3 key: {s3_key}")

            else:
                print(f"Unknown command: {command}")
                sys.exit(1)

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    asyncio.run(main())
