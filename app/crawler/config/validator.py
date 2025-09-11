"""
Configuration validation utilities.

Validates AWS resource connectivity and configuration correctness.
"""

import asyncio
import logging
from typing import Dict, List

import boto3
import redis.asyncio as redis
from botocore.exceptions import ClientError, NoCredentialsError

from .settings import CrawlerSettings

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """Raised when configuration validation fails"""

    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__(f"Configuration validation failed: {', '.join(errors)}")


class ConfigValidator:
    """
    Validates crawler configuration and AWS resource connectivity.
    """

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.errors: List[str] = []
        self.warnings: List[str] = []

    async def validate_all(self, check_connectivity: bool = True) -> Dict[str, bool]:
        """
        Run all validation checks.

        Args:
            check_connectivity: Whether to check actual connectivity to AWS resources

        Returns:
            Dictionary with validation results for each service

        Raises:
            ConfigValidationError: If critical validation fails
        """
        results: Dict[str, bool] = {}

        # Basic configuration validation
        self._validate_basic_config()

        if check_connectivity:
            # AWS service connectivity checks
            results["dynamodb"] = await self._check_dynamodb()
            results["sqs"] = await self._check_sqs()
            results["s3"] = await self._check_s3()
            results["redis"] = await self._check_redis()

        # If there are critical errors, raise exception
        if self.errors:
            raise ConfigValidationError(self.errors)

        # Log warnings if any
        for warning in self.warnings:
            logger.warning(warning)

        return results

    def _validate_basic_config(self) -> None:
        """Validate basic configuration parameters"""

        # Check required AWS resources are specified
        if not self.settings.dynamodb_table:
            self.errors.append("dynamodb_table is required")

        if not self.settings.sqs_crawl_queue_url:
            self.errors.append("sqs_crawl_queue_url is required")

        if not self.settings.s3_raw_bucket:
            self.errors.append("s3_raw_bucket is required")

        if self.settings.rate_limiter_enabled and not self.settings.redis_url:
            self.errors.append("redis_url is required when rate_limiter_enabled is True")

        # Validate numeric ranges
        if self.settings.max_concurrent_requests <= 0:
            self.errors.append("max_concurrent_requests must be positive")

        if self.settings.request_timeout <= 0:
            self.errors.append("request_timeout must be positive")

        if self.settings.default_qps_per_domain <= 0:
            self.errors.append("default_qps_per_domain must be positive")

        # Check domain overrides
        for domain, qps in self.settings.domain_qps_overrides.items():
            if qps <= 0:
                self.errors.append(f"Invalid QPS for domain {domain}: {qps}")

        # Validate retry configuration
        if self.settings.max_retries < 0:
            self.errors.append("max_retries cannot be negative")

        if self.settings.base_backoff_seconds <= 0:
            self.errors.append("base_backoff_seconds must be positive")

        if self.settings.max_backoff_seconds < self.settings.base_backoff_seconds:
            self.errors.append("max_backoff_seconds must be >= base_backoff_seconds")

        # Validate locking configuration
        if self.settings.acquisition_ttl_seconds < 60:
            self.warnings.append("acquisition_ttl_seconds is very short (< 1 minute)")

        if self.settings.heartbeat_interval_seconds >= self.settings.acquisition_ttl_seconds:
            self.errors.append("heartbeat_interval_seconds must be < acquisition_ttl_seconds")

    async def _check_dynamodb(self) -> bool:
        """Check DynamoDB connectivity and table existence"""
        try:
            # Create DynamoDB client
            if self.settings.localstack_endpoint:
                dynamodb = boto3.client(  # type: ignore
                    "dynamodb",
                    endpoint_url=self.settings.localstack_endpoint,
                    aws_access_key_id=self.settings.aws_access_key_id,
                    aws_secret_access_key=self.settings.aws_secret_access_key,
                    region_name=self.settings.aws_region,
                )
            else:
                dynamodb = boto3.client(  # type: ignore
                    "dynamodb",
                    region_name=self.settings.aws_region,
                )

            # Check if table exists
            dynamodb.describe_table(TableName=self.settings.dynamodb_table)
            logger.info(f"DynamoDB table '{self.settings.dynamodb_table}' is accessible")
            return True

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                self.errors.append(f"DynamoDB table '{self.settings.dynamodb_table}' not found")
            else:
                self.errors.append(f"DynamoDB error: {e}")
            return False
        except NoCredentialsError:
            self.errors.append("AWS credentials not configured for DynamoDB")
            return False
        except Exception as e:
            self.errors.append(f"Unexpected DynamoDB error: {e}")
            return False

    async def _check_sqs(self) -> bool:
        """Check SQS connectivity and queue existence"""
        try:
            # Create SQS client
            if self.settings.localstack_endpoint:
                sqs = boto3.client(  # type: ignore
                    "sqs",
                    endpoint_url=self.settings.localstack_endpoint,
                    aws_access_key_id=self.settings.aws_access_key_id,
                    aws_secret_access_key=self.settings.aws_secret_access_key,
                    region_name=self.settings.aws_region,
                )
            else:
                sqs = boto3.client(  # type: ignore
                    "sqs",
                    region_name=self.settings.aws_region,
                )

            # Check crawl queue
            sqs.get_queue_attributes(QueueUrl=self.settings.sqs_crawl_queue_url, AttributeNames=["QueueArn"])
            logger.info("SQS crawl queue is accessible")

            # Check discovery queue if specified
            if self.settings.sqs_discovery_queue_url:
                sqs.get_queue_attributes(QueueUrl=self.settings.sqs_discovery_queue_url, AttributeNames=["QueueArn"])
                logger.info("SQS discovery queue is accessible")

            return True

        except ClientError as e:
            self.errors.append(f"SQS error: {e}")
            return False
        except NoCredentialsError:
            self.errors.append("AWS credentials not configured for SQS")
            return False
        except Exception as e:
            self.errors.append(f"Unexpected SQS error: {e}")
            return False

    async def _check_s3(self) -> bool:
        """Check S3 connectivity and bucket access"""
        try:
            # Create S3 client
            if self.settings.localstack_endpoint:
                s3 = boto3.client(  # type: ignore
                    "s3",
                    endpoint_url=self.settings.localstack_endpoint,
                    aws_access_key_id=self.settings.aws_access_key_id,
                    aws_secret_access_key=self.settings.aws_secret_access_key,
                    region_name=self.settings.aws_region,
                )
            else:
                s3 = boto3.client(  # type: ignore
                    "s3",
                    region_name=self.settings.aws_region,
                )

            # Check if bucket exists and is accessible
            s3.head_bucket(Bucket=self.settings.s3_raw_bucket)
            logger.info(f"S3 bucket '{self.settings.s3_raw_bucket}' is accessible")
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                self.errors.append(f"S3 bucket '{self.settings.s3_raw_bucket}' not found")
            elif error_code == "403":
                self.errors.append(f"No permission to access S3 bucket '{self.settings.s3_raw_bucket}'")
            else:
                self.errors.append(f"S3 error: {e}")
            return False
        except NoCredentialsError:
            self.errors.append("AWS credentials not configured for S3")
            return False
        except Exception as e:
            self.errors.append(f"Unexpected S3 error: {e}")
            return False

    async def _check_redis(self) -> bool:
        """Check Redis connectivity"""
        if not self.settings.redis_url:
            logger.info("Redis URL not configured, skipping Redis connectivity check")
            return True  # Consider success if Redis is disabled

        redis_client = None
        try:
            # Create Redis client
            redis_client = redis.Redis.from_url(  # type: ignore
                self.settings.redis_url,
                password=self.settings.redis_password,
                db=self.settings.redis_db,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )

            # Test connection
            await redis_client.ping()  # type: ignore
            logger.info("Redis is accessible")
            return True

        except redis.AuthenticationError as e:
            self.errors.append(f"Redis authentication error: {e}")
            return False
        except redis.ConnectionError as e:
            self.errors.append(f"Redis connection error: {e}")
            return False
        except Exception as e:
            self.errors.append(f"Unexpected Redis error: {e}")
            return False
        finally:
            if redis_client:
                await redis_client.aclose()


async def validate_settings(settings: CrawlerSettings, check_connectivity: bool = True) -> Dict[str, bool]:
    """
    Validate crawler settings and optionally check connectivity.

    Args:
        settings: Settings to validate
        check_connectivity: Whether to check actual connectivity

    Returns:
        Dictionary with validation results

    Raises:
        ConfigValidationError: If validation fails
    """
    validator = ConfigValidator(settings)
    return await validator.validate_all(check_connectivity)


async def quick_health_check(settings: CrawlerSettings) -> Dict[str, bool]:
    """
    Quick health check of all services without detailed validation.

    Args:
        settings: Settings to check

    Returns:
        Dictionary with service health status
    """
    try:
        validator = ConfigValidator(settings)
        return await validator.validate_all(check_connectivity=True)
    except ConfigValidationError:
        # Return partial results even if some services fail
        return {
            "dynamodb": False,
            "sqs": False,
            "s3": False,
            "redis": False,
        }


if __name__ == "__main__":
    # CLI validation tool
    import sys

    from .settings import load_settings

    async def main():
        try:
            # Load settings from environment
            settings = load_settings()
            print(f"Validating configuration for environment: {settings.environment}")

            # Run validation
            results = await validate_settings(settings)

            print("\nValidation Results:")
            for service, status in results.items():
                status_text = "✓ PASS" if status else "✗ FAIL"
                print(f"  {service:12s}: {status_text}")

            if all(results.values()):
                print("\n✓ All services are accessible!")
                sys.exit(0)
            else:
                print("\n✗ Some services failed validation")
                sys.exit(1)

        except ConfigValidationError as e:
            print("\n✗ Configuration validation failed:")
            for error in e.errors:
                print(f"  - {error}")
            sys.exit(1)
        except Exception as e:
            print(f"\n✗ Unexpected error: {e}")
            sys.exit(1)

    asyncio.run(main())
