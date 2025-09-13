"""
Configuration management for the distributed crawler.

Uses pydantic-settings to load configuration from environment variables
and YAML files with proper validation.
"""

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from ...schema.common import Lang
from ..core.types import CrawlerConfig


class CrawlerSettings(BaseSettings):
    """
    Main settings class that loads configuration from environment variables
    and configuration files.
    """

    model_config = SettingsConfigDict(
        env_prefix="CRAWLER_", env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # Environment
    environment: str = Field("dev", description="Environment name (dev/staging/prod)")

    # AWS Configuration
    aws_region: str = Field("us-east-1")
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    localstack_endpoint: Optional[str] = Field(None, description="LocalStack endpoint for local development")

    # Required AWS Resources
    dynamodb_table: str = Field(..., description="DynamoDB table name for URL states")
    sqs_crawl_queue_url: str = Field(..., description="SQS queue URL for crawl tasks")
    sqs_discovery_queue_url: Optional[str] = Field(None, description="SQS queue URL for domain discovery")
    sqs_indexing_queue_url: Optional[str] = Field(None, description="SQS queue URL for indexing tasks")
    s3_raw_bucket: str = Field(..., description="S3 bucket for raw HTML content")
    s3_parsed_bucket: Optional[str] = Field(None, description="S3 bucket for parsed content (for indexing)")

    # Redis Configuration
    redis_url: Optional[str] = Field(None, description="Redis connection URL (None to disable)")
    redis_db: int = Field(0, ge=0, le=15)
    redis_password: Optional[str] = None
    rate_limiter_enabled: bool = Field(True, description="Whether to enable Redis-based rate limiting")

    # Crawler Identity
    crawler_id: Optional[str] = Field(None, description="Unique crawler instance ID")

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
    acquisition_ttl_seconds: int = Field(3600, ge=300)
    heartbeat_interval_seconds: int = Field(30, ge=10)

    # Content Processing
    max_content_length: int = Field(50 * 1024 * 1024, ge=1024)  # 50MB
    default_language: Lang = Field("ja")
    language_detection_confidence: float = Field(0.7, ge=0.0, le=1.0)

    # Logging
    log_level: str = Field("INFO", description="Logging level")
    json_logs: bool = Field(True, description="Whether to output JSON format logs")

    # Health Check
    health_check_port: int = Field(8080, ge=1024, le=65535)
    health_check_enabled: bool = Field(True)

    # Monitoring
    metrics_enabled: bool = Field(True)
    metrics_interval_seconds: int = Field(60, ge=10)

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        valid_envs = ["dev", "devlocal", "staging", "prod"]
        if v not in valid_envs:
            raise ValueError(f"Invalid environment: {v}. Must be one of {valid_envs}")
        return v

    @field_validator("domain_qps_overrides", mode="before")
    @classmethod
    def validate_domain_qps_overrides(cls, v: Union[str, dict, None]) -> Dict[str, int]:
        """Parse domain QPS overrides from JSON string or return dict"""
        if v is None or v == "":
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    return parsed
                else:
                    raise ValueError("JSON must be an object/dictionary")
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string for domain_qps_overrides: {e}")
        raise ValueError(f"domain_qps_overrides must be a dict or JSON string, got {type(v)}")

    @model_validator(mode="after")
    def validate_aws_config(self) -> "CrawlerSettings":
        """Validate AWS configuration based on environment"""
        if self.environment == "devlocal":
            # LocalStack development - require localstack_endpoint
            if not self.localstack_endpoint:
                raise ValueError("localstack_endpoint is required for devlocal environment")
        elif self.environment in ["staging", "prod"]:
            # Production environments - require proper AWS credentials or IRSA
            aws_access_key = self.aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
            aws_role_arn = os.getenv("AWS_ROLE_ARN")
            if not aws_access_key and not aws_role_arn:
                raise ValueError(
                    f"AWS credentials (access key or IAM role) required for {self.environment} environment"
                )

        return self

    def to_crawler_config(self) -> CrawlerConfig:
        """Convert to CrawlerConfig instance"""
        return CrawlerConfig(
            crawler_id=self.crawler_id or f"crawler-{os.getpid()}",
            aws_region=self.aws_region,
            dynamodb_table=self.dynamodb_table,
            sqs_crawl_queue_url=self.sqs_crawl_queue_url,
            sqs_discovery_queue_url=self.sqs_discovery_queue_url,
            sqs_indexing_queue_url=self.sqs_indexing_queue_url,
            s3_raw_bucket=self.s3_raw_bucket,
            s3_parsed_bucket=self.s3_parsed_bucket,
            redis_url=self.redis_url,
            max_concurrent_requests=self.max_concurrent_requests,
            request_timeout=self.request_timeout,
            user_agent=self.user_agent,
            default_qps_per_domain=self.default_qps_per_domain,
            domain_qps_overrides=self.domain_qps_overrides,
            max_retries=self.max_retries,
            base_backoff_seconds=self.base_backoff_seconds,
            max_backoff_seconds=self.max_backoff_seconds,
            acquisition_ttl_seconds=self.acquisition_ttl_seconds,
            heartbeat_interval_seconds=self.heartbeat_interval_seconds,
            max_content_length=self.max_content_length,
            default_language=self.default_language,
            language_detection_confidence=self.language_detection_confidence,
        )


def _expand_env_variables(obj: Any) -> Any:
    """Recursively expand environment variables in configuration values."""
    if isinstance(obj, str):
        # Match ${VAR_NAME} or ${VAR_NAME:default_value} patterns
        def replace_env_var(match):
            var_with_default = match.group(1)
            if ":" in var_with_default:
                var_name, default_value = var_with_default.split(":", 1)
                return os.getenv(var_name, default_value)
            else:
                return os.getenv(var_with_default, match.group(0))  # Return original if not found

        return re.sub(r"\$\{([^}]+)\}", replace_env_var, obj)
    elif isinstance(obj, dict):
        return {key: _expand_env_variables(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_expand_env_variables(item) for item in obj]
    else:
        return obj


def load_config_from_yaml(file_path: Path) -> Dict[str, Any]:
    """
    Load configuration from YAML file with environment variable expansion.

    Args:
        file_path: Path to YAML configuration file

    Returns:
        Dictionary containing configuration values with env vars expanded

    Raises:
        FileNotFoundError: If configuration file doesn't exist
        yaml.YAMLError: If YAML file is malformed
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
        return _expand_env_variables(config)


def get_config_file_path(environment: str) -> Path:
    """
    Get the path to the configuration file for the given environment.

    Args:
        environment: Environment name

    Returns:
        Path to configuration file
    """
    config_dir = Path(__file__).parent
    return config_dir / f"{environment}.yaml"


def load_settings(
    environment: Optional[str] = None, config_file: Optional[Path] = None, **overrides: Any
) -> CrawlerSettings:
    """
    Load crawler settings from environment variables and configuration files.

    Args:
        environment: Environment name (dev/staging/prod). If None, read from CRAWLER_ENVIRONMENT
        config_file: Path to configuration file. If None, use default path
        **overrides: Additional configuration overrides

    Returns:
        Configured CrawlerSettings instance

    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If required configuration file is missing
    """
    # Determine environment
    if environment is None:
        environment = os.getenv("CRAWLER_ENVIRONMENT", "dev")

    # Load from YAML file if specified or if default exists
    config_data: Dict[str, Any] = {}

    if config_file:
        config_data = load_config_from_yaml(config_file)
    else:
        default_config_file = get_config_file_path(environment)
        if default_config_file.exists():
            config_data = load_config_from_yaml(default_config_file)

    # Add environment to config data
    config_data["environment"] = environment

    # Apply overrides
    config_data.update(overrides)

    # Create settings instance
    settings = CrawlerSettings(**config_data)

    return settings


def get_settings() -> CrawlerSettings:
    """
    Get default crawler settings.

    This function can be used as a dependency in FastAPI or other frameworks.

    Returns:
        Default CrawlerSettings instance
    """
    return load_settings()


# Global settings instance (lazy-loaded)
_settings: Optional[CrawlerSettings] = None


def get_cached_settings() -> CrawlerSettings:
    """
    Get cached settings instance.

    Returns:
        Cached CrawlerSettings instance
    """
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


def reset_settings_cache() -> None:
    """Reset the cached settings instance (useful for testing)"""
    global _settings
    _settings = None
