"""
CLI entry point for the indexer service.
"""

import argparse
import asyncio
import logging
import sys

from .config import IndexerConfig
from .main import IndexerService


def setup_logging(log_level: str, json_logs: bool = False):
    """Setup logging configuration."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    if json_logs:
        # JSON logging format
        import datetime
        import json

        class JSONFormatter(logging.Formatter):
            def format(self, record):
                log_obj = {
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                if record.exc_info:
                    log_obj["exception"] = self.formatException(record.exc_info)
                return json.dumps(log_obj)

        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
    else:
        # Standard logging format
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)

    # Set specific loggers
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


async def health_check(config: IndexerConfig) -> bool:
    """Perform health check on all required services."""
    print("Performing health checks...")

    # Test SQS connectivity
    try:
        import boto3

        sqs = boto3.client("sqs", region_name=config.aws_region)
        sqs.get_queue_attributes(QueueUrl=config.sqs_indexing_queue_url)
        print("✅ SQS queue accessible")
    except Exception as e:
        print(f"❌ SQS queue not accessible: {e}")
        return False

    # Test S3 connectivity
    try:
        import boto3

        s3 = boto3.client("s3", region_name=config.aws_region)
        s3.head_bucket(Bucket=config.s3_parsed_bucket)
        print("✅ S3 bucket accessible")
    except Exception as e:
        print(f"❌ S3 bucket not accessible: {e}")
        return False

    # Test OpenSearch connectivity
    try:
        from .opensearch_client import OpenSearchClient

        os_client = OpenSearchClient(config.opensearch_config)
        is_healthy = await os_client.health_check()
        await os_client.close()

        if is_healthy:
            print("✅ OpenSearch cluster accessible")
        else:
            print("❌ OpenSearch cluster unhealthy")
            return False
    except Exception as e:
        print(f"❌ OpenSearch not accessible: {e}")
        return False

    # Test Bedrock connectivity (if enabled)
    if config.enable_embeddings and config.bedrock_config:
        try:
            from .bedrock_client import BedrockClient

            bedrock_client = BedrockClient(config.bedrock_config)
            is_working = await bedrock_client.test_connection()

            if is_working:
                print("✅ Bedrock service accessible")
            else:
                print("❌ Bedrock service not accessible")
                return False
        except Exception as e:
            print(f"❌ Bedrock not accessible: {e}")
            return False
    else:
        print("⚠️  Bedrock embeddings disabled")

    print("🎉 All health checks passed!")
    return True


async def run_indexer(config: IndexerConfig):
    """Run the main indexer service."""
    indexer = IndexerService(config)

    try:
        await indexer.start()
    except KeyboardInterrupt:
        print("\\nShutting down indexer service...")
        indexer.stop()
    except Exception as e:
        logging.error(f"Indexer service crashed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="AEDHack Indexer Service")

    parser.add_argument("command", choices=["run", "health", "config"], help="Command to execute")

    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level"
    )

    parser.add_argument("--json-logs", action="store_true", help="Output logs in JSON format")

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level, args.json_logs)

    try:
        # Load configuration
        config = IndexerConfig.from_environment()

        if args.command == "config":
            # Show configuration (without sensitive data)
            print("Indexer Configuration:")
            print(f"  AWS Region: {config.aws_region}")
            print(f"  SQS Queue: {config.sqs_indexing_queue_url}")
            print(f"  S3 Bucket: {config.s3_parsed_bucket}")
            print(f"  OpenSearch: {config.opensearch_config.endpoint}")
            print(f"  Batch Size: {config.batch_size}")
            print(f"  Poll Interval: {config.poll_interval_seconds}s")
            print(f"  Embeddings Enabled: {config.enable_embeddings}")
            if config.bedrock_config:
                print(f"  Bedrock Model: {config.bedrock_config.embedding_model}")

        elif args.command == "health":
            # Run health checks
            success = asyncio.run(health_check(config))
            sys.exit(0 if success else 1)

        elif args.command == "run":
            # Run the main indexer service
            print("Starting AEDHack Indexer Service...")
            print("Press Ctrl+C to stop")
            asyncio.run(run_indexer(config))

    except Exception as e:
        logging.error(f"Failed to start indexer: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
