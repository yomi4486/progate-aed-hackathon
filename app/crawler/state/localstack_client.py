"""
LocalStack-specific DynamoDB client that bypasses PynamoDB connection issues.

This module provides a direct boto3-based implementation for LocalStack environments
where PynamoDB has difficulty connecting to the custom endpoint.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from ..config.settings import CrawlerSettings
from ..core.types import URLStateEnum
from ..utils.url import extract_domain, generate_url_hash

logger = logging.getLogger(__name__)


class LocalStackDynamoDBClient:
    """
    Direct boto3 DynamoDB client for LocalStack environments.

    Provides the same interface as the PynamoDB-based client but uses
    boto3 directly to avoid endpoint connection issues.
    """

    def __init__(self, settings: CrawlerSettings):
        self.settings = settings
        self.table_name = settings.dynamodb_table

        # Create direct boto3 client
        self.client = boto3.client(  # type: ignore
            "dynamodb",
            endpoint_url=settings.localstack_endpoint,
            aws_access_key_id=settings.aws_access_key_id or "test",
            aws_secret_access_key=settings.aws_secret_access_key or "test",
            region_name=settings.aws_region,
        )

        logger.info(f"LocalStack DynamoDB client initialized: {settings.localstack_endpoint}")

    def _serialize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore
        """Convert Python dict to DynamoDB item format."""
        dynamodb_item: Dict[str, Any] = {}

        for key, value in item.items():
            if value is None:
                continue

            if isinstance(value, str):
                dynamodb_item[key] = {"S": value}
            elif isinstance(value, int):
                dynamodb_item[key] = {"N": str(value)}
            elif isinstance(value, float):
                dynamodb_item[key] = {"N": str(value)}
            elif isinstance(value, bool):
                dynamodb_item[key] = {"BOOL": value}
            elif isinstance(value, datetime):
                dynamodb_item[key] = {"S": value.isoformat()}
            else:
                # Convert to string as fallback
                dynamodb_item[key] = {"S": str(value)}

        return dynamodb_item

    def _deserialize_item(self, dynamodb_item: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore
        """Convert DynamoDB item format to Python dict."""
        item: Dict[str, Any] = {}

        for key, value_dict in dynamodb_item.items():
            if "S" in value_dict:
                item[key] = value_dict["S"]
            elif "N" in value_dict:
                # Try to convert to int first, then float
                try:
                    item[key] = int(value_dict["N"])
                except ValueError:
                    item[key] = float(value_dict["N"])
            elif "BOOL" in value_dict:
                item[key] = value_dict["BOOL"]
            else:
                # Handle other types as needed
                item[key] = str(value_dict)

        return item

    async def get_item(self, url_hash: str) -> Optional[Dict[str, Any]]:
        """Get a single item by URL hash."""
        try:
            response = self.client.get_item(TableName=self.table_name, Key={"url_hash": {"S": url_hash}})

            if "Item" in response:
                return self._deserialize_item(response["Item"])
            return None

        except ClientError as e:
            logger.error(f"Error getting item {url_hash}: {e}")
            return None

    async def put_item(self, item: Dict[str, Any], condition_expression: Optional[str] = None) -> bool:
        """Put an item to the table."""
        try:
            dynamodb_item = self._serialize_item(item)

            put_args = {"TableName": self.table_name, "Item": dynamodb_item}

            if condition_expression:
                put_args["ConditionExpression"] = condition_expression

            self.client.put_item(**put_args)  # type: ignore
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":  # type: ignore
                logger.debug(f"Conditional check failed for item: {item.get('url_hash', 'unknown')}")
                return False
            logger.error(f"Error putting item: {e}")
            return False

    async def update_item(
        self, url_hash: str, updates: Dict[str, Any], condition_expression: Optional[str] = None
    ) -> bool:
        """Update an item in the table."""
        try:
            # Build update expression
            set_parts: List[str] = []
            remove_parts: List[str] = []
            expression_values: Dict[str, Any] = {}
            expression_names: Dict[str, str] = {}

            for key, value in updates.items():
                if value is None:
                    # Remove attribute
                    remove_parts.append(f"#{key}")
                    expression_names[f"#{key}"] = key
                else:
                    # Set attribute
                    set_parts.append(f"#{key} = :{key}")
                    expression_names[f"#{key}"] = key

                    if isinstance(value, str):
                        expression_values[f":{key}"] = {"S": value}
                    elif isinstance(value, int):
                        expression_values[f":{key}"] = {"N": str(value)}
                    elif isinstance(value, datetime):
                        expression_values[f":{key}"] = {"S": value.isoformat()}
                    else:
                        expression_values[f":{key}"] = {"S": str(value)}

            # Build proper UpdateExpression
            update_expression_parts: List[str] = []
            if set_parts:
                update_expression_parts.append(f"SET {', '.join(set_parts)}")
            if remove_parts:
                update_expression_parts.append(f"REMOVE {', '.join(remove_parts)}")

            if not update_expression_parts:
                return True

            update_args = {
                "TableName": self.table_name,
                "Key": {"url_hash": {"S": url_hash}},
                "UpdateExpression": " ".join(update_expression_parts),
                "ExpressionAttributeNames": expression_names,
            }

            if expression_values:
                update_args["ExpressionAttributeValues"] = expression_values

            if condition_expression:
                update_args["ConditionExpression"] = condition_expression

            self.client.update_item(**update_args)  # type: ignore
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":  # type: ignore
                logger.debug(f"Conditional check failed for update: {url_hash}")
                return False
            logger.error(f"Error updating item {url_hash}: {e}")
            return False

    async def query_by_domain_state(self, domain: str, state: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Query items by domain and state using GSI."""
        try:
            query_args = {
                "TableName": self.table_name,
                "IndexName": "DomainStateIndex",
                "KeyConditionExpression": "domain = :domain AND #state = :state",
                "ExpressionAttributeNames": {"#state": "state"},
                "ExpressionAttributeValues": {":domain": {"S": domain}, ":state": {"S": state}},
            }

            if limit:
                query_args["Limit"] = limit  # type: ignore

            response = self.client.query(**query_args)  # type: ignore

            items: List[Dict[str, Any]] = []
            for item in response.get("Items", []):
                items.append(self._deserialize_item(item))  # type: ignore

            return items

        except ClientError as e:
            logger.error(f"Error querying domain {domain} state {state}: {e}")
            return []

    async def scan_table(
        self, filter_expression: Optional[str] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Scan the entire table with optional filtering."""
        try:
            scan_args = {"TableName": self.table_name}

            if filter_expression:
                scan_args["FilterExpression"] = filter_expression

            if limit:
                scan_args["Limit"] = limit  # type: ignore

            response = self.client.scan(**scan_args)  # type: ignore

            items: List[Dict[str, Any]] = []
            for item in response.get("Items", []):
                items.append(self._deserialize_item(item))  # type: ignore

            return items

        except ClientError as e:
            logger.error(f"Error scanning table: {e}")
            return []

    async def batch_get_items(self, url_hashes: List[str]) -> List[Dict[str, Any]]:
        """Batch get multiple items."""
        if not url_hashes:
            return []

        try:
            # Process in chunks of 100 (DynamoDB limit)
            chunk_size = 100
            all_items: List[Dict[str, Any]] = []

            for i in range(0, len(url_hashes), chunk_size):
                chunk = url_hashes[i : i + chunk_size]

                keys = [{"url_hash": {"S": url_hash}} for url_hash in chunk]

                response = self.client.batch_get_item(RequestItems={self.table_name: {"Keys": keys}})  # type: ignore

                items = response.get("Responses", {}).get(self.table_name, [])
                for item in items:
                    all_items.append(self._deserialize_item(item))  # type: ignore

            return all_items

        except ClientError as e:
            logger.error(f"Error batch getting items: {e}")
            return []

    async def table_exists(self) -> bool:
        """Check if the table exists."""
        try:
            self.client.describe_table(TableName=self.table_name)  # type: ignore
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":  # type: ignore
                return False
            raise


class LocalStackURLStateManager:
    """
    URL state manager using LocalStack DynamoDB client.

    Provides the same interface as URLStateManager but uses the LocalStack
    client for better compatibility.
    """

    def __init__(self, crawler_id: str, settings: CrawlerSettings):
        self.crawler_id = crawler_id
        self.settings = settings
        self.client = LocalStackDynamoDBClient(settings)

        logger.info(f"LocalStack URL state manager initialized for crawler {crawler_id}")

    async def add_url(
        self, url: str, domain: Optional[str] = None, initial_state: URLStateEnum = URLStateEnum.PENDING
    ) -> str:
        """Add a new URL to the system."""
        url_hash = generate_url_hash(url)

        if domain is None:
            domain = extract_domain(url)

        now = datetime.now(timezone.utc)

        # Check if URL already exists
        existing = await self.client.get_item(url_hash)
        if existing:
            logger.debug(f"URL {url} already exists")
            return url_hash

        # Create new item
        item = {
            "url_hash": url_hash,
            "url": url,
            "domain": domain,
            "state": initial_state.value,
            "created_at": now,
            "updated_at": now,
            "ttl": int(now.timestamp()) + self.settings.acquisition_ttl_seconds,
        }

        success = await self.client.put_item(item)
        if success:
            logger.info(f"Added URL {url} with state {initial_state.value}")
        else:
            logger.error(f"Failed to add URL {url}")

        return url_hash

    async def update_state(
        self,
        url_hash: str,
        new_state: URLStateEnum,
        crawler_id: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """Update URL state."""
        if crawler_id is None:
            crawler_id = self.crawler_id

        now = datetime.now(timezone.utc)

        updates: Dict[str, Any] = {"state": new_state.value, "updated_at": now}

        if new_state == URLStateEnum.IN_PROGRESS:
            updates["crawler_id"] = crawler_id
            updates["acquired_at"] = now
            updates["ttl"] = int(now.timestamp()) + self.settings.acquisition_ttl_seconds  # type: ignore

        elif new_state == URLStateEnum.DONE:
            updates["last_crawled"] = now
            updates["crawler_id"] = None  # type: ignore  # Clear lock
            updates["acquired_at"] = None  # type: ignore

        elif new_state == URLStateEnum.FAILED:
            if error_message:
                updates["error_message"] = error_message
            updates["crawler_id"] = None  # type: ignore  # Clear lock
            updates["acquired_at"] = None  # type: ignore

        elif new_state == URLStateEnum.PENDING:
            updates["crawler_id"] = None  # type: ignore  # Clear all locks
            updates["acquired_at"] = None  # type: ignore
            updates["error_message"] = None  # type: ignore

        success = await self.client.update_item(url_hash, updates)
        if success:
            logger.info(f"Updated URL {url_hash} state to {new_state.value}")
        else:
            logger.error(f"Failed to update URL {url_hash} state")

        return success

    async def get_pending_urls_for_domain(self, domain: str, limit: int = 100) -> List[str]:
        """Get pending URLs for a domain."""
        items = await self.client.query_by_domain_state(domain, URLStateEnum.PENDING.value, limit)

        # Filter out recently failed URLs (those with unexpired TTL)
        now = int(datetime.now(timezone.utc).timestamp())
        valid_items: List[str] = []

        for item in items:
            ttl = item.get("ttl")
            if ttl is None or ttl <= now:
                valid_items.append(item["url_hash"])  # type: ignore

        logger.debug(f"Found {len(valid_items)} pending URLs for domain {domain}")
        return valid_items

    async def batch_get_url_states(self, urls: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get URL states for multiple URLs."""
        if not urls:
            return {}

        url_hashes = [generate_url_hash(url) for url in urls]
        items = await self.client.batch_get_items(url_hashes)

        result: Dict[str, Dict[str, Any]] = {}
        for item in items:
            result[item["url_hash"]] = item  # type: ignore

        return result

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        try:
            exists = await self.client.table_exists()
            return {"status": "healthy" if exists else "degraded", "table_exists": exists}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
