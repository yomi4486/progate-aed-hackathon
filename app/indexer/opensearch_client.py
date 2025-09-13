"""
OpenSearch client for indexing and searching documents.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import aiohttp

from .config import OpenSearchConfig
from .document_processor import ProcessedDocument

logger = logging.getLogger(__name__)


class OpenSearchClient:
    """
    Async OpenSearch client for document indexing and search operations.
    """

    def __init__(self, config: OpenSearchConfig):
        self.config = config
        self.base_url = config.endpoint.rstrip("/")
        self.index_name = config.index_name
        self.session: Optional[aiohttp.ClientSession] = None
        self.embedding_dimension = 1536  # Default to Titan v1 dimension

        # Prepare auth
        self.auth = None
        if config.username and config.password:
            self.auth = aiohttp.BasicAuth(config.username, config.password)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            connector = aiohttp.TCPConnector(ssl=self.config.verify_certs if self.config.use_ssl else False)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector, auth=self.auth)
        return self.session

    async def close(self):
        """Close the HTTP session."""
        if self.session and not self.session.closed:
            await self.session.close()

    def set_embedding_dimension(self, dimension: int):
        """Set the embedding dimension for index mapping."""
        self.embedding_dimension = dimension
        logger.info(f"OpenSearch embedding dimension set to {dimension}")

    async def health_check(self) -> bool:
        """Check if OpenSearch is accessible."""
        try:
            session = await self._get_session()
            url = urljoin(self.base_url, "_cluster/health")

            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("status") in ["green", "yellow"]
                return False
        except Exception as e:
            logger.error(f"OpenSearch health check failed: {e}")
            return False

    async def create_index_if_not_exists(self) -> bool:
        """Create the search index if it doesn't exist."""
        try:
            session = await self._get_session()
            index_url = urljoin(self.base_url, self.index_name)

            # Check if index exists
            async with session.head(index_url) as response:
                if response.status == 200:
                    logger.info(f"Index '{self.index_name}' already exists")
                    return True

            # Create index with mappings
            index_mapping = self._get_index_mapping(self.embedding_dimension)

            async with session.put(index_url, json=index_mapping) as response:
                if response.status in [200, 201]:
                    logger.info(f"Created OpenSearch index: {self.index_name}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create index: {response.status} - {error_text}")
                    return False

        except Exception as e:
            logger.error(f"Error creating OpenSearch index: {e}")
            return False

    async def index_document(self, document: ProcessedDocument) -> bool:
        """Index a single document to OpenSearch."""
        try:
            session = await self._get_session()
            doc_url = urljoin(self.base_url, f"{self.index_name}/_doc/{document.document_id}")

            # Convert document to OpenSearch format
            os_document = document.to_opensearch_document()

            async with session.put(doc_url, json=os_document) as response:
                if response.status in [200, 201]:
                    logger.debug(f"Successfully indexed document: {document.url}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to index document {document.url}: {response.status} - {error_text}")
                    return False

        except Exception as e:
            logger.error(f"Error indexing document {document.url}: {e}")
            return False

    async def bulk_index_documents(self, documents: List[ProcessedDocument]) -> Dict[str, int]:
        """Bulk index multiple documents."""
        if not documents:
            return {"success": 0, "failed": 0}

        try:
            session = await self._get_session()
            bulk_url = urljoin(self.base_url, "_bulk")

            # Prepare bulk request body
            bulk_body = []
            for doc in documents:
                # Index operation
                bulk_body.append(json.dumps({"index": {"_index": self.index_name, "_id": doc.document_id}}))
                # Document content
                bulk_body.append(json.dumps(doc.to_opensearch_document()))

            bulk_data = "\\n".join(bulk_body) + "\\n"

            headers = {"Content-Type": "application/json"}
            async with session.post(bulk_url, data=bulk_data, headers=headers) as response:
                if response.status == 200:
                    result = await response.json()
                    return self._parse_bulk_response(result)
                else:
                    error_text = await response.text()
                    logger.error(f"Bulk indexing failed: {response.status} - {error_text}")
                    return {"success": 0, "failed": len(documents)}

        except Exception as e:
            logger.error(f"Error in bulk indexing: {e}")
            return {"success": 0, "failed": len(documents)}

    async def search(self, query: str, size: int = 10, from_: int = 0) -> Dict[str, Any]:
        """Search documents using hybrid BM25 + vector search."""
        try:
            session = await self._get_session()
            search_url = urljoin(self.base_url, f"{self.index_name}/_search")

            search_query = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^2", "content", "keywords"],
                        "type": "best_fields",
                        "fuzziness": "AUTO",
                    }
                },
                "highlight": {"fields": {"title": {}, "content": {"fragment_size": 150, "number_of_fragments": 3}}},
                "size": size,
                "from": from_,
                "_source": {
                    "excludes": ["embedding"]  # Don't return large embedding vectors
                },
            }

            async with session.post(search_url, json=search_query) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    logger.error(f"Search failed: {response.status} - {error_text}")
                    return {"hits": {"hits": [], "total": {"value": 0}}}

        except Exception as e:
            logger.error(f"Error in search: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    def _get_index_mapping(self, embedding_dimension: int = 1536) -> Dict[str, Any]:
        """
        Get the index mapping configuration.

        Args:
            embedding_dimension: Dimension of embeddings (e.g., 1536 for Titan v1, 1024 for Titan v2)
        """
        return {
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "url_hash": {"type": "keyword"},
                    "domain": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "japanese_analyzer",
                        "search_analyzer": "japanese_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256},
                            "standard": {"type": "text", "analyzer": "standard"}  # Fallback for non-Japanese
                        },
                    },
                    "content": {
                        "type": "text", 
                        "analyzer": "japanese_analyzer",
                        "search_analyzer": "japanese_analyzer",
                        "fields": {
                            "standard": {"type": "text", "analyzer": "standard"}  # Fallback for non-Japanese
                        }
                    },
                    "content_type": {"type": "keyword"},
                    "language": {"type": "keyword"},
                    "fetched_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                    "indexed_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                    "content_length": {"type": "integer"},
                    "processing_priority": {"type": "integer"},
                    "status_code": {"type": "integer"},
                    "keywords": {
                        "type": "text", 
                        "analyzer": "japanese_analyzer",
                        "search_analyzer": "japanese_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "standard": {"type": "text", "analyzer": "standard"}
                        }
                    },
                    "categories": {"type": "keyword"},
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": embedding_dimension,  # Dynamic embedding dimensions
                        "method": {"name": "hnsw", "space_type": "cosinesim", "engine": "lucene"},
                    },
                    "raw_s3_key": {"type": "keyword", "index": False},
                    "parsed_s3_key": {"type": "keyword", "index": False},
                }
            },
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,  # Single node for development
                    "refresh_interval": "5s",
                },
                "analysis": {
                    "analyzer": {
                        "japanese_analyzer": {
                            "type": "custom",
                            "tokenizer": "kuromoji_tokenizer",
                            "filter": [
                                "kuromoji_baseform",
                                "kuromoji_part_of_speech",
                                "cjk_width",
                                "lowercase",
                                "kuromoji_stemmer"
                            ],
                        }
                    },
                    "tokenizer": {
                        "kuromoji_tokenizer": {
                            "type": "kuromoji_tokenizer",
                            "mode": "search"
                        }
                    }
                },
            },
        }

    def _parse_bulk_response(self, response: Dict[str, Any]) -> Dict[str, int]:
        """Parse bulk operation response."""
        success_count = 0
        failed_count = 0

        for item in response.get("items", []):
            for operation, result in item.items():
                if result.get("status", 500) in [200, 201]:
                    success_count += 1
                else:
                    failed_count += 1
                    error = result.get("error", {})
                    logger.error(f"Bulk operation failed: {error}")

        return {"success": success_count, "failed": failed_count}

    async def search_raw(self, search_body: Dict[str, Any]) -> Dict[str, Any]:
        """Execute raw search query against OpenSearch."""
        try:
            session = await self._get_session()
            search_url = urljoin(self.base_url, f"{self.index_name}/_search")

            async with session.post(search_url, json=search_body) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    logger.error(f"Raw search failed: {response.status} - {error_text}")
                    return {"hits": {"hits": [], "total": {"value": 0}}}

        except Exception as e:
            logger.error(f"Error in raw search: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    async def create_index_template(self, template_name: str, template_body: Dict[str, Any]) -> bool:
        """Create or update index template."""
        try:
            session = await self._get_session()
            template_url = urljoin(self.base_url, f"_index_template/{template_name}")

            async with session.put(template_url, json=template_body) as response:
                if response.status in [200, 201]:
                    logger.info(f"Created/updated index template: {template_name}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create index template {template_name}: {response.status} - {error_text}")
                    return False

        except Exception as e:
            logger.error(f"Error creating index template {template_name}: {e}")
            return False

    async def create_index(self, index_name: str, index_body: Optional[Dict[str, Any]] = None) -> bool:
        """Create index."""
        try:
            session = await self._get_session()
            index_url = urljoin(self.base_url, index_name)

            body = index_body or {}
            async with session.put(index_url, json=body) as response:
                if response.status in [200, 201]:
                    logger.info(f"Created index: {index_name}")
                    return True
                elif response.status == 400 and "resource_already_exists_exception" in await response.text():
                    logger.info(f"Index {index_name} already exists")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create index {index_name}: {response.status} - {error_text}")
                    return False

        except Exception as e:
            logger.error(f"Error creating index {index_name}: {e}")
            return False
