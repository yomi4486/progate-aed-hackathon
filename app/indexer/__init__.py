"""
Indexer service for processing crawled content and indexing to OpenSearch.

This module handles the flow:
1. Receive IndexingMessage from SQS queue
2. Download parsed content from S3
3. Generate embeddings via Bedrock
4. Index to OpenSearch with BM25 + vector search support
5. Delete processed SQS message
"""
