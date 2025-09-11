"""
Storage components for the distributed crawler.

Provides S3 integration for storing crawled content and managing
the data pipeline from raw HTML to processed documents.
"""

from .pipeline import DataPipeline, IndexingMessage, PipelineClient, ProcessingEvent
from .s3_client import S3StorageClient

__all__ = [
    "S3StorageClient",
    "DataPipeline",
    "PipelineClient",
    "IndexingMessage",
    "ProcessingEvent",
]
