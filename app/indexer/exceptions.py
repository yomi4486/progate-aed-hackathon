"""
Custom exceptions for the indexer service.
"""


from typing import Optional


class IndexerException(Exception):
    """Base exception for all indexer-related errors."""

    pass


class RetryableException(IndexerException):
    """Exception that indicates a message should be retried."""

    def __init__(self, message: str, retry_delay: int = 0):
        super().__init__(message)
        self.retry_delay = retry_delay


class NonRetryableException(IndexerException):
    """Exception that indicates a message should not be retried."""

    pass


class OpenSearchException(RetryableException):
    """OpenSearch-related errors."""

    def __init__(self, message: str, status_code: Optional[int] = None, retry_delay: int = 5):
        super().__init__(message, retry_delay)
        self.status_code = status_code


class BedrockException(RetryableException):
    """Bedrock-related errors."""

    def __init__(self, message: str, error_code: Optional[str] = None, retry_delay: int = 10):
        super().__init__(message, retry_delay)
        self.error_code = error_code


class S3Exception(RetryableException):
    """S3-related errors."""

    def __init__(self, message: str, error_code: Optional[str] = None, retry_delay: int = 3):
        super().__init__(message, retry_delay)
        self.error_code = error_code


class DocumentProcessingException(NonRetryableException):
    """Document processing errors that should not be retried."""

    pass


class ConfigurationException(NonRetryableException):
    """Configuration-related errors."""

    pass


class ThrottlingException(RetryableException):
    """API throttling errors with exponential backoff."""

    def __init__(self, message: str, service: str, retry_delay: int = 30):
        super().__init__(message, retry_delay)
        self.service = service


class ValidationException(NonRetryableException):
    """Input validation errors."""

    pass
