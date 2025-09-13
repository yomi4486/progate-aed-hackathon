"""
Bedrock client for generating text embeddings using Amazon Titan.
"""

import asyncio
import json
import logging
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError

from .config import BedrockConfig

logger = logging.getLogger(__name__)


class BedrockClient:
    """
    Client for Amazon Bedrock embedding generation.
    """

    def __init__(self, config: BedrockConfig):
        self.config = config
        self.client = boto3.client("bedrock-runtime", region_name=config.region)

        # Model configurations
        self.model_configs = {
            "amazon.titan-embed-text-v1": {
                "model_id": "amazon.titan-embed-text-v1",
                "max_input_length": 8192,
                "embedding_dimension": 1536,
                "request_format": "titan",
            },
            "amazon.titan-embed-text-v2:0": {
                "model_id": "amazon.titan-embed-text-v2:0",
                "max_input_length": 8192,
                "embedding_dimension": 1024,
                "request_format": "titan_v2",
            },
        }

        self.current_model = self.model_configs.get(config.embedding_model)
        if not self.current_model:
            raise ValueError(f"Unsupported embedding model: {config.embedding_model}")

    async def generate_embeddings(self, text: str) -> Optional[List[float]]:
        """
        Generate embeddings for the given text.

        Args:
            text: Input text to generate embeddings for

        Returns:
            List of embedding values, or None if generation failed
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for embedding generation")
            return None

        try:
            # Truncate text if it's too long
            text = self._truncate_text(text)

            # Generate embeddings in executor to avoid blocking
            loop = asyncio.get_event_loop()
            embeddings = await loop.run_in_executor(None, self._generate_embeddings_sync, text)

            return embeddings

        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            return None

    def _generate_embeddings_sync(self, text: str) -> List[float]:
        """Synchronous embedding generation."""
        try:
            # Prepare request body based on model
            if self.current_model["request_format"] == "titan":
                body = json.dumps({"inputText": text})
            elif self.current_model["request_format"] == "titan_v2":
                body = json.dumps(
                    {"inputText": text, "dimensions": self.current_model["embedding_dimension"], "normalize": True}
                )
            else:
                raise ValueError(f"Unknown request format: {self.current_model['request_format']}")

            # Make request to Bedrock
            response = self.client.invoke_model(
                modelId=self.current_model["model_id"],
                contentType="application/json",
                accept="application/json",
                body=body,
            )

            # Parse response
            response_body = json.loads(response["body"].read().decode("utf-8"))

            # Extract embeddings based on model response format
            if "embedding" in response_body:
                return response_body["embedding"]
            elif "embeddings" in response_body:
                return response_body["embeddings"][0]  # First embedding
            else:
                logger.error(f"Unexpected response format: {response_body}")
                return []

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ValidationException":
                logger.error(f"Input validation failed: {e}")
            elif error_code == "ThrottlingException":
                logger.error("Bedrock API throttled - consider reducing request rate")
            else:
                logger.error(f"Bedrock API error: {error_code} - {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in embedding generation: {e}")
            raise

    async def generate_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        Generate embeddings for multiple texts.

        Args:
            texts: List of input texts

        Returns:
            List of embedding lists (or None for failed generations)
        """
        if not texts:
            return []

        # Process texts concurrently with rate limiting
        semaphore = asyncio.Semaphore(5)  # Limit concurrent requests

        async def generate_with_semaphore(text: str) -> Optional[List[float]]:
            async with semaphore:
                return await self.generate_embeddings(text)
                await asyncio.sleep(0.1)  # Small delay to avoid rate limiting

        tasks = [generate_with_semaphore(text) for text in texts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to None
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Embedding generation failed: {result}")
                processed_results.append(None)
            else:
                processed_results.append(result)

        return processed_results

    def _truncate_text(self, text: str) -> str:
        """Truncate text to fit within model limits."""
        max_length = self.current_model["max_input_length"]

        if len(text) <= max_length:
            return text

        # Truncate at word boundary near the limit
        truncated = text[:max_length]
        last_space = truncated.rfind(" ")

        if last_space > max_length * 0.8:  # If we found a space in the last 20%
            truncated = truncated[:last_space]

        logger.warning(f"Text truncated from {len(text)} to {len(truncated)} characters")
        return truncated

    def get_embedding_dimension(self) -> int:
        """Get the embedding dimension for the current model."""
        return self.current_model["embedding_dimension"]

    async def test_connection(self) -> bool:
        """Test connection to Bedrock service."""
        try:
            test_embedding = await self.generate_embeddings("test")
            return test_embedding is not None and len(test_embedding) > 0
        except Exception as e:
            logger.error(f"Bedrock connection test failed: {e}")
            return False
