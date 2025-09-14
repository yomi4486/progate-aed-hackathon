"""
Text chunking functionality for processing long documents.
"""

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import MeCab

from .config import ChunkingConfig

logger = logging.getLogger(__name__)


@dataclass
class TextChunk:
    """Represents a chunk of text with metadata."""

    content: str
    chunk_index: int
    start_position: int
    end_position: int
    token_count: Optional[int] = None
    language: str = "unknown"
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

    @property
    def length(self) -> int:
        """Get the length of the chunk content."""
        return len(self.content)


class ChunkingStrategy(ABC):
    """Abstract base class for chunking strategies."""

    @abstractmethod
    def chunk_text(self, text: str, max_size: int, overlap: int) -> List[TextChunk]:
        """Chunk text according to the strategy."""
        pass


class FixedSizeChunkingStrategy(ChunkingStrategy):
    """Simple fixed-size chunking strategy."""

    def chunk_text(self, text: str, max_size: int, overlap: int) -> List[TextChunk]:
        """Chunk text into fixed-size pieces with overlap."""
        if len(text) <= max_size:
            return [TextChunk(content=text, chunk_index=0, start_position=0, end_position=len(text))]

        chunks: List[TextChunk] = []
        chunk_index = 0
        start = 0

        while start < len(text):
            end = min(start + max_size, len(text))

            # Find a good break point near the end (prefer word boundaries)
            if end < len(text):
                # Look for word boundary within the last 10% of the chunk
                search_start = max(start, end - max_size // 10)
                word_boundary = text.rfind(" ", search_start, end)

                if word_boundary > start:
                    end = word_boundary + 1

            chunk_content = text[start:end].strip()
            if chunk_content:
                chunks.append(
                    TextChunk(content=chunk_content, chunk_index=chunk_index, start_position=start, end_position=end)
                )
                chunk_index += 1

            # Calculate next start position with overlap
            start = max(start + 1, end - overlap) if end < len(text) else end

        return chunks


class SentenceChunkingStrategy(ChunkingStrategy):
    """Sentence-aware chunking strategy."""

    def __init__(self):
        # Sentence boundary patterns for different languages
        self.sentence_patterns = {"en": r"[.!?]+\s+", "ja": r"[。！？]+\s*", "default": r"[.!?。！？]+\s*"}

    def chunk_text(self, text: str, max_size: int, overlap: int) -> List[TextChunk]:
        """Chunk text by sentences, respecting size limits."""
        # Detect language (simple heuristic)
        language = self._detect_language(text)
        pattern = self.sentence_patterns.get(language, self.sentence_patterns["default"])

        # Split into sentences
        sentences = re.split(pattern, text)
        sentences = [s.strip() for s in sentences if s.strip()]

        if not sentences:
            return []

        chunks: List[TextChunk] = []
        chunk_index = 0
        current_chunk = ""
        current_start = 0

        for _, sentence in enumerate(sentences):
            # Check if adding this sentence would exceed the limit
            potential_chunk = current_chunk + (" " if current_chunk else "") + sentence

            if len(potential_chunk) > max_size and current_chunk:
                # Save current chunk
                chunks.append(
                    TextChunk(
                        content=current_chunk.strip(),
                        chunk_index=chunk_index,
                        start_position=current_start,
                        end_position=current_start + len(current_chunk),
                        language=language,
                    )
                )
                chunk_index += 1

                # Start new chunk with overlap
                overlap_text = self._get_overlap_text(current_chunk, overlap)
                current_chunk = overlap_text + (" " if overlap_text else "") + sentence
                current_start = current_start + len(current_chunk) - len(overlap_text) - len(sentence) - 1
            else:
                if not current_chunk:
                    current_start = text.find(sentence)
                current_chunk = potential_chunk

        # Add the last chunk
        if current_chunk.strip():
            chunks.append(
                TextChunk(
                    content=current_chunk.strip(),
                    chunk_index=chunk_index,
                    start_position=current_start,
                    end_position=current_start + len(current_chunk),
                    language=language,
                )
            )

        return chunks

    def _detect_language(self, text: str) -> str:
        """Simple language detection based on character patterns."""
        # Count Japanese characters
        japanese_chars = len(re.findall(r"[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9faf]", text))
        total_chars = len(text)

        if total_chars == 0:
            return "default"

        if japanese_chars / total_chars > 0.1:
            return "ja"
        else:
            return "en"

    def _get_overlap_text(self, text: str, overlap_size: int) -> str:
        """Get the last `overlap_size` characters as overlap."""
        if len(text) <= overlap_size:
            return text
        return text[-overlap_size:]


class SemanticChunkingStrategy(ChunkingStrategy):
    """Semantic chunking strategy using morphological analysis."""

    def __init__(self):
        self.mecab = MeCab.Tagger("-Owakati")
        logger.info("MeCab initialized for semantic chunking")

    def chunk_text(self, text: str, max_size: int, overlap: int) -> List[TextChunk]:
        """Chunk text using semantic boundaries."""
        if self.mecab and self._is_japanese_text(text):
            return self._chunk_japanese_text(text, max_size, overlap)
        else:
            # Fallback to sentence chunking
            sentence_chunker = SentenceChunkingStrategy()
            return sentence_chunker.chunk_text(text, max_size, overlap)

    def _is_japanese_text(self, text: str) -> bool:
        """Check if text contains significant Japanese content."""
        japanese_chars = len(re.findall(r"[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9faf]", text))
        return japanese_chars > len(text) * 0.1

    def _chunk_japanese_text(self, text: str, max_size: int, overlap: int) -> List[TextChunk]:
        """Chunk Japanese text using MeCab morphological analysis."""
        try:
            # Get morphological analysis
            tokens: List[str] = str(self.mecab.parse(text)).strip().split()  # type: ignore

            # Reconstruct text with proper boundaries
            chunks: List[TextChunk] = []
            chunk_index = 0
            current_chunk = ""
            current_start = 0

            # Process tokens and build chunks
            for token in tokens:
                potential_chunk = current_chunk + token + " "

                if len(potential_chunk) > max_size and current_chunk:
                    # Save current chunk
                    chunks.append(
                        TextChunk(
                            content=current_chunk.strip(),
                            chunk_index=chunk_index,
                            start_position=current_start,
                            end_position=current_start + len(current_chunk),
                            language="ja",
                        )
                    )
                    chunk_index += 1

                    # Start new chunk with overlap
                    current_chunk = token + " "
                    current_start = current_start + len(current_chunk) - len(token) - 1
                else:
                    current_chunk = potential_chunk

            # Add the last chunk
            if current_chunk.strip():
                chunks.append(
                    TextChunk(
                        content=current_chunk.strip(),
                        chunk_index=chunk_index,
                        start_position=current_start,
                        end_position=current_start + len(current_chunk),
                        language="ja",
                    )
                )

            return chunks

        except Exception as e:
            logger.error(f"Error in Japanese semantic chunking: {e}")
            # Fallback to sentence chunking
            sentence_chunker = SentenceChunkingStrategy()
            return sentence_chunker.chunk_text(text, max_size, overlap)


class TextChunker:
    """Main text chunking class that coordinates different strategies."""

    def __init__(self, config: ChunkingConfig):
        self.config = config
        self.strategies = {
            "fixed": FixedSizeChunkingStrategy(),
            "sentence": SentenceChunkingStrategy(),
            "semantic": SemanticChunkingStrategy(),
        }

    def chunk_text(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> List[TextChunk]:
        """
        Chunk text according to the configured strategy.

        Args:
            text: The text to chunk
            metadata: Optional metadata to attach to chunks

        Returns:
            List of text chunks
        """
        if not self.config.enable_chunking:
            return [
                TextChunk(
                    content=text, chunk_index=0, start_position=0, end_position=len(text), metadata=metadata or {}
                )
            ]

        # Skip chunking if text is already small enough
        if len(text) <= self.config.max_chunk_size:
            return [
                TextChunk(
                    content=text, chunk_index=0, start_position=0, end_position=len(text), metadata=metadata or {}
                )
            ]

        # Get the appropriate strategy
        strategy = self.strategies.get(self.config.chunk_strategy, self.strategies["semantic"])

        # Chunk the text
        chunks = strategy.chunk_text(text, self.config.max_chunk_size, self.config.chunk_overlap)

        # Add metadata to all chunks
        if metadata:
            for chunk in chunks:
                chunk.metadata.update(metadata)  # type: ignore

        logger.info(f"Text chunked into {len(chunks)} pieces using {self.config.chunk_strategy} strategy")

        return chunks

    def estimate_token_count(self, text: str) -> int:
        """Estimate token count for text (rough approximation)."""
        # Simple estimation: 1 token per 4 characters for English, 1 per 2 for Japanese
        japanese_chars = len(re.findall(r"[\u3040-\u309f\u30a0-\u30ff\u4e00-\u9faf]", text))
        english_chars = len(text) - japanese_chars

        return int((english_chars / 4) + (japanese_chars / 2))

    def should_chunk(self, text: str) -> bool:
        """Determine if text should be chunked based on configuration."""
        if not self.config.enable_chunking:
            return False

        return len(text) > self.config.max_chunk_size

    def get_optimal_chunk_size(self, text_length: int, target_chunks: int = 3) -> int:
        """Calculate optimal chunk size for a given text length."""
        if target_chunks <= 1:
            return min(text_length, self.config.max_chunk_size)

        # Calculate size that would result in approximately target_chunks
        estimated_size = text_length // target_chunks

        # Ensure it doesn't exceed max size
        return min(estimated_size, self.config.max_chunk_size)
