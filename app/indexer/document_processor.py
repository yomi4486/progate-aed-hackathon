"""
Document processor for converting parsed content into search-ready documents.
"""

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from ..crawler.storage.pipeline import IndexingMessage
from .config import IndexerConfig
from .mecab_analyzer import get_mecab_analyzer


@dataclass
class ProcessedDocument:
    """A document ready for indexing to OpenSearch."""

    # Required fields (no defaults)
    document_id: str
    url: str
    url_hash: str
    domain: str
    title: str
    content: str
    fetched_at: datetime
    indexed_at: datetime
    content_length: int
    processing_priority: int
    status_code: int

    # Optional fields (with defaults)
    content_type: str = "html"
    language: str = "ja"
    embedding: Optional[List[float]] = None
    keywords: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    raw_s3_key: str = ""
    parsed_s3_key: str = ""

    def to_opensearch_document(self) -> Dict[str, Union[str, int, float, List[str], List[float]]]:
        """Convert to OpenSearch document format."""
        doc: Dict[str, Union[str, int, float, List[str], List[float]]] = {
            "url": self.url,
            "url_hash": self.url_hash,
            "domain": self.domain,
            "title": self.title,
            "content": self.content,
            "content_type": self.content_type,
            "language": self.language,
            "fetched_at": self.fetched_at.isoformat(),
            "indexed_at": self.indexed_at.isoformat(),
            "content_length": self.content_length,
            "processing_priority": self.processing_priority,
            "status_code": self.status_code,
            "raw_s3_key": self.raw_s3_key,
            "parsed_s3_key": self.parsed_s3_key,
        }

        # Add optional fields
        if self.embedding:
            doc["embedding"] = self.embedding
        if self.keywords:
            doc["keywords"] = self.keywords
        if self.categories:
            doc["categories"] = self.categories

        return doc


class DocumentProcessor:
    """
    Processes parsed content into search-ready documents.
    """

    def __init__(self, config: IndexerConfig):
        self.config = config
        self.enable_preprocessing = config.enable_content_preprocessing
        self.mecab_analyzer = get_mecab_analyzer()

    async def process_document(
        self, indexing_msg: IndexingMessage, parsed_content: Dict[str, Any]
    ) -> ProcessedDocument:
        """
        Process a parsed document into a search-ready document.

        Args:
            indexing_msg: The indexing message from SQS
            parsed_content: The parsed content from S3

        Returns:
            ProcessedDocument ready for indexing
        """

        # Extract content from parsed data
        title = self._extract_title(parsed_content)
        content = self._extract_content(parsed_content)

        # Apply preprocessing if enabled
        if self.enable_preprocessing:
            title = self._preprocess_text(title)
            content = self._preprocess_text(content)

        # Generate document ID
        document_id = self._generate_document_id(indexing_msg.url)

        # Extract metadata
        language = parsed_content.get("language", indexing_msg.language or "ja")
        keywords = self._extract_keywords(parsed_content, title, content, language) if self.enable_preprocessing else []
        categories = self._extract_categories(parsed_content, indexing_msg.domain)

        return ProcessedDocument(
            document_id=document_id,
            url=str(indexing_msg.url),
            url_hash=indexing_msg.url_hash,
            domain=indexing_msg.domain,
            title=title,
            content=content,
            content_type="html",
            language=language,
            fetched_at=indexing_msg.fetched_at,
            indexed_at=datetime.now(timezone.utc),
            content_length=indexing_msg.content_length or len(content),
            processing_priority=indexing_msg.processing_priority,
            status_code=indexing_msg.status_code,
            keywords=keywords,
            categories=categories,
            raw_s3_key=indexing_msg.raw_s3_key,
            parsed_s3_key=indexing_msg.parsed_s3_key or "",
        )

    def _extract_title(self, parsed_content: Dict[str, Any]) -> str:
        """Extract title from parsed content."""
        # Try multiple sources for title
        title_sources = [
            parsed_content.get("title"),
            parsed_content.get("metadata", {}).get("title"),
            parsed_content.get("og_title"),
            parsed_content.get("h1"),
        ]

        for title in title_sources:
            if title and isinstance(title, str) and title.strip():
                return title.strip()[:200]  # Limit title length

        # Fallback: extract from URL
        url = parsed_content.get("url", "")
        if "/" in url:
            path_part = url.split("/")[-1]
            if path_part and not path_part.endswith((".html", ".php", ".asp")):
                return path_part.replace("-", " ").replace("_", " ").title()

        return "Untitled Document"

    def _extract_content(self, parsed_content: Dict[str, Any]) -> str:
        """Extract main content from parsed data."""
        # Try multiple content sources in order of preference
        content_sources = [
            parsed_content.get("content"),
            parsed_content.get("text"),
            parsed_content.get("body"),
            parsed_content.get("article"),
            parsed_content.get("main"),
        ]

        for content in content_sources:
            if content and isinstance(content, str) and content.strip():
                return content.strip()

        # Fallback: combine multiple text fields
        fallback_fields = ["description", "summary", "excerpt"]
        fallback_content: List[str] = []

        for field in fallback_fields:
            value = parsed_content.get(field)
            if value and isinstance(value, str):
                fallback_content.append(value.strip())

        return " ".join(fallback_content) if fallback_content else "No content available"

    def _preprocess_text(self, text: str) -> str:
        """Preprocess text for better search indexing."""
        if not text:
            return ""

        # Remove excessive whitespace
        text = re.sub(r"\\s+", " ", text)

        # Remove common HTML entities that might have been missed
        html_entities = {
            "&nbsp;": " ",
            "&amp;": "&",
            "&lt;": "<",
            "&gt;": ">",
            "&quot;": '"',
            "&#39;": "'",
            "&hellip;": "...",
        }

        for entity, replacement in html_entities.items():
            text = text.replace(entity, replacement)

        # Clean up Japanese-specific formatting
        if self._is_japanese_text(text):
            text = self._preprocess_japanese_text(text)

        return text.strip()

    def _is_japanese_text(self, text: str) -> bool:
        """Check if text contains significant Japanese content."""
        japanese_chars = re.findall(r"[ひらがなカタカナ一-龯]", text)
        return len(japanese_chars) > len(text) * 0.1  # More than 10% Japanese chars

    def _preprocess_japanese_text(self, text: str) -> str:
        """Japanese-specific text preprocessing with MeCab normalization."""
        # Use MeCab for advanced Japanese processing if available
        if self.mecab_analyzer.is_available():
            # Get normalized text using MeCab morphological analysis
            normalized_text = self.mecab_analyzer.normalize_text(text)
            if normalized_text:
                return normalized_text

        # Fallback to basic normalization
        # Normalize full-width characters to half-width where appropriate
        full_to_half = str.maketrans(
            "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
        )
        text = text.translate(full_to_half)

        # Clean up common Japanese web formatting
        text = re.sub(r"[・※◆★☆■□◎●○△▲▼▽]", "", text)

        return text

    def _extract_keywords(self, parsed_content: Dict[str, Any], title: str, content: str, language: str) -> List[str]:
        """Extract keywords from parsed content using MeCab for Japanese text."""
        keywords: set[str] = set()

        # From meta keywords
        meta_keywords = parsed_content.get("metadata", {}).get("keywords", "")
        if meta_keywords:
            keywords.update(k.strip() for k in meta_keywords.split(",") if k.strip())

        # Use MeCab for Japanese text analysis
        if language in ["ja", "jp"] and self.mecab_analyzer.is_available():
            # Extract keywords from title
            if title:
                title_keywords = self.mecab_analyzer.extract_keywords(title, max_keywords=5)
                keywords.update(title_keywords)

                # Also extract compound nouns from title
                noun_phrases = self.mecab_analyzer.extract_noun_phrases(title)
                keywords.update(noun_phrases[:3])

            # Extract keywords from content (limit text length for performance)
            if content:
                # Use first 1000 characters for keyword extraction to avoid performance issues
                content_sample = content[:1000] if len(content) > 1000 else content
                content_keywords = self.mecab_analyzer.extract_keywords(content_sample, max_keywords=10)
                keywords.update(content_keywords)

                # Extract compound nouns from content
                noun_phrases = self.mecab_analyzer.extract_noun_phrases(content_sample)
                keywords.update(noun_phrases[:5])
        else:
            # Fallback for non-Japanese text or when MeCab is not available
            if title:
                # Simple keyword extraction - split on common separators
                title_keywords = re.split(r"[|\\-—–:：｜]", title)
                keywords.update(k.strip() for k in title_keywords if len(k.strip()) > 2)

            # Extract English words from content
            if content:
                english_words = re.findall(r"\b[A-Za-z]{3,}\b", content)
                keywords.update(word.lower() for word in english_words[:15])

        # Clean and limit keywords
        cleaned_keywords: List[str] = []
        for kw in keywords:
            if len(kw) >= 2 and len(kw) <= 50:  # Reasonable length
                cleaned_keywords.append(kw)

        return cleaned_keywords[:20]  # Limit to 20 keywords maximum

    def _extract_categories(self, parsed_content: Dict[str, Any], domain: str) -> List[str]:
        """Extract categories based on content and domain."""
        categories: List[str] = []

        # Domain-based categorization
        domain_categories = {
            "github.com": ["technology", "software"],
            "stackoverflow.com": ["technology", "programming"],
            "qiita.com": ["technology", "programming", "japanese"],
            "zenn.dev": ["technology", "programming", "japanese"],
            "note.com": ["blog", "japanese"],
            "medium.com": ["blog", "article"],
            "wikipedia.org": ["reference", "encyclopedia"],
            "news.yahoo.co.jp": ["news", "japanese"],
            "nikkei.com": ["news", "business", "japanese"],
        }

        if domain in domain_categories:
            categories.extend(domain_categories[domain])

        # Content-based categorization (simple keyword matching)
        content = parsed_content.get("content", "").lower()
        title = parsed_content.get("title", "").lower()
        combined_text = f"{title} {content}"

        tech_keywords = ["python", "javascript", "react", "aws", "docker", "kubernetes", "api", "database"]
        business_keywords = ["business", "marketing", "sales", "finance", "management", "strategy"]
        news_keywords = ["news", "breaking", "update", "report", "announcement"]

        if any(keyword in combined_text for keyword in tech_keywords):
            categories.append("technology")
        if any(keyword in combined_text for keyword in business_keywords):
            categories.append("business")
        if any(keyword in combined_text for keyword in news_keywords):
            categories.append("news")

        return list(set(categories))  # Remove duplicates

    def _generate_document_id(self, url: Union[str, Any]) -> str:
        """Generate a unique document ID based on URL."""
        # Use URL hash for consistent ID generation
        url_str = str(url)
        return hashlib.sha256(url_str.encode("utf-8")).hexdigest()[:16]
