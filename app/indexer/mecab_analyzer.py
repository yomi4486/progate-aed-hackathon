"""
MeCab-based Japanese text analyzer for enhanced natural language processing.

Provides morphological analysis, keyword extraction, and text normalization
specifically optimized for Japanese content indexing and search.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Global module variables for MeCab
MeCab: Any = None
unidic_lite: Any = None
mecab_available = False

try:
    import MeCab
    import unidic_lite

    mecab_available = True
except ImportError as e:
    logger.warning(f"MeCab not available: {e}")
    mecab_available = False


@dataclass
class MorphologicalFeature:
    """Represents a morphological analysis result from MeCab."""

    surface: str  # Original word
    part_of_speech: str  # 品詞 (part of speech)
    pos_detail1: str  # 品詞細分類1
    pos_detail2: str  # 品詞細分類2
    pos_detail3: str  # 品詞細分類3
    inflection_type: str  # 活用型
    inflection_form: str  # 活用形
    base_form: str  # 基本形
    reading: str  # 読み
    pronunciation: str  # 発音

    def is_content_word(self) -> bool:
        """Check if this morpheme is a content word (meaningful for search)."""
        content_pos = {
            "名詞",  # Noun
            "動詞",  # Verb
            "形容詞",  # Adjective
            "副詞",  # Adverb
            "連体詞",  # Adnominal adjective
            "感動詞",  # Interjection
        }
        return self.part_of_speech in content_pos

    def is_stop_word(self) -> bool:
        """Check if this morpheme should be considered a stop word."""
        stop_pos = {
            "助詞",  # Particle
            "助動詞",  # Auxiliary verb
            "記号",  # Symbol
            "フィラー",  # Filler
        }

        stop_pos_details = {
            "代名詞",  # Pronoun
            "非自立",  # Dependent
            "数",  # Number
        }

        return (
            self.part_of_speech in stop_pos
            or self.pos_detail1 in stop_pos_details
            or len(self.surface) < 2  # Too short
        )


class MeCabAnalyzer:
    """
    MeCab-based Japanese text analyzer with enhanced NLP features.

    Provides morphological analysis, keyword extraction, and text normalization
    specifically for search indexing purposes.
    """

    def __init__(self):
        self.tagger: Optional[Any] = None
        self._initialize_mecab()

    def _initialize_mecab(self) -> None:
        """Initialize MeCab tagger with UniDic dictionary."""
        if not mecab_available:
            logger.warning("MeCab not available, Japanese analysis will be limited")
            return

        try:
            # Use UniDic dictionary for better analysis
            if unidic_lite is not None:
                dicdir = unidic_lite.DICDIR
                self.tagger = MeCab.Tagger(f"-d {dicdir}")
                logger.info("MeCab initialized successfully with UniDic dictionary")

        except Exception as e:
            logger.error(f"Failed to initialize MeCab: {e}")
            self.tagger = None

    def is_available(self) -> bool:
        """Check if MeCab analyzer is available."""
        return self.tagger is not None

    def analyze(self, text: str) -> List[MorphologicalFeature]:
        """
        Perform morphological analysis on Japanese text.

        Args:
            text: Japanese text to analyze

        Returns:
            List of morphological features
        """
        if not self.is_available():
            return []

        try:
            # Clean text before analysis
            text = self._preprocess_text(text)

            # Perform morphological analysis
            if self.tagger is not None:
                node = self.tagger.parseToNode(text)
                features: List[MorphologicalFeature] = []

                while node:
                    if hasattr(node, "surface") and node.surface:  # Skip BOS/EOS nodes
                        feature_parts = node.feature.split(",")

                        # Handle incomplete feature vectors
                        while len(feature_parts) < 9:
                            feature_parts.append("*")

                        feature = MorphologicalFeature(
                            surface=str(node.surface),
                            part_of_speech=str(feature_parts[0]),
                            pos_detail1=str(feature_parts[1]),
                            pos_detail2=str(feature_parts[2]),
                            pos_detail3=str(feature_parts[3]),
                            inflection_type=str(feature_parts[4]),
                            inflection_form=str(feature_parts[5]),
                            base_form=str(feature_parts[6]) if feature_parts[6] != "*" else str(node.surface),
                            reading=str(feature_parts[7]) if len(feature_parts) > 7 else "*",
                            pronunciation=str(feature_parts[8]) if len(feature_parts) > 8 else "*",
                        )

                        features.append(feature)

                    if hasattr(node, "next"):
                        node = node.next
                    else:
                        break

                return features

            return []

        except Exception as e:
            logger.error(f"MeCab analysis failed: {e}")
            return []

    def extract_keywords(self, text: str, max_keywords: int = 20) -> List[str]:
        """
        Extract important keywords from Japanese text using MeCab.

        Args:
            text: Japanese text to analyze
            max_keywords: Maximum number of keywords to return

        Returns:
            List of important keywords
        """
        features = self.analyze(text)
        if not features:
            return self._fallback_keyword_extraction(text, max_keywords)

        keyword_scores: dict[str, float] = {}

        for feature in features:
            if feature.is_stop_word():
                continue

            # Use base form for better keyword normalization
            keyword = feature.base_form

            # Skip if too short or not meaningful
            if len(keyword) < 2 or keyword in ["*", "", "する", "ある", "なる"]:
                continue

            # Score based on part of speech
            score = self._calculate_keyword_score(feature)
            if score > 0:
                keyword_scores[keyword] = keyword_scores.get(keyword, 0.0) + score

        # Sort by score and return top keywords
        sorted_keywords = sorted(keyword_scores.items(), key=lambda x: x[1], reverse=True)
        return [kw for kw, _score in sorted_keywords[:max_keywords]]

    def normalize_text(self, text: str) -> str:
        """
        Normalize Japanese text using MeCab analysis.

        Args:
            text: Japanese text to normalize

        Returns:
            Normalized text with base forms
        """
        features = self.analyze(text)
        if not features:
            return self._basic_normalize(text)

        normalized_words: List[str] = []

        for feature in features:
            if feature.is_stop_word():
                continue

            # Use base form for better search matching
            word = feature.base_form if feature.base_form != "*" else feature.surface

            if len(word) >= 2:
                normalized_words.append(word)

        return " ".join(normalized_words)

    def extract_noun_phrases(self, text: str) -> List[str]:
        """
        Extract noun phrases (compound nouns) from Japanese text.

        Args:
            text: Japanese text to analyze

        Returns:
            List of noun phrases
        """
        features = self.analyze(text)
        if not features:
            return []

        noun_phrases: List[str] = []
        current_phrase: List[str] = []

        for feature in features:
            if feature.part_of_speech == "名詞":  # Noun
                current_phrase.append(feature.surface)
            else:
                if len(current_phrase) > 1:  # Multi-word noun phrase
                    phrase = "".join(current_phrase)
                    if len(phrase) >= 3:  # Minimum phrase length
                        noun_phrases.append(phrase)
                current_phrase = []

        # Handle phrase at end of text
        if len(current_phrase) > 1:
            phrase = "".join(current_phrase)
            if len(phrase) >= 3:
                noun_phrases.append(phrase)

        return noun_phrases

    def _calculate_keyword_score(self, feature: MorphologicalFeature) -> float:
        """Calculate importance score for a morphological feature."""
        base_scores = {
            "名詞": 3.0,  # Noun - highly important
            "動詞": 2.0,  # Verb
            "形容詞": 2.0,  # Adjective
            "副詞": 1.5,  # Adverb
            "連体詞": 1.0,  # Adnominal
        }

        score = base_scores.get(feature.part_of_speech, 0.0)

        # Boost score for specific noun types
        if feature.part_of_speech == "名詞":
            if feature.pos_detail1 in ["固有", "一般"]:  # Proper noun, general noun
                score += 1.0
            if feature.pos_detail1 == "固有" and feature.pos_detail2 in ["人名", "地域", "組織"]:
                score += 2.0  # Names, places, organizations

        # Boost longer words
        if len(feature.surface) >= 4:
            score += 0.5

        # Boost words with kanji
        if re.search(r"[一-龯]", feature.surface):
            score += 0.5

        return score

    def _preprocess_text(self, text: str) -> str:
        """Preprocess text before MeCab analysis."""
        if not text:
            return ""

        # Remove excessive whitespace
        text = re.sub(r"\s+", " ", text.strip())

        # Remove problematic characters that might confuse MeCab
        text = re.sub(r"[︎■□●○◆◇△▲▼▽★☆※]", "", text)

        # Normalize some punctuation
        text = text.replace("・", " ")
        text = text.replace("…", "")
        text = text.replace("‥", "")

        return text

    def _basic_normalize(self, text: str) -> str:
        """Basic text normalization without MeCab."""
        # Full-width to half-width for ASCII characters
        text = text.translate(
            str.maketrans(
                "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
                "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
            )
        )

        # Remove symbols
        text = re.sub(r"[・※◆★☆■□◎●○△▲▼▽]", "", text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text.strip())

        return text

    def _fallback_keyword_extraction(self, text: str, max_keywords: int) -> List[str]:
        """Fallback keyword extraction when MeCab is not available."""
        # Basic approach: extract words based on character types
        words: set[str] = set()

        # Extract sequences of kanji/hiragana/katakana
        japanese_words = re.findall(r"[ひらがなカタカナ一-龯]+", text)
        for word in japanese_words:
            if len(word) >= 2:
                words.add(word)

        # Extract English words
        english_words = re.findall(r"[A-Za-z]+", text)
        for word in english_words:
            if len(word) >= 3:
                words.add(word.lower())

        return list(words)[:max_keywords]


# Global analyzer instance (lazy initialized)
_analyzer_instance: Optional[MeCabAnalyzer] = None


def get_mecab_analyzer() -> MeCabAnalyzer:
    """Get global MeCab analyzer instance."""
    global _analyzer_instance
    if _analyzer_instance is None:
        _analyzer_instance = MeCabAnalyzer()
    return _analyzer_instance


def analyze_japanese_text(text: str) -> Tuple[List[str], str]:
    """
    Convenient function to analyze Japanese text and return keywords and normalized text.

    Args:
        text: Japanese text to analyze

    Returns:
        Tuple of (keywords, normalized_text)
    """
    analyzer = get_mecab_analyzer()
    keywords = analyzer.extract_keywords(text)
    normalized = analyzer.normalize_text(text)
    return keywords, normalized
