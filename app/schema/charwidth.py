"""Schema for character width analysis."""

from typing import List, Optional
from pydantic import BaseModel


class CharWidthAnalysis(BaseModel):
    """Character width analysis result."""
    text: str
    total_chars: int
    fullwidth_chars: List[str]
    halfwidth_chars: List[str]
    other_chars: List[str]
    fullwidth_count: int
    halfwidth_count: int
    other_count: int
    is_all_fullwidth: bool
    is_all_halfwidth: bool
    has_mixed_widths: bool


class CharWidthResponse(BaseModel):
    """Response for character width analysis."""
    original_query: str
    is_chahan_related: bool
    analysis: CharWidthAnalysis
    converted_fullwidth: str
    converted_halfwidth: str
    message: str