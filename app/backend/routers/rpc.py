from random import random

from fastapi import APIRouter

from ...schema import SearchHit, SearchResponse, CharWidthAnalysis, CharWidthResponse
from ..utils.japanese_chars import (
    analyze_char_widths,
    convert_to_fullwidth,
    convert_to_halfwidth,
    is_chahan_related,
)

rpc_router = APIRouter()


@rpc_router.get("/search")
async def search_items(query: str, page: int = 1, size: int = 30) -> SearchResponse:
    all_hits = [
        SearchHit(
            id=str(i),
            title=f"Random result {i} for '{query}'",
            url=f"https://example{i}.com/{hash(query)}",
            site=f"example{i}.com",
            lang="en",
            score=round(random(), 2),
        )
        for i in range(1, 101)
    ]

    start = (page - 1) * size
    end = start + size
    paginated_hits = all_hits[start:end]

    return SearchResponse(
        total=len(all_hits),
        hits=paginated_hits,
        page=page,
        size=size,
    )


@rpc_router.get("/analyze-chahan")
async def analyze_chahan_width(query: str) -> CharWidthResponse:
    """
    Analyze character widths for fried rice (チャーハン) related queries.
    
    This endpoint specifically addresses the issue:
    "これは半角チャーハンですか？いいえ、全角チャーハンです！"
    (Is this half-width fried rice? No, it's full-width fried rice!)
    """
    # Analyze the character widths
    analysis_data = analyze_char_widths(query)
    analysis = CharWidthAnalysis(**analysis_data)
    
    # Check if it's chahan related
    chahan_related = is_chahan_related(query)
    
    # Generate conversions
    fullwidth_version = convert_to_fullwidth(query)
    halfwidth_version = convert_to_halfwidth(query)
    
    # Create appropriate message
    if chahan_related:
        if analysis.is_all_fullwidth:
            message = "これは全角チャーハンです！高菜麻婆チャーハンも美味しいですね！"
        elif analysis.is_all_halfwidth:
            message = "これは半角チャーハンですね。でも全角チャーハンの方が美味しく見えませんか？"
        elif analysis.has_mixed_widths:
            message = "混合幅のチャーハンですね！全角と半角が混在しています。"
        else:
            message = "チャーハンに関連していますが、文字幅を分析中です..."
    else:
        message = "チャーハンではありませんが、文字幅を分析しました。"
    
    return CharWidthResponse(
        original_query=query,
        is_chahan_related=chahan_related,
        analysis=analysis,
        converted_fullwidth=fullwidth_version,
        converted_halfwidth=halfwidth_version,
        message=message,
    )
