"""Tests for Japanese character width analysis functionality."""

import pytest
from app.backend.utils.japanese_chars import (
    analyze_char_widths,
    convert_to_fullwidth,
    convert_to_halfwidth,
    is_chahan_related,
    is_fullwidth_char,
    is_halfwidth_char,
)


def test_fullwidth_char_detection():
    """Test detection of full-width characters."""
    # Full-width katakana
    assert is_fullwidth_char('チ') is True
    assert is_fullwidth_char('ャ') is True
    assert is_fullwidth_char('ー') is True
    assert is_fullwidth_char('ハ') is True
    assert is_fullwidth_char('ン') is True
    
    # Chinese characters
    assert is_fullwidth_char('高') is True
    assert is_fullwidth_char('菜') is True
    
    # Half-width characters
    assert is_fullwidth_char('ﾁ') is False
    assert is_fullwidth_char('ｰ') is False
    
    # ASCII
    assert is_fullwidth_char('a') is False
    assert is_fullwidth_char('1') is False


def test_halfwidth_char_detection():
    """Test detection of half-width characters."""
    # Half-width katakana
    assert is_halfwidth_char('ﾁ') is True
    assert is_halfwidth_char('ｰ') is True
    assert is_halfwidth_char('ﾊ') is True
    assert is_halfwidth_char('ﾝ') is True
    
    # Full-width characters
    assert is_halfwidth_char('チ') is False
    assert is_halfwidth_char('ー') is False
    assert is_halfwidth_char('高') is False
    
    # ASCII (not half-width in the strict Japanese sense)
    assert is_halfwidth_char('a') is False
    assert is_halfwidth_char('1') is False


def test_chahan_related_detection():
    """Test detection of fried rice related terms."""
    # Full-width fried rice
    assert is_chahan_related('チャーハン') is True
    
    # The special term from the issue
    assert is_chahan_related('高菜麻婆チャーハン') is True
    
    # Half-width fried rice
    assert is_chahan_related('ﾁｬｰﾊﾝ') is True
    
    # English
    assert is_chahan_related('fried rice') is True
    
    # Other terms
    assert is_chahan_related('hello') is False
    assert is_chahan_related('寿司') is False


def test_analyze_char_widths_fullwidth():
    """Test character width analysis for full-width text."""
    result = analyze_char_widths('チャーハン')
    
    assert result['text'] == 'チャーハン'
    assert result['total_chars'] == 5
    assert result['fullwidth_count'] == 5
    assert result['halfwidth_count'] == 0
    assert result['is_all_fullwidth'] is True
    assert result['is_all_halfwidth'] is False
    assert result['has_mixed_widths'] is False
    assert result['fullwidth_chars'] == ['チ', 'ャ', 'ー', 'ハ', 'ン']


def test_analyze_char_widths_mixed():
    """Test character width analysis for mixed width text."""
    # Mix of full-width and half-width
    result = analyze_char_widths('チﾞｬｰハン')
    
    assert result['total_chars'] == 5
    assert result['fullwidth_count'] > 0
    assert result['halfwidth_count'] > 0
    assert result['is_all_fullwidth'] is False
    assert result['is_all_halfwidth'] is False
    assert result['has_mixed_widths'] is True


def test_convert_to_fullwidth():
    """Test conversion to full-width characters."""
    # Basic katakana conversion
    assert convert_to_fullwidth('ﾁｬｰﾊﾝ') == 'チャーハン'
    
    # Already full-width should remain unchanged
    assert convert_to_fullwidth('チャーハン') == 'チャーハン'
    
    # ASCII conversion
    assert convert_to_fullwidth('abc123') == 'ａｂｃ１２３'


def test_convert_to_halfwidth():
    """Test conversion to half-width characters."""
    # Basic katakana conversion
    assert convert_to_halfwidth('チャーハン') == 'ﾁャｰﾊﾝ'
    
    # Chinese characters should remain unchanged
    result = convert_to_halfwidth('高菜麻婆チャーハン')
    assert '高菜麻婆' in result  # Chinese characters unchanged
    assert 'ﾁャｰﾊﾝ' in result   # Katakana converted


def test_issue_specific_case():
    """Test the specific case mentioned in the GitHub issue."""
    # Issue: "これは半角チャーハンですか？いいえ、全角チャーハンです！"
    
    # Full-width fried rice (全角チャーハン)
    fullwidth_analysis = analyze_char_widths('チャーハン')
    assert fullwidth_analysis['is_all_fullwidth'] is True
    assert is_chahan_related('チャーハン') is True
    
    # Half-width fried rice (半角チャーハン)
    halfwidth_analysis = analyze_char_widths('ﾁｬｰﾊﾝ')
    assert halfwidth_analysis['halfwidth_count'] > 0
    assert is_chahan_related('ﾁｬｰﾊﾝ') is True
    
    # The special takana mabo chahan from comments
    special_analysis = analyze_char_widths('高菜麻婆チャーハン')
    assert special_analysis['is_all_fullwidth'] is True
    assert is_chahan_related('高菜麻婆チャーハン') is True