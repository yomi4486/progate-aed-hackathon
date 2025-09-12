"""Utilities for Japanese character width detection."""

import unicodedata
from typing import Dict, List


def is_fullwidth_char(char: str) -> bool:
    """
    Check if a character is full-width (全角).
    
    Args:
        char: Single character to check
        
    Returns:
        True if the character is full-width, False otherwise
    """
    if not char:
        return False
    
    # Get the Unicode East Asian Width property
    width = unicodedata.east_asian_width(char)
    
    # F = Full, W = Wide are considered full-width
    # H = Half, Na = Narrow, A = Ambiguous, N = Neutral are not full-width
    return width in ('F', 'W')


def is_halfwidth_char(char: str) -> bool:
    """
    Check if a character is half-width (半角).
    
    Args:
        char: Single character to check
        
    Returns:
        True if the character is half-width, False otherwise
    """
    if not char:
        return False
    
    # Get the Unicode East Asian Width property
    width = unicodedata.east_asian_width(char)
    
    # H = Half are considered half-width
    return width == 'H'


def analyze_char_widths(text: str) -> Dict[str, any]:
    """
    Analyze the character widths in a given text.
    
    Args:
        text: Text to analyze
        
    Returns:
        Dictionary containing analysis results
    """
    fullwidth_chars = []
    halfwidth_chars = []
    other_chars = []
    
    for char in text:
        if is_fullwidth_char(char):
            fullwidth_chars.append(char)
        elif is_halfwidth_char(char):
            halfwidth_chars.append(char)
        else:
            other_chars.append(char)
    
    return {
        "text": text,
        "total_chars": len(text),
        "fullwidth_chars": fullwidth_chars,
        "halfwidth_chars": halfwidth_chars,
        "other_chars": other_chars,
        "fullwidth_count": len(fullwidth_chars),
        "halfwidth_count": len(halfwidth_chars),
        "other_count": len(other_chars),
        "is_all_fullwidth": len(fullwidth_chars) == len(text) and len(text) > 0,
        "is_all_halfwidth": len(halfwidth_chars) == len(text) and len(text) > 0,
        "has_mixed_widths": len(fullwidth_chars) > 0 and len(halfwidth_chars) > 0
    }


def convert_to_fullwidth(text: str) -> str:
    """
    Convert half-width characters to full-width where possible.
    
    Args:
        text: Text to convert
        
    Returns:
        Converted text with half-width characters replaced by full-width equivalents
    """
    # Basic katakana half-width to full-width mapping
    halfwidth_to_fullwidth = {
        'ｱ': 'ア', 'ｲ': 'イ', 'ｳ': 'ウ', 'ｴ': 'エ', 'ｵ': 'オ',
        'ｶ': 'カ', 'ｷ': 'キ', 'ｸ': 'ク', 'ｹ': 'ケ', 'ｺ': 'コ',
        'ｻ': 'サ', 'ｼ': 'シ', 'ｽ': 'ス', 'ｾ': 'セ', 'ｿ': 'ソ',
        'ﾀ': 'タ', 'ﾁ': 'チ', 'ﾂ': 'ツ', 'ﾃ': 'テ', 'ﾄ': 'ト',
        'ﾅ': 'ナ', 'ﾆ': 'ニ', 'ﾇ': 'ヌ', 'ﾈ': 'ネ', 'ﾉ': 'ノ',
        'ﾊ': 'ハ', 'ﾋ': 'ヒ', 'ﾌ': 'フ', 'ﾍ': 'ヘ', 'ﾎ': 'ホ',
        'ﾏ': 'マ', 'ﾐ': 'ミ', 'ﾑ': 'ム', 'ﾒ': 'メ', 'ﾓ': 'モ',
        'ﾔ': 'ヤ', 'ﾕ': 'ユ', 'ﾖ': 'ヨ',
        'ﾗ': 'ラ', 'ﾘ': 'リ', 'ﾙ': 'ル', 'ﾚ': 'レ', 'ﾛ': 'ロ',
        'ﾜ': 'ワ', 'ｦ': 'ヲ', 'ﾝ': 'ン',
        'ｰ': 'ー',
        # Half-width ASCII to full-width
        '!': '！', '"': '"', '#': '＃', '$': '＄', '%': '％',
        '&': '＆', "'": "'", '(': '（', ')': '）', '*': '＊',
        '+': '＋', ',': '，', '-': '－', '.': '．', '/': '／',
        '0': '０', '1': '１', '2': '２', '3': '３', '4': '４',
        '5': '５', '6': '６', '7': '７', '8': '８', '9': '９',
        ':': '：', ';': '；', '<': '＜', '=': '＝', '>': '＞',
        '?': '？', '@': '＠',
        'A': 'Ａ', 'B': 'Ｂ', 'C': 'Ｃ', 'D': 'Ｄ', 'E': 'Ｅ',
        'F': 'Ｆ', 'G': 'Ｇ', 'H': 'Ｈ', 'I': 'Ｉ', 'J': 'Ｊ',
        'K': 'Ｋ', 'L': 'Ｌ', 'M': 'Ｍ', 'N': 'Ｎ', 'O': 'Ｏ',
        'P': 'Ｐ', 'Q': 'Ｑ', 'R': 'Ｒ', 'S': 'Ｓ', 'T': 'Ｔ',
        'U': 'Ｕ', 'V': 'Ｖ', 'W': 'Ｗ', 'X': 'Ｘ', 'Y': 'Ｙ', 'Z': 'Ｚ',
        '[': '［', '\\': '＼', ']': '］', '^': '＾', '_': '＿',
        '`': '｀',
        'a': 'ａ', 'b': 'ｂ', 'c': 'ｃ', 'd': 'ｄ', 'e': 'ｅ',
        'f': 'ｆ', 'g': 'ｇ', 'h': 'ｈ', 'i': 'ｉ', 'j': 'ｊ',
        'k': 'ｋ', 'l': 'ｌ', 'm': 'ｍ', 'n': 'ｎ', 'o': 'ｏ',
        'p': 'ｐ', 'q': 'ｑ', 'r': 'ｒ', 's': 'ｓ', 't': 'ｔ',
        'u': 'ｕ', 'v': 'ｖ', 'w': 'ｗ', 'x': 'ｘ', 'y': 'ｙ', 'z': 'ｚ',
        '{': '｛', '|': '｜', '}': '｝', '~': '～'
    }
    
    result = ""
    for char in text:
        result += halfwidth_to_fullwidth.get(char, char)
    
    return result


def convert_to_halfwidth(text: str) -> str:
    """
    Convert full-width characters to half-width where possible.
    
    Args:
        text: Text to convert
        
    Returns:
        Converted text with full-width characters replaced by half-width equivalents
    """
    # Basic katakana full-width to half-width mapping (reverse of above)
    fullwidth_to_halfwidth = {
        'ア': 'ｱ', 'イ': 'ｲ', 'ウ': 'ｳ', 'エ': 'ｴ', 'オ': 'ｵ',
        'カ': 'ｶ', 'キ': 'ｷ', 'ク': 'ｸ', 'ケ': 'ｹ', 'コ': 'ｺ',
        'サ': 'ｻ', 'シ': 'ｼ', 'ス': 'ｽ', 'セ': 'ｾ', 'ソ': 'ｿ',
        'タ': 'ﾀ', 'チ': 'ﾁ', 'ツ': 'ﾂ', 'テ': 'ﾃ', 'ト': 'ﾄ',
        'ナ': 'ﾅ', 'ニ': 'ﾆ', 'ヌ': 'ﾇ', 'ネ': 'ﾈ', 'ノ': 'ﾉ',
        'ハ': 'ﾊ', 'ヒ': 'ﾋ', 'フ': 'ﾌ', 'ヘ': 'ﾍ', 'ホ': 'ﾎ',
        'マ': 'ﾏ', 'ミ': 'ﾐ', 'ム': 'ﾑ', 'メ': 'ﾒ', 'モ': 'ﾓ',
        'ヤ': 'ﾔ', 'ユ': 'ﾕ', 'ヨ': 'ﾖ',
        'ラ': 'ﾗ', 'リ': 'ﾘ', 'ル': 'ﾙ', 'レ': 'ﾚ', 'ロ': 'ﾛ',
        'ワ': 'ﾜ', 'ヲ': 'ｦ', 'ン': 'ﾝ',
        'ー': 'ｰ',
    }
    
    result = ""
    for char in text:
        result += fullwidth_to_halfwidth.get(char, char)
    
    return result


def is_chahan_related(text: str) -> bool:
    """
    Check if text is related to fried rice (チャーハン).
    
    Args:
        text: Text to check
        
    Returns:
        True if text contains fried rice related terms
    """
    chahan_terms = [
        'チャーハン',  # full-width
        'ﾁｮｰﾊﾝ',     # half-width (though this isn't exactly correct)
        'ﾁｬｰﾊﾝ',     # half-width
        'チャーハン',   # already full-width
        'ちゃーはん',   # hiragana
        'fried rice',  # English
        'chaahan',     # romanized
        'chahaan',     # romanized alternative
        '炒飯',        # Chinese characters
        '高菜麻婆チャーハン',  # specific type mentioned in comments
        'takana mabo',  # romanized
    ]
    
    text_lower = text.lower()
    return any(term.lower() in text_lower for term in chahan_terms)