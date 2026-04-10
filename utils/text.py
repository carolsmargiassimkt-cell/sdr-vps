import unicodedata

def normalize_text(text: str) -> str:
    """
    Normalizes text to UTF-8, removing accents and fixing common encoding issues.
    Example: 'vocÃª' -> 'voce'
    """
    if not text:
        return ""
    
    # First, try to fix common encoding artifacts if they look like double-encoded UTF-8
    try:
        # This handles cases where UTF-8 bytes were interpreted as Latin-1
        text = text.encode('latin1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    # Normalize unicode characters (NFD decomposes characters, e.g., 'ê' -> 'e' + '^')
    normalized = unicodedata.normalize('NFD', text)
    
    # Filter out non-spacing mark characters (accents)
    shaved = "".join(c for c in normalized if unicodedata.category(c) != 'Mn')
    
    # Normalize whitespace
    return " ".join(shaved.split())
