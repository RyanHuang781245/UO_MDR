import re


_CHAPTER_RANGE_RE = re.compile(
    r"^(\d+(?:\.\d+)*\.?)(?:\s*[-~～至到]\s*(\d+(?:\.\d+)*\.?))?(?:\s+(.+))?$"
)
_INLINE_SECTION_RE = re.compile(r"\b(\d+(?:\.\d+)*)\b")


def parse_chapter_section_expression(text: str) -> tuple[str, str, str]:
    """
    Parse chapter section expressions like:
    - "1. 測試1.1" -> ("1", "", "測試1.1")
    - "1.1.1 - 1.1.3 測試標題" -> ("1.1.1", "1.1.3", "測試標題")
    - "1.1.1-1.1.3" -> ("1.1.1", "1.1.3", "")
    """
    raw = str(text or "").strip()
    if not raw:
        return "", "", ""
    match = _CHAPTER_RANGE_RE.match(raw)
    if not match:
        inline = _INLINE_SECTION_RE.search(raw)
        if not inline:
            return "", "", ""
        start = inline.group(1).rstrip(".")
        return start, "", raw
    start = (match.group(1) or "").rstrip(".")
    end = (match.group(2) or "").rstrip(".")
    title = (match.group(3) or "").strip()
    return start, end, title
