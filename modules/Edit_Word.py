from spire.doc import *
from spire.doc.common import *

# ------------------------------------------------------------
# Helpers: text insertion + numbered headings (Arabic & Roman)
# ------------------------------------------------------------

def insert_text(
    section: Section,
    text: str,
    *,
    align: str = "left",   # left|center|right|justify
    bold: bool = False,
    font_size: float = 12.0,
    before_space: float = 0,
    after_space: float = 6,
    page_break_before: bool = False,
) -> Paragraph:
    para = section.AddParagraph()
    if page_break_before:
        para.AppendBreak(BreakType.PageBreak)
    run = para.AppendText(text)
    run.CharacterFormat.Bold = bold
    run.CharacterFormat.FontSize = font_size
    align_map = {
        "left": HorizontalAlignment.Left,
        "center": HorizontalAlignment.Center,
        "right": HorizontalAlignment.Right,
        "justify": HorizontalAlignment.Justify,
    }
    para.Format.HorizontalAlignment = align_map.get(align.lower(), HorizontalAlignment.Left)
    para.Format.BeforeSpacing = before_space
    para.Format.AfterSpacing = after_space
    return para


def _ensure_outline_numbering_style(doc: Document, style_name: str = "outlineHeading") -> ListStyle:
    # Arabic multi-level (1., 1.1., 1.1.1.)
    for i in range(doc.ListStyles.Count):
        if doc.ListStyles.get_Item(i).Name == style_name:
            return doc.ListStyles.get_Item(i)
    ls = ListStyle(doc, ListType.Numbered)
    ls.Name = style_name
    # level 0: 1.
    ls.Levels[0].PatternType = ListPatternType.Arabic
    ls.Levels[0].NumberSuffix = "."
    ls.Levels[0].TextPosition = 20.0
    # level 1: %1.%2.
    ls.Levels[1].PatternType = ListPatternType.Arabic
    ls.Levels[1].NumberPrefix = "%1."
    ls.Levels[1].NumberSuffix = "."
    ls.Levels[1].TextPosition = 30.0
    # level 2: %1.%2.%3.
    ls.Levels[2].PatternType = ListPatternType.Arabic
    ls.Levels[2].NumberPrefix = "%1.%2."
    ls.Levels[2].NumberSuffix = "."
    ls.Levels[2].TextPosition = 40.0
    doc.ListStyles.Add(ls)
    return ls


def insert_numbered_heading(
    section: Section,
    text: str,
    level: int = 0,
    style_name: str = "outlineHeading",
    bold: bool = True,
    font_size: float = 14.0,
) -> Paragraph:
    doc = section.Document
    _ensure_outline_numbering_style(doc, style_name)
    p = section.AddParagraph()
    r = p.AppendText(text)
    r.CharacterFormat.Bold = bold
    r.CharacterFormat.FontSize = font_size
    p.ListFormat.ApplyStyle(style_name)
    p.ListFormat.ListLevelNumber = max(0, min(level, 8))
    p.ListFormat.ContinueListNumbering()
    p.Format.HorizontalAlignment = HorizontalAlignment.Left
    return p


def _ensure_roman_numbering_style(doc: Document, style_name: str = "romanHeading") -> ListStyle:
    # Upper Roman (I., II., III., ...)
    for i in range(doc.ListStyles.Count):
        if doc.ListStyles.get_Item(i).Name == style_name:
            return doc.ListStyles.get_Item(i)
    ls = ListStyle(doc, ListType.Numbered)
    ls.Name = style_name
    ls.Levels[0].PatternType = ListPatternType.UpRoman
    ls.Levels[0].NumberSuffix = "."
    ls.Levels[0].TextPosition = 20.0
    doc.ListStyles.Add(ls)
    return ls


def insert_roman_heading(
    section: Section,
    text: str,
    level: int = 0,
    style_name: str = "romanHeading",
    bold: bool = True,
    font_size: float = 14.0,
) -> Paragraph:
    doc = section.Document
    _ensure_roman_numbering_style(doc, style_name)
    p = section.AddParagraph()
    r = p.AppendText(text)
    r.CharacterFormat.Bold = bold
    r.CharacterFormat.FontSize = font_size
    p.ListFormat.ApplyStyle(style_name)
    p.ListFormat.ListLevelNumber = max(0, min(level, 8))
    p.ListFormat.ContinueListNumbering()
    p.Format.HorizontalAlignment = HorizontalAlignment.Left
    return p


def _ensure_bulleted_style(doc: Document, style_name="bulletHeading", bullet_char="•") -> ListStyle:
    # 若已有同名樣式則直接回傳
    for i in range(doc.ListStyles.Count):
        if doc.ListStyles.get_Item(i).Name == style_name:
            return doc.ListStyles.get_Item(i)
    ls = ListStyle(doc, ListType.Bulleted)
    ls.Name = style_name
    level = ls.Levels[0]
    level.BulletCharacter = bullet_char
    level.CharacterFormat.FontName = "Symbol"
    level.TextPosition = 20.0
    doc.ListStyles.Add(ls)
    return ls


def insert_bulleted_heading(section: Section, text: str, level: int = 0,
                            style_name: str = "bulletHeading",
                            bullet_char: str = "•",
                            bold: bool = True, font_size: float = 14.0) -> Paragraph:
    doc = section.Document
    _ensure_bulleted_style(doc, style_name, bullet_char)
    p = section.AddParagraph()
    r = p.AppendText(text)
    r.CharacterFormat.Bold = bold
    r.CharacterFormat.FontSize = font_size
    p.ListFormat.ApplyStyle(style_name)
    p.ListFormat.ListLevelNumber = max(0, min(level, 8))
    p.ListFormat.ContinueListNumbering()
    p.Format.HorizontalAlignment = HorizontalAlignment.Left
    return p
