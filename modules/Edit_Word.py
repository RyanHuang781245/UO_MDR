from spire.doc import *
from spire.doc.common import *
import re

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


def renumber_figures_tables(
    doc: Document,
    *,
    scope: str = "global",
    figure_prefixes: tuple = ("Figure", "Fig."),
    table_prefixes: tuple = ("Table", "Tab."),
    start_fig: int = 1,
    start_table: int = 1,
) -> None:
    """Renumber figure and table captions and update cross references.

    Parameters
    ----------
    doc : Document
        Target Word document from Spire.Doc.
    scope : str
        "global" or "per-section" numbering. Global numbering by default.
    figure_prefixes : tuple
        Recognized prefixes for figure captions/references.
    table_prefixes : tuple
        Recognized prefixes for table captions/references.
    start_fig : int
        Starting number for figures.
    start_table : int
        Starting number for tables.
    """

    fig_caption_re = re.compile(
        r"^(?P<prefix>(?:" + "|".join(re.escape(p) for p in figure_prefixes) + r"))\s*(?P<num>\d+)(?P<rest>.*)",
        re.IGNORECASE,
    )
    tbl_caption_re = re.compile(
        r"^(?P<prefix>(?:" + "|".join(re.escape(p) for p in table_prefixes) + r"))\s*(?P<num>\d+)(?P<rest>.*)",
        re.IGNORECASE,
    )

    figure_map = {}
    table_map = {}

    def iter_paragraphs(section: Section):
        body = section.Body
        for i in range(body.ChildObjects.Count):
            para = body.ChildObjects.get_Item(i)
            if isinstance(para, Paragraph):
                yield para

    def replace_caption(para: Paragraph, prefix: str, old: str, new: str):
        try:
            para.Replace(f"{prefix} {old}", f"{prefix} {new}", False, False)
        except Exception:
            # Fallback: rebuild paragraph text if Replace isn't supported
            while para.ChildObjects.Count > 0:
                para.ChildObjects.RemoveAt(0)
            para.AppendText(f"{prefix} {new}")

    fig_idx = start_fig
    tbl_idx = start_table

    sections = [doc.Sections.get_Item(i) for i in range(doc.Sections.Count)]

    for s_idx, section in enumerate(sections, start=1):
        for para in iter_paragraphs(section):
            text = para.Text.strip()
            m = fig_caption_re.match(text)
            if m:
                old = m.group("num")
                figure_map[int(old)] = fig_idx
                replace_caption(para, m.group("prefix"), old, str(fig_idx) + m.group("rest"))
                fig_idx += 1
                continue
            m = tbl_caption_re.match(text)
            if m:
                old = m.group("num")
                table_map[int(old)] = tbl_idx
                replace_caption(para, m.group("prefix"), old, str(tbl_idx) + m.group("rest"))
                tbl_idx += 1
        if scope == "per-section":
            fig_idx = start_fig
            tbl_idx = start_table

    # Update references throughout the document
    for old, new in figure_map.items():
        for p in figure_prefixes:
            try:
                doc.Replace(f"{p} {old}", f"{p} {new}", False, False)
            except Exception:
                pass

    for old, new in table_map.items():
        for p in table_prefixes:
            try:
                doc.Replace(f"{p} {old}", f"{p} {new}", False, False)
            except Exception:
                pass

    # Try refreshing generated lists and fields
    try:
        doc.UpdateTableOfContents()
    except Exception:
        pass
    try:
        doc.UpdateTableOfFigures()
    except Exception:
        pass

    try:
        doc.IsUpdateFields = True
    except Exception:
        pass
