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
    numbering_scope: str = "global",
    figure_start: int = 1,
    table_start: int = 1,
) -> None:
    """Renumber figures and tables and update cross-references.

    Parameters
    ----------
    doc : Document
        The Spire.Doc document to operate on.
    numbering_scope : str, optional
        "global" for one continuous sequence across the document or
        "per-section" to reset numbering for each top-level section.
    figure_start : int, optional
        Starting index for figure numbering, by default 1.
    table_start : int, optional
        Starting index for table numbering, by default 1.
    """

    caption_regex = re.compile(r"^(Figure|Fig\.?|Table|Tab\.?)\s*(\d+)", re.IGNORECASE)
    ref_regex = re.compile(r"\b(Figure|Fig\.?|Table|Tab\.?)\s*(\d+)\b", re.IGNORECASE)

    figure_map = {}
    table_map = {}

    for sec_idx in range(doc.Sections.Count):
        section = doc.Sections.get_Item(sec_idx)
        fig_counter = figure_start
        tab_counter = table_start
        if numbering_scope.lower() == "global" and sec_idx > 0:
            fig_counter = figure_map.get("__next__", figure_start)
            tab_counter = table_map.get("__next__", table_start)

        for p_idx in range(section.Paragraphs.Count):
            para = section.Paragraphs.get_Item(p_idx)

            # Build paragraph text for caption detection
            para_text = "".join(
                para.ChildObjects.get_Item(i).Text
                for i in range(para.ChildObjects.Count)
                if isinstance(para.ChildObjects.get_Item(i), TextRange)
            )

            m = caption_regex.match(para_text.strip())
            if m:
                prefix, old_num = m.group(1), m.group(2)
                if prefix.lower().startswith("f"):
                    new_num = f"{sec_idx + 1}-{fig_counter}" if numbering_scope.lower() == "per-section" else str(fig_counter)
                    figure_map[old_num] = new_num
                    fig_counter += 1
                else:
                    new_num = f"{sec_idx + 1}-{tab_counter}" if numbering_scope.lower() == "per-section" else str(tab_counter)
                    table_map[old_num] = new_num
                    tab_counter += 1

            def repl(match: re.Match) -> str:
                prefix, old = match.group(1), match.group(2)
                lower = prefix.lower()
                if lower.startswith("f"):
                    new = figure_map.get(old)
                    if new:
                        return f"{prefix} {new}"
                else:
                    new = table_map.get(old)
                    if new:
                        return f"{prefix} {new}"
                return match.group(0)

            # Replace text in each run
            for r_idx in range(para.ChildObjects.Count):
                child = para.ChildObjects.get_Item(r_idx)
                if isinstance(child, TextRange):
                    new_text = ref_regex.sub(repl, child.Text)
                    if new_text != child.Text:
                        child.Text = new_text

        if numbering_scope.lower() == "global":
            figure_map["__next__"] = fig_counter
            table_map["__next__"] = tab_counter

    # Update any generated tables/lists if available
    try:
        doc.UpdateTableOfContents()
    except Exception:
        pass
    try:
        doc.UpdateTableOfFigures()
    except Exception:
        pass
    try:
        doc.UpdateTableOfTables()
    except Exception:
        pass
