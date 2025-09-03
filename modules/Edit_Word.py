from spire.doc import *
from spire.doc.common import *
import re
from collections import deque

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

    prefix_pattern = r"(Figure|Fig\.?|圖|图|Table|Tab\.?|表)"
    number_pattern = r"\d+(?:-\d+)*"
    # Allow common English and Chinese prefixes and digits with optional hyphen (e.g. 1-1)
    caption_regex = re.compile(
        rf"^{prefix_pattern}([\s\u00A0]*)({number_pattern})",
        re.IGNORECASE,
    )
    # Only guard against preceding ASCII letters so Chinese text can still match
    ref_regex = re.compile(
        rf"(?<![A-Za-z]){prefix_pattern}([\s\u00A0]*)({number_pattern})",
        re.IGNORECASE,
    )

    figure_map: dict[str, deque] = {}
    table_map: dict[str, deque] = {}

    # -------- Pass 1: build mapping of old numbers -> deque of new numbers --------
    fig_counter = figure_start
    tab_counter = table_start
    for sec_idx in range(doc.Sections.Count):
        section = doc.Sections.get_Item(sec_idx)
        if numbering_scope.lower() == "per-section" and sec_idx > 0:
            fig_counter = figure_start
            tab_counter = table_start

        for p_idx in range(section.Paragraphs.Count):
            para = section.Paragraphs.get_Item(p_idx)
            para_text = "".join(
                para.ChildObjects.get_Item(i).Text
                for i in range(para.ChildObjects.Count)
                if isinstance(para.ChildObjects.get_Item(i), TextRange)
            )
            text_stripped = para_text.strip()
            m = caption_regex.match(text_stripped)
            if not m:
                continue
            style_name = (para.StyleName or "").lower().replace(" ", "")
            if (
                "tableoffigures" in style_name
                or "tableoftables" in style_name
                or "tableofcontents" in style_name
            ):
                # Skip list/table-of entries and generated tables of contents
                continue
            prefix, sep, old_num = m.group(1), m.group(2), m.group(3)
            if prefix.lower().startswith("f"):
                new_num = (
                    f"{sec_idx + 1}-{fig_counter}" if numbering_scope.lower() == "per-section" else str(fig_counter)
                )
                figure_map.setdefault(old_num, deque()).append(new_num)
                fig_counter += 1
            else:
                new_num = (
                    f"{sec_idx + 1}-{tab_counter}" if numbering_scope.lower() == "per-section" else str(tab_counter)
                )
                table_map.setdefault(old_num, deque()).append(new_num)
                tab_counter += 1

    used_fig: dict[str, list[str]] = {k: [] for k in figure_map}
    used_tab: dict[str, list[str]] = {k: [] for k in table_map}

    def _caption_repl(match: re.Match) -> str:
        prefix, sep, old = match.group(1), match.group(2), match.group(3)
        lower = prefix.lower()
        if lower.startswith("f"):
            dq = figure_map.get(old)
            if dq:
                new = dq.popleft()
                used_fig.setdefault(old, []).append(new)
                return f"{prefix}{sep}{new}"
        else:
            dq = table_map.get(old)
            if dq:
                new = dq.popleft()
                used_tab.setdefault(old, []).append(new)
                return f"{prefix}{sep}{new}"
        return match.group(0)

    def _ref_repl(match: re.Match) -> str:
        prefix, sep, old = match.group(1), match.group(2), match.group(3)
        lower = prefix.lower()
        if lower.startswith("f"):
            if used_fig.get(old):
                new = used_fig[old][-1]
                return f"{prefix}{sep}{new}"
            dq = figure_map.get(old)
            if dq:
                return f"{prefix}{sep}{dq[0]}"
        else:
            if used_tab.get(old):
                new = used_tab[old][-1]
                return f"{prefix}{sep}{new}"
            dq = table_map.get(old)
            if dq:
                return f"{prefix}{sep}{dq[0]}"
        return match.group(0)

    # -------- Pass 2: replace captions and in-text references --------
    for sec_idx in range(doc.Sections.Count):
        section = doc.Sections.get_Item(sec_idx)
        for p_idx in range(section.Paragraphs.Count):
            para = section.Paragraphs.get_Item(p_idx)
            para_text = "".join(
                para.ChildObjects.get_Item(i).Text
                for i in range(para.ChildObjects.Count)
                if isinstance(para.ChildObjects.get_Item(i), TextRange)
            )
            text_stripped = para_text.strip()
            m_caption = caption_regex.match(text_stripped)
            style_name = (para.StyleName or "").lower().replace(" ", "")
            is_caption = bool(m_caption) and not (
                "tableoffigures" in style_name
                or "tableoftables" in style_name
                or "tableofcontents" in style_name
            )
            for r_idx in range(para.ChildObjects.Count):
                child = para.ChildObjects.get_Item(r_idx)
                if isinstance(child, TextRange):
                    if is_caption:
                        new_text = ref_regex.sub(_caption_repl, child.Text)
                    else:
                        new_text = ref_regex.sub(_ref_repl, child.Text)
                    if new_text != child.Text:
                        child.Text = new_text

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


def renumber_figures_tables_file(
    docx_path: str,
    *,
    numbering_scope: str = "global",
    figure_start: int = 1,
    table_start: int = 1,
) -> None:
    """Load a DOCX file, renumber figures/tables, and save it back.

    Parameters
    ----------
    docx_path : str
        Path to the DOCX file to update in place.
    numbering_scope : str, optional
        "global" for one continuous sequence across the document or
        "per-section" to reset numbering for each top-level section.
    figure_start : int, optional
        Starting index for figure numbering, by default 1.
    table_start : int, optional
        Starting index for table numbering, by default 1.
    """

    doc = Document()
    doc.LoadFromFile(docx_path)
    renumber_figures_tables(
        doc,
        numbering_scope=numbering_scope,
        figure_start=figure_start,
        table_start=table_start,
    )
    doc.SaveToFile(docx_path, FileFormat.Docx)
    doc.Close()
