from lxml import etree

from modules.extract_word_chapter import (
    _materialize_heading_numbering,
    find_section_range_children,
    qn,
)


def _paragraph(text: str) -> etree._Element:
    p = etree.Element(qn("w:p"))
    r = etree.SubElement(p, qn("w:r"))
    t = etree.SubElement(r, qn("w:t"))
    t.text = text
    return p

def _heading(text: str, style_id: str = "Heading1") -> etree._Element:
    p = _paragraph(text)
    pPr = etree.SubElement(p, qn("w:pPr"))
    p_style = etree.SubElement(pPr, qn("w:pStyle"))
    p_style.set(qn("w:val"), style_id)
    return p


def _paragraph_ilvl(p: etree._Element) -> str | None:
    ilvl = p.find("w:pPr/w:numPr/w:ilvl", namespaces={"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"})
    if ilvl is None:
        return None
    return ilvl.get(qn("w:val"))


def test_plain_text_numbering_stops_at_next_same_level_heading() -> None:
    body_children = [
        _paragraph("1.1.1 Scope"),
        _paragraph("body A"),
        _paragraph("1.1.1.1 Details"),
        _paragraph("body B"),
        _paragraph("1.1.2 Device description"),
        _paragraph("body C"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Scope",
        start_number="1.1.1",
        style_outline={},
        style_based={},
    )

    assert (start_idx, end_idx) == (0, 4)


def test_plain_text_numbering_supports_fullwidth_dot() -> None:
    body_children = [
        _paragraph("1．1．1"),
        _paragraph("body A"),
        _paragraph("1．1．2 Device description"),
        _paragraph("body B"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="1.1.1",
        start_number="1.1.1",
        style_outline={},
        style_based={},
    )

    assert (start_idx, end_idx) == (0, 2)


def test_auto_numbered_heading_can_find_numeric_chapter_without_title() -> None:
    body_children = [
        _heading("Introduction"),
        _paragraph("body A"),
        _heading("Scope"),
        _paragraph("body B"),
        _heading("Results"),
        _paragraph("body C"),
        _heading("Appendix"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="3",
        start_number="3",
        style_outline={"Heading1": 0},
        style_based={},
    )

    assert (start_idx, end_idx) == (4, 6)


def test_materialize_numbering_fallback_sets_first_non_empty_paragraph() -> None:
    body_children = [
        _paragraph(""),
        _paragraph("Chapter Title Without Heading Style"),
        _paragraph("body"),
    ]

    _materialize_heading_numbering(
        section_children=body_children,
        start_number="1.1",
        style_outline={},
        style_based={},
        num_id=9,
    )

    assert _paragraph_ilvl(body_children[1]) == "1"


def test_materialize_numbering_accepts_numeric_style_id_as_heading_level() -> None:
    body_children = [
        _heading("Chapter Title", style_id="2"),
        _paragraph("body"),
    ]

    _materialize_heading_numbering(
        section_children=body_children,
        start_number="1.1",
        style_outline={},
        style_based={},
        num_id=9,
    )

    assert _paragraph_ilvl(body_children[0]) == "1"
