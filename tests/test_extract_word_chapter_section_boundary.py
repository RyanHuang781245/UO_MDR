from lxml import etree

from modules.extract_word_chapter import (
    _is_plain_text_number_boundary,
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
    p_pr = etree.SubElement(p, qn("w:pPr"))
    p_style = etree.SubElement(p_pr, qn("w:pStyle"))
    p_style.set(qn("w:val"), style_id)
    return p


def _table_with_cell_texts(*texts: str) -> etree._Element:
    tbl = etree.Element(qn("w:tbl"))
    tr = etree.SubElement(tbl, qn("w:tr"))
    for text in texts:
        tc = etree.SubElement(tr, qn("w:tc"))
        tc.append(_paragraph(text))
    return tbl


def test_plain_text_number_boundary_requires_matching_prefix() -> None:
    assert _is_plain_text_number_boundary([6, 1], [6, 2]) is True
    assert _is_plain_text_number_boundary([6, 13], [7]) is True
    assert _is_plain_text_number_boundary([6, 1], [13, 565]) is False


def test_section_range_ignores_table_numeric_values_for_plain_text_boundary() -> None:
    body_children = [
        _paragraph("Biocompatibility"),
        _paragraph("body A"),
        _table_with_cell_texts("13.565", "19.49"),
        _paragraph("Reference and documents"),
        _paragraph("body B"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Biocompatibility",
        start_number="6.1",
        style_outline={},
        style_based={},
    )

    assert (start_idx, end_idx) == (0, len(body_children))


def test_section_range_prefers_structured_heading_over_plain_duplicate_title() -> None:
    body_children = [
        _paragraph("Device Description"),
        _heading("Device Description"),
        _paragraph("body A"),
        _heading("Next Chapter"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Device Description",
        start_number="1.1",
        style_outline={"Heading1": 0},
        style_based={},
    )

    assert (start_idx, end_idx) == (1, 3)


def test_section_range_uses_style_heading_rank_when_outline_levels_collapse() -> None:
    body_children = [
        _heading("User Information", style_id="S1"),
        _heading("Device or Product labelling", style_id="S2"),
        _paragraph("body A"),
        _heading("Sterile packaging labelling", style_id="S2"),
        _paragraph("body B"),
        _heading("References and documents", style_id="S1"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="User Information",
        start_number="2.1",
        style_outline={"S1": 0, "S2": 0},
        style_based={},
        style_heading_rank={"S1": 1, "S2": 2},
    )

    assert (start_idx, end_idx) == (0, 5)
