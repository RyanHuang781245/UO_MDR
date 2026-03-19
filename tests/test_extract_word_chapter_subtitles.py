from lxml import etree

from modules.extract_word_chapter import qn, trim_to_subheading_range


def _paragraph(text: str, *, style_id: str | None = None, bold: bool = False) -> etree._Element:
    p = etree.Element(qn("w:p"))
    if style_id is not None:
        p_pr = etree.SubElement(p, qn("w:pPr"))
        p_style = etree.SubElement(p_pr, qn("w:pStyle"))
        p_style.set(qn("w:val"), style_id)

    r = etree.SubElement(p, qn("w:r"))
    if bold:
        r_pr = etree.SubElement(r, qn("w:rPr"))
        etree.SubElement(r_pr, qn("w:b"))
    t = etree.SubElement(r, qn("w:t"))
    t.text = text
    return p


def test_trim_to_subheading_range_stops_at_next_inline_subtitle() -> None:
    section_children = [
        _paragraph("Device trade name", bold=True),
        _paragraph("USTAR II Knee System"),
        _paragraph("Principles of operation and mode of action", bold=True),
        _paragraph("Detailed description"),
    ]

    trimmed = trim_to_subheading_range(section_children, "Device trade name")

    assert len(trimmed) == 2
    assert trimmed[0] is section_children[0]
    assert trimmed[1] is section_children[1]


def test_trim_to_subheading_range_ignores_styled_deep_headings_for_inline_start() -> None:
    section_children = [
        _paragraph("General description", bold=True),
        _paragraph("Body paragraph"),
        _paragraph("Femoral Component, Hinged", style_id="a9", bold=True),
        _paragraph("Component body"),
        _paragraph("Device trade name", bold=True),
        _paragraph("Target body"),
    ]

    trimmed = trim_to_subheading_range(section_children, "General description")

    assert len(trimmed) == 4
    assert trimmed[0] is section_children[0]
    assert trimmed[-1] is section_children[3]


def test_trim_to_subheading_range_accepts_styled_bold_subtitles_and_skips_captions() -> None:
    section_children = [
        _paragraph("Information on product label", style_id="MDR", bold=True),
        _paragraph("Body paragraph"),
        _paragraph("Figure 2. Packaging overview", style_id="MDR", bold=True),
        _paragraph("Caption explanation"),
        _paragraph("Direct marking on product", style_id="MDR", bold=True),
        _paragraph("Direct marking body"),
    ]

    trimmed = trim_to_subheading_range(section_children, "Information on product label")

    assert len(trimmed) == 4
    assert trimmed[0] is section_children[0]
    assert trimmed[-1] is section_children[3]
