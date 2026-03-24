from lxml import etree

from modules.extract_word_chapter import (
    _ensure_numbering_instance,
    _is_plain_text_number_boundary,
    _set_paragraph_numpr,
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


def _styled_paragraph(text: str, style_id: str, ilvl: int | None = None) -> etree._Element:
    p = _heading(text, style_id=style_id)
    if ilvl is not None:
        p_pr = p.find(qn("w:pPr"))
        num_pr = etree.SubElement(p_pr, qn("w:numPr"))
        ilvl_node = etree.SubElement(num_pr, qn("w:ilvl"))
        ilvl_node.set(qn("w:val"), str(ilvl))
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


def test_section_range_ignores_body_paragraph_reusing_heading_style_rank() -> None:
    body_children = [
        _heading("Implant card information", style_id="S2"),
        _styled_paragraph(
            "The manufacturer of an implantable device shall provide the implant card together with the device.",
            style_id="S2",
            ilvl=0,
        ),
        _paragraph("Implant card drawing"),
        _heading("Electronic IFU", style_id="S2"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Implant card information",
        start_number="2.1.9",
        style_outline={"S2": 0},
        style_based={},
        style_heading_rank={"S2": 2},
    )

    assert (start_idx, end_idx) == (0, 3)


def test_section_range_stops_at_next_numbered_heading_with_same_style_and_ilvl() -> None:
    body_children = [
        _paragraph("Cleaning and Sterilization"),
        _styled_paragraph("Cleaning validation", style_id="S111", ilvl=2),
        _paragraph("body A"),
        _styled_paragraph("Sterilizing agent", style_id="S111", ilvl=2),
        _paragraph("body B"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Cleaning validation",
        start_number="6.13.1",
        style_outline={"S111": 0},
        style_based={},
        style_heading_rank={"S111": 2},
    )

    assert (start_idx, end_idx) == (1, 3)


def test_explicit_end_range_stops_before_following_numbered_heading() -> None:
    body_children = [
        _paragraph("Cleaning and Sterilization"),
        _styled_paragraph("Cleaning validation", style_id="S111", ilvl=2),
        _paragraph("body A"),
        _styled_paragraph("Sterilizing agent", style_id="S111", ilvl=2),
        _paragraph("body B"),
        _styled_paragraph("Gamma radiation sterilization validation", style_id="S111", ilvl=2),
        _paragraph("body C"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Cleaning validation",
        start_number="6.13.1",
        style_outline={"S111": 0},
        style_based={},
        style_heading_rank={"S111": 2},
        explicit_end_title="Sterilizing agent",
        explicit_end_number="6.13.2",
    )

    assert (start_idx, end_idx) == (1, 5)


def test_explicit_end_ignores_body_text_ending_with_title_until_structured_heading() -> None:
    body_children = [
        _paragraph("Cleaning and Sterilization"),
        _styled_paragraph("Cleaning validation", style_id="S111", ilvl=2),
        _paragraph("This section describes the sterilizing agent"),
        _paragraph("body A"),
        _styled_paragraph("Sterilizing agent", style_id="S111", ilvl=2),
        _paragraph("body B"),
        _styled_paragraph("Gamma radiation sterilization validation", style_id="S111", ilvl=2),
        _paragraph("body C"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Cleaning validation",
        start_number="6.13.1",
        style_outline={"S111": 0},
        style_based={},
        style_heading_rank={"S111": 2},
        explicit_end_title="Sterilizing agent",
        explicit_end_number="6.13.2",
    )

    assert (start_idx, end_idx) == (1, 6)


def test_section_range_ignores_sentence_like_same_style_and_ilvl_body_paragraph() -> None:
    body_children = [
        _paragraph("Cleaning and Sterilization"),
        _styled_paragraph("Cleaning validation", style_id="S111", ilvl=2),
        _styled_paragraph(
            "The validated process shall be performed before terminal sterilization.",
            style_id="S111",
            ilvl=2,
        ),
        _paragraph("body A"),
        _styled_paragraph("Sterilizing agent", style_id="S111", ilvl=2),
        _paragraph("body B"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Cleaning validation",
        start_number="6.13.1",
        style_outline={"S111": 0},
        style_based={},
        style_heading_rank={"S111": 2},
    )

    assert (start_idx, end_idx) == (1, 4)


def test_section_range_falls_back_to_rendered_number_boundary_when_start_heading_is_plain_text() -> None:
    body_children = [
        _heading("Seed numbered heading", style_id="S111"),
        _styled_paragraph("6.13.2 Sterilizing agent", style_id="S111", ilvl=0),
        _paragraph("body A"),
        _heading("Gamma radiation sterilization validation", style_id="S111"),
        _paragraph("body B"),
    ]
    file_map: dict[str, bytes] = {}
    num_id = _ensure_numbering_instance(file_map, [6, 13, 2])
    _set_paragraph_numpr(body_children[0], num_id=num_id, ilvl=2)
    _set_paragraph_numpr(body_children[3], num_id=num_id, ilvl=2)

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Sterilizing agent",
        start_number="6.13.2",
        style_outline={"S111": 0},
        style_based={},
        style_heading_rank={"S111": 2},
        numbering_xml=file_map["word/numbering.xml"],
    )

    assert (start_idx, end_idx) == (1, 3)


def test_section_range_uses_llm_boundary_candidate_when_rules_run_out() -> None:
    body_children = [
        _styled_paragraph("Cleaning validation", style_id="S111", ilvl=2),
        _paragraph("body A"),
        _paragraph("Sterilizing agent"),
        _paragraph("body B"),
    ]

    def resolver(**kwargs) -> int | None:
        candidates = kwargs["candidates"]
        assert [item["block_index"] for item in candidates] == [2]
        assert candidates[0]["text"] == "Sterilizing agent"
        assert "Sterilizing agent" in candidates[0]["xml_excerpt"]
        return 2

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Cleaning validation",
        start_number="6.13.1",
        style_outline={"S111": 0},
        style_based={},
        style_heading_rank={"S111": 2},
        llm_boundary_fallback=True,
        llm_boundary_resolver=resolver,
    )

    assert (start_idx, end_idx) == (0, 2)


def test_section_range_rejects_llm_candidate_outside_candidate_set() -> None:
    body_children = [
        _styled_paragraph("Cleaning validation", style_id="S111", ilvl=2),
        _paragraph("body A"),
        _paragraph("Sterilizing agent"),
        _paragraph("body B"),
    ]

    start_idx, end_idx = find_section_range_children(
        body_children=body_children,
        start_heading_text="Cleaning validation",
        start_number="6.13.1",
        style_outline={"S111": 0},
        style_based={},
        style_heading_rank={"S111": 2},
        llm_boundary_fallback=True,
        llm_boundary_resolver=lambda **kwargs: 1,
    )

    assert (start_idx, end_idx) == (0, len(body_children))
