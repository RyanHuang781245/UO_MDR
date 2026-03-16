from lxml import etree

from modules.extract_word_chapter import (
    NS,
    get_all_text,
    materialize_paragraph_numpr_as_text,
    normalize_paragraph_to_plain_text_run,
    qn,
)


def _build_numbering_xml(lvl_text: str) -> bytes:
    root = etree.Element(qn("w:numbering"), nsmap={"w": NS["w"]})
    abstract = etree.SubElement(root, qn("w:abstractNum"))
    abstract.set(qn("w:abstractNumId"), "1")
    lvl = etree.SubElement(abstract, qn("w:lvl"))
    lvl.set(qn("w:ilvl"), "0")
    start = etree.SubElement(lvl, qn("w:start"))
    start.set(qn("w:val"), "1")
    num_fmt = etree.SubElement(lvl, qn("w:numFmt"))
    num_fmt.set(qn("w:val"), "decimal")
    lvl_text_node = etree.SubElement(lvl, qn("w:lvlText"))
    lvl_text_node.set(qn("w:val"), lvl_text)

    num = etree.SubElement(root, qn("w:num"))
    num.set(qn("w:numId"), "7")
    abstract_ref = etree.SubElement(num, qn("w:abstractNumId"))
    abstract_ref.set(qn("w:val"), "1")
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone="yes")


def _build_numbered_paragraph(text: str) -> etree._Element:
    para = etree.Element(qn("w:p"))
    p_pr = etree.SubElement(para, qn("w:pPr"))
    num_pr = etree.SubElement(p_pr, qn("w:numPr"))
    ilvl = etree.SubElement(num_pr, qn("w:ilvl"))
    ilvl.set(qn("w:val"), "0")
    num_id = etree.SubElement(num_pr, qn("w:numId"))
    num_id.set(qn("w:val"), "7")
    run = etree.SubElement(para, qn("w:r"))
    text_node = etree.SubElement(run, qn("w:t"))
    text_node.text = text
    return para


def test_materialize_caption_numpr_as_plain_text_keeps_table_number() -> None:
    first = _build_numbered_paragraph("Device overview")
    second = _build_numbered_paragraph("Specification summary")
    numbering_xml = _build_numbering_xml("Table %1")

    materialize_paragraph_numpr_as_text(second, [first, second], numbering_xml)

    assert second.find("w:pPr/w:numPr", namespaces=NS) is None
    assert get_all_text(second) == "Table 2 Specification summary"


def test_materialize_caption_numpr_as_plain_text_keeps_figure_number() -> None:
    first = _build_numbered_paragraph("System architecture")
    second = _build_numbered_paragraph("Packaging overview")
    numbering_xml = _build_numbering_xml("Figure %1.")

    materialize_paragraph_numpr_as_text(second, [first, second], numbering_xml)

    assert second.find("w:pPr/w:numPr", namespaces=NS) is None
    assert get_all_text(second) == "Figure 2. Packaging overview"


def test_normalize_paragraph_to_plain_text_run_merges_multiple_runs() -> None:
    para = etree.Element(qn("w:p"))
    run1 = etree.SubElement(para, qn("w:r"))
    text1 = etree.SubElement(run1, qn("w:t"))
    text1.text = "Figure 2."
    run2 = etree.SubElement(para, qn("w:r"))
    text2 = etree.SubElement(run2, qn("w:t"))
    text2.text = " Packaging overview"

    normalize_paragraph_to_plain_text_run(para)

    runs = para.findall("w:r", namespaces=NS)
    assert len(runs) == 1
    assert get_all_text(para) == "Figure 2. Packaging overview"


def test_normalize_paragraph_prefers_following_run_format_for_number_prefix() -> None:
    para = etree.Element(qn("w:p"))

    prefix_run = etree.SubElement(para, qn("w:r"))
    prefix_text = etree.SubElement(prefix_run, qn("w:t"))
    prefix_text.text = "Figure 2. "

    text_run = etree.SubElement(para, qn("w:r"))
    text_rpr = etree.SubElement(text_run, qn("w:rPr"))
    fonts = etree.SubElement(text_rpr, qn("w:rFonts"))
    fonts.set(qn("w:ascii"), "Calibri")
    fonts.set(qn("w:hAnsi"), "Calibri")
    body_text = etree.SubElement(text_run, qn("w:t"))
    body_text.text = "Packaging overview"

    normalize_paragraph_to_plain_text_run(para, prefer_following_text_run=True)

    runs = para.findall("w:r", namespaces=NS)
    assert len(runs) == 1
    merged_fonts = runs[0].find("w:rPr/w:rFonts", namespaces=NS)
    assert merged_fonts is not None
    assert merged_fonts.get(qn("w:ascii")) == "Calibri"
    assert get_all_text(para) == "Figure 2. Packaging overview"
