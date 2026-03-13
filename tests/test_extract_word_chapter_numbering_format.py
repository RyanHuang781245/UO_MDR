from lxml import etree

from modules.extract_word_chapter import NS, _ensure_numbering_instance, _set_paragraph_numpr, qn


def _read_lvl_format(numbering_xml: bytes, num_id: int, ilvl: int) -> tuple[str | None, str | None]:
    root = etree.fromstring(numbering_xml)
    num_node = root.xpath(f".//w:num[@w:numId='{num_id}']", namespaces=NS)[0]
    abs_id = num_node.find("w:abstractNumId", namespaces=NS).get(qn("w:val"))
    lvl_node = root.xpath(
        f".//w:abstractNum[@w:abstractNumId='{abs_id}']/w:lvl[@w:ilvl='{ilvl}']",
        namespaces=NS,
    )[0]
    lvl_text = lvl_node.find("w:lvlText", namespaces=NS)
    suff = lvl_node.find("w:suff", namespaces=NS)
    return (
        lvl_text.get(qn("w:val")) if lvl_text is not None else None,
        suff.get(qn("w:val")) if suff is not None else None,
    )


def test_single_level_numbering_has_trailing_dot() -> None:
    file_map: dict[str, bytes] = {}
    num_id = _ensure_numbering_instance(file_map, [3])
    lvl_text, suff = _read_lvl_format(file_map["word/numbering.xml"], num_id, 0)
    assert lvl_text == "%1."
    assert suff == "space"


def test_multi_level_numbering_keeps_compound_style() -> None:
    file_map: dict[str, bytes] = {}
    num_id = _ensure_numbering_instance(file_map, [1, 2])
    lvl_text, _suff = _read_lvl_format(file_map["word/numbering.xml"], num_id, 1)
    assert lvl_text == "%1.%2"


def test_set_numpr_zeroes_indent_for_top_level() -> None:
    p = etree.Element(qn("w:p"))
    p_pr = etree.SubElement(p, qn("w:pPr"))
    ind = etree.SubElement(p_pr, qn("w:ind"))
    ind.set(qn("w:left"), "480")
    ind.set(qn("w:leftChars"), "200")

    _set_paragraph_numpr(p, num_id=9, ilvl=0)

    ind_after = p.find("w:pPr/w:ind", namespaces=NS)
    assert ind_after is not None
    assert ind_after.get(qn("w:left")) == "0"
    assert ind_after.get(qn("w:hanging")) == "0"
    assert ind_after.get(qn("w:leftChars")) == "0"
    assert ind_after.get(qn("w:hangingChars")) == "0"
    assert ind_after.get(qn("w:firstLine")) == "0"
    assert ind_after.get(qn("w:firstLineChars")) == "0"
