from lxml import etree

from modules.extract_word_chapter import (
    NS,
    _ensure_style_without_numpr,
    parse_styles_numpr,
    qn,
    resolve_style_numpr,
)


def _build_style_xml() -> bytes:
    root = etree.Element(qn("w:styles"), nsmap={"w": NS["w"]})

    def add_style(
        style_id: str,
        name: str,
        based_on: str | None = None,
        *,
        with_numpr: bool = False,
        num_id: int | None = None,
        ilvl: int | None = None,
    ) -> None:
        style = etree.SubElement(root, qn("w:style"))
        style.set(qn("w:type"), "paragraph")
        style.set(qn("w:styleId"), style_id)
        style_name = etree.SubElement(style, qn("w:name"))
        style_name.set(qn("w:val"), name)
        if based_on:
            based = etree.SubElement(style, qn("w:basedOn"))
            based.set(qn("w:val"), based_on)
        p_pr = etree.SubElement(style, qn("w:pPr"))
        if with_numpr:
            num_pr = etree.SubElement(p_pr, qn("w:numPr"))
            if ilvl is not None:
                ilvl_node = etree.SubElement(num_pr, qn("w:ilvl"))
                ilvl_node.set(qn("w:val"), str(ilvl))
            if num_id is not None:
                num_id_node = etree.SubElement(num_pr, qn("w:numId"))
                num_id_node.set(qn("w:val"), str(num_id))

    add_style("a", "Normal")
    add_style("a9", "List Paragraph", based_on="a")
    add_style("11", "1.1階層", based_on="a9", with_numpr=True, num_id=4, ilvl=1)
    add_style("111", "1.1.1階層", based_on="11", with_numpr=True, ilvl=2)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone="yes")


def test_ensure_style_without_numpr_clones_numbered_basedon_chain() -> None:
    updated_xml, new_style_id = _ensure_style_without_numpr(_build_style_xml(), "111")

    root = etree.fromstring(updated_xml)
    styles = {
        (st.get(qn("w:styleId")) or ""): st
        for st in root.xpath(".//w:style[@w:type='paragraph']", namespaces=NS)
    }

    assert new_style_id != "111"
    cloned_style = styles[new_style_id]
    cloned_base = cloned_style.find("w:basedOn", namespaces=NS)
    assert cloned_base is not None
    assert cloned_base.get(qn("w:val")) != "11"
    assert cloned_base.get(qn("w:val"), "").endswith("_NoNum")
    assert cloned_style.find("w:pPr/w:numPr", namespaces=NS) is None

    cloned_parent = styles[cloned_base.get(qn("w:val"))]
    parent_base = cloned_parent.find("w:basedOn", namespaces=NS)
    assert parent_base is not None
    assert parent_base.get(qn("w:val")) == "a9"
    assert cloned_parent.find("w:pPr/w:numPr", namespaces=NS) is None


def test_resolve_style_numpr_preserves_child_ilvl_when_numid_is_inherited() -> None:
    style_based, style_numpr = parse_styles_numpr(_build_style_xml())

    assert resolve_style_numpr("11", style_based, style_numpr) == (4, 1)
    assert resolve_style_numpr("111", style_based, style_numpr) == (4, 2)
