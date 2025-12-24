import zipfile
import re
from lxml import etree

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"w": W_NS}
NS_REL = {"r": PKG_REL_NS}

def qn(tag: str) -> str:
    p, l = tag.split(":")
    if p != "w":
        raise ValueError("qn only supports w:")
    return f"{{{W_NS}}}{l}"

def normalize(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())

def get_all_text(el) -> str:
    return "".join(el.xpath(".//w:t/text()", namespaces=NS))

def get_pStyle(p) -> str | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None:
        return None
    ps = pPr.find("w:pStyle", namespaces=NS)
    return ps.get(qn("w:val")) if ps is not None else None

def iter_paragraphs(block):
    if block.tag == qn("w:p"):
        yield block
    for p in block.xpath(".//w:p", namespaces=NS):
        yield p

# ---------------- TOC 判斷（多特徵） ----------------
def is_toc_paragraph(p) -> bool:
    style = (get_pStyle(p) or "").upper()
    if style.startswith("TOC"):
        return True

    instr = "".join(p.xpath(".//w:instrText/text()", namespaces=NS)).upper()
    if "TOC" in instr:
        return True

    anchors = p.xpath(".//w:hyperlink/@w:anchor", namespaces=NS)
    if any((a or "").startswith("_Toc") for a in anchors):
        return True

    leaders = p.xpath(".//w:tab/@w:leader", namespaces=NS)
    if any(l in ("dot", "middleDot") for l in leaders):
        return True

    return False

# ---------------- styles.xml：styleId -> outlineLvl（含 basedOn） ----------------
def build_style_maps(styles_xml: bytes) -> tuple[dict[str, int], dict[str, str]]:
    styles_root = etree.fromstring(styles_xml)
    style_outline: dict[str, int] = {}
    style_based: dict[str, str] = {}

    for st in styles_root.xpath(".//w:style[@w:type='paragraph']", namespaces=NS):
        sid = st.get(qn("w:styleId"))
        if not sid:
            continue

        ol = st.find(".//w:pPr/w:outlineLvl", namespaces=NS)
        if ol is not None:
            v = ol.get(qn("w:val"))
            if v and v.isdigit():
                style_outline[sid] = int(v)

        based = st.find("w:basedOn", namespaces=NS)
        if based is not None:
            base_id = based.get(qn("w:val"))
            if base_id:
                style_based[sid] = base_id

    return style_outline, style_based

def resolve_style_outline(style_id: str | None, style_outline: dict[str, int], style_based: dict[str, str]) -> int | None:
    if not style_id:
        return None
    cur = style_id
    for _ in range(30):
        if cur in style_outline:
            return style_outline[cur]
        cur = style_based.get(cur)
        if not cur:
            break
    return None

def get_effective_outline_level(p, style_outline: dict[str, int], style_based: dict[str, str]) -> int | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is not None:
        ol = pPr.find("w:outlineLvl", namespaces=NS)
        if ol is not None:
            v = ol.get(qn("w:val"))
            if v and v.isdigit():
                return int(v)

    return resolve_style_outline(get_pStyle(p), style_outline, style_based)

def is_heading_paragraph(p, style_outline: dict[str, int], style_based: dict[str, str]) -> bool:
    return get_effective_outline_level(p, style_outline, style_based) is not None

# ---------------- 忽略頁首/頁尾：移除所有 sectPr 的 header/footer references ----------------
def remove_all_header_footer_refs(doc_root):
    for sectPr in doc_root.xpath(".//w:sectPr", namespaces=NS):
        for tag in ("w:headerReference", "w:footerReference"):
            for n in list(sectPr.findall(tag, namespaces=NS)):
                sectPr.remove(n)

# ---------------- 主流程：忽略 TOC + 忽略 TOC 前內容 + 忽略頁首/頁尾 ----------------
def extract_body_with_options(
    input_docx: str,
    output_docx: str,
    ignore_toc_and_before: bool = True,
    ignore_header_footer: bool = True,
):
    import zipfile
    from lxml import etree

    with zipfile.ZipFile(input_docx, "r") as zin:
        file_map = {name: zin.read(name) for name in zin.namelist()}

    if "word/document.xml" not in file_map:
        raise RuntimeError("找不到 word/document.xml")
    if "word/styles.xml" not in file_map:
        raise RuntimeError("找不到 word/styles.xml")

    # ---------- 解析 styles.xml ----------
    style_outline, style_based = build_style_maps(file_map["word/styles.xml"])

    doc_root = etree.fromstring(file_map["word/document.xml"])
    body = doc_root.find("w:body", namespaces=NS)
    children = list(body)

    # ---------- 是否忽略 TOC 與之前內容 ----------
    if ignore_toc_and_before:
        last_toc_child_idx = None
        for i, blk in enumerate(children):
            for p in iter_paragraphs(blk):
                if is_toc_paragraph(p):
                    last_toc_child_idx = i
                    break

        search_start = (last_toc_child_idx + 1) if last_toc_child_idx is not None else 0

        start_idx = None
        for i in range(search_start, len(children)):
            blk = children[i]
            for p in iter_paragraphs(blk):
                if is_toc_paragraph(p):
                    continue
                if is_heading_paragraph(p, style_outline, style_based):
                    start_idx = i
                    break
            if start_idx is not None:
                break

        if start_idx is None:
            raise RuntimeError("找不到正文章節起點（非 TOC 標題）")

        kept_children = children[start_idx:]
    else:
        # 不忽略：整份文件本文全保留
        kept_children = children

    # ---------- 重建 body ----------
    for ch in list(body):
        body.remove(ch)
    for ch in kept_children:
        body.append(ch)

    # ---------- 是否忽略頁首 / 頁尾 ----------
    if ignore_header_footer:
        remove_all_header_footer_refs(doc_root)

    # ---------- 回寫 document.xml ----------
    file_map["word/document.xml"] = etree.tostring(
        doc_root,
        xml_declaration=True,
        encoding="UTF-8",
        standalone="yes",
    )

    # ---------- 輸出 DOCX ----------
    with zipfile.ZipFile(output_docx, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, data in file_map.items():
            zout.writestr(name, data)


if __name__ == "__main__":
    extract_body_with_options(
        input_docx=r"C:\Users\ne025\Desktop\Section 1_Device Description_v1_hip.docx",
        output_docx="output_body_only_no_toc_no_header_footer.docx",
        ignore_toc_and_before=False,
        ignore_header_footer=True,
    )
    print("Done")
