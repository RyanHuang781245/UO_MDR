import re
import shutil
import warnings
import zipfile
from collections.abc import Iterator
from copy import deepcopy
from lxml import etree

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": W_NS}
NUMBER_PREFIX_RE = re.compile(r"^\s*(\d+(?:\.\d+)*)")

def qn(tag: str) -> str:
    prefix, local = tag.split(":")
    if prefix != "w":
        raise ValueError("qn() only supports w: namespace")
    return f"{{{W_NS}}}{local}"

def normalize_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())

def get_all_text(node: etree._Element) -> str:
    return "".join(node.xpath(".//w:t/text()", namespaces=NS)).strip()

def parse_number_parts(text: str) -> list[int] | None:
    if not text:
        return None
    match = NUMBER_PREFIX_RE.match(text)
    if not match:
        return None
    number_text = match.group(1).rstrip(".")
    parts = number_text.split(".") if number_text else []
    if not parts or any(not p.isdigit() for p in parts):
        return None
    return [int(p) for p in parts]

def is_number_prefix_match(txt: str, start_number: str, start_heading_text: str) -> bool:
    if not start_number:
        return False
    return txt.startswith(start_number) and start_heading_text in txt

def get_pStyle(p: etree._Element) -> str | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None:
        return None
    ps = pPr.find("w:pStyle", namespaces=NS)
    return ps.get(qn("w:val")) if ps is not None else None

def get_ilvl(p: etree._Element) -> int | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None:
        return None
    numPr = pPr.find("w:numPr", namespaces=NS)
    if numPr is None:
        return None
    il = numPr.find("w:ilvl", namespaces=NS)
    if il is None:
        return None
    v = il.get(qn("w:val"))
    return int(v) if v and v.isdigit() else None

def iter_paragraphs(block: etree._Element) -> Iterator[etree._Element]:
    if block.tag == qn("w:p"):
        yield block
    for p in block.xpath(".//w:p", namespaces=NS):
        yield p

# ---------- TOC 偵測（多特徵） ----------
def is_toc_paragraph(p: etree._Element) -> bool:
    style = (get_pStyle(p) or "").upper()
    has_toc_style = style.startswith("TOC")
    if has_toc_style:
        return True

    instr = "".join(p.xpath(".//w:instrText/text()", namespaces=NS)).upper()
    has_instr_toc = "TOC" in instr
    if has_instr_toc:
        return True

    anchors = p.xpath(".//w:hyperlink/@w:anchor", namespaces=NS)
    has_toc_anchor = any((a or "").startswith("_Toc") for a in anchors)
    if has_toc_anchor:
        return True

    # Dot leaders alone are too noisy; keep them as auxiliary signals only.
    return False

# ---------- Header/Footer ----------
def remove_header_footer_references_in_sectPr(sectPr: etree._Element):
    for tag in ("w:headerReference", "w:footerReference"):
        for node in list(sectPr.findall(tag, namespaces=NS)):
            sectPr.remove(node)

def remove_all_header_footer_references(document_root: etree._Element):
    for sectPr in document_root.xpath(".//w:body/w:sectPr", namespaces=NS):
        remove_header_footer_references_in_sectPr(sectPr)
    for sectPr in document_root.xpath(".//w:pPr/w:sectPr", namespaces=NS):
        remove_header_footer_references_in_sectPr(sectPr)

# ---------- styles.xml outlineLvl（含 basedOn 繼承） ----------
def build_style_outline_map(styles_xml: bytes) -> tuple[dict[str, int], dict[str, str]]:
    style_outline: dict[str, int] = {}
    style_based: dict[str, str] = {}

    root = etree.fromstring(styles_xml)
    for st in root.xpath(".//w:style[@w:type='paragraph']", namespaces=NS):
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

def get_effective_outline_level(p: etree._Element, style_outline: dict[str, int], style_based: dict[str, str]) -> int | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is not None:
        ol = pPr.find("w:outlineLvl", namespaces=NS)
        if ol is not None:
            v = ol.get(qn("w:val"))
            if v and v.isdigit():
                return int(v)
    return resolve_style_outline(get_pStyle(p), style_outline, style_based)

# ---------- 表格排除：只在判斷小標題起/終點時，忽略表格內的段落 ----------
def is_inside_table(p: etree._Element) -> bool:
    return bool(p.xpath("ancestor::w:tbl", namespaces=NS))

def iter_paragraphs_no_table(block: etree._Element) -> Iterator[etree._Element]:
    """
    只 yield 不在表格內的段落，用於小標題起點/終點判斷，避免表格粗體誤觸發。
    """
    if block.tag == qn("w:p"):
        if not is_inside_table(block):
            yield block
        return

    if block.tag == qn("w:tbl"):
        return

    for p in block.xpath(".//w:p", namespaces=NS):
        if not is_inside_table(p):
            yield p

def get_bold_state_from_rPr(rPr: etree._Element | None) -> bool | None:
    if rPr is None:
        return None
    b = rPr.find("w:b", namespaces=NS)
    if b is None:
        return None
    val = b.get(qn("w:val"))
    if val is None:
        return True
    val = val.strip().lower()
    if val in ("0", "false", "off", "no"):
        return False
    return True

# ---------- 小標題（inline subtitle）判斷：Normal + 無顯式非粗體，且至少有粗體提示 ----------
def is_inline_subtitle_xml(p: etree._Element) -> bool:
    style = get_pStyle(p)
    if style not in (None, "Normal"):
        return False

    runs = p.findall(".//w:r", namespaces=NS)
    if not runs:
        return False

    has_text = False
    has_bold_hint = False
    for r in runs:
        texts = r.findall(".//w:t", namespaces=NS)
        if not texts:
            continue
        txt = "".join((t.text or "") for t in texts)
        if not normalize_text(txt):
            continue
        has_text = True

        rPr = r.find("w:rPr", namespaces=NS)
        bold_state = get_bold_state_from_rPr(rPr)
        if bold_state is False:
            return False
        if bold_state is True:
            has_bold_hint = True
        elif rPr is not None and rPr.find("w:rStyle", namespaces=NS) is not None:
            has_bold_hint = True

    return has_text and has_bold_hint

def match_subheading(p: etree._Element, subheading_text: str, strict: bool = True) -> bool:
    txt = normalize_text(get_all_text(p))
    target = normalize_text(subheading_text)
    if strict:
        return txt == target
    return target in txt

# ---------- 章節範圍定位（outlineLvl 優先，ilvl 備援），支援 ignore_toc ----------
def find_section_range_children(
    body_children: list[etree._Element],
    start_heading_text: str,
    start_number: str,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    explicit_end_title: str | None = None,
    ignore_toc: bool = True,
) -> tuple[int, int]:
    start_idx, start_outline, _ = find_start_index(
        body_children=body_children,
        start_heading_text=start_heading_text,
        start_number=start_number,
        style_outline=style_outline,
        style_based=style_based,
        ignore_toc=ignore_toc,
    )

    end_idx = find_end_index(
        body_children=body_children,
        start_idx=start_idx,
        start_outline=start_outline,
        start_heading_text=start_heading_text,
        start_number=start_number,
        style_outline=style_outline,
        style_based=style_based,
        explicit_end_title=explicit_end_title,
        ignore_toc=ignore_toc,
    )

    return start_idx, end_idx

def find_start_index(
    body_children: list[etree._Element],
    start_heading_text: str,
    start_number: str,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    ignore_toc: bool = True,
) -> tuple[int, int | None, str]:
    start_idx = None
    start_outline = None
    found_kind = None  # "exact" | "number_prefix_match"

    for i, block in enumerate(body_children):
        for p in iter_paragraphs(block):
            if ignore_toc and is_toc_paragraph(p):
                continue

            txt = normalize_text(get_all_text(p))

            if txt == start_heading_text:
                start_idx = i
                start_outline = get_effective_outline_level(p, style_outline, style_based)
                found_kind = "exact"
                break

            if is_number_prefix_match(txt, start_number, start_heading_text):
                if found_kind is None:
                    start_idx = i
                    start_outline = get_effective_outline_level(p, style_outline, style_based)
                    found_kind = "number_prefix_match"

        if found_kind == "exact":
            break

    if start_idx is None:
        raise RuntimeError(f"找不到章節起點：{start_number} / {start_heading_text}")

    return start_idx, start_outline, found_kind

def find_end_index(
    body_children: list[etree._Element],
    start_idx: int,
    start_outline: int | None,
    start_heading_text: str,
    start_number: str,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    explicit_end_title: str | None = None,
    ignore_toc: bool = True,
) -> int:
    end_idx = len(body_children)
    explicit_end_found = False
    auto_end_idx = None
    start_number_parts = parse_number_parts(start_number)
    for j in range(start_idx + 1, len(body_children)):
        block = body_children[j]
        for p in iter_paragraphs(block):
            if ignore_toc and is_toc_paragraph(p):
                continue

            txt = normalize_text(get_all_text(p))
            if not txt:
                continue

            if explicit_end_title:
                if explicit_end_title == txt or explicit_end_title in txt:
                    explicit_end_found = True
                    return j

            lvl = get_effective_outline_level(p, style_outline, style_based)
            if start_outline is not None and lvl is not None:
                boundary = False
                if lvl < start_outline:
                    boundary = True
                elif lvl == start_outline:
                    candidate_number_parts = parse_number_parts(txt)
                    if start_number_parts and candidate_number_parts:
                        boundary = candidate_number_parts != start_number_parts
                    elif start_heading_text not in txt:
                        boundary = True

                if boundary:
                    if explicit_end_title:
                        if auto_end_idx is None:
                            auto_end_idx = j
                    else:
                        return j

    if explicit_end_title and not explicit_end_found:
        if auto_end_idx is not None:
            warnings.warn(
                f"explicit_end_title not found; falling back to outline-based end at index {auto_end_idx}."
            )
            return auto_end_idx
        raise RuntimeError(
            f"explicit_end_title not found after start: {start_number} / {start_heading_text}"
        )

    return end_idx

def is_all_bold_paragraph(p: etree._Element) -> bool:
    runs = p.findall(".//w:r", namespaces=NS)
    if not runs:
        return False

    has_text = False
    for r in runs:
        texts = r.findall(".//w:t", namespaces=NS)
        if not texts:
            continue
        txt = "".join((t.text or "") for t in texts)
        if not normalize_text(txt):
            continue

        has_text = True
        rPr = r.find("w:rPr", namespaces=NS)
        if get_bold_state_from_rPr(rPr) is not True:
            return False

    return has_text

def has_body_text_after_candidate(
    section_children: list[etree._Element],
    candidate_block_index: int,
    lookahead_blocks: int = 6,
) -> bool:
    """
    從 candidate_block_index 後面開始，往後找最多 lookahead_blocks 個非空段落（忽略表格）：
    - 只要找到一個「不是全粗體」的段落（視為正文），就回 True
    - 都找不到就回 False
    """
    checked = 0
    for k in range(candidate_block_index + 1, len(section_children)):
        blk = section_children[k]
        for p in iter_paragraphs_no_table(blk):
            txt = normalize_text(get_all_text(p))
            if not txt:
                continue

            checked += 1
            if not is_all_bold_paragraph(p):
                return True

            if checked >= lookahead_blocks:
                return False

    return False


# ---------- 在章節範圍內擷取「小標題內容」（終點：下一個小標題；但不掃表格內段落） ----------
def trim_to_subheading_range(
    section_children: list[etree._Element],
    subheading_text: str,
    strict_match: bool = True,
    debug: bool = False,
) -> list[etree._Element]:
    sub_start = None

    # 1) 找小標題起點：優先「inline subtitle + 文字匹配」（且排除表格內段落）
    for i, block in enumerate(section_children):
        for p in iter_paragraphs_no_table(block):
            if is_inline_subtitle_xml(p) and match_subheading(p, subheading_text, strict=strict_match):
                sub_start = i
                break
        if sub_start is not None:
            break

    # 2) 若找不到，退回：只用文字匹配（仍排除表格內段落）
    if sub_start is None:
        for i, block in enumerate(section_children):
            for p in iter_paragraphs_no_table(block):
                if match_subheading(p, subheading_text, strict=strict_match):
                    sub_start = i
                    break
            if sub_start is not None:
                break

    if sub_start is None:
        raise RuntimeError(f"在章節範圍內找不到指定小標題：{subheading_text}")

    # 3) 找小標題終點：下一個 inline subtitle（不含）；但不把表格內粗體當終點
    sub_end = len(section_children)
    for j in range(sub_start + 1, len(section_children)):
        blk = section_children[j]
        for p in iter_paragraphs_no_table(blk):
            # 候選：inline subtitle 規則（Normal + 粗體提示）
            if is_inline_subtitle_xml(p):
                # 新增：確認它後面真的跟著正文（不是一路粗體）
                if has_body_text_after_candidate(section_children, j, lookahead_blocks=2):
                    sub_end = j
                    if debug:
                        print("小標題擷取結束於段落（確認為小標題）：", repr(get_all_text(p)))
                        print("-" * 60)
                    break
                else:
                    if debug:
                        print("略過候選小標題（後面無正文）：", repr(get_all_text(p)))
                        print("-" * 60)
        if sub_end != len(section_children):
            break

    return section_children[sub_start:sub_end]

def extract_body_children(root: etree._Element) -> tuple[list[etree._Element], etree._Element | None]:
    body = root.find("w:body", namespaces=NS)
    if body is None:
        raise RuntimeError("document.xml 找不到 w:body")

    children = list(body)
    if children and children[-1].tag == qn("w:sectPr"):
        return children[:-1], children[-1]
    return children, None

def build_new_document_xml(
    root: etree._Element,
    kept_section: list[etree._Element],
    sectPr: etree._Element | None,
    ignore_header_footer: bool,
) -> bytes:
    body = root.find("w:body", namespaces=NS)
    if body is None:
        raise RuntimeError("document.xml 找不到 w:body")

    for ch in list(body):
        body.remove(ch)

    for ch in kept_section:
        body.append(deepcopy(ch))

    if sectPr is not None:
        body.append(deepcopy(sectPr))

    if ignore_header_footer:
        remove_all_header_footer_references(root)

    return etree.tostring(
        root, xml_declaration=True, encoding="UTF-8", standalone="yes"
    )

def extract_section_docx_xml(
    input_docx: str,
    output_docx: str,
    start_heading_text: str,
    start_number: str,
    explicit_end_title: str | None = None,
    ignore_header_footer: bool = True,
    ignore_toc: bool = True,
    subheading_text: str | None = None,
    subheading_strict_match: bool = True,
    subheading_debug: bool = False,
):
    # 複製整包 docx（保留 styles/numbering/media/rels 等）
    shutil.copyfile(input_docx, output_docx)

    with zipfile.ZipFile(output_docx, "r") as zin:
        file_map = {name: zin.read(name) for name in zin.namelist()}

    if "word/document.xml" not in file_map:
        raise RuntimeError("DOCX 中找不到 word/document.xml")
    if "word/styles.xml" not in file_map:
        raise RuntimeError("DOCX 中找不到 word/styles.xml（需要用它回推 outlineLvl）")

    style_outline, style_based = build_style_outline_map(file_map["word/styles.xml"])

    root = etree.fromstring(file_map["word/document.xml"])
    content_children, sectPr = extract_body_children(root)

    # 1) 先擷取大章節範圍
    start_idx, end_idx = find_section_range_children(
        content_children,
        start_heading_text=start_heading_text,
        start_number=start_number,
        style_outline=style_outline,
        style_based=style_based,
        explicit_end_title=explicit_end_title,
        ignore_toc=ignore_toc,
    )
    kept_section = content_children[start_idx:end_idx]

    # 2) 若指定 subheading_text，再在章節範圍內裁切成小標題內容
    if subheading_text:
        kept_section = trim_to_subheading_range(
            kept_section,
            subheading_text=subheading_text,
            strict_match=subheading_strict_match,
            debug=subheading_debug,
        )

    # 3) 重建 document.xml
    new_doc_xml = build_new_document_xml(
        root=root,
        kept_section=kept_section,
        sectPr=sectPr,
        ignore_header_footer=ignore_header_footer,
    )

    # 5) 重建 zip，只覆蓋 document.xml
    with zipfile.ZipFile(output_docx, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, data in file_map.items():
            if name == "word/document.xml":
                zout.writestr(name, new_doc_xml)
            else:
                zout.writestr(name, data)

if __name__ == "__main__":
    extract_section_docx_xml(
        input_docx=r"c:\Users\ne025\Desktop\Test_File\Section 6.13_Cleaning and Sterilization_v2.docx",
        output_docx=r"Extract_1.1.1_General_description_knee.docx",
        start_heading_text="Sterilizing agent",
        start_number="6.13.2",
        explicit_end_title="References and documents",
        ignore_header_footer=True,
        ignore_toc=True,
        subheading_text="",
        subheading_strict_match=True,
        subheading_debug=False,
    )
    print("Done")
