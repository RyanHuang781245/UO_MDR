from __future__ import annotations

import re
import zipfile
from copy import deepcopy
from typing import Optional

from lxml import etree

from modules.chapter_section_parse import (
    parse_chapter_section_expression as _parse_chapter_section_expression,
)
from modules.extract_word_chapter import (
    NS,
    build_style_outline_map,
    find_section_range_children,
    get_all_text,
    get_pStyle,
    normalize_text,
    qn,
    remove_all_header_footer_references,
    trim_to_subheading_range,
)


FIGURE_NUMBER_PREFIX_RE = re.compile(
    r"^\s*(figure|fig\.?)\s+\d+(?:\s*[-.:])?\s+\S+",
    re.IGNORECASE,
)

FIGURE_CAPTION_STYLES = {"caption", "figurecaption", "figcaption"}


def _match_caption(text: str, target_caption_label: str) -> bool:
    pattern = re.compile(
        rf"^\s*{re.escape(normalize_text(target_caption_label))}",
        re.IGNORECASE,
    )
    return bool(pattern.match(text or ""))


def _parse_caption_target(target_caption_label: str) -> tuple[str, Optional[int]]:
    normalized = normalize_text(target_caption_label)
    match = re.match(r"^([A-Za-z.]+)\s*(\d+)?", normalized)
    if not match:
        return normalized, None
    prefix = (match.group(1) or "").strip().rstrip(".")
    number = int(match.group(2)) if match.group(2) else None
    return prefix, number


def _extract_seq_names(block: etree._Element) -> list[str]:
    instr_text = " ".join(block.xpath(".//w:instrText/text()", namespaces=NS))
    return re.findall(r"\bSEQ\s+([^\s\\]+)", instr_text, flags=re.IGNORECASE)


def _block_seq_debug(block: Optional[etree._Element]) -> dict:
    if block is None:
        return {
            "instr_text": "",
            "seq_names": [],
            "has_seq_figure": False,
        }

    instr_text = " ".join(block.xpath(".//w:instrText/text()", namespaces=NS))
    seq_names = re.findall(r"\bSEQ\s+([^\s\\]+)", instr_text, flags=re.IGNORECASE)
    seq_names_lower = [name.lower().rstrip(".") for name in seq_names]
    return {
        "instr_text": normalize_text(instr_text),
        "seq_names": seq_names,
        "has_seq_figure": ("figure" in seq_names_lower or "fig" in seq_names_lower),
    }


def _match_figure_caption_block(
    block: etree._Element,
    target_caption_label: str,
    seq_counters: dict[str, int],
) -> bool:
    text = normalize_text(get_all_text(block))
    if _match_caption(text, target_caption_label):
        return True

    prefix, target_number = _parse_caption_target(target_caption_label)
    if not prefix:
        return False

    prefix_key = prefix.lower().rstrip(".")
    if prefix_key == "fig":
        acceptable_prefixes = {"fig", "figure"}
    else:
        acceptable_prefixes = {prefix_key}

    if not re.match(
        rf"^({'|'.join(re.escape(p) for p in acceptable_prefixes)})(?:\s|$|[:.：-])",
        text,
        re.IGNORECASE,
    ):
        return False

    seq_names = [name.lower().rstrip(".") for name in _extract_seq_names(block)]
    if not any(name in acceptable_prefixes for name in seq_names):
        return False

    counter_key = "figure"
    seq_counters[counter_key] = seq_counters.get(counter_key, 0) + 1

    if target_number is None:
        return True
    return seq_counters[counter_key] == target_number


def _match_figure_title_block(block: etree._Element, target_figure_title: str) -> bool:
    if block.tag != qn("w:p"):
        return False
    title = normalize_text(target_figure_title)
    if not title:
        return False
    text = normalize_text(get_all_text(block))
    return text == title


def _paragraph_has_image(block: etree._Element) -> bool:
    """
    判斷 block 內是否有 Word 圖片：
    - DrawingML: w:drawing / a:blip
    - VML: w:pict / v:shape / v:imagedata
    """
    if block is None:
        return False

    if block.tag != qn("w:p"):
        return False

    has_drawing = bool(
        block.xpath(
            ".//w:drawing | .//a:blip | .//pic:pic | .//w:pict | .//v:imagedata",
            namespaces={
                **NS,
                "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
                "pic": "http://schemas.openxmlformats.org/drawingml/2006/picture",
                "v": "urn:schemas-microsoft-com:vml",
            },
        )
    )
    return has_drawing


def _get_first_image_block(block: etree._Element) -> Optional[etree._Element]:
    """
    與表格版 _get_first_table_element 類似，但圖片通常在段落內。
    這裡直接回傳「包含圖片的 paragraph block」。
    """
    if _paragraph_has_image(block):
        return block
    return None


def _block_text(block: Optional[etree._Element]) -> str:
    if block is None:
        return ""
    return normalize_text(get_all_text(block))


def _analyze_figure_caption_candidate(block: Optional[etree._Element]) -> dict:
    """
    給 figure_index 模式用：
    當只知道第幾張圖時，分析圖片下方第一個非空段落是否像圖名。
    """
    if block is None:
        return {"accepted": False, "reason": "missing_block"}
    if block.tag != qn("w:p"):
        return {"accepted": False, "reason": "not_paragraph"}

    text = _block_text(block)
    if not text:
        return {"accepted": False, "reason": "empty_text"}

    word_count = len(text.split())
    char_count = len(text)
    style = (get_pStyle(block) or "").strip().lower()

    if char_count > 180:
        return {
            "accepted": False,
            "reason": "too_long",
            "text": text,
            "style": style,
            "word_count": word_count,
            "char_count": char_count,
        }

    if style in FIGURE_CAPTION_STYLES or style.startswith("heading"):
        return {
            "accepted": True,
            "reason": "style_match",
            "text": text,
            "style": style,
            "word_count": word_count,
        }

    if FIGURE_NUMBER_PREFIX_RE.match(text):
        return {
            "accepted": True,
            "reason": "figure_number_prefix",
            "text": text,
            "style": style,
            "word_count": word_count,
        }

    # 沒有明確 Figure 1 開頭時，也接受較短、獨立成行的段落作為 fallback
    if word_count <= 20 and char_count <= 120:
        return {
            "accepted": True,
            "reason": "short_standalone_line",
            "text": text,
            "style": style,
            "word_count": word_count,
        }

    return {
        "accepted": False,
        "reason": "no_caption_signal",
        "text": text,
        "style": style,
        "word_count": word_count,
        "char_count": char_count,
    }


def _replace_body_with_blocks(
    document_xml: bytes,
    kept_blocks: list[etree._Element],
) -> bytes:
    root = etree.fromstring(document_xml)
    body = root.find("w:body", namespaces=NS)
    if body is None:
        raise RuntimeError("document.xml missing w:body")

    children = list(body)
    sect_pr = children[-1] if children and children[-1].tag == qn("w:sectPr") else None

    for child in list(body):
        body.remove(child)

    for block in kept_blocks:
        body.append(deepcopy(block))

    if sect_pr is not None:
        body.append(deepcopy(sect_pr))

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone="yes")


def extract_specific_figure_from_word_xml(
    input_file: str,
    output_docx_path: str | None,
    target_chapter_section: str,
    target_caption_label: str = "",
    target_subtitle: str | None = None,
    target_chapter_title: str | None = None,
    *,
    target_figure_title: str | None = None,
    target_figure_index: int | None = None,
    include_caption: bool = True,
    ignore_header_footer: bool = True,
    save_output: bool = True,
    return_reason: bool = False,
) -> bool | dict:
    """
    擷取指定章節中的圖片。
    注意：這個版本假設「圖名在圖片下面」，
    也就是先掃到圖片，再往下找 caption / title。
    """

    chapter_section = (target_chapter_section or "").strip()
    chapter_title = (target_chapter_title or "").strip()
    parsed_start, _parsed_end, parsed_title = _parse_chapter_section_expression(chapter_section)
    if parsed_start:
        chapter_section = parsed_start
        if not chapter_title and parsed_title:
            chapter_title = parsed_title

    caption_label = (target_caption_label or "").strip()
    figure_title = (target_figure_title or "").strip()
    figure_index_raw = str(target_figure_index).strip() if target_figure_index is not None else ""
    figure_index = int(figure_index_raw) if figure_index_raw else None

    if not caption_label and not figure_title and figure_index is None:
        raise ValueError(
            "One of target_caption_label, target_figure_title, or target_figure_index is required"
        )
    if figure_index is not None and figure_index <= 0:
        raise ValueError("target_figure_index must be >= 1")

    subtitle = (target_subtitle or "").strip()
    use_chapter = bool(chapter_section or chapter_title)
    start_heading_text = chapter_title or chapter_section

    with zipfile.ZipFile(input_file, "r") as zin:
        file_map = {name: zin.read(name) for name in zin.namelist()}

    if "word/document.xml" not in file_map:
        raise RuntimeError("DOCX missing word/document.xml")
    if "word/styles.xml" not in file_map:
        raise RuntimeError("DOCX missing word/styles.xml")

    style_outline, style_based = build_style_outline_map(file_map["word/styles.xml"])
    root = etree.fromstring(file_map["word/document.xml"])
    body = root.find("w:body", namespaces=NS)
    if body is None:
        raise RuntimeError("document.xml missing w:body")

    body_children = list(body)
    content_children = body_children[:-1] if body_children and body_children[-1].tag == qn("w:sectPr") else body_children

    if use_chapter:
        start_idx, end_idx = find_section_range_children(
            content_children,
            start_heading_text=start_heading_text,
            start_number=chapter_section,
            style_outline=style_outline,
            style_based=style_based,
            ignore_toc=True,
        )
        section_children = content_children[start_idx:end_idx]
    else:
        section_children = content_children

    subtitle_found = not bool(subtitle)
    if subtitle:
        try:
            section_children = trim_to_subheading_range(
                section_children,
                subheading_text=subtitle,
                strict_match=True,
                debug=False,
            )
            subtitle_found = True
        except RuntimeError:
            subtitle_found = False
            if return_reason:
                return {"ok": False, "subtitle_found": False, "reason": "subtitle_not_found"}
            return False

    # 與表格版不同：先等圖片，再等圖名
    waiting_for_caption_after_image = False
    waiting_for_title_after_image = False

    saved_image_block: etree._Element | None = None
    saved_caption_block: etree._Element | None = None
    saved_title_block: etree._Element | None = None

    pending_image_block: etree._Element | None = None
    pending_caption_candidate: etree._Element | None = None

    seq_counters: dict[str, int] = {}
    candidate_figures: list[tuple[etree._Element, Optional[etree._Element]]] = []
    selected_caption_analysis: dict | None = None
    match_mode = ""

    for block in section_children:
        image_block = _get_first_image_block(block)
        if image_block is not None:
            # 先記住這張圖，等待下一個段落當圖名
            pending_image_block = image_block
            pending_caption_candidate = None
            waiting_for_caption_after_image = bool(caption_label)
            waiting_for_title_after_image = bool(figure_title)

            candidate_figures.append((image_block, None))

            # 若是連續兩張圖，中間沒 caption，則後圖覆蓋前圖的 pending 狀態
            continue

        if block.tag != qn("w:p"):
            continue

        text = normalize_text(get_all_text(block))
        if not text:
            continue

        # 只有在「前面已經有 pending 圖片」時，才把本段當作圖片下方的候選圖名
        if pending_image_block is not None:
            pending_caption_candidate = block
            candidate_figures[-1] = (pending_image_block, block)

            if caption_label and _match_figure_caption_block(block, caption_label, seq_counters):
                saved_image_block = pending_image_block
                saved_caption_block = block
                saved_title_block = None
                match_mode = "caption"
                break

            if figure_title and _match_figure_title_block(block, figure_title):
                saved_image_block = pending_image_block
                saved_title_block = block
                saved_caption_block = None
                match_mode = "title"
                break

            # 已經檢查過這個「圖片下方第一個非空段落」了，不再往後延伸
            pending_image_block = None
            pending_caption_candidate = None
            waiting_for_caption_after_image = False
            waiting_for_title_after_image = False
            continue

        waiting_for_caption_after_image = False
        waiting_for_title_after_image = False

    if saved_image_block is None:
        if figure_index is not None and len(candidate_figures) >= figure_index:
            saved_image_block, fallback_caption_block = candidate_figures[figure_index - 1]
            saved_title_block = None
            saved_caption_block = None

            selected_caption_analysis = _analyze_figure_caption_candidate(fallback_caption_block)
            if selected_caption_analysis.get("accepted"):
                saved_caption_block = fallback_caption_block

            match_mode = "figure_index"

    if saved_image_block is None:
        if return_reason:
            return {
                "ok": False,
                "subtitle_found": subtitle_found,
                "reason": "figure_not_found",
                "chapter_section": chapter_section,
                "chapter_title": chapter_title,
                "target_subtitle": subtitle,
                "target_caption_label": caption_label,
                "target_figure_title": figure_title,
                "target_figure_index": figure_index,
                "figure_candidates_found": len(candidate_figures),
            }
        return False

    kept_blocks: list[etree._Element] = [saved_image_block]
    if include_caption and saved_caption_block is not None:
        kept_blocks.append(saved_caption_block)
    elif include_caption and saved_title_block is not None:
        kept_blocks.append(saved_title_block)

    if save_output and output_docx_path:
        updated_document_xml = _replace_body_with_blocks(file_map["word/document.xml"], kept_blocks)
        if ignore_header_footer:
            updated_root = etree.fromstring(updated_document_xml)
            remove_all_header_footer_references(updated_root)
            updated_document_xml = etree.tostring(
                updated_root,
                xml_declaration=True,
                encoding="UTF-8",
                standalone="yes",
            )
        with zipfile.ZipFile(output_docx_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for name, data in file_map.items():
                zout.writestr(name, updated_document_xml if name == "word/document.xml" else data)

    if return_reason:
        if selected_caption_analysis is None and saved_caption_block is not None:
            selected_caption_analysis = _analyze_figure_caption_candidate(saved_caption_block)

        selected_caption_seq = _block_seq_debug(saved_caption_block)
        return {
            "ok": True,
            "subtitle_found": subtitle_found,
            "reason": "ok",
            "match_mode": match_mode or "figure_index",
            "selected_caption_text": _block_text(saved_caption_block),
            "selected_caption_analysis": selected_caption_analysis or {},
            "selected_caption_seq_debug": selected_caption_seq,
        }

    return True
