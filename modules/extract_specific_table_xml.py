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
    build_style_heading_rank_map,
    build_style_outline_map,
    find_section_range_children,
    get_all_text,
    get_pStyle,
    normalize_text,
    qn,
    materialize_paragraph_numpr_as_text,
    normalize_paragraph_to_plain_text_run,
    remove_all_header_footer_references,
    trim_to_subheading_range,
)

TABLE_NUMBER_PREFIX_RE = re.compile(r"^\s*table\s+\d+(?:\s*[-.:])?\s+\S+", re.IGNORECASE)
TITLE_STOPWORDS = {
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "has",
    "have",
    "had",
    "shows",
    "shown",
    "indicates",
    "indicated",
    "provides",
    "provided",
    "describes",
    "described",
    "includes",
    "including",
    "contains",
    "containing",
    "summarizes",
    "summarized",
    "lists",
    "listed",
}


def _match_caption(text: str, target_caption_label: str) -> bool:
    pattern = re.compile(rf"^\s*{re.escape(normalize_text(target_caption_label))}", re.IGNORECASE)
    return bool(pattern.match(text or ""))


def _parse_caption_target(target_caption_label: str) -> tuple[str, Optional[int]]:
    normalized = normalize_text(target_caption_label)
    match = re.match(r"^([A-Za-z]+)\s*(\d+)?", normalized)
    if not match:
        return normalized, None
    prefix = (match.group(1) or "").strip()
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
            "has_seq_table": False,
        }

    instr_text = " ".join(block.xpath(".//w:instrText/text()", namespaces=NS))
    seq_names = re.findall(r"\bSEQ\s+([^\s\\]+)", instr_text, flags=re.IGNORECASE)
    seq_names_lower = [name.lower() for name in seq_names]
    return {
        "instr_text": normalize_text(instr_text),
        "seq_names": seq_names,
        "has_seq_table": "table" in seq_names_lower,
    }


def _match_caption_block(
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

    prefix_key = prefix.lower()
    if not re.match(rf"^{re.escape(prefix)}(?:\s|$|[:.：-])", text, re.IGNORECASE):
        return False

    seq_names = [name.lower() for name in _extract_seq_names(block)]
    if prefix_key not in seq_names:
        return False

    seq_counters[prefix_key] = seq_counters.get(prefix_key, 0) + 1
    if target_number is None:
        return True
    return seq_counters[prefix_key] == target_number


def _match_table_title_block(block: etree._Element, target_table_title: str) -> bool:
    if block.tag != qn("w:p"):
        return False
    title = normalize_text(target_table_title)
    if not title:
        return False
    text = normalize_text(get_all_text(block))
    return text == title


def _get_first_table_element(block: etree._Element) -> Optional[etree._Element]:
    if block.tag == qn("w:tbl"):
        return block
    tables = block.xpath(".//w:tbl", namespaces=NS)
    return tables[0] if tables else None


def _block_text(block: Optional[etree._Element]) -> str:
    if block is None:
        return ""
    return normalize_text(get_all_text(block))


def _analyze_table_title_candidate(
    block: Optional[etree._Element],
) -> dict:
    if block is None:
        return {"accepted": False, "reason": "missing_block"}
    if block.tag != qn("w:p"):
        return {"accepted": False, "reason": "not_paragraph"}

    text = _block_text(block)
    if not text:
        return {"accepted": False, "reason": "empty_text"}

    lowered = text.lower()
    word_count = len(text.split())
    if len(text) > 140:
        return {
            "accepted": False,
            "reason": "too_long",
            "text": text,
            "word_count": word_count,
            "char_count": len(text),
        }

    style = (get_pStyle(block) or "").strip().lower()
    if style in {"caption", "tabletitle"} or style.startswith("heading"):
        return {
            "accepted": True,
            "reason": "style_match",
            "text": text,
            "style": style,
            "word_count": word_count,
        }

    if TABLE_NUMBER_PREFIX_RE.match(text):
        return {
            "accepted": True,
            "reason": "table_number_prefix",
            "text": text,
            "style": style,
            "word_count": word_count,
        }

    words_lower = re.findall(r"[A-Za-z]+", lowered)
    has_title_stopword = any(word in TITLE_STOPWORDS for word in words_lower)
    if not has_title_stopword:
        return {
            "accepted": True,
            "reason": "short_standalone_line",
            "text": text,
            "style": style,
            "word_count": word_count,
        }

    return {
        "accepted": False,
        "reason": "no_title_signal",
        "text": text,
        "style": style,
        "word_count": word_count,
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


def extract_specific_table_from_word_xml(
    input_file: str,
    output_docx_path: str | None,
    target_chapter_section: str,
    target_caption_label: str = "",
    target_subtitle: str | None = None,
    target_chapter_title: str | None = None,
    *,
    target_table_title: str | None = None,
    target_table_index: int | None = None,
    include_caption: bool = True,
    ignore_header_footer: bool = True,
    save_output: bool = True,
    return_reason: bool = False,
) -> bool | dict:
    """
    Extract the first table after a target caption within a specific DOCX chapter
    by parsing ``word/document.xml`` directly.

    The original implementation remains untouched in ``Extract_AllFile_to_FinalWord.py``.
    """

    chapter_section = (target_chapter_section or "").strip()
    chapter_title = (target_chapter_title or "").strip()
    parsed_start, _parsed_end, parsed_title = _parse_chapter_section_expression(chapter_section)
    if parsed_start:
        chapter_section = parsed_start
        if not chapter_title and parsed_title:
            chapter_title = parsed_title

    caption_label = (target_caption_label or "").strip()
    table_title = (target_table_title or "").strip()
    table_index_raw = str(target_table_index).strip() if target_table_index is not None else ""
    table_index = int(table_index_raw) if table_index_raw else None
    if not caption_label and not table_title and table_index is None:
        raise ValueError("One of target_caption_label, target_table_title, or target_table_index is required")
    if table_index is not None and table_index <= 0:
        raise ValueError("target_table_index must be >= 1")

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
    style_heading_rank = build_style_heading_rank_map(file_map["word/styles.xml"])
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
            style_heading_rank=style_heading_rank,
            ignore_toc=True,
            numbering_xml=file_map.get("word/numbering.xml"),
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

    waiting_for_table = False
    saved_caption_block: etree._Element | None = None
    saved_table_block: etree._Element | None = None
    saved_title_block: etree._Element | None = None
    seq_counters: dict[str, int] = {}
    waiting_for_title_table = False
    candidate_tables: list[tuple[etree._Element, Optional[etree._Element]]] = []
    last_nonempty_paragraph: etree._Element | None = None
    selected_title_analysis: dict | None = None
    match_mode = ""

    for block in section_children:
        if block.tag == qn("w:p"):
            text = normalize_text(get_all_text(block))
            if caption_label and _match_caption_block(block, caption_label, seq_counters):
                waiting_for_table = True
                waiting_for_title_table = False
                saved_caption_block = block
                saved_title_block = None
                continue
            if table_title and _match_table_title_block(block, table_title):
                waiting_for_title_table = True
                waiting_for_table = False
                saved_title_block = block
                saved_caption_block = None
                continue
            if text:
                last_nonempty_paragraph = block
                waiting_for_table = False
                waiting_for_title_table = False

        table_element = _get_first_table_element(block)
        if table_element is None:
            continue

        candidate_tables.append((table_element, last_nonempty_paragraph))
        if waiting_for_table or waiting_for_title_table:
            saved_table_block = table_element
            match_mode = "caption" if waiting_for_table else "title"
            break

    if saved_table_block is None:
        if table_index is not None and len(candidate_tables) >= table_index:
            saved_table_block, fallback_title_block = candidate_tables[table_index - 1]
            saved_caption_block = None
            selected_title_analysis = _analyze_table_title_candidate(fallback_title_block)
            saved_title_block = fallback_title_block if selected_title_analysis.get("accepted") else None
            match_mode = "table_index"

    if saved_table_block is None:
        if return_reason:
            return {
                "ok": False,
                "subtitle_found": subtitle_found,
                "reason": "table_not_found",
                "chapter_section": chapter_section,
                "chapter_title": chapter_title,
                "target_subtitle": subtitle,
                "target_caption_label": caption_label,
                "target_table_title": table_title,
                "target_table_index": table_index,
                "table_candidates_found": len(candidate_tables),
            }
        return False

    kept_blocks: list[etree._Element] = []
    if include_caption and saved_caption_block is not None:
        materialized = materialize_paragraph_numpr_as_text(
            saved_caption_block,
            content_children,
            file_map.get("word/numbering.xml"),
        )
        normalize_paragraph_to_plain_text_run(
            saved_caption_block,
            prefer_following_text_run=materialized,
        )
        kept_blocks.append(saved_caption_block)
    elif include_caption and saved_title_block is not None:
        materialized = materialize_paragraph_numpr_as_text(
            saved_title_block,
            content_children,
            file_map.get("word/numbering.xml"),
        )
        normalize_paragraph_to_plain_text_run(
            saved_title_block,
            prefer_following_text_run=materialized,
        )
        kept_blocks.append(saved_title_block)
    kept_blocks.append(saved_table_block)

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
        if selected_title_analysis is None and saved_title_block is not None:
            selected_title_analysis = _analyze_table_title_candidate(saved_title_block)
        selected_caption_seq = _block_seq_debug(saved_caption_block)
        selected_title_seq = _block_seq_debug(saved_title_block)
        return {
            "ok": True,
            "subtitle_found": subtitle_found,
            "reason": "ok",
            "match_mode": match_mode or "table_index",
            "selected_caption_text": _block_text(saved_caption_block),
            "selected_caption_seq_debug": selected_caption_seq,
            "selected_title_text": _block_text(saved_title_block),
            "selected_title_analysis": selected_title_analysis or {},
            "selected_title_seq_debug": selected_title_seq,
        }
    return True
