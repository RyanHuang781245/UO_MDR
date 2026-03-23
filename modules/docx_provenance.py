import json
import os
import re
import zipfile
from pathlib import Path
from typing import Any

from lxml import etree

_W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_NS = {"w": _W_NS}
_PROVENANCE_PREFIX = "prov_src_"
_PROVENANCE_CACHE_VERSION = 1


def _qn(tag: str) -> str:
    prefix, local = tag.split(":", 1)
    if prefix != "w":
        raise ValueError(f"Unsupported namespace prefix: {prefix}")
    return f"{{{_W_NS}}}{local}"


def _normalize_trace_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def build_provenance_descriptor(sequence: int) -> dict[str, Any]:
    source_id = f"src_{sequence:06d}"
    bookmark_id = 100000 + sequence
    return {
        "source_id": source_id,
        "bookmark_start": f"{_PROVENANCE_PREFIX}{source_id}_s",
        "bookmark_end": f"{_PROVENANCE_PREFIX}{source_id}_e",
        "bookmark_end_marker": f"_prov_end_{source_id}",
        "bookmark_id": bookmark_id,
    }


def _load_docx_parts(docx_path: str) -> dict[str, bytes]:
    with zipfile.ZipFile(docx_path, "r") as zin:
        return {name: zin.read(name) for name in zin.namelist()}


def _write_docx_parts(docx_path: str, parts: dict[str, bytes]) -> None:
    with zipfile.ZipFile(docx_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, data in parts.items():
            zout.writestr(name, data)


def _iter_body_blocks(body: etree._Element) -> list[etree._Element]:
    return [
        child
        for child in body.iterchildren()
        if child.tag in {_qn("w:p"), _qn("w:tbl")}
    ]


def _first_descendant_paragraph(block: etree._Element) -> etree._Element | None:
    if block.tag == _qn("w:p"):
        return block
    paragraphs = block.xpath(".//w:p", namespaces=_NS)
    return paragraphs[0] if paragraphs else None


def _last_descendant_paragraph(block: etree._Element) -> etree._Element | None:
    if block.tag == _qn("w:p"):
        return block
    paragraphs = block.xpath(".//w:p", namespaces=_NS)
    return paragraphs[-1] if paragraphs else None


def _insert_bookmark_start(
    paragraph: etree._Element,
    bookmark_name: str,
    bookmark_id: int,
    *,
    at_end: bool = False,
) -> None:
    bookmark = etree.Element(_qn("w:bookmarkStart"))
    bookmark.set(_qn("w:id"), str(bookmark_id))
    bookmark.set(_qn("w:name"), bookmark_name)

    if at_end:
        paragraph.append(bookmark)
        return

    insert_pos = 0
    children = list(paragraph)
    if children and children[0].tag == _qn("w:pPr"):
        insert_pos = 1
    paragraph.insert(insert_pos, bookmark)


def _insert_bookmark_end(paragraph: etree._Element, bookmark_id: int) -> None:
    bookmark = etree.Element(_qn("w:bookmarkEnd"))
    bookmark.set(_qn("w:id"), str(bookmark_id))
    paragraph.append(bookmark)


def annotate_docx_with_provenance(
    docx_path: str,
    *,
    bookmark_start: str,
    bookmark_end: str,
    bookmark_end_marker: str | None = None,
    bookmark_id: int,
) -> bool:
    if not docx_path or not os.path.isfile(docx_path):
        return False

    parts = _load_docx_parts(docx_path)
    document_xml = parts.get("word/document.xml")
    if not document_xml:
        return False

    root = etree.fromstring(document_xml)
    body = root.find("w:body", namespaces=_NS)
    if body is None:
        return False

    blocks = _iter_body_blocks(body)
    if not blocks:
        return False

    first_paragraph = _first_descendant_paragraph(blocks[0])
    last_paragraph = _last_descendant_paragraph(blocks[-1])
    if first_paragraph is None or last_paragraph is None:
        return False

    _insert_bookmark_start(first_paragraph, bookmark_start, bookmark_id)
    if bookmark_end_marker:
        _insert_bookmark_start(last_paragraph, bookmark_end_marker, bookmark_id + 500000, at_end=True)
    _insert_bookmark_end(last_paragraph, bookmark_id)

    parts["word/document.xml"] = etree.tostring(
        root,
        xml_declaration=True,
        encoding="UTF-8",
        standalone="yes",
    )
    _write_docx_parts(docx_path, parts)
    return True


def _extract_source_id_from_bookmark_name(bookmark_name: str) -> str:
    match = re.match(rf"^{re.escape(_PROVENANCE_PREFIX)}(.+)_s$", bookmark_name or "")
    return match.group(1) if match else ""


def repair_provenance_bookmarks(docx_path: str) -> int:
    if not docx_path or not os.path.isfile(docx_path):
        return 0

    parts = _load_docx_parts(docx_path)
    document_xml = parts.get("word/document.xml")
    if not document_xml:
        return 0

    root = etree.fromstring(document_xml)
    body = root.find("w:body", namespaces=_NS)
    if body is None:
        return 0

    existing_end_ids = {
        str(node.get(_qn("w:id")) or "")
        for node in body.xpath(".//w:bookmarkEnd", namespaces=_NS)
    }
    marker_nodes: dict[str, etree._Element] = {}
    for node in body.xpath(".//w:bookmarkStart", namespaces=_NS):
        name = str(node.get(_qn("w:name")) or "")
        if name.startswith("_prov_end_"):
            source_id = name[len("_prov_end_"):]
            if source_id:
                marker_nodes[source_id] = node
    unresolved_ids: list[str] = []
    repaired = 0
    previous_block: etree._Element | None = None

    for block in _iter_body_blocks(body):
        current_start_nodes = [
            node
            for node in block.xpath(".//w:bookmarkStart", namespaces=_NS)
            if str(node.get(_qn("w:name")) or "").startswith(_PROVENANCE_PREFIX)
        ]
        current_start_ids = []
        for node in current_start_nodes:
            bookmark_id = str(node.get(_qn("w:id")) or "")
            bookmark_name = str(node.get(_qn("w:name")) or "")
            if not bookmark_id or bookmark_id in existing_end_ids:
                continue
            source_id = _extract_source_id_from_bookmark_name(bookmark_name)
            marker_node = marker_nodes.get(source_id)
            if marker_node is not None:
                parent = marker_node.getparent()
                if parent is not None:
                    marker_index = parent.index(marker_node)
                    bookmark_end = etree.Element(_qn("w:bookmarkEnd"))
                    bookmark_end.set(_qn("w:id"), bookmark_id)
                    parent.insert(marker_index + 1, bookmark_end)
                    parent.remove(marker_node)
                    repaired += 1
                    existing_end_ids.add(bookmark_id)
                    continue
            current_start_ids.append(bookmark_id)
        if current_start_ids and unresolved_ids and previous_block is not None:
            target_paragraph = _last_descendant_paragraph(previous_block)
            if target_paragraph is not None:
                for bookmark_id in unresolved_ids:
                    _insert_bookmark_end(target_paragraph, int(bookmark_id))
                    repaired += 1
                    existing_end_ids.add(bookmark_id)
            unresolved_ids = []

        unresolved_ids.extend(current_start_ids)
        previous_block = block

    if unresolved_ids and previous_block is not None:
        target_paragraph = _last_descendant_paragraph(previous_block)
        if target_paragraph is not None:
            for bookmark_id in unresolved_ids:
                _insert_bookmark_end(target_paragraph, int(bookmark_id))
                repaired += 1
                existing_end_ids.add(bookmark_id)

    if repaired:
        parts["word/document.xml"] = etree.tostring(
            root,
            xml_declaration=True,
            encoding="UTF-8",
            standalone="yes",
        )
        _write_docx_parts(docx_path, parts)

    return repaired


def _paragraph_text(block: etree._Element) -> str:
    texts = block.xpath(".//w:t/text()", namespaces=_NS)
    return _normalize_trace_text("".join(texts))


def _table_probe_texts(block: etree._Element, *, hide_set: set[str] | None = None) -> list[str]:
    hide_values = hide_set or set()
    texts: list[str] = []
    seen: set[str] = set()

    for row in block.xpath("./w:tr", namespaces=_NS):
        row_parts: list[str] = []
        for cell in row.xpath("./w:tc", namespaces=_NS):
            cell_text = _normalize_trace_text(" ".join(cell.xpath(".//w:t/text()", namespaces=_NS)))
            if not cell_text or cell_text in hide_values:
                continue
            row_parts.append(cell_text)
            if len(cell_text) >= 20 and cell_text not in seen:
                seen.add(cell_text)
                texts.append(cell_text)
        row_text = _normalize_trace_text(" ".join(row_parts))
        if len(row_text) >= 24 and row_text not in seen:
            seen.add(row_text)
            texts.append(row_text)

    return texts


def _paragraph_has_drawing(block: etree._Element) -> bool:
    return bool(
        block.xpath(
            ".//w:drawing | .//w:pict | .//w:object | .//w:binData",
            namespaces=_NS,
        )
    )


def extract_docx_blocks(
    docx_path: str,
    *,
    hide_set: set[str] | None = None,
    content_type: str = "",
    primary_probe_texts: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not docx_path or not os.path.isfile(docx_path):
        return []

    parts = _load_docx_parts(docx_path)
    document_xml = parts.get("word/document.xml")
    if not document_xml:
        return []

    root = etree.fromstring(document_xml)
    body = root.find("w:body", namespaces=_NS)
    if body is None:
        return []

    hide_values = hide_set or set()
    fallback_probes = [
        _normalize_trace_text(str(item))
        for item in (primary_probe_texts or [])
        if _normalize_trace_text(str(item))
    ]
    blocks: list[dict[str, Any]] = []

    for block_index, block in enumerate(_iter_body_blocks(body)):
        if block.tag == _qn("w:p"):
            text = _paragraph_text(block)
            normalized = _normalize_trace_text(text)
            has_drawing = _paragraph_has_drawing(block)
            if normalized and normalized not in hide_values:
                blocks.append(
                    {
                        "block_index": block_index,
                        "block_type": "paragraph",
                        "text": text,
                        "normalized_text": normalized,
                        "probe_texts": [normalized],
                        "has_drawing": has_drawing,
                    }
                )
            elif has_drawing:
                blocks.append(
                    {
                        "block_index": block_index,
                        "block_type": content_type or "drawing",
                        "text": "",
                        "normalized_text": "",
                        "probe_texts": fallback_probes[:],
                        "has_drawing": True,
                    }
                )
        elif block.tag == _qn("w:tbl"):
            probe_texts = _table_probe_texts(block, hide_set=hide_values)
            if probe_texts:
                blocks.append(
                    {
                        "block_index": block_index,
                        "block_type": "table",
                        "text": "",
                        "normalized_text": "",
                        "probe_texts": probe_texts,
                        "has_drawing": False,
                    }
                )

    return blocks


def _block_match_score(source_block: dict[str, Any], target_block: dict[str, Any]) -> int:
    source_type = str(source_block.get("block_type") or "")
    target_type = str(target_block.get("block_type") or "")
    if source_type == "table" or target_type == "table":
        if source_type != target_type:
            return 0
        source_probes = [str(item) for item in (source_block.get("probe_texts") or []) if str(item).strip()]
        target_probes = [str(item) for item in (target_block.get("probe_texts") or []) if str(item).strip()]
        score = 0
        target_set = set(target_probes)
        for probe in source_probes[:12]:
            if probe in target_set:
                score += min(len(probe), 80)
        return score

    source_text = str(source_block.get("normalized_text") or "")
    target_text = str(target_block.get("normalized_text") or "")
    if source_text and target_text:
        if source_text == target_text:
            return 200 + min(len(source_text), 120)
        if len(source_text) >= 18 and (source_text in target_text or target_text in source_text):
            return 80 + min(len(source_text), len(target_text), 80)

    source_has_drawing = bool(source_block.get("has_drawing"))
    target_has_drawing = bool(target_block.get("has_drawing"))
    if source_has_drawing and target_has_drawing:
        score = 40
        for probe in [str(item) for item in (source_block.get("probe_texts") or []) if str(item).strip()]:
            normalized_probe = _normalize_trace_text(probe)
            if normalized_probe and normalized_probe in target_text:
                score += min(len(normalized_probe), 80)
        return score

    return 0


def _find_matching_block_index(
    source_block: dict[str, Any],
    target_blocks: list[dict[str, Any]],
    start_idx: int,
) -> int | None:
    best_index: int | None = None
    best_score = 0
    for idx in range(start_idx, len(target_blocks)):
        score = _block_match_score(source_block, target_blocks[idx])
        if score <= 0:
            continue
        if score > best_score:
            best_index = idx
            best_score = score
        if score >= 200:
            return idx
    return best_index


def _annotate_result_docx_range(
    docx_path: str,
    *,
    bookmark_start: str,
    bookmark_end: str,
    bookmark_id: int,
    start_block_index: int,
    end_block_index: int,
) -> bool:
    if not docx_path or not os.path.isfile(docx_path):
        return False

    parts = _load_docx_parts(docx_path)
    document_xml = parts.get("word/document.xml")
    if not document_xml:
        return False

    root = etree.fromstring(document_xml)
    body = root.find("w:body", namespaces=_NS)
    if body is None:
        return False

    blocks = _iter_body_blocks(body)
    if (
        start_block_index < 0
        or end_block_index < start_block_index
        or start_block_index >= len(blocks)
        or end_block_index >= len(blocks)
    ):
        return False

    start_paragraph = _first_descendant_paragraph(blocks[start_block_index])
    end_paragraph = _last_descendant_paragraph(blocks[end_block_index])
    if start_paragraph is None or end_paragraph is None:
        return False

    _insert_bookmark_start(start_paragraph, bookmark_start, bookmark_id)
    _insert_bookmark_end(end_paragraph, bookmark_id)
    parts["word/document.xml"] = etree.tostring(
        root,
        xml_declaration=True,
        encoding="UTF-8",
        standalone="yes",
    )
    _write_docx_parts(docx_path, parts)
    return True


def apply_final_provenance(result_docx: str, source_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not result_docx or not os.path.isfile(result_docx):
        return []

    target_blocks = extract_docx_blocks(result_docx)
    if not target_blocks:
        return []

    cursor = 0
    applied: list[dict[str, Any]] = []
    for record in source_records:
        fragment_path = str(record.get("fragment_path") or "")
        source_id = str(record.get("source_id") or "")
        if not fragment_path or not source_id:
            continue

        source_blocks = extract_docx_blocks(
            fragment_path,
            content_type=str(record.get("content_type") or ""),
            primary_probe_texts=[str(item) for item in (record.get("primary_probe_texts") or []) if str(item).strip()],
        )
        if not source_blocks:
            continue

        matched_indexes: list[int] = []
        search_cursor = cursor
        for source_block in source_blocks:
            matched_idx = _find_matching_block_index(source_block, target_blocks, search_cursor)
            if matched_idx is None:
                matched_indexes = []
                break
            matched_indexes.append(matched_idx)
            search_cursor = matched_idx + 1

        if not matched_indexes:
            continue

        start_idx = matched_indexes[0]
        end_idx = matched_indexes[-1]
        if _annotate_result_docx_range(
            result_docx,
            bookmark_start=str(record.get("bookmark_start") or ""),
            bookmark_end=str(record.get("bookmark_end") or ""),
            bookmark_id=int(record.get("bookmark_id") or 0),
            start_block_index=int(target_blocks[start_idx]["block_index"]),
            end_block_index=int(target_blocks[end_idx]["block_index"]),
        ):
            applied.append(
                {
                    **record,
                    "result_block_start": int(target_blocks[start_idx]["block_index"]),
                    "result_block_end": int(target_blocks[end_idx]["block_index"]),
                }
            )
            cursor = end_idx + 1

    return applied


def extract_provenance_block_trace(
    result_docx: str,
    source_lookup: dict[str, dict[str, Any]],
    *,
    hide_set: set[str] | None = None,
) -> list[dict[str, Any]]:
    if not result_docx or not os.path.isfile(result_docx):
        return []

    parts = _load_docx_parts(result_docx)
    document_xml = parts.get("word/document.xml")
    if not document_xml:
        return []

    root = etree.fromstring(document_xml)
    body = root.find("w:body", namespaces=_NS)
    if body is None:
        return []

    bookmark_name_to_source_id = {
        str(meta.get("bookmark_start") or ""): source_id
        for source_id, meta in source_lookup.items()
        if meta.get("bookmark_start")
    }
    bookmark_id_to_source_id: dict[str, str] = {}
    active_sources: list[str] = []
    trace: list[dict[str, Any]] = []
    block_index = 0

    for block in _iter_body_blocks(body):
        start_names = [
            str(node.get(_qn("w:name")) or "")
            for node in block.xpath(".//w:bookmarkStart", namespaces=_NS)
        ]
        for bookmark_name in start_names:
            source_id = bookmark_name_to_source_id.get(bookmark_name)
            if not source_id:
                continue
            active_sources.append(source_id)
        for node in block.xpath(".//w:bookmarkStart", namespaces=_NS):
            bookmark_name = str(node.get(_qn("w:name")) or "")
            source_id = bookmark_name_to_source_id.get(bookmark_name)
            bookmark_id = str(node.get(_qn("w:id")) or "")
            if source_id and bookmark_id:
                bookmark_id_to_source_id[bookmark_id] = source_id

        source_id = active_sources[-1] if active_sources else ""
        meta = source_lookup.get(source_id, {}) if source_id else {}
        source_file = str(meta.get("source_file") or "未知來源") if source_id else "未知來源"
        source_step = str(meta.get("source_step") or "") if source_id else ""
        content_type = str(meta.get("content_type") or "") if source_id else ""

        if block.tag == _qn("w:p"):
            text = _paragraph_text(block)
            normalized = _normalize_trace_text(text)
            if normalized and normalized not in (hide_set or set()):
                trace.append(
                    {
                        "block_index": block_index,
                        "block_type": "paragraph",
                        "source_id": source_id,
                        "source_file": source_file,
                        "source_step": source_step,
                        "content_type": content_type or "paragraph",
                        "text": text,
                        "probe_texts": [normalized],
                    }
                )
                block_index += 1
            elif source_id and content_type in {"figure", "pdf_image"}:
                primary_probe_texts = [
                    _normalize_trace_text(str(item))
                    for item in (meta.get("primary_probe_texts") or [])
                    if _normalize_trace_text(str(item))
                ]
                if primary_probe_texts:
                    trace.append(
                        {
                            "block_index": block_index,
                            "block_type": content_type,
                            "source_id": source_id,
                            "source_file": source_file,
                            "source_step": source_step,
                            "content_type": content_type,
                            "text": "",
                            "probe_texts": primary_probe_texts,
                        }
                    )
                    block_index += 1
        elif block.tag == _qn("w:tbl"):
            probe_texts = _table_probe_texts(block, hide_set=hide_set)
            if probe_texts:
                trace.append(
                    {
                        "block_index": block_index,
                        "block_type": "table",
                        "source_id": source_id,
                        "source_file": source_file,
                        "source_step": source_step,
                        "content_type": content_type or "table",
                        "text": "",
                        "probe_texts": probe_texts,
                    }
                )
                block_index += 1

        end_ids = [
            str(node.get(_qn("w:id")) or "")
            for node in block.xpath(".//w:bookmarkEnd", namespaces=_NS)
        ]
        for bookmark_id in end_ids:
            source_id = bookmark_id_to_source_id.get(bookmark_id)
            if not source_id:
                continue
            for idx in range(len(active_sources) - 1, -1, -1):
                if active_sources[idx] == source_id:
                    active_sources.pop(idx)
                    break

    return trace


def build_provenance_cache_payload(
    *,
    source_lookup: dict[str, dict[str, Any]],
    block_trace: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "version": _PROVENANCE_CACHE_VERSION,
        "sources": list(source_lookup.values()),
        "block_trace": block_trace,
    }


def load_cached_provenance_payload(cache_path: str) -> dict[str, Any] | None:
    if not cache_path or not os.path.isfile(cache_path):
        return None
    try:
        payload = json.loads(Path(cache_path).read_text(encoding="utf-8"))
    except Exception:
        return None
    if payload.get("version") != _PROVENANCE_CACHE_VERSION:
        return None
    if not isinstance(payload.get("block_trace"), list):
        return None
    return payload
