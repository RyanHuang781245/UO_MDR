import hashlib
import json
import os
import re
import zipfile
from typing import Any, Dict, List, Tuple

from docxtpl import DocxTemplate
from lxml import etree

from modules.template_mapping import (
    NS,
    parse_paragraph_numbering,
    qn,
    read_docx_parts,
)

CACHE_DIR_NAME = "_template_cache"


def _hash_file(path: str) -> str:
    """Return an md5 digest for the given file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _cache_path(template_path: str, digest: str) -> str:
    base_dir = os.path.dirname(template_path)
    cache_dir = os.path.join(base_dir, CACHE_DIR_NAME)
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{digest}_{os.path.basename(template_path)}.json")


def parse_template_paragraphs(template_path: str, *, use_cache: bool = True) -> List[Dict[str, Any]]:
    """Parse numbering-aware paragraph metadata from a template file."""
    if not os.path.isfile(template_path):
        raise FileNotFoundError(f"Template file not found: {template_path}")

    digest_full = _hash_file(template_path)
    digest_short = digest_full[:8]

    if use_cache:
        cache_file = _cache_path(template_path, digest_short)
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as fp:
                    cached = json.load(fp)
                if cached.get("hash") == digest_full and isinstance(cached.get("paragraphs"), list):
                    return cached["paragraphs"]
            except Exception:
                pass

    paragraphs = parse_paragraph_numbering(template_path)
    try:
        with open(_cache_path(template_path, digest_short), "w", encoding="utf-8") as fp:
            json.dump(
                {
                    "template": os.path.basename(template_path),
                    "hash": digest_full,
                    "paragraphs": paragraphs,
                },
                fp,
                ensure_ascii=False,
                indent=2,
            )
    except Exception:
        pass

    return paragraphs


def _write_zip(parts: Dict[str, bytes], out_path: str) -> None:
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, data in parts.items():
            zout.writestr(name, data)


def _new_placeholder_paragraph(var_name: str, *, as_subdoc: bool = False) -> etree._Element:
    """Build a placeholder-only paragraph for docxtpl subdoc insertion."""
    p = etree.Element(qn("w:p"))
    r = etree.SubElement(p, qn("w:r"))
    t = etree.SubElement(r, qn("w:t"))
    t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    if as_subdoc:
        # `{{p ...}}` tells docxtpl to inject a subdocument as paragraph XML
        # instead of escaping it as plain text inside <w:t>.
        t.text = f"{{{{p {var_name} }}}}"
    else:
        t.text = f"{{{{ {var_name} }}}}"
    return p


def _clear_paragraph_keep_ppr(p: etree._Element) -> None:
    children = list(p)
    for ch in children:
        if ch.tag != qn("w:pPr"):
            p.remove(ch)


def add_docxtpl_var_at_paragraph_index(
    doc_xml_bytes: bytes,
    idx: int,
    var_name: str,
    mode: str,
    *,
    as_subdoc: bool = False,
) -> bytes:
    """
    Insert or replace a paragraph at index with a docxtpl placeholder.

    mode:
      - insert_after: insert a new placeholder paragraph after the target
      - replace: replace the target paragraph content with the placeholder
    """
    root = etree.fromstring(doc_xml_bytes)
    # Ensure common Word namespaces are declared (attributes like w14:paraId).
    if "w14" not in (root.nsmap or {}):
        nsmap = dict(root.nsmap or {})
        nsmap["w14"] = "http://schemas.microsoft.com/office/word/2010/wordml"
        new_root = etree.Element(root.tag, nsmap=nsmap)
        new_root.attrib.update(root.attrib)
        new_root.text = root.text
        new_root.tail = root.tail
        new_root[:] = list(root)
        root = new_root
    paras = root.xpath("//w:p", namespaces=NS)
    if idx < 0 or idx >= len(paras):
        return doc_xml_bytes

    target = paras[idx]
    parent = target.getparent()
    if parent is None:
        return doc_xml_bytes

    if mode == "replace":
        _clear_paragraph_keep_ppr(target)
        ph = _new_placeholder_paragraph(var_name, as_subdoc=as_subdoc)
        for ch in list(ph):
            target.append(ch)
    else:
        ph = _new_placeholder_paragraph(var_name, as_subdoc=as_subdoc)
        insert_pos = parent.index(target) + 1
        parent.insert(insert_pos, ph)

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone="yes")


def make_var_name(display: str, text: str) -> str:
    base = f"{display} {text}".strip()
    base = re.sub(r"\s+", " ", base)
    base = re.sub(r"[^0-9A-Za-z_ ]+", "_", base)
    base = base.replace(" ", "_")
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "section"
    if base[0].isdigit():
        base = "sec_" + base
    if len(base) > 60:
        h = hashlib.md5(base.encode("utf-8")).hexdigest()[:8]
        base = base[:60] + "_" + h
    return base


def order_template_mappings(raw_mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    annotated: List[Tuple[int, int, str, Dict[str, Any]]] = []
    for seq, mp in enumerate(raw_mappings):
        idx = int(mp["index"])
        source_order = int(mp.get("source_order", seq))
        mode = (mp.get("mode") or "insert_after").strip() or "insert_after"
        annotated.append((idx, source_order, mode, mp))

    ordered: List[Dict[str, Any]] = []
    for idx in sorted({item[0] for item in annotated}, reverse=True):
        same_index = [item for item in annotated if item[0] == idx]
        replace_items = sorted(
            [item for item in same_index if item[2] == "replace"],
            key=lambda item: item[1],
        )
        insert_after_items = sorted(
            [item for item in same_index if item[2] != "replace"],
            key=lambda item: item[1],
            reverse=True,
        )
        # Only the first replace stays a true replace. Later replaces are
        # downgraded to insert_after during rendering, so they must be sent in
        # reverse order to preserve the user's original step order.
        if replace_items:
            ordered.append(replace_items[0][3])
        ordered.extend(item[3] for item in insert_after_items)
        ordered.extend(item[3] for item in reversed(replace_items[1:]))
    return ordered


def display_order_template_mappings(raw_mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    annotated: List[Tuple[int, int, int, Dict[str, Any]]] = []
    for seq, mp in enumerate(raw_mappings):
        idx = int(mp["index"])
        source_order = int(mp.get("source_order", seq))
        mode = (mp.get("mode") or "insert_after").strip() or "insert_after"
        mode_rank = 0 if mode == "replace" else 1
        annotated.append((idx, mode_rank, source_order, mp))
    annotated.sort(key=lambda item: (item[0], item[1], item[2]))
    return [item[3] for item in annotated]


def render_template_with_mappings(
    template_docx: str,
    output_docx: str,
    mappings: List[Dict[str, Any]],
    parsed_results: List[Dict[str, Any]],
) -> Tuple[str, List[Tuple[str, Dict[str, Any]]]]:
    """
    Apply template mappings to a DOCX using docxtpl subdocs.

    mappings: list of dicts containing index/mode/content_docx_path/content_text.
    parsed_results: output from parse_template_paragraphs (index/display/text).
    """
    parts = read_docx_parts(template_docx)
    if "word/document.xml" not in parts:
        raise ValueError("模板缺少 word/document.xml")

    doc_xml = parts["word/document.xml"]

    meta: Dict[int, Tuple[str, str]] = {}
    for r in parsed_results:
        try:
            meta[int(r["index"])] = (r.get("display", ""), r.get("text", ""))
        except Exception:
            continue

    used_vars = set()
    var_records: List[Tuple[str, Dict[str, Any]]] = []
    replaced_once = set()

    mappings_sorted = order_template_mappings(mappings)
    for mp in mappings_sorted:
        idx = int(mp["index"])
        display, text = meta.get(idx, ("", ""))
        var_name = make_var_name(display, text)

        base = var_name
        suffix = 2
        while var_name in used_vars:
            var_name = f"{base}_{suffix}"
            suffix += 1
        used_vars.add(var_name)

        mode = (mp.get("mode") or "insert_after").strip()
        if mode == "replace":
            if idx in replaced_once:
                mode = "insert_after"
            else:
                replaced_once.add(idx)
        doc_xml = add_docxtpl_var_at_paragraph_index(
            doc_xml,
            idx,
            var_name,
            mode,
            as_subdoc=bool(mp.get("content_docx_path")),
        )
        var_records.append((var_name, mp))

    parts["word/document.xml"] = doc_xml

    tmp_tpl = os.path.join(os.path.dirname(output_docx), f"__tpl_{os.path.basename(output_docx)}")
    _write_zip(parts, tmp_tpl)

    tpl = DocxTemplate(tmp_tpl)

    ctx: Dict[str, Any] = {}
    for var_name, mp in var_records:
        cdoc = mp.get("content_docx_path")
        ctext = (mp.get("content_text") or "").strip()
        if cdoc:
            ctx[var_name] = tpl.new_subdoc(cdoc)
        else:
            ctx[var_name] = ctext

    tpl.render(ctx)
    tpl.save(output_docx)

    try:
        os.remove(tmp_tpl)
    except OSError:
        pass

    return output_docx, var_records
