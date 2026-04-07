import json
import re
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from lxml import etree

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": W_NS}


def qn(tag: str) -> str:
    prefix, local = tag.split(":")
    if prefix != "w":
        raise ValueError("qn() only supports w: namespace")
    return f"{{{W_NS}}}{local}"


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


@dataclass
class TocEntry:
    order: int
    level: int | None
    number: str
    title: str
    page: str
    anchor: str
    raw_text: str
    style_id: str
    style_name: str

    def to_dict(self) -> dict:
        return asdict(self)


def _read_docx_parts(docx_path: str | Path) -> dict[str, bytes]:
    with zipfile.ZipFile(docx_path, "r") as zin:
        return {name: zin.read(name) for name in zin.namelist()}


def _build_style_name_map(styles_xml: bytes | None) -> dict[str, str]:
    if not styles_xml:
        return {}
    root = etree.fromstring(styles_xml)
    style_names: dict[str, str] = {}
    for style in root.xpath(".//w:style[@w:type='paragraph']", namespaces=NS):
        style_id = style.get(qn("w:styleId")) or ""
        if not style_id:
            continue
        name = style.find("w:name", namespaces=NS)
        style_names[style_id] = (name.get(qn("w:val")) or "") if name is not None else ""
    return style_names


def _get_style_id(paragraph: etree._Element) -> str:
    p_pr = paragraph.find("w:pPr", namespaces=NS)
    if p_pr is None:
        return ""
    p_style = p_pr.find("w:pStyle", namespaces=NS)
    return (p_style.get(qn("w:val")) or "") if p_style is not None else ""


def _paragraph_text_with_tabs(paragraph: etree._Element) -> str:
    parts: list[str] = []
    for node in paragraph.iter():
        if node.tag == qn("w:t"):
            parts.append(node.text or "")
        elif node.tag == qn("w:tab"):
            parts.append("\t")
        elif node.tag == qn("w:br"):
            parts.append("\n")
    return "".join(parts)


def _is_toc_paragraph(paragraph: etree._Element, *, style_name: str = "", style_id: str = "") -> bool:
    style_tokens = " ".join(filter(None, [style_id, style_name])).upper()
    if "TOC" in style_tokens or "目錄" in style_tokens or "目录" in style_tokens:
        return True

    instr = "".join(paragraph.xpath(".//w:instrText/text()", namespaces=NS)).upper()
    if "TOC" in instr:
        return True

    anchors = paragraph.xpath(".//w:hyperlink/@w:anchor", namespaces=NS)
    if any((anchor or "").startswith("_Toc") for anchor in anchors):
        return True

    leaders = paragraph.xpath(".//w:tab/@w:leader", namespaces=NS)
    if any(leader in ("dot", "middleDot") for leader in leaders):
        return True

    return False


def _extract_level(style_name: str, style_id: str, number_text: str) -> int | None:
    for token in (style_name, style_id):
        match = re.search(r"TOC\s*([0-9]+)", token, re.IGNORECASE)
        if match:
            return int(match.group(1))

    number = normalize_text(number_text).rstrip(".．")
    if number and re.fullmatch(r"\d+(?:[\.．]\d+)*", number):
        return len(re.split(r"[\.．]", number))
    return None


def _split_toc_text_and_page(raw_text: str) -> tuple[str, str]:
    text = raw_text.replace("\r", "").replace("\n", " ")
    chunks = [normalize_text(chunk) for chunk in text.split("\t")]
    chunks = [chunk for chunk in chunks if chunk]
    if not chunks:
        return "", ""
    if len(chunks) >= 2 and re.fullmatch(r"\d+", chunks[-1]):
        return normalize_text(" ".join(chunks[:-1])), chunks[-1]
    merged = normalize_text(" ".join(chunks))
    match = re.match(r"^(.*?)(\d{1,5})$", merged)
    if match and not re.search(r"\d", match.group(1).rsplit(" ", 1)[-1]):
        return normalize_text(match.group(1)), match.group(2)
    return merged, ""


def _split_number_and_title(text: str) -> tuple[str, str]:
    cleaned = normalize_text(text)
    if not cleaned:
        return "", ""
    match = re.match(r"^(\d+(?:[\.．]\d+)*)(?:\s*)(.+)?$", cleaned)
    if match:
        number = match.group(1).replace("．", ".")
        title = normalize_text(match.group(2) or "")
        return number, title
    return "", cleaned


def extract_toc_entries_from_parts(parts: dict[str, bytes]) -> list[TocEntry]:
    document_xml = parts.get("word/document.xml")
    if not document_xml:
        raise RuntimeError("DOCX 中找不到 word/document.xml")

    style_name_map = _build_style_name_map(parts.get("word/styles.xml"))
    root = etree.fromstring(document_xml)
    paragraphs = root.xpath(".//w:body//w:p", namespaces=NS)

    entries: list[TocEntry] = []
    for idx, paragraph in enumerate(paragraphs, start=1):
        style_id = _get_style_id(paragraph)
        style_name = style_name_map.get(style_id, "")
        if not _is_toc_paragraph(paragraph, style_name=style_name, style_id=style_id):
            continue

        raw_text = _paragraph_text_with_tabs(paragraph)
        heading_text, page = _split_toc_text_and_page(raw_text)
        number, title = _split_number_and_title(heading_text)
        if not title and not number:
            continue

        anchors = paragraph.xpath(".//w:hyperlink/@w:anchor", namespaces=NS)
        anchor = next((anchor for anchor in anchors if anchor), "")
        entries.append(
            TocEntry(
                order=idx,
                level=_extract_level(style_name, style_id, number),
                number=number,
                title=title,
                page=page,
                anchor=anchor,
                raw_text=normalize_text(raw_text.replace("\t", " ")),
                style_id=style_id,
                style_name=style_name,
            )
        )
    return entries


def extract_toc_entries(docx_path: str | Path) -> list[TocEntry]:
    return extract_toc_entries_from_parts(_read_docx_parts(docx_path))


def toc_entries_as_json(entries: Iterable[TocEntry], *, pretty: bool = True) -> str:
    payload = [entry.to_dict() for entry in entries]
    return json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None)
