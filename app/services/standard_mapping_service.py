from __future__ import annotations

import copy
import html
import os
import re
import tempfile
import zipfile
from difflib import SequenceMatcher

from lxml import etree
from openpyxl import load_workbook

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": W_NS}
XML_NS = "http://www.w3.org/XML/1998/namespace"

EXCEL_STANDARD_COL_INDEX = 5
ISO_FAMILY_SHEETS = ["ISO", "BS-EN-DIN(歐洲國家標準)"]
RED_COLOR = "FF0000"
BLUE_COLOR = "2563EB"
DEFAULT_ISO_PRIORITY = ("BS EN ISO", "EN ISO", "ISO")
DEFAULT_PREFER_LATEST_EN_VARIANTS = True

HEADER_ALIASES = {
    "Standards": [
        "Standards",
        "Standard",
        "Standard No",
        "Standard Number",
    ],
    "Issued Year": [
        "Issued Year",
        "Issue Year",
        "Year",
    ],
    "EU Harmonised Standards under MDR 2017/745 (YES/NO)": [
        "EU Harmonised Standards",
        "EU Harmonised Standards under MDR",
        "EU Harmonised Standards under MDR 2017/745 (YES/NO)",
        "EU Harmonised Standards under MDR 2017/745(YES/NO)",
        "EU Harmonized Standards under MDR 2017/745 (YES/NO)",
        "EU Harmonized Standards under MDR 2017/745(YES/NO)",
        "EU Harmonised Standards under MDR 2017/745",
        "EU Harmonized Standards under MDR 2017/745",
    ],
    "Title": [
        "Title",
        "Standard Title",
    ],
}

HEADER_KEYWORDS = {
    "Standards": {"STANDARD"},
    "Issued Year": {"ISSUED", "YEAR"},
    "EU Harmonised Standards under MDR 2017/745 (YES/NO)": {"EU", "HARMONISED", "MDR", "2017/745"},
    "Title": {"TITLE"},
}


def qn(tag: str) -> str:
    prefix, local = tag.split(":")
    if prefix != "w":
        raise ValueError("qn() only supports w: namespace")
    return f"{{{W_NS}}}{local}"


def normalize_text(text: str) -> str:
    if text is None:
        return ""
    text = str(text).replace("\n", " ").replace("\r", " ")
    return re.sub(r"\s+", " ", text).strip()


def normalize_key_for_search(text: str) -> str:
    text = normalize_text(text).upper()
    text = text.replace("–", "-").replace("—", "-")
    text = text.replace("：", ":").replace("／", "/")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*([\(\)])\s*", r"\1", text)
    return text


def compact_key_for_search(text: str) -> str:
    normalized = normalize_key_for_search(text)
    return re.sub(r"[^A-Z0-9]+", "", normalized)


def header_matches_target(header_text: str, target_name: str) -> bool:
    normalized_header = normalize_key_for_search(header_text)
    compact_header = compact_key_for_search(header_text)
    aliases = HEADER_ALIASES.get(target_name, [target_name])

    for alias in aliases:
        if normalized_header == normalize_key_for_search(alias):
            return True
        if compact_header == compact_key_for_search(alias):
            return True

    keywords = HEADER_KEYWORDS.get(target_name)
    if keywords:
        header_words = set(re.findall(r"[A-Z0-9/]+", normalized_header))
        if keywords.issubset(header_words):
            return True

    return False


def detect_search_family(standard_name: str) -> str | None:
    s = normalize_key_for_search(standard_name)
    if not s:
        return None
    if "ASTM" in s:
        return "ASTM"
    if re.match(r"^(?:BS\s+EN\s+ISO|DIN\s+EN\s+ISO|EN\s+ISO|ISO)\b", s):
        return "ISO_FAMILY"
    if re.match(r"^(?:BS\s+EN|DIN\s+EN|EN)\b", s):
        return "EN_FAMILY"
    return None


def classify_standard_level(std_no: str) -> tuple[str, int]:
    s = normalize_key_for_search(std_no)
    if s.startswith("BS EN ISO "):
        return "BS EN ISO", 3
    if s.startswith("DIN EN ISO ") or s.startswith("EN ISO "):
        return "EN ISO", 2
    if s.startswith("ISO "):
        return "ISO", 1
    return "OTHER", 0


def normalize_iso_priority(priority_order: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not priority_order:
        return DEFAULT_ISO_PRIORITY
    normalized = []
    seen = set()
    for item in priority_order:
        value = normalize_key_for_search(item)
        if value == "BS EN ISO":
            label = "BS EN ISO"
        elif value == "EN ISO":
            label = "EN ISO"
        elif value == "ISO":
            label = "ISO"
        else:
            continue
        if label not in seen:
            normalized.append(label)
            seen.add(label)
    for label in DEFAULT_ISO_PRIORITY:
        if label not in seen:
            normalized.append(label)
    return tuple(normalized[:3])


def extract_iso_family_core(std_no: str) -> str:
    s = normalize_key_for_search(std_no)
    if not s:
        return ""
    s = re.sub(r":\s*(19\d{2}|20\d{2}).*$", "", s).strip()
    return re.sub(r"^(?:BS\s+EN\s+ISO|DIN\s+EN\s+ISO|EN\s+ISO|ISO)\s+", "", s).strip()


def extract_display_standard_no(std_no: str) -> str:
    family = detect_search_family(std_no)
    s = normalize_text(std_no)
    if family in {"ISO_FAMILY", "EN_FAMILY"}:
        s = s.replace("：", ":")
        return re.sub(r"\s*:\s*(19\d{2}|20\d{2}).*$", "", s).strip()
    if family == "ASTM":
        return re.sub(r"\s*-\s*(\d{2}[A-Z]?)(?!\d).*$", "", s).strip()
    return s


def make_row_key(table_index: int, row_index: int) -> str:
    return f"table-{table_index}-row-{row_index}"


def make_candidate_id(candidate: dict) -> str:
    return "|".join([
        normalize_text(candidate.get("sheet_name", "")),
        str(candidate.get("excel_row_index", "")),
        normalize_text(candidate.get("matched_standard_no", "")),
    ])


def get_all_text(node: etree._Element) -> str:
    texts = node.xpath(".//w:t/text()", namespaces=NS)
    return normalize_text("".join(texts))


def get_grid_span(tc: etree._Element) -> int:
    vals = tc.xpath("./w:tcPr/w:gridSpan/@w:val", namespaces=NS)
    if vals:
        try:
            return int(vals[0])
        except Exception:
            return 1
    return 1


def ensure_cell_has_text_node(tc: etree._Element) -> etree._Element:
    t_nodes = tc.xpath(".//w:t", namespaces=NS)
    if t_nodes:
        return t_nodes[0]
    p = tc.find("w:p", namespaces=NS)
    if p is None:
        p = etree.SubElement(tc, qn("w:p"))
    r = p.find("w:r", namespaces=NS)
    if r is None:
        r = etree.SubElement(p, qn("w:r"))
    return etree.SubElement(r, qn("w:t"))


def merge_text_segments(segments: list[tuple[str, bool]]) -> list[tuple[str, bool]]:
    merged: list[tuple[str, bool]] = []
    for text, is_red in segments:
        if not text:
            continue
        if merged and merged[-1][1] == is_red:
            merged[-1] = (merged[-1][0] + text, is_red)
        else:
            merged.append((text, is_red))
    return merged or [("", False)]


def build_diff_segments(old_text: str, new_text: str) -> list[tuple[str, bool]]:
    old_text = "" if old_text is None else str(old_text)
    new_text = "" if new_text is None else str(new_text)
    if old_text == new_text:
        return [(new_text, False)]
    if old_text and new_text.endswith(old_text):
        prefix = new_text[:-len(old_text)]
        prefix_core = prefix.rstrip()
        prefix_space = prefix[len(prefix_core):]
        return merge_text_segments([
            (prefix_core, True),
            (prefix_space, False),
            (old_text, False),
        ])
    matcher = SequenceMatcher(a=old_text, b=new_text)
    segments = []
    for tag, _, _, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            segments.append((new_text[j1:j2], False))
        elif tag in {"replace", "insert"}:
            segments.append((new_text[j1:j2], True))
    return merge_text_segments(segments)


def build_year_segments(old_text: str, new_text: str) -> list[tuple[str, bool]]:
    old_text = normalize_text(old_text)
    new_text = normalize_text(new_text)
    if old_text == new_text:
        return [(new_text, False)]
    if (
        re.fullmatch(r"(19|20)\d{2}", old_text)
        and re.fullmatch(r"(19|20)\d{2}", new_text)
        and old_text[:2] == new_text[:2]
    ):
        return merge_text_segments([(new_text[:2], False), (new_text[2:], True)])
    return build_diff_segments(old_text, new_text)


def get_first_run_properties(tc: etree._Element) -> etree._Element | None:
    rpr = tc.find(".//w:r/w:rPr", namespaces=NS)
    return copy.deepcopy(rpr) if rpr is not None else None


def set_run_color(run: etree._Element, color_hex: str):
    rpr = run.find("w:rPr", namespaces=NS)
    if rpr is None:
        rpr = etree.SubElement(run, qn("w:rPr"))
    color = rpr.find("w:color", namespaces=NS)
    if color is None:
        color = etree.SubElement(rpr, qn("w:color"))
    color.set(qn("w:val"), color_hex)


def rebuild_cell_with_segments(tc: etree._Element, segments: list[tuple[str, bool]]):
    segments = merge_text_segments(segments)
    p_nodes = tc.findall("w:p", namespaces=NS)
    paragraph = p_nodes[0] if p_nodes else etree.SubElement(tc, qn("w:p"))
    template_rpr = get_first_run_properties(tc)

    for child in list(tc):
        if child.tag == qn("w:tcPr") or child is paragraph:
            continue
        tc.remove(child)

    for child in list(paragraph):
        if child.tag != qn("w:pPr"):
            paragraph.remove(child)

    for text, is_red in segments:
        run = etree.SubElement(paragraph, qn("w:r"))
        if template_rpr is not None:
            run.append(copy.deepcopy(template_rpr))
        if is_red:
            set_run_color(run, RED_COLOR)
        t = etree.SubElement(run, qn("w:t"))
        if text != text.strip() or "  " in text:
            t.set(f"{{{XML_NS}}}space", "preserve")
        t.text = text


def rebuild_cell_with_single_color(tc: etree._Element, text: str, color_hex: str):
    rebuild_cell_with_segments(tc, [(normalize_text(text), False)])
    paragraph = tc.find("w:p", namespaces=NS)
    if paragraph is None:
        return
    first_run = paragraph.find("w:r", namespaces=NS)
    if first_run is not None:
        set_run_color(first_run, color_hex)


def get_run_color(run: etree._Element) -> str:
    vals = run.xpath("./w:rPr/w:color/@w:val", namespaces=NS)
    if not vals:
        return ""
    color = normalize_text(vals[0]).upper()
    return "" if color == "AUTO" else color


def extract_cell_runs(tc: etree._Element) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    paragraphs = tc.xpath("./w:p", namespaces=NS)
    for p_idx, paragraph in enumerate(paragraphs):
        if p_idx > 0:
            segments.append(("\n", ""))
        runs = paragraph.xpath("./w:r", namespaces=NS)
        for run in runs:
            text = "".join(run.xpath(".//w:t/text()", namespaces=NS))
            if text:
                segments.append((text, get_run_color(run)))
    if segments:
        return segments
    plain_text = get_all_text(tc)
    return [(plain_text, "")] if plain_text else []


def format_cell_runs_as_html(tc: etree._Element) -> str:
    parts = []
    for text, color in extract_cell_runs(tc):
        escaped = html.escape(text).replace("\n", "<br>")
        if color:
            parts.append(f'<span style="color: #{color.lower()};">{escaped}</span>')
        else:
            parts.append(escaped)
    return "".join(parts) or "&nbsp;"


def unzip_docx(docx_path: str, extract_dir: str):
    with zipfile.ZipFile(docx_path, "r") as archive:
        archive.extractall(extract_dir)


def zip_to_docx(folder_path: str, output_docx_path: str):
    with zipfile.ZipFile(output_docx_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for root, _, files in os.walk(folder_path):
            for file in files:
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, folder_path)
                archive.write(abs_path, rel_path)


def detect_sheet_type(standard_name: str) -> str | None:
    s = normalize_key_for_search(standard_name)
    if "ASTM" in s:
        return "ASTM"
    if "EN ISO" in s or "BS EN" in s or re.search(r"\bEN\b", s):
        return "BS-EN-DIN(歐洲國家標準)"
    if re.search(r"\bISO\b", s):
        return "ISO"
    return None


def extract_latest_year_from_en_iso_style(std_no: str) -> int | None:
    years = re.findall(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)", normalize_text(std_no))
    if not years:
        return None
    return max(int(y) for y in years)


def astm_two_digit_to_full_year(two_digit: int) -> int:
    return 2000 + two_digit if 0 <= two_digit <= 49 else 1900 + two_digit


def normalize_astm_standard_text(std_no: str) -> str:
    text = normalize_text(std_no).upper()
    text = text.replace("–", "-").replace("—", "-")
    return re.sub(r"\s*-\s*", "-", text)


def extract_latest_year_from_astm_style(std_no: str) -> int | None:
    normalized = normalize_astm_standard_text(std_no)
    matches = re.findall(r"-(\d{2}[A-Z]?)(?!\d)", normalized)
    if not matches:
        return None
    years = [astm_two_digit_to_full_year(int(re.match(r"\d{2}", x).group(0))) for x in matches]
    return max(years) if years else None


def extract_standard_match_key(std_no: str, sheet_name: str) -> str:
    family = detect_search_family(std_no)
    s = normalize_key_for_search(std_no)
    if not s:
        return ""
    if family == "ISO_FAMILY":
        return extract_iso_family_core(std_no)
    if family == "ASTM":
        return re.sub(r"\s*-\s*(\d{2}[A-Z]?)(?!\d).*$", "", s).strip()
    s = re.sub(r":\s*(19\d{2}|20\d{2}).*$", "", s).strip()
    if sheet_name == "BS-EN-DIN(歐洲國家標準)":
        s = re.sub(r"^(?:BS|DIN)\s+(?=EN\b)", "", s).strip()
    return s


def build_sheet_records(ws, std_col_index: int = EXCEL_STANDARD_COL_INDEX) -> list[dict]:
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    records = []
    for row_idx, row in enumerate(rows[1:], start=2):
        if row is None:
            continue
        values = list(row)
        std_val = values[std_col_index] if std_col_index < len(values) else None
        if std_val is None:
            continue
        std_val = normalize_text(std_val)
        if not std_val:
            continue
        standard_level, standard_level_rank = classify_standard_level(std_val)
        records.append({
            "sheet_name": ws.title,
            "excel_row_index": row_idx,
            "excel_col_letter": "F",
            "standard_no": std_val,
            "standard_match_key": extract_standard_match_key(std_val, ws.title),
            "search_family": detect_search_family(std_val),
            "standard_display_no": extract_display_standard_no(std_val),
            "standard_level": standard_level,
            "standard_level_rank": standard_level_rank,
        })
    return records


def load_excel_index(excel_path: str) -> dict:
    wb = load_workbook(excel_path, data_only=True)
    needed_sheets = ["BS-EN-DIN(歐洲國家標準)", "ISO", "ASTM"]
    index = {}
    for sheet_name in needed_sheets:
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Excel 缺少工作表: {sheet_name}")
        index[sheet_name] = build_sheet_records(wb[sheet_name], EXCEL_STANDARD_COL_INDEX)
    return index


def find_latest_year_from_excel(
    standard_name: str,
    excel_index: dict,
    iso_priority: list[str] | tuple[str, ...] | None = None,
    prefer_latest_en_variants: bool = DEFAULT_PREFER_LATEST_EN_VARIANTS,
) -> dict | None:
    family = detect_search_family(standard_name)
    if not family:
        return None
    query_key = extract_standard_match_key(standard_name, "")
    if not query_key:
        return None

    candidates = []
    normalized_iso_priority = normalize_iso_priority(iso_priority)

    if family == "ISO_FAMILY":
        target_sheets = ISO_FAMILY_SHEETS
    elif family == "ASTM":
        target_sheets = ["ASTM"]
    else:
        sheet_name = detect_sheet_type(standard_name)
        if not sheet_name:
            return None
        target_sheets = [sheet_name]

    for sheet_name in target_sheets:
        for rec in excel_index.get(sheet_name, []):
            if query_key != rec["standard_match_key"]:
                continue
            year = (
                extract_latest_year_from_astm_style(rec["standard_no"])
                if family == "ASTM"
                else extract_latest_year_from_en_iso_style(rec["standard_no"])
            )
            if year is None:
                continue
            candidates.append({
                "sheet_name": sheet_name,
                "excel_col_letter": "F",
                "excel_row_index": rec["excel_row_index"],
                "matched_standard_no": rec["standard_no"],
                "matched_display_standard_no": rec["standard_display_no"],
                "latest_year": year,
                "standard_level": rec["standard_level"],
                "standard_level_rank": rec["standard_level_rank"],
                "search_family": rec["search_family"],
                "decision": "kept",
                "decision_reason": "納入初始候選",
                "candidate_id": "",
            })

    if not candidates:
        return None

    all_candidates = [dict(item) for item in candidates]
    if family == "ISO_FAMILY":
        priority_index = {label: idx for idx, label in enumerate(normalized_iso_priority)}
        candidates = [x for x in all_candidates if x["standard_level"] in normalized_iso_priority]
        if not candidates:
            return None
        has_en_variant_pair = (
            any(x["standard_level"] == "EN ISO" for x in candidates)
            and any(x["standard_level"] == "BS EN ISO" for x in candidates)
        )
        for candidate in all_candidates:
            if candidate["standard_level"] not in normalized_iso_priority:
                candidate["decision"] = "excluded"
                candidate["decision_reason"] = "不屬於 EN ISO / BS EN ISO / ISO 候選，排除"
            elif prefer_latest_en_variants and has_en_variant_pair and candidate["standard_level"] in {"EN ISO", "BS EN ISO"}:
                candidate["decision_reason"] = "EN ISO 與 BS EN ISO 同時存在，依年份較新優先"
            elif prefer_latest_en_variants and has_en_variant_pair and candidate["standard_level"] == "ISO":
                candidate["decision_reason"] = "保留 ISO 候選，但 EN ISO 與 BS EN ISO 會先依年份比較"
            else:
                candidate["decision_reason"] = f"納入 { ' > '.join(normalized_iso_priority) } 優先級候選，進入排序"
    else:
        for candidate in all_candidates:
            candidate["decision_reason"] = "符合查詢條件，進入最終排序"

    if not candidates:
        return None

    has_en_variant_pair = (
        family == "ISO_FAMILY"
        and any(x["standard_level"] == "EN ISO" for x in candidates)
        and any(x["standard_level"] == "BS EN ISO" for x in candidates)
    )

    def candidate_sort_key(candidate: dict) -> tuple:
        latest_year = candidate["latest_year"]
        name_length = len(candidate["matched_standard_no"])
        if family == "ISO_FAMILY":
            priority_rank = -priority_index.get(candidate["standard_level"], 99)
            if prefer_latest_en_variants and has_en_variant_pair:
                if candidate["standard_level"] in {"EN ISO", "BS EN ISO"}:
                    return (2, latest_year, priority_rank, name_length)
                return (1, priority_rank, latest_year, name_length)
            return (1, priority_rank, latest_year, name_length)
        return (1, 0, latest_year, name_length)

    candidates.sort(
        key=candidate_sort_key,
        reverse=True,
    )
    selected = candidates[0]
    for candidate in candidates[1:]:
        if candidate.get("decision") != "excluded":
            candidate["decision"] = "kept"
            candidate["decision_reason"] = "通過篩選，但排序結果未被選用"
    selected["decision"] = "selected"
    if family == "ISO_FAMILY" and prefer_latest_en_variants and has_en_variant_pair:
        selected["decision_reason"] = "最終採用：EN ISO 與 BS EN ISO 同時存在，依年份較新選中"
    else:
        selected["decision_reason"] = "最終採用：依優先級與年份排序後選中"

    for candidate in all_candidates:
        candidate["candidate_id"] = make_candidate_id(candidate)
    ordered_candidates = sorted(
        all_candidates,
        key=lambda x: (
            1 if x.get("decision") == "selected" else 0,
            1 if x.get("decision") == "kept" else 0,
            *candidate_sort_key(x),
        ),
        reverse=True,
    )
    result = dict(selected)
    result["all_candidates"] = ordered_candidates
    result["selected_candidate_id"] = make_candidate_id(selected)
    result["auto_selected_candidate_id"] = make_candidate_id(selected)
    result["iso_priority"] = list(normalized_iso_priority)
    result["prefer_latest_en_variants"] = prefer_latest_en_variants
    return result


def apply_candidate_override(match_info: dict, override_candidate_id: str | None) -> dict:
    result = copy.deepcopy(match_info)
    candidates = result.get("all_candidates") or []
    if not candidates:
        return result
    auto_selected_id = result.get("auto_selected_candidate_id") or make_candidate_id(result)
    selected = None
    for candidate in candidates:
        candidate["candidate_id"] = candidate.get("candidate_id") or make_candidate_id(candidate)
        if candidate["candidate_id"] == override_candidate_id:
            selected = candidate
    if selected is None:
        result["selected_candidate_id"] = auto_selected_id
        result["auto_selected_candidate_id"] = auto_selected_id
        return result
    for candidate in candidates:
        if candidate["candidate_id"] == selected["candidate_id"]:
            candidate["decision"] = "selected"
            candidate["decision_reason"] = (
                "最終採用：依自動規則選中"
                if candidate["candidate_id"] == auto_selected_id
                else "人工覆寫：使用者改選此候選"
            )
        elif candidate["candidate_id"] == auto_selected_id and selected["candidate_id"] != auto_selected_id:
            candidate["decision"] = "kept"
            candidate["decision_reason"] = "自動規則原本選用，但已被人工覆寫"
        elif candidate.get("decision") != "excluded":
            candidate["decision"] = "kept"
            candidate["decision_reason"] = "通過篩選，但未被人工選用"
    for key, value in selected.items():
        if key != "all_candidates":
            result[key] = value
    result["all_candidates"] = candidates
    result["selected_candidate_id"] = selected["candidate_id"]
    result["auto_selected_candidate_id"] = auto_selected_id
    result["manually_overridden"] = selected["candidate_id"] != auto_selected_id
    return result


def parse_table_rows(tbl: etree._Element) -> list[list[dict]]:
    parsed_rows = []
    for tr_idx, tr in enumerate(tbl.xpath("./w:tr", namespaces=NS)):
        row_items = []
        logical_col = 0
        for tc_idx, tc in enumerate(tr.xpath("./w:tc", namespaces=NS)):
            span = get_grid_span(tc)
            row_items.append({
                "tr": tr,
                "tr_idx": tr_idx,
                "tc": tc,
                "tc_idx": tc_idx,
                "text": get_all_text(tc),
                "logical_col_start": logical_col,
                "logical_col_end": logical_col + span - 1,
                "grid_span": span,
            })
            logical_col += span
        parsed_rows.append(row_items)
    return parsed_rows


def expand_row_to_logical_cells(parsed_row: list[dict]) -> list[dict | None]:
    if not parsed_row:
        return []
    max_col = max(item["logical_col_end"] for item in parsed_row)
    expanded = [None] * (max_col + 1)
    for item in parsed_row:
        for col in range(item["logical_col_start"], item["logical_col_end"] + 1):
            expanded[col] = item
    return expanded


def find_header_row_and_map(parsed_rows: list[list[dict]]) -> tuple[int, dict] | tuple[None, None]:
    for row_idx, row in enumerate(parsed_rows):
        expanded = expand_row_to_logical_cells(row)
        texts = [x["text"] if x else "" for x in expanded]
        has_standards = any(header_matches_target(t, "Standards") for t in texts if t)
        has_issued_year = any(header_matches_target(t, "Issued Year") for t in texts if t)
        has_title = any(header_matches_target(t, "Title") for t in texts if t)
        has_harmonised = any(
            header_matches_target(t, "EU Harmonised Standards under MDR 2017/745 (YES/NO)")
            for t in texts
            if t
        )
        if has_standards and has_issued_year and has_title and has_harmonised:
            header_map = {}
            for item in row:
                cell_text = normalize_text(item["text"])
                if cell_text and cell_text not in header_map:
                    header_map[cell_text] = item["tc_idx"]
            return row_idx, header_map
    return None, None


def get_logical_col(header_map: dict, target_name: str) -> int | None:
    for header_name, logical_col in header_map.items():
        if header_matches_target(header_name, target_name):
            return logical_col
    return None


def parse_word_tables_for_update(document_xml_path: str) -> tuple[etree._ElementTree, list[dict]]:
    tree = etree.parse(document_xml_path)
    root = tree.getroot()
    tables = root.xpath(".//w:tbl", namespaces=NS)
    all_records = []
    for table_index, tbl in enumerate(tables):
        parsed_rows = parse_table_rows(tbl)
        header_row_idx, header_map = find_header_row_and_map(parsed_rows)
        if header_row_idx is None:
            continue
        standards_col = get_logical_col(header_map, "Standards")
        issued_year_col = get_logical_col(header_map, "Issued Year")
        harmonised_col = get_logical_col(header_map, "EU Harmonised Standards under MDR 2017/745 (YES/NO)")
        title_col = get_logical_col(header_map, "Title")
        if standards_col is None or issued_year_col is None:
            continue
        current_category = ""
        for row_idx in range(header_row_idx + 1, len(parsed_rows)):
            row_items = parsed_rows[row_idx]

            def get_text_at(tc_idx: int | None) -> str:
                if tc_idx is None or tc_idx >= len(row_items):
                    return ""
                return normalize_text(row_items[tc_idx]["text"])

            def get_tc_at(tc_idx: int | None):
                if tc_idx is None or tc_idx >= len(row_items):
                    return None
                return row_items[tc_idx]["tc"]

            standards_text = get_text_at(standards_col)
            issued_year_text = get_text_at(issued_year_col)
            harmonised_text = get_text_at(harmonised_col)
            title_text = get_text_at(title_col)
            nonempty_texts = [normalize_text(item["text"]) for item in row_items if normalize_text(item["text"])]

            is_single_cell_category = len(row_items) == 1 and len(nonempty_texts) == 1
            if is_single_cell_category:
                current_category = nonempty_texts[0]
                continue

            if standards_text == "" and issued_year_text == "" and harmonised_text == "" and title_text == "":
                if nonempty_texts:
                    current_category = nonempty_texts[0]
                continue

            if standards_text:
                all_records.append({
                    "table_index": table_index,
                    "row_index": row_idx,
                    "category": current_category,
                    "standards": standards_text,
                    "issued_year": issued_year_text,
                    "harmonised": harmonised_text,
                    "title": title_text,
                    "standards_tc": get_tc_at(standards_col),
                    "issued_year_tc": get_tc_at(issued_year_col),
                })
    return tree, all_records


def build_preview_tables(tree: etree._ElementTree, row_reference_map: dict) -> tuple[list[dict], dict]:
    root = tree.getroot()
    tables = root.xpath(".//w:tbl", namespaces=NS)
    preview_tables = []
    reference_payload: dict[str, dict] = {}
    table_number = 0

    for table_index, tbl in enumerate(tables):
        parsed_rows = parse_table_rows(tbl)
        header_row_idx, header_map = find_header_row_and_map(parsed_rows)
        if header_row_idx is None:
            continue
        standards_col = get_logical_col(header_map, "Standards")
        issued_year_col = get_logical_col(header_map, "Issued Year")
        harmonised_col = get_logical_col(header_map, "EU Harmonised Standards under MDR 2017/745 (YES/NO)")
        title_col = get_logical_col(header_map, "Title")
        header_row_items = parsed_rows[header_row_idx]
        header_labels = {
            item["tc_idx"]: normalize_text(item["text"])
            for item in header_row_items
        }
        fixed_columns = [
            ("Standards", standards_col),
            ("Issued Year", issued_year_col),
            ("EU Harmonised Standards under MDR 2017/745 (YES/NO)", harmonised_col),
            ("Title", title_col),
        ]
        table_number += 1
        rows_data = []

        for row_idx, row in enumerate(parsed_rows):
            def get_item(tc_idx: int | None):
                if tc_idx is None or tc_idx >= len(row):
                    return None
                return row[tc_idx]

            row_classes = []
            if row_idx == header_row_idx:
                row_classes.append("is-header")
            elif row_idx > header_row_idx:
                nonempty_texts = [normalize_text(item["text"]) for item in row if normalize_text(item["text"])]
                is_single_cell_category = len(row) == 1 and len(nonempty_texts) == 1
                standards_item = get_item(standards_col)
                issued_year_item = get_item(issued_year_col)
                harmonised_item = get_item(harmonised_col)
                title_item = get_item(title_col)
                standards_text = normalize_text(standards_item["text"]) if standards_item else ""
                issued_year_text = normalize_text(issued_year_item["text"]) if issued_year_item else ""
                harmonised_text = normalize_text(harmonised_item["text"]) if harmonised_item else ""
                title_text = normalize_text(title_item["text"]) if title_item else ""
                if is_single_cell_category or (
                    standards_text == ""
                    and issued_year_text == ""
                    and harmonised_text == ""
                    and title_text == ""
                    and nonempty_texts
                ):
                    row_classes.append("is-category")

            row_meta = row_reference_map.get((table_index, row_idx))
            if row_meta:
                row_classes.append(f"status-{row_meta.get('status', '').lower().replace(':', '-')}")

            cells = []
            if row_idx == header_row_idx:
                for label, tc_idx in fixed_columns:
                    item = get_item(tc_idx)
                    cells.append({
                        "tag": "th",
                        "colspan": 1,
                        "content_html": format_cell_runs_as_html(item["tc"]) if item is not None else html.escape(label),
                        "reference_key": None,
                        "header_text": label,
                    })
            elif "is-category" in row_classes:
                category_item = next((item for item in row if normalize_text(item["text"])), None)
                cells.append({
                    "tag": "td",
                    "colspan": 4,
                    "content_html": format_cell_runs_as_html(category_item["tc"]) if category_item is not None else "",
                    "reference_key": None,
                    "header_text": "",
                })
            else:
                for label, tc_idx in fixed_columns:
                    item = get_item(tc_idx)
                    reference_key = None
                    if row_meta and label == "Standards":
                        reference_key = f"{row_meta['row_key']}:standards"
                        reference_payload[reference_key] = {**row_meta, "field_label": "Standards"}
                    elif row_meta and label == "Issued Year":
                        reference_key = f"{row_meta['row_key']}:issued_year"
                        reference_payload[reference_key] = {**row_meta, "field_label": "Issued Year"}
                    cells.append({
                        "tag": "td",
                        "colspan": 1,
                        "content_html": format_cell_runs_as_html(item["tc"]) if item is not None else "",
                        "reference_key": reference_key,
                        "header_text": header_labels.get(tc_idx, label),
                    })

            rows_data.append({
                "classes": row_classes,
                "cells": cells,
                "row_key": row_meta.get("row_key", "") if row_meta else "",
            })

        preview_tables.append({
            "title": f"表格 {table_number}",
            "rows": rows_data,
        })

    return preview_tables, reference_payload


def process_document(
    word_path: str,
    excel_path: str,
    override_map: dict | None = None,
    output_path: str | None = None,
    iso_priority: list[str] | tuple[str, ...] | None = None,
    prefer_latest_en_variants: bool = DEFAULT_PREFER_LATEST_EN_VARIANTS,
) -> dict:
    override_map = override_map or {}
    normalized_iso_priority = normalize_iso_priority(iso_priority)
    excel_index = load_excel_index(excel_path)
    with tempfile.TemporaryDirectory() as tmpdir:
        unzip_docx(word_path, tmpdir)
        document_xml_path = os.path.join(tmpdir, "word", "document.xml")
        tree, records = parse_word_tables_for_update(document_xml_path)
        report = []
        updated_count = 0
        row_reference_map = {}

        for rec in records:
            row_key = make_row_key(rec["table_index"], rec["row_index"])
            standards = rec["standards"]
            word_year_text = normalize_text(rec["issued_year"])
            match_info = find_latest_year_from_excel(
                standards,
                excel_index,
                normalized_iso_priority,
                prefer_latest_en_variants=prefer_latest_en_variants,
            )
            if match_info:
                match_info = apply_candidate_override(match_info, override_map.get(row_key))

            if not match_info:
                if rec["standards_tc"] is not None:
                    rebuild_cell_with_single_color(rec["standards_tc"], standards, BLUE_COLOR)
                row_reference_map[(rec["table_index"], rec["row_index"])] = {
                    "row_key": row_key,
                    "status": "NOT_FOUND",
                    "sheet_name": "",
                    "excel_col_letter": "",
                    "excel_row_index": "",
                    "matched_standard_no": "",
                    "matched_display_standard_no": "",
                    "excel_year": "",
                    "word_standard": standards,
                    "word_year": word_year_text,
                    "all_candidates": [],
                    "selected_candidate_id": "",
                    "auto_selected_candidate_id": "",
                }
                report.append({
                    "status": "NOT_FOUND",
                    "category": rec["category"],
                    "standards": standards,
                    "word_year": word_year_text,
                    "excel_year": "",
                    "sheet_name": "",
                    "excel_col_letter": "",
                    "excel_row_index": "",
                    "matched_standard_no": "",
                })
                continue

            latest_year = str(match_info["latest_year"])
            matched_standard_no = match_info["matched_standard_no"]
            matched_display_standard_no = match_info["matched_display_standard_no"]
            standards_needs_update = normalize_key_for_search(standards) != normalize_key_for_search(matched_display_standard_no)
            year_needs_update = word_year_text != latest_year

            if standards_needs_update and rec["standards_tc"] is not None:
                rebuild_cell_with_segments(rec["standards_tc"], build_diff_segments(standards, matched_display_standard_no))
            if year_needs_update and rec["issued_year_tc"] is not None:
                rebuild_cell_with_segments(rec["issued_year_tc"], build_year_segments(word_year_text, latest_year))

            row_updated = standards_needs_update or year_needs_update
            if row_updated:
                updated_count += 1

            status = "UPDATED" if row_updated else "SAME_NO_UPDATE"
            row_reference_map[(rec["table_index"], rec["row_index"])] = {
                "row_key": row_key,
                "status": status,
                "sheet_name": match_info["sheet_name"],
                "excel_col_letter": match_info["excel_col_letter"],
                "excel_row_index": match_info["excel_row_index"],
                "matched_standard_no": matched_standard_no,
                "matched_display_standard_no": matched_display_standard_no,
                "excel_year": latest_year,
                "word_standard": standards,
                "word_year": word_year_text,
                "all_candidates": match_info.get("all_candidates", []),
                "selected_candidate_id": match_info.get("selected_candidate_id", ""),
                "auto_selected_candidate_id": match_info.get("auto_selected_candidate_id", ""),
            }
            report.append({
                "status": status,
                "category": rec["category"],
                "standards": standards,
                "word_year": word_year_text,
                "excel_year": latest_year,
                "sheet_name": match_info["sheet_name"],
                "excel_col_letter": match_info["excel_col_letter"],
                "excel_row_index": match_info["excel_row_index"],
                "matched_standard_no": matched_standard_no,
            })

        tree.write(document_xml_path, xml_declaration=True, encoding="UTF-8", standalone="yes")
        preview_tables, reference_payload = build_preview_tables(tree, row_reference_map)
        if output_path:
            zip_to_docx(tmpdir, output_path)
        return {
            "report": report,
            "updated_count": updated_count,
            "preview_tables": preview_tables,
            "reference_payload": reference_payload,
            "iso_priority": list(normalized_iso_priority),
            "prefer_latest_en_variants": prefer_latest_en_variants,
        }
