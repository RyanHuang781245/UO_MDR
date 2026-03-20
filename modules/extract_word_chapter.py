import re
import shutil
import zipfile
from copy import deepcopy
from lxml import etree

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
XML_NS = "http://www.w3.org/XML/1998/namespace"
NS = {"w": W_NS}

def qn(tag: str) -> str:
    prefix, local = tag.split(":")
    if prefix != "w":
        raise ValueError("qn() only supports w: namespace")
    return f"{{{W_NS}}}{local}"

def normalize_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())

_SUBTITLE_TOGGLE_TOKENS = {
    "收合",
    "展開",
    "展开",
    "收起",
    "collapse",
    "expand",
    "collapsed",
    "expanded",
    "▼",
    "▲",
    "▶",
    "◀",
    "▸",
    "▾",
}

def _strip_toggle_tokens(text: str) -> str:
    cleaned = normalize_text(text)
    if not cleaned:
        return ""
    for token in _SUBTITLE_TOGGLE_TOKENS:
        cleaned = re.sub(
            rf"\s*[\(\（\[\【<「『]\s*{re.escape(token)}\s*[\)\）\]\】>」』]\s*$",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(rf"\s*{re.escape(token)}\s*$", "", cleaned, flags=re.IGNORECASE)
    return normalize_text(cleaned)

def _is_toggle_token_text(text: str) -> bool:
    cleaned = normalize_text(text)
    return bool(cleaned) and _strip_toggle_tokens(cleaned) == ""

def _run_is_bold(rPr: etree._Element | None) -> bool:
    if rPr is None:
        return False
    b = rPr.find("w:b", namespaces=NS)
    if b is None:
        return False
    val = (b.get(qn("w:val")) or "").strip().lower()
    return val in ("", "1", "true")

def get_all_text(node: etree._Element) -> str:
    return "".join(node.xpath(".//w:t/text()", namespaces=NS)).strip()

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

def _parse_number_parts(number_text: str) -> list[int]:
    parts: list[int] = []
    for token in (number_text or "").split("."):
        token = token.strip()
        if not token:
            continue
        if not token.isdigit():
            return []
        parts.append(int(token))
    return parts

def _parse_number_tokens(number_text: str) -> list[str]:
    normalized = normalize_text(number_text or "").strip().rstrip(".．")
    if not normalized:
        return []
    tokens = [t.strip() for t in re.split(r"[\.．]", normalized) if t.strip()]
    if not tokens or any(not t.isdigit() for t in tokens):
        return []
    return tokens

def _build_heading_number_prefix_regex(number_text: str) -> str | None:
    tokens = _parse_number_tokens(number_text)
    if not tokens:
        return None
    joined = r"[\.．]".join(re.escape(t) for t in tokens)
    return rf"^{joined}(?!\d)"

def _extract_leading_number_parts(paragraph_text: str) -> list[int]:
    txt = normalize_text(paragraph_text)
    match = re.match(r"^(\d+(?:[\.．]\d+)*)", txt)
    if not match:
        return []
    if match.end() < len(txt) and txt[match.end()].isdigit():
        return []
    return _parse_number_parts(match.group(1).replace("．", "."))

def _is_plain_text_number_boundary(start_parts: list[int], candidate_parts: list[int]) -> bool:
    if not start_parts or not candidate_parts:
        return False
    if len(candidate_parts) > len(start_parts):
        return False
    if candidate_parts == start_parts:
        return False

    prefix_len = len(candidate_parts) - 1
    if prefix_len and candidate_parts[:prefix_len] != start_parts[:prefix_len]:
        return False

    return candidate_parts[-1] != start_parts[prefix_len]

def _is_heading_paragraph_xml(
    p: etree._Element,
    style_outline: dict[str, int],
    style_based: dict[str, str],
) -> bool:
    style = (get_pStyle(p) or "").lower()
    if style.startswith("heading"):
        return True
    return get_effective_outline_level(p, style_outline, style_based) is not None

def _set_paragraph_numpr(
    p: etree._Element,
    *,
    num_id: int,
    ilvl: int,
) -> None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None:
        pPr = etree.Element(qn("w:pPr"))
        p.insert(0, pPr)
    numPr = pPr.find("w:numPr", namespaces=NS)
    if numPr is None:
        numPr = etree.SubElement(pPr, qn("w:numPr"))
    ilvl_node = numPr.find("w:ilvl", namespaces=NS)
    if ilvl_node is None:
        ilvl_node = etree.SubElement(numPr, qn("w:ilvl"))
    ilvl_node.set(qn("w:val"), str(max(0, min(ilvl, 8))))
    num_id_node = numPr.find("w:numId", namespaces=NS)
    if num_id_node is None:
        num_id_node = etree.SubElement(numPr, qn("w:numId"))
    num_id_node.set(qn("w:val"), str(num_id))

def _remove_paragraph_numpr(p: etree._Element) -> None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None:
        return
    numPr = pPr.find("w:numPr", namespaces=NS)
    if numPr is not None:
        pPr.remove(numPr)

def _strip_paragraph_indents(p: etree._Element) -> None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None:
        return
    tabs = pPr.find("w:tabs", namespaces=NS)
    if tabs is not None:
        pPr.remove(tabs)
    ind = pPr.find("w:ind", namespaces=NS)
    if ind is None:
        ind = etree.SubElement(pPr, qn("w:ind"))
    ind.set(qn("w:left"), "0")
    ind.set(qn("w:right"), "0")
    ind.set(qn("w:firstLine"), "0")
    ind.set(qn("w:hanging"), "0")

def _prepend_paragraph_text(p: etree._Element, prefix: str) -> None:
    if not prefix:
        return
    run = etree.Element(qn("w:r"))
    t = etree.SubElement(run, qn("w:t"))
    if prefix.endswith(" "):
        t.set(f"{{{XML_NS}}}space", "preserve")
    t.text = prefix
    pPr = p.find("w:pPr", namespaces=NS)
    insert_pos = 1 if pPr is not None else 0
    p.insert(insert_pos, run)
    _strip_leading_whitespace_after_prefix(p, run)

def _strip_leading_whitespace_after_prefix(p: etree._Element, prefix_run: etree._Element) -> None:
    found_prefix = False
    for child in list(p):
        if child is prefix_run:
            found_prefix = True
            continue
        if not found_prefix:
            continue
        if child.tag != qn("w:r"):
            continue
        # Remove leading tabs in the first run after prefix
        for tab in list(child.findall("w:tab", namespaces=NS)):
            child.remove(tab)
        # Strip leading whitespace from the first text node after prefix
        t = child.find("w:t", namespaces=NS)
        if t is not None and t.text is not None:
            new_text = t.text.lstrip()
            t.text = new_text
            break

def _get_paragraph_numpr(p: etree._Element) -> tuple[int, int] | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None:
        return None
    numPr = pPr.find("w:numPr", namespaces=NS)
    if numPr is None:
        return None
    num_id_node = numPr.find("w:numId", namespaces=NS)
    ilvl_node = numPr.find("w:ilvl", namespaces=NS)
    if num_id_node is None or ilvl_node is None:
        return None
    num_id_raw = (num_id_node.get(qn("w:val")) or "").strip()
    ilvl_raw = (ilvl_node.get(qn("w:val")) or "").strip()
    if not num_id_raw.isdigit() or not ilvl_raw.isdigit():
        return None
    return int(num_id_raw), int(ilvl_raw)

def _format_number_token(value: int, num_fmt: str) -> str:
    fmt = (num_fmt or "decimal").strip().lower()
    if fmt in {"decimal", "decimalzero"}:
        return str(value)
    if fmt == "upperroman":
        return _to_roman_number(value).upper()
    if fmt == "lowerroman":
        return _to_roman_number(value).lower()
    if fmt == "upperletter":
        return _to_alpha_number(value).upper()
    if fmt == "lowerletter":
        return _to_alpha_number(value).lower()
    return str(value)

def _to_roman_number(value: int) -> str:
    if value <= 0:
        return str(value)
    pairs = [
        (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
        (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
        (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
    ]
    parts: list[str] = []
    current = value
    for arabic, roman in pairs:
        while current >= arabic:
            parts.append(roman)
            current -= arabic
    return "".join(parts)

def _to_alpha_number(value: int) -> str:
    if value <= 0:
        return str(value)
    chars: list[str] = []
    current = value
    while current > 0:
        current -= 1
        chars.append(chr(ord("A") + (current % 26)))
        current //= 26
    return "".join(reversed(chars))

def _load_numbering_maps(numbering_xml: bytes | None) -> tuple[dict[int, dict], dict[int, dict]]:
    if not numbering_xml:
        return {}, {}

    root = etree.fromstring(numbering_xml)
    abstract_map: dict[int, dict] = {}
    num_map: dict[int, dict] = {}

    for abstract in root.findall("w:abstractNum", namespaces=NS):
        abs_raw = (abstract.get(qn("w:abstractNumId")) or "").strip()
        if not abs_raw.isdigit():
            continue
        abs_id = int(abs_raw)
        levels: dict[int, dict] = {}
        for lvl in abstract.findall("w:lvl", namespaces=NS):
            ilvl_raw = (lvl.get(qn("w:ilvl")) or "").strip()
            if not ilvl_raw.isdigit():
                continue
            ilvl = int(ilvl_raw)
            start_node = lvl.find("w:start", namespaces=NS)
            num_fmt_node = lvl.find("w:numFmt", namespaces=NS)
            lvl_text_node = lvl.find("w:lvlText", namespaces=NS)
            levels[ilvl] = {
                "start": int((start_node.get(qn("w:val")) or "1")) if start_node is not None else 1,
                "num_fmt": (num_fmt_node.get(qn("w:val")) or "decimal") if num_fmt_node is not None else "decimal",
                "lvl_text": (lvl_text_node.get(qn("w:val")) or "") if lvl_text_node is not None else "",
            }
        abstract_map[abs_id] = levels

    for num in root.findall("w:num", namespaces=NS):
        num_raw = (num.get(qn("w:numId")) or "").strip()
        if not num_raw.isdigit():
            continue
        num_id = int(num_raw)
        abs_ref = num.find("w:abstractNumId", namespaces=NS)
        abs_raw = (abs_ref.get(qn("w:val")) or "").strip() if abs_ref is not None else ""
        if not abs_raw.isdigit():
            continue
        overrides: dict[int, dict] = {}
        for override in num.findall("w:lvlOverride", namespaces=NS):
            ilvl_raw = (override.get(qn("w:ilvl")) or "").strip()
            if not ilvl_raw.isdigit():
                continue
            ilvl = int(ilvl_raw)
            start_override = override.find("w:startOverride", namespaces=NS)
            override_data: dict[str, int | str] = {}
            if start_override is not None:
                start_raw = (start_override.get(qn("w:val")) or "").strip()
                if start_raw.isdigit():
                    override_data["start"] = int(start_raw)
            lvl = override.find("w:lvl", namespaces=NS)
            if lvl is not None:
                num_fmt_node = lvl.find("w:numFmt", namespaces=NS)
                lvl_text_node = lvl.find("w:lvlText", namespaces=NS)
                if num_fmt_node is not None:
                    override_data["num_fmt"] = num_fmt_node.get(qn("w:val")) or "decimal"
                if lvl_text_node is not None:
                    override_data["lvl_text"] = lvl_text_node.get(qn("w:val")) or ""
            if override_data:
                overrides[ilvl] = override_data
        num_map[num_id] = {
            "abstract_id": int(abs_raw),
            "overrides": overrides,
        }

    return abstract_map, num_map

def _resolve_numbering_level(
    abstract_map: dict[int, dict],
    num_map: dict[int, dict],
    *,
    num_id: int,
    ilvl: int,
) -> dict:
    num_info = num_map.get(num_id) or {}
    abstract_id = num_info.get("abstract_id")
    base = {}
    if isinstance(abstract_id, int):
        base = dict((abstract_map.get(abstract_id) or {}).get(ilvl) or {})
    override = dict((num_info.get("overrides") or {}).get(ilvl) or {})
    if override:
        base.update(override)
    if "start" not in base:
        base["start"] = 1
    if "num_fmt" not in base:
        base["num_fmt"] = "decimal"
    if "lvl_text" not in base:
        base["lvl_text"] = f"%{ilvl + 1}"
    return base

def _render_numbering_prefix(
    p: etree._Element,
    content_children: list[etree._Element],
    numbering_xml: bytes | None,
) -> str:
    numpr = _get_paragraph_numpr(p)
    if numpr is None:
        return ""

    target_num_id, target_ilvl = numpr
    abstract_map, num_map = _load_numbering_maps(numbering_xml)
    states: dict[int, dict[int, int]] = {}

    for block in content_children:
        for para in iter_paragraphs(block):
            para_numpr = _get_paragraph_numpr(para)
            if para_numpr is None:
                continue
            num_id, ilvl = para_numpr
            level_info = _resolve_numbering_level(abstract_map, num_map, num_id=num_id, ilvl=ilvl)
            counters = states.setdefault(num_id, {})

            for depth in list(counters.keys()):
                if depth > ilvl:
                    del counters[depth]

            start_value = int(level_info.get("start") or 1)
            counters[ilvl] = counters.get(ilvl, start_value - 1) + 1

            lvl_text = str(level_info.get("lvl_text") or f"%{ilvl + 1}")
            for token_idx in range(1, 10):
                token = f"%{token_idx}"
                if token not in lvl_text:
                    continue
                current_level = token_idx - 1
                current_info = _resolve_numbering_level(
                    abstract_map,
                    num_map,
                    num_id=num_id,
                    ilvl=current_level,
                )
                current_value = counters.get(current_level)
                if current_value is None:
                    current_value = int(current_info.get("start") or 1)
                lvl_text = lvl_text.replace(
                    token,
                    _format_number_token(current_value, str(current_info.get("num_fmt") or "decimal")),
                )

            if para is p and num_id == target_num_id and ilvl == target_ilvl:
                return normalize_text(lvl_text)

    return ""

def materialize_paragraph_numpr_as_text(
    p: etree._Element | None,
    content_children: list[etree._Element],
    numbering_xml: bytes | None,
) -> bool:
    if p is None or p.tag != qn("w:p"):
        return False
    if _get_paragraph_numpr(p) is None:
        return False

    prefix = _render_numbering_prefix(p, content_children, numbering_xml)
    _remove_paragraph_numpr(p)
    _strip_paragraph_indents(p)

    if not prefix:
        return True

    paragraph_text = normalize_text(get_all_text(p))
    normalized_prefix = normalize_text(prefix)
    if paragraph_text.startswith(normalized_prefix):
        return True
    if not prefix.endswith(" "):
        prefix = f"{prefix} "
    _prepend_paragraph_text(p, prefix)
    return True

def normalize_paragraph_to_plain_text_run(
    p: etree._Element | None,
    *,
    prefer_following_text_run: bool = False,
) -> None:
    if p is None or p.tag != qn("w:p"):
        return

    original_children = list(p)
    line_texts = _paragraph_line_texts(p)
    if not line_texts:
        text = normalize_text(get_all_text(p))
        line_texts = [text] if text else []

    pPr = p.find("w:pPr", namespaces=NS)
    text_runs = []
    for child in original_children:
        if child.tag != qn("w:r"):
            continue
        run_text = "".join(child.xpath(".//w:t/text()", namespaces=NS))
        if run_text and normalize_text(run_text):
            text_runs.append(child)

    style_source = None
    if prefer_following_text_run and len(text_runs) >= 2:
        style_source = text_runs[1]
    elif text_runs:
        style_source = text_runs[0]

    for child in original_children:
        if child is pPr:
            continue
        p.remove(child)

    if not line_texts:
        return

    run = etree.Element(qn("w:r"))
    if style_source is not None:
        r_pr = style_source.find("w:rPr", namespaces=NS)
        if r_pr is not None:
            run.append(deepcopy(r_pr))
    for idx, line in enumerate(line_texts):
        if idx > 0:
            etree.SubElement(run, qn("w:br"))
        text_node = etree.SubElement(run, qn("w:t"))
        if line.startswith(" ") or line.endswith(" "):
            text_node.set(f"{{{XML_NS}}}space", "preserve")
        text_node.text = line
    p.append(run)

def _ensure_numbering_instance(
    file_map: dict[str, bytes],
    start_parts: list[int],
) -> int:
    numbering_name = "word/numbering.xml"
    if numbering_name in file_map:
        root = etree.fromstring(file_map[numbering_name])
    else:
        root = etree.Element(qn("w:numbering"), nsmap={"w": W_NS})

    abs_ids: list[int] = []
    num_ids: list[int] = []
    for node in root.findall("w:abstractNum", namespaces=NS):
        val = node.get(qn("w:abstractNumId"))
        if val and val.isdigit():
            abs_ids.append(int(val))
    for node in root.findall("w:num", namespaces=NS):
        val = node.get(qn("w:numId"))
        if val and val.isdigit():
            num_ids.append(int(val))

    new_abs_id = (max(abs_ids) + 1) if abs_ids else 1
    new_num_id = (max(num_ids) + 1) if num_ids else 1

    abs_node = etree.SubElement(root, qn("w:abstractNum"))
    abs_node.set(qn("w:abstractNumId"), str(new_abs_id))
    multi = etree.SubElement(abs_node, qn("w:multiLevelType"))
    multi.set(qn("w:val"), "multilevel")

    for ilvl in range(9):
        lvl = etree.SubElement(abs_node, qn("w:lvl"))
        lvl.set(qn("w:ilvl"), str(ilvl))
        start = etree.SubElement(lvl, qn("w:start"))
        start_val = start_parts[ilvl] if ilvl < len(start_parts) else 1
        start.set(qn("w:val"), str(max(1, int(start_val))))
        num_fmt = etree.SubElement(lvl, qn("w:numFmt"))
        num_fmt.set(qn("w:val"), "decimal")
        lvl_text = etree.SubElement(lvl, qn("w:lvlText"))
        lvl_text.set(qn("w:val"), ".".join(f"%{k}" for k in range(1, ilvl + 2)))
        lvl_jc = etree.SubElement(lvl, qn("w:lvlJc"))
        lvl_jc.set(qn("w:val"), "left")
        # Prevent numbering glyphs from inheriting italic formatting
        # from paragraph/run direct formatting.
        lvl_rpr = etree.SubElement(lvl, qn("w:rPr"))
        italic = etree.SubElement(lvl_rpr, qn("w:i"))
        italic.set(qn("w:val"), "0")
        italic_cs = etree.SubElement(lvl_rpr, qn("w:iCs"))
        italic_cs.set(qn("w:val"), "0")

    num = etree.SubElement(root, qn("w:num"))
    num.set(qn("w:numId"), str(new_num_id))
    abs_ref = etree.SubElement(num, qn("w:abstractNumId"))
    abs_ref.set(qn("w:val"), str(new_abs_id))

    file_map[numbering_name] = etree.tostring(
        root, xml_declaration=True, encoding="UTF-8", standalone="yes"
    )
    return new_num_id

def _materialize_heading_numbering(
    section_children: list[etree._Element],
    start_number: str,
    start_heading_text: str | None,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    num_id: int,
) -> None:
    start_parts = _parse_number_parts(start_number)
    if not start_parts:
        return

    base_ilvl: int | None = None
    offset: int | None = None
    initialized = False

    for block in section_children:
        for p in iter_paragraphs(block):
            if not _is_heading_paragraph_xml(p, style_outline, style_based):
                continue
            level = get_ilvl(p)
            if level is None:
                level = get_effective_outline_level(p, style_outline, style_based)
            if level is None:
                continue

            if not initialized:
                base_ilvl = level
                offset = len(start_parts) - (base_ilvl + 1)
                if offset < 0:
                    return
                _set_paragraph_numpr(
                    p, num_id=num_id, ilvl=(len(start_parts) - 1)
                )
                initialized = True
                continue

            expected_len = level + 1 + (offset or 0)
            if expected_len <= 0:
                continue
            _set_paragraph_numpr(
                p, num_id=num_id, ilvl=(expected_len - 1)
            )

    if initialized:
        return

    # Fallback: if no heading styles were detected, try to apply numbering
    # to the paragraph whose text matches the requested heading text.
    target_text = normalize_text(start_heading_text or "")
    if not target_text:
        return
    num_pattern = _build_heading_number_prefix_regex(start_number)
    for block in section_children:
        for p in iter_paragraphs(block):
            text = normalize_text(get_all_text(p))
            if not text:
                continue
            if num_pattern and re.match(num_pattern, text):
                continue
            if text == target_text:
                _set_paragraph_numpr(p, num_id=num_id, ilvl=(len(start_parts) - 1))
                return

def _force_plain_heading_numbers(
    section_children: list[etree._Element],
    start_number: str,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    *,
    max_level: int = 2,
) -> None:
    start_parts = _parse_number_parts(start_number)
    if not start_parts:
        return
    base_level: int | None = None
    offset: int | None = None
    counters: list[int] = []

    for block in section_children:
        for p in iter_paragraphs(block):
            level = get_effective_outline_level(p, style_outline, style_based)
            if level is None:
                level = get_ilvl(p)
            if level is None:
                continue
            if level > max_level:
                continue

            if base_level is None:
                base_level = level
                offset = len(start_parts) - (base_level + 1)
                if offset < 0:
                    offset = 0
                expected_len = base_level + 1 + offset
                if len(start_parts) >= expected_len:
                    counters = start_parts[:expected_len]
                else:
                    counters = start_parts + [1] * (expected_len - len(start_parts))
            else:
                expected_len = level + 1 + (offset or 0)
                if expected_len <= 0:
                    continue
                if not counters:
                    counters = [1] * expected_len
                if expected_len > len(counters):
                    counters.extend([1] * (expected_len - len(counters)))
                else:
                    counters = counters[:expected_len]
                    counters[-1] += 1

            number_text = ".".join(str(n) for n in counters)
            paragraph_text = normalize_text(get_all_text(p))
            existing_parts = _extract_leading_number_parts(paragraph_text)
            _remove_paragraph_numpr(p)
            _strip_paragraph_indents(p)
            if existing_parts:
                if len(existing_parts) >= len(counters):
                    counters = existing_parts[: len(counters)]
                continue
            if not number_text:
                continue
            if expected_len == 1:
                if re.match(rf"^{re.escape(number_text)}(?:\\.|\\s)(?!\\d)", paragraph_text):
                    continue
                prefix = f"{number_text}. "
            else:
                if re.match(rf"^{re.escape(number_text)}(?!\\d)", paragraph_text):
                    continue
                prefix = f"{number_text} "
            _prepend_paragraph_text(p, prefix)

def _apply_plain_heading_to_target(
    section_children: list[etree._Element],
    start_number: str,
    start_heading_text: str,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    file_map: dict[str, bytes],
) -> None:
    num = normalize_text(start_number or "").rstrip(".")
    title = normalize_text(start_heading_text or "")
    num_pattern = _build_heading_number_prefix_regex(num) if num else None

    target_para = None
    for block in section_children:
        for p in iter_paragraphs(block):
            txt = normalize_text(get_all_text(p))
            if not txt:
                continue
            if title and txt == title:
                target_para = p
                break
            if num_pattern and re.match(num_pattern, txt):
                if not title or title in txt:
                    target_para = p
                    break
            if title and title in txt:
                target_para = p
                break
        if target_para is not None:
            break

    if target_para is None:
        return

    style_id = get_pStyle(target_para)
    if style_id and file_map.get("word/styles.xml"):
        updated_styles, new_style_id = _ensure_style_without_numpr(file_map["word/styles.xml"], style_id)
        file_map["word/styles.xml"] = updated_styles
        if new_style_id != style_id:
            pPr = target_para.find("w:pPr", namespaces=NS)
            if pPr is None:
                pPr = etree.Element(qn("w:pPr"))
                target_para.insert(0, pPr)
            pStyle = pPr.find("w:pStyle", namespaces=NS)
            if pStyle is None:
                pStyle = etree.SubElement(pPr, qn("w:pStyle"))
            pStyle.set(qn("w:val"), new_style_id)

    _remove_paragraph_numpr(target_para)
    _strip_paragraph_indents(target_para)

    if not num:
        return
    paragraph_text = normalize_text(get_all_text(target_para))
    if re.match(r"^\d+(?:[\.．]\d+)*(?:[\.．]|\s|$)", paragraph_text):
        return
    level_parts = num.split(".")
    if len(level_parts) == 1:
        if re.match(rf"^{re.escape(num)}(?:\\.|\\s)(?!\\d)", paragraph_text):
            return
        prefix = f"{num}. "
    else:
        if re.match(rf"^{re.escape(num)}(?!\\d)", paragraph_text):
            return
        prefix = f"{num} "
    _prepend_paragraph_text(target_para, prefix)

def iter_paragraphs(block: etree._Element):
    if block.tag == qn("w:p"):
        yield block
    for p in block.xpath(".//w:p", namespaces=NS):
        yield p

def _paragraph_line_texts(p: etree._Element) -> list[str]:
    lines = [""]
    for node in p.iter():
        if node.tag in (qn("w:br"), qn("w:cr")):
            lines.append("")
            continue
        if node.tag == qn("w:t"):
            lines[-1] += node.text or ""
    return [normalize_text(line) for line in lines if line is not None]

def _truncate_paragraph_at_break_index(p: etree._Element, break_index: int) -> bool:
    breaks = p.xpath(".//w:br | .//w:cr", namespaces=NS)
    if break_index < 0 or break_index >= len(breaks):
        return False
    br = breaks[break_index]
    carrier = br
    while carrier is not None and carrier.getparent() is not p:
        carrier = carrier.getparent()
    if carrier is None:
        return False

    # Trim within carrier: remove break node and anything after it
    node_in_carrier = br
    while node_in_carrier is not None and node_in_carrier.getparent() is not carrier:
        node_in_carrier = node_in_carrier.getparent()
    if node_in_carrier is None:
        return False

    # If the break is inside a run, remove break and following nodes in that run
    if node_in_carrier.tag == qn("w:r"):
        run_children = list(node_in_carrier)
        if br in run_children:
            idx = run_children.index(br)
            for child in run_children[idx:]:
                node_in_carrier.remove(child)

    # Remove following siblings within the carrier
    carrier_children = list(carrier)
    if node_in_carrier in carrier_children:
        idx = carrier_children.index(node_in_carrier)
        for child in carrier_children[idx + 1:]:
            carrier.remove(child)

    # Remove following siblings of the carrier in paragraph
    p_children = list(p)
    if carrier in p_children:
        idx = p_children.index(carrier)
        for child in p_children[idx + 1:]:
            p.remove(child)
    return True

def _trim_inline_heading_breaks(
    section_children: list[etree._Element],
    start_number: str,
) -> list[etree._Element]:
    start_parts = _parse_number_parts(start_number)
    for block_idx, block in enumerate(section_children):
        for p in iter_paragraphs(block):
            lines = _paragraph_line_texts(p)
            if len(lines) <= 1:
                continue
            for line_idx in range(1, len(lines)):
                line = _strip_toggle_tokens(lines[line_idx])
                parts = _extract_leading_number_parts(line)
                if not parts:
                    continue
                if start_parts:
                    if parts == start_parts:
                        continue
                    if len(parts) > len(start_parts):
                        continue
                if _truncate_paragraph_at_break_index(p, line_idx - 1):
                    return section_children[: block_idx + 1]
    return section_children

# ---------- TOC 偵測（多特徵） ----------
def is_toc_paragraph(p: etree._Element) -> bool:
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


_STYLE_NAME_HEADING_RE = re.compile(r"^\s*(\d+(?:[\.．]\d+)+)")


def _extract_style_heading_rank(style_name: str | None) -> int | None:
    name = normalize_text(style_name or "")
    if not name:
        return None
    match = _STYLE_NAME_HEADING_RE.match(name)
    if not match:
        return None
    parts = _parse_number_parts(match.group(1).replace("．", "."))
    if not parts:
        return None
    return max(0, len(parts) - 1)


def build_style_heading_rank_map(styles_xml: bytes) -> dict[str, int]:
    style_rank: dict[str, int] = {}

    root = etree.fromstring(styles_xml)
    for st in root.xpath(".//w:style[@w:type='paragraph']", namespaces=NS):
        sid = st.get(qn("w:styleId"))
        if not sid:
            continue
        name = st.find("w:name", namespaces=NS)
        rank = _extract_style_heading_rank(name.get(qn("w:val")) if name is not None else None)
        if rank is not None:
            style_rank[sid] = rank

    return style_rank

def _ensure_style_without_numpr(
    styles_xml: bytes,
    style_id: str,
) -> tuple[bytes, str]:
    """Clone the paragraph style and remove numPr from the clone."""
    if not styles_xml or not style_id:
        return styles_xml, style_id
    root = etree.fromstring(styles_xml)
    style = root.find(f".//w:style[@w:styleId='{style_id}']", namespaces=NS)
    if style is None:
        return styles_xml, style_id
    pPr = style.find("w:pPr", namespaces=NS)
    if pPr is None:
        return styles_xml, style_id
    numPr = pPr.find("w:numPr", namespaces=NS)
    if numPr is None:
        return styles_xml, style_id

    base_id = f"{style_id}_NoNum"
    new_id = base_id
    existing_ids = {
        (st.get(qn("w:styleId")) or "") for st in root.findall(".//w:style", namespaces=NS)
    }
    idx = 1
    while new_id in existing_ids:
        idx += 1
        new_id = f"{base_id}{idx}"

    clone = deepcopy(style)
    clone.set(qn("w:styleId"), new_id)
    name = clone.find("w:name", namespaces=NS)
    if name is not None:
        name.set(qn("w:val"), f"{name.get(qn('w:val'))} (NoNum)")
    clone_pPr = clone.find("w:pPr", namespaces=NS)
    if clone_pPr is not None:
        clone_numPr = clone_pPr.find("w:numPr", namespaces=NS)
        if clone_numPr is not None:
            clone_pPr.remove(clone_numPr)
    root.append(clone)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone="yes"), new_id

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


def resolve_style_heading_rank(
    style_id: str | None,
    style_heading_rank: dict[str, int] | None,
    style_based: dict[str, str],
) -> int | None:
    if not style_id or not style_heading_rank:
        return None
    cur = style_id
    best: int | None = None
    for _ in range(30):
        if cur in style_heading_rank:
            rank = style_heading_rank[cur]
            best = rank if best is None else max(best, rank)
        cur = style_based.get(cur)
        if not cur:
            break
    return best

def get_effective_outline_level(p: etree._Element, style_outline: dict[str, int], style_based: dict[str, str]) -> int | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is not None:
        ol = pPr.find("w:outlineLvl", namespaces=NS)
        if ol is not None:
            v = ol.get(qn("w:val"))
            if v and v.isdigit():
                return int(v)
    return resolve_style_outline(get_pStyle(p), style_outline, style_based)


def get_effective_heading_depth(
    p: etree._Element,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    style_heading_rank: dict[str, int] | None = None,
) -> int | None:
    outline_depth = get_effective_outline_level(p, style_outline, style_based)
    style_depth = resolve_style_heading_rank(get_pStyle(p), style_heading_rank, style_based)
    depths = [depth for depth in (outline_depth, style_depth) if depth is not None]
    if not depths:
        return None
    return max(depths)

# ---------- 表格排除：只在判斷小標題起/終點時，忽略表格內的段落 ----------
def is_inside_table(p: etree._Element) -> bool:
    return bool(p.xpath("ancestor::w:tbl", namespaces=NS))

def iter_paragraphs_no_table(block: etree._Element):
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

# ---------- 小標題（inline subtitle）判斷：Normal + 文字 run 全粗體 ----------
def is_inline_subtitle_xml(p: etree._Element) -> bool:
    style = get_pStyle(p)
    if style not in (None, "Normal"):
        return False

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
        if _is_toggle_token_text(txt):
            continue
        has_text = True

        rPr = r.find("w:rPr", namespaces=NS)
        if not _run_is_bold(rPr):
            return False

    return has_text


_CAPTION_LIKE_SUBTITLE_RE = re.compile(
    r"^\s*(?:figure|table)\s+\d+(?:[\.:]\d+)*[\.:]?(?:\s|$)",
    re.IGNORECASE,
)


def classify_subheading_candidate_xml(p: etree._Element) -> tuple[str | None, str | None]:
    if is_inline_subtitle_xml(p):
        return "inline", None

    if not is_all_bold_paragraph(p):
        return None, None

    text = _strip_toggle_tokens(get_all_text(p))
    if not text:
        return None, None

    style = (get_pStyle(p) or "").strip()
    style_key = style.lower()
    if not style_key or style_key == "normal":
        return None, None
    if style_key.startswith("heading") or style_key.startswith("toc"):
        return None, None

    if _CAPTION_LIKE_SUBTITLE_RE.match(text):
        return None, None

    return "styled_bold", style_key


def is_subheading_candidate_xml(p: etree._Element) -> bool:
    kind, _ = classify_subheading_candidate_xml(p)
    return kind is not None

def match_subheading(p: etree._Element, subheading_text: str, strict: bool = True) -> bool:
    txt = _strip_toggle_tokens(get_all_text(p))
    target = _strip_toggle_tokens(subheading_text)
    if strict:
        return txt == target
    return target in txt


def match_heading_by_number_and_title(
    paragraph_text: str,
    heading_number: str | None = None,
    heading_title: str | None = None,
) -> bool:
    """Match heading text using chapter number and/or title (test2.py style)."""
    txt = normalize_text(paragraph_text)
    num = normalize_text(heading_number or "").rstrip(".")
    title = normalize_text(heading_title or "")
    number_pattern = _build_heading_number_prefix_regex(num)

    if num:
        if (number_pattern and re.match(number_pattern, txt)) or (
            not number_pattern and re.match(rf"^{re.escape(num)}(?!\d)", txt)
        ):
            if title:
                return title in txt
            return True
        # Some documents put chapter number in TOC line, but real heading line is title-only.
        if title and (txt == title or txt.endswith(title)):
            return True
        return False

    if title:
        return txt == title or title in txt

    return False

# ---------- 章節範圍定位（outlineLvl 優先，ilvl 備援），支援 ignore_toc ----------
def find_section_range_children(
    body_children: list[etree._Element],
    start_heading_text: str,
    start_number: str,
    style_outline: dict[str, int],
    style_based: dict[str, str],
    style_heading_rank: dict[str, int] | None = None,
    explicit_end_title: str | None = None,
    explicit_end_number: str | None = None,
    include_end_chapter: bool = True,
    ignore_toc: bool = True,
) -> tuple[int, int]:
    start_idx = None
    start_heading_depth = None
    start_style = None
    start_ilvl = None
    fallback_start_idx = None
    fallback_start_heading_depth = None
    fallback_start_style = None
    fallback_start_ilvl = None
    fallback_start_number_parts: list[int] | None = None
    start_heading_is_number = bool(_parse_number_tokens(start_heading_text))
    start_number_parts = _parse_number_parts(normalize_text(start_number).replace("．", ".").rstrip("."))
    requested_start_depth = (len(start_number_parts) - 1) if start_number_parts else None
    start_num_pattern = _build_heading_number_prefix_regex(start_number)
    found_kind = None  # "exact" | "toc"

    # ---- 找起點 ----
    for i, block in enumerate(body_children):
        for p in iter_paragraphs(block):
            if ignore_toc and is_toc_paragraph(p):
                continue

            txt = normalize_text(get_all_text(p))

            if txt == start_heading_text:
                cand_heading_depth = get_effective_heading_depth(
                    p,
                    style_outline,
                    style_based,
                    style_heading_rank,
                )
                cand_style = get_pStyle(p)
                cand_ilvl = get_ilvl(p)
                cand_number_parts = _extract_leading_number_parts(txt) or start_number_parts

                if cand_heading_depth is not None or cand_ilvl is not None:
                    start_idx = i
                    start_heading_depth = cand_heading_depth
                    start_style = cand_style
                    start_ilvl = cand_ilvl
                    start_number_parts = cand_number_parts
                    found_kind = "exact"
                    break

                if fallback_start_idx is None:
                    fallback_start_idx = i
                    fallback_start_heading_depth = cand_heading_depth
                    fallback_start_style = cand_style
                    fallback_start_ilvl = cand_ilvl
                    fallback_start_number_parts = cand_number_parts

            if start_num_pattern and re.match(start_num_pattern, txt) and (
                start_heading_is_number or start_heading_text in txt
            ):
                if found_kind is None:
                    start_idx = i
                    start_heading_depth = get_effective_heading_depth(
                        p,
                        style_outline,
                        style_based,
                        style_heading_rank,
                    )
                    start_style = get_pStyle(p)
                    start_ilvl = get_ilvl(p)
                    start_number_parts = _extract_leading_number_parts(txt) or start_number_parts
                    found_kind = "toc"

        if found_kind == "exact":
            break

    if start_idx is None:
        if fallback_start_idx is not None:
            start_idx = fallback_start_idx
            start_heading_depth = fallback_start_heading_depth
            start_style = fallback_start_style
            start_ilvl = fallback_start_ilvl
            start_number_parts = fallback_start_number_parts or start_number_parts
            found_kind = "exact"
        else:
            raise RuntimeError(f"找不到章節起點：{start_number} / {start_heading_text}")

    if requested_start_depth is not None:
        if start_heading_depth is None:
            start_heading_depth = requested_start_depth
        else:
            start_heading_depth = max(start_heading_depth, requested_start_depth)

    has_explicit_end = bool((explicit_end_title or "").strip() or (explicit_end_number or "").strip())
    explicit_end_parts = _parse_number_parts(normalize_text(explicit_end_number or "").replace("．", ".").rstrip("."))
    explicit_end_depth = (len(explicit_end_parts) - 1) if explicit_end_parts else None

    # ---- 找終點 ----
    end_idx = len(body_children)
    for j in range(start_idx + 1, len(body_children)):
        block = body_children[j]
        for p in iter_paragraphs(block):
            if ignore_toc and is_toc_paragraph(p):
                continue

            txt = normalize_text(get_all_text(p))

            if has_explicit_end:
                if match_heading_by_number_and_title(
                    paragraph_text=txt,
                    heading_number=explicit_end_number,
                    heading_title=explicit_end_title,
                ):
                    if include_end_chapter:
                        end_heading_depth = get_effective_heading_depth(
                            p,
                            style_outline,
                            style_based,
                            style_heading_rank,
                        )
                        if explicit_end_depth is not None:
                            end_heading_depth = (
                                explicit_end_depth
                                if end_heading_depth is None
                                else max(end_heading_depth, explicit_end_depth)
                            )
                        end_style = get_pStyle(p)
                        end_ilvl = get_ilvl(p)
                        for k in range(j + 1, len(body_children)):
                            next_block = body_children[k]
                            for next_p in iter_paragraphs(next_block):
                                if ignore_toc and is_toc_paragraph(next_p):
                                    continue
                                next_heading_depth = get_effective_heading_depth(
                                    next_p,
                                    style_outline,
                                    style_based,
                                    style_heading_rank,
                                )
                                if (
                                    end_heading_depth is not None
                                    and next_heading_depth is not None
                                    and next_heading_depth <= end_heading_depth
                                ):
                                    return start_idx, k
                                if end_heading_depth is None:
                                    next_style = get_pStyle(next_p)
                                    next_ilvl = get_ilvl(next_p)
                                    if (
                                        end_style is not None
                                        and next_style == end_style
                                        and end_ilvl is not None
                                        and next_ilvl is not None
                                        and next_ilvl <= end_ilvl
                                    ):
                                        return start_idx, k
                        return start_idx, len(body_children)
                    return start_idx, j

            # If explicit end is provided, do not stop at the next same-level heading.
            if has_explicit_end:
                continue

            heading_depth = get_effective_heading_depth(
                p,
                style_outline,
                style_based,
                style_heading_rank,
            )
            if start_heading_depth is not None and heading_depth is not None and heading_depth <= start_heading_depth:
                return start_idx, j

            if start_heading_depth is None:
                style = get_pStyle(p)
                ilvl = get_ilvl(p)
                if (
                    start_style is not None
                    and style == start_style
                    and start_ilvl is not None
                    and ilvl is not None
                    and ilvl <= start_ilvl
                ):
                    return start_idx, j
                if start_ilvl is None and not is_inside_table(p):
                    candidate_parts = _extract_leading_number_parts(txt)
                    if _is_plain_text_number_boundary(start_number_parts, candidate_parts):
                        return start_idx, j

    if has_explicit_end:
        raise RuntimeError(
            f"找不到指定終點章節：number={explicit_end_number or ''} title={explicit_end_title or ''}"
        )

    return start_idx, end_idx

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
        if _is_toggle_token_text(txt):
            continue

        has_text = True
        rPr = r.find("w:rPr", namespaces=NS)
        if not _run_is_bold(rPr):
            return False

    return has_text

def has_body_text_after_candidate(
    section_children: list[etree._Element],
    candidate_block_index: int,
    lookahead_blocks: int = 6,
) -> bool:
    """
    從 candidate_block_index 後面開始，往後找最多 lookahead_blocks 個 block，
    只看「非表格」段落且略過空段落：
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
    start_kind: str | None = None
    start_style_key: str | None = None

    # 1) 找小標題起點：優先「inline subtitle + 文字匹配」（且排除表格內段落）
    for i, block in enumerate(section_children):
        for p in iter_paragraphs_no_table(block):
            kind, style_key = classify_subheading_candidate_xml(p)
            if kind and match_subheading(p, subheading_text, strict=strict_match):
                sub_start = i
                start_kind = kind
                start_style_key = style_key
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
            # 候選：沿用起點的小標題型別，避免深層零件標題誤切成同層子標題
            kind, style_key = classify_subheading_candidate_xml(p)
            if not kind:
                continue

            if start_kind == "styled_bold":
                if kind == "styled_bold" and style_key != start_style_key:
                    continue
            elif kind != "inline":
                continue

            # 新增：確認它後面真的跟著正文（不是一路粗體）
            if has_body_text_after_candidate(section_children, j, lookahead_blocks=1):
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

def extract_section_docx_xml(
    input_docx: str,
    output_docx: str,
    start_heading_text: str,
    start_number: str,
    explicit_end_title: str | None = None,
    explicit_end_number: str | None = None,
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
    style_heading_rank = build_style_heading_rank_map(file_map["word/styles.xml"])

    root = etree.fromstring(file_map["word/document.xml"])
    body = root.find("w:body", namespaces=NS)
    if body is None:
        raise RuntimeError("document.xml 找不到 w:body")

    children = list(body)

    # 保留 body 最後的 sectPr（頁面設定）
    sectPr = None
    if children and children[-1].tag == qn("w:sectPr"):
        sectPr = children[-1]
        content_children = children[:-1]
    else:
        content_children = children

    # 1) 先擷取大章節範圍
    start_idx, end_idx = find_section_range_children(
        content_children,
        start_heading_text=start_heading_text,
        start_number=start_number,
        style_outline=style_outline,
        style_based=style_based,
        style_heading_rank=style_heading_rank,
        explicit_end_title=explicit_end_title,
        explicit_end_number=explicit_end_number,
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
    else:
        kept_section = _trim_inline_heading_breaks(kept_section, start_number)

    # 3) 重建 body：只保留（章節 or 小標題）內容 + sectPr
    # Keep heading as true numbering (not plain text) and force start values
    # from original chapter number to avoid restart after merge.
    start_parts = _parse_number_parts(start_number)
    if not subheading_text:
        _apply_plain_heading_to_target(
            kept_section,
            start_number=start_number,
            start_heading_text=start_heading_text,
            style_outline=style_outline,
            style_based=style_based,
            file_map=file_map,
        )

    for ch in list(body):
        body.remove(ch)

    for ch in kept_section:
        body.append(deepcopy(ch))

    if sectPr is not None:
        body.append(deepcopy(sectPr))

    # 4) 忽略頁首/頁尾
    if ignore_header_footer:
        remove_all_header_footer_references(root)

    new_doc_xml = etree.tostring(
        root, xml_declaration=True, encoding="UTF-8", standalone="yes"
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
        input_docx=r"C:\Users\ne025\Desktop\Test_File\Section 1_Device Description_v1_knee.docx",
        output_docx=r"Extract_1.1.1_General_description_knee.docx",
        start_heading_text="Accessories not included but necessary for use",
        start_number="1.1.3",
        # explicit_end_title="Accessories not included but necessary for use",
        # explicit_end_number="1.1.3",
        ignore_header_footer=True,
        ignore_toc=True,
        subheading_strict_match=True,
        subheading_debug=False,
    )
    print("Done")
