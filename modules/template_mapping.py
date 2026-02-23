import zipfile
import re
from collections import defaultdict
from lxml import etree

# Word XML 命名空間
W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": W_NS}

def qn(tag: str) -> str:
    """處理命名空間標籤"""
    prefix, local = tag.split(":")
    if prefix != "w":
        raise ValueError("qn() only supports w: namespace")
    return f"{{{W_NS}}}{local}"

# --- 輔助工具函式 ---

def normalize_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())

def get_p_text(p: etree._Element) -> str:
    return normalize_text("".join(p.xpath(".//w:t/text()", namespaces=NS)))

def get_pStyle(p: etree._Element) -> str | None:
    pPr = p.find("w:pPr", namespaces=NS)
    if pPr is None: return None
    ps = pPr.find("w:pStyle", namespaces=NS)
    return ps.get(qn("w:val")) if ps is not None else None

def get_numPr_from_pPr(pPr: etree._Element) -> tuple[int | None, int | None]:
    numPr = pPr.find("w:numPr", namespaces=NS)
    if numPr is None: return None, None
    numId_el = numPr.find("w:numId", namespaces=NS)
    ilvl_el = numPr.find("w:ilvl", namespaces=NS)
    numId = numId_el.get(qn("w:val")) if numId_el is not None else None
    ilvl = ilvl_el.get(qn("w:val")) if ilvl_el is not None else None
    return (int(numId) if numId and numId.isdigit() else None,
            int(ilvl) if ilvl and ilvl.isdigit() else None)

def read_docx_parts(docx_path: str) -> dict[str, bytes]:
    with zipfile.ZipFile(docx_path, "r") as zin:
        return {name: zin.read(name) for name in zin.namelist()}

# --- 核心解析邏輯 ---

def parse_styles_numPr(styles_xml: bytes | None):
    """解析樣式表，處理樣式繼承中的編號屬性"""
    if not styles_xml:
        return {}, {}
    root = etree.fromstring(styles_xml)
    style_based = {}
    style_numpr = {}
    for st in root.xpath(".//w:style[@w:type='paragraph']", namespaces=NS):
        sid = st.get(qn("w:styleId"))
        if not sid: continue
        based = st.find("w:basedOn", namespaces=NS)
        if based is not None:
            style_based[sid] = based.get(qn("w:val"))
        pPr = st.find("w:pPr", namespaces=NS)
        if pPr is not None:
            numId, ilvl = get_numPr_from_pPr(pPr)
            if numId is not None: style_numpr[sid] = (numId, ilvl)
    return style_based, style_numpr

def resolve_style_numPr(style_id, style_based, style_numpr):
    if not style_id: return None, None
    cur = style_id
    for _ in range(30): # 避免死迴圈
        if cur in style_numpr: return style_numpr[cur]
        cur = style_based.get(cur)
        if not cur: break
    return None, None

def parse_numbering(numbering_xml: bytes | None):
    """解析 numbering.xml，包含模板、實例與覆寫值"""
    if not numbering_xml:
        return {}, {}, defaultdict(dict)
    root = etree.fromstring(numbering_xml)
    num_to_abstract = {}
    num_id_overrides = defaultdict(dict)
    abstract_levels = {}

    # 1. 解析 w:num (編號實例)
    for num in root.xpath(".//w:num", namespaces=NS):
        nid = int(num.get(qn("w:numId")))
        abs_el = num.find("w:abstractNumId", namespaces=NS)
        if abs_el is not None:
            num_to_abstract[nid] = int(abs_el.get(qn("w:val")))
        # 關鍵：讀取 w:startOverride
        for override in num.xpath("./w:lvlOverride", namespaces=NS):
            ilvl = int(override.get(qn("w:ilvl")))
            s_ov = override.find("./w:startOverride", namespaces=NS)
            if s_ov is not None:
                num_id_overrides[nid][ilvl] = int(s_ov.get(qn("w:val")))

    # 2. 解析 w:abstractNum (編號模板)
    for absn in root.xpath(".//w:abstractNum", namespaces=NS):
        aid = int(absn.get(qn("w:abstractNumId")))
        levels = {}
        for lvl in absn.xpath("./w:lvl", namespaces=NS):
            ilvl = int(lvl.get(qn("w:ilvl")))
            levels[ilvl] = {
                "numFmt": (lvl.find("w:numFmt", namespaces=NS).get(qn("w:val")) or "decimal"),
                "lvlText": (lvl.find("w:lvlText", namespaces=NS).get(qn("w:val")) or ""),
                "start": int(lvl.find("w:start", namespaces=NS).get(qn("w:val")) or "1"),
            }
        abstract_levels[aid] = levels
    return num_to_abstract, abstract_levels, num_id_overrides

# --- 格式化工具 ---

def to_roman(n: int) -> str:
    vals = [(1000, "M"), (900, "CM"), (500, "D"), (400, "CD"), (100, "C"), (90, "XC"), (50, "L"), (40, "XL"), (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I")]
    out = []
    for v, s in vals:
        while n >= v:
            out.append(s); n -= v
    return "".join(out)

def to_alpha(n: int) -> str:
    out = []
    while n > 0:
        n -= 1; out.append(chr(ord("A") + (n % 26))); n //= 26
    return "".join(reversed(out))

def format_counter(value: int, numFmt: str) -> str:
    if numFmt in ("decimal", "decimalZero"): return str(value)
    if numFmt == "upperRoman": return to_roman(value).upper()
    if numFmt == "lowerRoman": return to_roman(value).lower()
    if numFmt == "upperLetter": return to_alpha(value).upper()
    if numFmt == "lowerLetter": return to_alpha(value).lower()
    if numFmt == "bullet": return "•"
    return str(value)

def compute_display_label(lvlText, numFmt, counters, ilvl):
    if numFmt == "bullet": return "•"
    def repl(m):
        idx = int(m.group(1)) - 1
        v = counters.get(idx, 1)
        return format_counter(v, numFmt if idx == ilvl else "decimal")
    return re.sub(r"%(\d+)", repl, lvlText) if lvlText else format_counter(counters.get(ilvl, 1), numFmt)

# --- 主解析函式 ---

def parse_paragraph_numbering(docx_path: str):
    parts = read_docx_parts(docx_path)
    style_based, style_numpr = parse_styles_numPr(parts.get("word/styles.xml"))
    num_to_abstract, abstract_levels, num_id_overrides = parse_numbering(parts.get("word/numbering.xml"))
    
    root = etree.fromstring(parts["word/document.xml"])
    paragraphs = root.xpath("//w:p", namespaces=NS)

    # 狀態追蹤
    counters_by_numId = defaultdict(lambda: defaultdict(int))
    started_by_numId = defaultdict(lambda: defaultdict(bool))
    results = []

    for idx, p in enumerate(paragraphs):
        txt = get_p_text(p)
        if not txt and not p.xpath(".//w:numPr", namespaces=NS): continue

        # 取得編號屬性 (直接設定 vs 樣式設定)
        pPr = p.find("w:pPr", namespaces=NS)
        d_numId, d_ilvl = get_numPr_from_pPr(pPr) if pPr is not None else (None, None)
        s_id = get_pStyle(p)
        s_numId, s_ilvl = resolve_style_numPr(s_id, style_based, style_numpr)

        numId = d_numId if d_numId is not None else s_numId
        ilvl = d_ilvl if d_ilvl is not None else s_ilvl

        display = ""
        if numId is not None and ilvl is not None:
            absId = num_to_abstract.get(numId)
            if absId is not None:
                # 方法二：初始化當前與所有父層級
                for l in range(ilvl + 1):
                    if not started_by_numId[numId][l]:
                        # 優先順序：1. 實例覆寫值 -> 2. 模板預設值 -> 3. 預設 1
                        s_val = num_id_overrides.get(numId, {}).get(l)
                        if s_val is None:
                            s_val = abstract_levels.get(absId, {}).get(l, {}).get("start", 1)
                        
                        counters_by_numId[numId][l] = s_val
                        started_by_numId[numId][l] = True
                        if l == ilvl: counters_by_numId[numId][l] -= 1 # 抵銷後面的 +1

                # 更新計數器
                counters_by_numId[numId][ilvl] += 1
                # 重置子層級
                for child_l in [k for k in counters_by_numId[numId].keys() if k > ilvl]:
                    counters_by_numId[numId][child_l] = 0
                    started_by_numId[numId][child_l] = False

                # 格式化顯示
                l_info = abstract_levels[absId].get(ilvl, {})
                display = compute_display_label(l_info.get("lvlText",""), l_info.get("numFmt",""), counters_by_numId[numId], ilvl)

        results.append({"index": idx, "display": display, "text": txt, "style": s_id})

    return results

if __name__ == "__main__":
    path = r"C:\Users\ne025\Desktop\Test_File\3. 01.02.2025_Device Description_Template.docx"
    for r in parse_paragraph_numbering(path):
        if r["display"]:
            print(f"[{r['index']:>3}] {r['display']:<10} {r['text']}")
