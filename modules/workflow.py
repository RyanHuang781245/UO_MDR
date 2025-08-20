
import os
import json
from typing import List, Dict, Any
from spire.doc import *
from spire.doc.common import *

from .Edit_Word import insert_text, insert_numbered_heading, insert_roman_heading, insert_bulleted_heading
from .Extract_AllFile_to_FinalWord import (
    extract_pdf_chapter_to_table,
    extract_word_all_content,
    extract_word_chapter
)

SUPPORTED_STEPS = {
    "extract_pdf_chapter_to_table": {
        "label": "擷取 PDF 章節至表格（上傳 ZIP）",
        "inputs": ["pdf_zip", "target_section"],
        "accepts": {"pdf_zip": "file:zip", "target_section": "text"}
    },
    "extract_word_all_content": {
        "label": "擷取 Word 全部內容",
        "inputs": ["input_file"],
        "accepts": {"input_file": "file:docx"}
    },
    "extract_word_chapter": {
        "label": "擷取 Word 指定章節/標題",
        "inputs": ["input_file", "target_chapter_section", "target_title", "target_title_section"],
        "accepts": {"input_file": "file:docx", "target_chapter_section": "text", "target_title": "bool", "target_title_section": "text"}
    },
    "insert_text": {
        "label": "插入純文字段落",
        "inputs": ["text", "align", "bold", "font_size", "before_space", "after_space", "page_break_before"],
        "accepts": {"text":"text","align":"align","bold":"bool","font_size":"float","before_space":"float","after_space":"float","page_break_before":"bool"}
    },
    "insert_numbered_heading": {
        "label": "插入阿拉伯數字標題",
        "inputs": ["text", "level", "bold", "font_size"],
        "accepts": {"text":"text","level":"int","bold":"bool","font_size":"float"}
    },
    "insert_roman_heading": {
        "label": "插入羅馬數字標題",
        "inputs": ["text", "level", "bold", "font_size"],
        "accepts": {"text":"text","level":"int","bold":"bool","font_size":"float"}
    },
    "insert_bulleted_heading": {
        "label": "插入項目符號標題",
        "inputs": ["text", "font_size"],
        "accepts": {"text":"text","font_size":"float"}
    }
}

def boolish(v:str)->bool:
    return str(v).lower() in ["1","true","yes","y","on"]

def _int_to_roman(num: int) -> str:
    vals = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]
    syms = ["M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"]
    res = ""
    for v, s in zip(vals, syms):
        while num >= v:
            res += s
            num -= v
    return res


def run_workflow(steps: List[Dict[str, Any]], workdir: str) -> Dict[str, Any]:
    log = []
    sections_info = []
    current_section = None
    roman_idx = 0

    output_doc = Document()
    section = output_doc.AddSection()

    for idx, step in enumerate(steps, start=1):
        stype = step.get("type")
        params = step.get("params", {})

        # heading bookkeeping
        if stype == "insert_roman_heading":
            roman_idx += 1
            current_section = _int_to_roman(roman_idx)
            sections_info.append({
                "index": roman_idx,
                "numeral": current_section,
                "text": params.get("text", "")
            })
            entry = {"step": idx, "type": stype, "params": params, "section": current_section}
        else:
            entry = {"step": idx, "type": stype, "params": params}
            if stype in ["extract_pdf_chapter_to_table", "extract_word_all_content", "extract_word_chapter"]:
                entry["section"] = current_section

        log.append(entry)

        try:
            if stype == "extract_pdf_chapter_to_table":
                import zipfile
                zip_path = params.get("pdf_zip")
                target = params["target_section"]
                if not zip_path or not os.path.isfile(zip_path):
                    raise RuntimeError("未提供 PDF ZIP 檔或路徑錯誤")
                extract_dir = os.path.join(workdir, f"pdfs_extracted_{idx}")
                os.makedirs(extract_dir, exist_ok=True)
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.extractall(extract_dir)
                extract_pdf_chapter_to_table(extract_dir, target, output_doc=output_doc, section=section)

            elif stype == "extract_word_all_content":
                infile = params["input_file"]
                extract_word_all_content(
                    infile,
                    output_image_path=os.path.join(workdir, "images"),
                    output_doc=output_doc,
                    section=section,
                )

            elif stype == "extract_word_chapter":
                infile = params["input_file"]
                tsec = params.get("target_chapter_section", "")
                use_title = boolish(params.get("target_title", "false"))
                title_text = params.get("target_title_section", "")
                extract_word_chapter(
                    infile,
                    tsec,
                    target_title=use_title,
                    target_title_section=title_text,
                    output_image_path=os.path.join(workdir, "images"),
                    output_doc=output_doc,
                    section=section,
                )

            elif stype == "insert_text":
                insert_text(
                    section,
                    params.get("text", ""),
                    align=params.get("align", "left"),
                    bold=boolish(params.get("bold", "false")),
                    font_size=float(params.get("font_size", 12)),
                    before_space=float(params.get("before_space", 0)),
                    after_space=float(params.get("after_space", 6)),
                    page_break_before=boolish(params.get("page_break_before", "false")),
                )

            elif stype == "insert_numbered_heading":
                insert_numbered_heading(
                    section,
                    params.get("text", ""),
                    level=int(params.get("level", 0)),
                    bold=boolish(params.get("bold", "true")),
                    font_size=float(params.get("font_size", 14)),
                )

            elif stype == "insert_roman_heading":
                insert_roman_heading(
                    section,
                    params.get("text", ""),
                    level=int(params.get("level", 0)),
                    bold=boolish(params.get("bold", "true")),
                    font_size=float(params.get("font_size", 14)),
                )

            elif stype == "insert_bulleted_heading":
                insert_bulleted_heading(
                    section,
                    params.get("text", ""),
                    level=0,
                    bullet_char='•',
                    bold=True,
                    font_size=float(params.get("font_size", 14)),
                )

            else:
                raise RuntimeError(f"Unknown step type: {stype}")

            log[-1]["status"] = "ok"

        except Exception as e:
            log[-1]["status"] = "error"
            log[-1]["error"] = str(e)

    out_docx = os.path.join(workdir, "result.docx")
    output_doc.SaveToFile(out_docx, FileFormat.Docx)
    output_doc.Close()

    out_log = os.path.join(workdir, "log.json")
    data = {"steps": log, "sections": sections_info}
    with open(out_log, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return {"result_docx": out_docx, "log": out_log, "log_json": data}
