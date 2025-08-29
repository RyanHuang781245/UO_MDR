
import os
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

def run_workflow(steps: List[Dict[str, Any]], workdir: str) -> Dict[str, Any]:
    """Execute workflow steps and record source information.

    During execution this collects chapter/source mappings so later
    stages can embed them directly into the result HTML without parsing
    the log or output files."""

    log: List[Dict[str, Any]] = []
    output_doc = Document()
    section = output_doc.AddSection()

    chapter_sources: Dict[str, List[str]] = {}
    source_urls: Dict[str, str] = {}
    converted_docx: Dict[str, str] = {}
    current_chapter = None

    for idx, step in enumerate(steps, start=1):
        stype = step.get("type")
        params = step.get("params", {})
        log.append({"step": idx, "type": stype, "params": params})
        try:
            if stype == "extract_pdf_chapter_to_table":
                import zipfile
                zip_path = params.get("pdf_zip")
                target = params["target_section"]
                if not zip_path or not os.path.isfile(zip_path):
                    raise RuntimeError("未提供 PDF ZIP 檔或路徑錯誤")
                extract_dir = os.path.join(workdir, "pdfs_extracted")
                os.makedirs(extract_dir, exist_ok=True)
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.extractall(extract_dir)
                pdfs = [fn for fn in sorted(os.listdir(extract_dir)) if fn.lower().endswith('.pdf')]
                for fn in pdfs:
                    source_urls[fn] = os.path.join("pdfs_extracted", fn)
                chapter_sources.setdefault(current_chapter or "未分類", []).extend(pdfs)
                extract_pdf_chapter_to_table(extract_dir, target, output_doc=output_doc, section=section)

            elif stype == "extract_word_all_content":
                infile = params["input_file"]
                extract_word_all_content(
                    infile,
                    output_image_path=os.path.join(workdir, "images"),
                    output_doc=output_doc,
                    section=section,
                )
                base = os.path.basename(infile)
                chapter_sources.setdefault(current_chapter or "未分類", []).append(base)
                if base not in converted_docx and infile and os.path.exists(infile):
                    preview_dir = os.path.join(workdir, "source_html")
                    os.makedirs(preview_dir, exist_ok=True)
                    html_name_src = f"{os.path.splitext(base)[0]}.html"
                    html_rel = os.path.join("source_html", html_name_src)
                    html_path_src = os.path.join(workdir, html_rel)
                    doc = Document()
                    doc.LoadFromFile(infile)
                    doc.HtmlExportOptions.ImageEmbedded = True
                    doc.SaveToFile(html_path_src, FileFormat.Html)
                    doc.Close()
                    converted_docx[base] = html_rel
                if base in converted_docx:
                    source_urls[base] = converted_docx[base]

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
                base = os.path.basename(infile)
                info = base
                if tsec:
                    info += f" 章節 {tsec}"
                if use_title and title_text:
                    info += f" 標題 {title_text}"
                chapter_sources.setdefault(current_chapter or "未分類", []).append(info)
                if base not in converted_docx and infile and os.path.exists(infile):
                    preview_dir = os.path.join(workdir, "source_html")
                    os.makedirs(preview_dir, exist_ok=True)
                    html_name_src = f"{os.path.splitext(base)[0]}.html"
                    html_rel = os.path.join("source_html", html_name_src)
                    html_path_src = os.path.join(workdir, html_rel)
                    doc = Document()
                    doc.LoadFromFile(infile)
                    doc.HtmlExportOptions.ImageEmbedded = True
                    doc.SaveToFile(html_path_src, FileFormat.Html)
                    doc.Close()
                    converted_docx[base] = html_rel
                if base in converted_docx:
                    source_urls[info] = converted_docx[base]

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
                current_chapter = params.get("text", "")
                chapter_sources.setdefault(current_chapter, [])
                insert_roman_heading(
                    section,
                    current_chapter,
                    level=int(params.get("level", 0)),
                    bold=boolish(params.get("bold", "true")),
                    font_size=float(params.get("font_size", 14)),
                )

            elif stype == "insert_bulleted_heading":
                insert_bulleted_heading(
                    section,
                    params.get("text", ""),
                    level=0,
                    bullet_char='·',
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
    with open(out_log, "w", encoding="utf-8") as f:
        import json
        json.dump(log, f, ensure_ascii=False, indent=2)

    sources_path = os.path.join(workdir, "sources.json")
    with open(sources_path, "w", encoding="utf-8") as f:
        import json
        json.dump({"chapter_sources": chapter_sources, "source_urls": source_urls}, f, ensure_ascii=False, indent=2)

    return {"result_docx": out_docx, "log": out_log, "log_json": log, "sources": sources_path}
