
import os
import hashlib
from datetime import datetime
from typing import List, Dict, Any
from docx import Document as DocxDocument
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.text.paragraph import Paragraph as DocxParagraph
from docx.oxml.ns import qn
from .Extract_AllFile_to_FinalWord import (
    extract_pdf_chapter_to_table,
    extract_word_all_content,
    extract_word_chapter,
    extract_specific_figure_from_word,
    extract_specific_table_from_word,
)
from .file_copier import copy_files
from .docx_merger import merge_word_docs
from .template_manager import (
    parse_template_paragraphs,
    render_template_with_mappings,
)


def _resolve_fragment_path(workdir: str, user_path: str | None, idx: int) -> str:
    """Build an absolute path for a fragment DOCX inside workdir."""
    if user_path:
        path = user_path if os.path.isabs(user_path) else os.path.join(workdir, user_path)
    else:
        path = os.path.join(workdir, f"fragment_{idx}.docx")
    os.makedirs(os.path.dirname(path) or workdir, exist_ok=True)
    return path


def _new_docx_fragment(path: str) -> DocxDocument:
    """Return a blank python-docx document ready to save to path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return DocxDocument()


def _set_alignment(paragraph: DocxParagraph, align: str) -> None:
    align_map = {
        "left": WD_ALIGN_PARAGRAPH.LEFT,
        "center": WD_ALIGN_PARAGRAPH.CENTER,
        "right": WD_ALIGN_PARAGRAPH.RIGHT,
        "justify": WD_ALIGN_PARAGRAPH.JUSTIFY,
    }
    paragraph.alignment = align_map.get(align.lower(), WD_ALIGN_PARAGRAPH.LEFT)


def _clear_list_formatting(paragraph: DocxParagraph) -> None:
    ppr = paragraph._p.get_or_add_pPr()
    num_pr = ppr.find(qn("w:numPr"))
    if num_pr is not None:
        ppr.remove(num_pr)


def _clear_indent(paragraph: DocxParagraph) -> None:
    pf = paragraph.paragraph_format
    pf.left_indent = None
    pf.first_line_indent = None
    pf.right_indent = None
    pf.hanging_indent = None


def _to_roman(num: int) -> str:
    if num <= 0:
        return ""
    pairs = [
        (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
        (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
        (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
    ]
    result = []
    for value, symbol in pairs:
        while num >= value:
            result.append(symbol)
            num -= value
    return "".join(result)

SUPPORTED_STEPS = {
    "extract_pdf_chapter_to_table": {
        "label": "擷取 PDF 章節至表格（上傳 ZIP）",
        "inputs": ["pdf_zip", "target_section", "template_index", "template_mode"],
        "accepts": {
            "pdf_zip": "file:zip",
            "target_section": "text",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "extract_word_all_content": {
        "label": "擷取 Word 全部內容",
        "inputs": ["input_file", "ignore_toc_and_before", "ignore_header_footer", "template_index", "template_mode"],
        "accepts": {
            "input_file": "file:docx",
            "ignore_toc_and_before": "bool",
            "ignore_header_footer": "bool",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "extract_word_chapter": {
        "label": "擷取 Word 指定章節/標題",
        "inputs": [
            "input_file",
            "target_chapter_section",
            "target_title",
            "target_title_section",
            "explicit_end_title",
            "subheading_text",
            "subheading_strict_match",
            "ignore_toc",
            "ignore_header_footer",
            "template_index",
            "template_mode",
        ],
        "accepts": {
            "input_file": "file:docx",
            "target_chapter_section": "text",
            "target_title": "bool",
            "target_title_section": "text",
            "explicit_end_title": "text",
            "subheading_text": "text",
            "subheading_strict_match": "bool",
            "ignore_toc": "bool",
            "ignore_header_footer": "bool",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "extract_specific_figure_from_word": {
        "label": "擷取 Word 指定章節/標題的特定圖",
        "inputs": [
            "input_file",
            "target_chapter_section",
            "target_chapter_title",
            "target_subtitle",
            "target_figure_label",
            "include_caption",
            "template_index",
            "template_mode",
        ],
        "accepts": {
            "input_file": "file:docx",
            "target_chapter_section": "text",
            "target_chapter_title": "text",
            "target_subtitle": "text",
            "target_figure_label": "text",
            "include_caption": "bool",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "extract_specific_table_from_word": {
        "label": "擷取 Word 指定章節/標題的特定表格",
        "inputs": [
            "input_file",
            "target_chapter_section",
            "target_chapter_title",
            "target_table_label",
            "target_subtitle",
            "include_caption",
            "template_index",
            "template_mode",
        ],
        "accepts": {
            "input_file": "file:docx",
            "target_chapter_section": "text",
            "target_chapter_title": "text",
            "target_table_label": "text",
            "target_subtitle": "text",
            "include_caption": "bool",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "insert_text": {
        "label": "插入純文字段落",
        "inputs": ["text", "align", "bold", "font_size", "page_break_before", "template_index", "template_mode"],
        "accepts": {
            "text": "text",
            "align": "align",
            "bold": "bool",
            "font_size": "float",
            "page_break_before": "bool",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "insert_numbered_heading": {
        "label": "插入阿拉伯數字標題",
        "inputs": ["text", "level", "bold", "font_size", "template_index", "template_mode"],
        "accepts": {
            "text": "text",
            "level": "int",
            "bold": "bool",
            "font_size": "float",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "insert_roman_heading": {
        "label": "插入羅馬數字標題",
        "inputs": ["text", "level", "bold", "font_size", "template_index", "template_mode"],
        "accepts": {
            "text": "text",
            "level": "int",
            "bold": "bool",
            "font_size": "float",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "insert_bulleted_heading": {
        "label": "插入項目符號標題",
        "inputs": ["text", "bold", "font_size", "template_index", "template_mode"],
        "accepts": {
            "text": "text",
            "bold": "bool",
            "font_size": "float",
            "template_index": "text",
            "template_mode": "text",
        }
    },
    "copy_files": {
        "label": "複製檔案",
        "inputs": ["source_dir", "dest_dir", "keywords"],
        "accepts": {
            "source_dir": "file:dir",
            "dest_dir": "file:dir",
            "keywords": "text"
        }
    },
    "renumber_figures_tables": {
        "label": "重新編號圖表並更新參照",
        "inputs": ["numbering_scope", "figure_start", "table_start"],
        "accepts": {
            "numbering_scope": "text",
            "figure_start": "int",
            "table_start": "int",
        }
    }
}

def boolish(v:str)->bool:
    return str(v).lower() in ["1","true","yes","y","on"]



def run_workflow(steps: List[Dict[str, Any]], workdir: str, template: Dict[str, Any] | None = None) -> Dict[str, Any]:
    def _hash_file(path: str) -> str:
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                sha.update(chunk)
        return sha.hexdigest()

    def _collect_file_metadata(
        steps_data: List[Dict[str, Any]], template_cfg: Dict[str, Any] | None
    ) -> List[Dict[str, Any]]:
        entries: list[Dict[str, Any]] = []
        seen: set[str] = set()

        def add_path(path_raw: str, kind: str) -> None:
            if not path_raw:
                return
            path = os.path.abspath(path_raw)
            if path in seen:
                return
            seen.add(path)
            exists = os.path.exists(path)
            meta = {
                "name": os.path.basename(path),
                "source": path,
                "type": kind,
                "version": "",
                "updated_at": "",
                "size_bytes": None,
                "exists": exists,
            }
            if exists:
                stat = os.stat(path)
                meta["updated_at"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                if os.path.isfile(path):
                    meta["size_bytes"] = stat.st_size
                    try:
                        meta["version"] = _hash_file(path)
                    except Exception:
                        meta["version"] = ""
            entries.append(meta)

        for step in steps_data:
            stype = step.get("type")
            schema = SUPPORTED_STEPS.get(stype, {})
            accepts = schema.get("accepts", {})
            params = step.get("params", {}) or {}
            for key, acc in accepts.items():
                if not isinstance(acc, str) or not acc.startswith("file"):
                    continue
                path_val = params.get(key)
                if not path_val:
                    continue
                kind = "dir" if acc.endswith(":dir") else "file"
                add_path(str(path_val), kind)

        if template_cfg and template_cfg.get("path"):
            add_path(str(template_cfg.get("path")), "template")

        return entries

    log = []
    fragments: list[str] = []
    template_cfg = template or {}
    file_metadata = _collect_file_metadata(steps, template_cfg)
    if file_metadata:
        log.append({"type": "file_metadata", "files": file_metadata, "status": "ok"})
    template_mappings: list[Dict[str, Any]] = []
    template_mode_default = (template_cfg.get("default_mode") or "insert_after").strip()
    template_paragraphs = template_cfg.get("paragraphs")
    arabic_counters: list[int] = [0, 0, 0]
    roman_counter = 0

    def _next_arabic(level_raw: Any) -> str:
        try:
            level = int(level_raw)
        except Exception:
            level = 0
        if level < 0:
            level = 0
        if level >= len(arabic_counters):
            arabic_counters.extend([0] * (level + 1 - len(arabic_counters)))
        for i in range(level):
            if arabic_counters[i] == 0:
                arabic_counters[i] = 1
        arabic_counters[level] += 1
        for i in range(level + 1, len(arabic_counters)):
            arabic_counters[i] = 0
        parts = [str(arabic_counters[i]) for i in range(level + 1)]
        return ".".join(parts) + ". "

    def _next_roman() -> str:
        nonlocal roman_counter
        roman_counter += 1
        return _to_roman(roman_counter) + ". "

    def _route_fragment(path: str, params: Dict[str, Any], stype: str) -> None:
        t_idx_raw = params.get("template_index")
        if template_cfg and t_idx_raw not in (None, "", "None"):
            try:
                t_idx = int(t_idx_raw)
            except Exception:
                fragments.append(path)
                return
            mode = (params.get("template_mode") or template_mode_default or "insert_after").strip() or "insert_after"
            template_mappings.append(
                {
                    "index": t_idx,
                    "mode": mode,
                    "content_docx_path": path,
                    "source_step": stype,
                }
            )
            log[-1]["template_index"] = t_idx
            log[-1]["template_mode"] = mode
            return
        fragments.append(path)

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
                    raise RuntimeError("Missing or invalid PDF ZIP path")
                extract_dir = os.path.join(workdir, "pdfs_extracted")
                os.makedirs(extract_dir, exist_ok=True)
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.extractall(extract_dir)
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                doc = DocxDocument()
                extract_pdf_chapter_to_table(extract_dir, target, output_doc=doc, section=None)
                doc.save(frag_path)
                _route_fragment(frag_path, params, stype)

            elif stype == "extract_word_all_content":
                infile = params["input_file"]
                ignore_toc = boolish(params.get("ignore_toc_and_before", "true"))
                ignore_header_footer = boolish(params.get("ignore_header_footer", "true"))
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                result = extract_word_all_content(
                    infile,
                    output_doc=None,
                    section=None,
                    output_docx_path=frag_path,
                    ignore_toc_and_before=ignore_toc,
                    ignore_header_footer=ignore_header_footer,
                )
                out_path = None
                if isinstance(result, dict):
                    out_path = result.get("output_docx") or frag_path
                    log[-1]["output_docx"] = out_path
                if out_path:
                    _route_fragment(out_path, params, stype)

            elif stype == "extract_word_chapter":
                infile = params["input_file"]
                tsec = params.get("target_chapter_section","")
                use_title = boolish(params.get("target_title","false"))
                title_text = params.get("target_title_section","")
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                result = extract_word_chapter(
                    infile,
                    tsec,
                    target_title=use_title,
                    target_title_section=title_text,
                    explicit_end_title=params.get("explicit_end_title") or None,
                    subheading_text=params.get("subheading_text"),
                    subheading_strict_match=boolish(params.get("subheading_strict_match", "true")),
                    ignore_header_footer=boolish(params.get("ignore_header_footer", "true")),
                    ignore_toc=boolish(params.get("ignore_toc", "true")),
                    output_docx_path=frag_path,
                    output_doc=None,
                    section=None
                )
                out_path = None
                if isinstance(result, dict):
                    log[-1]["captured_titles"] = result.get("captured_titles", [])
                    out_path = result.get("output_docx") or frag_path
                    log[-1]["output_docx"] = out_path
                if out_path:
                    _route_fragment(out_path, params, stype)

            elif stype == "extract_specific_figure_from_word":
                infile = params["input_file"]
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                result = extract_specific_figure_from_word(
                    infile,
                    params.get("target_chapter_section", ""),
                    params.get("target_figure_label", ""),
                    target_subtitle=params.get("target_subtitle"),
                    target_chapter_title=params.get("target_chapter_title"),
                    output_image_path=os.path.join(workdir, "images"),
                    output_doc=None,
                    section=None,
                )
                image_dir = os.path.join(workdir, "images")
                os.makedirs(image_dir, exist_ok=True)
                include_caption = boolish(params.get("include_caption", "true"))
                added = False
                doc = DocxDocument()
                if isinstance(result, dict):
                    image_filename = result.get("image_filename")
                    caption_text = result.get("caption")
                    log[-1]["image_filename"] = image_filename
                    log[-1]["caption"] = caption_text
                    if image_filename:
                        img_path = os.path.join(image_dir, image_filename)
                        if os.path.exists(img_path):
                            doc.add_picture(img_path)
                            added = True
                    if include_caption and caption_text:
                        doc.add_paragraph(caption_text)
                        added = True
                if added:
                    doc.save(frag_path)
                    _route_fragment(frag_path, params, stype)

            elif stype == "extract_specific_table_from_word":
                infile = params["input_file"]
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                extract_specific_table_from_word(
                    infile,
                    frag_path,
                    params.get("target_chapter_section", ""),
                    params.get("target_table_label", ""),
                    params.get("target_subtitle") or None,
                    target_chapter_title=params.get("target_chapter_title"),
                    include_caption=boolish(params.get("include_caption", "false")),
                    save_output=True,
                )
                if os.path.isfile(frag_path):
                    _route_fragment(frag_path, params, stype)
                    log[-1]["output_docx"] = frag_path

            elif stype == "insert_text":
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                doc = _new_docx_fragment(frag_path)
                para = doc.add_paragraph(params.get("text",""))
                para.runs[0].bold = boolish(params.get("bold","false"))
                try:
                    para.runs[0].font.size = Pt(float(params.get("font_size", 12)))
                except Exception:
                    para.runs[0].font.size = None
                _set_alignment(para, params.get("align","left"))
                doc.save(frag_path)
                _route_fragment(frag_path, params, stype)

            elif stype == "insert_numbered_heading":
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                doc = _new_docx_fragment(frag_path)
                heading_text = params.get("text","")
                prefix = _next_arabic(params.get("level", 0))
                para = doc.add_paragraph(f"{prefix}{heading_text}")
                if "Normal" in doc.styles:
                    para.style = doc.styles["Normal"]
                _clear_list_formatting(para)
                _clear_indent(para)
                para.runs[0].bold = boolish(params.get("bold","true"))
                try:
                    para.runs[0].font.size = Pt(float(params.get("font_size", 12)))
                except Exception:
                    para.runs[0].font.size = None
                doc.save(frag_path)
                _route_fragment(frag_path, params, stype)

            elif stype == "insert_roman_heading":
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                doc = _new_docx_fragment(frag_path)
                heading_text = params.get("text","")
                prefix = _next_roman()
                para = doc.add_paragraph(f"{prefix}{heading_text}")
                if "Normal" in doc.styles:
                    para.style = doc.styles["Normal"]
                _clear_list_formatting(para)
                _clear_indent(para)
                para.runs[0].bold = boolish(params.get("bold","true"))
                try:
                    para.runs[0].font.size = Pt(float(params.get("font_size", 12)))
                except Exception:
                    para.runs[0].font.size = None
                doc.save(frag_path)
                _route_fragment(frag_path, params, stype)

            elif stype == "insert_bulleted_heading":
                frag_path = _resolve_fragment_path(workdir, params.get("output_docx_path"), idx)
                doc = _new_docx_fragment(frag_path)
                heading_text = params.get("text","")
                para = doc.add_paragraph(heading_text)
                if "List Bullet" in doc.styles:
                    para.style = doc.styles["List Bullet"]
                else:
                    para.text = f"• {heading_text}"
                _clear_indent(para)
                para.runs[0].bold = boolish(params.get("bold","true"))
                try:
                    para.runs[0].font.size = Pt(float(params.get("font_size", 12)))
                except Exception:
                    para.runs[0].font.size = None
                doc.save(frag_path)
                _route_fragment(frag_path, params, stype)

            elif stype == "copy_files":
                keywords = [k.strip() for k in params.get("keywords", "").split(",") if k.strip()]
                copied = copy_files(
                    params.get("source_dir", ""),
                    params.get("dest_dir", ""),
                    keywords,
                )
                log[-1]["copied_files"] = copied

            elif stype == "renumber_figures_tables":
                # Skipped here to avoid Spire save (watermark); can be run externally if licensed.
                log[-1]["status"] = "skipped"
                log[-1]["note"] = "renumber_figures_tables skipped to avoid Spire watermark"
                continue

            else:
                raise RuntimeError(f"Unknown step type: {stype}")

            if "status" not in log[-1]:
                log[-1]["status"] = "ok"

        except Exception as e:
            log[-1]["status"] = "error"
            log[-1]["error"] = str(e)

    if template_cfg.get("path") and template_mappings:
        try:
            parsed = template_paragraphs or parse_template_paragraphs(template_cfg["path"])
            template_result_path = os.path.join(workdir, "template_result.docx")
            render_template_with_mappings(
                template_cfg["path"],
                template_result_path,
                template_mappings,
                parsed,
            )
            log.append(
                {
                    "step": len(log) + 1,
                    "type": "template_merge",
                    "template_file": template_cfg["path"],
                    "mappings": [
                        {
                            "index": m.get("index"),
                            "mode": m.get("mode"),
                            "source_step": m.get("source_step"),
                        }
                        for m in template_mappings
                    ],
                    "output_docx": template_result_path,
                    "status": "ok",
                }
            )
            fragments.insert(0, template_result_path)
        except Exception as e:
            log.append(
                {
                    "step": len(log) + 1,
                    "type": "template_merge",
                    "template_file": template_cfg.get("path"),
                    "mappings": template_mappings,
                    "status": "error",
                    "error": str(e),
                }
            )
            for mp in template_mappings:
                cdoc = mp.get("content_docx_path")
                if cdoc:
                    fragments.append(cdoc)

    if not fragments:
        empty_path = os.path.join(workdir, "result.docx")
        DocxDocument().save(empty_path)
        fragments.append(empty_path)

    out_docx = os.path.join(workdir, "result.docx")
    merge_word_docs(fragments, out_docx)

    out_log = os.path.join(workdir, "log.json")
    with open(out_log, "w", encoding="utf-8") as f:
        import json
        json.dump(log, f, ensure_ascii=False, indent=2)

    return {"result_docx": out_docx, "log": out_log, "log_json": log}
