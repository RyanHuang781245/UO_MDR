import os
import re
from typing import Any, Dict, List, Tuple

from spire.doc import Document, FileFormat

from .Edit_Word import (
    renumber_figures_tables_file,
    insert_text,
    insert_roman_heading,
    insert_bulleted_heading,
    insert_numbered_heading,
)
from .Extract_AllFile_to_FinalWord import (
    extract_word_all_content,
    extract_word_chapter,
    center_table_figure_paragraphs,
    apply_basic_style,
    remove_hidden_runs,
)
from .file_copier import copy_files


def _find_file(base: str, filename: str) -> str | None:
    """Search *base* recursively for *filename* ignoring case."""
    target = filename.lower()
    for root, _dirs, files in os.walk(base):
        for fn in files:
            if fn.lower() == target:
                return os.path.join(root, fn)
    return None


def _find_directory(base: str, path: str) -> str | None:
    """Locate a directory relative to *base* ignoring case."""
    parts = [p for p in os.path.normpath(path).split(os.sep) if p]
    current = base
    for part in parts:
        match = None
        for name in os.listdir(current):
            candidate = os.path.join(current, name)
            if os.path.isdir(candidate) and name.lower() == part.lower():
                match = candidate
                break
        if match is None:
            return None
        current = match
    return current


def _resolve_input_file(base: str, name: str) -> str | None:
    """Resolve *name* to a file path.

    If *name* includes an extension, it is treated as a filename and searched
    within *base*. If it has no extension, it is treated as a directory and the
    first document file inside that directory is returned.
    """

    if "." in os.path.basename(name):
        return _find_file(base, name)

    dir_path = _find_directory(base, name)
    if not dir_path:
        return None
    for fn in os.listdir(dir_path):
        if fn.lower().endswith((".docx", ".doc")):
            return os.path.join(dir_path, fn)
    return None


def insert_title(section, title: str):
    """Insert *title* into *section* with appropriate heading style.

    - Titles beginning with Roman numerals (e.g. ``"I."``, ``"II."``) use
      :func:`insert_roman_heading`.
    - Titles beginning with a ``"⚫"`` bullet use :func:`insert_bulleted_heading`.
    - All other titles use :func:`insert_numbered_heading`.
    """

    if not title:
        return None

    # Strip leading chapter numbers like "6.4.2" from the title
    title = re.sub(r"^[0-9]+(?:\.[0-9]+)*\s*", "", title)

    roman_match = re.match(r"^[IVXLCDM]+\.\s*(.*)", title)
    if roman_match:
        text = roman_match.group(1).strip() or title
        return insert_roman_heading(section, text, level=0, bold=True, font_size=12)

    if title.startswith("⚫"):
        text = title.lstrip("⚫").strip()
        return insert_bulleted_heading(section, text, level=0, bullet_char='·', bold=True, font_size=12)

    return insert_numbered_heading(section, title, level=0, bold=True, font_size=12)

def process_mapping_excel(mapping_path: str, task_files_dir: str, output_dir: str) -> Dict[str, List[str]]:
    """Process mapping Excel file and generate documents or copy files.

    The spreadsheet must provide columns:
        A: output Word document name
        B: heading title to insert
        C: folder containing the source file
        D: source file name (if no extension, treated as a subfolder)
        E: extraction instruction or file-copy keywords
    Returns a dict with keys:
        logs: list of messages
        outputs: list of generated docx paths
    """
    logs: List[str] = []
    docs: Dict[str, Tuple[Document, Any, List[Dict[str, Any]]]] = {}
    outputs: List[str] = []

    try:
        from openpyxl import load_workbook
    except Exception as e:  # pragma: no cover
        raise RuntimeError("openpyxl is required to process mapping files") from e

    wb = load_workbook(mapping_path)
    ws = wb.active

    for row in ws.iter_rows(min_row=2, values_only=True):
        raw_out, raw_title, raw_folder, raw_input, raw_instruction = row[:5]
        out_name = str(raw_out).strip() if raw_out else ""
        title = str(raw_title).strip() if raw_title else ""
        folder = str(raw_folder).strip() if raw_folder else ""
        input_name = str(raw_input).strip() if raw_input else ""
        instruction = str(raw_instruction).strip() if raw_instruction else ""
        if not instruction:
            continue

        base_dir = task_files_dir
        if folder:
            found_dir = _find_directory(task_files_dir, folder)
            if not found_dir:
                logs.append(f"{out_name or '未命名'}: 找不到資料夾 {folder}")
                continue
            base_dir = found_dir

        is_all = instruction.lower() == "all"
        chapter_match = re.match(r"^([0-9]+(?:\.[0-9]+)*)(?:.*)", instruction)

        if is_all or chapter_match:
            if not input_name:
                logs.append(f"{out_name or '未命名'}: 未提供輸入檔案名稱")
                continue
            infile = _resolve_input_file(base_dir, input_name)
            if not infile:
                logs.append(f"{out_name or '未命名'}: 找不到檔案 {input_name}")
                continue

            doc, section, log_entries = docs.get(out_name, (None, None, None))
            if doc is None:
                doc = Document()
                section = doc.AddSection()
                log_entries = []
                docs[out_name] = (doc, section, log_entries)

            # record heading insertion in step log
            if title:
                roman_match = re.match(r"^[IVXLCDM]+\.\s*(.*)", title)
                if roman_match:
                    log_entries.append({"type": "insert_roman_heading", "params": {"text": roman_match.group(1).strip() or title}})
                elif title.startswith("⚫"):
                    log_entries.append({"type": "insert_bulleted_heading", "params": {"text": title.lstrip('⚫').strip()}})
                else:
                    cleaned = re.sub(r"^[0-9]+(?:\.[0-9]+)*\s*", "", title)
                    log_entries.append({"type": "insert_numbered_heading", "params": {"text": cleaned, "level": 0}})

            insert_title(section, title)

            if is_all:
                extract_word_all_content(infile, output_doc=doc, section=section)
                logs.append(f"擷取 {input_name} (全部內容)")
                log_entries.append({"type": "extract_word_all_content", "params": {"input_file": infile}})
            else:
                chapter = chapter_match.group(1)
                if "," in instruction:
                    _prefix, after = instruction.split(",", 1)
                    extract_word_chapter(
                        infile,
                        chapter,
                        target_title=True,
                        target_title_section=after.strip(),
                        output_doc=doc,
                        section=section,
                    )
                    logs.append(f"擷取 {input_name} (章節: {chapter} 標題: {after.strip()})")
                    log_entries.append({
                        "type": "extract_word_chapter",
                        "params": {
                            "input_file": infile,
                            "target_chapter_section": chapter,
                            "target_title": True,
                            "target_title_section": after.strip(),
                        },
                    })
                else:
                    extract_word_chapter(
                        infile,
                        chapter,
                        output_doc=doc,
                        section=section,
                    )
                    logs.append(f"擷取 {input_name} (章節: {chapter})")
                    log_entries.append({
                        "type": "extract_word_chapter",
                        "params": {
                            "input_file": infile,
                            "target_chapter_section": chapter,
                            "target_title": False,
                            "target_title_section": "",
                        },
                    })
        else:
            dest = os.path.join(task_files_dir, out_name or "output")
            if title:
                dest = os.path.join(dest, title)

            search_root = base_dir
            if input_name:
                if "." in os.path.basename(input_name):
                    found = _resolve_input_file(base_dir, input_name)
                    if found:
                        search_root = os.path.dirname(found)
                else:
                    dir_path = _find_directory(base_dir, input_name)
                    if dir_path:
                        search_root = dir_path

            keywords = [
                k.strip()
                for k in re.split(r"[,，]+", instruction)
                if k.strip()
            ]
            try:
                copied = copy_files(search_root, dest, keywords)
                kw_display = ", ".join(keywords)
                logs.append(
                    f"複製 {len(copied)} 個檔案至 {os.path.relpath(dest, task_files_dir)} (關鍵字: {kw_display})"
                )
            except Exception as e:
                logs.append(f"複製檔案失敗: {e}")

    os.makedirs(output_dir, exist_ok=True)
    step_logs: Dict[str, List[Dict[str, Any]]] = {}
    for name, (doc, _section, log_entries) in docs.items():
        out_path = os.path.join(output_dir, f"{name}.docx")
        doc.SaveToFile(out_path, FileFormat.Docx)
        doc.Close()
        remove_hidden_runs(out_path)
        renumber_figures_tables_file(out_path)
        center_table_figure_paragraphs(out_path)
        apply_basic_style(out_path)
        # Strip Spire evaluation warning paragraph if present
        try:
            from docx import Document as DocxDocument

            docx_obj = DocxDocument(out_path)
            if docx_obj.paragraphs and "Evaluation Warning" in docx_obj.paragraphs[0].text:
                p = docx_obj.paragraphs[0]._element
                p.getparent().remove(p)
                docx_obj.save(out_path)
        except Exception:
            pass
        outputs.append(out_path)
        step_logs[out_path] = log_entries
        # logs.append(f"產生文件 {out_path}")

    return {"logs": logs, "outputs": outputs, "step_logs": step_logs}
