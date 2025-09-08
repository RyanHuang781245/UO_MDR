import os
import re
from typing import Dict, List, Tuple

from spire.doc import Document, FileFormat

from .Edit_Word import insert_numbered_heading
from .Extract_AllFile_to_FinalWord import (
    extract_word_all_content,
    extract_word_chapter,
)
from .file_copier import copy_files


def _find_file(base: str, filename: str) -> str | None:
    for root, _dirs, files in os.walk(base):
        for fn in files:
            if fn == filename:
                return os.path.join(root, fn)
    return None


def process_mapping_excel(mapping_path: str, task_files_dir: str, output_dir: str) -> Dict[str, List[str]]:
    """Process mapping Excel file and generate documents or copy files.

    Returns a dict with keys:
        logs: list of messages
        outputs: list of generated docx paths
    """
    logs: List[str] = []
    docs: Dict[str, Tuple[Document, any]] = {}
    outputs: List[str] = []

    try:
        from openpyxl import load_workbook
    except Exception as e:  # pragma: no cover
        raise RuntimeError("openpyxl is required to process mapping files") from e

    wb = load_workbook(mapping_path)
    ws = wb.active

    for row in ws.iter_rows(min_row=2, values_only=True):
        out_name, title, input_name, instruction = row[:4]
        if not instruction:
            continue
        instruction = str(instruction).strip()

        # Determine operation type
        is_all = instruction.lower() == "all"
        chapter_match = re.match(r"^([0-9]+(?:\.[0-9]+)*)(?:.*)", instruction)
        if is_all or chapter_match:
            # extraction from document
            if not input_name:
                logs.append(f"{out_name}: 未提供輸入檔案名稱")
                continue
            infile = _find_file(task_files_dir, str(input_name))
            if not infile:
                logs.append(f"{out_name}: 找不到檔案 {input_name}")
                continue
            doc, section = docs.get(out_name, (None, None))
            if doc is None:
                doc = Document()
                section = doc.AddSection()
                docs[out_name] = (doc, section)
            if title:
                insert_numbered_heading(section, str(title), level=0, bold=True, font_size=14)
            if is_all:
                extract_word_all_content(infile, output_doc=doc, section=section)
                logs.append(f"擷取 {input_name} 全部內容")
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
                    logs.append(f"擷取 {input_name} 章節 {chapter} 標題 {after.strip()}")
                else:
                    extract_word_chapter(
                        infile,
                        chapter,
                        output_doc=doc,
                        section=section,
                    )
                    logs.append(f"擷取 {input_name} 章節 {chapter}")
        else:
            # copy files by keywords
            dest = os.path.join(task_files_dir, str(out_name or "output"))
            if title:
                dest = os.path.join(dest, str(title))
            keywords = [k.strip() for k in instruction.split(",") if k.strip()]
            try:
                copied = copy_files(task_files_dir, dest, keywords)
                logs.append(f"複製 {len(copied)} 個檔案至 {os.path.relpath(dest, task_files_dir)}")
            except Exception as e:
                logs.append(f"複製檔案失敗: {e}")

    os.makedirs(output_dir, exist_ok=True)
    for name, (doc, _section) in docs.items():
        out_path = os.path.join(output_dir, f"{name}.docx")
        doc.SaveToFile(out_path, FileFormat.Docx)
        doc.Close()
        outputs.append(out_path)
        logs.append(f"產生文件 {out_path}")

    return {"logs": logs, "outputs": outputs}
