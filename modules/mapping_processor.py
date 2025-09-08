import os
import re
from typing import Dict, List, Tuple

from spire.doc import Document, FileFormat

from .Edit_Word import (
    insert_numbered_heading,
    insert_text,
    insert_roman_heading,
    insert_bulleted_heading,
)
from .Extract_AllFile_to_FinalWord import (
    extract_word_all_content,
    extract_word_chapter,
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


def insert_title(section, title: str):
    """Insert *title* into *section* with appropriate heading style.

    - Titles beginning with Roman numerals (e.g. ``"I."``, ``"II."``) use
      :func:`insert_roman_heading`.
    - Titles beginning with a ``"⚫"`` bullet use :func:`insert_bulleted_heading`.
    - All other titles use :func:`insert_numbered_heading`.
    """

    if not title:
        return None

    roman_match = re.match(r"^[IVXLCDM]+\.\s*(.*)", title)
    if roman_match:
        text = roman_match.group(1).strip() or title
        return insert_roman_heading(section, text, level=0, bold=True, font_size=12)

    if title.startswith("⚫"):
        text = title.lstrip("⚫").strip()
        return insert_bulleted_heading(section, text, level=0, bullet_char='·', bold=True, font_size=12)

    return insert_text(section, title, align="left", bold=True, font_size=12)

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
        raw_out, raw_title, raw_input, raw_instruction = row[:4]
        out_name = str(raw_out).strip() if raw_out else ""
        title = str(raw_title).strip() if raw_title else ""
        input_name = str(raw_input).strip() if raw_input else ""
        instruction = str(raw_instruction).strip() if raw_instruction else ""
        if not instruction:
            continue

        # Step 2: 確認欄位D需擷取內容
        is_all = instruction.lower() == "all"
        chapter_match = re.match(r"^([0-9]+(?:\.[0-9]+)*)(?:.*)", instruction)

        if is_all or chapter_match:
            # Step 1: 確認欄位C輸入檔案名稱
            if not input_name:
                logs.append(f"{out_name or '未命名'}: 未提供輸入檔案名稱")
                continue
            infile = _find_file(task_files_dir, input_name)
            if not infile:
                logs.append(f"{out_name or '未命名'}: 找不到檔案 {input_name}")
                continue

            # Step 3: 確認欄位A輸出檔案名稱
            doc, section = docs.get(out_name, (None, None))
            if doc is None:
                doc = Document()
                section = doc.AddSection()
                docs[out_name] = (doc, section)

            # Step 4: 確認欄位B需寫入文件的標題
            insert_title(section, title)

            # Step 5: 建構文件流程
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
            dest = os.path.join(task_files_dir, out_name or "output")
            if title:
                dest = os.path.join(dest, title)
            # Allow multiple keywords separated by commas (e.g. "Shipping simulation test, EO")
            # and ensure that matched files contain *all* keywords.
            keywords = [
                k.strip()
                for k in re.split(r"[,，]+", instruction)
                if k.strip()
            ]
            try:
                copied = copy_files(task_files_dir, dest, keywords)
                kw_display = ", ".join(keywords)
                logs.append(
                    f"複製 {len(copied)} 個檔案至 {os.path.relpath(dest, task_files_dir)} (關鍵字: {kw_display})"
                )
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
