"""Utilities for parsing mapping Excel files into workflow steps.

This module reads an Excel mapping file with four columns:
A: output Word document name
B: heading title inside the Word document
C: input file name to search within task files
D: extraction instruction. When it contains a chapter number (e.g. "6.12.1"),
   the specified chapter is extracted via ``extract_word_chapter``. When the
   value is ``all`` (case-insensitive) the entire document is extracted via
   ``extract_word_all_content``. Otherwise the value is treated as keywords for
   file copying.

The parsing result consists of two collections:
- A dictionary mapping each output document name to a list of workflow steps
  (heading insertion and content extraction steps) that can later be executed
  by ``run_workflow``.
- A list of copy jobs describing keyword based file copying tasks.
"""
from __future__ import annotations

import os
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Optional

# Type aliases for clarity
Workflow = Dict[str, List[Dict[str, dict]]]
CopyJob = Dict[str, object]

def _find_file(base_dir: str, name: str) -> Optional[str]:
    """Search for a file whose basename matches ``name`` (case-insensitive)."""
    if not name:
        return None
    lowered = name.lower()
    for root, _dirs, files in os.walk(base_dir):
        for fn in files:
            if fn.lower() == lowered:
                return os.path.join(root, fn)
    return None


def _column_index(col_ref: str) -> int:
    idx = 0
    for ch in col_ref:
        if 'A' <= ch <= 'Z':
            idx = idx * 26 + (ord(ch) - 64)
    return idx - 1


def _read_rows(xlsx_path: str) -> List[List[str]]:
    """Very small helper to read rows from the first worksheet of an XLSX file."""
    rows: List[List[str]] = []
    with zipfile.ZipFile(xlsx_path) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            ns = {"t": root.tag.split('}')[0].strip('{')}
            for si in root.findall(".//t:si", ns):
                text_parts = [t.text or "" for t in si.findall(".//t:t", ns)]
                shared_strings.append("".join(text_parts))
        sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        ns_sheet = {"t": sheet.tag.split('}')[0].strip('{')}
        for row in sheet.findall(".//t:row", ns_sheet):
            cells: List[str] = []
            for c in row.findall("t:c", ns_sheet):
                r = c.get("r", "A1")
                col_letters = re.match(r"([A-Z]+)", r).group(1)
                idx = _column_index(col_letters)
                while len(cells) <= idx:
                    cells.append("")
                t = c.get("t")
                v = c.find("t:v", ns_sheet)
                val = v.text if v is not None else ""
                if t == "s" and val.isdigit():
                    sidx = int(val)
                    val = shared_strings[sidx] if sidx < len(shared_strings) else ""
                cells[idx] = val
            rows.append(cells)
    return rows

def parse_mapping_file(xlsx_path: str, task_files_dir: str) -> Tuple[Workflow, List[CopyJob]]:
    """Parse mapping instructions from an Excel file.

    Parameters
    ----------
    xlsx_path: str
        Path to the mapping Excel file.
    task_files_dir: str
        Base directory containing uploaded task files.

    Returns
    -------
    Tuple[Workflow, List[CopyJob]]
        ``Workflow`` maps output document names to lists of workflow steps.
        ``CopyJob`` items describe keyword based file copying operations.
    """
    workflows: Workflow = {}
    copy_jobs: List[CopyJob] = []

    rows = _read_rows(xlsx_path)
    for row in rows[1:]:  # skip header
        out_doc, heading, filename, instruction = row[:4]
        if not any([out_doc, heading, filename, instruction]):
            continue
        out_doc = str(out_doc).strip() if out_doc else ""  # group by document
        heading = str(heading).strip() if heading else ""
        filename = str(filename).strip() if filename else ""
        instruction = str(instruction).strip() if instruction else ""

        # Determine file path if a filename is provided
        file_path = _find_file(task_files_dir, filename) if filename else None

        # Normalise workflow list for this document
        if out_doc:
            steps = workflows.setdefault(out_doc, [])
        else:
            steps = workflows.setdefault("result", [])

        if instruction.lower() == "all" and file_path:
            # Extract entire document
            steps.append({
                "type": "insert_numbered_heading",
                "params": {"text": heading, "level": 1},
            })
            rel = os.path.relpath(file_path, task_files_dir)
            steps.append({
                "type": "extract_word_all_content",
                "params": {"input_file": rel},
            })
            continue

        m = re.match(r"([\d\.]+)\s*(.*)", instruction)
        if m and file_path:
            # Extract specific chapter (and optional title)
            chapter = m.group(1)
            title_section = m.group(2).strip()
            steps.append({
                "type": "insert_numbered_heading",
                "params": {"text": heading, "level": 1},
            })
            params = {
                "input_file": os.path.relpath(file_path, task_files_dir),
                "target_chapter_section": chapter,
                "target_title": bool(title_section),
                "target_title_section": title_section,
            }
            steps.append({"type": "extract_word_chapter", "params": params})
            continue

        # Otherwise treat as keywords for file copy
        keywords = [k.strip() for k in instruction.split(",") if k.strip()]
        dest_dir = os.path.join(task_files_dir, out_doc, heading)
        copy_jobs.append({
            "source": task_files_dir,
            "dest": dest_dir,
            "keywords": keywords or ([filename] if filename else []),
        })

    return workflows, copy_jobs
