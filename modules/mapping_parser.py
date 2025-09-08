"""Utilities for parsing mapping_file.xlsx and building workflows.

This module reads a simple mapping spreadsheet and converts it into
workflow steps compatible with :mod:`modules.workflow`. It also
generates file-copy instructions based on keyword rows.
"""

import os
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Iterable

NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}

def _col_to_index(col: str) -> int:
    idx = 0
    for c in col:
        idx = idx * 26 + (ord(c) - ord('A') + 1)
    return idx - 1

def _read_xlsx_rows(path: str) -> List[List[str]]:
    with zipfile.ZipFile(path, 'r') as zf:
        sheet = ET.fromstring(zf.read('xl/worksheets/sheet1.xml'))
        shared = []
        if 'xl/sharedStrings.xml' in zf.namelist():
            ss = ET.fromstring(zf.read('xl/sharedStrings.xml'))
            for si in ss.findall('m:si', NS):
                text = ''.join(t for t in si.itertext())
                shared.append(text)
        rows: List[List[str]] = []
        for row in sheet.find('m:sheetData', NS).findall('m:row', NS):
            data = [''] * 4
            for c in row.findall('m:c', NS):
                ref = c.get('r')
                if not ref:
                    continue
                col_letters = re.match(r'[A-Z]+', ref).group(0)
                ci = _col_to_index(col_letters)
                if ci >= 4:
                    continue
                value = ''
                t = c.get('t')
                v = c.find('m:v', NS)
                if v is not None:
                    if t == 's':
                        value = shared[int(v.text)] if v.text is not None else ''
                    else:
                        value = v.text or ''
                data[ci] = value
            rows.append(data)
        return rows

def parse_mapping_file(mapping_path: str, files_dir: str) -> Tuple[Dict[str, List[Dict[str, Dict[str, str]]]], List[Dict[str, Iterable[str]]]]:
    """Parse mapping file to build workflow steps and copy operations.

    Returns
    -------
    (docs, copies):
        docs: mapping of document name to list of workflow step dicts.
        copies: list of dicts with keys source, dest, keywords.
    """
    rows = _read_xlsx_rows(mapping_path)
    rows = rows[2:]  # skip first two header rows
    docs: Dict[str, List[Dict[str, Dict[str, str]]]] = {}
    copies: List[Dict[str, Iterable[str]]] = []

    def find_file(name: str) -> str:
        low = name.lower()
        for root, _dirs, files in os.walk(files_dir):
            for fn in files:
                if low in fn.lower():
                    return os.path.join(root, fn)
        return ''

    for a, b, c, d in rows:
        if not any([a, b, c, d]):
            continue
        doc_name = a.strip() if a else ''
        heading = b.strip() if b else ''
        input_name = c.strip() if c else ''
        section = d.strip() if d else ''
        if section and (section.lower() == 'all' or re.match(r'^\d+(?:\.\d+)*', section)):
            steps = docs.setdefault(doc_name, [])
            if heading:
                steps.append({'type': 'insert_text', 'params': {'text': heading}})
            file_path = find_file(input_name)
            if section.lower() == 'all':
                steps.append({'type': 'extract_word_all_content', 'params': {'input_file': file_path}})
            else:
                m = re.match(r'^\s*(\d+(?:\.\d+)*)(?:[^,]*)(?:,\s*(.+))?$', section)
                if not m:
                    continue
                chapter = m.group(1)
                title = m.group(2) or ''
                params = {
                    'input_file': file_path,
                    'target_chapter_section': chapter,
                }
                if title:
                    params['target_title'] = 'true'
                    params['target_title_section'] = title
                steps.append({'type': 'extract_word_chapter', 'params': params})
        else:
            keywords = [k.strip() for k in input_name.split(',') if k.strip()]
            dest = os.path.join(files_dir, doc_name)
            if heading:
                dest = os.path.join(dest, heading)
            copies.append({'source': files_dir, 'dest': dest, 'keywords': keywords})
    return docs, copies
