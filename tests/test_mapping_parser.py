import os
import zipfile
from xml.sax.saxutils import escape

from modules.mapping_parser import parse_mapping_file


def make_xlsx(path, rows):
    shared = []
    def idx(val):
        if val not in shared:
            shared.append(val)
        return shared.index(val)
    sheet_rows = []
    for r_idx, row in enumerate(rows, start=1):
        cells = []
        for c_idx, val in enumerate(row):
            if val is None:
                continue
            cell_ref = chr(ord('A') + c_idx) + str(r_idx)
            cells.append(f'<c r="{cell_ref}" t="s"><v>{idx(val)}</v></c>')
        sheet_rows.append(f'<row r="{r_idx}">' + ''.join(cells) + '</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetData>' + ''.join(sheet_rows) + '</sheetData></worksheet>'
    )
    sst_items = ''.join(f'<si><t>{escape(s)}</t></si>' for s in shared)
    shared_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="{len(shared)}" uniqueCount="{len(shared)}">'
        + sst_items + '</sst>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>'
        '</Relationships>'
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>'
        '</Types>'
    )
    with zipfile.ZipFile(path, 'w') as z:
        z.writestr('[Content_Types].xml', content_types)
        z.writestr('_rels/.rels', rels_xml)
        z.writestr('xl/workbook.xml', workbook_xml)
        z.writestr('xl/_rels/workbook.xml.rels', workbook_rels)
        z.writestr('xl/worksheets/sheet1.xml', sheet_xml)
        z.writestr('xl/sharedStrings.xml', shared_xml)


def test_parse_mapping_file(tmp_path):
    rows = [
        ['H1', 'H2', 'H3', 'H4'],
        ['H5', 'H6', 'H7', 'H8'],
        ['DocA', 'Heading1', 'FileOne', 'all'],
        ['DocA', 'Heading2', 'FileTwo', '6.1.2 Something'],
        ['FolderA', 'Sub', 'EO', None],
    ]
    mapping = tmp_path / 'map.xlsx'
    make_xlsx(mapping, rows)
    files_dir = tmp_path / 'files'
    files_dir.mkdir()
    (files_dir / 'FileOne.docx').write_text('x')
    (files_dir / 'FileTwo.docx').write_text('y')
    docs, copies = parse_mapping_file(str(mapping), str(files_dir))
    assert 'DocA' in docs
    steps = docs['DocA']
    assert steps[0]['type'] == 'insert_text'
    assert steps[1]['type'] == 'extract_word_all_content'
    assert steps[1]['params']['input_file'].endswith('FileOne.docx')
    assert steps[3]['type'] == 'extract_word_chapter'
    assert steps[3]['params']['target_chapter_section'] == '6.1.2'
    assert copies and copies[0]['dest'].endswith(os.path.join('FolderA', 'Sub'))
    assert copies[0]['keywords'] == ['EO']
