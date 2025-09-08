import os
import os
import os
import zipfile
import xml.etree.ElementTree as ET

from modules.mapping_parser import parse_mapping_file


def create_mapping(path):
    strings = [
        "doc",
        "title",
        "file",
        "instruction",
        "Doc1",
        "Heading1",
        "a.docx",
        "1.2 Intro",
        "Heading2",
        "b.docx",
        "all",
        "Doc2",
        "CopySec",
        "keyword",
    ]

    # sharedStrings.xml
    ss = ET.Element(
        "sst",
        xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        count=str(len(strings)),
        uniqueCount=str(len(strings)),
    )
    for s in strings:
        si = ET.SubElement(ss, "si")
        t = ET.SubElement(si, "t")
        t.text = s
    shared_strings = ET.tostring(ss, encoding="utf-8", xml_declaration=True)

    # sheet1.xml with shared string references
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    sheet = ET.Element("worksheet", xmlns=ns)
    sheetData = ET.SubElement(sheet, "sheetData")

    def add_row(r_idx, indices):
        row = ET.SubElement(sheetData, "row", r=str(r_idx))
        for c_idx, idx in enumerate(indices):
            if idx is None:
                continue
            col = chr(65 + c_idx)
            c = ET.SubElement(row, "c", r=f"{col}{r_idx}", t="s")
            v = ET.SubElement(c, "v")
            v.text = str(idx)

    add_row(1, [0, 1, 2, 3])
    add_row(2, [4, 5, 6, 7])
    add_row(3, [4, 8, 9, 10])
    add_row(4, [11, 12, None, 13])
    sheet_xml = ET.tostring(sheet, encoding="utf-8", xml_declaration=True)

    content_types = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">
  <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>
  <Default Extension=\"xml\" ContentType=\"application/xml\"/>
  <Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>
  <Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>
  <Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>
  <Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>
  <Override PartName=\"/docProps/core.xml\" ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>
  <Override PartName=\"/docProps/app.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>
</Types>"""

    rels_rels = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">
  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>
  <Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>
  <Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>
</Relationships>"""

    workbook = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">
  <sheets>
    <sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/>
  </sheets>
</workbook>"""

    workbook_rels = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">
  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/>
  <Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" Target=\"sharedStrings.xml\"/>
  <Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/>
</Relationships>"""

    styles = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">
  <fonts count=\"1\"><font><sz val=\"11\"/><color theme=\"1\"/><name val=\"Calibri\"/><family val=\"2\"/></font></fonts>
  <fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills>
  <borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>
  <cellXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/></cellXfs>
</styleSheet>"""

    core = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">
  <dc:creator>test</dc:creator>
  <cp:lastModifiedBy>test</cp:lastModifiedBy>
  <dcterms:created xsi:type=\"dcterms:W3CDTF\">2024-01-01T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type=\"dcterms:W3CDTF\">2024-01-01T00:00:00Z</dcterms:modified>
</cp:coreProperties>"""

    app = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">
  <Application>Python</Application>
</Properties>"""

    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels_rels)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/styles.xml", styles)
        zf.writestr("xl/sharedStrings.xml", shared_strings)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        zf.writestr("docProps/core.xml", core)
        zf.writestr("docProps/app.xml", app)


def test_parse_mapping_file(tmp_path):
    mapping_path = tmp_path / "map.xlsx"
    create_mapping(mapping_path)

    files_dir = tmp_path / "files"
    files_dir.mkdir()
    (files_dir / "a.docx").write_text("")
    (files_dir / "b.docx").write_text("")

    flows, copies = parse_mapping_file(str(mapping_path), str(files_dir))

    assert "Doc1" in flows
    steps = flows["Doc1"]
    assert len(steps) == 4
    assert steps[0]["type"] == "insert_numbered_heading"
    assert steps[1]["type"] == "extract_word_chapter"
    assert steps[1]["params"]["target_chapter_section"] == "1.2"
    assert steps[3]["type"] == "extract_word_all_content"

    assert copies
    job = copies[0]
    assert job["dest"].endswith(os.path.join("Doc2", "CopySec"))
    assert job["keywords"] == ["keyword"]
