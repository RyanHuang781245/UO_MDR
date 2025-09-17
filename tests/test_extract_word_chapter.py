from pathlib import Path

from docx import Document as DocxDocument
from spire.doc import Document, FileFormat, BuiltinStyle

from modules.Extract_AllFile_to_FinalWord import extract_word_chapter


def test_extract_word_chapter_excludes_title_from_output(tmp_path: Path) -> None:
    src = Document()
    sec = src.AddSection()
    sec.AddParagraph().AppendText("1.1 Sample Title")
    sec.AddParagraph().AppendText("Body text")
    src_path = tmp_path / "source.docx"
    src.SaveToFile(str(src_path), FileFormat.Docx)
    src.Close()

    out_doc = Document()
    out_section = out_doc.AddSection()

    result = extract_word_chapter(
        str(src_path),
        "1.1",
        output_doc=out_doc,
        section=out_section,
    )

    out_path = tmp_path / "out.docx"
    out_doc.SaveToFile(str(out_path), FileFormat.Docx)
    out_doc.Close()

    assert result == {"captured_titles": ["1.1 Sample Title"]}

    docx_doc = DocxDocument(out_path)
    paragraphs = [p for p in docx_doc.paragraphs if p.text.strip()]
    assert all(p.text != "1.1 Sample Title" for p in paragraphs)
    assert any("Body text" in p.text for p in paragraphs)


def test_extract_word_chapter_stops_at_next_section(tmp_path: Path) -> None:
    src = Document()
    sec = src.AddSection()
    p0 = sec.AddParagraph()
    p0.ApplyStyle(BuiltinStyle.Heading1)
    p0.AppendText("2 Section Title")
    sec.AddParagraph().AppendText("First paragraph")
    p1 = sec.AddParagraph()
    p1.ApplyStyle(BuiltinStyle.Heading2)
    p1.AppendText("2.1 Sub section")
    sec.AddParagraph().AppendText("Details")
    p2 = sec.AddParagraph()
    p2.ApplyStyle(BuiltinStyle.Heading1)
    p2.AppendText("3 Next Section")
    src_path = tmp_path / "src.docx"
    src.SaveToFile(str(src_path), FileFormat.Docx)
    src.Close()

    out_doc = Document()
    out_section = out_doc.AddSection()

    extract_word_chapter(
        str(src_path),
        "2",
        output_doc=out_doc,
        section=out_section,
    )

    out_path = tmp_path / "out.docx"
    out_doc.SaveToFile(str(out_path), FileFormat.Docx)
    out_doc.Close()

    docx_doc = DocxDocument(out_path)
    texts = [p.text.strip() for p in docx_doc.paragraphs if p.text.strip()]
    assert "2 Section Title" not in texts
    assert "3 Next Section" not in texts
    assert "First paragraph" in texts
    assert "Details" in texts


def test_extract_word_chapter_matches_title_without_number(tmp_path: Path) -> None:
    src = Document()
    sec = src.AddSection()
    sec.AddParagraph().AppendText("4.1 Complex Title")
    sec.AddParagraph().AppendText("Body A")
    sec.AddParagraph().AppendText("4.2 Other Title")
    src_path = tmp_path / "src.docx"
    src.SaveToFile(str(src_path), FileFormat.Docx)
    src.Close()

    out_doc = Document()
    out_section = out_doc.AddSection()

    result = extract_word_chapter(
        str(src_path),
        "4.1",
        target_title=True,
        target_title_section="Complex Title",
        output_doc=out_doc,
        section=out_section,
    )

    out_path = tmp_path / "out.docx"
    out_doc.SaveToFile(str(out_path), FileFormat.Docx)
    out_doc.Close()

    assert result == {"captured_titles": ["4.1 Complex Title"]}

    docx_doc = DocxDocument(out_path)
    texts = [p.text.strip() for p in docx_doc.paragraphs if p.text.strip()]
    assert "4.1 Complex Title" not in texts
    assert "4.2 Other Title" not in texts
    assert "Body A" in texts
