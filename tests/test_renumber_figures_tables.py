import os
from docx import Document
from modules.Extract_AllFile_to_FinalWord import renumber_figures_tables

def test_renumber_figures_tables(tmp_path):
    doc_path = tmp_path / "sample.docx"
    doc = Document()
    doc.add_paragraph("Figure 5 Example figure caption")
    doc.add_paragraph("This refers to Figure 5 and Table 10.")
    doc.add_paragraph("Table 10 Example table caption")
    doc.save(doc_path)

    assert renumber_figures_tables(str(doc_path))

    new_doc = Document(str(doc_path))
    texts = [p.text for p in new_doc.paragraphs]
    assert texts[0].startswith("Figure 1")
    assert texts[2].startswith("Table 1")
    assert "Figure 1" in texts[1]
    assert "Table 1" in texts[1]
