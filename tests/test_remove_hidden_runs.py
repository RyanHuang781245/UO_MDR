from pathlib import Path

from docx import Document

from modules.Extract_AllFile_to_FinalWord import remove_hidden_runs


def test_remove_hidden_runs_keeps_paragraph_in_table_cell(tmp_path: Path) -> None:
    doc = Document()
    table = doc.add_table(rows=1, cols=1)
    cell = table.cell(0, 0)
    para = cell.paragraphs[0]
    run = para.add_run("to hide")
    run.font.hidden = True

    doc_path = tmp_path / "table.docx"
    doc.save(doc_path)

    assert remove_hidden_runs(str(doc_path))

    updated = Document(doc_path)
    cell_after = updated.tables[0].cell(0, 0)

    # Even though all visible text was removed, the table cell must keep a paragraph
    assert len(cell_after.paragraphs) == 1
    assert cell_after.paragraphs[0].text == ""
