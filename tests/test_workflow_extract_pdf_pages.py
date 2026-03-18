from pathlib import Path

from docx import Document as DocxDocument

from modules.workflow import SUPPORTED_STEPS, run_workflow


def _create_docx(path: Path, text: str = "result") -> None:
    doc = DocxDocument()
    doc.add_paragraph(text)
    doc.save(path)


def test_supported_steps_include_extract_pdf_pages_as_images() -> None:
    step = SUPPORTED_STEPS["extract_pdf_pages_as_images"]

    assert step["label"] == "擷取 PDF 標籤圖片"
    assert step["accepts"]["input_file"] == "file:pdf"


def test_workflow_extract_pdf_pages_as_images_saves_fragment_and_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    src = tmp_path / "source.pdf"
    src.write_bytes(b"%PDF-1.4\n%stub\n")
    captured: dict[str, object] = {}

    def fake_extract_pdf_pages_to_docx(input_pdf, output_docx, **_kwargs):
        captured["input_pdf"] = input_pdf
        captured["output_docx"] = output_docx
        out_path = Path(str(output_docx))
        _create_docx(out_path, text="PDF page image")
        return {"output_docx": str(out_path), "pages": 3}

    monkeypatch.setattr(
        "modules.workflow.extract_pdf_pages_to_docx",
        fake_extract_pdf_pages_to_docx,
    )

    steps = [
        {
            "type": "extract_pdf_pages_as_images",
            "params": {
                "input_file": str(src),
            },
        }
    ]

    result = run_workflow(steps, str(tmp_path / "job_pdf_pages"))
    entry = next(e for e in result["log_json"] if e.get("type") == "extract_pdf_pages_as_images")

    assert entry["status"] == "ok"
    assert entry["pages"] == 3
    assert Path(entry["output_docx"]).is_file()
    assert Path(result["result_docx"]).is_file()
    assert captured["input_pdf"] == str(src)
