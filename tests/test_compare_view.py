from pathlib import Path

from spire.doc import Document, FileFormat

from app import app
from modules.workflow import run_workflow


def test_compare_view_includes_titles_to_hide(tmp_path: Path) -> None:
    original_testing = app.config.get("TESTING")
    original_task_folder = app.config.get("TASK_FOLDER")
    app.config["TESTING"] = True
    app.config["TASK_FOLDER"] = str(tmp_path)

    src = Document()
    sec = src.AddSection()
    sec.AddParagraph().AppendText("1.1 Sample Title")
    sec.AddParagraph().AppendText("Body")
    src_path = tmp_path / "source.docx"
    src.SaveToFile(str(src_path), FileFormat.Docx)
    src.Close()

    task_id = "task1"
    job_id = "job1"
    task_dir = tmp_path / task_id
    job_dir = task_dir / "jobs" / job_id
    job_dir.mkdir(parents=True)

    steps = [
        {
            "type": "extract_word_chapter",
            "params": {
                "input_file": str(src_path),
                "target_chapter_section": "1.1",
            },
        }
    ]

    try:
        run_workflow(steps, str(job_dir))

        client = app.test_client()
        resp = client.get(f"/tasks/{task_id}/compare/{job_id}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "TITLES_TO_HIDE" in body
        assert "1.1 Sample Title" in body
    finally:
        app.config["TASK_FOLDER"] = original_task_folder
        app.config["TESTING"] = original_testing
