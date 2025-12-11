import os
import pytest

from app import app, deduplicate_name, validate_nas_path


def test_validate_nas_path_rules(tmp_path):
    allowed_root = tmp_path / "nas"
    allowed_root.mkdir()
    target_file = allowed_root / "project" / "doc.docx"
    target_file.parent.mkdir(parents=True)
    target_file.touch()

    with pytest.raises(ValueError):
        validate_nas_path("", [str(allowed_root)])
    with pytest.raises(ValueError):
        validate_nas_path("/abs/path.docx", [str(allowed_root)])
    with pytest.raises(ValueError):
        validate_nas_path("../escape.docx", [str(allowed_root)])
    with pytest.raises(FileNotFoundError):
        validate_nas_path("missing.docx", [str(allowed_root)])

    resolved = validate_nas_path("project/doc.docx", [str(allowed_root)])
    assert os.path.abspath(resolved) == os.path.abspath(target_file)


def test_deduplicate_name(tmp_path):
    base = tmp_path / "files"
    base.mkdir()
    existing = base / "report.docx"
    existing.touch()
    (base / "folder").mkdir()

    assert deduplicate_name(str(base), "report.docx") == "report (1).docx"
    assert deduplicate_name(str(base), "folder") == "folder (1)"


def test_upload_task_file_from_nas(tmp_path):
    client = app.test_client()
    original_task_folder = app.config["TASK_FOLDER"]
    original_roots = app.config.get("ALLOWED_SOURCE_ROOTS")

    task_dir = tmp_path / "tasks"
    files_dir = task_dir / "task1" / "files"
    files_dir.mkdir(parents=True)

    allowed_root = tmp_path / "nas"
    allowed_root.mkdir()
    source_file = allowed_root / "docs" / "a.docx"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("content")

    source_dir = allowed_root / "bundle"
    nested = source_dir / "nested"
    nested.mkdir(parents=True)
    (nested / "b.pdf").write_text("pdf")

    try:
        app.config["TASK_FOLDER"] = str(task_dir)
        app.config["ALLOWED_SOURCE_ROOTS"] = [str(allowed_root)]

        resp = client.post("/tasks/task1/files", data={"nas_file_path": "docs/a.docx"})
        assert resp.status_code == 302
        assert (files_dir / "a.docx").exists()

        resp = client.post("/tasks/task1/files", data={"nas_file_path": "docs/a.docx"})
        assert resp.status_code == 302
        assert (files_dir / "a (1).docx").exists()

        resp = client.post("/tasks/task1/files", data={"nas_file_path": "bundle"})
        assert resp.status_code == 302
        assert (files_dir / "bundle").is_dir()
        assert (files_dir / "bundle" / "nested" / "b.pdf").exists()
    finally:
        app.config["TASK_FOLDER"] = original_task_folder
        app.config["ALLOWED_SOURCE_ROOTS"] = original_roots
