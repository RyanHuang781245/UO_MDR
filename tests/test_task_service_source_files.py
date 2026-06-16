from app.services.task_service import gather_available_files


def test_gather_available_files_hides_office_lock_files(tmp_path):
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    (files_dir / "source.docx").write_text("ok", encoding="utf-8")
    (files_dir / "~$source.docx").write_text("lock", encoding="utf-8")
    (files_dir / "~$sheet.xlsx").write_text("lock", encoding="utf-8")

    files = gather_available_files(str(files_dir))

    assert files["docx"] == ["source.docx"]
    assert "~$source.docx" not in files["path"]
    assert "~$sheet.xlsx" not in files["path"]
