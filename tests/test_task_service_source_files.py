import stat

from app.services.task_service import _copytree_with_count, gather_available_files


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


def test_copytree_with_count_normalizes_imported_permissions(tmp_path):
    src = tmp_path / "src"
    src_child = src / "readonly-dir"
    dest = tmp_path / "dest"
    src_child.mkdir(parents=True)
    src_file = src_child / "readonly.txt"
    src_file.write_text("copy me", encoding="utf-8")
    src_child.chmod(0o555)
    src_file.chmod(0o444)

    copied = _copytree_with_count(str(src), str(dest))

    dest_child_mode = stat.S_IMODE((dest / "readonly-dir").stat().st_mode)
    dest_file_mode = stat.S_IMODE((dest / "readonly-dir" / "readonly.txt").stat().st_mode)
    assert copied == 1
    assert dest_child_mode & stat.S_IWUSR
    assert dest_child_mode & stat.S_IWGRP
    assert dest_file_mode & stat.S_IWUSR
    assert dest_file_mode & stat.S_IWGRP
