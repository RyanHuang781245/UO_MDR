from pathlib import Path

from modules.workflow import SUPPORTED_STEPS, run_workflow


def test_copy_files_step_accepts_file_or_directory_source() -> None:
    assert SUPPORTED_STEPS["copy_files"]["accepts"]["source_dir"] == "file:path"


def test_workflow_copy_files_can_copy_single_selected_file(tmp_path: Path) -> None:
    source_root = tmp_path / "source_root"
    source_root.mkdir()
    source_file = source_root / "ifu_demo.docx"
    source_file.write_text("demo", encoding="utf-8")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()

    result = run_workflow(
        [
            {
                "type": "copy_files",
                "params": {
                    "source_dir": str(source_file),
                    "dest_dir": str(dest_root),
                    "keywords": "ignored,keyword",
                    "target_name": "renamed_output",
                },
            }
        ],
        str(tmp_path / "job_copy_single_selected_file"),
    )

    entry = next(e for e in result["log_json"] if e.get("type") == "copy_files")
    assert entry["copied_files"] == [str(dest_root / "renamed_output.docx")]
    assert (dest_root / "renamed_output.docx").read_text(encoding="utf-8") == "demo"
    assert entry["note"] == "已選擇單一來源檔案，已忽略關鍵字。"
