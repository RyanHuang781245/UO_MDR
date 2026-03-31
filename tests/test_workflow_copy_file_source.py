from pathlib import Path

from app.blueprints.flows.execution_helpers import _resolve_runtime_step_params
from app.blueprints.flows.flow_file_helpers import _normalize_step_file_value
from modules.workflow import SUPPORTED_STEPS, run_workflow


def test_copy_files_step_accepts_file_or_directory_source() -> None:
    assert SUPPORTED_STEPS["copy_files"]["accepts"]["source_dir"] == "file:path"


def test_copy_files_root_directory_value_is_preserved_for_mixed_path_source() -> None:
    assert _normalize_step_file_value(".", "file:path") == "."


def test_copy_files_runtime_resolves_root_directory_for_mixed_path_source(tmp_path: Path) -> None:
    files_dir = tmp_path / "files"
    files_dir.mkdir()

    params = _resolve_runtime_step_params(
        str(files_dir),
        SUPPORTED_STEPS["copy_files"],
        {"source_dir": ".", "keywords": "IFU"},
    )

    assert params["source_dir"] == str(files_dir)


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
