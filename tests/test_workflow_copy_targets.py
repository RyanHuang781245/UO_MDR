from pathlib import Path

from modules.workflow import SUPPORTED_STEPS, run_workflow


def test_supported_copy_steps_include_target_name() -> None:
    assert "target_name" in SUPPORTED_STEPS["copy_files"]["inputs"]
    assert "target_name" in SUPPORTED_STEPS["copy_directory"]["inputs"]
    assert SUPPORTED_STEPS["copy_files"]["accepts"]["target_name"] == "text"
    assert SUPPORTED_STEPS["copy_directory"]["accepts"]["target_name"] == "text"


def test_workflow_copy_directory_can_override_target_name(tmp_path: Path) -> None:
    source_dir = tmp_path / "source_bundle"
    source_dir.mkdir()
    (source_dir / "root.txt").write_text("root", encoding="utf-8")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()

    result = run_workflow(
        [
            {
                "type": "copy_directory",
                "params": {
                    "source_dir": str(source_dir),
                    "dest_dir": str(dest_root),
                    "target_name": "renamed_bundle",
                },
            }
        ],
        str(tmp_path / "job_copy_directory_rename"),
    )

    entry = next(e for e in result["log_json"] if e.get("type") == "copy_directory")
    assert Path(entry["copied_dir"]).name == "renamed_bundle"
    assert (dest_root / "renamed_bundle" / "root.txt").read_text(encoding="utf-8") == "root"


def test_workflow_copy_directory_keyword_single_match_can_override_target_name(tmp_path: Path) -> None:
    source_root = tmp_path / "source_root"
    matched_dir = source_root / "IFU"
    matched_dir.mkdir(parents=True)
    (matched_dir / "ifu.txt").write_text("ifu", encoding="utf-8")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()

    result = run_workflow(
        [
            {
                "type": "copy_directory",
                "params": {
                    "source_dir": str(source_root),
                    "dest_dir": str(dest_root),
                    "keywords": "IFU",
                    "target_name": "IFU_custom",
                },
            }
        ],
        str(tmp_path / "job_copy_directory_keyword_rename"),
    )

    entry = next(e for e in result["log_json"] if e.get("type") == "copy_directory")
    assert entry["copied_dirs"] == [str(dest_root / "IFU_custom")]
    assert (dest_root / "IFU_custom" / "ifu.txt").read_text(encoding="utf-8") == "ifu"


def test_workflow_copy_files_single_match_can_override_target_name(tmp_path: Path) -> None:
    source_root = tmp_path / "source_root"
    source_root.mkdir()
    (source_root / "demo_test.docx").write_text("demo", encoding="utf-8")
    (source_root / "other.docx").write_text("other", encoding="utf-8")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()

    result = run_workflow(
        [
            {
                "type": "copy_files",
                "params": {
                    "source_dir": str(source_root),
                    "dest_dir": str(dest_root),
                    "keywords": "demo,test",
                    "target_name": "renamed_output",
                },
            }
        ],
        str(tmp_path / "job_copy_files_rename"),
    )

    entry = next(e for e in result["log_json"] if e.get("type") == "copy_files")
    assert entry["copied_files"] == [str(dest_root / "renamed_output.docx")]
    assert (dest_root / "renamed_output.docx").read_text(encoding="utf-8") == "demo"
