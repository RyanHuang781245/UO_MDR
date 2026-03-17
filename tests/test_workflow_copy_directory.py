from pathlib import Path

from modules.workflow import SUPPORTED_STEPS, run_workflow


def test_supported_steps_include_copy_directory() -> None:
    step = SUPPORTED_STEPS["copy_directory"]

    assert step["label"] == "複製資料夾"
    assert step["accepts"]["source_dir"] == "file:dir"
    assert step["accepts"]["dest_dir"] == "file:dir"
    assert step["accepts"]["keywords"] == "text"


def test_workflow_copy_directory_preserves_source_folder_name(tmp_path: Path) -> None:
    source_dir = tmp_path / "source_bundle"
    nested_dir = source_dir / "nested"
    nested_dir.mkdir(parents=True)
    (source_dir / "root.txt").write_text("root", encoding="utf-8")
    (nested_dir / "child.txt").write_text("child", encoding="utf-8")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()

    steps = [
        {
            "type": "copy_directory",
            "params": {
                "source_dir": str(source_dir),
                "dest_dir": str(dest_root),
            },
        }
    ]

    result = run_workflow(steps, str(tmp_path / "job_copy_directory"))
    entry = next(e for e in result["log_json"] if e.get("type") == "copy_directory")

    copied_dir = dest_root / "source_bundle"
    assert entry["status"] == "ok"
    assert Path(entry["copied_dir"]) == copied_dir
    assert (copied_dir / "root.txt").read_text(encoding="utf-8") == "root"
    assert (copied_dir / "nested" / "child.txt").read_text(encoding="utf-8") == "child"


def test_workflow_copy_directory_adds_product_suffix_on_name_conflict(tmp_path: Path) -> None:
    knee_ifu = (
        tmp_path
        / "輸入-測試路徑"
        / "TD-III-011-USTAR II Knee System"
        / "Section 2_Information Supplied by the Manufacturer"
        / "IFU"
    )
    hip_ifu = (
        tmp_path
        / "輸入-測試路徑"
        / "TD-III-012-USTAR II Hip System"
        / "Section 2_Information Supplied by the Manufacturer"
        / "IFU"
    )
    knee_ifu.mkdir(parents=True)
    hip_ifu.mkdir(parents=True)
    (knee_ifu / "knee.txt").write_text("knee", encoding="utf-8")
    (hip_ifu / "hip.txt").write_text("hip", encoding="utf-8")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()

    steps = [
        {
            "type": "copy_directory",
            "params": {
                "source_dir": str(knee_ifu),
                "dest_dir": str(dest_root),
            },
        },
        {
            "type": "copy_directory",
            "params": {
                "source_dir": str(hip_ifu),
                "dest_dir": str(dest_root),
            },
        },
    ]

    result = run_workflow(steps, str(tmp_path / "job_copy_directory_conflict"))
    copy_entries = [e for e in result["log_json"] if e.get("type") == "copy_directory"]

    assert [Path(e["copied_dir"]).name for e in copy_entries] == ["IFU_knee", "IFU_hip"]
    assert not (dest_root / "IFU").exists()
    assert (dest_root / "IFU_knee" / "knee.txt").read_text(encoding="utf-8") == "knee"
    assert (dest_root / "IFU_hip" / "hip.txt").read_text(encoding="utf-8") == "hip"


def test_workflow_copy_directory_can_copy_matching_named_subfolders(tmp_path: Path) -> None:
    source_root = tmp_path / "source_root"
    knee_ifu = source_root / "Knee System" / "IFU"
    hip_ifu = source_root / "Hip System" / "IFU"
    other_folder = source_root / "Misc System" / "Label sample"
    knee_ifu.mkdir(parents=True)
    hip_ifu.mkdir(parents=True)
    other_folder.mkdir(parents=True)
    (knee_ifu / "knee.txt").write_text("knee", encoding="utf-8")
    (hip_ifu / "hip.txt").write_text("hip", encoding="utf-8")
    (other_folder / "label.txt").write_text("label", encoding="utf-8")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()

    steps = [
        {
            "type": "copy_directory",
            "params": {
                "source_dir": str(source_root),
                "dest_dir": str(dest_root),
                "keywords": "IFU",
            },
        }
    ]

    result = run_workflow(steps, str(tmp_path / "job_copy_directory_keywords"))
    entry = next(e for e in result["log_json"] if e.get("type") == "copy_directory")

    assert entry["status"] == "ok"
    assert sorted(Path(p).name for p in entry["copied_dirs"]) == ["IFU_hip", "IFU_knee"]
    assert (dest_root / "IFU_knee" / "knee.txt").read_text(encoding="utf-8") == "knee"
    assert (dest_root / "IFU_hip" / "hip.txt").read_text(encoding="utf-8") == "hip"
    assert not (dest_root / "Label sample").exists()
