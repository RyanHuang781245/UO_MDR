from pathlib import Path

from openpyxl import Workbook

from modules.mapping_processor import process_mapping_excel


def test_legacy_mapping_copy_writes_only_to_run_output(tmp_path: Path) -> None:
    task_files_dir = tmp_path / "task-files"
    source_dir = task_files_dir / "source-folder"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "keyword-hit.pdf").write_bytes(b"pdf")

    mapping_path = tmp_path / "legacy-mapping.xlsx"
    wb = Workbook()
    ws = wb.active
    ws["A1"] = "legacy"
    ws.append([])
    ws.append(["package", "", "", "source-folder", "keyword"])
    wb.save(mapping_path)

    output_dir = tmp_path / "mapping-output"

    result = process_mapping_excel(
        str(mapping_path),
        str(task_files_dir),
        str(output_dir),
        log_dir=str(output_dir),
    )

    assert result["logs"] == ["Copied 1 files to package (keywords keyword)"]
    assert (output_dir / "package" / "keyword-hit.pdf").is_file()
    assert not (task_files_dir / "package").exists()
