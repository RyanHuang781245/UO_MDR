from __future__ import annotations

import json
import zipfile
from pathlib import Path

from docx import Document as DocxDocument
from openpyxl import Workbook

from modules.mapping_processor import process_mapping_excel


HEADERS = [
    "檔案名稱/資料夾名稱/文字內容",
    "擷取段落/操作",
    "類型",
    "包含標題",
    "檔案路徑",
    "檔案名稱",
    "模板文件",
    "插入段落名稱/目的資料夾名稱",
]


def _write_mapping(path: Path, rows: list[list[str]]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(HEADERS)
    for row in rows:
        ws.append(row)
    wb.save(path)


def _run_validate_mapping(
    tmp_path: Path,
    operation: str,
    item_type: str = "",
    include_title: str = "",
    source_name: str = "source.docx",
    source_files: list[str] | None = None,
) -> tuple[dict, dict]:
    files_dir = tmp_path / "files"
    out_dir = tmp_path / "output"
    log_dir = tmp_path / "logs"
    files_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "out").mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    for rel_path in source_files or ["source.docx"]:
        src = files_dir / rel_path
        src.parent.mkdir(parents=True, exist_ok=True)
        src.write_text("dummy", encoding="utf-8")

    mapping_path = tmp_path / "mapping.xlsx"
    rows = [[source_name, operation, item_type, include_title, "out", "result.docx", "", ""]]
    _write_mapping(mapping_path, rows)

    result = process_mapping_excel(
        str(mapping_path),
        str(files_dir),
        str(out_dir),
        log_dir=str(log_dir),
        validate_only=True,
    )

    log_data = {"messages": [], "runs": []}
    log_file = result.get("log_file")
    if log_file:
        with open(log_dir / log_file, "r", encoding="utf-8") as f:
            log_data = json.load(f)
    return result, log_data


def _first_step_params(log_data: dict) -> dict:
    runs = log_data.get("runs") or []
    assert runs, "expected at least one run entry"
    steps = runs[0].get("steps") or []
    assert steps, "expected at least one workflow step"
    return (steps[0].get("params") or {})


def _first_step_type(log_data: dict) -> str:
    runs = log_data.get("runs") or []
    assert runs, "expected at least one run entry"
    steps = runs[0].get("steps") or []
    assert steps, "expected at least one workflow step"
    return str(steps[0].get("type") or "")


def test_mapping_blank_type_defaults_to_extract_chapter(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 General description", item_type="")
    assert _first_step_type(log_data) == "extract_word_chapter"
    params = _first_step_params(log_data)
    assert params.get("target_chapter_section") == "1.1"
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_figure_tail_title(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 Figure 1|title=Overview Figure")
    params = _first_step_params(log_data)
    assert params.get("target_caption_label") == "Figure 1"
    assert params.get("target_figure_title") == "Overview Figure"
    assert "target_figure_index" not in params
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_figure_tail_index(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 Figure 1|index=2")
    params = _first_step_params(log_data)
    assert params.get("target_caption_label") == "Figure 1"
    assert params.get("target_figure_index") == 2
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_figure_tail_title_and_index(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 Figure 1|title=System Overview|index=3")
    params = _first_step_params(log_data)
    assert params.get("target_figure_title") == "System Overview"
    assert params.get("target_figure_index") == 3
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_tail_requires_figure_or_table_label(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 Chapter|title=No Label")
    assert any("使用 title/index 參數時必須指定 Figure 或 Table 標籤" in msg for msg in result.get("logs", []))
    runs = log_data.get("runs") or []
    assert runs
    assert all(not (run.get("steps") or []) for run in runs)


def test_mapping_type_figure_allows_tail_without_label(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 General description|title=System overview|index=2",
        item_type="Figure",
    )
    assert _first_step_type(log_data) == "extract_specific_figure_from_word"
    params = _first_step_params(log_data)
    assert params.get("target_caption_label") == ""
    assert params.get("target_figure_title") == "System overview"
    assert params.get("target_figure_index") == 2
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_type_table_allows_tail_without_label(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 General description|title=Table summary|index=1",
        item_type="Table",
    )
    assert _first_step_type(log_data) == "extract_specific_table_from_word"
    params = _first_step_params(log_data)
    assert params.get("target_caption_label") == ""
    assert params.get("target_table_title") == "Table summary"
    assert params.get("target_table_index") == 1
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_type_conflicts_with_label(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 Table 1", item_type="Figure")
    assert any("類型欄位與操作內容不一致" in msg for msg in result.get("logs", []))
    runs = log_data.get("runs") or []
    assert runs
    assert all(not (run.get("steps") or []) for run in runs)


def test_mapping_type_figure_requires_caption_or_title_or_index(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 General description", item_type="Figure")
    assert any("Figure 擷取至少需提供 caption、title 或 index 其中之一" in msg for msg in result.get("logs", []))
    runs = log_data.get("runs") or []
    assert runs
    assert all(not (run.get("steps") or []) for run in runs)


def test_mapping_figure_index_validation(tmp_path: Path) -> None:
    bad_result, _ = _run_validate_mapping(tmp_path / "bad", "1.1 Figure 1|index=abc")
    zero_result, _ = _run_validate_mapping(tmp_path / "zero", "1.1 Figure 1|index=0")
    assert any("index 必須是正整數: abc" in msg for msg in bad_result.get("logs", []))
    assert any("index 必須大於 0: 0" in msg for msg in zero_result.get("logs", []))


def test_mapping_table_tail_behavior_unchanged(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(tmp_path, "1.1 Table 1|title=Table Name|index=4")
    params = _first_step_params(log_data)
    assert params.get("target_caption_label") == "Table 1"
    assert params.get("target_table_title") == "Table Name"
    assert params.get("target_table_index") == 4
    assert "target_figure_title" not in params
    assert "target_figure_index" not in params
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_chapter_include_title_false_sets_hide_flag(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 General description",
        include_title="FALSE",
    )
    params = _first_step_params(log_data)
    assert params.get("hide_chapter_title") is True
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_figure_include_title_false_disables_caption(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 Figure 1|title=Overview Figure",
        include_title="否",
    )
    params = _first_step_params(log_data)
    assert params.get("include_caption") is False
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_table_include_title_false_disables_caption(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 Table 1|title=Table Name",
        include_title="N",
    )
    params = _first_step_params(log_data)
    assert params.get("include_caption") is False
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_include_title_invalid_value_errors(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 General description",
        include_title="maybe",
    )
    assert any("包含標題欄位值無效: maybe" in msg for msg in result.get("logs", []))
    runs = log_data.get("runs") or []
    assert not runs or all(not (run.get("steps") or []) for run in runs)


def test_mapping_source_relative_path_resolves_file(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 General description",
        source_name="FolderA/source.docx",
        source_files=["FolderA/source.docx"],
    )
    params = _first_step_params(log_data)
    assert Path(str(params.get("input_file", ""))).name == "source.docx"
    assert "FolderA" in str(params.get("input_file", ""))
    assert not any("ERROR:" in msg for msg in result.get("logs", []))


def test_mapping_duplicate_filename_requires_relative_path(tmp_path: Path) -> None:
    result, log_data = _run_validate_mapping(
        tmp_path,
        "1.1 General description",
        source_name="source.docx",
        source_files=["FolderA/source.docx", "FolderB/source.docx"],
    )
    assert any("multiple files found for source.docx" in msg for msg in result.get("logs", []))
    runs = log_data.get("runs") or []
    assert runs
    assert all(not (run.get("steps") or []) for run in runs)


def test_mapping_outputs_are_packaged_into_zip(tmp_path: Path, monkeypatch) -> None:
    files_dir = tmp_path / "files"
    out_dir = tmp_path / "output"
    log_dir = tmp_path / "logs"
    files_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    src = files_dir / "source.docx"
    src.write_text("dummy", encoding="utf-8")

    mapping_path = tmp_path / "mapping.xlsx"
    rows = [
        ["source.docx", "1.1 General description", "", "", "pkg/A", "a.docx", "", ""],
        ["source.docx", "1.2 General description", "", "", "pkg/B", "b.docx", "", ""],
    ]
    _write_mapping(mapping_path, rows)

    def fake_run_workflow(steps, workdir, template=None):
        result_docx = Path(workdir) / "result.docx"
        doc = DocxDocument()
        doc.add_paragraph(f"generated-{len(steps)}")
        doc.save(result_docx)
        return {"result_docx": str(result_docx), "log_json": []}

    monkeypatch.setattr("modules.mapping_processor.run_workflow", fake_run_workflow)
    monkeypatch.setattr("modules.mapping_processor.apply_basic_style", lambda *args, **kwargs: True)
    monkeypatch.setattr("modules.mapping_processor.remove_hidden_runs", lambda *args, **kwargs: True)
    monkeypatch.setattr("modules.mapping_processor.hide_paragraphs_with_text", lambda *args, **kwargs: True)

    result = process_mapping_excel(
        str(mapping_path),
        str(files_dir),
        str(out_dir),
        log_dir=str(log_dir),
        validate_only=False,
    )

    zip_file = result.get("zip_file")
    assert zip_file
    zip_path = out_dir / zip_file
    assert zip_path.is_file()
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = sorted(zf.namelist())
    assert names == ["pkg/A/a.docx", "pkg/B/b.docx"]
