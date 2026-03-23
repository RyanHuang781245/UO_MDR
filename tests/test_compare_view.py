from pathlib import Path

from spire.doc import Document, FileFormat

from app.blueprints.tasks import routes as task_routes
from modules.workflow import run_workflow


def test_compare_view_includes_titles_to_hide(tmp_path: Path, app) -> None:
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
                "hide_chapter_title": "true",
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
        assert "PAGE_SOURCE_MAP" in body
        assert "1.1 Sample Title" in body
        assert "source.docx" in body
    finally:
        app.config["TASK_FOLDER"] = original_task_folder
        app.config["TESTING"] = original_testing


def test_compare_view_disambiguates_same_basename_sources(tmp_path: Path, app) -> None:
    original_testing = app.config.get("TESTING")
    original_task_folder = app.config.get("TASK_FOLDER")
    app.config["TESTING"] = True
    app.config["TASK_FOLDER"] = str(tmp_path)

    src_dir_a = tmp_path / "alpha"
    src_dir_b = tmp_path / "beta"
    src_dir_a.mkdir(parents=True)
    src_dir_b.mkdir(parents=True)
    src_a = src_dir_a / "duplicate.docx"
    src_b = src_dir_b / "duplicate.docx"

    for target, text in ((src_a, "Alpha content"), (src_b, "Beta content")):
        doc = Document()
        sec = doc.AddSection()
        sec.AddParagraph().AppendText(text)
        doc.SaveToFile(str(target), FileFormat.Docx)
        doc.Close()

    task_id = "task_same_name"
    job_id = "job_same_name"
    job_dir = tmp_path / task_id / "jobs" / job_id
    job_dir.mkdir(parents=True)

    steps = [
        {"type": "extract_word_all_content", "params": {"input_file": str(src_a)}},
        {"type": "extract_word_all_content", "params": {"input_file": str(src_b)}},
    ]

    try:
        run_workflow(steps, str(job_dir))

        client = app.test_client()
        resp = client.get(f"/tasks/{task_id}/compare/{job_id}")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "duplicate.docx" in body
        assert "alpha" in body
        assert "beta" in body
    finally:
        app.config["TASK_FOLDER"] = original_task_folder
        app.config["TESTING"] = original_testing


def test_select_page_sources_for_display_drops_low_confidence_secondary_source() -> None:
    selected = task_routes._select_page_sources_for_display(
        [("file_a.docx", 5), ("file_b.docx", 1)]
    )
    assert selected == [("file_a.docx", 5)]


def test_select_page_sources_for_display_keeps_meaningful_secondary_source() -> None:
    selected = task_routes._select_page_sources_for_display(
        [("file_a.docx", 4), ("file_b.docx", 2)]
    )
    assert selected == [("file_a.docx", 4), ("file_b.docx", 2)]


def test_page_has_explicit_paragraph_sources_only_when_count_positive() -> None:
    assert task_routes._page_has_explicit_paragraph_sources({"file_a.docx": 1}) is True
    assert task_routes._page_has_explicit_paragraph_sources({"file_a.docx": 0}) is False
    assert task_routes._page_has_explicit_paragraph_sources({}) is False


def test_select_object_candidate_pages_prefers_primary_probe_match() -> None:
    page_texts = [
        "Generic terms repeated here",
        "Figure 7 Knee Implant Packaging Overview with generic terms repeated here",
        "Generic terms repeated here again",
    ]
    source_counts_by_page = [{}, {}, {}]

    selected = task_routes._select_object_candidate_pages(
        page_texts,
        source_counts_by_page,
        primary_probe_texts=["Figure 7 Knee Implant Packaging Overview"],
        fallback_probe_texts=["generic terms repeated here"],
        allow_multi_page=False,
    )

    assert selected == [1]


def test_select_object_candidate_pages_limits_multi_page_table_to_best_contiguous_cluster() -> None:
    page_texts = [
        "generic header",
        "table 3 bill of materials femoral component titanium alloy uhmwpe astm f75 repeated data",
        "femoral component titanium alloy uhmwpe astm f75 repeated data continued next page",
        "generic header",
        "femoral component titanium alloy uhmwpe astm f75 repeated data stray mention elsewhere",
    ]
    source_counts_by_page = [{}, {}, {}, {}, {}]

    selected = task_routes._select_object_candidate_pages(
        page_texts,
        source_counts_by_page,
        primary_probe_texts=[],
        fallback_probe_texts=[
            "table 3 bill of materials",
            "femoral component titanium alloy uhmwpe astm f75 repeated data",
        ],
        allow_multi_page=True,
    )

    assert selected == [1, 2]
