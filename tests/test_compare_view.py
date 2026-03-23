from pathlib import Path
import sys
import types

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


def test_select_page_sources_for_display_preserves_explicit_object_source() -> None:
    selected = task_routes._select_page_sources_for_display(
        [("file_a.docx", 3), ("file_b.docx", 1)],
        preserve_sources={"file_b.docx"},
    )
    assert selected == [("file_a.docx", 3), ("file_b.docx", 1)]


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


def test_build_trace_from_provenance_blocks_marks_template_context_without_counting_source() -> None:
    paragraph_trace, object_candidates = task_routes._build_trace_from_provenance_blocks(
        [
            {
                "block_type": "paragraph",
                "source_id": "",
                "source_file": "未知來源",
                "source_step": "",
                "content_type": "",
                "text": "Template heading",
                "probe_texts": ["template heading"],
                "block_index": 0,
            },
            {
                "block_type": "paragraph",
                "source_id": "src_000001",
                "source_file": "A.docx",
                "source_step": "extract_word_chapter",
                "content_type": "paragraph",
                "text": "Actual source text",
                "probe_texts": ["actual source text"],
                "block_index": 1,
            },
        ]
    )

    assert not object_candidates
    assert paragraph_trace[0]["match_status"] == "context"
    assert paragraph_trace[0]["count_as_source"] is False
    assert paragraph_trace[1]["match_status"] == "provenance"
    assert paragraph_trace[1]["count_as_source"] is True


def test_build_page_source_map_does_not_inherit_source_across_template_only_page(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pdf_path = tmp_path / "result.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    class _FakePage:
        def __init__(self, text: str) -> None:
            self._text = text

        def get_text(self, _mode: str) -> str:
            return self._text

    class _FakePdf:
        def __init__(self, pages: list[str]) -> None:
            self._pages = [_FakePage(text) for text in pages]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def __iter__(self):
            return iter(self._pages)

    fake_fitz = types.SimpleNamespace(
        open=lambda _path: _FakePdf(
            [
                "alpha source paragraph",
                "template section divider",
                "bravo source paragraph",
            ]
        )
    )
    monkeypatch.setitem(sys.modules, "fitz", fake_fitz)

    paragraph_trace = [
        {
            "merged_paragraph_index": 0,
            "source_file": "A.docx",
            "count_as_source": True,
            "text": "alpha source paragraph",
        },
        {
            "merged_paragraph_index": 1,
            "source_file": "未知來源",
            "count_as_source": False,
            "text": "template section divider",
        },
        {
            "merged_paragraph_index": 2,
            "source_file": "B.docx",
            "count_as_source": True,
            "text": "bravo source paragraph",
        },
    ]

    _, page_source_map = task_routes._build_page_source_map(
        str(tmp_path),
        str(pdf_path),
        paragraph_trace,
        [],
    )

    assert page_source_map[0]["dominant_source"] == "A.docx"
    assert page_source_map[1]["sources"] == []
    assert page_source_map[2]["dominant_source"] == "B.docx"


def test_build_page_source_map_preserves_table_caption_source_on_mixed_page(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pdf_path = tmp_path / "result.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    class _FakePage:
        def __init__(self, text: str) -> None:
            self._text = text

        def get_text(self, _mode: str) -> str:
            return self._text

    class _FakePdf:
        def __init__(self, pages: list[str]) -> None:
            self._pages = [_FakePage(text) for text in pages]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def __iter__(self):
            return iter(self._pages)

    fake_fitz = types.SimpleNamespace(
        open=lambda _path: _FakePdf(
            [
                "knee content body knee content body table 3 bill of materials of direct contact with the patients",
            ]
        )
    )
    monkeypatch.setitem(sys.modules, "fitz", fake_fitz)

    paragraph_trace = [
        {
            "merged_paragraph_index": 0,
            "source_file": "Knee.docx",
            "source_step": "extract_word_chapter",
            "count_as_source": True,
            "text": "knee content body",
        },
        {
            "merged_paragraph_index": 1,
            "source_file": "Knee.docx",
            "source_step": "extract_word_chapter",
            "count_as_source": True,
            "text": "knee content body",
        },
        {
            "merged_paragraph_index": 2,
            "source_file": "Knee.docx",
            "source_step": "extract_word_chapter",
            "count_as_source": True,
            "text": "knee content body",
        },
        {
            "merged_paragraph_index": 3,
            "source_file": "Hip.docx",
            "source_step": "extract_specific_table_from_word",
            "count_as_source": True,
            "text": "table 3 bill of materials of direct contact with the patients",
        },
    ]

    _, page_source_map = task_routes._build_page_source_map(
        str(tmp_path),
        str(pdf_path),
        paragraph_trace,
        [],
    )

    assert page_source_map[0]["sources"] == [
        {"source_file": "Knee.docx", "count": 3, "inherited": False},
        {"source_file": "Hip.docx", "count": 1, "inherited": False},
    ]
