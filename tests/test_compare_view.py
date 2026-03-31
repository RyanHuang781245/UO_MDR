from pathlib import Path
import sys
import types

from spire.doc import Document, FileFormat

from app.blueprints.tasks import compare_compat as task_routes
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


def test_select_page_sources_for_display_keeps_all_positive_page_sources() -> None:
    selected = task_routes._select_page_sources_for_display(
        [("file_a.docx", 5), ("file_b.docx", 1)]
    )
    assert selected == [("file_a.docx", 5), ("file_b.docx", 1)]


def test_select_page_sources_for_display_drops_zero_count_sources() -> None:
    selected = task_routes._select_page_sources_for_display(
        [("file_a.docx", 4), ("file_b.docx", 0)]
    )
    assert selected == [("file_a.docx", 4)]


def test_select_page_sources_for_display_ignores_preserve_sources_when_count_is_zero() -> None:
    selected = task_routes._select_page_sources_for_display(
        [("file_a.docx", 3), ("file_b.docx", 0)],
        preserve_sources={"file_b.docx"},
    )
    assert selected == [("file_a.docx", 3)]


def test_order_page_sources_by_first_seen_prefers_appearance_order() -> None:
    ordered = task_routes._order_page_sources_by_first_seen(
        [("file_a.docx", 4), ("file_b.docx", 2)],
        {"file_b.docx": 0, "file_a.docx": 1},
    )

    assert ordered == [("file_b.docx", 2), ("file_a.docx", 4)]


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
    assert paragraph_trace[1]["result_block_index"] == 1


def test_extract_preview_page_sources_reads_preview_labels_in_page_order() -> None:
    page_sources = task_routes._extract_preview_page_sources(
        [
            "來源: Beta.docx beta body 來源: Alpha.docx alpha body 來源: Beta.docx repeated beta body",
            "no preview label here",
        ],
        {
            "src_000001": {"source_file": "Alpha.docx"},
            "src_000002": {"source_file": "Beta.docx"},
        },
    )

    assert page_sources == [["Beta.docx", "Alpha.docx", "Beta.docx"], []]


def test_merge_page_source_map_with_preview_labels_only_fills_empty_pages() -> None:
    merged = task_routes._merge_page_source_map_with_preview_labels(
        [
            {
                "page_number": 1,
                "dominant_source": "Alpha.docx",
                "sources": [
                    {"source_file": "Alpha.docx", "count": 3, "inherited": False},
                    {"source_file": "Gamma.docx", "count": 1, "inherited": False},
                ],
            }
        ,
            {
                "page_number": 2,
                "dominant_source": "Gamma.docx",
                "sources": [
                    {"source_file": "Gamma.docx", "count": 2, "inherited": False},
                ],
            },
        ],
        [["Beta.docx", "Alpha.docx"], []],
    )

    assert merged[0]["dominant_source"] == "Alpha.docx"
    assert merged[0]["preview_label_sequence"] == ["Beta.docx", "Alpha.docx"]
    assert merged[0]["sources"] == [
        {"source_file": "Alpha.docx", "count": 3, "inherited": False},
        {"source_file": "Gamma.docx", "count": 1, "inherited": False},
    ]
    assert merged[1]["dominant_source"] == "Gamma.docx"
    assert merged[1]["preview_label_sequence"] == []
    assert merged[1]["sources"] == [
        {"source_file": "Gamma.docx", "count": 2, "inherited": False},
    ]


def test_build_provenance_trace_distinguishes_duplicate_text_by_source_id(tmp_path: Path) -> None:
    source_a = tmp_path / "alpha.docx"
    source_b = tmp_path / "beta.docx"
    result_path = tmp_path / "result.docx"
    log_path = tmp_path / "log.json"

    for target in (source_a, source_b):
        doc = Document()
        sec = doc.AddSection()
        sec.AddParagraph().AppendText("Shared source paragraph")
        doc.SaveToFile(str(target), FileFormat.Docx)
        doc.Close()

    fragment_a = tmp_path / "fragment_a.docx"
    fragment_b = tmp_path / "fragment_b.docx"
    for source_path, fragment_path in ((source_a, fragment_a), (source_b, fragment_b)):
        doc = Document()
        sec = doc.AddSection()
        sec.AddParagraph().AppendText("Shared source paragraph")
        doc.SaveToFile(str(fragment_path), FileFormat.Docx)
        doc.Close()

    from modules.docx_provenance import apply_final_provenance, build_provenance_descriptor
    from modules.docx_merger import merge_word_docs

    desc_a = build_provenance_descriptor(1)
    desc_b = build_provenance_descriptor(2)
    merge_word_docs([str(fragment_a), str(fragment_b)], str(result_path))
    apply_final_provenance(
        str(result_path),
        [
            {
                **desc_a,
                "fragment_path": str(fragment_a),
                "content_type": "paragraph",
                "source_id": "src_000001",
                "primary_probe_texts": ["Shared source paragraph"],
            },
            {
                **desc_b,
                "fragment_path": str(fragment_b),
                "content_type": "paragraph",
                "source_id": "src_000002",
                "primary_probe_texts": ["Shared source paragraph"],
            },
        ],
    )
    log_path.write_text("[]", encoding="utf-8")

    paragraph_trace, object_candidates = task_routes._build_provenance_trace(
        str(tmp_path),
        str(result_path),
        str(log_path),
        [
            {
                "type": "extract_word_all_content",
                "params": {"input_file": str(source_a)},
                "output_docx": str(fragment_a),
                "provenance": {
                    **desc_a,
                    "source_id": "src_000001",
                    "content_type": "paragraph",
                    "fragment_path": str(fragment_a),
                },
            },
            {
                "type": "extract_word_all_content",
                "params": {"input_file": str(source_b)},
                "output_docx": str(fragment_b),
                "provenance": {
                    **desc_b,
                    "source_id": "src_000002",
                    "content_type": "paragraph",
                    "fragment_path": str(fragment_b),
                },
            },
        ],
        [],
    )

    assert not object_candidates
    shared_items = [
        item
        for item in paragraph_trace
        if item["text"] == "Shared source paragraph"
    ]
    assert len(shared_items) == 2
    assert [item["source_file"] for item in shared_items] == [
        task_routes._format_source_file_label(str(source_a)),
        task_routes._format_source_file_label(str(source_b)),
    ]
    assert [item["source_id"] for item in shared_items] == ["src_000001", "src_000002"]


def test_build_page_source_map_prefers_provenance_blocks_when_available(
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
                "shared source paragraph",
                "shared source paragraph",
            ]
        )
    )
    monkeypatch.setitem(sys.modules, "fitz", fake_fitz)

    paragraph_trace = [
        {
            "merged_paragraph_index": 0,
            "source_id": "src_000001",
            "source_file": "Alpha.docx",
            "source_step": "extract_word_all_content",
            "content_type": "paragraph",
            "count_as_source": True,
            "result_block_index": 0,
            "text": "shared source paragraph",
            "probe_texts": ["shared source paragraph"],
        },
        {
            "merged_paragraph_index": 1,
            "source_id": "src_000002",
            "source_file": "Beta.docx",
            "source_step": "extract_word_all_content",
            "content_type": "paragraph",
            "count_as_source": True,
            "result_block_index": 1,
            "text": "shared source paragraph",
            "probe_texts": ["shared source paragraph"],
        },
    ]

    annotated_trace, page_source_map = task_routes._build_page_source_map(
        str(tmp_path),
        str(pdf_path),
        paragraph_trace,
        [],
    )

    assert [item["result_page"] for item in annotated_trace] == [1, 1]
    assert page_source_map[0]["sources"] == [
        {"source_file": "Alpha.docx", "count": 1, "inherited": False},
        {"source_file": "Beta.docx", "count": 1, "inherited": False},
    ]


def test_build_page_source_map_does_not_override_explicit_page_sources_with_preview_labels(
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
                "來源: Beta.docx beta body preview text",
            ]
        )
    )
    monkeypatch.setitem(sys.modules, "fitz", fake_fitz)

    paragraph_trace = [
        {
            "merged_paragraph_index": 0,
            "source_id": "src_000001",
            "source_file": "Alpha.docx",
            "source_step": "extract_word_all_content",
            "content_type": "paragraph",
            "count_as_source": True,
            "result_block_index": 0,
            "text": "beta body preview text",
            "probe_texts": ["beta body preview text"],
        },
    ]

    _, page_source_map = task_routes._build_page_source_map(
        str(tmp_path),
        str(pdf_path),
        paragraph_trace,
        [],
        {
            "src_000001": {"source_file": "Alpha.docx"},
            "src_000002": {"source_file": "Beta.docx"},
        },
    )

    assert page_source_map[0]["dominant_source"] == "Alpha.docx"
    assert page_source_map[0]["sources"][0] == {
        "source_file": "Alpha.docx",
        "count": 1,
        "inherited": False,
    }


def test_build_page_source_map_uses_preview_labels_only_on_empty_pages(
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
                "來源: Alpha.docx alpha body",
                "continued alpha body without a new label",
                "來源: Beta.docx beta body",
            ]
        )
    )
    monkeypatch.setitem(sys.modules, "fitz", fake_fitz)

    annotated_trace, page_source_map = task_routes._build_page_source_map(
        str(tmp_path),
        str(pdf_path),
        [],
        [],
        {
            "src_000001": {"source_file": "Alpha.docx"},
            "src_000002": {"source_file": "Beta.docx"},
        },
    )

    assert annotated_trace == []
    assert page_source_map[0]["dominant_source"] == "Alpha.docx"
    assert page_source_map[0]["sources"] == [
        {
            "source_file": "Alpha.docx",
            "count": 1,
            "inherited": False,
            "from_preview_label": True,
            "preview_segment_role": "label",
        }
    ]
    assert page_source_map[1]["dominant_source"] == ""
    assert page_source_map[1]["sources"] == []
    assert page_source_map[2]["dominant_source"] == "Beta.docx"


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


def test_build_page_source_map_orders_sources_by_first_appearance_on_page(
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
                "hip intro knee body knee appendix",
            ]
        )
    )
    monkeypatch.setitem(sys.modules, "fitz", fake_fitz)

    paragraph_trace = [
        {
            "merged_paragraph_index": 0,
            "source_file": "Hip.docx",
            "source_step": "extract_word_chapter",
            "count_as_source": True,
            "text": "hip intro",
        },
        {
            "merged_paragraph_index": 1,
            "source_file": "Knee.docx",
            "source_step": "extract_word_chapter",
            "count_as_source": True,
            "text": "knee body",
        },
        {
            "merged_paragraph_index": 2,
            "source_file": "Knee.docx",
            "source_step": "extract_word_chapter",
            "count_as_source": True,
            "text": "knee appendix",
        },
    ]

    _, page_source_map = task_routes._build_page_source_map(
        str(tmp_path),
        str(pdf_path),
        paragraph_trace,
        [],
    )

    assert page_source_map[0]["dominant_source"] == "Knee.docx"
    assert page_source_map[0]["sources"] == [
        {"source_file": "Hip.docx", "count": 1, "inherited": False},
        {"source_file": "Knee.docx", "count": 2, "inherited": False},
    ]
