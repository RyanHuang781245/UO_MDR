from pathlib import Path

import modules.Extract_AllFile_to_FinalWord as chapter_module
from modules.Extract_AllFile_to_FinalWord import _parse_chapter_section_expression, extract_word_chapter


def test_parse_section_with_dot_and_title() -> None:
    assert _parse_chapter_section_expression("1. 測試1.1") == ("1", "", "測試1.1")


def test_parse_section_range_with_title() -> None:
    assert _parse_chapter_section_expression("1.1.1 - 1.1.3 測試標題") == ("1.1.1", "1.1.3", "測試標題")


def test_parse_section_without_title() -> None:
    assert _parse_chapter_section_expression("1.1.1-1.1.3") == ("1.1.1", "1.1.3", "")


def test_parse_end_marker_with_trailing_dot_and_title() -> None:
    assert _parse_chapter_section_expression("2. Materials") == ("2", "", "Materials")


def test_extract_word_chapter_splits_end_marker_with_trailing_dot(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "source.docx"
    src.write_bytes(b"stub")
    captured: dict = {}

    def fake_extract_section_docx_xml(*_args, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(chapter_module, "extract_section_docx_xml", fake_extract_section_docx_xml)
    monkeypatch.setattr(chapter_module, "_read_first_paragraph_text", lambda *_args, **_kwargs: "")

    extract_word_chapter(
        str(src),
        "1.1",
        target_chapter_title="Start title",
        explicit_end_title="2. Materials",
        output_docx_path=str(tmp_path / "out.docx"),
    )

    assert captured["explicit_end_number"] == "2"
    assert captured["explicit_end_title"] == "Materials"
