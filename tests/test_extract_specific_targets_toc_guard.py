from pathlib import Path

from docx import Document as DocxDocument

import modules.extract_specific_figure_xml as figure_xml
import modules.extract_specific_table_xml as table_xml
from modules.docx_toc import TocEntry


def _create_docx(path: Path) -> None:
    doc = DocxDocument()
    doc.add_paragraph("Device Under Evaluation")
    doc.add_paragraph("Body text")
    doc.save(path)


def test_extract_specific_figure_uses_toc_backed_mismatch_guard(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "figure_source.docx"
    _create_docx(src)
    captured: dict[str, object] = {}

    def fake_find_section_range_children(*args, **kwargs):
        captured["strict_heading_number_match"] = kwargs.get("strict_heading_number_match")
        captured["allow_start_number_mismatch_fallback"] = kwargs.get(
            "allow_start_number_mismatch_fallback"
        )
        captured["style_numpr"] = kwargs.get("style_numpr")
        return 0, len(args[0])

    def fake_extract_toc_entries_from_parts(_parts):
        return [
            TocEntry(
                order=1,
                level=3,
                number="3.2.1",
                title="Device Under Evaluation",
                page="11",
                anchor="_Toc1",
                raw_text="3.2.1 Device Under Evaluation 11",
                style_id="33",
                style_name="toc 3",
            )
        ]

    monkeypatch.setattr(figure_xml, "find_section_range_children", fake_find_section_range_children)
    monkeypatch.setattr(
        "modules.docx_toc.extract_toc_entries_from_parts",
        fake_extract_toc_entries_from_parts,
    )

    result = figure_xml.extract_specific_figure_from_word_xml(
        input_file=str(src),
        output_docx_path=None,
        target_chapter_section="3.2.1",
        target_caption_label="Figure 1.",
        target_chapter_title="Device Under Evaluation",
        save_output=False,
        return_reason=True,
    )

    assert result["ok"] is False
    assert captured["strict_heading_number_match"] is True
    assert captured["allow_start_number_mismatch_fallback"] is True
    assert isinstance(captured["style_numpr"], dict)


def test_extract_specific_table_disables_mismatch_guard_without_toc_match(
    monkeypatch, tmp_path: Path
) -> None:
    src = tmp_path / "table_source.docx"
    _create_docx(src)
    captured: dict[str, object] = {}

    def fake_find_section_range_children(*args, **kwargs):
        captured["strict_heading_number_match"] = kwargs.get("strict_heading_number_match")
        captured["allow_start_number_mismatch_fallback"] = kwargs.get(
            "allow_start_number_mismatch_fallback"
        )
        captured["style_numpr"] = kwargs.get("style_numpr")
        return 0, len(args[0])

    def fake_extract_toc_entries_from_parts(_parts):
        return [
            TocEntry(
                order=1,
                level=3,
                number="3.2.1",
                title="Device Under Evaluation",
                page="11",
                anchor="_Toc1",
                raw_text="3.2.1 Device Under Evaluation 11",
                style_id="33",
                style_name="toc 3",
            )
        ]

    monkeypatch.setattr(table_xml, "find_section_range_children", fake_find_section_range_children)
    monkeypatch.setattr(
        "modules.docx_toc.extract_toc_entries_from_parts",
        fake_extract_toc_entries_from_parts,
    )

    result = table_xml.extract_specific_table_from_word_xml(
        input_file=str(src),
        output_docx_path=None,
        target_chapter_section="9.9.9",
        target_caption_label="Table 1.",
        target_chapter_title="Device Under Evaluation",
        save_output=False,
        return_reason=True,
    )

    assert result["ok"] is False
    assert captured["strict_heading_number_match"] is True
    assert captured["allow_start_number_mismatch_fallback"] is False
    assert isinstance(captured["style_numpr"], dict)
