from pathlib import Path
import base64
from zipfile import ZipFile

from docx import Document as DocxDocument
from docx.shared import Inches
from lxml import etree

from modules.docx_merger import merge_word_docs
from modules.docx_provenance import (
    apply_final_provenance,
    build_provenance_descriptor,
    extract_provenance_block_trace,
)
from modules.template_manager import render_template_with_mappings
from modules.workflow import run_workflow


_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def _create_docx(path: Path, paragraphs: list[str] | None = None) -> None:
    doc = DocxDocument()
    for text in paragraphs or []:
        doc.add_paragraph(text)
    doc.save(path)


def _create_table_docx(path: Path, rows: list[list[str]]) -> None:
    doc = DocxDocument()
    table = doc.add_table(rows=len(rows), cols=len(rows[0]))
    for row_idx, row in enumerate(rows):
        for col_idx, text in enumerate(row):
            table.cell(row_idx, col_idx).text = text
    doc.save(path)


def _collect_provenance_pairs(path: Path) -> tuple[dict[str, str], set[str]]:
    with ZipFile(path, "r") as zf:
        root = etree.fromstring(zf.read("word/document.xml"))
    starts = {
        str(node.get("{%s}id" % _NS["w"]) or ""): str(node.get("{%s}name" % _NS["w"]) or "")
        for node in root.xpath("//w:bookmarkStart", namespaces=_NS)
        if str(node.get("{%s}name" % _NS["w"]) or "").startswith("prov_src_")
    }
    ends = {
        str(node.get("{%s}id" % _NS["w"]) or "")
        for node in root.xpath("//w:bookmarkEnd", namespaces=_NS)
    }
    return starts, ends


def test_extract_provenance_block_trace_survives_docx_merge(tmp_path: Path) -> None:
    first_fragment = tmp_path / "fragment_a.docx"
    second_fragment = tmp_path / "fragment_b.docx"
    result_path = tmp_path / "result.docx"

    _create_docx(first_fragment, ["Alpha source paragraph"])
    _create_table_docx(
        second_fragment,
        [["Distinct table row content for provenance tracking", "Second cell content"]],
    )

    first_desc = build_provenance_descriptor(1)
    second_desc = build_provenance_descriptor(2)
    merge_word_docs([str(first_fragment), str(second_fragment)], str(result_path))

    applied = apply_final_provenance(
        str(result_path),
        [
            {
                **first_desc,
                "fragment_path": str(first_fragment),
                "content_type": "paragraph",
                "source_id": "src_000001",
            },
            {
                **second_desc,
                "fragment_path": str(second_fragment),
                "content_type": "table",
                "source_id": "src_000002",
            },
        ],
    )
    assert len(applied) == 2

    starts, ends = _collect_provenance_pairs(result_path)
    assert starts
    assert set(starts).issubset(ends)

    trace = extract_provenance_block_trace(
        str(result_path),
        {
            "src_000001": {
                **first_desc,
                "source_file": "A.docx",
                "source_step": "extract_word_chapter",
                "content_type": "paragraph",
            },
            "src_000002": {
                **second_desc,
                "source_file": "B.docx",
                "source_step": "extract_specific_table_from_word",
                "content_type": "table",
            },
        },
    )

    assert any(
        item["block_type"] == "paragraph"
        and item["source_file"] == "A.docx"
        and item["text"] == "Alpha source paragraph"
        for item in trace
    )
    assert any(
        item["block_type"] == "table"
        and item["source_file"] == "B.docx"
        and "Distinct table row content for provenance tracking" in item["probe_texts"]
        for item in trace
    )


def test_extract_provenance_block_trace_survives_template_merge_subdoc(tmp_path: Path) -> None:
    template_path = tmp_path / "template.docx"
    content_path = tmp_path / "content.docx"
    output_path = tmp_path / "result.docx"

    _create_docx(template_path, ["Anchor"])
    _create_docx(content_path, ["Inserted provenance paragraph"])

    desc = build_provenance_descriptor(1)
    render_template_with_mappings(
        str(template_path),
        str(output_path),
        [
            {
                "index": 0,
                "mode": "insert_after",
                "content_docx_path": str(content_path),
                "source_order": 0,
            }
        ],
        [{"index": 0, "display": "", "text": "Anchor"}],
    )

    applied = apply_final_provenance(
        str(output_path),
        [
            {
                **desc,
                "fragment_path": str(content_path),
                "content_type": "paragraph",
                "source_id": "src_000001",
            }
        ],
    )
    assert len(applied) == 1

    trace = extract_provenance_block_trace(
        str(output_path),
        {
            "src_000001": {
                **desc,
                "source_file": "Template Source.docx",
                "source_step": "extract_word_all_content",
                "content_type": "paragraph",
            }
        },
    )

    assert any(
        item["block_type"] == "paragraph"
        and item["source_file"] == "Template Source.docx"
        and item["text"] == "Inserted provenance paragraph"
        for item in trace
    )


def test_extract_provenance_block_trace_uses_metadata_for_empty_figure_paragraph(tmp_path: Path) -> None:
    figure_docx = tmp_path / "figure.docx"
    image_path = tmp_path / "pixel.png"
    image_path.write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9sX9n0cAAAAASUVORK5CYII="
        )
    )
    doc = DocxDocument()
    doc.add_paragraph().add_run().add_picture(str(image_path), width=Inches(0.2))
    doc.save(figure_docx)

    desc = build_provenance_descriptor(1)
    apply_final_provenance(
        str(figure_docx),
        [
            {
                **desc,
                "fragment_path": str(figure_docx),
                "content_type": "figure",
                "source_id": "src_000001",
                "primary_probe_texts": ["Figure 8 Packaging"],
            }
        ],
    )

    trace = extract_provenance_block_trace(
        str(figure_docx),
        {
            "src_000001": {
                **desc,
                "source_file": "Figure Source.docx",
                "source_step": "extract_specific_figure_from_word",
                "content_type": "figure",
                "primary_probe_texts": ["Figure 8 Packaging"],
            }
        },
    )

    assert any(
        item["block_type"] == "figure"
        and item["source_file"] == "Figure Source.docx"
        and "Figure 8 Packaging" in item["probe_texts"]
        for item in trace
    )


def test_apply_final_provenance_covers_all_merged_fragments(tmp_path: Path) -> None:
    fragments: list[Path] = []
    source_records = []
    for idx in range(1, 9):
        fragment = tmp_path / f"fragment_{idx}.docx"
        _create_docx(fragment, [f"Fragment {idx}"])
        desc = build_provenance_descriptor(idx)
        fragments.append(fragment)
        source_records.append(
            {
                **desc,
                "fragment_path": str(fragment),
                "content_type": "paragraph",
                "source_id": f"src_{idx:06d}",
            }
        )

    result_path = tmp_path / "result.docx"
    merge_word_docs([str(path) for path in fragments], str(result_path))
    applied = apply_final_provenance(str(result_path), source_records)

    starts, ends = _collect_provenance_pairs(result_path)
    assert len(applied) == 8
    assert len(starts) == 8
    assert set(starts).issubset(ends)


def test_apply_final_provenance_inserts_exact_range_on_final_docx(tmp_path: Path) -> None:
    fragment = tmp_path / "fragment.docx"
    result_path = tmp_path / "result.docx"
    _create_docx(fragment, ["First paragraph", "Second paragraph"])
    _create_docx(result_path, ["Prefix", "First paragraph", "Second paragraph", "Suffix"])

    desc = build_provenance_descriptor(1)
    applied = apply_final_provenance(
        str(result_path),
        [
            {
                **desc,
                "fragment_path": str(fragment),
                "content_type": "paragraph",
                "source_id": "src_000001",
            }
        ],
    )

    assert len(applied) == 1
    assert applied[0]["result_block_start"] == 1
    assert applied[0]["result_block_end"] == 2

    starts, ends = _collect_provenance_pairs(result_path)
    assert starts
    assert set(starts).issubset(ends)


def test_run_workflow_applies_template_provenance_in_display_order(tmp_path: Path) -> None:
    template_path = tmp_path / "template.docx"
    job_dir = tmp_path / "job_template_order"

    _create_docx(
        template_path,
        [
            "Anchor 0",
            "Anchor 1",
            "Anchor 2",
            "Anchor 3",
        ],
    )

    steps = []
    for idx in range(4):
        steps.append(
            {
                "type": "insert_text",
                "params": {
                    "text": f"Section {idx} - First",
                    "template_index": str(idx),
                    "template_mode": "insert_after",
                },
            }
        )
        steps.append(
            {
                "type": "insert_text",
                "params": {
                    "text": f"Section {idx} - Second",
                    "template_index": str(idx),
                    "template_mode": "insert_after",
                },
            }
        )

    result = run_workflow(steps, str(job_dir), template={"path": str(template_path)})
    provenance_entries = [
        entry["provenance"]
        for entry in result["log_json"]
        if isinstance(entry, dict) and isinstance(entry.get("provenance"), dict)
    ]

    assert len(provenance_entries) == 8
    assert all(entry.get("result_block_start") is not None for entry in provenance_entries)
    assert all(entry.get("result_block_end") is not None for entry in provenance_entries)

    starts, ends = _collect_provenance_pairs(Path(result["result_docx"]))
    assert len(starts) == 8
    assert set(starts).issubset(ends)
