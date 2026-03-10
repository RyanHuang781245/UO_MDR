from pathlib import Path

from docx import Document as DocxDocument

from modules.workflow import run_workflow


def _create_docx(path: Path, text: str = "source") -> None:
    doc = DocxDocument()
    doc.add_paragraph(text)
    doc.save(path)


def _extract_step(input_file: str, **extra_params):
    params = {
        "input_file": input_file,
        "target_chapter_section": "1.1",
        "target_caption_label": "Figure 1.",
    }
    params.update(extra_params)
    return {"type": "extract_specific_figure_from_word", "params": params}


def _extract_table_step(input_file: str, **extra_params):
    params = {
        "input_file": input_file,
        "target_chapter_section": "1.1",
        "target_caption_label": "Table 1.",
    }
    params.update(extra_params)
    return {"type": "extract_specific_table_from_word", "params": params}


def test_workflow_extract_figure_success_saves_fragment_and_result(
    tmp_path: Path,
    monkeypatch,
) -> None:
    src = tmp_path / "source.docx"
    _create_docx(src)
    captured: dict[str, object] = {}

    def fake_extract_specific_figure_from_word(*_args, **kwargs):
        captured["include_caption"] = kwargs.get("include_caption")
        output_docx_path = kwargs.get("output_docx_path")
        assert output_docx_path
        out_path = Path(str(output_docx_path))
        _create_docx(out_path, text="Figure 1. Demo")
        return {"ok": True, "reason": "ok"}

    monkeypatch.setattr(
        "modules.workflow.extract_specific_figure_from_word",
        fake_extract_specific_figure_from_word,
    )

    steps = [_extract_step(str(src), include_caption="false")]
    result = run_workflow(steps, str(tmp_path / "job_success"))
    figure_entry = next(e for e in result["log_json"] if e.get("type") == "extract_specific_figure_from_word")

    assert figure_entry["status"] == "ok"
    assert Path(figure_entry["output_docx"]).is_file()
    assert figure_entry["result"]["ok"] is True
    assert captured["include_caption"] is False


def test_workflow_extract_figure_not_found_uses_result_reason(
    tmp_path: Path,
    monkeypatch,
) -> None:
    src = tmp_path / "source.docx"
    _create_docx(src)

    def fake_extract_specific_figure_from_word(*_args, **_kwargs):
        return {"ok": False, "reason": "figure_not_found"}

    monkeypatch.setattr(
        "modules.workflow.extract_specific_figure_from_word",
        fake_extract_specific_figure_from_word,
    )

    steps = [_extract_step(str(src))]
    result = run_workflow(steps, str(tmp_path / "job_not_found"))
    figure_entry = next(e for e in result["log_json"] if e.get("type") == "extract_specific_figure_from_word")

    assert figure_entry["status"] == "error"
    assert figure_entry["error"] == "figure_not_found"


def test_workflow_extract_figure_include_caption_defaults_true(
    tmp_path: Path,
    monkeypatch,
) -> None:
    src = tmp_path / "source.docx"
    _create_docx(src)
    captured: dict[str, object] = {}

    def fake_extract_specific_figure_from_word(*_args, **kwargs):
        captured["include_caption"] = kwargs.get("include_caption")
        output_docx_path = kwargs.get("output_docx_path")
        out_path = Path(str(output_docx_path))
        _create_docx(out_path, text="Figure 1. Default Caption")
        return {"ok": True, "reason": "ok"}

    monkeypatch.setattr(
        "modules.workflow.extract_specific_figure_from_word",
        fake_extract_specific_figure_from_word,
    )

    steps = [_extract_step(str(src))]
    result = run_workflow(steps, str(tmp_path / "job_default_caption"))
    figure_entry = next(e for e in result["log_json"] if e.get("type") == "extract_specific_figure_from_word")

    assert figure_entry["status"] == "ok"
    assert captured["include_caption"] is True


def test_workflow_extract_figure_forwards_title_and_index(
    tmp_path: Path,
    monkeypatch,
) -> None:
    src = tmp_path / "source.docx"
    _create_docx(src)
    captured: dict[str, object] = {}

    def fake_extract_specific_figure_from_word(*_args, **kwargs):
        captured["target_figure_title"] = kwargs.get("target_figure_title")
        captured["target_figure_index"] = kwargs.get("target_figure_index")
        output_docx_path = kwargs.get("output_docx_path")
        out_path = Path(str(output_docx_path))
        _create_docx(out_path, text="Figure result")
        return {"ok": True, "reason": "ok"}

    monkeypatch.setattr(
        "modules.workflow.extract_specific_figure_from_word",
        fake_extract_specific_figure_from_word,
    )

    steps = [
        _extract_step(
            str(src),
            target_caption_label="",
            target_figure_title="System architecture",
            target_figure_index="2",
        )
    ]
    result = run_workflow(steps, str(tmp_path / "job_title_index"))
    figure_entry = next(e for e in result["log_json"] if e.get("type") == "extract_specific_figure_from_word")

    assert figure_entry["status"] == "ok"
    assert captured["target_figure_title"] == "System architecture"
    assert captured["target_figure_index"] == "2"


def test_workflow_extract_table_include_caption_defaults_true(
    tmp_path: Path,
    monkeypatch,
) -> None:
    src = tmp_path / "source.docx"
    _create_docx(src)
    captured: dict[str, object] = {}

    def fake_extract_specific_table_from_word(*args, **kwargs):
        captured["include_caption"] = kwargs.get("include_caption")
        output_docx_path = kwargs.get("output_docx_path") or (args[1] if len(args) > 1 else None)
        out_path = Path(str(output_docx_path))
        _create_docx(out_path, text="Table 1. Default Caption")
        return {"ok": True, "reason": "ok"}

    monkeypatch.setattr(
        "modules.workflow.extract_specific_table_from_word",
        fake_extract_specific_table_from_word,
    )

    steps = [_extract_table_step(str(src))]
    result = run_workflow(steps, str(tmp_path / "job_table_default_caption"))
    table_entry = next(e for e in result["log_json"] if e.get("type") == "extract_specific_table_from_word")

    assert table_entry["status"] == "ok"
    assert captured["include_caption"] is True
