import json

import pytest

from app.services.flow_service import DEFAULT_DOCUMENT_FORMAT_KEY, DOCUMENT_FORMAT_PRESETS


@pytest.fixture
def task_env(tmp_path, app):
    tasks_dir = tmp_path / "task_store"
    output_dir = tmp_path / "output"
    tasks_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)

    old_task_folder = app.config["TASK_FOLDER"]
    old_output_folder = app.config["OUTPUT_FOLDER"]

    app.config["TASK_FOLDER"] = str(tasks_dir)
    app.config["OUTPUT_FOLDER"] = str(output_dir)

    try:
        yield tasks_dir
    finally:
        app.config["TASK_FOLDER"] = old_task_folder
        app.config["OUTPUT_FOLDER"] = old_output_folder


def _write_json(path, data):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def test_update_flow_format_updates_existing_metadata(task_env, client):
    task_id = "abc123"
    flow_dir = task_env / task_id / "flows"
    flow_dir.mkdir(parents=True)
    flow_path = flow_dir / "demo.json"

    original = {
        "created": "2024-01-01 12:00",
        "steps": [{"type": "insert_title", "params": {"text": "Demo"}}],
        "center_titles": False,
        "document_format": DEFAULT_DOCUMENT_FORMAT_KEY,
        "line_spacing": 1.5,
    }
    _write_json(flow_path, original)

    response = client.post(
        f"/tasks/{task_id}/flows/update-format/demo",
        data={"document_format": "modern", "line_spacing": "2"},
    )

    assert response.status_code == 302

    with open(flow_path, "r", encoding="utf-8") as fh:
        updated = json.load(fh)

    assert updated["document_format"] == "modern"
    assert updated["line_spacing"] == 2.0
    assert updated["center_titles"] is False
    assert updated["created"] == original["created"]
    assert updated["steps"] == original["steps"]


def test_update_flow_format_converts_legacy_flow_list(task_env, client):
    task_id = "legacy"
    flow_dir = task_env / task_id / "flows"
    flow_dir.mkdir(parents=True)
    flow_path = flow_dir / "legacy.json"

    legacy_steps = [{"type": "insert_title", "params": {"text": "Legacy"}}]
    _write_json(flow_path, legacy_steps)

    response = client.post(
        f"/tasks/{task_id}/flows/update-format/legacy",
        data={
            "document_format": list(DOCUMENT_FORMAT_PRESETS.keys())[0],
            "line_spacing": "1.25",
        },
    )

    assert response.status_code == 302

    with open(flow_path, "r", encoding="utf-8") as fh:
        updated = json.load(fh)

    assert isinstance(updated, dict)
    assert updated["steps"] == legacy_steps
    assert updated["document_format"] in DOCUMENT_FORMAT_PRESETS
    assert updated["line_spacing"] == 1.25
    assert updated["center_titles"] is True
    assert isinstance(updated.get("created"), str)
