import json
import re
from pathlib import Path

from flask import url_for

from app.services.standard_update_service import HARMONISED_SOURCE_CUSTOM, create_standard_update


def _anchor_classes(html: str, href: str) -> str:
    match = re.search(rf'<a class="([^"]*)" href="{re.escape(href)}">', html)
    assert match, f"anchor not found for {href}"
    return match.group(1)


def test_standard_update_pages_hide_general_task_sidebar(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("Sidebar Hidden", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)

    with app.test_request_context():
        task_detail_href = url_for("tasks_bp.task_detail", task_id=task_id)
        task_mapping_href = url_for("tasks_bp.task_mapping", task_id=task_id)
        task_flow_href = url_for("flow_builder_bp.flow_builder", task_id=task_id)
        standard_detail_href = url_for("standard_updates_bp.detail", task_id=task_id)
        standard_mapping_href = url_for("standard_updates_bp.mapping", task_id=task_id)
        standard_list_href = url_for("standard_updates_bp.list")

    for path in (f"/standards/{task_id}", f"/standards/{task_id}/mapping"):
        response = client.get(path)
        html = response.get_data(as_text=True)

        assert response.status_code == 200
        assert "標準更新" in html
        assert "標準更新任務" in html
        assert "任務內容" not in html
        assert "標準更新工作區" in html
        assert f'href="{task_detail_href}"' not in html
        assert f'href="{task_mapping_href}"' not in html
        assert f'href="{task_flow_href}"' not in html
        assert f'href="{standard_detail_href}"' in html
        assert f'href="{standard_mapping_href}"' in html
        assert "active" not in _anchor_classes(html, standard_list_href).split()

    detail_html = client.get(f"/standards/{task_id}").get_data(as_text=True)
    mapping_html = client.get(f"/standards/{task_id}/mapping").get_data(as_text=True)
    assert "active" in _anchor_classes(detail_html, standard_detail_href).split()
    assert "active" not in _anchor_classes(detail_html, standard_mapping_href).split()
    assert "active" in _anchor_classes(mapping_html, standard_mapping_href).split()
    assert "active" not in _anchor_classes(mapping_html, standard_detail_href).split()


def test_task_detail_sidebar_highlights_only_workspace_link(app, client, tmp_path):
    task_root = tmp_path / "task_store"
    task_id = "task1234"
    task_dir = task_root / task_id
    files_dir = task_dir / "files"
    output_dir = task_dir / "output"
    files_dir.mkdir(parents=True)
    output_dir.mkdir()
    (task_dir / "meta.json").write_text(
        json.dumps(
            {
                "name": "Sidebar Task",
                "description": "",
                "creator": "NF025 黃倫",
                "created": "2026-05-21 10:00",
                "output_path": str(output_dir),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    app.config["TASK_FOLDER"] = str(task_root)

    response = client.get(f"/tasks/{task_id}")
    html = response.get_data(as_text=True)

    with app.test_request_context():
        launcher_href = url_for("tasks_bp.launcher")
        tasks_href = url_for("tasks_bp.tasks")
        detail_href = url_for("tasks_bp.task_detail", task_id=task_id)

    assert response.status_code == 200
    assert "任務工作區" in html
    assert "active" not in _anchor_classes(html, launcher_href).split()
    assert "active" not in _anchor_classes(html, tasks_href).split()
    assert "active" in _anchor_classes(html, detail_href).split()
