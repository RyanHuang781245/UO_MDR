from pathlib import Path

from flask import url_for

from app.services.standard_update_service import HARMONISED_SOURCE_CUSTOM, create_standard_update


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

    for path in (f"/standards/{task_id}", f"/standards/{task_id}/mapping"):
        response = client.get(path)
        html = response.get_data(as_text=True)

        assert response.status_code == 200
        assert "標準更新" in html
        assert "任務管理" in html
        assert "任務內容" not in html
        assert f'href="{task_detail_href}"' not in html
        assert f'href="{task_mapping_href}"' not in html
        assert f'href="{task_flow_href}"' not in html
