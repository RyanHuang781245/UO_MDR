import json
import re
from pathlib import Path

from flask import url_for

from app.services.standard_update_service import HARMONISED_SOURCE_CUSTOM, create_standard_update

LONG_TEXT = "超長文字" * 20


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
        assert "任務管理" in html
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
    assert "文件轉換工作區" in html
    assert "active" not in _anchor_classes(html, launcher_href).split()
    assert "active" not in _anchor_classes(html, tasks_href).split()
    assert "active" in _anchor_classes(html, detail_href).split()


def test_standard_update_list_shows_detail_action(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("List Detail", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)

    with app.test_request_context():
        detail_href = url_for("standard_updates_bp.detail", task_id=task_id)

    response = client.get("/standards")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert f'href="{detail_href}"' in html
    assert "standard-update-detail-trigger" in html
    assert "standardUpdateDrawer" in html
    assert "standardUpdateNameCount" in html
    assert "standardUpdateDescCount" in html
    assert "bi bi-eye" in html


def test_standard_update_description_route_updates_metadata(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("Update Desc", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)

    response = client.post(
        f"/standards/{task_id}/description",
        data={"description": "updated description", "next": "/standards"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    meta = json.loads((standard_update_root / task_id / "meta.json").read_text(encoding="utf-8"))
    assert meta["description"] == "updated description"


def test_standard_update_drawer_actions_support_ajax_updates(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update("原標題", description="原描述", harmonised_source_mode=HARMONISED_SOURCE_CUSTOM)
    headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}

    rename_response = client.post(
        f"/standards/{task_id}/rename",
        data={"name": "更新後名稱"},
        headers=headers,
    )
    desc_response = client.post(
        f"/standards/{task_id}/description",
        data={"description": "更新後描述"},
        headers=headers,
    )

    assert rename_response.status_code == 200
    assert rename_response.is_json
    assert rename_response.get_json()["name"] == "更新後名稱"
    assert desc_response.status_code == 200
    assert desc_response.is_json
    assert desc_response.get_json()["description"] == "更新後描述"

    meta = json.loads((standard_update_root / task_id / "meta.json").read_text(encoding="utf-8"))
    assert meta["name"] == "更新後名稱"
    assert meta["description"] == "更新後描述"


def test_standard_update_list_truncates_long_description(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update(
        "Long Desc",
        description="這是一段很長很長的標準更新任務描述，用來確認列表頁會用省略樣式處理而不是直接把整列撐高。",
        harmonised_source_mode=HARMONISED_SOURCE_CUSTOM,
    )

    response = client.get("/standards")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert task_id in html
    assert "standard-update-desc-truncate" in html
    assert 'title="這是一段很長很長的標準更新任務描述，用來確認列表頁會用省略樣式處理而不是直接把整列撐高。"' in html


def test_standard_update_list_truncates_long_name(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_name = "這是一個很長很長的標準更新任務名稱，用來確認列表頁任務名稱也會用省略樣式處理"
    create_standard_update(
        task_name,
        harmonised_source_mode=HARMONISED_SOURCE_CUSTOM,
    )

    response = client.get("/standards")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert f'title="{task_name}"' in html
    assert 'class="standard-task-name standard-update-desc-truncate d-block text-primary"' in html


def test_standard_update_detail_handles_long_name_and_description(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    task_id = create_standard_update(
        "標準更新5標準更新5標準更新5標準更新5標準更新5標準更新5標準更新5標準更新5",
        description="這是一段很長很長的任務描述，內容會持續延伸，用來確認任務詳情摘要卡在遇到長名稱與長敘述時會換行而不是把右側任務 ID 區塊擠壞。",
        harmonised_source_mode=HARMONISED_SOURCE_CUSTOM,
    )

    response = client.get(f"/standards/{task_id}")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "detail-summary-header" in html
    assert "detail-summary-title" in html
    assert "detail-summary-desc" in html
    assert "detail-task-id-meta" in html
    assert "align-items: start;" in html


def test_task_detail_handles_long_name_and_description(app, client, tmp_path):
    task_root = tmp_path / "task_store"
    task_id = "task5678"
    task_dir = task_root / task_id
    files_dir = task_dir / "files"
    output_dir = task_dir / "output"
    files_dir.mkdir(parents=True)
    output_dir.mkdir()
    (task_dir / "meta.json").write_text(
        json.dumps(
            {
                "name": "testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest",
                "description": "這是一段很長很長的文件轉換任務描述，內容會持續延伸，用來確認任務詳情摘要卡在遇到長名稱與長敘述時會換行而不是把右側任務 ID 區塊擠壞。",
                "creator": "NF025 黃倫",
                "created": "2026-05-21 10:00",
                "nas_path": "C:/nas/path",
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

    assert response.status_code == 200
    assert "task-detail-summary-header" in html
    assert "task-detail-summary-title" in html
    assert "task-detail-summary-desc" in html
    assert "task-detail-summary-id" in html


def test_tasks_page_shows_length_counters(app, client, tmp_path):
    task_root = tmp_path / "task_store"
    task_root.mkdir()
    app.config["TASK_FOLDER"] = str(task_root)

    response = client.get("/tasks")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "taskNameCount" in html
    assert "taskDescCount" in html
    assert "copyTaskNameCount" in html


def test_standard_update_create_rejects_text_over_50(app, client, tmp_path):
    standard_update_root = tmp_path / "standard_update_store"
    harmonised_root = tmp_path / "harmonised_store"
    standard_update_root.mkdir()
    harmonised_root.mkdir()

    app.config["STANDARD_UPDATE_FOLDER"] = str(standard_update_root)
    app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"] = str(harmonised_root)

    response = client.post(
        "/standards",
        data={
            "name": LONG_TEXT,
            "description": "",
            "harmonised_source_mode": "custom",
        },
        follow_redirects=True,
    )

    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "標準更新任務名稱最多 50 字" in html
    assert not any(standard_update_root.iterdir())


def test_task_rename_rejects_name_over_50(app, client, tmp_path):
    task_root = tmp_path / "task_store"
    task_id = "task9012"
    task_dir = task_root / task_id
    files_dir = task_dir / "files"
    output_dir = task_dir / "output"
    files_dir.mkdir(parents=True)
    output_dir.mkdir()
    meta_path = task_dir / "meta.json"
    meta_path.write_text(
        json.dumps({"name": "原名稱", "description": "", "output_path": str(output_dir)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    app.config["TASK_FOLDER"] = str(task_root)

    response = client.post(
        f"/tasks/{task_id}/rename",
        data={"name": LONG_TEXT},
    )

    assert response.status_code == 400
    assert "任務名稱最多 50 字" in response.get_data(as_text=True)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["name"] == "原名稱"


def test_task_description_rejects_text_over_50(app, client, tmp_path):
    task_root = tmp_path / "task_store"
    task_id = "task3456"
    task_dir = task_root / task_id
    files_dir = task_dir / "files"
    output_dir = task_dir / "output"
    files_dir.mkdir(parents=True)
    output_dir.mkdir()
    meta_path = task_dir / "meta.json"
    meta_path.write_text(
        json.dumps({"name": "原名稱", "description": "原描述", "output_path": str(output_dir)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    app.config["TASK_FOLDER"] = str(task_root)

    response = client.post(
        f"/tasks/{task_id}/description",
        data={"description": LONG_TEXT},
    )

    assert response.status_code == 400
    assert "任務描述最多 50 字" in response.get_data(as_text=True)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["description"] == "原描述"


def test_task_drawer_actions_support_ajax_updates(app, client, tmp_path):
    task_root = tmp_path / "task_store"
    task_id = "task7788"
    task_dir = task_root / task_id
    files_dir = task_dir / "files"
    output_dir = task_dir / "output"
    files_dir.mkdir(parents=True)
    output_dir.mkdir()
    meta_path = task_dir / "meta.json"
    meta_path.write_text(
        json.dumps({"name": "原名稱", "description": "原描述", "output_path": str(output_dir)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    app.config["TASK_FOLDER"] = str(task_root)
    headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}

    rename_response = client.post(
        f"/tasks/{task_id}/rename",
        data={"name": "更新後名稱"},
        headers=headers,
    )
    desc_response = client.post(
        f"/tasks/{task_id}/description",
        data={"description": "更新後描述"},
        headers=headers,
    )

    assert rename_response.status_code == 200
    assert rename_response.is_json
    assert rename_response.get_json()["name"] == "更新後名稱"
    assert desc_response.status_code == 200
    assert desc_response.is_json
    assert desc_response.get_json()["description"] == "更新後描述"

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["name"] == "更新後名稱"
    assert meta["description"] == "更新後描述"
