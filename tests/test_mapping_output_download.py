from pathlib import Path

from flask import url_for


def test_mapping_output_query_download_supports_chinese_path(app, client) -> None:
    task_id = "mapping-download-cn"
    base_dir = Path(app.config["TASK_FOLDER"]) / task_id / "mapping_job" / "中文資料夾"
    base_dir.mkdir(parents=True, exist_ok=True)
    target = base_dir / "測試文件.docx"
    target.write_bytes(b"test-content")

    with app.test_request_context():
        url = url_for(
            "tasks_bp.task_download_output_query",
            task_id=task_id,
            filename="中文資料夾/測試文件.docx",
        )

    response = client.get(url)

    assert response.status_code == 200
    assert response.data == b"test-content"
