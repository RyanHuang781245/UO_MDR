from __future__ import annotations

import json
from pathlib import Path

from app.extensions import db
from app.models.task import TaskRecord
from app.services.task_service import list_tasks


def _write_task_meta(task_dir: Path, *, name: str, description: str = "", nas_path: str = "") -> None:
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "files").mkdir(exist_ok=True)
    (task_dir / "flows").mkdir(exist_ok=True)
    (task_dir / "output").mkdir(exist_ok=True)
    (task_dir / "meta.json").write_text(
        json.dumps(
            {
                "name": name,
                "description": description,
                "nas_path": nas_path,
                "output_path": str(task_dir / "output"),
                "created": "2026-05-21 18:00",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_copy_task_records_new_task_in_db(app, client, tmp_path: Path) -> None:
    app.config["TASK_FOLDER"] = str(tmp_path)
    source_task_id = "source01"
    source_task_dir = tmp_path / source_task_id
    _write_task_meta(
        source_task_dir,
        name="來源任務",
        description="原始描述",
        nas_path=r"\\nas\folder\project",
    )
    (source_task_dir / "files" / "sample.docx").write_text("copy-me", encoding="utf-8")

    response = client.post(
        f"/tasks/{source_task_id}/copy",
        data={"name": "複製後任務"},
        follow_redirects=False,
    )

    assert response.status_code == 302

    rows = TaskRecord.query.filter_by(name="複製後任務").all()
    assert len(rows) == 1
    copied = rows[0]
    assert copied.description == "原始描述"
    assert copied.nas_path == r"\\nas\folder\project"
    assert Path(copied.output_path) == tmp_path / copied.id / "output"
    assert (tmp_path / copied.id / "files" / "sample.docx").read_text(encoding="utf-8") == "copy-me"


def test_list_tasks_backfills_missing_task_records(app, tmp_path: Path) -> None:
    app.config["TASK_FOLDER"] = str(tmp_path)
    task_id = "legacy01"
    task_dir = tmp_path / task_id
    _write_task_meta(
        task_dir,
        name="歷史任務",
        description="從檔案系統補齊",
        nas_path=r"D:\legacy-source",
    )

    assert db.session.get(TaskRecord, task_id) is None

    task_rows = list_tasks()

    assert any(row["id"] == task_id for row in task_rows)
    record = db.session.get(TaskRecord, task_id)
    assert record is not None
    assert record.name == "歷史任務"
    assert record.description == "從檔案系統補齊"
    assert record.nas_path == r"D:\legacy-source"
