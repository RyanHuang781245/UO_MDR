import json
import re
from pathlib import Path


def _create_task(task_root: Path, task_id: str, day: int) -> None:
    task_dir = task_root / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "meta.json").write_text(
        json.dumps(
            {
                "name": f"Task {day:02d}",
                "description": "",
                "created": f"2026-05-{day:02d} 09:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _listed_task_ids(html: str) -> list[str]:
    return re.findall(r'<tr class="task-row" data-task-id="([^"]+)"', html)


def test_task_listing_pinned_task_from_later_page_moves_to_first_page(app, client, tmp_path):
    app.config["TASK_FOLDER"] = str(tmp_path)
    for day in range(1, 13):
        _create_task(tmp_path, f"task-{day:02d}", day)

    default_resp = client.get("/tasks")
    default_ids = _listed_task_ids(default_resp.get_data(as_text=True))
    assert "task-01" not in default_ids

    pinned_resp = client.get("/tasks?pinned_task_ids=task-01")
    pinned_ids = _listed_task_ids(pinned_resp.get_data(as_text=True))

    assert pinned_ids[0] == "task-01"
    assert len(pinned_ids) == 10
    assert "task-12" in pinned_ids
