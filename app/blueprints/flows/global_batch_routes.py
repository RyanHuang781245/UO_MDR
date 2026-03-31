from __future__ import annotations

import json
import os
import re
import threading
import uuid
import zipfile
from datetime import datetime

from flask import current_app, flash, redirect, render_template, request, send_file, url_for

from app.services.audit_service import record_audit
from app.services.task_service import load_task_context as _load_task_context
from app.services.user_context_service import get_actor_info as _get_actor_info
from .global_batch_blueprint import global_batch_bp
from .flow_route_helpers import _write_json_with_replace_retry
from .run_helpers import (
    _load_batch_status,
    _load_saved_flows,
    _run_flow_batch,
    _write_batch_status,
)


def _global_batch_status_path(batch_id: str) -> str:
    path = os.path.join(current_app.config["TASK_FOLDER"], "global_batches")
    os.makedirs(path, exist_ok=True)
    return os.path.join(path, f"{batch_id}.json")


def _normalize_global_task_ids(raw_ids: str) -> list[str]:
    task_ids: list[str] = []
    seen: set[str] = set()
    for part in (raw_ids or "").split(","):
        tid = part.strip()
        if not tid or tid in seen:
            continue
        seen.add(tid)
        task_ids.append(tid)
    return task_ids


def _write_global_batch_status(batch_id: str, payload: dict) -> None:
    path = _global_batch_status_path(batch_id)
    _write_json_with_replace_retry(path, payload)


def _load_global_batch_status(batch_id: str) -> dict | None:
    path = _global_batch_status_path(batch_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            status = json.load(f)
        if isinstance(status, dict):
            return _enrich_global_batch_status(status)
        return None
    except Exception:
        return None


def _build_job_relpaths(task_id: str, job_id: str) -> dict:
    base = os.path.join(task_id, "jobs", job_id).replace("\\", "/")
    return {
        "job_relpath": base,
        "result_relpath": f"{base}/result.docx",
        "log_relpath": f"{base}/log.json",
    }


def _enrich_global_batch_status(status: dict) -> dict:
    results = status.get("results")
    if not isinstance(results, list):
        return status

    for task_result in results:
        if not isinstance(task_result, dict):
            continue
        task_id = (task_result.get("task_id") or "").strip()
        flows = task_result.get("flows")
        if not isinstance(flows, list):
            continue
        for flow in flows:
            if not isinstance(flow, dict):
                continue
            job_id = (flow.get("job_id") or "").strip()
            if not task_id or not job_id:
                continue
            paths = _build_job_relpaths(task_id, job_id)
            flow.setdefault("job_relpath", paths["job_relpath"])
            flow.setdefault("result_relpath", paths["result_relpath"])
            flow.setdefault("log_relpath", paths["log_relpath"])
    return status


def _list_global_batch_statuses(limit: int = 100) -> list[dict]:
    status_dir = os.path.join(current_app.config["TASK_FOLDER"], "global_batches")
    if not os.path.isdir(status_dir):
        return []
    items: list[dict] = []
    for fn in os.listdir(status_dir):
        if not fn.endswith(".json"):
            continue
        batch_id = os.path.splitext(fn)[0]
        status = _load_global_batch_status(batch_id) or {}
        created_at = status.get("created_at")
        if not created_at:
            created_at = datetime.fromtimestamp(
                os.path.getmtime(os.path.join(status_dir, fn))
            ).strftime("%Y-%m-%d %H:%M:%S")
        tasks = status.get("tasks") or []
        results = status.get("results") or []
        ok_count = sum(1 for item in results if item.get("ok"))
        fail_count = sum(1 for item in results if not item.get("ok"))
        items.append(
            {
                "id": batch_id,
                "status": (status.get("status") or "unknown").lower(),
                "created_at": created_at,
                "completed_at": status.get("completed_at") or "",
                "current_task_name": status.get("current_task_name") or "",
                "task_count": len(tasks),
                "ok_count": ok_count,
                "fail_count": fail_count,
            }
        )
    items.sort(key=lambda r: r["created_at"], reverse=True)
    return items[: max(limit, 0)]


def _run_tasks_batch(app, task_ids: list[str], batch_id: str, actor: dict) -> None:
    with app.app_context():
        status = _load_global_batch_status(batch_id) or {}
        status.update(
            {
                "status": "running",
                "current_index": 0,
                "current_task_id": "",
                "current_task_name": "",
            }
        )
        _write_global_batch_status(batch_id, status)

        results = []
        any_failed = False
        terminal_error = ""
        try:
            for i, tid in enumerate(task_ids, start=1):
                task_meta = _load_task_context(tid) or {}
                task_name = (task_meta.get("name") or tid).strip() or tid
                status["current_task_id"] = tid
                status["current_index"] = i
                status["current_task_name"] = task_name
                _write_global_batch_status(batch_id, status)

                tdir = os.path.join(current_app.config["TASK_FOLDER"], tid)
                flow_dir = os.path.join(tdir, "flows")
                task_ok = True
                task_errors = []
                flow_results: list[dict] = []
                task_batch_id = ""

                if not task_meta:
                    task_ok = False
                    task_errors.append("Task not found")
                elif not os.path.isdir(flow_dir):
                    task_ok = False
                    task_errors.append("Flow directory not found")
                else:
                    flows_to_run = _load_saved_flows(flow_dir)
                    if not flows_to_run:
                        task_ok = False
                        task_errors.append("No saved flow found")
                    else:
                        flow_sequence = [
                            (f.get("name") or "").strip()
                            for f in flows_to_run
                            if (f.get("name") or "").strip()
                        ]
                        if not flow_sequence:
                            task_ok = False
                            task_errors.append("No runnable flow found")
                        else:
                            task_batch_id = str(uuid.uuid4())[:8]
                            _write_batch_status(
                                tid,
                                task_batch_id,
                                {
                                    "id": task_batch_id,
                                    "status": "queued",
                                    "flows": flow_sequence,
                                    "current_index": 0,
                                    "current_flow": "",
                                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "actor": actor.get("label") or actor.get("work_id", ""),
                                },
                            )
                            _run_flow_batch(
                                app,
                                tid,
                                flow_sequence,
                                task_batch_id,
                                actor,
                                source="global_batch",
                                global_batch_id=batch_id,
                            )
                            task_batch_status = _load_batch_status(tid, task_batch_id) or {}
                            task_ok = (task_batch_status.get("status") or "").lower() == "completed"
                            batch_results = task_batch_status.get("results") or []
                            for item in batch_results:
                                flow_name = (item.get("flow") or "").strip()
                                flow_ok = (item.get("status") or "").lower() == "completed"
                                flow_error = (item.get("error") or "").strip()
                                flow_job_id = (item.get("job_id") or "").strip()
                                path_info = _build_job_relpaths(tid, flow_job_id) if flow_job_id else {}
                                flow_results.append(
                                    {
                                        "flow": flow_name,
                                        "ok": flow_ok,
                                        "job_id": flow_job_id,
                                        "error": flow_error,
                                        "job_relpath": path_info.get("job_relpath", ""),
                                        "result_relpath": path_info.get("result_relpath", ""),
                                        "log_relpath": path_info.get("log_relpath", ""),
                                    }
                                )
                                if not flow_ok:
                                    task_errors.append(
                                        f"{flow_name}: {flow_error or 'Workflow step failed'}"
                                    )

                results.append(
                    {
                        "task_id": tid,
                        "name": task_name,
                        "ok": task_ok,
                        "errors": task_errors,
                        "flows": flow_results,
                        "task_batch_id": task_batch_id,
                    }
                )
                any_failed = any_failed or (not task_ok)
                status["results"] = results
                _write_global_batch_status(batch_id, status)

        except Exception as exc:
            current_app.logger.exception("Global batch failed")
            any_failed = True
            terminal_error = str(exc)
            status["error"] = terminal_error
        finally:
            completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status["status"] = "failed" if any_failed else "completed"
            status["completed_at"] = completed_at
            status["results"] = results
            _write_global_batch_status(batch_id, status)

            actor_work_id = actor.get("work_id", "")
            actor_label = actor.get("label", "")

            record_audit(
                action="global_task_batch_completed" if status["status"] == "completed" else "global_task_batch_failed",
                actor={"work_id": actor_work_id, "label": actor_label},
                detail={
                    "batch_id": batch_id,
                    "status": status["status"],
                    "tasks": task_ids,
                    "count": len(results),
                    "failed_count": sum(1 for item in results if not item.get("ok")),
                    "error": terminal_error or status.get("error") or "",
                },
                task_id=None,
            )


@global_batch_bp.get("", endpoint="global_batch_page")
def global_batch_page():
    raw_ids = request.args.get("task_ids", "")
    task_ids = _normalize_global_task_ids(raw_ids)
    batch_id = request.args.get("batch")
    batch_status = _load_global_batch_status(batch_id) if batch_id else None

    if not task_ids and batch_id:
        status = _load_global_batch_status(batch_id)
        if status:
            task_ids = status.get("tasks", [])
        else:
            flash("找不到指定的任務排程批次。", "warning")
            batch_id = None

    tasks = []
    for tid in task_ids:
        task_meta = _load_task_context(tid) or {}
        tasks.append(
            {
                "id": tid,
                "name": (task_meta.get("name") or tid).strip() or tid,
                "description": task_meta.get("description", ""),
                "missing": not bool(task_meta),
            }
        )

    all_history = _list_global_batch_statuses(limit=500)
    page = request.args.get("page", 1, type=int)
    per_page = 10
    total_count = len(all_history)
    total_pages = (total_count + per_page - 1) // per_page
    start = (page - 1) * per_page
    history_slice = all_history[start : start + per_page]

    pagination = {
        "page": page,
        "total_pages": total_pages,
        "total_count": total_count,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }

    return render_template(
        "flows/global_batch.html",
        tasks=tasks,
        batch_id=batch_id,
        batch_status=batch_status,
        global_batches=history_slice,
        pagination=pagination,
    )


@global_batch_bp.post("/run", endpoint="run_global_batch")
def run_global_batch():
    raw_ids = request.form.get("task_ids", "")
    task_ids = _normalize_global_task_ids(raw_ids)
    if not task_ids:
        flash("無效的任務清單。", "danger")
        return redirect(url_for("tasks_bp.tasks"))

    valid_task_ids = []
    invalid_task_ids = []
    for tid in task_ids:
        if _load_task_context(tid):
            valid_task_ids.append(tid)
        else:
            invalid_task_ids.append(tid)

    if not valid_task_ids:
        flash("找不到可執行的任務。", "danger")
        return redirect(url_for("tasks_bp.tasks"))
    if invalid_task_ids:
        flash(f"以下任務不存在，已略過：{', '.join(invalid_task_ids)}", "warning")

    batch_id = str(uuid.uuid4())[:8]
    work_id, label = _get_actor_info()

    status = {
        "id": batch_id,
        "status": "queued",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tasks": valid_task_ids,
        "invalid_tasks": invalid_task_ids,
        "current_index": 0,
        "current_task_id": "",
        "current_task_name": "",
        "actor": {"work_id": work_id, "label": label},
        "results": [],
    }
    _write_global_batch_status(batch_id, status)

    app = current_app._get_current_object()
    thread = threading.Thread(
        target=_run_tasks_batch,
        args=(app, valid_task_ids, batch_id, {"work_id": work_id, "label": label}),
        daemon=True,
    )
    thread.start()

    record_audit(
        action="global_task_batch_queued",
        actor={"work_id": work_id, "label": label},
        detail={"batch_id": batch_id, "tasks": valid_task_ids, "invalid_tasks": invalid_task_ids},
        task_id=None,
    )

    return redirect(url_for("global_batch_bp.global_batch_page", batch=batch_id))


@global_batch_bp.get("/<batch_id>/status", endpoint="global_batch_status")
def global_batch_status(batch_id):
    status = _load_global_batch_status(batch_id)
    if not status:
        return {"ok": False, "error": "Batch not found"}, 404
    return {"ok": True, "status": status}


@global_batch_bp.post("/<batch_id>/download", endpoint="download_global_batch")
def download_global_batch(batch_id):
    status = _load_global_batch_status(batch_id)
    if not status:
        flash("Batch not found", "warning")
        return redirect(url_for("global_batch_bp.global_batch_page"))

    kind = (request.form.get("kind") or "docx").strip().lower()
    if kind not in {"docx", "log"}:
        kind = "docx"
    filename = "result.docx" if kind == "docx" else "log.json"

    status_dir = os.path.dirname(_global_batch_status_path(batch_id))
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_name = f"global_batch_{batch_id}_{kind}_{stamp}.zip"
    zip_path = os.path.join(status_dir, zip_name)

    added = 0
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for task_result in status.get("results") or []:
                task_id = (task_result.get("task_id") or "").strip()
                if not task_id:
                    continue
                task_name = (task_result.get("name") or task_id).strip() or task_id
                task_slug = re.sub(r"[^\w\-]+", "_", task_name).strip("_") or task_id
                for flow in task_result.get("flows") or []:
                    if not flow.get("ok"):
                        continue
                    job_id = (flow.get("job_id") or "").strip()
                    flow_name = (flow.get("flow") or "flow").strip() or "flow"
                    flow_slug = re.sub(r"[^\w\-]+", "_", flow_name).strip("_") or "flow"
                    if not job_id:
                        continue
                    job_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id, "jobs", job_id)
                    src = os.path.join(job_dir, filename)
                    if not os.path.exists(src):
                        continue
                    arcname = os.path.join(task_slug, flow_slug, job_id, filename)
                    zf.write(src, arcname=arcname)
                    added += 1
        if added == 0:
            flash("No downloadable files found in this batch", "warning")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return redirect(url_for("global_batch_bp.global_batch_page", batch=batch_id))
        return send_file(zip_path, as_attachment=True, download_name=zip_name)
    except Exception:
        current_app.logger.exception("Failed to build global batch download zip")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        flash("Failed to prepare batch download", "danger")
        return redirect(url_for("global_batch_bp.global_batch_page", batch=batch_id))
