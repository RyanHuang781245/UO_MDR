from __future__ import annotations

import json
import os
import re
import threading
import uuid
import zipfile
from datetime import datetime

from flask import current_app, flash, redirect, render_template, request, send_file, url_for

from app.blueprints.tasks.mapping_scheme_helpers import (
    execute_saved_mapping_scheme,
    list_mapping_schemes,
)
from app.services.global_batch_items import encode_batch_item, normalize_batch_items
from app.services.audit_service import record_audit
from app.services.notification_service import send_batch_notification
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


def _normalize_global_batch_items_from_request(raw_items: str = "", raw_task_ids: str = "") -> list[dict]:
    return normalize_batch_items(raw_items=raw_items, raw_task_ids=raw_task_ids)


def _build_mapping_relpaths(task_id: str, run_id: str, zip_file: str = "", log_file: str = "") -> dict:
    base = os.path.join(task_id, "mapping_job", run_id).replace("\\", "/")
    return {
        "run_relpath": base,
        "zip_relpath": f"{base}/{zip_file}" if zip_file else "",
        "log_relpath": f"{base}/{log_file}" if log_file else "",
    }


def _batch_item_label(item: dict) -> str:
    kind = (item.get("kind") or "task").strip().lower()
    task_id = (item.get("task_id") or "").strip()
    task_meta = _load_task_context(task_id) or {}
    task_name = (task_meta.get("name") or task_id).strip() or task_id
    if kind == "mapping_scheme":
        return f"{task_name} / 全部 Mapping"
    return f"{task_name} / 全部流程"


def _resolve_batch_items_for_template(items: list[dict]) -> list[dict]:
    resolved: list[dict] = []
    for item in items:
        kind = (item.get("kind") or "task").strip().lower()
        task_id = (item.get("task_id") or "").strip()
        task_meta = _load_task_context(task_id) or {}
        task_name = (task_meta.get("name") or task_id).strip() or task_id
        mapping_schemes = list_mapping_schemes(task_id) if task_meta else []
        runnable_mapping_schemes = [scheme for scheme in mapping_schemes if scheme.get("is_runnable")]
        mapping_token = (
            encode_batch_item({"kind": "mapping_scheme", "task_id": task_id, "scheme_id": ""})
            if mapping_schemes
            else ""
        )
        selected_mode = "mapping" if kind == "mapping_scheme" and mapping_token else "flow"
        mapping_count = len(mapping_schemes)
        runnable_mapping_count = len(runnable_mapping_schemes)
        if mapping_count:
            mapping_status_label = f"共 {mapping_count} 個，可執行 {runnable_mapping_count} 個"
        else:
            mapping_status_label = "尚未保存 Mapping"

        resolved.append(
            {
                "kind": kind,
                "task_id": task_id,
                "name": task_name,
                "task_name": task_name,
                "description": task_meta.get("description", ""),
                "summary": "流程" if selected_mode == "flow" else f"Mapping：{mapping_count} 個方案",
                "missing": not bool(task_meta),
                "is_runnable": bool(task_meta),
                "status_label": "可執行" if task_meta else "任務不存在",
                "item_token": encode_batch_item({"kind": "task", "task_id": task_id, "scheme_id": ""}) if selected_mode == "flow" else mapping_token,
                "task_token": encode_batch_item({"kind": "task", "task_id": task_id, "scheme_id": ""}),
                "mapping_token": mapping_token,
                "selected_mode": selected_mode,
                "mapping_name": f"{mapping_count} 個方案" if mapping_count else "",
                "mapping_status_label": mapping_status_label,
                "mapping_runnable": bool(runnable_mapping_schemes),
                "mapping_count": mapping_count,
                "runnable_mapping_count": runnable_mapping_count,
            }
        )
    return resolved


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

    for item_result in results:
        if not isinstance(item_result, dict):
            continue
        task_id = (item_result.get("task_id") or "").strip()
        flows = item_result.get("flows")
        if not isinstance(flows, list):
            flows = []
        for flow in flows:
            if isinstance(flow, dict):
                job_id = (flow.get("job_id") or "").strip()
                if not task_id or not job_id:
                    continue
                paths = _build_job_relpaths(task_id, job_id)
                flow.setdefault("job_relpath", paths["job_relpath"])
                flow.setdefault("result_relpath", paths["result_relpath"])
                flow.setdefault("log_relpath", paths["log_relpath"])

        mapping_runs = item_result.get("mapping_runs")
        if not isinstance(mapping_runs, list):
            mapping_runs = []
        for run in mapping_runs:
            if isinstance(run, dict):
                run_id = (run.get("run_id") or "").strip()
                if not task_id or not run_id:
                    continue
                paths = _build_mapping_relpaths(
                    task_id,
                    run_id,
                    zip_file=(run.get("zip_file") or "").strip(),
                    log_file=(run.get("log_file") or "").strip(),
                )
                run.setdefault("run_relpath", paths["run_relpath"])
                run.setdefault("zip_relpath", paths["zip_relpath"])
                run.setdefault("log_relpath", paths["log_relpath"])
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
        items_in_batch = status.get("items") or status.get("tasks") or []
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
                "task_count": len(items_in_batch),
                "ok_count": ok_count,
                "fail_count": fail_count,
            }
        )
    items.sort(key=lambda r: r["created_at"], reverse=True)
    return items[: max(limit, 0)]


def _run_tasks_batch(app, batch_items: list[dict], batch_id: str, actor: dict) -> None:
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
            for i, item in enumerate(batch_items, start=1):
                kind = (item.get("kind") or "task").strip().lower() or "task"
                tid = (item.get("task_id") or "").strip()
                task_meta = _load_task_context(tid) or {}
                task_name = (task_meta.get("name") or tid).strip() or tid
                status["current_task_id"] = tid
                status["current_index"] = i
                status["current_task_name"] = _batch_item_label(item)
                _write_global_batch_status(batch_id, status)

                tdir = os.path.join(current_app.config["TASK_FOLDER"], tid)
                flow_dir = os.path.join(tdir, "flows")
                task_ok = True
                task_errors = []
                flow_results: list[dict] = []
                mapping_results: list[dict] = []
                task_batch_id = ""

                if not task_meta:
                    task_ok = False
                    task_errors.append("Task not found")
                elif kind == "mapping_scheme":
                    schemes = list_mapping_schemes(tid)
                    if not schemes:
                        task_ok = False
                        task_errors.append("No saved mapping scheme found")
                    else:
                        for scheme in schemes:
                            scheme_id = (scheme.get("id") or "").strip()
                            scheme_name = (scheme.get("display_name") or scheme_id).strip() or scheme_id
                            if not scheme.get("is_runnable"):
                                error_message = scheme.get("status_label") or "Mapping scheme is not runnable"
                                mapping_results.append(
                                    {
                                        "scheme_id": scheme_id,
                                        "scheme_name": scheme_name,
                                        "ok": False,
                                        "run_id": "",
                                        "output_count": 0,
                                        "zip_file": "",
                                        "log_file": "",
                                        "zip_relpath": "",
                                        "log_relpath": "",
                                        "error": error_message,
                                    }
                                )
                                task_errors.append(f"{scheme_name}: {error_message}")
                                continue

                            try:
                                run_result = execute_saved_mapping_scheme(
                                    tid,
                                    scheme_id,
                                    actor=actor,
                                    source="global_batch",
                                    global_batch_id=batch_id,
                                )
                            except Exception as exc:
                                mapping_results.append(
                                    {
                                        "scheme_id": scheme_id,
                                        "scheme_name": scheme_name,
                                        "ok": False,
                                        "run_id": "",
                                        "output_count": 0,
                                        "zip_file": "",
                                        "log_file": "",
                                        "zip_relpath": "",
                                        "log_relpath": "",
                                        "error": str(exc),
                                    }
                                )
                                task_errors.append(f"{scheme_name}: {exc}")
                                continue

                            mapping_results.append(
                                {
                                    "scheme_id": scheme_id,
                                    "scheme_name": scheme_name,
                                    "ok": bool(run_result.get("ok")),
                                    "run_id": run_result.get("run_id") or "",
                                    "output_count": int(run_result.get("output_count") or 0),
                                    "zip_file": run_result.get("zip_file") or "",
                                    "log_file": run_result.get("log_file") or "",
                                    "zip_relpath": run_result.get("zip_relpath") or "",
                                    "log_relpath": run_result.get("log_relpath") or "",
                                    "error": run_result.get("error") or "",
                                }
                            )
                            if not run_result.get("ok"):
                                task_errors.append(run_result.get("error") or "Mapping execution failed")
                        task_ok = all(run.get("ok") for run in mapping_results)
                        failed_mapping_count = sum(1 for run in mapping_results if not run.get("ok"))
                        send_batch_notification(
                            task_id=tid,
                            batch_id=batch_id,
                            status="failed" if failed_mapping_count else "completed",
                            results=mapping_results,
                            actor_work_id=actor.get("work_id", ""),
                            actor_label=actor.get("label", ""),
                            completed_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            error=(f"{failed_mapping_count} mapping scheme(s) failed" if failed_mapping_count else None),
                        )
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
                        "kind": kind,
                        "task_id": tid,
                        "name": task_name,
                        "label": _batch_item_label(item),
                        "ok": task_ok,
                        "errors": task_errors,
                        "flows": flow_results,
                        "mapping_runs": mapping_results,
                        "scheme_id": "",
                        "scheme_name": "",
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
                    "items": batch_items,
                    "count": len(results),
                    "failed_count": sum(1 for item in results if not item.get("ok")),
                    "error": terminal_error or status.get("error") or "",
                },
                task_id=None,
            )


@global_batch_bp.get("", endpoint="global_batch_page")
def global_batch_page():
    raw_ids = request.args.get("task_ids", "")
    raw_items = request.args.get("items", "")
    batch_items = _normalize_global_batch_items_from_request(raw_items, raw_ids)
    batch_id = request.args.get("batch")
    batch_status = _load_global_batch_status(batch_id) if batch_id else None

    if not batch_items and batch_id:
        status = _load_global_batch_status(batch_id)
        if status:
            batch_items = status.get("items") or normalize_batch_items(raw_task_ids=",".join(status.get("tasks") or []))
        else:
            flash("找不到指定的任務排程批次。", "warning")
            batch_id = None

    tasks = _resolve_batch_items_for_template(batch_items)
    batch_item_tokens = ",".join(item.get("item_token") or "" for item in tasks if item.get("item_token"))
    task_ids = [item["task_id"] for item in batch_items if (item.get("kind") or "task") == "task"]

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
        batch_item_tokens=batch_item_tokens,
        batch_task_ids=",".join(task_ids),
        batch_id=batch_id,
        batch_status=batch_status,
        global_batches=history_slice,
        pagination=pagination,
    )


@global_batch_bp.post("/run", endpoint="run_global_batch")
def run_global_batch():
    raw_ids = request.form.get("task_ids", "")
    raw_items = request.form.get("batch_items", "")
    batch_items = _normalize_global_batch_items_from_request(raw_items, raw_ids)
    if not batch_items:
        flash("無效的任務清單。", "danger")
        return redirect(url_for("tasks_bp.tasks"))

    valid_items = []
    invalid_items = []
    for item in batch_items:
        task_id = (item.get("task_id") or "").strip()
        if not _load_task_context(task_id):
            invalid_items.append(_batch_item_label(item))
            continue
        if (item.get("kind") or "task") == "mapping_scheme":
            schemes = list_mapping_schemes(task_id)
            if not schemes:
                invalid_items.append(_batch_item_label(item))
                continue
        valid_items.append(item)

    if not valid_items:
        flash("找不到可執行的任務。", "danger")
        return redirect(url_for("tasks_bp.tasks"))
    if invalid_items:
        flash(f"以下項目不存在，已略過：{', '.join(invalid_items)}", "warning")

    batch_id = str(uuid.uuid4())[:8]
    work_id, label = _get_actor_info()

    stored_items = [{**item, "label": _batch_item_label(item)} for item in valid_items]

    status = {
        "id": batch_id,
        "status": "queued",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tasks": [item["task_id"] for item in valid_items if (item.get("kind") or "task") == "task"],
        "items": stored_items,
        "invalid_tasks": invalid_items,
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
        args=(app, valid_items, batch_id, {"work_id": work_id, "label": label}),
        daemon=True,
    )
    thread.start()

    record_audit(
        action="global_task_batch_queued",
        actor={"work_id": work_id, "label": label},
        detail={"batch_id": batch_id, "items": valid_items, "invalid_items": invalid_items},
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
                for run in task_result.get("mapping_runs") or []:
                    if not run.get("ok"):
                        continue
                    run_id = (run.get("run_id") or "").strip()
                    if not run_id:
                        continue
                    scheme_name = (run.get("scheme_name") or "mapping").strip() or "mapping"
                    scheme_slug = re.sub(r"[^\w\-]+", "_", scheme_name).strip("_") or "mapping"
                    mapping_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id, "mapping_job", run_id)
                    mapping_filename = (
                        (run.get("zip_file") or "").strip()
                        if kind == "docx"
                        else (run.get("log_file") or "").strip()
                    )
                    if not mapping_filename:
                        continue
                    src = os.path.join(mapping_dir, mapping_filename)
                    if not os.path.exists(src):
                        continue
                    arcname = os.path.join(task_slug, scheme_slug, run_id, mapping_filename)
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
