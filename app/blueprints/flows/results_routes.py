from __future__ import annotations

import json
import os
import shutil
import zipfile
from datetime import datetime
from urllib.parse import urlparse

from flask import abort, current_app, flash, redirect, request, send_file, url_for

from app.extensions import db
from app.models.execution import JobRecord
from app.models.mapping_metadata import MappingRunRecord
from app.services.execution_service import cancel_job, delete_job_record, retry_job
from app.services.user_context_service import get_actor_info as _get_actor_info
from .flow_results_blueprint import flow_results_bp
from .mapping_run_blueprint import mapping_run_bp
from .run_helpers import (
    _job_has_error,
    _list_flow_runs,
    _read_job_meta,
    _read_mapping_run_meta,
)


def _read_flow_log_entries(job_dir: str) -> list[dict]:
    log_json_path = os.path.join(job_dir, "log.json")
    if not os.path.isfile(log_json_path):
        return []
    try:
        with open(log_json_path, "r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _redirect_with_fallback(default_endpoint: str, **default_values):
    next_url = (request.form.get("next") or request.args.get("next") or "").strip()
    if next_url:
        parsed = urlparse(next_url)
        if not parsed.scheme and not parsed.netloc and next_url.startswith("/"):
            return redirect(next_url)
    return redirect(url_for(default_endpoint, **default_values))


def _default_results_redirect(task_id: str, active_tab: str = "flows"):
    if active_tab == "mapping":
        query_params = {"mapping_tab": "results"}
        legacy_param_map = {
            "page": "mrpage",
            "q": "mq",
            "status": "mstatus",
            "start_date": "mstart_date",
            "end_date": "mend_date",
        }
        endpoint = "tasks_bp.task_mapping"
    else:
        query_params = {"flow_tab": "results"}
        legacy_param_map = {
            "page": "rpage",
            "q": "rq",
            "status": "rstatus",
            "start_date": "rstart_date",
            "end_date": "rend_date",
        }
        endpoint = "flow_builder_bp.flow_builder"

    for source_name, target_name in legacy_param_map.items():
        value = (request.args.get(source_name) or "").strip()
        if value:
            query_params[target_name] = value

    return redirect(url_for(endpoint, task_id=task_id, **query_params))


@flow_results_bp.post("/runs/<job_id>/delete", endpoint="delete_flow_run")
def delete_flow_run(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    record = db.session.get(JobRecord, job_id)
    if not os.path.isdir(job_dir) and not record:
        abort(404)
    try:
        if os.path.isdir(job_dir):
            shutil.rmtree(job_dir)
        delete_job_record(job_id)
        flash("已刪除執行紀錄。", "success")
    except Exception:
        current_app.logger.exception("Failed to delete flow run")
        flash("刪除失敗，請稍後再試。", "danger")
    return _redirect_with_fallback("flow_builder_bp.flow_builder", task_id=task_id, flow_tab="results")


@flow_results_bp.post("/runs/<job_id>/cancel", endpoint="cancel_flow_run")
def cancel_flow_run(task_id, job_id):
    record = db.session.get(JobRecord, job_id)
    if not record or record.task_id != task_id:
        abort(404)
    ok, message = cancel_job(job_id)
    flash(message, "success" if ok else "warning")
    return _redirect_with_fallback("flow_builder_bp.flow_builder", task_id=task_id, flow_tab="results")


@flow_results_bp.post("/runs/<job_id>/retry", endpoint="retry_flow_run")
def retry_flow_run(task_id, job_id):
    record = db.session.get(JobRecord, job_id)
    if not record or record.task_id != task_id:
        abort(404)
    work_id, label = _get_actor_info()
    ok, message, new_job_id = retry_job(job_id, actor={"work_id": work_id, "label": label})
    flash(message if not new_job_id else f"{message}：{new_job_id}", "success" if ok else "warning")
    return _redirect_with_fallback("flow_builder_bp.flow_builder", task_id=task_id, flow_tab="results")


@flow_results_bp.post("/runs/delete", endpoint="delete_flow_runs_bulk")
def delete_flow_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    raw = request.form.get("job_ids", "")
    job_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not job_ids:
        flash("請先選取要刪除的執行紀錄。", "warning")
        return _redirect_with_fallback("flow_builder_bp.flow_builder", task_id=task_id, flow_tab="results")
    deleted = 0
    for job_id in job_ids:
        job_dir = os.path.join(jobs_dir, job_id)
        if not os.path.isdir(job_dir) and not db.session.get(JobRecord, job_id):
            continue
        try:
            if os.path.isdir(job_dir):
                shutil.rmtree(job_dir)
            delete_job_record(job_id)
            deleted += 1
        except Exception:
            current_app.logger.exception("Failed to delete flow run")
    if deleted:
        flash(f"已刪除 {deleted} 筆執行紀錄。", "success")
    else:
        flash("沒有可刪除的執行紀錄。", "warning")
    return _redirect_with_fallback("flow_builder_bp.flow_builder", task_id=task_id, flow_tab="results")


@flow_results_bp.post("/runs/download", endpoint="download_flow_runs_bulk")
def download_flow_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    kind = request.form.get("kind", "docx")
    raw = request.form.get("job_ids", "")
    job_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not job_ids:
        flash("請先選取要下載的執行紀錄。", "warning")
        return _redirect_with_fallback("flow_builder_bp.flow_builder", task_id=task_id, flow_tab="results")
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_name = f"flow_runs_{kind}_{stamp}.zip"
    zip_path = os.path.join(jobs_dir, zip_name)
    added = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for job_id in job_ids:
            job_dir = os.path.join(jobs_dir, job_id)
            if not os.path.isdir(job_dir):
                continue
            filename = "result.docx" if kind == "docx" else "log.json"
            src = os.path.join(job_dir, filename)
            if not os.path.exists(src):
                continue
            zf.write(src, arcname=os.path.join(job_id, filename))
            added += 1
    if added == 0:
        flash("沒有可下載的檔案。", "warning")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return _redirect_with_fallback("flow_builder_bp.flow_builder", task_id=task_id, flow_tab="results")
    return send_file(zip_path, as_attachment=True, download_name=zip_name)


@mapping_run_bp.post("/<run_id>/delete", endpoint="delete_mapping_run")
def delete_mapping_run(task_id, run_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    run_dir = os.path.join(tdir, "mapping_job", run_id)
    record = db.session.get(JobRecord, run_id)
    if not os.path.isdir(run_dir) and not record:
        abort(404)
    try:
        if os.path.isdir(run_dir):
            shutil.rmtree(run_dir)
        delete_job_record(run_id)
        mapping_record = db.session.get(MappingRunRecord, run_id)
        if mapping_record:
            db.session.delete(mapping_record)
            db.session.commit()
        flash("已刪除 Mapping 執行紀錄。", "success")
    except Exception:
        current_app.logger.exception("Failed to delete mapping run")
        flash("刪除失敗，請稍後再試。", "danger")
    return _redirect_with_fallback("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")


@mapping_run_bp.post("/<run_id>/cancel", endpoint="cancel_mapping_run")
def cancel_mapping_run(task_id, run_id):
    record = db.session.get(JobRecord, run_id)
    if not record or record.task_id != task_id:
        abort(404)
    ok, message = cancel_job(run_id)
    flash(message, "success" if ok else "warning")
    return _redirect_with_fallback("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")


@mapping_run_bp.post("/<run_id>/retry", endpoint="retry_mapping_run")
def retry_mapping_run(task_id, run_id):
    record = db.session.get(JobRecord, run_id)
    if not record or record.task_id != task_id:
        abort(404)
    work_id, label = _get_actor_info()
    ok, message, new_job_id = retry_job(run_id, actor={"work_id": work_id, "label": label})
    flash(message if not new_job_id else f"{message}：{new_job_id}", "success" if ok else "warning")
    return _redirect_with_fallback("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")


@mapping_run_bp.post("/delete", endpoint="delete_mapping_runs_bulk")
def delete_mapping_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    mapping_dir = os.path.join(tdir, "mapping_job")
    raw = request.form.get("run_ids", "")
    run_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not run_ids:
        flash("請先選取要刪除的 Mapping 執行紀錄。", "warning")
        return _redirect_with_fallback("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")
    deleted = 0
    for run_id in run_ids:
        run_dir = os.path.join(mapping_dir, run_id)
        if not os.path.isdir(run_dir) and not db.session.get(JobRecord, run_id):
            continue
        try:
            if os.path.isdir(run_dir):
                shutil.rmtree(run_dir)
            delete_job_record(run_id)
            mapping_record = db.session.get(MappingRunRecord, run_id)
            if mapping_record:
                db.session.delete(mapping_record)
                db.session.commit()
            deleted += 1
        except Exception:
            current_app.logger.exception("Failed to delete mapping run")
    if deleted:
        flash(f"已刪除 {deleted} 筆 Mapping 執行紀錄。", "success")
    else:
        flash("沒有可刪除的 Mapping 執行紀錄。", "warning")
    return _redirect_with_fallback("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")


@mapping_run_bp.post("/download", endpoint="download_mapping_runs_bulk")
def download_mapping_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    mapping_dir = os.path.join(tdir, "mapping_job")
    kind = request.form.get("kind", "zip")
    raw = request.form.get("run_ids", "")
    run_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not run_ids:
        flash("請先選取要下載的 Mapping 執行紀錄。", "warning")
        return _redirect_with_fallback("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    zip_name = f"mapping_runs_{kind}_{stamp}.zip"
    zip_path = os.path.join(mapping_dir, zip_name)
    added = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for run_id in run_ids:
            run_dir = os.path.join(mapping_dir, run_id)
            if not os.path.isdir(run_dir):
                continue
            meta = _read_mapping_run_meta(run_dir)
            filename = (meta.get("zip_file") if kind == "zip" else meta.get("log_file")) or ""
            filename = str(filename).strip()
            if not filename:
                continue
            src = os.path.join(run_dir, filename)
            if not os.path.exists(src):
                continue
            zf.write(src, arcname=os.path.join(run_id, filename))
            added += 1
    if added == 0:
        flash("沒有可下載的檔案。", "warning")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return _redirect_with_fallback("tasks_bp.task_mapping", task_id=task_id, mapping_tab="results")
    return send_file(zip_path, as_attachment=True, download_name=zip_name)


@flow_results_bp.get("/results", endpoint="flow_results")
def flow_results(task_id):
    view = (request.args.get("view") or "single").lower()
    if view == "batch":
        return redirect(url_for("global_batch_bp.global_batch_page"))
    active_tab = (request.args.get("tab") or "flows").strip().lower()
    if active_tab not in {"flows", "mapping"}:
        active_tab = "flows"
    return _default_results_redirect(task_id, active_tab=active_tab)


@flow_results_bp.get("/runs/<job_id>/status", endpoint="flow_run_status")
def flow_run_status(task_id, job_id):
    record = db.session.get(JobRecord, job_id)
    if not record or record.task_id != task_id:
        return {"ok": False, "error": "Run not found"}, 404
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    meta = _read_job_meta(job_dir) if os.path.isdir(job_dir) else {}
    status = str(record.status or meta.get("status") or "unknown").strip().lower()
    if os.path.isdir(job_dir) and _job_has_error(job_dir):
        status = "failed"
    flow_name = str(record.target_name or meta.get("flow_name") or "").strip()
    result_exists = os.path.isfile(os.path.join(job_dir, "result.docx")) if os.path.isdir(job_dir) else False
    log_exists = os.path.isfile(os.path.join(job_dir, "log.json")) if os.path.isdir(job_dir) else False
    return {
        "ok": True,
        "status": status,
        "flow_name": flow_name,
        "has_result": bool(result_exists),
        "has_log": bool(log_exists),
        "detail_url": url_for("flow_results_bp.flow_run_detail", task_id=task_id, job_id=job_id),
        "compare_url": url_for("tasks_bp.task_compare", task_id=task_id, job_id=job_id) if result_exists else "",
        "docx_url": url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="docx"),
        "log_url": url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="log"),
        "cancel_url": url_for("flow_results_bp.cancel_flow_run", task_id=task_id, job_id=job_id),
        "retry_url": url_for("flow_results_bp.retry_flow_run", task_id=task_id, job_id=job_id),
    }


@flow_results_bp.get("/runs/<job_id>/detail", endpoint="flow_run_detail")
def flow_run_detail(task_id, job_id):
    record = db.session.get(JobRecord, job_id)
    if not record or record.task_id != task_id:
        return {"ok": False, "error": "Run not found"}, 404
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    meta = _read_job_meta(job_dir) if os.path.isdir(job_dir) else {}
    status = str(record.status or meta.get("status") or "unknown").strip().lower()
    if os.path.isdir(job_dir) and _job_has_error(job_dir):
        status = "failed"
    flow_name = str(record.target_name or meta.get("flow_name") or "").strip() or "未命名流程"
    log_entries = _read_flow_log_entries(job_dir) if os.path.isdir(job_dir) else []
    has_result = os.path.isfile(os.path.join(job_dir, "result.docx")) if os.path.isdir(job_dir) else False
    has_log = os.path.isfile(os.path.join(job_dir, "log.json")) if os.path.isdir(job_dir) else False
    return {
        "ok": True,
        "job_id": job_id,
        "flow_name": flow_name,
        "status": status,
        "error": str(record.error_summary or meta.get("error") or "").strip(),
        "log_entries": log_entries,
        "has_result": has_result,
        "has_log": has_log,
        "docx_url": url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="docx") if has_result else "",
        "log_url": url_for("tasks_bp.task_download", task_id=task_id, job_id=job_id, kind="log") if has_log else "",
        "compare_url": url_for("tasks_bp.task_compare", task_id=task_id, job_id=job_id) if has_result else "",
    }


@flow_results_bp.get("/runs/active", endpoint="flow_run_active")
def flow_run_active(task_id):
    runs = _list_flow_runs(task_id)
    active = [r for r in runs if r["status"] in ("queued", "running")]
    return {
        "ok": True,
        "runs": [
            {"job_id": r["job_id"], "status": r["status"], "flow_name": r["flow_name"]}
            for r in active
        ],
    }


@mapping_run_bp.get("/<run_id>/status", endpoint="mapping_run_status")
def mapping_run_status(task_id, run_id):
    record = db.session.get(JobRecord, run_id)
    if not record or record.task_id != task_id:
        return {"ok": False, "error": "Run not found"}, 404

    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    run_dir = os.path.join(tdir, "mapping_job", run_id)
    meta = _read_mapping_run_meta(run_dir) if os.path.isdir(run_dir) else {}
    status = str(record.status or meta.get("status") or "unknown").strip().lower()
    mapping_name = str(record.target_name or meta.get("mapping_file") or "").strip()

    zip_file = str(meta.get("zip_file") or "").strip()
    log_file = str(meta.get("log_file") or "").strip()
    zip_rel = f"{run_id}/{zip_file}" if zip_file and "/" not in zip_file and "\\" not in zip_file else zip_file
    log_rel = f"{run_id}/{log_file}" if log_file and "/" not in log_file and "\\" not in log_file else log_file
    zip_exists = os.path.isfile(os.path.join(tdir, "mapping_job", zip_rel.replace("/", os.sep))) if zip_rel else False
    log_exists = os.path.isfile(os.path.join(tdir, "mapping_job", log_rel.replace("/", os.sep))) if log_rel else False

    return {
        "ok": True,
        "status": status,
        "mapping_name": mapping_name,
        "has_zip": bool(zip_exists),
        "has_log": bool(log_exists),
        "zip_url": url_for("tasks_bp.task_download_output_query", task_id=task_id, filename=zip_rel) if zip_rel else "",
        "log_url": url_for("tasks_bp.task_download_output_query", task_id=task_id, filename=log_rel) if log_rel else "",
        "cancel_url": url_for("mapping_run_bp.cancel_mapping_run", task_id=task_id, run_id=run_id),
        "retry_url": url_for("mapping_run_bp.retry_mapping_run", task_id=task_id, run_id=run_id),
    }
