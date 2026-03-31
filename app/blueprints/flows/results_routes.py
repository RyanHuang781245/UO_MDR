from __future__ import annotations

import os
import shutil
import zipfile
from datetime import datetime

from flask import abort, current_app, flash, redirect, render_template, request, send_file, url_for

from app.services.task_service import load_task_context as _load_task_context

from .flow_results_blueprint import flow_results_bp
from .mapping_run_blueprint import mapping_run_bp
from .run_helpers import (
    _job_has_error,
    _list_flow_runs,
    _list_mapping_runs,
    _read_job_meta,
    _read_mapping_run_meta,
)


@flow_results_bp.post("/runs/<job_id>/delete", endpoint="delete_flow_run")
def delete_flow_run(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    if not os.path.isdir(job_dir):
        abort(404)
    try:
        shutil.rmtree(job_dir)
        flash("已刪除執行紀錄。", "success")
    except Exception:
        current_app.logger.exception("Failed to delete flow run")
        flash("刪除失敗，請稍後再試。", "danger")
    return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single"))


@flow_results_bp.post("/runs/delete", endpoint="delete_flow_runs_bulk")
def delete_flow_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    raw = request.form.get("job_ids", "")
    job_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not job_ids:
        flash("請先選取要刪除的執行紀錄。", "warning")
        return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single"))
    deleted = 0
    for job_id in job_ids:
        job_dir = os.path.join(jobs_dir, job_id)
        if not os.path.isdir(job_dir):
            continue
        try:
            shutil.rmtree(job_dir)
            deleted += 1
        except Exception:
            current_app.logger.exception("Failed to delete flow run")
    if deleted:
        flash(f"已刪除 {deleted} 筆執行紀錄。", "success")
    else:
        flash("沒有可刪除的執行紀錄。", "warning")
    return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single"))


@flow_results_bp.post("/runs/download", endpoint="download_flow_runs_bulk")
def download_flow_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    jobs_dir = os.path.join(tdir, "jobs")
    kind = request.form.get("kind", "docx")
    raw = request.form.get("job_ids", "")
    job_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not job_ids:
        flash("請先選取要下載的執行紀錄。", "warning")
        return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single"))
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
        return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single"))
    return send_file(zip_path, as_attachment=True, download_name=zip_name)


@mapping_run_bp.post("/<run_id>/delete", endpoint="delete_mapping_run")
def delete_mapping_run(task_id, run_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    run_dir = os.path.join(tdir, "mapping_job", run_id)
    if not os.path.isdir(run_dir):
        abort(404)
    try:
        shutil.rmtree(run_dir)
        flash("已刪除 Mapping 執行紀錄。", "success")
    except Exception:
        current_app.logger.exception("Failed to delete mapping run")
        flash("刪除失敗，請稍後再試。", "danger")
    return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single", tab="mapping"))


@mapping_run_bp.post("/delete", endpoint="delete_mapping_runs_bulk")
def delete_mapping_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    mapping_dir = os.path.join(tdir, "mapping_job")
    raw = request.form.get("run_ids", "")
    run_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not run_ids:
        flash("請先選取要刪除的 Mapping 執行紀錄。", "warning")
        return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single", tab="mapping"))
    deleted = 0
    for run_id in run_ids:
        run_dir = os.path.join(mapping_dir, run_id)
        if not os.path.isdir(run_dir):
            continue
        try:
            shutil.rmtree(run_dir)
            deleted += 1
        except Exception:
            current_app.logger.exception("Failed to delete mapping run")
    if deleted:
        flash(f"已刪除 {deleted} 筆 Mapping 執行紀錄。", "success")
    else:
        flash("沒有可刪除的 Mapping 執行紀錄。", "warning")
    return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single", tab="mapping"))


@mapping_run_bp.post("/download", endpoint="download_mapping_runs_bulk")
def download_mapping_runs_bulk(task_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    mapping_dir = os.path.join(tdir, "mapping_job")
    kind = request.form.get("kind", "zip")
    raw = request.form.get("run_ids", "")
    run_ids = [j.strip() for j in raw.split(",") if j.strip()]
    if not run_ids:
        flash("請先選取要下載的 Mapping 執行紀錄。", "warning")
        return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single", tab="mapping"))
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
        return redirect(url_for("flow_results_bp.flow_results", task_id=task_id, view="single", tab="mapping"))
    return send_file(zip_path, as_attachment=True, download_name=zip_name)


@flow_results_bp.get("/results", endpoint="flow_results")
def flow_results(task_id):
    task_context = _load_task_context(task_id)
    if not task_context:
        abort(404)
    task_context["id"] = task_id

    view = (request.args.get("view") or "single").lower()
    if view == "batch":
        return redirect(url_for("global_batch_bp.global_batch_page"))
    active_tab = (request.args.get("tab") or "flows").strip().lower()
    if active_tab not in {"flows", "mapping"}:
        active_tab = "flows"

    page = max(request.args.get("page", 1, type=int), 1)
    per_page = 10
    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()

    def _date_prefix(text: str) -> str:
        text = (text or "").strip()
        return text[:10] if len(text) >= 10 else ""

    def _match_date(value: str) -> bool:
        d = _date_prefix(value)
        if start_date and (not d or d < start_date):
            return False
        if end_date and (not d or d > end_date):
            return False
        return True

    flow_runs_all = _list_flow_runs(task_id)
    mapping_runs_all = _list_mapping_runs(task_id)
    runs_all = flow_runs_all if active_tab == "flows" else mapping_runs_all
    if q:
        q_lower = q.lower()
        if active_tab == "flows":
            runs_all = [
                r
                for r in runs_all
                if q_lower in (r.get("flow_name") or "").lower()
                or q_lower in (r.get("started_at") or "").lower()
                or q_lower in (r.get("job_id") or "").lower()
            ]
        else:
            runs_all = [
                r
                for r in runs_all
                if q_lower in (r.get("mapping_file") or "").lower()
                or q_lower in (r.get("started_at") or "").lower()
                or q_lower in (r.get("run_id") or "").lower()
            ]
    if status:
        runs_all = [r for r in runs_all if (r.get("status") or "").lower() == status]
    if start_date or end_date:
        runs_all = [r for r in runs_all if _match_date(r.get("started_at") or "")]

    total_count = len(runs_all)
    total_pages = max((total_count + per_page - 1) // per_page, 1)
    page = min(page, total_pages)
    start = (page - 1) * per_page
    runs = runs_all[start : start + per_page]
    pagination = {
        "page": page,
        "per_page": per_page,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }
    running = [r for r in flow_runs_all if r["status"] in ("running", "queued")]

    return render_template(
        "flows/results.html",
        task=task_context,
        view="single",
        runs=runs,
        active_tab=active_tab,
        batches=[],
        running=running,
        pagination=pagination,
        tab_counts={"flows": len(flow_runs_all), "mapping": len(mapping_runs_all)},
        filters={
            "q": q,
            "status": status,
            "start_date": start_date,
            "end_date": end_date,
        },
    )


@flow_results_bp.get("/runs/<job_id>/status", endpoint="flow_run_status")
def flow_run_status(task_id, job_id):
    tdir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    if not os.path.isdir(job_dir):
        return {"ok": False, "error": "Run not found"}, 404
    meta = _read_job_meta(job_dir)
    status = meta.get("status") or "unknown"
    if _job_has_error(job_dir):
        status = "failed"
    flow_name = meta.get("flow_name") or ""
    return {"ok": True, "status": status, "flow_name": flow_name}


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
