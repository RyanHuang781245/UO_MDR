from __future__ import annotations

import os
import shutil

from flask import abort, current_app, redirect, request, send_file, url_for
from werkzeug.utils import secure_filename

from app.services.flow_version_service import flow_versions_dir as _flow_versions_dir

from .flow_crud_blueprint import flow_crud_bp
from .flow_route_helpers import _touch_task_last_edit
from .flow_validation_helpers import _validate_flow_name


@flow_crud_bp.post("/delete/<flow_name>", endpoint="delete_flow")
def delete_flow(task_id, flow_name):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(task_dir, "flows")
    path = os.path.join(flow_dir, f"{flow_name}.json")
    if os.path.exists(path):
        os.remove(path)
        versions_dir = _flow_versions_dir(flow_dir, flow_name)
        if os.path.isdir(versions_dir):
            shutil.rmtree(versions_dir, ignore_errors=True)
        _touch_task_last_edit(task_id)
    return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, fpage=request.form.get("fpage")))


@flow_crud_bp.post("/rename/<flow_name>", endpoint="rename_flow")
def rename_flow(task_id, flow_name):
    new_name = request.form.get("name", "").strip()
    name_error = _validate_flow_name(new_name)
    if name_error:
        return name_error, 400
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(task_dir, "flows")
    old_path = os.path.join(flow_dir, f"{flow_name}.json")
    new_path = os.path.join(flow_dir, f"{new_name}.json")
    if not os.path.exists(old_path):
        abort(404)
    if os.path.exists(new_path):
        return "流程名稱已存在", 400
    os.rename(old_path, new_path)
    old_versions_dir = _flow_versions_dir(flow_dir, flow_name)
    new_versions_dir = _flow_versions_dir(flow_dir, new_name)
    if os.path.isdir(old_versions_dir):
        os.makedirs(os.path.dirname(new_versions_dir), exist_ok=True)
        if os.path.isdir(new_versions_dir):
            shutil.rmtree(new_versions_dir, ignore_errors=True)
        os.rename(old_versions_dir, new_versions_dir)
    _touch_task_last_edit(task_id)
    return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, fpage=request.form.get("fpage")))


@flow_crud_bp.get("/export/<flow_name>", endpoint="export_flow")
def export_flow(task_id, flow_name):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    path = os.path.join(task_dir, "flows", f"{flow_name}.json")
    if not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=f"{flow_name}.json")


@flow_crud_bp.post("/import", endpoint="import_flow")
def import_flow(task_id):
    task_dir = os.path.join(current_app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(task_dir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    uploaded = request.files.get("flow_file")
    if not uploaded or not uploaded.filename.endswith(".json"):
        return "請上傳 JSON 檔", 400
    name = os.path.splitext(secure_filename(uploaded.filename))[0]
    path = os.path.join(flow_dir, f"{name}.json")
    uploaded.save(path)
    _touch_task_last_edit(task_id)
    return redirect(url_for("flow_builder_bp.flow_builder", task_id=task_id, fpage=request.form.get("fpage")))
