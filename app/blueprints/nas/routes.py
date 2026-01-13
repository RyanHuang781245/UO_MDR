from __future__ import annotations

import os

from flask import Blueprint, current_app, flash, jsonify, redirect, request, url_for

from app.services import nas_service

nas_bp = Blueprint("nas_bp", __name__, template_folder="templates")


@nas_bp.get("/api/nas/dirs", endpoint="api_nas_list_dirs")
def api_nas_list_dirs():
    root_index = request.args.get("root_index", type=int)
    rel_path_raw = (request.args.get("path") or "").strip()
    payload, status = nas_service.list_nas_dirs(root_index, rel_path_raw)
    return jsonify(payload), status


@nas_bp.post("/nas/add-root", endpoint="add_nas_root_route")
def add_nas_root_route():
    path = request.form.get("nas_root", "").strip()
    if not path:
        flash("請輸入 NAS 根目錄", "danger")
        return redirect(url_for("tasks"))
    try:
        added = nas_service.add_nas_root(path)
        if added:
            flash("已新增 NAS 根目錄", "success")
        else:
            flash("NAS 根目錄已存在", "info")
    except FileNotFoundError as exc:
        flash(str(exc), "danger")
    except ValueError as exc:
        flash(str(exc), "danger")
    except Exception:
        current_app.logger.exception("Failed to add NAS root")
        flash("新增 NAS 根目錄時發生錯誤", "danger")
    return redirect(url_for("tasks"))


@nas_bp.post("/nas/remove-root", endpoint="remove_nas_root_route")
def remove_nas_root_route():
    path = request.form.get("nas_root_remove", "").strip()
    if not path:
        flash("請選擇要移除的 NAS 根目錄", "danger")
        return redirect(url_for("tasks"))
    try:
        abs_path = os.path.abspath(path)
        removed = nas_service.remove_nas_root(abs_path)
        if removed:
            flash("已移除 NAS 根目錄", "success")
        else:
            flash("找不到指定的 NAS 根目錄", "warning")
    except Exception:
        current_app.logger.exception("Failed to remove NAS root")
        flash("移除 NAS 根目錄時發生錯誤", "danger")
    return redirect(url_for("tasks"))
