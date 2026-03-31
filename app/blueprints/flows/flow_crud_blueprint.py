from __future__ import annotations

from flask import Blueprint

flow_crud_bp = Blueprint(
    "flow_crud_bp",
    __name__,
    template_folder="templates",
    url_prefix="/tasks/<task_id>/flows",
)
