from __future__ import annotations

from flask import Blueprint

flow_file_bp = Blueprint(
    "flow_file_bp",
    __name__,
    template_folder="templates",
    url_prefix="/api/tasks/<task_id>/flow-files",
)
