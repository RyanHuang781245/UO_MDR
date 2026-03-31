from __future__ import annotations

from flask import Blueprint

flow_version_api_bp = Blueprint(
    "flow_version_api_bp",
    __name__,
    template_folder="templates",
    url_prefix="/api/tasks/<task_id>/flows/<flow_name>/versions",
)
