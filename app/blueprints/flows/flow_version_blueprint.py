from __future__ import annotations

from flask import Blueprint

flow_version_bp = Blueprint(
    "flow_version_bp",
    __name__,
    template_folder="templates",
    url_prefix="/tasks/<task_id>/flows/<flow_name>/versions",
)
