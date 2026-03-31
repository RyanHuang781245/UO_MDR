from __future__ import annotations

from flask import Blueprint

mapping_run_bp = Blueprint(
    "mapping_run_bp",
    __name__,
    template_folder="templates",
    url_prefix="/tasks/<task_id>/mapping/runs",
)
