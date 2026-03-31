from __future__ import annotations

from flask import abort, render_template, request

from app.services.flow_version_service import flow_version_count as _flow_version_count

from .blueprint import flow_builder_bp
from .builder_helpers import build_flow_builder_context
from .flow_validation_helpers import _validate_flow_name


@flow_builder_bp.get("", endpoint="flow_builder")
def flow_builder(task_id):
    try:
        context = build_flow_builder_context(task_id, request.args)
    except FileNotFoundError:
        abort(404)
    return render_template("flows/flow.html", **context)
