from __future__ import annotations

from .auth import auth_bp, register_auth_routes
from .flows import (
    flow_builder_bp,
    flow_crud_bp,
    flow_execution_bp,
    flow_file_bp,
    flow_results_bp,
    flow_version_api_bp,
    flow_version_bp,
    global_batch_bp,
    mapping_run_bp,
    register_flow_routes,
)
from .nas import nas_bp, register_nas_routes
from .tasks import register_task_routes, tasks_bp


def register_blueprints(app) -> None:
    register_auth_routes()
    register_nas_routes()
    register_task_routes()
    register_flow_routes()
    app.register_blueprint(auth_bp)
    app.register_blueprint(nas_bp)
    app.register_blueprint(tasks_bp)
    app.register_blueprint(flow_builder_bp)
    app.register_blueprint(flow_crud_bp)
    app.register_blueprint(flow_execution_bp)
    app.register_blueprint(flow_file_bp)
    app.register_blueprint(flow_results_bp)
    app.register_blueprint(flow_version_api_bp)
    app.register_blueprint(flow_version_bp)
    app.register_blueprint(global_batch_bp)
    app.register_blueprint(mapping_run_bp)
