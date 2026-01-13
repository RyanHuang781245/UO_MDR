from __future__ import annotations

from .auth import auth_bp
from .nas import nas_bp
from .tasks import tasks_bp
from .flows import flows_bp


def register_blueprints(app) -> None:
    app.register_blueprint(auth_bp)
    app.register_blueprint(nas_bp)
    app.register_blueprint(tasks_bp)
    app.register_blueprint(flows_bp)
    _register_endpoint_aliases(app)


def _register_endpoint_aliases(app) -> None:
    blueprint_names = {auth_bp.name, nas_bp.name, tasks_bp.name, flows_bp.name}
    for rule in list(app.url_map.iter_rules()):
        endpoint = rule.endpoint
        if "." not in endpoint:
            continue
        bp_name, endpoint_name = endpoint.split(".", 1)
        if bp_name not in blueprint_names:
            continue
        if endpoint_name in app.view_functions:
            continue
        view_func = app.view_functions.get(endpoint)
        if view_func is None:
            continue
        methods = rule.methods
        if methods:
            app.add_url_rule(rule.rule, endpoint=endpoint_name, view_func=view_func, methods=methods)
        else:
            app.add_url_rule(rule.rule, endpoint=endpoint_name, view_func=view_func)
