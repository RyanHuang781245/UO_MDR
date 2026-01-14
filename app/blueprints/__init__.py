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
