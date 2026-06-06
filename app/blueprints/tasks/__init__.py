from .blueprint import tasks_bp

_ROUTES_REGISTERED = False


def register_task_routes() -> None:
    global _ROUTES_REGISTERED
    if _ROUTES_REGISTERED:
        return
    from app.blueprints.standard_mapping import routes as standard_mapping_routes  # noqa: F401
    from app.blueprints.task_compare import routes as task_compare_routes  # noqa: F401
    from app.blueprints.task_mapping import routes as task_mapping_routes  # noqa: F401
    from . import nas_routes  # noqa: F401
    from . import task_routes  # noqa: F401

    _ROUTES_REGISTERED = True


__all__ = ["tasks_bp", "register_task_routes"]
