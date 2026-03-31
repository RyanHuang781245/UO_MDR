from .blueprint import flow_builder_bp
from .flow_crud_blueprint import flow_crud_bp
from .flow_execution_blueprint import flow_execution_bp
from .flow_file_blueprint import flow_file_bp
from .flow_results_blueprint import flow_results_bp
from .flow_version_api_blueprint import flow_version_api_bp
from .flow_version_blueprint import flow_version_bp
from .global_batch_blueprint import global_batch_bp
from .mapping_run_blueprint import mapping_run_bp

_ROUTES_REGISTERED = False


def register_flow_routes() -> None:
    global _ROUTES_REGISTERED
    if _ROUTES_REGISTERED:
        return
    from . import execution_routes  # noqa: F401
    from . import flow_crud_routes  # noqa: F401
    from . import flow_file_routes  # noqa: F401
    from . import global_batch_routes  # noqa: F401
    from . import results_routes  # noqa: F401
    from . import routes  # noqa: F401
    from . import version_routes  # noqa: F401

    _ROUTES_REGISTERED = True


__all__ = [
    "flow_builder_bp",
    "flow_crud_bp",
    "flow_execution_bp",
    "flow_file_bp",
    "flow_results_bp",
    "flow_version_api_bp",
    "flow_version_bp",
    "global_batch_bp",
    "mapping_run_bp",
    "register_flow_routes",
]
