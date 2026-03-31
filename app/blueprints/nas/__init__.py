from .blueprint import nas_bp

_ROUTES_REGISTERED = False


def register_nas_routes() -> None:
    global _ROUTES_REGISTERED
    if _ROUTES_REGISTERED:
        return
    from . import routes  # noqa: F401

    _ROUTES_REGISTERED = True


__all__ = ["nas_bp", "register_nas_routes"]
