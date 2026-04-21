from .blueprint import standard_updates_bp

_ROUTES_REGISTERED = False


def register_standard_update_routes() -> None:
    global _ROUTES_REGISTERED
    if _ROUTES_REGISTERED:
        return
    from . import routes  # noqa: F401

    _ROUTES_REGISTERED = True


__all__ = ["standard_updates_bp", "register_standard_update_routes"]
