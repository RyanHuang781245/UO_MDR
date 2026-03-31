from .blueprint import auth_bp

_ROUTES_REGISTERED = False


def register_auth_routes() -> None:
    global _ROUTES_REGISTERED
    if _ROUTES_REGISTERED:
        return
    from . import routes  # noqa: F401

    _ROUTES_REGISTERED = True


__all__ = ["auth_bp", "register_auth_routes"]
