from __future__ import annotations

from app import create_app
from app.services import authn_service


def test_db_error_during_user_load_redirects_to_login_with_message(monkeypatch):
    app = create_app("testing")
    app.config["AUTH_ENABLED"] = True
    logged_errors: list[tuple[str, tuple[object, ...]]] = []

    @app.get("/protected-test")
    def protected_test():
        return "ok"

    def fail_get_user_by_id(user_id: int):
        raise RuntimeError("database offline")

    def fake_logger_exception(message: str, *args, **kwargs):
        logged_errors.append((message, args))

    monkeypatch.setattr(authn_service, "get_user_by_id", fail_get_user_by_id)
    monkeypatch.setattr(app.logger, "exception", fake_logger_exception)

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = "123"
        session["_fresh"] = True

    response = client.get("/protected-test")

    assert response.status_code == 302
    assert "auth_error=database_unavailable" in response.headers["Location"]
    assert logged_errors == [
        ("Failed to load authenticated user from database. user_id=%s", ("123",))
    ]

    login_response = client.get(response.headers["Location"])

    assert login_response.status_code == 200
    assert "系統暫時無法連線資料庫，請聯絡管理員。" in login_response.get_data(as_text=True)
