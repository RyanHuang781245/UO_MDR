from __future__ import annotations

from app.extensions import db
from app.models.auth import ROLE_ADMIN, Role, User, ensure_schema, seed_roles, upsert_user_role
from app.services.authn_service import set_local_password


def _create_local_admin(app, password: str = "secret-pass") -> None:
    with app.app_context():
        ensure_schema()
        seed_roles()
        role = Role.query.filter_by(name=ROLE_ADMIN).first()
        user = User(work_id="LOCAL001", display_name="Local Admin", active=True)
        set_local_password(user, password)
        db.session.add(user)
        db.session.flush()
        upsert_user_role(user, role)
        db.session.commit()


def test_local_auth_login_succeeds_without_ldap(app, client) -> None:
    app.config["AUTH_ENABLED"] = True
    app.config["AUTH_MODE"] = "local"
    _create_local_admin(app)

    response = client.post(
        "/auth/login",
        data={"username": "LOCAL001", "password": "secret-pass"},
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")


def test_local_auth_login_rejects_bad_password(app, client) -> None:
    app.config["AUTH_ENABLED"] = True
    app.config["AUTH_MODE"] = "local"
    _create_local_admin(app)

    response = client.post(
        "/auth/login",
        data={"username": "LOCAL001", "password": "wrong-pass"},
    )

    assert response.status_code == 200
    assert "憑證無效".encode("utf-8") in response.data
