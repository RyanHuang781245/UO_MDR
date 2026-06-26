from __future__ import annotations

import smtplib
import urllib.error

import pytest
from sqlalchemy.exc import OperationalError

from app import create_app
from app.services.frontend_error_service import (
    DATABASE_FRONTEND_ERROR,
    DOWNLOAD_FRONTEND_ERROR,
    LDAP_FRONTEND_ERROR,
    NAS_FRONTEND_ERROR,
    SMTP_FRONTEND_ERROR,
    classify_frontend_error,
)


def test_classify_database_error():
    exc = OperationalError("select 1", {}, RuntimeError("Login timeout expired"))

    assert classify_frontend_error(exc) == DATABASE_FRONTEND_ERROR


def test_classify_external_download_error():
    exc = urllib.error.URLError("timed out")

    assert classify_frontend_error(exc) == DOWNLOAD_FRONTEND_ERROR


def test_classify_smtp_error():
    exc = smtplib.SMTPConnectError(421, "unavailable")

    assert classify_frontend_error(exc) == SMTP_FRONTEND_ERROR


def test_classify_nas_error():
    exc = PermissionError("Permission denied")

    assert classify_frontend_error(exc) == NAS_FRONTEND_ERROR


def test_classify_ldap_text_error():
    exc = RuntimeError("LDAP server unavailable")

    assert classify_frontend_error(exc) == LDAP_FRONTEND_ERROR


@pytest.mark.parametrize(
    ("route", "exc", "message"),
    [
        ("/db-error", OperationalError("select 1", {}, RuntimeError("Login timeout expired")), DATABASE_FRONTEND_ERROR.message),
        ("/ldap-error", RuntimeError("LDAP server unavailable"), LDAP_FRONTEND_ERROR.message),
        ("/smtp-error", smtplib.SMTPConnectError(421, "unavailable"), SMTP_FRONTEND_ERROR.message),
        ("/download-error", urllib.error.URLError("timed out"), DOWNLOAD_FRONTEND_ERROR.message),
        ("/nas-error", PermissionError("Permission denied"), NAS_FRONTEND_ERROR.message),
    ],
)
def test_global_error_handler_returns_frontend_message(route, exc, message):
    app = create_app("testing")

    @app.get(route)
    def error_route():
        raise exc

    response = app.test_client().get(route)

    assert response.status_code == 500
    assert message in response.get_data(as_text=True)


def test_global_error_handler_returns_json_message():
    app = create_app("testing")

    @app.get("/db-json-error")
    def db_json_error():
        raise OperationalError("select 1", {}, RuntimeError("Login timeout expired"))

    response = app.test_client().get("/db-json-error", headers={"Accept": "application/json"})

    assert response.status_code == 500
    assert response.get_json() == {"ok": False, "error": DATABASE_FRONTEND_ERROR.message}
