from __future__ import annotations

import html
import smtplib
import urllib.error
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy.exc import SQLAlchemyError

try:  # pragma: no cover - pyodbc may be unavailable in some environments.
    import pyodbc
except Exception:  # pragma: no cover
    pyodbc = None

try:  # pragma: no cover - requests is an optional network client in tests.
    import requests
except Exception:  # pragma: no cover
    requests = None

try:  # pragma: no cover - ldap3 is optional outside the deployed app.
    from ldap3.core.exceptions import LDAPException
except Exception:  # pragma: no cover
    LDAPException = None


@dataclass(frozen=True)
class FrontendError:
    code: str
    message: str


DEFAULT_FRONTEND_ERROR = FrontendError("unknown", "系統發生未預期錯誤，請聯絡管理員。")
DATABASE_FRONTEND_ERROR = FrontendError("database", "系統暫時無法連線資料庫，請聯絡管理員。")
LDAP_FRONTEND_ERROR = FrontendError("ldap", "無法連線驗證服務，請稍後再試或聯絡管理員。")
NAS_FRONTEND_ERROR = FrontendError("nas", "無法存取 NAS 路徑，請確認網路或權限後再試。")
SMTP_FRONTEND_ERROR = FrontendError("smtp", "郵件通知發送失敗，請聯絡管理員。")
DOWNLOAD_FRONTEND_ERROR = FrontendError("external_download", "無法連線外部下載來源，請稍後再試。")


def _walk_exception_chain(exc: Exception) -> Iterable[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _contains_any(exc: BaseException, needles: tuple[str, ...]) -> bool:
    text = f"{exc.__class__.__module__}.{exc.__class__.__name__} {exc}".lower()
    return any(needle in text for needle in needles)


def classify_frontend_error(exc: Exception) -> FrontendError:
    chain = list(_walk_exception_chain(exc))

    if any(isinstance(item, SQLAlchemyError) for item in chain):
        return DATABASE_FRONTEND_ERROR
    if pyodbc is not None and any(isinstance(item, pyodbc.Error) for item in chain):
        return DATABASE_FRONTEND_ERROR
    if any(_contains_any(item, ("sqldriverconnect", "login timeout expired", "sql server")) for item in chain):
        return DATABASE_FRONTEND_ERROR

    if LDAPException is not None and any(isinstance(item, LDAPException) for item in chain):
        return LDAP_FRONTEND_ERROR
    if any(_contains_any(item, ("ldap", "active directory")) for item in chain):
        return LDAP_FRONTEND_ERROR

    if any(isinstance(item, smtplib.SMTPException) for item in chain):
        return SMTP_FRONTEND_ERROR

    if requests is not None and any(isinstance(item, requests.RequestException) for item in chain):
        return DOWNLOAD_FRONTEND_ERROR
    if any(isinstance(item, urllib.error.URLError) for item in chain):
        return DOWNLOAD_FRONTEND_ERROR

    if any(isinstance(item, PermissionError) for item in chain):
        return NAS_FRONTEND_ERROR
    if any(_contains_any(item, ("nas", "network path", "permission denied")) for item in chain):
        return NAS_FRONTEND_ERROR

    return DEFAULT_FRONTEND_ERROR


def frontend_error_message(exc: Exception) -> str:
    return classify_frontend_error(exc).message


def simple_error_html(message: str) -> str:
    escaped_message = html.escape(message or DEFAULT_FRONTEND_ERROR.message)
    return (
        "<!doctype html><html lang=\"zh-Hant\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        "<title>系統錯誤</title>"
        "<style>body{font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;"
        "margin:0;background:#f6f7f9;color:#1f2937}.wrap{max-width:640px;margin:12vh auto;"
        "padding:0 24px}.panel{background:#fff;border:1px solid #d9dee7;border-radius:8px;"
        "padding:24px;box-shadow:0 8px 24px rgba(15,23,42,.08)}h1{font-size:22px;margin:0 0 12px}"
        "p{font-size:16px;line-height:1.6;margin:0}</style></head><body><main class=\"wrap\">"
        f"<section class=\"panel\"><h1>系統錯誤</h1><p>{escaped_message}</p></section>"
        "</main></body></html>"
    )
