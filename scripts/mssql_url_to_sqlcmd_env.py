#!/usr/bin/env python3
from __future__ import annotations

import os
import shlex
from urllib.parse import unquote, urlsplit


def emit(name: str, value: str | None) -> None:
    if value:
        print(f"{name}={shlex.quote(value)}")


def main() -> int:
    raw_url = (os.environ.get("DATABASE_URL") or os.environ.get("RBAC_DATABASE_URL") or "").strip()
    if not raw_url:
        return 0

    parsed = urlsplit(raw_url)
    if not parsed.scheme.startswith("mssql"):
        return 0

    server = parsed.hostname or ""
    if parsed.port:
        server = f"{server},{parsed.port}"
    database = unquote((parsed.path or "").lstrip("/"))
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")

    if not os.environ.get("SQLCMD_SERVER"):
        emit("SQLCMD_SERVER", server)
    if not os.environ.get("SQLCMD_USER"):
        emit("SQLCMD_USER", username)
    if not os.environ.get("SQLCMD_PASSWORD"):
        emit("SQLCMD_PASSWORD", password)
    if not os.environ.get("MSSQL_DATABASE"):
        emit("MSSQL_DATABASE", database)
    if not os.environ.get("SQLCMD_TRUST_CERT") and "TrustServerCertificate=yes" in raw_url:
        emit("SQLCMD_TRUST_CERT", "1")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
