import os
import sys
from pathlib import Path

import pytest

from app import create_app


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 測試時避免依賴外部 MSSQL（RBAC/auth 可用環境變數開關）。
os.environ.setdefault("AUTH_ENABLED", "0")


@pytest.fixture
def app():
    app = create_app("testing")
    ctx = app.app_context()
    ctx.push()
    try:
        yield app
    finally:
        ctx.pop()


@pytest.fixture
def client(app):
    return app.test_client()
