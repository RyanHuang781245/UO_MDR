from __future__ import annotations

import os
from pathlib import Path

from ldap3 import BASE, LEVEL, SUBTREE

from app.utils import parse_bool

BASE_DIR = Path(__file__).resolve().parents[1]


def _resolve_ldap_scope():
    raw = os.environ.get("LDAP_USER_SEARCH_SCOPE", "SUBTREE").upper()
    scope_map = {"BASE": BASE, "LEVEL": LEVEL, "SUBTREE": SUBTREE}
    return scope_map.get(raw, SUBTREE)


class BaseConfig:
    BASE_DIR = BASE_DIR
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret")
    OUTPUT_FOLDER = str(BASE_DIR / "output")
    TASK_FOLDER = str(BASE_DIR / "task_store")
    ALLOWED_SOURCE_ROOTS = []

    AUTH_ENABLED = parse_bool(os.environ.get("AUTH_ENABLED"), True)
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = parse_bool(os.environ.get("SESSION_COOKIE_SECURE"), False)

    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL") or os.environ.get("RBAC_DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}

    LDAP_HOST = os.environ.get("LDAP_HOST")
    LDAP_BASE_DN = os.environ.get("LDAP_BASE_DN")
    LDAP_BIND_USER_DN = os.environ.get("LDAP_BIND_DN")
    LDAP_BIND_USER_PASSWORD = os.environ.get("LDAP_BIND_PASSWORD")
    LDAP_USER_LOGIN_ATTR = os.environ.get("LDAP_USER_LOGIN_ATTR", "sAMAccountName")
    LDAP_USER_OBJECT_FILTER = os.environ.get(
        "LDAP_USER_OBJECT_FILTER", "(&(objectClass=user)(!(objectClass=computer)))"
    )
    LDAP_GROUP_GATE_ENABLED = parse_bool(os.environ.get("LDAP_GROUP_GATE_ENABLED"), True)
    LDAP_USER_SEARCH_SCOPE = _resolve_ldap_scope()
    ALLOWED_GROUP_DN = os.environ.get("ALLOWED_GROUP_DN")


class TestingConfig(BaseConfig):
    TESTING = True
    AUTH_ENABLED = False
    SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"


class DevelopmentConfig(BaseConfig):
    DEBUG = True


class ProductionConfig(BaseConfig):
    DEBUG = False


CONFIG_MAP = {
    "testing": TestingConfig,
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "default": BaseConfig,
}
