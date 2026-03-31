from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from flask import current_app
from ldap3 import BASE, SUBTREE, Connection, Server
from ldap3.utils.conv import escape_filter_chars

from app.extensions import ldap_manager, login_manager
from app.models.auth import (
    LDAPProfile,
    ROLE_ADMIN,
    Role,
    User,
    commit_session,
    db,
    get_user_by_id,
    upsert_user_role,
)


@dataclass(frozen=True)
class LDAPUserInfo:
    dn: str
    work_id: str
    data: dict
    memberships: list

    @property
    def username(self) -> str:
        return self.work_id


def register_ldap_handlers() -> None:
    @ldap_manager.save_user
    def save_ldap_user(dn, username, data, memberships):
        return LDAPUserInfo(dn=dn, work_id=username, data=data or {}, memberships=memberships or [])

    @login_manager.user_loader
    def load_user(user_id: str) -> Optional[User]:
        try:
            return get_user_by_id(int(user_id))
        except Exception:
            return None


def _normalize_ldap_value(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _get_ldap_search_config() -> dict:
    host = current_app.config.get("LDAP_HOST")
    base_dn = current_app.config.get("LDAP_BASE_DN")
    bind_dn = current_app.config.get("LDAP_BIND_USER_DN")
    bind_pw = current_app.config.get("LDAP_BIND_USER_PASSWORD")
    login_attr = current_app.config.get("LDAP_USER_LOGIN_ATTR", "sAMAccountName")
    obj_filter = current_app.config.get(
        "LDAP_USER_OBJECT_FILTER", "(&(objectClass=user)(!(objectClass=computer)))"
    )
    scope = current_app.config.get("LDAP_USER_SEARCH_SCOPE")

    if not host or not base_dn or not bind_dn or not bind_pw:
        raise ValueError("LDAP search configuration is missing")

    return {
        "host": host,
        "base_dn": base_dn,
        "bind_dn": bind_dn,
        "bind_pw": bind_pw,
        "login_attr": login_attr,
        "obj_filter": obj_filter,
        "scope": scope,
    }


def search_ad_users(keyword: str) -> list[dict]:
    keyword = (keyword or "").strip()
    if not keyword:
        return []

    cfg = _get_ldap_search_config()
    escaped = escape_filter_chars(keyword)
    pattern = f"*{escaped}*"
    login_attr = cfg["login_attr"]
    search_filter = (
        f"(&{cfg['obj_filter']}(|({login_attr}={pattern})"
        f"(displayName={pattern})(mail={pattern})))"
    )
    attributes = [login_attr, "displayName", "mail", "distinguishedName"]

    server = Server(cfg["host"])
    conn = Connection(server, user=cfg["bind_dn"], password=cfg["bind_pw"], auto_bind=True)
    try:
        conn.search(
            search_base=cfg["base_dn"],
            search_filter=search_filter,
            search_scope=cfg["scope"] or SUBTREE,
            attributes=attributes,
        )
        results = []
        for entry in conn.entries:
            data = entry.entry_attributes_as_dict
            work_id = _normalize_ldap_value(data.get(login_attr))
            if not work_id:
                continue
            results.append(
                {
                    "work_id": work_id,
                    "display_name": _normalize_ldap_value(data.get("displayName")),
                    "email": _normalize_ldap_value(data.get("mail")),
                    "dn": entry.entry_dn,
                }
            )
        return results
    finally:
        conn.unbind()


def build_ldap_profile(ldap_user: LDAPUserInfo) -> LDAPProfile:
    data = ldap_user.data or {}
    display_name = _normalize_ldap_value(
        data.get("displayName")
        or data.get("cn")
        or data.get("name")
        or data.get("givenName")
    )
    email = _normalize_ldap_value(data.get("mail"))
    return LDAPProfile(work_id=ldap_user.work_id, display_name=display_name, email=email)


def is_allowed_group_member(user_dn: str) -> bool:
    if not current_app.config.get("LDAP_GROUP_GATE_ENABLED", True):
        return True
    allowed_group_dn = current_app.config.get("ALLOWED_GROUP_DN")
    if not allowed_group_dn:
        raise ValueError("ALLOWED_GROUP_DN is not configured")
    host = current_app.config.get("LDAP_HOST")
    bind_dn = current_app.config.get("LDAP_BIND_USER_DN")
    bind_pw = current_app.config.get("LDAP_BIND_USER_PASSWORD")
    if not host or not bind_dn or not bind_pw:
        raise ValueError("LDAP bind configuration is missing")

    server = Server(host)
    conn = Connection(server, user=bind_dn, password=bind_pw, auto_bind=True)
    try:
        escaped_user_dn = escape_filter_chars(user_dn)
        search_filter = (
            "(&(objectClass=group)(member:1.2.840.113556.1.4.1941:="
            + escaped_user_dn
            + "))"
        )
        conn.search(
            search_base=allowed_group_dn,
            search_filter=search_filter,
            search_scope=BASE,
            attributes=["distinguishedName"],
        )
        return bool(conn.entries)
    finally:
        conn.unbind()


def bootstrap_admins() -> None:
    raw = os.environ.get("BOOTSTRAP_ADMIN", "")
    work_ids = [entry.strip() for entry in raw.split(",") if entry.strip()]
    if not work_ids:
        return

    admin_role = Role.query.filter_by(name=ROLE_ADMIN).first()
    if not admin_role:
        admin_role = Role(name=ROLE_ADMIN)
        db.session.add(admin_role)
        db.session.flush()

    for work_id in work_ids:
        user = User.query.filter_by(work_id=work_id).first()
        if not user:
            user = User(work_id=work_id, active=True)
            db.session.add(user)
            db.session.flush()
        upsert_user_role(user, admin_role)

    commit_session()
