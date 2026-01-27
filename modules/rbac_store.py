from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    and_,
    create_engine,
    delete,
    func,
    insert,
    select,
    inspect,
    text,
)
from sqlalchemy.engine import Engine, URL, make_url
from sqlalchemy.orm import Session
from werkzeug.security import check_password_hash, generate_password_hash


ROLE_ADMIN = "admin"
ROLE_EDITOR = "editor"

PERM_USER_MANAGE = "user:manage"

DEFAULT_PERMISSIONS_BY_ROLE: dict[str, set[str]] = {
    ROLE_ADMIN: {PERM_USER_MANAGE},
    ROLE_EDITOR: set(),
}

ROLE_LABELS_ZH = {
    ROLE_ADMIN: "系統管理者",
    ROLE_EDITOR: "編輯者",
}


metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("work_id", String(100), nullable=False),
    Column("password_hash", String(255), nullable=False),
    Column("is_active", Boolean, nullable=False, server_default="1"),
    Column("created_at", DateTime, nullable=False, server_default=func.sysdatetime()),
    UniqueConstraint("work_id", name="uq_users_work_id"),
)

roles = Table(
    "roles",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(50), nullable=False),
    UniqueConstraint("name", name="uq_roles_name"),
)

permissions = Table(
    "permissions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(100), nullable=False),
    UniqueConstraint("name", name="uq_permissions_name"),
)

user_roles = Table(
    "user_roles",
    metadata,
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
)

role_permissions = Table(
    "role_permissions",
    metadata,
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
    Column(
        "permission_id",
        Integer,
        ForeignKey("permissions.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class RBACConfigError(RuntimeError):
    pass


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def build_mssql_engine_from_env() -> Engine:
    database_url = os.environ.get("DATABASE_URL") or os.environ.get("RBAC_DATABASE_URL")
    if database_url:
        trust_cert = _parse_bool(os.environ.get("MSSQL_TRUST_SERVER_CERT"), True)
        encrypt_env = os.environ.get("MSSQL_ENCRYPT")
        driver_override = os.environ.get("MSSQL_DRIVER")

        try:
            url = make_url(database_url)
            if url.get_backend_name() == "mssql" and (url.get_driver_name() or "") == "pyodbc":
                query = dict(url.query or {})
                normalized_keys = {str(k).lower() for k in query.keys()}

                if driver_override and "driver" not in normalized_keys:
                    query["driver"] = driver_override

                if encrypt_env is not None and "encrypt" not in normalized_keys:
                    query["Encrypt"] = "yes" if _parse_bool(encrypt_env, True) else "no"

                if trust_cert and "trustservercertificate" not in normalized_keys:
                    query["TrustServerCertificate"] = "yes"

                url = url.set(query=query)

            return create_engine(url, pool_pre_ping=True, future=True)
        except Exception:
            return create_engine(database_url, pool_pre_ping=True, future=True)

    host = os.environ.get("MSSQL_SERVER") or os.environ.get("MSSQL_HOST")
    database = os.environ.get("MSSQL_DB") or os.environ.get("MSSQL_DATABASE")
    if not host or not database:
        raise RBACConfigError(
            "尚未設定 MSSQL 連線資訊，請設定 DATABASE_URL 或 MSSQL_SERVER/MSSQL_DB。"
        )

    driver = os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    trusted = _parse_bool(os.environ.get("MSSQL_TRUSTED_CONNECTION"), False)
    trust_cert = _parse_bool(os.environ.get("MSSQL_TRUST_SERVER_CERT"), True)
    encrypt_env = os.environ.get("MSSQL_ENCRYPT")

    query: dict[str, str] = {"driver": driver}
    if trusted:
        query["trusted_connection"] = "yes"
    if encrypt_env is not None:
        query["Encrypt"] = "yes" if _parse_bool(encrypt_env, True) else "no"
    if trust_cert:
        query["TrustServerCertificate"] = "yes"

    if trusted:
        url = URL.create("mssql+pyodbc", host=host, database=database, query=query)
    else:
        db_username = os.environ.get("MSSQL_USER") or os.environ.get("MSSQL_USERNAME")
        password = os.environ.get("MSSQL_PASSWORD")
        if not db_username or not password:
            raise RBACConfigError(
                "MSSQL_USER/MSSQL_PASSWORD 未設定（或改用 MSSQL_TRUSTED_CONNECTION=1）。"
            )
        url = URL.create(
            "mssql+pyodbc",
            username=db_username,
            password=password,
            host=host,
            database=database,
            query=query,
        )

    return create_engine(url, pool_pre_ping=True, future=True)


def ensure_schema(engine: Engine) -> None:
    """
    Create missing tables and apply minimal, safe upgrades for pre-existing schemas.

    SQLAlchemy's `create_all()` does not ALTER existing tables. If you previously created
    tables manually (or with a different schema), we add required columns when missing.
    For full migrations in production, use Alembic.
    """
    metadata.create_all(engine)

    if engine.dialect.name != "mssql":
        return

    inspector = inspect(engine)
    if "users" not in set(inspector.get_table_names()):
        return

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                IF COL_LENGTH('users', 'work_id') IS NULL AND COL_LENGTH('users', 'username') IS NOT NULL
                BEGIN
                    EXEC sp_rename 'users.username', 'work_id', 'COLUMN';
                END
                """
            )
        )

        conn.execute(
            text(
                """
                IF COL_LENGTH('users', 'is_active') IS NULL
                BEGIN
                    ALTER TABLE users
                    ADD is_active BIT NOT NULL
                    CONSTRAINT DF_users_is_active DEFAULT(1);
                END
                """
            )
        )
        conn.execute(
            text(
                """
                IF COL_LENGTH('users', 'created_at') IS NULL
                BEGIN
                    ALTER TABLE users
                    ADD created_at DATETIME2 NOT NULL
                    CONSTRAINT DF_users_created_at DEFAULT(SYSDATETIME());
                END
                """
            )
        )


def _get_or_create_id(session: Session, table: Table, name: str) -> int:
    existing = session.execute(select(table.c.id).where(table.c.name == name)).scalar_one_or_none()
    if existing is not None:
        return int(existing)
    new_id = session.execute(insert(table).values(name=name)).inserted_primary_key[0]
    return int(new_id)


def seed_defaults(engine: Engine) -> None:
    with Session(engine) as session:
        for role_name, perms in DEFAULT_PERMISSIONS_BY_ROLE.items():
            role_id = _get_or_create_id(session, roles, role_name)
            for perm_name in sorted(perms):
                perm_id = _get_or_create_id(session, permissions, perm_name)
                existing = session.execute(
                    select(role_permissions.c.role_id).where(
                        and_(
                            role_permissions.c.role_id == role_id,
                            role_permissions.c.permission_id == perm_id,
                        )
                    )
                ).first()
                if not existing:
                    session.execute(
                        insert(role_permissions).values(role_id=role_id, permission_id=perm_id)
                    )
        session.commit()


@dataclass(frozen=True)
class UserRecord:
    id: int
    work_id: str
    is_active: bool
    created_at: Optional[datetime] = None


def create_user(engine: Engine, work_id: str, password: str, role: str) -> int:
    work_id = (work_id or "").strip()
    if not work_id:
        raise ValueError("work_id 不可為空")
    if role not in {ROLE_ADMIN, ROLE_EDITOR}:
        raise ValueError("role 不合法")

    password_hash = generate_password_hash(password)
    with Session(engine) as session:
        existing = session.execute(select(users.c.id).where(users.c.work_id == work_id)).scalar_one_or_none()
        if existing is not None:
            raise ValueError("使用者已存在")

        user_id = session.execute(
            insert(users).values(work_id=work_id, password_hash=password_hash, is_active=True)
        ).inserted_primary_key[0]
        role_id = _get_or_create_id(session, roles, role)
        session.execute(insert(user_roles).values(user_id=user_id, role_id=role_id))
        session.commit()
        return int(user_id)


def set_user_password(engine: Engine, user_id: int, new_password: str) -> None:
    new_hash = generate_password_hash(new_password)
    with Session(engine) as session:
        session.execute(
            users.update().where(users.c.id == user_id).values(password_hash=new_hash)
        )
        session.commit()


def set_user_active(engine: Engine, user_id: int, is_active: bool) -> None:
    with Session(engine) as session:
        session.execute(users.update().where(users.c.id == user_id).values(is_active=bool(is_active)))
        session.commit()


def set_user_role(engine: Engine, user_id: int, role: str) -> None:
    if role not in {ROLE_ADMIN, ROLE_EDITOR}:
        raise ValueError("role 不合法")
    with Session(engine) as session:
        session.execute(delete(user_roles).where(user_roles.c.user_id == user_id))
        role_id = _get_or_create_id(session, roles, role)
        session.execute(insert(user_roles).values(user_id=user_id, role_id=role_id))
        session.commit()


def authenticate(engine: Engine, work_id: str, password: str) -> Optional[UserRecord]:
    work_id = (work_id or "").strip()
    if not work_id or not password:
        return None
    with Session(engine) as session:
        row = session.execute(
            select(users.c.id, users.c.work_id, users.c.password_hash, users.c.is_active, users.c.created_at).where(
                users.c.work_id == work_id
            )
        ).first()
        if not row:
            return None
        user_id, uname, password_hash, is_active, created_at = row
        if not bool(is_active):
            return None
        if not check_password_hash(password_hash, password):
            return None
        return UserRecord(id=int(user_id), work_id=str(uname), is_active=True, created_at=created_at)


def get_user_by_id(engine: Engine, user_id: int) -> Optional[UserRecord]:
    with Session(engine) as session:
        row = session.execute(
            select(users.c.id, users.c.work_id, users.c.is_active, users.c.created_at).where(users.c.id == user_id)
        ).first()
        if not row:
            return None
        uid, uname, is_active, created_at = row
        return UserRecord(id=int(uid), work_id=str(uname), is_active=bool(is_active), created_at=created_at)


def list_users(engine: Engine) -> list[dict]:
    with Session(engine) as session:
        rows = session.execute(
            select(
                users.c.id,
                users.c.work_id,
                users.c.is_active,
                users.c.created_at,
            ).order_by(users.c.id.asc())
        ).all()
        users_data: list[dict] = []
        for uid, uname, is_active, created_at in rows:
            role_names = session.execute(
                select(roles.c.name)
                .select_from(user_roles.join(roles, roles.c.id == user_roles.c.role_id))
                .where(user_roles.c.user_id == uid)
            ).scalars().all()
            users_data.append(
                {
                    "id": int(uid),
                    "work_id": str(uname),
                    "is_active": bool(is_active),
                    "created_at": created_at,
                    "roles": list(role_names),
                }
            )
        return users_data


def user_has_permission(engine: Engine, user_id: int, permission_name: str) -> bool:
    with Session(engine) as session:
        row = session.execute(
            select(permissions.c.id)
            .select_from(
                user_roles.join(roles, roles.c.id == user_roles.c.role_id)
                .join(role_permissions, role_permissions.c.role_id == roles.c.id)
                .join(permissions, permissions.c.id == role_permissions.c.permission_id)
            )
            .where(and_(user_roles.c.user_id == user_id, permissions.c.name == permission_name))
            .limit(1)
        ).first()
        return bool(row)


def get_user_roles(engine: Engine, user_id: int) -> list[str]:
    with Session(engine) as session:
        return (
            session.execute(
                select(roles.c.name)
                .select_from(user_roles.join(roles, roles.c.id == user_roles.c.role_id))
                .where(user_roles.c.user_id == user_id)
            )
            .scalars()
            .all()
        )
