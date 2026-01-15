from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, inspect, text

db = SQLAlchemy()

ROLE_ADMIN = "admin"
ROLE_EDITOR = "editor"

PERM_USER_MANAGE = "user:manage"

ROLE_LABELS_ZH = {
    ROLE_ADMIN: "Admin",
    ROLE_EDITOR: "Editor",
}


class User(db.Model, UserMixin):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    work_id = db.Column(db.String(100), nullable=False, unique=True)
    display_name = db.Column(db.String(200))
    email = db.Column(db.String(200))
    active = db.Column("is_active", db.Boolean, nullable=False, server_default="1")
    created_at = db.Column(db.DateTime, nullable=False, server_default=func.now())
    last_login_at = db.Column(db.DateTime)

    user_role = db.relationship(
        "UserRole",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __str__(self) -> str:
        if self.display_name:
            return f"{self.work_id} ({self.display_name})"
        return self.work_id

    @property
    def is_active(self) -> bool:
        return bool(self.active)

    @is_active.setter
    def is_active(self, value: bool) -> None:
        self.active = bool(value)

    @property
    def role_name(self) -> Optional[str]:
        if self.user_role and self.user_role.role:
            return self.user_role.role.name
        return None


class Role(db.Model):
    __tablename__ = "roles"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True)

    def __str__(self) -> str:
        return self.name


class UserRole(db.Model):
    __tablename__ = "user_roles"

    user_id = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    role_id = db.Column(db.Integer, db.ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True)

    user = db.relationship("User", back_populates="user_role")
    role = db.relationship("Role")

    __table_args__ = (db.UniqueConstraint("user_id", name="uq_user_roles_user_id"),)


@dataclass(frozen=True)
class LDAPProfile:
    work_id: str
    display_name: Optional[str] = None
    email: Optional[str] = None


def commit_session() -> None:
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise


def ensure_schema() -> None:
    db.create_all()

    engine = db.engine
    inspector = inspect(engine)
    if "users" not in set(inspector.get_table_names()):
        return

    existing_columns = {col["name"].lower() for col in inspector.get_columns("users")}
    with engine.begin() as conn:
        if engine.dialect.name == "mssql":
            if "work_id" not in existing_columns and "username" in existing_columns:
                conn.execute(text("EXEC sp_rename 'users.username', 'work_id', 'COLUMN';"))
                existing_columns.discard("username")
                existing_columns.add("work_id")
            if "display_name" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD display_name NVARCHAR(200) NULL;"))
            if "email" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD email NVARCHAR(200) NULL;"))
            if "last_login_at" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD last_login_at DATETIME2 NULL;"))
            if "created_at" not in existing_columns:
                conn.execute(
                    text(
                        """
                        ALTER TABLE users
                        ADD created_at DATETIME2 NOT NULL
                        CONSTRAINT DF_users_created_at DEFAULT(SYSDATETIME());
                        """
                    )
                )
            if "is_active" not in existing_columns:
                conn.execute(
                    text(
                        """
                        ALTER TABLE users
                        ADD is_active BIT NOT NULL
                        CONSTRAINT DF_users_is_active DEFAULT(1);
                        """
                    )
                )

            conn.execute(
                text(
                    """
                    IF NOT EXISTS (
                        SELECT 1
                        FROM sys.indexes
                        WHERE name = 'uq_user_roles_user_id'
                          AND object_id = OBJECT_ID('user_roles')
                    )
                    BEGIN
                        CREATE UNIQUE INDEX uq_user_roles_user_id ON user_roles(user_id);
                    END
                    """
                )
            )
        elif engine.dialect.name == "sqlite":
            if "work_id" not in existing_columns and "username" in existing_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN work_id VARCHAR(100);"))
                conn.execute(text("UPDATE users SET work_id = username WHERE work_id IS NULL;"))
                conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_work_id ON users(work_id);"))
            if "display_name" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN display_name VARCHAR(200);"))
            if "email" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR(200);"))
            if "last_login_at" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN last_login_at DATETIME;"))
            if "created_at" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN created_at DATETIME;"))
            if "is_active" not in existing_columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN is_active BOOLEAN;"))


def seed_roles() -> None:
    existing = {r.name for r in Role.query.all()}
    for role_name in (ROLE_ADMIN, ROLE_EDITOR):
        if role_name not in existing:
            db.session.add(Role(name=role_name))
    commit_session()


def get_role(role_name: str) -> Optional[Role]:
    return Role.query.filter_by(name=role_name).first()


def get_user_by_work_id(work_id: str) -> Optional[User]:
    return User.query.filter_by(work_id=work_id).first()


def get_user_by_id(user_id: int) -> Optional[User]:
    return db.session.get(User, user_id)


def get_user_role_names(user_id: int) -> list[str]:
    rows = (
        db.session.query(Role.name)
        .join(UserRole, Role.id == UserRole.role_id)
        .filter(UserRole.user_id == user_id)
        .all()
    )
    return [name for (name,) in rows]


def user_has_role(user_id: int, role_name: str) -> bool:
    return (
        db.session.query(Role.id)
        .join(UserRole, Role.id == UserRole.role_id)
        .filter(UserRole.user_id == user_id, Role.name == role_name)
        .first()
        is not None
    )


def count_admins() -> int:
    admin = get_role(ROLE_ADMIN)
    if not admin:
        return 0
    return UserRole.query.filter_by(role_id=admin.id).count()


def upsert_user_role(user: User, role: Role) -> None:
    existing = UserRole.query.filter_by(user_id=user.id).first()
    if existing:
        existing.role_id = role.id
    else:
        db.session.add(UserRole(user_id=user.id, role_id=role.id))


def sync_user_from_ldap(profile: LDAPProfile) -> User:
    user = get_user_by_work_id(profile.work_id)
    if not user:
        user = User(
            work_id=profile.work_id,
            display_name=profile.display_name,
            email=profile.email,
            active=True,
        )
        db.session.add(user)
        db.session.flush()
    else:
        if profile.display_name and user.display_name != profile.display_name:
            user.display_name = profile.display_name
        if profile.email and user.email != profile.email:
            user.email = profile.email
    return user
