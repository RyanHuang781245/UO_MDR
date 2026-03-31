from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

from app.models.auth import PERM_USER_MANAGE, ROLE_ADMIN, User, user_has_role


def sanitize_next_url(raw_next: Optional[str]) -> Optional[str]:
    if not raw_next:
        return None
    candidate = raw_next.strip()
    if candidate.endswith("?"):
        candidate = candidate[:-1]
    if not candidate.startswith("/") or candidate.startswith("//"):
        return None
    parsed = urlparse(candidate)
    if parsed.scheme or parsed.netloc:
        return None
    return candidate


def user_has_permission(user_id: int, permission_name: str) -> bool:
    if permission_name == PERM_USER_MANAGE:
        return user_has_role(user_id, ROLE_ADMIN)
    return False


def user_is_admin(user: User) -> bool:
    return bool(user and user.is_authenticated and user_has_role(user.id, ROLE_ADMIN))
