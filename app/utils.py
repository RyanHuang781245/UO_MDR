from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


def parse_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


TAIWAN_TZ = timezone(timedelta(hours=8))


def format_tw_datetime(value: Optional[datetime], assume_tz: timezone = timezone.utc) -> str:
    if not value:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=assume_tz)
    return value.astimezone(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S")
