from __future__ import annotations

import os
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


_INVALID_FILENAME_CHARS = r'\\/:*?"<>|'
_WINDOWS_RESERVED_FILE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


def normalize_docx_output_filename(value: Optional[str], default: str = "") -> tuple[str, Optional[str]]:
    text = (value or "").strip()
    if not text:
        return default, None
    if text in {".", ".."}:
        return "", "輸出檔名不合法"
    if any(ord(ch) < 32 for ch in text):
        return "", "輸出檔名含有不可見控制字元"
    if any(ch in _INVALID_FILENAME_CHARS for ch in text):
        return "", '輸出檔名不可包含 \\ / : * ? " < > |'
    if text[-1] in {" ", "."}:
        return "", "輸出檔名結尾不可為空白或句點"

    stem, ext = os.path.splitext(text)
    if ext:
        if ext.lower() != ".docx":
            return "", "輸出檔名副檔名僅支援 .docx"
        if not stem:
            return "", "輸出檔名不合法"
    else:
        stem = text
        text = f"{text}.docx"

    if stem.upper() in _WINDOWS_RESERVED_FILE_NAMES:
        return "", "輸出檔名為系統保留字，請更換名稱"

    return text, None


def normalize_docx_output_path(value: Optional[str], default: str = "") -> tuple[str, Optional[str]]:
    text = (value or "").strip()
    if not text:
        return default, None
    if text in {".", ".."}:
        return "", "輸出路徑不合法"
    if any(ord(ch) < 32 for ch in text):
        return "", "輸出路徑含有不可見控制字元"
    if os.path.isabs(text) or text.startswith(("/", "\\")):
        return "", "輸出路徑必須為相對路徑"

    normalized = os.path.normpath(text.replace("\\", "/")).replace("\\", "/")
    if normalized in {"", ".", ".."} or normalized.startswith("../"):
        return "", "輸出路徑不合法"

    parts = normalized.split("/")
    for segment in parts[:-1]:
        if not segment:
            return "", "輸出路徑不合法"
        if any(ch in _INVALID_FILENAME_CHARS for ch in segment):
            return "", '輸出路徑不可包含 \\ : * ? " < > |'
        if segment[-1] in {" ", "."}:
            return "", "輸出路徑結尾不可為空白或句點"

    filename = parts[-1]
    normalized_filename, error = normalize_docx_output_filename(filename, default="")
    if error:
        return "", error.replace("輸出檔名", "輸出路徑")
    parts[-1] = normalized_filename
    return "/".join(parts), None
