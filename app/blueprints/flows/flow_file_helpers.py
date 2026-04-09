from __future__ import annotations

import os


def _normalize_task_file_rel_path(raw_path: str) -> str:
    cleaned = (raw_path or "").strip().replace("\\", "/")
    if cleaned in {"", ".", "/"}:
        return ""
    if cleaned.startswith("/") or os.path.isabs(cleaned):
        raise ValueError("無效的檔案路徑")
    normalized = os.path.normpath(cleaned).replace("\\", "/")
    if normalized in {"", "."}:
        return ""
    if normalized == ".." or normalized.startswith("../"):
        raise ValueError("無效的檔案路徑")
    return normalized


def _resolve_task_file_path(files_dir: str, rel_path: str, expect_dir: bool | None = None) -> str:
    rel = _normalize_task_file_rel_path(rel_path)
    base_abs = os.path.abspath(files_dir)
    candidate = os.path.abspath(os.path.join(base_abs, rel))
    try:
        if os.path.commonpath([base_abs, candidate]) != base_abs:
            raise ValueError("無效的檔案路徑")
    except ValueError as exc:
        raise ValueError("無效的檔案路徑") from exc

    if expect_dir is True and not os.path.isdir(candidate):
        raise FileNotFoundError("未找到路徑")
    if expect_dir is False and not os.path.isfile(candidate):
        raise FileNotFoundError("未找到檔案")
    return candidate


def _validate_new_folder_name(name: str) -> str:
    text = (name or "").strip()
    if not text:
        raise ValueError("缺少資料夾名稱")
    if text in {".", ".."}:
        raise ValueError("資料夾名稱不合法")
    if any(ord(ch) < 32 for ch in text):
        raise ValueError("資料夾名稱含有不可見控制字元")
    if any(ch in r'\/:*?"<>|' for ch in text):
        raise ValueError('資料夾名稱不可包含 \\ / : * ? " < > |')
    if text[-1] in {" ", "."}:
        raise ValueError("資料夾名稱結尾不可為空白或句點")
    return text


def _normalize_step_file_value(raw_value: str, accept: str) -> str:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return ""
    rel = _normalize_task_file_rel_path(cleaned)
    if accept.endswith(":dir") and rel == "":
        return "."
    if accept.endswith(":path") and rel == "":
        return "."
    return rel
