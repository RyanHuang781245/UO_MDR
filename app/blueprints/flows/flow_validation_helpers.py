from __future__ import annotations


_INVALID_FLOW_NAME_CHARS = r'\\/:*?"<>|'
_WINDOWS_RESERVED_FLOW_NAMES = {
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


def _validate_flow_name(name: str) -> str | None:
    text = (name or "").strip()
    if not text:
        return "缺少流程名稱"
    if len(text) > 50:
        return "流程名稱最多 50 字"
    if text in {".", ".."}:
        return "流程名稱不合法"
    if any(ord(ch) < 32 for ch in text):
        return "流程名稱含有不可見控制字元"
    if any(ch in _INVALID_FLOW_NAME_CHARS for ch in text):
        return '流程名稱不可包含 \\ / : * ? " < > |'
    if text[-1] in {" ", "."}:
        return "流程名稱結尾不可為空白或句點"
    if text.upper() in _WINDOWS_RESERVED_FLOW_NAMES:
        return "流程名稱為系統保留字，請更換名稱"
    return None
