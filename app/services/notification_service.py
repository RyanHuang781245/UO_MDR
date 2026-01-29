from __future__ import annotations

import os
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from typing import Iterable

from flask import current_app

from modules.auth_models import User
from modules.settings_models import SystemSetting


def _get_system_settings() -> SystemSetting | None:
    try:
        return SystemSetting.query.order_by(SystemSetting.id).first()
    except Exception:
        current_app.logger.exception("Failed to load system settings")
        return None


def email_notifications_enabled() -> bool:
    settings = _get_system_settings()
    return bool(settings and settings.email_batch_notify_enabled)


def _get_smtp_config() -> tuple[str | None, int, str | None]:
    host = current_app.config.get("SMTP_HOST")
    port = int(current_app.config.get("SMTP_PORT") or 25)
    sender = current_app.config.get("SMTP_SENDER")
    return host, port, sender


def _send_email(to_addrs: Iterable[str], subject: str, body: str) -> bool:
    to_list = [addr for addr in to_addrs if addr]
    if not to_list:
        return False
    host, port, sender = _get_smtp_config()
    if not host or not sender:
        current_app.logger.warning("SMTP is not configured; skip sending email")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = sender
    msg["To"] = ", ".join(to_list)

    server = None
    try:
        server = smtplib.SMTP(host, port, timeout=10)
        server.ehlo()
        server.sendmail(sender, to_list, msg.as_string())
        return True
    except Exception:
        current_app.logger.exception("Failed to send notification email")
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


def _load_task_name(task_id: str) -> str:
    meta_path = os.path.join(current_app.config["TASK_FOLDER"], task_id, "meta.json")
    if not os.path.exists(meta_path):
        return task_id
    try:
        import json

        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        return (meta.get("name") or "").strip() or task_id
    except Exception:
        return task_id


def _format_results(results: list[dict]) -> list[str]:
    lines = []
    for item in results or []:
        flow = (item.get("flow") or "").strip() or "未命名流程"
        ok = bool(item.get("ok"))
        status = "成功" if ok else "失敗"
        job_id = item.get("job_id") if ok else ""
        error = (item.get("error") or "").strip()
        parts = [f"- {flow}: {status}"]
        if job_id:
            parts.append(f"(job: {job_id})")
        if error:
            parts.append(f"- {error}")
        lines.append(" ".join(parts))
    return lines


def send_batch_notification(
    task_id: str,
    batch_id: str,
    status: str,
    results: list[dict],
    actor_work_id: str,
    actor_label: str,
    completed_at: str | None = None,
    error: str | None = None,
) -> None:
    if not email_notifications_enabled():
        return

    recipient = None
    if actor_work_id:
        user = User.query.filter_by(work_id=actor_work_id).first()
        recipient = user.email if user else None
    if not recipient:
        current_app.logger.warning("Batch notification skipped: user email not found")
        return

    task_name = _load_task_name(task_id)
    status_label = "完成" if status == "completed" else "失敗"
    ok_count = sum(1 for item in results or [] if item.get("ok"))
    fail_count = sum(1 for item in results or [] if not item.get("ok"))

    subject = f"[法規文件轉換系統] 批次執行{status_label} - {task_name} ({batch_id})"

    lines = [
        f"批次執行已{status_label}。",
        f"任務：{task_name} ({task_id})",
        f"批次 ID：{batch_id}",
        f"執行者：{actor_label or actor_work_id}",
    ]
    if completed_at:
        lines.append(f"完成時間：{completed_at}")
    lines.append(f"成功：{ok_count}，失敗：{fail_count}")
    if error:
        lines.append(f"錯誤：{error}")
    if results:
        lines.append("")
        lines.append("流程結果：")
        lines.extend(_format_results(results))

    body = "\n".join(lines).strip() + "\n"
    _send_email([recipient], subject, body)
