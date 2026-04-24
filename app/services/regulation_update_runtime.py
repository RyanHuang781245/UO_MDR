from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from app import create_job_app

BASE_DIR = Path(__file__).resolve().parents[2]
STATE_FILE = BASE_DIR / "harmonised_store" / "last_state.json"
DEFAULT_PAGE_URL = "https://single-market-economy.ec.europa.eu/single-market/goods/european-standards/harmonised-standards/medical-devices_en"
DEFAULT_LINK_TEXT = "Summary list as xls file"


def can_use_database() -> bool:
    database_url = (
        os.environ.get("DATABASE_URL") or os.environ.get("RBAC_DATABASE_URL") or ""
    ).strip()
    if not database_url:
        return False
    try:
        from sqlalchemy.engine.url import make_url

        make_url(database_url)
        return True
    except Exception:
        return False


@contextmanager
def job_app_context(*, require_database: bool = True, config_name: str | None = None) -> Iterator[object | None]:
    if require_database and not can_use_database():
        yield None
        return

    effective_config = config_name
    if effective_config is None and not require_database:
        effective_config = "testing"

    app = create_job_app(effective_config)
    with app.app_context():
        yield app


def load_download_source_settings() -> tuple[str, str]:
    page_url = ""
    link_text = ""
    try:
        from app.models.settings import SystemSetting

        with job_app_context(require_database=can_use_database()) as _app:
            if _app is not None:
                page_url = (_app.config.get("REGULATION_DOWNLOAD_PAGE_URL") or "").strip()
                link_text = (_app.config.get("REGULATION_DOWNLOAD_LINK_TEXT") or "").strip()
                setting = SystemSetting.query.order_by(SystemSetting.id).first()
                if setting:
                    page_url = (setting.regulation_download_page_url or "").strip() or page_url
                    link_text = (setting.regulation_download_link_text or "").strip() or link_text
    except Exception as exc:
        print(f"讀取下載來源設定失敗，改用預設值: {exc}")
    return page_url or DEFAULT_PAGE_URL, link_text or DEFAULT_LINK_TEXT


def resolve_save_dir() -> tuple[Path, str, str]:
    with job_app_context(require_database=False) as app:
        if app is None:
            return BASE_DIR / "harmonised_store", "default", "未設定主要存取路徑"

        save_dir = Path(app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"])
        storage_mode = (app.config.get("REGULATION_EU_2017_745_REFERENCE_STORAGE_MODE") or "default").strip()
        status_message = (app.config.get("REGULATION_EU_2017_745_REFERENCE_STATUS_MESSAGE") or "").strip()
        return save_dir, storage_mode, status_message


def load_last_state_from_db() -> dict | None:
    if not can_use_database():
        return None
    try:
        from app.models.settings import get_regulation_sync_state

        with job_app_context(require_database=True) as _app:
            if _app is None:
                return None
            state = get_regulation_sync_state()
            if not state:
                return None
            if not any(
                [
                    (state.last_filename or "").strip(),
                    (state.last_uuid or "").strip(),
                    (state.last_url or "").strip(),
                ]
            ):
                return None
            return {
                "filename": (state.last_filename or "").strip(),
                "uuid": (state.last_uuid or "").strip(),
                "url": (state.last_url or "").strip(),
            }
    except Exception as exc:
        print(f"讀取資料庫 last_state 失敗，改用檔案: {exc}")
        return None


def load_last_state() -> tuple[dict | None, str]:
    state = load_last_state_from_db()
    if state:
        return state, "database"
    if not STATE_FILE.exists():
        return None, "missing"

    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f), "file"


def save_last_state(state: dict) -> None:
    if can_use_database():
        try:
            from app.models.settings import upsert_regulation_sync_state

            with job_app_context(require_database=True) as _app:
                if _app is not None:
                    upsert_regulation_sync_state(
                        last_filename=(state or {}).get("filename") or None,
                        last_uuid=(state or {}).get("uuid") or None,
                        last_url=(state or {}).get("url") or None,
                    )
        except Exception as exc:
            print(f"寫入資料庫 last_state 失敗，改用檔案: {exc}")

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def register_downloaded_release(file_path: str, source_url: str = "") -> dict:
    if not can_use_database():
        print("未設定 DATABASE_URL，略過 active release 註冊")
        return {}

    try:
        from app.services.standard_update_service import register_downloaded_harmonised_release

        with job_app_context(require_database=True) as _app:
            if _app is None:
                return {}
            return register_downloaded_harmonised_release(
                file_path,
                source_url=source_url,
            )
    except Exception as exc:
        print(f"註冊 active release 失敗: {exc}")
        return {}
