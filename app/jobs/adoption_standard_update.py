from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from flask import has_app_context

from modules.env_loader import load_dotenv_if_present

BASE_DIR = Path(__file__).resolve().parents[2]
STATE_FILE = BASE_DIR / "harmonised_store" / "last_state.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

_LAST_DOWNLOADED_FILE: Path | None = None
_LAST_DOWNLOAD_SOURCE_URL = ""

load_dotenv_if_present(str(BASE_DIR))


def _can_use_database() -> bool:
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


def _load_last_state_from_db():
    if not _can_use_database():
        return None
    try:
        from app import create_app
        from app.models.settings import RegulationSyncState

        app = create_app()
        with app.app_context():
            state = RegulationSyncState.query.filter_by(source_key="regulation_eu_2017_745").first()
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


def _save_last_state_to_db(state):
    if not _can_use_database():
        return False
    try:
        from app import create_app
        from app.extensions import db
        from app.models.auth import commit_session
        from app.models.settings import RegulationSyncState

        app = create_app()
        with app.app_context():
            sync_state = RegulationSyncState.query.filter_by(source_key="regulation_eu_2017_745").first()
            if not sync_state:
                sync_state = RegulationSyncState(source_key="regulation_eu_2017_745")
                db.session.add(sync_state)
            sync_state.last_filename = (state or {}).get("filename") or None
            sync_state.last_uuid = (state or {}).get("uuid") or None
            sync_state.last_url = (state or {}).get("url") or None
            commit_session()
        return True
    except Exception as exc:
        print(f"寫入資料庫 last_state 失敗，改用檔案: {exc}")
        return False


def load_download_source_settings() -> tuple[str, str]:
    page_url = ""
    link_text = ""
    database_url = (
        os.environ.get("DATABASE_URL") or os.environ.get("RBAC_DATABASE_URL") or ""
    ).strip()
    try:
        from app import create_app
        from app.models.settings import SystemSetting

        if database_url:
            from sqlalchemy.engine.url import make_url

            make_url(database_url)
        app = create_app("testing" if not database_url else None)
        with app.app_context():
            page_url = (app.config.get("REGULATION_DOWNLOAD_PAGE_URL") or "").strip()
            link_text = (app.config.get("REGULATION_DOWNLOAD_LINK_TEXT") or "").strip()
            setting = SystemSetting.query.order_by(SystemSetting.id).first()
            if setting:
                page_url = (setting.regulation_download_page_url or "").strip() or page_url
                link_text = (setting.regulation_download_link_text or "").strip() or link_text
    except Exception as exc:
        print(f"讀取下載來源設定失敗，改用預設值: {exc}")
        page_url = page_url or "https://single-market-economy.ec.europa.eu/single-market/goods/european-standards/harmonised-standards/medical-devices_en"
        link_text = link_text or "Summary list as xls file"
    return page_url, link_text


def get_download_link(page_url: str, link_text: str):
    resp = requests.get(page_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        if link_text.lower() in text.lower():
            return urljoin(page_url, a["href"])

    raise Exception("找不到下載連結")


def parse_download_info(url):
    parsed = urlparse(url)
    path_parts = parsed.path.strip("/").split("/")
    uuid_part = path_parts[-1] if path_parts else ""
    query = parse_qs(parsed.query)
    filename = query.get("filename", ["download.xlsx"])[0]
    filename = unquote(filename)

    return {
        "filename": filename,
        "uuid": uuid_part,
        "url": url
    }


def load_last_state():
    state = _load_last_state_from_db()
    if state:
        return state
    if not STATE_FILE.exists():
        return None

    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    _save_last_state_to_db(state)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def is_updated(current, last):
    if last is None:
        return True

    return (
        current["filename"] != last.get("filename") or
        current["uuid"] != last.get("uuid") or
        current["url"] != last.get("url")
    )


def _ensure_storage_available(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path, prefix=".write-test-", delete=True):
            pass
        return True
    except Exception:
        return False


def resolve_save_dir() -> tuple[Path, str]:
    configured_root = (os.environ.get("REGULATION_EU_2017_745_REFERENCE_FOLDER") or "").strip()
    fallback_dir = BASE_DIR / "harmonised_store"

    if configured_root:
        primary_dir = Path(configured_root)
        if _ensure_storage_available(primary_dir):
            return primary_dir, "primary"
        print(f"主要儲存路徑不可用，改用本機備援目錄。主路徑: {primary_dir}")
        if _ensure_storage_available(fallback_dir):
            return fallback_dir, "fallback"
        raise RuntimeError(f"主要與備援儲存路徑都不可用: {primary_dir}, {fallback_dir}")

    if _ensure_storage_available(fallback_dir):
        return fallback_dir, "default"
    raise RuntimeError(f"本機備援目錄不可用: {fallback_dir}")


def download_file(url, filename, save_dir: Path):
    save_dir.mkdir(parents=True, exist_ok=True)

    path = save_dir / filename

    resp = requests.get(url, headers=HEADERS, stream=True, timeout=60)
    resp.raise_for_status()

    with path.open("wb") as f:
        for chunk in resp.iter_content(8192):
            if chunk:
                f.write(chunk)

    print("下載完成:", path)
    return path


def sync_frontend_active_release():
    database_url = (
        os.environ.get("DATABASE_URL") or os.environ.get("RBAC_DATABASE_URL") or ""
    ).strip()
    if not database_url:
        print("未設定 DATABASE_URL，略過前端 active 版本同步")
        return {}

    try:
        from sqlalchemy.engine.url import make_url

        make_url(database_url)
    except Exception:
        print("DATABASE_URL 格式無法被 SQLAlchemy 解析，略過前端 active 版本同步")
        return {}

    from app.services.standard_update_service import activate_harmonised_release

    if has_app_context():
        result = activate_harmonised_release(
            str(_LAST_DOWNLOADED_FILE or ""),
            source_url=_LAST_DOWNLOAD_SOURCE_URL,
        )
    else:
        from app import create_app

        app = create_app()
        with app.app_context():
            result = activate_harmonised_release(
                str(_LAST_DOWNLOADED_FILE or ""),
                source_url=_LAST_DOWNLOAD_SOURCE_URL,
            )
    if result:
        print("已同步前端顯示版本:", result.get("file_name"))
        print("Active 路徑:", result.get("path"))
    else:
        print("沒有可同步的 harmonised 版本")
    return result


def run_update(*, force_download: bool = False, page_url: str | None = None, link_text: str | None = None) -> dict:
    global _LAST_DOWNLOADED_FILE, _LAST_DOWNLOAD_SOURCE_URL

    print("開始檢查更新...")
    save_dir, storage_mode = resolve_save_dir()
    print("下載目錄:", save_dir)
    print("儲存模式:", storage_mode)
    effective_page_url = (page_url or "").strip()
    effective_link_text = (link_text or "").strip()
    if not effective_page_url or not effective_link_text:
        loaded_page_url, loaded_link_text = load_download_source_settings()
        effective_page_url = effective_page_url or loaded_page_url
        effective_link_text = effective_link_text or loaded_link_text
    print("來源網址:", effective_page_url)
    print("LINK_TEXT:", effective_link_text)

    url = get_download_link(effective_page_url, effective_link_text)
    print("抓到連結:", url)

    current = parse_download_info(url)
    print("目前檔案:", current["filename"])
    print("UUID:", current["uuid"])

    last = load_last_state()

    if force_download or is_updated(current, last):
        print("發現新版本，開始下載...")

        downloaded_path = download_file(current["url"], current["filename"], save_dir)
        _LAST_DOWNLOADED_FILE = downloaded_path
        _LAST_DOWNLOAD_SOURCE_URL = current["url"]

        save_state(current)
        print("已更新狀態紀錄")
        sync_result = sync_frontend_active_release()
        return {
            "downloaded": True,
            "forced": force_download,
            "path": str(downloaded_path),
            "storage_mode": storage_mode,
            "sync_result": sync_result,
            "current": current,
        }

    print("沒有更新，跳過")
    return {
        "downloaded": False,
        "forced": force_download,
        "path": "",
        "storage_mode": storage_mode,
        "sync_result": {},
        "current": current,
    }


def main():
    run_update()
