from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
import sys
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy import MetaData, Table, create_engine, insert, select, update
from sqlalchemy.exc import IntegrityError, NoSuchTableError, SQLAlchemyError

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from modules.env_loader import load_dotenv_if_present

load_dotenv_if_present(str(BASE_DIR))

STATE_FILE = BASE_DIR / "harmonised_store" / "last_state.json"
DEFAULT_PAGE_URL = "https://single-market-economy.ec.europa.eu/single-market/goods/european-standards/harmonised-standards/medical-devices_en"
DEFAULT_LINK_TEXT = "Summary list as xls file"
REGULATION_SYNC_SOURCE_KEY = "regulation_eu_2017_745"
ALLOWED_EXCEL_EXTENSIONS = {".xlsx", ".xlsm", ".xltx", ".xltm", ".xls"}
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


@dataclass(frozen=True)
class UpdateDecision:
    current: dict
    last: dict | None
    last_source: str
    should_download: bool
    reasons: list[str]


def get_database_url() -> str:
    return (os.environ.get("DATABASE_URL") or os.environ.get("RBAC_DATABASE_URL") or "").strip()


def can_use_database() -> bool:
    database_url = get_database_url()
    if not database_url:
        return False
    try:
        from sqlalchemy.engine.url import make_url

        make_url(database_url)
        return True
    except Exception:
        return False


@lru_cache(maxsize=1)
def get_engine():
    database_url = get_database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for regulation update job")
    return create_engine(database_url, pool_pre_ping=True)


@lru_cache(maxsize=1)
def get_table(table_name: str) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=get_engine())


def resolve_save_dir() -> tuple[Path, str]:
    configured_root = (os.environ.get("REGULATION_EU_2017_745_REFERENCE_FOLDER") or "").strip()
    fallback_root = BASE_DIR / "harmonised_store"

    def _is_writable(path: Path) -> bool:
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".write-test"
            with probe.open("w", encoding="utf-8") as fh:
                fh.write("ok")
            probe.unlink(missing_ok=True)
            return True
        except Exception:
            return False

    if configured_root:
        primary = Path(configured_root)
        if _is_writable(primary):
            return primary, "primary"
        if _is_writable(fallback_root):
            print(f"主要存取路徑不可用，改用本機備援目錄。主路徑: {primary}")
            return fallback_root, "fallback"
        raise RuntimeError(f"主要與備援儲存路徑都不可用: {primary}, {fallback_root}")

    if _is_writable(fallback_root):
        print("未設定主要存取路徑，使用本機預設儲存目錄。")
        return fallback_root, "default"
    raise RuntimeError(f"本機預設儲存目錄不可用: {fallback_root}")


def load_download_source_settings() -> tuple[str, str]:
    page_url = ""
    link_text = ""
    if not can_use_database():
        return DEFAULT_PAGE_URL, DEFAULT_LINK_TEXT

    try:
        settings_table = get_table("system_settings")
        stmt = select(
            settings_table.c.regulation_download_page_url,
            settings_table.c.regulation_download_link_text,
        ).limit(1)
        with get_engine().connect() as conn:
            row = conn.execute(stmt).mappings().first()
        if row:
            page_url = (row.get("regulation_download_page_url") or "").strip()
            link_text = (row.get("regulation_download_link_text") or "").strip()
    except (NoSuchTableError, SQLAlchemyError) as exc:
        print(f"讀取下載來源設定失敗，改用預設值: {exc}")

    return page_url or DEFAULT_PAGE_URL, link_text or DEFAULT_LINK_TEXT


def get_download_link(page_url: str, link_text: str) -> str:
    resp = requests.get(page_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ", strip=True)
        if link_text.lower() in text.lower():
            return urljoin(page_url, anchor["href"])

    raise RuntimeError("找不到下載連結")


def parse_download_info(url: str) -> dict:
    parsed = urlparse(url)
    path_parts = parsed.path.strip("/").split("/")
    uuid_part = path_parts[-1] if path_parts else ""
    query = parse_qs(parsed.query)
    filename = unquote(query.get("filename", ["download.xlsx"])[0])
    return {
        "filename": filename,
        "uuid": uuid_part,
        "url": url,
    }


def load_last_state_from_db() -> dict | None:
    if not can_use_database():
        return None

    try:
        sync_table = get_table("regulation_sync_states")
        stmt = select(
            sync_table.c.last_filename,
            sync_table.c.last_uuid,
            sync_table.c.last_url,
        ).where(sync_table.c.source_key == REGULATION_SYNC_SOURCE_KEY)
        with get_engine().connect() as conn:
            row = conn.execute(stmt).mappings().first()
        if not row:
            return None
        if not any([(row.get("last_filename") or "").strip(), (row.get("last_uuid") or "").strip(), (row.get("last_url") or "").strip()]):
            return None
        return {
            "filename": (row.get("last_filename") or "").strip(),
            "uuid": (row.get("last_uuid") or "").strip(),
            "url": (row.get("last_url") or "").strip(),
        }
    except (NoSuchTableError, SQLAlchemyError) as exc:
        print(f"讀取資料庫 last_state 失敗，改用檔案: {exc}")
        return None


def load_last_state() -> tuple[dict | None, str]:
    state = load_last_state_from_db()
    if state:
        return state, "database"
    if not STATE_FILE.exists():
        return None, "missing"
    with STATE_FILE.open("r", encoding="utf-8") as fh:
        return json.load(fh), "file"


def save_last_state(state: dict) -> None:
    if can_use_database():
        try:
            sync_table = get_table("regulation_sync_states")
            with get_engine().begin() as conn:
                row = conn.execute(
                    select(sync_table.c.id).where(sync_table.c.source_key == REGULATION_SYNC_SOURCE_KEY)
                ).mappings().first()
                values = {
                    "last_filename": (state or {}).get("filename") or None,
                    "last_uuid": (state or {}).get("uuid") or None,
                    "last_url": (state or {}).get("url") or None,
                }
                if row:
                    conn.execute(
                        update(sync_table)
                        .where(sync_table.c.id == row["id"])
                        .values(**values)
                    )
                else:
                    try:
                        conn.execute(
                            insert(sync_table).values(
                                source_key=REGULATION_SYNC_SOURCE_KEY,
                                **values,
                            )
                        )
                    except IntegrityError:
                        conn.execute(
                            update(sync_table)
                            .where(sync_table.c.source_key == REGULATION_SYNC_SOURCE_KEY)
                            .values(**values)
                        )
        except (NoSuchTableError, SQLAlchemyError) as exc:
            print(f"寫入資料庫 last_state 失敗，改用檔案: {exc}")

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)


def is_updated(current: dict, last: dict | None) -> bool:
    if last is None:
        return True
    return any(
        [
            current["filename"] != last.get("filename"),
            current["uuid"] != last.get("uuid"),
            current["url"] != last.get("url"),
        ]
    )


def build_update_reasons(current: dict, last: dict | None, *, force_download: bool = False) -> list[str]:
    if force_download:
        return ["強制下載模式啟用"]
    if last is None:
        return ["沒有既有狀態紀錄"]

    reasons: list[str] = []
    if current["filename"] != last.get("filename"):
        reasons.append(f"filename: {last.get('filename')} -> {current['filename']}")
    if current["uuid"] != last.get("uuid"):
        reasons.append(f"uuid: {last.get('uuid')} -> {current['uuid']}")
    if current["url"] != last.get("url"):
        reasons.append("url 已變更")
    return reasons


def build_update_decision(current: dict, *, force_download: bool = False) -> UpdateDecision:
    last, last_source = load_last_state()
    should_download = force_download or is_updated(current, last)
    reasons = build_update_reasons(current, last, force_download=force_download)
    return UpdateDecision(
        current=current,
        last=last,
        last_source=last_source,
        should_download=should_download,
        reasons=reasons,
    )


def download_file(url: str, filename: str, save_dir: Path) -> Path:
    save_dir.mkdir(parents=True, exist_ok=True)
    path = save_dir / filename

    resp = requests.get(url, headers=HEADERS, stream=True, timeout=60)
    resp.raise_for_status()
    with path.open("wb") as fh:
        for chunk in resp.iter_content(8192):
            if chunk:
                fh.write(chunk)

    print("下載完成:", path)
    return path


def _sha1_file(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def register_downloaded_release(file_path: str, source_url: str = "") -> dict:
    if not can_use_database():
        print("未設定 DATABASE_URL，略過 active release 註冊")
        return {}

    path = Path(file_path)
    if not path.is_file() or path.suffix.lower() not in ALLOWED_EXCEL_EXTENSIONS:
        return {}

    try:
        release_table = get_table("harmonised_releases")
        stat = path.stat()
        downloaded_at = datetime.fromtimestamp(stat.st_mtime)
        version_label = downloaded_at.strftime("%Y%m%d-%H%M")
        checksum = _sha1_file(path)

        with get_engine().begin() as conn:
            conn.execute(update(release_table).values(is_active=False))
            result = conn.execute(
                insert(release_table).values(
                    source_url=source_url or None,
                    file_name=path.name,
                    nas_path=str(path.resolve()),
                    version_label=str(version_label),
                    checksum=checksum or None,
                    is_active=True,
                    download_status="available",
                    downloaded_at=downloaded_at,
                )
            )
            inserted_id = result.inserted_primary_key[0] if result.inserted_primary_key else None
        return {
            "id": inserted_id,
            "file_name": path.name,
            "path": str(path.resolve()),
            "version_label": str(version_label),
            "downloaded_at": downloaded_at.strftime("%Y-%m-%d %H:%M"),
            "source_url": source_url or "",
        }
    except (NoSuchTableError, SQLAlchemyError, OSError) as exc:
        print(f"註冊 active release 失敗: {exc}")
        return {}


def log_update_decision(decision: UpdateDecision) -> None:
    print("目前檔案:", decision.current["filename"])
    print("UUID:", decision.current["uuid"])
    print("last_state 來源:", decision.last_source)
    if decision.last:
        print("上次檔案:", decision.last.get("filename"))
        print("上次 UUID:", decision.last.get("uuid"))
    else:
        print("上次狀態: 無")
    print("是否需要下載:", decision.should_download)
    if decision.reasons:
        print("更新判斷依據:", " | ".join(decision.reasons))


def perform_download(current: dict, save_dir: Path) -> dict:
    downloaded_path = download_file(current["url"], current["filename"], save_dir)
    save_last_state(current)
    print("已更新狀態紀錄")

    sync_result = register_downloaded_release(str(downloaded_path), current["url"])
    print("同步結果:", json.dumps(sync_result, ensure_ascii=False))
    if sync_result:
        print("已同步前端顯示版本:", sync_result.get("file_name"))
        print("Active 路徑:", sync_result.get("path"))
        print("Active record id:", sync_result.get("id"))
    else:
        print("沒有可同步的 harmonised 版本")

    return {
        "downloaded": True,
        "path": str(downloaded_path),
        "sync_result": sync_result,
        "current": current,
    }


def check_for_update(*, page_url: str | None = None, link_text: str | None = None) -> dict:
    effective_page_url = (page_url or "").strip()
    effective_link_text = (link_text or "").strip()
    if not effective_page_url or not effective_link_text:
        loaded_page_url, loaded_link_text = load_download_source_settings()
        effective_page_url = effective_page_url or loaded_page_url
        effective_link_text = effective_link_text or loaded_link_text

    print("開始檢查更新（不下載）...")
    print("來源網址:", effective_page_url)
    print("LINK_TEXT:", effective_link_text)

    url = get_download_link(effective_page_url, effective_link_text)
    print("抓到連結:", url)

    current = parse_download_info(url)
    decision = build_update_decision(current, force_download=False)
    log_update_decision(decision)
    return {
        "checked": True,
        "should_download": decision.should_download,
        "current": decision.current,
        "last": decision.last or {},
        "last_source": decision.last_source,
        "reasons": decision.reasons,
    }


def run_update(*, force_download: bool = False, page_url: str | None = None, link_text: str | None = None) -> dict:
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
    decision = build_update_decision(current, force_download=force_download)
    log_update_decision(decision)

    if not decision.should_download:
        print("沒有更新，跳過")
        print("目前狀態與上次狀態一致")
        return {
            "downloaded": False,
            "forced": force_download,
            "path": "",
            "storage_mode": storage_mode,
            "sync_result": {},
            "current": current,
        }

    print("開始下載新版本...")
    result = perform_download(decision.current, save_dir)
    return {
        "downloaded": result["downloaded"],
        "forced": force_download,
        "path": result["path"],
        "storage_mode": storage_mode,
        "sync_result": result["sync_result"],
        "current": decision.current,
    }


def main() -> None:
    run_update()


if __name__ == "__main__":
    main()
