import json
import os
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from app import create_app
from app.services.standard_update_service import sync_latest_harmonised_release_from_store

# ======================
# 設定
# ======================
BASE_DIR = Path(__file__).resolve().parent
PAGE_URL = "https://single-market-economy.ec.europa.eu/single-market/goods/european-standards/harmonised-standards/medical-devices_en"
LINK_TEXT = "Summary list as xls file"
STATE_FILE = BASE_DIR / "last_state.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ======================
# 取得下載連結
# ======================
def get_download_link():
    resp = requests.get(PAGE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        if LINK_TEXT.lower() in text.lower():
            return urljoin(PAGE_URL, a["href"])

    raise Exception("找不到下載連結")

# ======================
# 解析資訊（filename + uuid）
# ======================
def parse_download_info(url):
    parsed = urlparse(url)

    # UUID（從 path 抓）
    path_parts = parsed.path.strip("/").split("/")
    uuid_part = path_parts[-1] if path_parts else ""

    # filename（從 query 抓）
    query = parse_qs(parsed.query)
    filename = query.get("filename", ["download.xlsx"])[0]
    filename = unquote(filename)

    return {
        "filename": filename,
        "uuid": uuid_part,
        "url": url
    }

# ======================
# 讀取 / 儲存狀態
# ======================
def load_last_state():
    if not STATE_FILE.exists():
        return None

    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ======================
# 判斷是否更新
# ======================
def is_updated(current, last):
    if last is None:
        return True

    return (
        current["filename"] != last.get("filename") or
        current["uuid"] != last.get("uuid") or
        current["url"] != last.get("url")
    )

# ======================
# 下載檔案
# ======================
def resolve_save_dir():
    app = create_app()
    with app.app_context():
        save_dir = Path(app.config["REGULATION_EU_2017_745_REFERENCE_FOLDER"])
    save_dir.mkdir(parents=True, exist_ok=True)
    return save_dir


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
    app = create_app()
    with app.app_context():
        result = sync_latest_harmonised_release_from_store()
    if result:
        print("已同步前端顯示版本:", result.get("file_name"))
        print("Active 路徑:", result.get("path"))
    else:
        print("沒有可同步的 harmonised 版本")
    return result

# ======================
# 主流程
# ======================
def main():
    print("開始檢查更新...")
    save_dir = resolve_save_dir()
    print("下載目錄:", save_dir)

    # 1. 抓最新下載連結
    url = get_download_link()
    print("抓到連結:", url)

    # 2. 解析資訊
    current = parse_download_info(url)
    print("目前檔案:", current["filename"])
    print("UUID:", current["uuid"])

    # 3. 載入舊資料
    last = load_last_state()

    # 4. 判斷是否更新
    if is_updated(current, last):
        print("發現新版本，開始下載...")

        download_file(current["url"], current["filename"], save_dir)

        save_state(current)
        print("已更新狀態紀錄")
        sync_frontend_active_release()

    else:
        print("沒有更新，跳過")

# ======================
# 執行
# ======================
if __name__ == "__main__":
    main()
