import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, unquote

# ======================
# 設定
# ======================
PAGE_URL = "https://single-market-economy.ec.europa.eu/single-market/goods/european-standards/harmonised-standards/medical-devices_en"
LINK_TEXT = "Summary list as xls file"
SAVE_DIR = r"C:\Users\ne025\Desktop\UO_MDR"
STATE_FILE = "last_state.json"

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
    if not os.path.exists(STATE_FILE):
        return None

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
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
def download_file(url, filename):
    os.makedirs(SAVE_DIR, exist_ok=True)

    path = os.path.join(SAVE_DIR, filename)

    resp = requests.get(url, headers=HEADERS, stream=True, timeout=60)
    resp.raise_for_status()

    with open(path, "wb") as f:
        for chunk in resp.iter_content(8192):
            if chunk:
                f.write(chunk)

    print("下載完成:", path)

# ======================
# 主流程
# ======================
def main():
    print("開始檢查更新...")

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

        download_file(current["url"], current["filename"])

        save_state(current)
        print("已更新狀態紀錄")

    else:
        print("沒有更新，跳過")

# ======================
# 執行
# ======================
if __name__ == "__main__":
    main()