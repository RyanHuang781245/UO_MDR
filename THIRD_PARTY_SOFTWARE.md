# 第三方軟體安裝

本文整理 UO MDR 系統部署時所需的第三方軟體與 Ubuntu 系統套件安裝方式。系統環境需求請參考 [SYSTEM_ENVIRONMENT.md](./SYSTEM_ENVIRONMENT.md)，正式部署流程請參考 [SYSTEM_DEPLOYMENT.md](./SYSTEM_DEPLOYMENT.md)。

## 1. 基礎套件

```bash
sudo apt update
sudo apt install -y curl wget ca-certificates gnupg lsb-release
```

## 2. Nginx

```bash
sudo apt update
sudo apt install -y nginx
sudo systemctl start nginx
sudo systemctl enable nginx
sudo systemctl status nginx --no-pager
```

確認：

```bash
nginx -v
sudo nginx -t
```

## 3. uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
command -v uv
uv --version
```

若主機尚未安裝 Python 3.11.4，可由 `uv` 安裝：

```bash
uv python install 3.11.4
```

## 4. Microsoft ODBC Driver 18 與 unixODBC

系統透過 SQLAlchemy 與 `pyodbc` 連線 Microsoft SQL Server，因此 Ubuntu Server 必須安裝 unixODBC 與 Microsoft ODBC Driver 18。

### 安裝 unixODBC

`pyodbc` 需要 unixODBC 提供 `libodbc.so.2`。

```bash
sudo apt update
sudo apt install -y unixodbc unixodbc-dev
```

確認 ODBC library：

```bash
ldconfig -p | grep libodbc.so
```

正常應可看到類似：

```text
libodbc.so.2 => /lib/x86_64-linux-gnu/libodbc.so.2
```

### 加入 Microsoft 套件庫

以下範例適用 Ubuntu 24.04 noble：

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo mkdir -p /etc/apt/keyrings

curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
  sudo gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg

sudo chmod 644 /etc/apt/keyrings/microsoft.gpg

echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main" | \
  sudo tee /etc/apt/sources.list.d/microsoft-prod.list

sudo apt update
```

### 安裝 ODBC Driver 18

```bash
sudo ACCEPT_EULA=Y apt install -y msodbcsql18 unixodbc unixodbc-dev
```

確認 driver：

```bash
odbcinst -q -d
```

應可看到：

```text
[ODBC Driver 18 for SQL Server]
```

再次確認 `libodbc.so.2`：

```bash
ldconfig -p | grep libodbc.so.2
```

### Python 連線測試範例

```python
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=your_server;"
    "DATABASE=your_db;"
    "UID=your_user;"
    "PWD=your_password;"
    "TrustServerCertificate=yes;"
)

print("Connected!")
```

## 5. sqlcmd / mssql-tools18

資料庫備份與還原腳本會使用 `sqlcmd`。若尚未安裝 `mssql-tools18`：

```bash
sudo ACCEPT_EULA=Y apt install -y mssql-tools18
```

若要讓互動式 shell 可直接找到 `sqlcmd`：

```bash
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc
```

systemd 服務不一定會讀取使用者的 `.bashrc`，因此建議在 `.env` 指定完整路徑：

```env
SQLCMD_BIN=/opt/mssql-tools18/bin/sqlcmd
```

確認：

```bash
which sqlcmd || test -x /opt/mssql-tools18/bin/sqlcmd
/opt/mssql-tools18/bin/sqlcmd -?
```

## 6. LibreOffice

Ubuntu apt 安裝：

```bash
sudo apt update
sudo apt install -y libreoffice
soffice --version
```

若需指定既有驗證版本，可使用官方舊版封存檔安裝，例如 LibreOffice 24.2.7.2：

```bash
cd /tmp
wget https://downloadarchive.documentfoundation.org/libreoffice/old/24.2.7.2/deb/x86_64/LibreOffice_24.2.7.2_Linux_x86-64_deb.tar.gz
tar -xzf LibreOffice_24.2.7.2_Linux_x86-64_deb.tar.gz
cd LibreOffice_24.2.7.2_Linux_x86-64_deb/DEBS
sudo apt install -y ./*.deb
soffice --version
```

建議於 `.env` 指定執行檔路徑：

```env
LIBREOFFICE_BIN=/usr/bin/soffice
```

## 7. pandoc

若文件處理流程需要 pandoc，可安裝指定版本的 `.deb` 套件：

```bash
cd /tmp
wget https://github.com/jgm/pandoc/releases/download/3.9.0.2/pandoc-3.9.0.2-1-amd64.deb
sudo apt install -y ./pandoc-3.9.0.2-1-amd64.deb
pandoc --version | head -n 1
```

## 8. Noto CJK 繁中文字體

LibreOffice 產生 DOCX / PDF 預覽時，若系統沒有可用的繁體中文字體，可能出現中文字缺字、字型替換錯誤或版面跑版。因此部署環境建議安裝 Noto CJK 繁中字體。

專案已提供安裝腳本，會下載下列字體檔並更新 font cache：

- `NotoSansCJKtc-Regular.otf`
- `NotoSansCJKtc-Bold.otf`

預設安裝路徑：

```text
${HOME}/.local/share/fonts/noto-cjk
```

### 使用專案腳本安裝

```bash
cd /home/NE025/UO_MDR
bash scripts/install_noto_cjk_fonts.sh
```

### 強制重新下載

```bash
bash scripts/install_noto_cjk_fonts.sh --force
```

### 指定安裝目錄

```bash
bash scripts/install_noto_cjk_fonts.sh --install-dir /home/NE025/.local/share/fonts/noto-cjk
```

### deploy.sh 相關參數

```bash
# 略過字體安裝
INSTALL_NOTO_CJK_FONTS=0 bash deploy.sh

# 強制重新下載字體
INSTALL_NOTO_CJK_FONTS_FORCE=1 bash deploy.sh

# 指定字體安裝目錄
NOTO_CJK_FONTS_DIR=/home/NE025/.local/share/fonts/noto-cjk bash deploy.sh
```

### 手動下載來源

若部署環境無法執行腳本，可手動下載字體檔：

```bash
mkdir -p /home/NE025/.local/share/fonts/noto-cjk

curl -L --fail --show-error \
  https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Regular.otf \
  -o /home/NE025/.local/share/fonts/noto-cjk/NotoSansCJKtc-Regular.otf

curl -L --fail --show-error \
  https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Bold.otf \
  -o /home/NE025/.local/share/fonts/noto-cjk/NotoSansCJKtc-Bold.otf

fc-cache -f /home/NE025/.local/share/fonts/noto-cjk
```

### 安裝後確認

```bash
fc-match "Noto Sans CJK TC"
```

建議於 `.env` 指定預覽使用的東亞字體名稱：

```env
PROVENANCE_PREVIEW_LABEL_EAST_ASIA_FONT=Noto Sans CJK TC
```

安裝或更新字體後，建議重新啟動會產生預覽檔案的服務：

```bash
sudo systemctl restart uo_regulations
sudo systemctl restart uo_regulations_jobs_worker
sudo systemctl restart uo_regulations_flow_worker
sudo systemctl restart uo_regulations_batch_worker
```

## 9. 最小安裝檢查

```bash
nginx -v
sudo nginx -t
uv --version
odbcinst -q -d | rg "ODBC Driver 18 for SQL Server"
ldconfig -p | grep libodbc.so.2
/opt/mssql-tools18/bin/sqlcmd -?
soffice --version
fc-match "Noto Sans CJK TC"
pandoc --version | head -n 1
```
