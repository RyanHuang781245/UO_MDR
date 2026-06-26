# 系統環境說明

本文說明 UO MDR 系統執行所需的作業系統、Python 環境、第三方軟體、環境變數與路徑設定。實際部署流程請參考 [SYSTEM_DEPLOYMENT.md](./SYSTEM_DEPLOYMENT.md)，第三方軟體詳細安裝指令請參考 [THIRD_PARTY_SOFTWARE.md](./THIRD_PARTY_SOFTWARE.md)。

- [系統環境說明](#系統環境說明)
  - [1. 系統執行環境](#1-系統執行環境)
  - [2. Python 套件](#2-python-套件)
  - [3. 第三方軟體](#3-第三方軟體)
  - [4. 需先定義環境變數](#4-需先定義環境變數)
  - [5. 環境變數](#5-環境變數)
    - [Flask 與應用程式模式](#flask-與應用程式模式)
    - [資料庫](#資料庫)
    - [NAS 與檔案來源](#nas-與檔案來源)
    - [歐盟採認標準文件](#歐盟採認標準文件)
    - [系統 log](#系統-log)
    - [清理保留天數](#清理保留天數)
    - [AD / LDAP 與權限](#ad--ldap-與權限)
    - [SMTP](#smtp)
    - [Azure OpenAI](#azure-openai)
    - [文件處理與外部工具](#文件處理與外部工具)
    - [systemd timer 排程](#systemd-timer-排程)
  - [6. 本機目錄與權限](#6-本機目錄與權限)


## 1. 系統執行環境

| 項目 | 說明 |
| --- | --- |
| 作業系統 | Ubuntu 24.04.3 |
| 專案部署路徑 | `/home/NE025/UO_MDR` |
| Python 版本 | 3.11.4 |
| Python 虛擬環境 | `/home/NE025/UO_MDR/.venv` |
| 套件管理 | `uv`、`pyproject.toml`、`uv.lock` |
| Web Server | Nginx |
| WSGI Server | Gunicorn |
| Web Framework | Flask |
| 資料庫 | Microsoft SQL Server |
| 資料庫連線方式 | SQLAlchemy + pyodbc |
| 服務管理 | systemd |
| Gunicorn Socket | `/home/NE025/UO_MDR/uo_regulations.sock` |
| 環境變數檔案 | `/home/NE025/UO_MDR/.env` |

## 2. Python 套件

本專案以 `pyproject.toml` 宣告依賴，並以 `uv.lock` 固定部署版本，主要 Python 套件如下：

- `Flask`：Web 應用程式框架。
- `Gunicorn`：WSGI Server，用於啟動 Flask App。
- `Flask-SQLAlchemy` / `SQLAlchemy`：ORM 與資料庫操作。
- `pyodbc`：MSSQL ODBC 連線。
- `alembic`：資料庫 schema migration。
- `python-dotenv`：載入 `.env` 環境變數。
- `python-docx`、`PyMuPDF`、`docxcompose`、`docxtpl`、`openpyxl`：文件與試算表處理。
- `boto3`、`requests`、`beautifulsoup4`：外部服務、HTTP 請求與資料擷取。

## 3. 第三方軟體

系統需安裝下列第三方軟體或系統套件。詳細安裝步驟請參考 [THIRD_PARTY_SOFTWARE.md](./THIRD_PARTY_SOFTWARE.md)。

| 軟體 | 用途 | 檢查指令 |
| --- | --- | --- |
| Nginx | HTTP 入口、靜態檔案服務、反向代理至 Gunicorn Unix Socket。 | `nginx -v`、`sudo nginx -t` |
| uv | 建立與同步 Python 虛擬環境。 | `uv --version` |
| Microsoft ODBC Driver 18 for SQL Server | 提供 pyodbc 連線 MSSQL 所需 driver。 | `odbcinst -q -d \| rg "ODBC Driver 18 for SQL Server"` |
| sqlcmd / mssql-tools18 | 執行 MSSQL 備份、還原與連線檢查。 | `which sqlcmd`、`sqlcmd -?` |
| unixodbc / unixodbc-dev | 提供 ODBC runtime、開發元件與 `libodbc.so.2`。 | `ldconfig -p \| grep libodbc.so.2` |
| LibreOffice / soffice | 支援 DOCX 轉 PDF 與文件預覽流程。 | `soffice --version` |
| Noto CJK 繁中字體 | 讓 LibreOffice 預覽正確顯示繁體中文。 | `fc-match "Noto Sans CJK TC"` |

## 4. 需先定義環境變數

部署前需先在 `.env` 或部署命令中確認路徑型設定。這些路徑與主機掛載點、備份目錄或外部程式位置有關，無法只靠程式自動推斷。

| 變數 | 用途 | 設定注意事項 |
| --- | --- | --- |
| `ALLOWED_NAS_ROOTS_LINUX` | 指定存取的 NAS 根目錄。 | 必須先確認 NAS 已掛載，且服務使用者可讀取。 |
| `REGULATION_EU_2017_745_REFERENCE_FOLDER` | 歐盟採認標準文件主要存取資料夾。 | 指向 NAS 或正式文件存放路徑。 |
| `REGULATION_EU_2017_745_REFERENCE_FALLBACK_FOLDER` | 主要採認標準資料夾不可用時的本機備援資料夾。 | 建議設為 `$APP_ROOT/harmonised_store`。 |
| `APP_LOG_DIR` | 系統 log 檔案輸出目錄。 | 建議設為 `$APP_ROOT/logs`。 |
| `MSSQL_BACKUP_DIR` | SQL Server 寫入 `.bak` 備份檔的目錄。 | 此路徑是 SQL Server 主機可存取的路徑，不是 Ubuntu 本機路徑。 |
| `BACKUP_ROOT` | 系統檔案備份 archive 輸出目錄。 | 若未設定，預設為 `$APP_ROOT/backups/files`。 |
| `LIBREOFFICE_BIN` | LibreOffice / soffice 執行檔位置。 | 預設安裝位置通常為 `/usr/bin/soffice`。 |
| `SQLCMD_BIN` | Microsoft SQL Server command-line tool 執行檔位置。 | systemd 不一定會讀取 `.bashrc`， 預設安裝位置通常為 `/opt/mssql-tools18/bin/sqlcmd` |

範例：

```env
ALLOWED_NAS_ROOTS_LINUX=/mnt/nas/project-root
REGULATION_EU_2017_745_REFERENCE_FOLDER=/mnt/nas/standard-reference
REGULATION_EU_2017_745_REFERENCE_FALLBACK_FOLDER=/home/NE025/UO_MDR/harmonised_store
APP_LOG_DIR=/home/NE025/UO_MDR/logs
LIBREOFFICE_BIN=/usr/bin/soffice
SQLCMD_BIN=/opt/mssql-tools18/bin/sqlcmd
MSSQL_BACKUP_DIR='D:\MSSQL\Backup'
```

## 5. 環境變數

系統設定與機敏資料透過 `.env` 注入，檔案位置預設為 `專案目錄/.env`。

調整 `.env` 參數後，需重啟相關 systemd 服務：

```bash
sudo systemctl restart uo_regulations
sudo systemctl restart uo_regulations_jobs_worker
sudo systemctl restart uo_regulations_flow_worker
sudo systemctl restart uo_regulations_batch_worker
```

若修改 timer 排程，需重新 render systemd units：

```bash
bash deploy.sh
systemctl list-timers | grep uo_regulations
```

### Flask 與應用程式模式

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `SECRET_KEY` | Flask session、CSRF 或其他簽章用途的系統金鑰。 | 機敏資料，正式環境需使用高強度隨機值。 |
| `SESSION_COOKIE_NAME` | 指定瀏覽器 session cookie 名稱。 | 測試與正式環境可使用不同名稱，避免 cookie 互相干擾。 |
| `AUTO_SCHEMA_MANAGEMENT` | 控制應用程式是否自動管理或初始化 schema。 | 正式環境通常設為 `0`，由 migration 流程控制 schema。 |
| `APP_ENV` | 指定應用程式執行環境。 | 正式部署建議設為 `production`。 |
| `JOB_EXECUTOR_MODE` | 指定任務執行模式。 | 目前部署使用 `worker`，由 systemd worker services 處理背景任務。 |

### 資料庫

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `DATABASE_URL` | MSSQL 資料庫連線字串。 | 機敏資料，包含帳號密碼；部署、migration 與系統啟動皆依賴此設定。 |
| `MSSQL_QUERY_TIMEOUT` | MSSQL 查詢執行 timeout，單位秒。 | 例如 `30`；控制 pyodbc query timeout，變更後需重啟服務。 |

### NAS 與檔案來源

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `ALLOWED_NAS_ROOTS_LINUX` | Linux 環境允許瀏覽與匯入的 NAS 根目錄。 | 首次啟動且 `nas_roots` 表為空時，系統會以此作為初始 NAS root 資料來源。 |
| `NAS_MAX_COPY_FILE_SIZE_MB` | 從 NAS 複製檔案或資料夾時的大小限制，單位為 MB。 | 設為 `0` 或留空時代表不限制。 |

### 歐盟採認標準文件

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `REGULATION_EU_2017_745_REFERENCE_FOLDER` | 歐盟採認標準文件主要存取資料夾。 | 通常指向 NAS 或正式文件存放路徑，需確認服務使用者可讀寫。 |
| `REGULATION_EU_2017_745_REFERENCE_FALLBACK_FOLDER` | 主要資料夾不可用時的本機備援資料夾。 | 建議設為 `/home/NE025/UO_MDR/harmonised_store` 或其他服務可寫入路徑。 |

### 系統 log

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `APP_LOG_DIR` | 系統 log 檔案輸出目錄。 | 需確認 systemd service 的 `User` 具備寫入權限。 |
| `APP_LOG_LEVEL` | 檔案與 stdout log 層級。 | 常見值為 `DEBUG`、`INFO`、`WARNING`、`ERROR`。 |
| `APP_LOG_TO_FILE` | 是否輸出 log 至檔案。 | `true` / `false`。 |
| `APP_LOG_STDOUT` | 是否輸出 log 至 stdout。 | systemd journal 可讀取 stdout / stderr。 |
| `APP_LOG_MAX_MB` | 單一 log 檔案大小上限，單位為 MB。 | 達上限後依 logging 設定輪替。 |
| `APP_LOG_BACKUP_COUNT` | 保留的輪替 log 檔案數量。 | 值越大，占用磁碟空間越多。 |
| `SYSTEM_ERROR_DB_MIN_LEVEL` | 寫入 `system_error_logs` 資料表的最低 log level。 | 例如 `INFO`、`WARNING`、`ERROR`。 |
| `SYSTEM_ERROR_FALLBACK_MAX_MB` | `system-error-fallback.jsonl` 大小上限，單位為 MB。 | 預設 `50`；當下一筆 fallback log 會超過上限時，檔案會被重寫為新的一筆。設為 `0` 代表不限制。 |

### 清理保留天數

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `JOB_METADATA_RETENTION_DAYS` | 任務 metadata 或暫存紀錄保留天數。 | 變更後需重啟相關服務或重新部署，讓排程服務載入新值。 |
| `SYSTEM_ERROR_LOG_RETENTION_DAYS` | 系統錯誤紀錄保留天數。 | 由 metadata cleanup 排程依設定清理。 |
| `AUDIT_LOG_RETENTION_DAYS` | audit log 保留天數。 | 由 metadata cleanup 排程依設定清理。 |

### AD / LDAP 與權限

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `LDAP_HOST` | LDAP / AD 伺服器主機。 | 可填網域或 LDAP server 位址。 |
| `LDAP_BASE_DN` | LDAP 查詢基準 DN。 | 決定使用者與群組查詢範圍。 |
| `LDAP_BIND_DN` | LDAP bind 帳號。 | 機敏設定，通常為服務查詢帳號。 |
| `LDAP_BIND_PASSWORD` | LDAP bind 密碼。 | 機敏資料，不應提交至版本控制。 |
| `LDAP_USER_LOGIN_ATTR` | 使用者登入屬性。 | 常見值為 `sAMAccountName`。 |
| `LDAP_USER_SEARCH_SCOPE` | LDAP 使用者查詢範圍。 | 常見值為 `BASE`、`LEVEL`、`SUBTREE`。 |
| `LDAP_USER_OBJECT_FILTER` | LDAP 使用者查詢 filter。 | 用於排除非使用者物件或限制查詢條件。 |
| `LDAP_GROUP_GATE_ENABLED` | 是否啟用群組 gate。 | 設為 `0` / `false` 時略過群組 gate。 |
| `ALLOWED_GROUP_DN` | 允許登入或授權的群組 DN。 | 啟用群組 gate 時使用；可依實際 AD 群組設定。 |
| `INITIAL_ADMIN_WORK_IDS` | 初始管理員工號清單。 | 用於初始系統管理權限設定。 |

### SMTP

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `SMTP_HOST` | SMTP 伺服器主機。 | 用於系統寄送通知信。 |
| `SMTP_PORT` | SMTP 伺服器 port。 | 常見值為 `25`、`587`。 |
| `SMTP_SENDER` | 系統寄件者信箱。 | 應使用可被 SMTP server 接受的寄件者。 |

### Azure OpenAI

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `AZURE_OPENAI_API_KEY` | Azure OpenAI API key。 | 機敏資料，不應提交至版本控制。 |
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI endpoint。 | 需與實際 Azure OpenAI resource 對應。 |
| `AZURE_OPENAI_DEPLOYMENT` | Azure OpenAI deployment 名稱。 | 需與 Azure 上部署的模型 deployment 名稱一致。 |
| `AZURE_OPENAI_API_VERSION` | Azure OpenAI API version。 | 需與目前使用的 API 相容。 |

### 文件處理與外部工具

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `SKIP_DOCX_CLEANUP` | 是否跳過 DOCX 清理程序。 | 設為 `1` / `true` 時略過清理；變更後需重啟服務。 |
| `WORD_CHAPTER_LLM_BOUNDARY_FALLBACK` | 是否啟用 LLM 輔助判斷 Word 章節擷取中斷點。 | `true` / `false`。 |
| `LIBREOFFICE_BIN` | LibreOffice / soffice 執行檔位置。 | 常見值為 `/usr/bin/soffice`。 |
| `SQLCMD_BIN` | `sqlcmd` 執行檔位置。 | systemd 不一定讀取 `.bashrc`，建議填完整路徑。 |

### systemd timer 排程

| 變數 | 用途 | 注意事項 |
| --- | --- | --- |
| `UPDATE_ON_CALENDAR` | 採認標準更新 timer 排程。 | systemd `OnCalendar` 格式；值包含空白時需加引號。 |
| `CLEANUP_ON_CALENDAR` | metadata cleanup timer 排程。 | systemd `OnCalendar` 格式；值包含空白時需加引號。 |
| `BACKUP_ON_CALENDAR` | 備份 timer 排程。 | systemd `OnCalendar` 格式；值包含空白時需加引號。 |

`UPDATE_ON_CALENDAR`、`CLEANUP_ON_CALENDAR`、`BACKUP_ON_CALENDAR` 會在執行 `deploy.sh` 時寫入 systemd timer unit；修改 `.env` 後需重新執行部署或重新安裝 systemd units，timer 才會套用新排程。

## 6. 本機目錄與權限

首次部署前建議建立或確認下列目錄：

```bash
mkdir -p /home/NE025/UO_MDR/logs
mkdir -p /home/NE025/UO_MDR/task_store
mkdir -p /home/NE025/UO_MDR/standard_update_store
mkdir -p /home/NE025/UO_MDR/harmonised_store
mkdir -p /home/NE025/UO_MDR/backups/files
```

若服務使用者或群組有調整，需同步確認 owner 與 group：

```bash
sudo chown -R NE025:www-data /home/NE025/UO_MDR/logs
sudo chown -R NE025:www-data /home/NE025/UO_MDR/task_store
sudo chown -R NE025:www-data /home/NE025/UO_MDR/standard_update_store
sudo chown -R NE025:www-data /home/NE025/UO_MDR/harmonised_store
```

常用檢查：

```bash
test -d /home/NE025/UO_MDR && echo "APP_ROOT ok"
test -d /mnt/nas/project-root && echo "NAS root ok"
test -d /home/NE025/UO_MDR/harmonised_store && echo "fallback folder ok"
test -w /home/NE025/UO_MDR/logs && echo "log dir writable"
test -x /usr/bin/soffice && echo "libreoffice ok"
```
