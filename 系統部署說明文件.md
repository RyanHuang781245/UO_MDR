# 系統部署說明

說明系統於 Ubuntu 上的部署架構、部署流程、systemd 服務、Nginx 反向代理、啟停操作、部署驗證與故障排除。系統環境需求、第三方軟體、`.env` 與路徑設定請參考 [SYSTEM_ENVIRONMENT.md](./SYSTEM_ENVIRONMENT.md)；第三方軟體詳細安裝流程請參考 [THIRD_PARTY_SOFTWARE.md](./THIRD_PARTY_SOFTWARE.md)。

- [系統部署說明](#系統部署說明)
  - [1. 部署架構概述](#1-部署架構概述)
  - [2. 專案目錄結構](#2-專案目錄結構)
  - [3. 後端服務部署流程](#3-後端服務部署流程)
    - [deploy.sh 參數說明](#deploysh-參數說明)
      - [基本路徑與執行使用者](#基本路徑與執行使用者)
      - [Git 與程式碼更新](#git-與程式碼更新)
      - [Python / uv](#python--uv)
      - [Noto CJK 字體](#noto-cjk-字體)
      - [systemd 管理](#systemd-管理)
      - [systemd timer 排程](#systemd-timer-排程)
      - [Nginx 設定](#nginx-設定)
      - [資料庫 migration 與備份](#資料庫-migration-與備份)
      - [部署腳本執行順序](#部署腳本執行順序)
      - [常用部署組合](#常用部署組合)
  - [4. Gunicorn 與 systemd 服務設定](#4-gunicorn-與-systemd-服務設定)
    - [Web 服務](#web-服務)
    - [任務 worker 服務](#任務-worker-服務)
    - [歐盟採認標準更新服務與 timer](#歐盟採認標準更新服務與-timer)
    - [Metadata 清理服務與 timer](#metadata-清理服務與-timer)
    - [備份服務與 timer](#備份服務與-timer)
  - [5. Nginx 反向代理設定](#5-nginx-反向代理設定)
  - [6. 系統啟動、重啟與狀態檢查](#6-系統啟動重啟與狀態檢查)
  - [7. 部署驗證](#7-部署驗證)
  - [8. 維護與故障排除](#8-維護與故障排除)
    - [502 Bad Gateway](#502-bad-gateway)
    - [Permission denied](#permission-denied)
    - [Socket 權限錯誤](#socket-權限錯誤)
    - [環境變數未載入](#環境變數未載入)
    - [資料庫連線失敗](#資料庫連線失敗)
    - [NAS 權限不足或路徑無法存取](#nas-權限不足或路徑無法存取)
    - [Nginx 回傳 404](#nginx-回傳-404)
    - [背景任務未執行](#背景任務未執行)

## 1. 部署架構概述

系統採用 Nginx、Gunicorn、Flask App 與 MSSQL 分層部署。使用者透過瀏覽器連線至 Nginx，Nginx 負責接收 HTTP request、提供靜態檔案，並將動態請求透過 Unix Socket 轉發至 Gunicorn。Gunicorn 載入 `wsgi.py` 中的 Flask application object，Flask App 再依功能存取 MSSQL、NAS 掛載路徑與本機任務資料夾。

```text
使用者瀏覽器
    |
    | HTTP Request
    v
Nginx
    |-- /static/ -> static/
    |
    | reverse proxy
    v
Gunicorn Unix Socket
    |
    | WSGI
    v
Flask App
    |-- SQLAlchemy / pyodbc -> MSSQL
    |-- task_store/ -> 任務檔案與輸出結果
    |-- standard_update_store/ -> 標準更新資料
    |-- harmonised_store/ -> 歐盟採認標準資料
    |-- NAS 掛載路徑 -> 檔案來源
```

## 2. 專案目錄結構

專案主要目錄與檔案用途如下：

- `app.py`：Flask 開發入口，建立 Flask App；可供本機開發或簡易測試使用。
- `wsgi.py`：Gunicorn 使用的 WSGI 入口，匯入 `create_app()` 並建立 `app` 物件。
- `app/`：Flask 應用程式主要程式碼，包含 blueprints、models、services、jobs、templates 與系統設定。
- `app/config.py`：應用程式設定檔，負責讀取環境變數並設定資料庫、Secret Key、NAS 與檔案存放路徑等項目。
- `static/`：前端靜態資源目錄，由 Nginx 直接提供。
- `migrations/`：Alembic migration 檔案，用於管理資料庫 schema 版本。
- `pyproject.toml`：Python 專案設定與依賴套件宣告。
- `uv.lock`：鎖定 Python 套件版本，部署時用於建立可重現的虛擬環境。
- `.venv/`：Python 虛擬環境目錄，部署時由 `uv sync --frozen` 建立或更新。
- `.env`：部署環境變數檔案。變數內容請參考 [SYSTEM_ENVIRONMENT.md](./SYSTEM_ENVIRONMENT.md)。
- `deploy.sh`：正式部署腳本，負責同步虛擬環境、安裝 systemd unit、執行 migration、重啟服務與選擇性安裝 Nginx 設定。
- `deploy/systemd/*.service.template`：systemd service 範本，部署時會產生實際 unit 檔。
- `deploy/systemd/*.timer.template`：systemd timer 範本，用於排程備份、metadata 清理或標準更新作業。
- `build/systemd/`：部署腳本產生的 systemd unit 檔輸出目錄。
- `deploy/nginx-site.conf.template`：Nginx site 設定範本。
- `scripts/install_nginx_site.sh`：安裝 Nginx site 設定的輔助腳本。
- `scripts/backup_mssql_full.sh`：MSSQL 完整備份腳本。
- `scripts/backup.sh`：檔案備份腳本。
- `scripts/restore_mssql_replace.sh`：MSSQL 還原腳本。
- `scripts/restore_files.sh`：檔案備份還原腳本。
- `task_store/`：任務資料與輸出檔案存放目錄。
- `standard_update_store/`：標準更新資料存放目錄。
- `harmonised_store/`：歐盟採認標準相關資料存放目錄。
- `logs/`：系統執行過程中可能產生的日誌或輔助輸出目錄。

## 3. 後端服務部署流程

部署前請先完成 [SYSTEM_ENVIRONMENT.md](./SYSTEM_ENVIRONMENT.md) 所列環境、第三方軟體、`.env` 與路徑權限設定。

確認專案與 `.env`：

```bash
cd /home/NE025/UO_MDR
test -f .env && echo OK
```

同步 Python 虛擬環境：

```bash
uv sync --frozen
```

確認 Python 環境：

```bash
.venv/bin/python --version
.venv/bin/python -c "import flask, sqlalchemy, pyodbc; print('python env ok')"
```

在啟動 systemd 服務前，可先以 Flask 或 Gunicorn 進行基本啟動測試：

```bash
.venv/bin/flask --app app.py --debug run
```

或使用 Gunicorn 測試 WSGI 入口：

```bash
.venv/bin/gunicorn --workers 2 --worker-class gthread --threads 4 --timeout 300 --bind unix:uo_regulations.sock wsgi:app
```

正式部署：

```bash
bash deploy.sh
```

若部署目錄不是預設路徑，應明確指定：

```bash
APP_ROOT=/opt/UO_MDR ENV_FILE=/opt/UO_MDR/.env bash deploy.sh
```

若要同步安裝 Nginx 設定：

```bash
ENABLE_NGINX=1 bash deploy.sh
```

部署腳本會依設定執行虛擬環境同步、systemd unit 安裝、資料庫 migration、schema preflight、預設資料初始化、服務重啟，以及選擇性 Nginx 設定安裝。

### deploy.sh 參數說明

`deploy.sh` 以環境變數控制部署行為，使用方式為在指令前指定變數：

```bash
參數名稱=參數值 bash deploy.sh
```

例如：

```bash
RUN_DB_BACKUP=1 ENABLE_NGINX=1 bash deploy.sh
```

#### 基本路徑與執行使用者

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `APP_DIR` | `/home/NE025/UO_MDR` | 舊版相容用的專案目錄預設值。若未設定 `APP_ROOT`，會以此值作為專案根目錄。 |
| `APP_ROOT` | `$APP_DIR` | 專案根目錄。部署腳本會 `cd` 到此目錄，並以此路徑產生 systemd、Nginx 與備份相關設定。 |
| `ENV_FILE` | `$APP_ROOT/.env` | `.env` 路徑。systemd units 也會使用此檔作為 `EnvironmentFile`。 |
| `APP_USER` | 空值 | 指定 systemd `User=`。若未設定，`scripts/install_systemd_units.sh` 會使用 `APP_ROOT` 的目錄 owner。 |

常見用法：

```bash
APP_ROOT=/opt/UO_MDR ENV_FILE=/opt/UO_MDR/.env APP_USER=uoapp bash deploy.sh
```

#### Git 與程式碼更新

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `RUN_GIT_PULL` | `0` | 是否在部署前執行 `git pull`。設為 `1` 時會執行 `git pull origin $DEPLOY_BRANCH`。 |
| `DEPLOY_BRANCH` | `main` | `RUN_GIT_PULL=1` 時使用的 branch 名稱。 |

範例：

```bash
RUN_GIT_PULL=1 DEPLOY_BRANCH=main bash deploy.sh
```

#### Python / uv

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `UV_BIN` | `uv` | `uv` 指令名稱或完整路徑。若 systemd 或 shell 找不到 `uv`，可指定完整路徑。 |
| `UV_SYNC_ARGS` | `--frozen` | 傳給 `uv sync` 的參數。預設要求依照 `uv.lock` 安裝，不更新 lock file。 |

範例：

```bash
UV_BIN=/home/NE025/.local/bin/uv UV_SYNC_ARGS='--frozen' bash deploy.sh
```

#### Noto CJK 字體

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `INSTALL_NOTO_CJK_FONTS` | `1` | 是否執行 `scripts/install_noto_cjk_fonts.sh` 安裝或更新繁中文字體。 |
| `INSTALL_NOTO_CJK_FONTS_FORCE` | `0` | 是否強制重新下載 Noto CJK 字體。設為 `1` 時會傳入 `--force`。 |
| `NOTO_CJK_FONTS_DIR` | 空值 | 指定字體安裝目錄。設值時會傳入 `--install-dir`。 |

詳細字體下載流程請參考 [THIRD_PARTY_SOFTWARE.md](./THIRD_PARTY_SOFTWARE.md)。

範例：

```bash
INSTALL_NOTO_CJK_FONTS=0 bash deploy.sh
INSTALL_NOTO_CJK_FONTS_FORCE=1 bash deploy.sh
NOTO_CJK_FONTS_DIR=/home/NE025/.local/share/fonts/noto-cjk bash deploy.sh
```

#### systemd 管理

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `MANAGE_SYSTEMD_SERVICES` | `auto` | 控制是否管理 systemd 服務。`auto` 會偵測目前環境是否可用 systemd；也可設為 `1` / `true` / `yes` 強制啟用，或 `0` / `false` / `no` 停用。 |
| `INSTALL_SYSTEMD_UNITS` | `1` | 是否產生並安裝 systemd unit files。需 systemd 可用才會執行。 |
| `ENABLE_SYSTEMD_UNITS` | `1` | 是否執行 `systemctl enable`，讓 Web、worker 與 timer 開機自動啟動。 |
| `WEB_WORKERS` | `2` | Gunicorn worker 數量，會寫入 `uo_regulations.service`。 |
| `WEB_BIND` | `unix:uo_regulations.sock` | Gunicorn bind 位置。預設使用專案目錄下的 Unix Socket。 |

範例：

```bash
WEB_WORKERS=4 bash deploy.sh
WEB_BIND=unix:uo_regulations.sock bash deploy.sh
ENABLE_SYSTEMD_UNITS=0 bash deploy.sh
MANAGE_SYSTEMD_SERVICES=0 bash deploy.sh
```

#### systemd timer 排程

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `UPDATE_ON_CALENDAR` | `daily` | `adoption-standard-update.timer` 的 `OnCalendar`。 |
| `CLEANUP_ON_CALENDAR` | `*-*-* 03:30:00` | `uo_regulations_metadata_cleanup.timer` 的 `OnCalendar`。 |
| `BACKUP_ON_CALENDAR` | `*-*-* 02:00:00` | `uo_regulations_backup.timer` 的 `OnCalendar`。 |

實際部署時，timer 排程以 `.env` 中載入後的值為準。`deploy.sh` 會先設定上述預設值，再載入 `ENV_FILE`；因此若 `.env` 已定義 `UPDATE_ON_CALENDAR`、`CLEANUP_ON_CALENDAR` 或 `BACKUP_ON_CALENDAR`，會以 `.env` 內容覆蓋腳本預設值。

`.env` 設定範例：

```env
UPDATE_ON_CALENDAR="*-*-* 08:00:00"
CLEANUP_ON_CALENDAR="*-*-* 23:00:00"
BACKUP_ON_CALENDAR="*-*-* 23:00:00"
```

值需符合 systemd `OnCalendar` 格式；若值包含空白，必須在 `.env` 中加上引號。修改 `.env` 後需重新執行部署，讓 systemd timer unit 重新產生並安裝：

```bash
bash deploy.sh
```

若 `.env` 未定義這些變數，才會使用 `deploy.sh` 的預設值，或可由執行時環境變數指定：

```bash
UPDATE_ON_CALENDAR='*-*-* 08:00:00' bash deploy.sh
CLEANUP_ON_CALENDAR='*-*-* 23:00:00' bash deploy.sh
BACKUP_ON_CALENDAR='*-*-* 02:00:00' bash deploy.sh
```

#### Nginx 設定

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `ENABLE_NGINX` | `1` | 是否安裝或更新 Nginx site config。 |
| `NGINX_TEMPLATE` | `$APP_ROOT/deploy/nginx-site.conf.template` | Nginx site template 路徑。 |
| `NGINX_SITE_NAME` | `uo_regulations` | 安裝到 `/etc/nginx/sites-available/` 與 `/etc/nginx/sites-enabled/` 的 site 名稱。 |
| `NGINX_FILE` | `$APP_ROOT/build/nginx/$NGINX_SITE_NAME` | 渲染後的 Nginx 設定輸出檔。 |
| `DISABLE_NGINX_DEFAULT_SITE` | `1` | 是否刪除 default site symlink，避免 request 被預設站台接走。 |
| `NGINX_DEFAULT_SITE_LINK` | `/etc/nginx/sites-enabled/default` | default site symlink 路徑。 |

範例：

```bash
ENABLE_NGINX=1 bash deploy.sh
ENABLE_NGINX=0 bash deploy.sh
DISABLE_NGINX_DEFAULT_SITE=0 bash deploy.sh
NGINX_SITE_NAME=uo_regulations NGINX_FILE=/tmp/uo_regulations bash deploy.sh
```

#### 資料庫 migration 與備份

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `RUN_DB_BACKUP` | `0` | 是否在部署前執行 `scripts/backup_mssql_full.sh`。設為 `1` 時會先做 MSSQL full backup。 |
| `ALEMBIC_CONFIG_NAME` | `production` | Alembic 設定名稱，由部署腳本 export 後供 migration 使用。 |
| `ALEMBIC_DATABASE_URL` | `$DATABASE_URL` | Alembic migration 使用的資料庫連線字串。若未設定，預設使用 `.env` 中的 `DATABASE_URL`。 |

`RUN_DB_BACKUP=1` 時，`scripts/backup_mssql_full.sh` 會載入 `.env`，並可由 `DATABASE_URL` 推導 `sqlcmd` 所需連線欄位。仍需確認 `MSSQL_BACKUP_DIR` 是 SQL Server 主機可寫入的位置。

範例：

```bash
RUN_DB_BACKUP=1 bash deploy.sh
ALEMBIC_DATABASE_URL='mssql+pyodbc://user:password@host/database?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes' bash deploy.sh
```

#### 部署腳本執行順序

`deploy.sh` 主要流程如下：

1. 進入 `APP_ROOT`。
2. 載入 `ENV_FILE`。
3. 檢查 `DATABASE_URL` 與 `ALEMBIC_DATABASE_URL`。
4. 視 `RUN_GIT_PULL` 決定是否更新程式碼。
5. 執行 `uv sync $UV_SYNC_ARGS`。
6. 視設定安裝 Noto CJK 字體。
7. 偵測或套用 systemd 管理模式。
8. 視 `RUN_DB_BACKUP` 決定是否執行部署前資料庫備份。
9. 停止 timer、worker 與 Web service。
10. 視設定安裝 systemd units 並 enable。
11. 視 `ENABLE_NGINX` 決定是否安裝 Nginx site config。
12. 執行 Alembic migration。
13. 執行 schema preflight。
14. 初始化預設資料。
15. 重啟 Web、worker 與 timer。
16. 顯示服務狀態。

#### 常用部署組合

一般正式部署：

```bash
bash deploy.sh
```

部署前先更新程式碼：

```bash
RUN_GIT_PULL=1 DEPLOY_BRANCH=main bash deploy.sh
```

部署前先備份資料庫：

```bash
RUN_DB_BACKUP=1 bash deploy.sh
```

完整部署，包含 git pull、DB 備份與 Nginx 設定：

```bash
RUN_GIT_PULL=1 DEPLOY_BRANCH=main RUN_DB_BACKUP=1 ENABLE_NGINX=1 bash deploy.sh
```

只部署應用，不管理 systemd 與 Nginx：

```bash
MANAGE_SYSTEMD_SERVICES=0 ENABLE_NGINX=0 bash deploy.sh
```

調整 Gunicorn worker 與排程：

```bash
WEB_WORKERS=4 UPDATE_ON_CALENDAR='*-*-* 08:00:00' CLEANUP_ON_CALENDAR='*-*-* 23:00:00' BACKUP_ON_CALENDAR='*-*-* 02:00:00' bash deploy.sh
```

## 4. Gunicorn 與 systemd 服務設定

本系統使用 systemd 管理 Web 服務、背景 worker、排程清理、備份與標準更新作業。systemd unit 範本位於：

```text
deploy/systemd/
```

所有 systemd unit 如下：

| Unit 名稱 | 類型 | 用途 |
| --- | --- | --- |
| `uo_regulations.service` | long-running service | 啟動 Gunicorn，提供 Flask Web 系統服務。 |
| `uo_regulations_jobs_worker.service` | long-running service | 啟動一般任務 worker，處理 `default`、`light`、`heavy` queue。 |
| `uo_regulations_flow_worker.service` | long-running service | 啟動流程任務 worker，處理 `flow` queue。 |
| `uo_regulations_batch_worker.service` | long-running service | 啟動批次任務 worker，處理 `batch` queue，並以較低 CPU / I/O 優先權執行。 |
| `uo_regulations_metadata_cleanup.service` | oneshot service | 執行 metadata 清理，包含 mapping check 暫存資料、系統錯誤紀錄與 audit log 清理。 |
| `uo_regulations_metadata_cleanup.timer` | timer | 依排程觸發 `uo_regulations_metadata_cleanup.service`。 |
| `uo_regulations_backup.service` | oneshot service | 執行排程備份，依序備份 MSSQL 資料庫與流程管理中的流程檔案，不包含輸入與輸出檔案。 |
| `uo_regulations_backup.timer` | timer | 依排程觸發 `uo_regulations_backup.service`。 |
| `adoption-standard-update.service` | oneshot service | 執行採認標準更新作業。 |
| `adoption-standard-update.timer` | timer | 依排程觸發 `adoption-standard-update.service`。 |

### Web 服務

`uo_regulations.service` 是主要 Web 服務，負責啟動 Gunicorn 並載入 Flask App。範本位於：

```text
deploy/systemd/uo_regulations.service.template
```

核心設定範例：

```ini
[Service]
User=NE025
Group=www-data
WorkingDirectory=/home/NE025/UO_MDR
EnvironmentFile=/home/NE025/UO_MDR/.env
Environment="PATH=/home/NE025/UO_MDR/.venv/bin:..."
ExecStart=/home/NE025/UO_MDR/.venv/bin/gunicorn --workers 2 --worker-class gthread --threads 4 --timeout 300 --bind unix:uo_regulations.sock --user NE025 --group www-data -m 007 --error-logfile - wsgi:app
Restart=always
RestartSec=5
```

重要設定說明：

- `User` / `Group`：指定服務執行身分，需具備專案目錄與資料夾讀寫權限。
- `WorkingDirectory`：指定 Gunicorn 啟動時的工作目錄。
- `EnvironmentFile`：指定 systemd 啟動服務時載入的 `.env`。
- `ExecStart`：定義 Gunicorn 啟動命令與 WSGI 入口 `wsgi:app`。
- `--bind unix:uo_regulations.sock`：指定 Gunicorn 監聽 Unix Socket。
- `-m 007`：設定 socket 建立時的 umask，使同群組服務可存取 socket。
- `Restart=always`：服務異常結束時自動重啟。

### 任務 worker 服務

`uo_regulations_jobs_worker.service` 處理一般任務：

```ini
ExecStart=/home/NE025/UO_MDR/.venv/bin/flask --app app.py jobs-worker --queue default --queue light --queue heavy
Restart=always
RestartSec=5
```

`uo_regulations_flow_worker.service` 處理流程任務：

```ini
ExecStart=/home/NE025/UO_MDR/.venv/bin/flask --app app.py jobs-worker --queue flow
Restart=always
RestartSec=5
```

`uo_regulations_batch_worker.service` 處理批次任務：

```ini
Nice=10
CPUWeight=20
IOSchedulingClass=best-effort
IOSchedulingPriority=7
ExecStart=/home/NE025/UO_MDR/.venv/bin/flask --app app.py jobs-worker --queue batch
Restart=always
RestartSec=5
```

### 歐盟採認標準更新服務與 timer

`adoption-standard-update.service` 為 `oneshot` 服務：

```ini
Type=oneshot
ExecStart=/home/NE025/UO_MDR/.venv/bin/python -m app.jobs.adoption_standard_update
StandardOutput=journal
StandardError=journal
```

`adoption-standard-update.timer` 用於定期觸發：

```ini
[Timer]
OnCalendar={{UPDATE_ON_CALENDAR}}
Persistent=true
```

### Metadata 清理服務與 timer

`uo_regulations_metadata_cleanup.service` 為 `oneshot` 服務：

```ini
Type=oneshot
ExecStart=/home/NE025/UO_MDR/.venv/bin/flask --app app.py mapping-check-cleanup
ExecStart=/home/NE025/UO_MDR/.venv/bin/flask --app app.py system-error-cleanup
ExecStart=/home/NE025/UO_MDR/.venv/bin/flask --app app.py audit-cleanup
StandardOutput=journal
StandardError=journal
```

`uo_regulations_metadata_cleanup.timer` 用於定期觸發：

```ini
[Timer]
OnCalendar={{CLEANUP_ON_CALENDAR}}
Persistent=true
```

### 備份服務與 timer

`uo_regulations_backup.service` 為 `oneshot` 服務：

```ini
Type=oneshot
ExecStart=/usr/bin/env bash /home/NE025/UO_MDR/scripts/backup_mssql_full.sh
ExecStart=/usr/bin/env bash /home/NE025/UO_MDR/scripts/backup.sh
StandardOutput=journal
StandardError=journal
```

`uo_regulations_backup.timer` 用於定期觸發：

```ini
[Timer]
OnCalendar={{BACKUP_ON_CALENDAR}}
Persistent=true
```

## 5. Nginx 反向代理設定

Nginx site 設定範本位於：

```text
deploy/nginx-site.conf.template
```

主要設定如下：

```nginx
server {
    listen 80;
    server_name _;

    location /static/ {
        alias /home/NE025/UO_MDR/static/;
        access_log off;
        expires 7d;
        add_header Cache-Control "public";
    }

    location / {
        include proxy_params;
        proxy_pass http://unix:/home/NE025/UO_MDR/uo_regulations.sock;

        proxy_connect_timeout 30s;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }
}
```

Nginx 接收 HTTP Request 後，會依照 `location` 規則處理：

- `/static/`：由 Nginx 直接讀取專案 `static/` 目錄。
- `/`：其餘動態請求透過 `proxy_pass` 轉發至 Gunicorn Unix Socket。

若透過部署腳本安裝 Nginx site：

```bash
ENABLE_NGINX=1 bash deploy.sh
```

完成後檢查並 reload：

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 6. 系統啟動、重啟與狀態檢查

Web 服務常用指令：

```bash
sudo systemctl status uo_regulations --no-pager
sudo systemctl start uo_regulations
sudo systemctl stop uo_regulations
sudo systemctl restart uo_regulations
sudo systemctl enable uo_regulations
```

查看 Web 服務日誌：

```bash
journalctl -u uo_regulations --no-pager -n 100
journalctl -u uo_regulations -f
```

所有 service 狀態檢查：

```bash
sudo systemctl status uo_regulations --no-pager
sudo systemctl status uo_regulations_jobs_worker --no-pager
sudo systemctl status uo_regulations_flow_worker --no-pager
sudo systemctl status uo_regulations_batch_worker --no-pager
sudo systemctl status uo_regulations_metadata_cleanup.service --no-pager
sudo systemctl status uo_regulations_backup.service --no-pager
sudo systemctl status adoption-standard-update.service --no-pager
```

所有 timer 狀態檢查：

```bash
sudo systemctl status uo_regulations_metadata_cleanup.timer --no-pager
sudo systemctl status uo_regulations_backup.timer --no-pager
sudo systemctl status adoption-standard-update.timer --no-pager
```

啟動或重啟所有長駐服務：

```bash
sudo systemctl restart uo_regulations
sudo systemctl restart uo_regulations_jobs_worker
sudo systemctl restart uo_regulations_flow_worker
sudo systemctl restart uo_regulations_batch_worker
```

啟用 timer 開機自動排程：

```bash
sudo systemctl enable --now uo_regulations_metadata_cleanup.timer
sudo systemctl enable --now uo_regulations_backup.timer
sudo systemctl enable --now adoption-standard-update.timer
```

手動執行 oneshot service：

```bash
sudo systemctl start uo_regulations_metadata_cleanup.service
sudo systemctl start uo_regulations_backup.service
sudo systemctl start adoption-standard-update.service
```

查看背景服務與排程執行紀錄：

```bash
journalctl -u uo_regulations_jobs_worker --no-pager -n 100
journalctl -u uo_regulations_flow_worker --no-pager -n 100
journalctl -u uo_regulations_batch_worker --no-pager -n 100
journalctl -u uo_regulations_backup.service --no-pager -n 100
journalctl -u uo_regulations_metadata_cleanup.service --no-pager -n 100
journalctl -u adoption-standard-update.service --no-pager -n 100
```

Nginx 常用指令：

```bash
sudo nginx -t
sudo systemctl status nginx --no-pager
sudo systemctl reload nginx
sudo systemctl restart nginx
```

確認 socket 與資料夾權限：

```bash
ls -l /home/NE025/UO_MDR/uo_regulations.sock
ls -ld /home/NE025/UO_MDR/task_store
ls -ld /home/NE025/UO_MDR/standard_update_store
ls -ld /home/NE025/UO_MDR/harmonised_store
```

## 7. 部署驗證

部署完成後，應依序進行下列驗證。

確認 systemd 服務：

```bash
sudo systemctl status uo_regulations --no-pager
```

確認 Gunicorn socket：

```bash
ls -l /home/NE025/UO_MDR/uo_regulations.sock
```

確認 Nginx：

```bash
sudo nginx -t
sudo systemctl reload nginx
```

確認網站可連線：

```bash
curl -i http://127.0.0.1/tasks | sed -n '1,20p'
```

若尚未登入，正常情況可能會回傳 `302 FOUND` 並導向登入頁：

```text
HTTP/1.1 302 FOUND
Location: /auth/login?next=/tasks
```

確認資料庫連線：

```bash
.venv/bin/flask --app app.py schema-preflight
```

若需要初始化或確認預設資料：

```bash
.venv/bin/flask --app app.py seed-bootstrap
```

確認功能：

- 登入頁可正常顯示，帳號可登入。
- 未授權帳號無法存取受限制頁面。
- `.env` 中設定的 NAS 根目錄存在且可讀取。
- 建立測試任務後，任務輸出可寫入 `task_store/`。
- 文件處理或標準對應流程可由 worker 正常處理。
- `journalctl` 未出現未處理例外。

## 8. 維護與故障排除

### 502 Bad Gateway

可能原因：

- Gunicorn 服務未啟動。
- Gunicorn socket 不存在。
- Nginx 無權限存取 socket。
- Nginx `proxy_pass` 指向錯誤 socket 路徑。
- Flask App 啟動時發生例外。

檢查方式：

```bash
sudo systemctl status uo_regulations --no-pager
journalctl -u uo_regulations --no-pager -n 100
ls -l /home/NE025/UO_MDR/uo_regulations.sock
sudo nginx -t
```

### Permission denied

可能原因：

- systemd 服務執行使用者無權限讀寫專案目錄。
- `task_store/`、`standard_update_store/`、`harmonised_store/` 權限不足。
- NAS 掛載目錄權限不足。
- `.env` 權限過於嚴格，導致 systemd 服務使用者無法讀取。

檢查方式：

```bash
id NE025
ls -ld /home/NE025/UO_MDR
ls -ld /home/NE025/UO_MDR/task_store
ls -ld /home/NE025/UO_MDR/standard_update_store
ls -ld /home/NE025/UO_MDR/harmonised_store
ls -l /home/NE025/UO_MDR/.env
```

### Socket 權限錯誤

可能原因：

- Gunicorn 建立 socket 時權限不足。
- Nginx worker 使用者不在可讀寫 socket 的群組。
- service 設定中的 `Group` 或 `-m 007` 未正確套用。

檢查方式：

```bash
ls -l /home/NE025/UO_MDR/uo_regulations.sock
ps -ef | rg "nginx|gunicorn"
```

### 環境變數未載入

可能原因：

- `.env` 不存在或路徑與 `EnvironmentFile` 不一致。
- `.env` 格式錯誤。
- 調整 `.env` 後未重啟服務。
- systemd service 未重新載入更新後的 unit。

檢查方式：

```bash
test -f /home/NE025/UO_MDR/.env && echo OK
sudo systemctl cat uo_regulations
journalctl -u uo_regulations --no-pager -n 100
```

修正 unit 後需重新載入：

```bash
sudo systemctl daemon-reload
sudo systemctl restart uo_regulations
```

### 資料庫連線失敗

可能原因：

- `DATABASE_URL` 未設定或格式錯誤。
- 資料庫帳號或密碼錯誤。
- SQL Server 主機或 Port 無法連線。
- ODBC Driver 未安裝或版本不符。
- 資料庫權限不足。

檢查方式：

```bash
rg -n "DATABASE_URL|RBAC_DATABASE_URL" /home/NE025/UO_MDR/.env
.venv/bin/flask --app app.py schema-preflight
journalctl -u uo_regulations --no-pager -n 100
```

### NAS 權限不足或路徑無法存取

可能原因：

- NAS 尚未掛載。
- `.env` 中 NAS 根目錄設定錯誤。
- systemd 服務使用者沒有 NAS 目錄讀寫權限。
- NAS 掛載使用的帳號權限不足。

檢查方式：

```bash
mount | rg "nas|cifs|nfs"
ls -ld /mnt/nas
rg -n "ALLOWED_NAS_ROOTS|NAS_MAX_COPY_FILE_SIZE_MB" /home/NE025/UO_MDR/.env
```

### Nginx 回傳 404

可能原因：

- Nginx 未啟用正確 site。
- 預設站台攔截請求。
- `/etc/nginx/nginx.conf` 未載入 `sites-enabled/*`。
- 請求路徑不存在或未被 Flask route 處理。

檢查方式：

```bash
ls -l /etc/nginx/sites-enabled
sudo nginx -t
sudo sed -n '1,120p' /etc/nginx/sites-available/uo_regulations
curl -i http://127.0.0.1/tasks | sed -n '1,20p'
```

必要時停用 default site：

```bash
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

### 背景任務未執行

可能原因：

- worker service 未啟動。
- timer 未啟用。
- 任務資料或資料庫狀態異常。
- worker 無權限讀寫任務檔案。

檢查方式：

```bash
sudo systemctl status uo_regulations_jobs_worker --no-pager
sudo systemctl status uo_regulations_flow_worker --no-pager
sudo systemctl status uo_regulations_batch_worker --no-pager
systemctl list-timers | rg "uo_regulations|adoption"
journalctl -u uo_regulations_jobs_worker --no-pager -n 100
```
