# 系統部署流程

本文整理 UO MDR 系統的首次環境建立、正式部署、服務檢查與部署後驗證流程。

## 1. 系統組成

目前系統主要由以下部分組成：

- Web App：Flask + Gunicorn
- Web Service：`uo_regulations.service`
- Worker Services：
  - `uo_regulations_jobs_worker.service`
  - `uo_regulations_flow_worker.service`
  - `uo_regulations_batch_worker.service`
- Adoption Standard Update：
  - `adoption-standard-update.service`
  - `adoption-standard-update.timer`
- Database：MSSQL
- File Store：`/home/NE025/UO_MDR/task_store`
- Standard Update Store：`/home/NE025/UO_MDR/standard_update_store`
- Harmonised Store：`/home/NE025/UO_MDR/harmonised_store`

重要路徑：

```text
/home/NE025/UO_MDR
├── .env
├── deploy.sh
├── deploy/
│   ├── nginx.conf.template
│   └── systemd/
├── scripts/
│   ├── backup_mssql_full.sh
│   ├── restore_mssql_replace.sh
│   ├── backup.sh
│   └── restore_files.sh
├── backups/
│   └── files/
├── task_store/
├── standard_update_store/
└── harmonised_store/
```

## 2. 環境設定

正式環境設定放在：

```bash
/home/NE025/UO_MDR/.env
```

資料庫連線目前使用 SQLAlchemy URL：

```env
DATABASE_URL='mssql+pyodbc://user:password@10.30.12.162/regulations_filesystem_prod?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
```

如果部署時要執行資料庫備份，`.env` 也需要設定：

```env
MSSQL_BACKUP_DIR='D:\MSSQL\Backup'
```

注意：`MSSQL_BACKUP_DIR` 是 SQL Server 主機看得到、且 SQL Server service account 有權限寫入的路徑，不是 VM 本機路徑。

## 3. 首次建立 uv 環境

專案使用 `uv` 建立 Python 虛擬環境。`pyproject.toml` 目前指定：

```text
requires-python = "==3.11.4"
```

若系統尚未安裝 `uv`：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

重新載入 shell 確認 `uv` 可用：

```bash
command -v uv
uv --version
```

`deploy.sh` 會執行：

```bash
uv sync --frozen
```

這會依照 `pyproject.toml` 與 `uv.lock` 在 VM 本機建立或更新 `.venv`，並安裝依賴。從 GitHub clone 下來正常不會有 `.venv`，部署時由 `deploy.sh` 建立。`--frozen` 會要求依照既有 `uv.lock` 安裝，不在部署時更新 lock file。

部署後可確認環境：

```bash
.venv/bin/python --version
.venv/bin/python -c "import flask, sqlalchemy, pyodbc; print('python env ok')"
```

預期 Python 版本：

```text
Python 3.11.4
```

如果 `uv sync` 因 Python 版本無法建立環境，可先手動安裝 Python 3.11.4：

```bash
uv python install 3.11.4
bash deploy.sh
```

或改由系統管理員預先安裝 Python 3.11.4，再指定 Python 建立環境：

```bash
uv venv --python /path/to/python3.11 .venv
bash deploy.sh
```

## 4. 部署前檢查

進入專案：

```bash
cd /home/NE025/UO_MDR
```

確認 `.env` 存在：

```bash
test -f .env && echo OK
```

確認 `uv` 可用：

```bash
command -v uv
uv --version
```

如果部署會執行資料庫備份，確認 `sqlcmd` 可用：

```bash
command -v sqlcmd
```

如果顯示 `sqlcmd not found`，需要先安裝 Microsoft SQL Server command-line tools，或設定：

```bash
export SQLCMD_BIN='/path/to/sqlcmd'
```

## 5. 正式部署流程

部署腳本：

```bash
bash deploy.sh
```

部署腳本會做：

1. 進入 `APP_ROOT`，預設 `/home/NE025/UO_MDR`
2. 載入 `.env`
3. 執行 `uv sync --frozen` 建立或同步 Python `.venv`
4. 視設定執行部署前 DB 備份
5. 停止 timer 與主要 worker/web service
6. 安裝或更新 systemd units
7. 視設定更新 Nginx
8. 執行 Alembic migration
9. 執行 schema preflight
10. 初始化預設資料
11. 重啟 service
12. 顯示 service 狀態

常用部署參數：

```bash
RUN_DB_BACKUP=1 bash deploy.sh
```

如果部署目錄不是預設路徑，可用 `APP_ROOT` 指定；systemd `User=` 會預設使用 `APP_ROOT` 的目錄 owner：

```bash
APP_ROOT=/home/NE025/UO_MDR RUN_DB_BACKUP=1 bash deploy.sh
```

如果 systemd 執行使用者要與目錄 owner 不同，可明確指定 `APP_USER`：

```bash
APP_ROOT=/opt/UO_MDR APP_USER=uoapp RUN_DB_BACKUP=1 bash deploy.sh
```

如果部署前要先 git pull：

```bash
RUN_GIT_PULL=1 DEPLOY_BRANCH=main RUN_DB_BACKUP=1 bash deploy.sh
```

如果要同步安裝 Nginx 設定：

```bash
ENABLE_NGINX=1 RUN_DB_BACKUP=1 bash deploy.sh
```

Nginx 分工：

- `/etc/nginx/nginx.conf` 是全域設定，部署腳本不會修改；初次建機或需要調整全域效能參數時手動維護。
- `deploy/nginx.conf.template` 是 UO MDR site config template，`ENABLE_NGINX=1` 時會以 `APP_ROOT` 產生 `build/nginx/uo_regulations`。
- 產生後的 site config 會複製到 `/etc/nginx/sites-available/uo_regulations`，並建立或更新 `/etc/nginx/sites-enabled/uo_regulations` symlink。

Docker 或一般 container 測試環境通常不會以 systemd 作為 PID 1，因此 `deploy.sh`
會自動略過 systemd unit 安裝、服務啟停與狀態檢查，只執行環境同步、
migration、schema preflight 與 seed/bootstrap。也可以明確指定：

```bash
MANAGE_SYSTEMD_SERVICES=0 INSTALL_SYSTEMD_UNITS=0 bash deploy.sh
```

若要測試完整 systemd 部署流程，建議使用 Ubuntu VM。

## 6. 部署後檢查

檢查服務狀態：

```bash
sudo systemctl status uo_regulations --no-pager
sudo systemctl status uo_regulations_jobs_worker --no-pager
sudo systemctl status uo_regulations_flow_worker --no-pager
sudo systemctl status uo_regulations_batch_worker --no-pager
sudo systemctl status adoption-standard-update.service --no-pager
sudo systemctl status adoption-standard-update.timer --no-pager
```

Schema 檢查：

```bash
.venv/bin/flask --app app.py schema-preflight
```

初始化預設資料：

```bash
.venv/bin/flask --app app.py seed-bootstrap
```

確認 `.venv` 已由 `deploy.sh` 建立：

```bash
test -x .venv/bin/python && .venv/bin/python --version
test -x .venv/bin/alembic && .venv/bin/alembic --version
```

本機檢查 URL：

```bash
curl -i http://127.0.0.1/tasks | sed -n '1,20p'
```

未登入時正常應該看到：

```text
HTTP/1.1 302 FOUND
Location: /auth/login?next=/tasks
```

檢查 Nginx：

```bash
sudo nginx -t
ls -l /etc/nginx/sites-enabled
sudo sed -n '1,120p' /etc/nginx/sites-available/uo_regulations
```

`adoption-standard-update.service` 是 `oneshot` 服務，平常通常由 `adoption-standard-update.timer` 依排程觸發。若只是恢復排程，不要手動 `start` service；若要立即執行一次標準更新，才手動執行：

```bash
sudo systemctl start adoption-standard-update.service
```

查看最近執行紀錄：

```bash
journalctl -u adoption-standard-update.service --no-pager -n 100
```

## 7. 部署常見問題

### `.venv/bin/python` 不存在

代表 `deploy.sh` 執行 `uv sync --frozen` 時沒有成功建立 `.venv`。先確認 `uv` 可用，並查看部署輸出中的 `uv sync` 錯誤。

### `DATABASE_URL is empty after loading .env`

確認 `.env` 內有：

```bash
rg -n "DATABASE_URL" .env
```

### `sqlcmd not found`

如果部署時有 `RUN_DB_BACKUP=1`，需要安裝 `sqlcmd` 或指定：

```bash
export SQLCMD_BIN='/path/to/sqlcmd'
```

### Nginx 回 404

確認 enabled site：

```bash
ls -l /etc/nginx/sites-enabled
```

確認全域 `/etc/nginx/nginx.conf` 有載入 enabled sites：

```nginx
include /etc/nginx/sites-enabled/*;
```

確認 Nginx proxy：

```bash
sudo nginx -t
sudo systemctl reload nginx
```

直接測本機：

```bash
curl -i http://127.0.0.1/tasks | sed -n '1,20p'
```
