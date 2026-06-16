# 備份與還原流程

系統資料庫備份、檔案備份、資料庫還原、檔案還原、還原後驗證與常見問題排查流程

## 1. 備份還原範圍

目前備份流程分成兩部分：

- 資料庫備份：MSSQL `.bak`
- 檔案備份：`scripts/backup.sh` 產生的 `.tar.gz` 與 `.sha256`

運行腳本：

```text
scripts/backup_mssql_full.sh
scripts/restore_mssql_replace.sh
scripts/backup.sh
scripts/restore_files.sh
```

重要路徑：

```text
/UO_MDR/backups/files
/UO_MDR/task_store
/UO_MDR/standard_update_store
/UO_MDR/harmonised_store
```

## 2. 備份環境設定

資料庫連線目前使用 SQLAlchemy URL：

```env
DATABASE_URL='mssql+pyodbc://user:password@ip/databasename?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
```

MSSQL `.bak` 備份需要：

```env
MSSQL_BACKUP_DIR='D:\MSSQL\Backup'
```

注意：`MSSQL_BACKUP_DIR` 是 SQL Server 主機看得到，不是 VM 本機路徑。

如果要指定還原檔，需在執行還原前 export：

```bash
export MSSQL_BACKUP_FILE='D:\MSSQL\Backup\regulations_filesystem_prod_2026-06-03_090000_copyonly_full.bak'
```

## 3. 資料庫備份流程

資料庫備份使用：

```bash
scripts/backup_mssql_full.sh
```

此腳本會：

- 自動載入 `.env`
- 從 `DATABASE_URL` 拆出 `SQLCMD_SERVER`、`SQLCMD_USER`、`SQLCMD_PASSWORD`、`MSSQL_DATABASE`
- 執行 `BACKUP DATABASE`：建立 SQL Server 資料庫完整備份
- 使用 `COPY_ONLY`：不影響既有差異備份鏈，適合部署前或臨時備份
- 使用 `COMPRESSION`：壓縮備份檔，降低磁碟空間使用量
- 使用 `CHECKSUM`：備份時檢查資料頁 checksum，提早發現資料或 I/O 問題
- 備份完成後執行 `RESTORE VERIFYONLY WITH CHECKSUM`：驗證備份檔可讀取且 checksum 正常

執行：

```bash
cd /home/NE025/UO_MDR
bash scripts/backup_mssql_full.sh
```

成功時會輸出：

```text
backup_file=D:\MSSQL\Backup\regulations_filesystem_prod_2026-06-03_090000_copyonly_full.bak
```

請記下這個路徑。這個路徑是 SQL Server 主機上的 `.bak` 路徑。

如果要指定檔名：

```bash
BACKUP_FILE_NAME='manual_before_deploy.bak' bash scripts/backup_mssql_full.sh
```

如果 `.env` 內使用的是一般帳號，但備份需要另一個 SQL 帳號，可覆蓋：

```bash
export SQLCMD_USER='backup_user'
export SQLCMD_PASSWORD='password'
bash scripts/backup_mssql_full.sh
```

## 4. 檔案備份流程

檔案備份使用：

```bash
scripts/backup.sh
```

執行：

```bash
cd /home/NE025/UO_MDR
bash scripts/backup.sh
```

備份輸出位置：

```text
/UO_MDR/backups/files/*.tar.gz
/UO_MDR/backups/files/*.tar.gz.sha256
```

檔案備份預設使用輪替保留最新 3 份 archive，可用 `BACKUP_RETENTION_COUNT` 調整保留數量。每次新備份成功後，超過保留數量的舊 `.tar.gz` 與對應 `.sha256` 會被刪除。

備份路徑如下：
- `.env`
- `task_store`
- `standard_update_store`
- `harmonised_store`
- `deploy/systemd`

注意事項：檔案備份不會將 `task_store` 中完整任務檔案、執行結果與 Mapping 相關資料進行備份，會保留任務設定與流程。也就是會排除：
- `task_store/*/files/*`
- `task_store/*/jobs/*`
- `task_store/*/mappings`
- `task_store/*/mapping_job`

其中 `task_store/*/mappings` 是 Mapping 方案設定，`task_store/*/mapping_job` 是 Mapping 執行產物；兩者都會排除，避免還原後 Mapping 方案與附件內容或資料庫狀態不一致。


## 5. 建議備份策略

日常備份範例：

```bash
cd /home/NE025/UO_MDR
bash scripts/backup_mssql_full.sh
bash scripts/backup.sh
```

`uo_regulations_backup.timer`，預設每日 02:00 自動執行上述兩個備份腳本。若要調整排程時間：

```bash
BACKUP_ON_CALENDAR='*-*-* 02:00:00' bash deploy.sh
```

資料庫 `.bak` 預設也保留最新 3 份，可用 `MSSQL_BACKUP_RETENTION_COUNT` 或共用的 `BACKUP_RETENTION_COUNT` 調整。此清理只會在 `MSSQL_BACKUP_DIR` 對執行腳本的主機是可存取目錄時執行；若 `MSSQL_BACKUP_DIR` 是 SQL Server 主機上的 Windows 路徑，需另由 SQL Server 主機或 DBA 維護 `.bak` 清理。

若要立即執行一次定期備份服務：

```bash
sudo systemctl start uo_regulations_backup.service
journalctl -u uo_regulations_backup.service --no-pager -n 100
```

## 6. 完整還原前準備

還原前先確認：

- 要還原的 `.bak` 檔案路徑
- 要還原的 file backup `.tar.gz`
- `.tar.gz.sha256` 是否存在
- SQL Server 上 `.bak` 是否存在且 SQL Server 可讀
- 還原帳號是否有足夠權限
- 目前程式碼版本是否和備份資料庫相容

進入專案：

```bash
cd /home/NE025/UO_MDR
```

停止服務：

```bash
sudo systemctl stop adoption-standard-update.timer
sudo systemctl stop adoption-standard-update.service
sudo systemctl stop uo_regulations_backup.timer
sudo systemctl stop uo_regulations_backup.service
sudo systemctl stop uo_regulations_jobs_worker
sudo systemctl stop uo_regulations_flow_worker
sudo systemctl stop uo_regulations_batch_worker
sudo systemctl stop uo_regulations
```

## 7. 資料庫還原流程

資料庫還原使用：

```bash
scripts/restore_mssql_replace.sh
```

此腳本會：

- 自動載入 `.env`
- 從 `DATABASE_URL` 拆出 `sqlcmd` 連線資訊
- 顯示診斷資訊：
  - `login`
  - `system_user`
  - `is_sysadmin`
  - `is_dbcreator`
  - `target_database_exists`
  - `target_database_state`
- 如果目標 DB 存在，先切 `SINGLE_USER`
- 執行 `RESTORE DATABASE ... WITH REPLACE, RECOVERY, CHECKSUM`
- 還原後切回 `MULTI_USER`
- 如果目標 DB 不存在，會跳過 `SINGLE_USER`，直接從 `.bak` 建立 DB

執行：

```bash
export MSSQL_BACKUP_FILE='D:\MSSQL\Backup\regulations_filesystem_prod_2026-06-03_090000_copyonly_full.bak'
bash scripts/restore_mssql_replace.sh --yes
```

注意：

```text
MSSQL_BACKUP_FILE 是 SQL Server 主機看得到的 .bak 路徑，不是 VM 本機路徑。
```

如果 `.env` 的 app 帳號權限不足，可用 restore 專用帳號覆蓋：

```bash
export SQLCMD_USER='restore_user'
export SQLCMD_PASSWORD='password'
export MSSQL_BACKUP_FILE='D:\MSSQL\Backup\regulations_filesystem_prod_2026-06-03_090000_copyonly_full.bak'
bash scripts/restore_mssql_replace.sh --yes
```

還原帳號建議具備：

- `sysadmin`，或
- 足以執行 `RESTORE DATABASE`、`ALTER DATABASE`、`WITH REPLACE` 的權限

## 8. 檔案還原流程

檔案還原使用：

```bash
scripts/restore_files.sh
```

執行：

```bash
bash scripts/restore_files.sh backups/files/uochcldc01_files_2026-06-03_090100.tar.gz --yes
```

此腳本會：

1. 檢查 `.tar.gz` 是否存在
2. 如果旁邊有 `.sha256`，會驗證 checksum
3. 檢查 tar archive 結構
4. 還原前自動執行 `scripts/backup.sh` 保存目前狀態
5. 清除檔案備份管理的還原範圍：`.env`、`task_store`、`standard_update_store`、`harmonised_store`、`deploy/systemd`
6. 解壓到 `/home/NE025/UO_MDR`

如果要跳過還原前檔案備份：

```bash
SKIP_PRE_RESTORE_BACKUP=1 bash scripts/restore_files.sh backups/files/uochcldc01_files_2026-06-03_090100.tar.gz --yes
```

還原後如果任務頁 404，先檢查：

```bash
find task_store -mindepth 1 -maxdepth 2 -type d | sort
```

任務詳細頁需要：

```text
task_store/<task_id>/files/
```

如果只是缺空目錄，可補回：

```bash
find task_store -mindepth 1 -maxdepth 1 -type d ! -name global_batches \
  -exec sh -c 'for d do mkdir -p "$d/files"; done' sh {} +
```

一般檔案備份會排除 Mapping 方案與 Mapping 執行產物，但 MSSQL `.bak` 是完整資料庫備份，還原後資料庫可能仍保留 Mapping 相關紀錄。若使用一般檔案備份還原，請在 DB 還原與檔案還原後清除 Mapping metadata，避免頁面顯示沒有對應檔案的舊方案或舊執行紀錄：

```bash
.venv/bin/flask --app app.py cleanup-mapping-metadata
.venv/bin/flask --app app.py cleanup-mapping-metadata --yes
```

第一行只會顯示將清除的筆數，第二行才會實際刪除。

## 9. 還原後啟動服務

先跑 schema 檢查：

```bash
.venv/bin/flask --app app.py schema-preflight
```

如果程式碼版本比備份 DB 新，可能需要 migration：

```bash
.venv/bin/alembic upgrade head
```

初始化預設資料：

```bash
.venv/bin/flask --app app.py seed-bootstrap
```

啟動服務：

```bash
sudo systemctl start uo_regulations
sudo systemctl start uo_regulations_jobs_worker
sudo systemctl start uo_regulations_flow_worker
sudo systemctl start uo_regulations_batch_worker
sudo systemctl start uo_regulations_metadata_cleanup.timer
sudo systemctl start uo_regulations_backup.timer
sudo systemctl start adoption-standard-update.timer
```

檢查狀態：

```bash
sudo systemctl status uo_regulations --no-pager
sudo systemctl status uo_regulations_jobs_worker --no-pager
sudo systemctl status uo_regulations_flow_worker --no-pager
sudo systemctl status uo_regulations_batch_worker --no-pager
sudo systemctl status uo_regulations_metadata_cleanup.service --no-pager
sudo systemctl status uo_regulations_metadata_cleanup.timer --no-pager
sudo systemctl status uo_regulations_backup.service --no-pager
sudo systemctl status uo_regulations_backup.timer --no-pager
sudo systemctl status adoption-standard-update.service --no-pager
sudo systemctl status adoption-standard-update.timer --no-pager
```

`adoption-standard-update.service` 是 `oneshot` 服務，平常通常由 `adoption-standard-update.timer` 依排程觸發。若只是恢復排程，不要手動 `start` service；若要立即執行一次標準更新，才手動執行：

```bash
sudo systemctl start adoption-standard-update.service
```

查看最近執行紀錄：

```bash
journalctl -u adoption-standard-update.service --no-pager -n 100
```

## 10. 還原後驗證

至少驗證：

- 可以開登入頁
- 可以登入
- `/tasks` 任務列表正常
- 每個任務詳細頁可進入
- Flow 頁面可進入
- Mapping 頁面可進入
- 檔案瀏覽功能可用
- 背景 worker 沒有持續報錯
- Nginx 正常 proxy 到 Gunicorn

本機檢查 URL：

```bash
curl -i http://127.0.0.1/tasks | sed -n '1,20p'
```

未登入時正常應該看到：

```text
HTTP/1.1 302 FOUND
Location: /auth/login?next=/tasks
```

檢查 DB 目前任務：

```bash
.venv/bin/python - <<'PY'
from modules.env_loader import load_dotenv_if_present
load_dotenv_if_present('/home/NE025/UO_MDR')
from app import create_app
from app.extensions import db
from sqlalchemy import text

app = create_app('production')
with app.app_context():
    with db.engine.connect() as conn:
        print('db_name=' + conn.execute(text('SELECT DB_NAME()')).scalar())
        rows = conn.execute(text('SELECT id, name FROM tasks ORDER BY created_at DESC')).fetchall()
        print('task_count=' + str(len(rows)))
        for row in rows:
            print(f'{row.id}\t{row.name}')
PY
```

檢查 VM 上 task folder：

```bash
find task_store -mindepth 1 -maxdepth 2 -type d | sort
```

## 11. 常見問題排查

### Missing required environment variable: SQLCMD_SERVER

原因：

- 腳本沒有讀到 `.env`
- `.env` 沒有 `DATABASE_URL`
- `DATABASE_URL` 不是 MSSQL URL

處理：

```bash
cd /home/NE025/UO_MDR
rg -n "DATABASE_URL|MSSQL_BACKUP_DIR" .env
bash scripts/backup_mssql_full.sh
```

目前腳本已支援自動從 `.env` 的 `DATABASE_URL` 推導 sqlcmd 連線欄位。

### sqlcmd not found

原因：

- VM 沒有安裝 `sqlcmd`
- `sqlcmd` 不在 `PATH`

處理：

```bash
command -v sqlcmd
```

如果使用自訂位置：

```bash
export SQLCMD_BIN='/path/to/sqlcmd'
```

### BACKUP DATABASE 成功，但檔案不在 VM

這是正常的。

`.bak` 是 SQL Server 服務寫入的，所以：

```text
MSSQL_BACKUP_DIR='D:\MSSQL\Backup'
```

代表 SQL Server 主機上的 `D:\MSSQL\Backup`，不是 VM 上的 `/home/NE025/UO_MDR/backups`。

如果要保存到 VM，需要額外把 `.bak` 從 SQL Server 主機搬回 VM，或讓 SQL Server 寫到共享路徑。

### Msg 5011 / ALTER DATABASE 失敗

如果輸出：

```text
is_sysadmin=0
```

代表還原帳號權限不足。

如果輸出：

```text
is_sysadmin=1
target_database_exists=0
```

代表目標 DB 不存在。現在腳本已支援 DB 不存在時跳過 `SINGLE_USER`，直接 restore 建立 DB。

### 無法開啟登入所要求的資料庫

錯誤類似：

```text
無法開啟登入所要求的資料庫 "regulations_filesystem_prod"。登入失敗。 (4060)
```

可能原因：

- DB 還沒還原完成
- DB 名稱和 `.env` 的 `DATABASE_URL` 不一致
- SQL login 沒有該 DB 權限
- Web/worker 還沒重啟

處理：

```bash
sudo systemctl restart uo_regulations
sudo systemctl restart uo_regulations_jobs_worker
sudo systemctl restart uo_regulations_flow_worker
sudo systemctl restart uo_regulations_batch_worker
```

並確認 `.env` 的 DB 名稱：

```bash
rg -n "DATABASE_URL" .env
```

### 任務列表看得到，但進任務 Not Found

常見原因：

- DB 的 `tasks` 有資料
- 但 VM 上缺少 `task_store/<task_id>/files/`

檢查：

```bash
find task_store -mindepth 1 -maxdepth 2 -type d | sort
```

補回空目錄：

```bash
find task_store -mindepth 1 -maxdepth 1 -type d ! -name global_batches \
  -exec sh -c 'for d do mkdir -p "$d/files"; done' sh {} +
```

如果流程需要 `files/` 裡面的實際原始檔案，需從 NAS、完整備份或原環境補回。

## 12. 建議標準還原作業順序

完整流程：

```bash
cd /home/NE025/UO_MDR

sudo systemctl stop adoption-standard-update.timer
sudo systemctl stop adoption-standard-update.service
sudo systemctl stop uo_regulations_backup.timer
sudo systemctl stop uo_regulations_backup.service
sudo systemctl stop uo_regulations_jobs_worker
sudo systemctl stop uo_regulations_flow_worker
sudo systemctl stop uo_regulations_batch_worker
sudo systemctl stop uo_regulations

bash scripts/backup.sh

export MSSQL_BACKUP_FILE='D:\MSSQL\Backup\regulations_filesystem_prod_2026-06-03_090000_copyonly_full.bak'
bash scripts/restore_mssql_replace.sh --yes

bash scripts/restore_files.sh backups/files/uochcldc01_files_2026-06-03_090100.tar.gz --yes

.venv/bin/flask --app app.py cleanup-mapping-metadata
.venv/bin/flask --app app.py cleanup-mapping-metadata --yes

.venv/bin/flask --app app.py schema-preflight
.venv/bin/flask --app app.py seed-bootstrap

sudo systemctl start uo_regulations
sudo systemctl start uo_regulations_jobs_worker
sudo systemctl start uo_regulations_flow_worker
sudo systemctl start uo_regulations_batch_worker
sudo systemctl start uo_regulations_metadata_cleanup.timer
sudo systemctl start uo_regulations_backup.timer
sudo systemctl start adoption-standard-update.timer

sudo systemctl status uo_regulations --no-pager
curl -i http://127.0.0.1/tasks | sed -n '1,20p'
```
