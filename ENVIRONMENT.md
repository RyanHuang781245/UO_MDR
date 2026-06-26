# 環境變數說明

本文件整理目前 `.env` 使用的參數用途。不要把正式密碼、API key、DB 連線字串貼到文件或 commit 內容中。

## Flask / App

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `SECRET_KEY` | Flask session 與簽章用密鑰。 | 敏感資訊。正式環境不可使用預設值。 |
| `SESSION_COOKIE_NAME` | 瀏覽器 session cookie 名稱。 | 用於避免與其他系統 cookie 衝突。 |
| `AUTO_SCHEMA_MANAGEMENT` | 是否允許 app 啟動時自動建立或補 schema。 | 正式環境建議 `0`，改由 migration 與 `schema-preflight` 控制。 |
| `APP_ENV` | App 執行環境。 | 例如 `production`、`development`。 |

## 資料庫

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `DATABASE_URL` | SQLAlchemy 資料庫連線字串。 | 敏感資訊，包含 DB 帳密與連線位置。 |
| `MSSQL_QUERY_TIMEOUT` | MSSQL 查詢執行 timeout，單位秒。 | 例如 `30`；控制 pyodbc query timeout，變更後需重啟服務。 |

## NAS / 檔案來源

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `ALLOWED_NAS_ROOTS_LINUX` | Linux 環境允許瀏覽與使用的 NAS 根目錄。 | 首次啟動且 `nas_roots` 表為空時，會匯入 DB 作為初始資料。 |
| `NAS_MAX_COPY_FILE_SIZE_MB` | NAS 複製單檔大小限制，單位 MB。 | `0` 表示不限制。 |
| `REGULATION_EU_2017_745_REFERENCE_FOLDER` | 歐盟採認標準文件主要資料夾路徑。 | 用於標準更新與採認標準同步。 |
| `REGULATION_EU_2017_745_REFERENCE_FALLBACK_FOLDER` | 歐盟採認標準文件備援資料夾路徑。 | 主要路徑不可用時使用；未設定時預設為專案內 `harmonised_store`。 |

## 系統 Log

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `APP_LOG_DIR` | App log 存放目錄。 | 例如 `app-web.log`、`app-worker.log`。 |
| `APP_LOG_LEVEL` | App log 等級。 | 常用值：`INFO`、`WARNING`、`ERROR`。 |
| `APP_LOG_TO_FILE` | 是否寫入 log 檔。 | `true` / `false`。 |
| `APP_LOG_STDOUT` | 是否輸出到 stdout。 | systemd journal / `journalctl` 會讀到 stdout/stderr。 |
| `APP_LOG_MAX_MB` | 單一 app log 檔最大大小，單位 MB。 | 超過後會輪替，不會無限增加。 |
| `APP_LOG_BACKUP_COUNT` | App log 輪替保留份數。 | 例如 `10` 代表保留 `.1` 到 `.10`。 |
| `SYSTEM_ERROR_DB_MIN_LEVEL` | system error 寫入 DB 的最低等級。 | 例如 `ERROR` 代表 `INFO` / `WARNING` 不寫入 system error DB。 |

## 定期清理

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `JOB_METADATA_RETENTION_DAYS` | Job metadata 保留天數。 | 變更後需重啟相關服務。 |
| `SYSTEM_ERROR_LOG_RETENTION_DAYS` | DB 內 system error log 保留天數。 | 由 cleanup service / CLI 清理 DB 紀錄。 |
| `AUDIT_LOG_RETENTION_DAYS` | Audit log 保留天數。 | 由 cleanup service / CLI 清理 DB 紀錄。 |
| `UPDATE_ON_CALENDAR` | 採認標準更新 timer 排程。 | systemd `OnCalendar` 格式；值含空白時 `.env` 內需加引號，例如 `"*-*-* 08:00:00"`。 |
| `CLEANUP_ON_CALENDAR` | metadata cleanup timer 排程。 | systemd `OnCalendar` 格式；值含空白時 `.env` 內需加引號，例如 `"*-*-* 23:00:00"`。 |
| `BACKUP_ON_CALENDAR` | 檔案與 MSSQL 備份 timer 排程。 | systemd `OnCalendar` 格式；值含空白時 `.env` 內需加引號，例如 `"*-*-* 02:00:00"`。 |

## AD / LDAP

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `LDAP_HOST` | LDAP / AD server host。 | 用於登入驗證與 AD 查詢。 |
| `LDAP_BASE_DN` | LDAP 使用者搜尋 base DN。 | 例如 `DC=example,DC=com`。 |
| `LDAP_BIND_DN` | LDAP bind 帳號。 | 敏感資訊。 |
| `LDAP_BIND_PASSWORD` | LDAP bind 密碼。 | 敏感資訊。 |
| `LDAP_USER_LOGIN_ATTR` | 登入時比對的 AD 欄位。 | 常見值為 `sAMAccountName`。 |
| `LDAP_USER_SEARCH_SCOPE` | LDAP 搜尋範圍。 | 支援 `BASE`、`LEVEL`、`SUBTREE`。 |
| `LDAP_USER_OBJECT_FILTER` | LDAP 使用者搜尋 filter。 | 目前用來排除 computer object。 |
| `LDAP_GROUP_GATE_ENABLED` | 是否啟用 AD 群組 gate。 | `0` / `false` 表示略過群組檢查。 |
| `ALLOWED_GROUP_DN` | 允許登入的 AD 群組 DN。 | 只有 `LDAP_GROUP_GATE_ENABLED` 開啟時才有意義。 |
| `INITIAL_ADMIN_WORK_IDS` | 初始 Admin work_id 清單。 | 逗號分隔；`seed-bootstrap` 會將這些帳號加入 Admin 角色。 |

## SMTP

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `SMTP_HOST` | SMTP server host。 | 用於寄送通知信。 |
| `SMTP_PORT` | SMTP port。 | 常見為 `25`。 |
| `SMTP_SENDER` | 寄件者地址。 | 同時作為信件 `From` 與 SMTP envelope sender；需符合 SMTP relay 規則。 |

## Azure OpenAI

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `AZURE_OPENAI_API_KEY` | Azure OpenAI API key。 | 敏感資訊。 |
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI endpoint。 | 例如 Azure OpenAI resource endpoint。 |
| `AZURE_OPENAI_DEPLOYMENT` | Azure OpenAI deployment name。 | 程式呼叫的模型部署名稱。 |
| `AZURE_OPENAI_API_VERSION` | Azure OpenAI API version。 | 需與 Azure OpenAI 服務支援版本相容。 |

## 文件處理 / LLM

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `SKIP_DOCX_CLEANUP` | 是否跳過 docx 清理程序。 | `1` / `true` 會跳過；變更後需重啟服務。 |
| `WORD_CHAPTER_LLM_BOUNDARY_FALLBACK` | 章節擷取邊界判斷失敗時，是否啟用 LLM 輔助 fallback。 | 需要 Azure OpenAI 設定可用。 |
| `LIBREOFFICE_BIN` | LibreOffice / soffice 執行檔路徑。 | 用於文件轉換或預覽。 |

## Job 執行

| 參數 | 用途 | 備註 |
| --- | --- | --- |
| `JOB_EXECUTOR_MODE` | Job 執行模式。 | 正式環境建議 `worker`；`inline` 主要給測試使用，production 會強制改回 `worker`。 |
