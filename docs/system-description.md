# 系統說明文件

> 本文件依目前專案檔案與程式碼整理，閱讀對象為系統開發者、維護者與論文審查者。若無法由程式碼或既有文件確認，本文標示「此處需補充」。若內容為依程式碼行為推論，本文標示「依程式碼推測」。

## 1. 系統架構設計

### 主要依據檔案

- `SYSTEM_ARCHITECTURE.md`
- `SYSTEM_ENVIRONMENT.md`
- `app/__init__.py`
- `app/blueprints/__init__.py`
- `app/config.py`
- `app/services/execution_service.py`
- `app/services/task_service.py`
- `modules/workflow.py`
- `modules/mapping_processor.py`
- `pyproject.toml`

本系統為 Flask 架構之文件處理與任務工作站，主要以伺服器端渲染頁面提供任務、流程、Mapping、標準更新、來源比對與管理功能。系統後端以 Flask Blueprint 分割功能路由，以 SQLAlchemy 操作資料庫，並透過背景工作佇列執行耗時文件處理。依 `SYSTEM_ARCHITECTURE.md` 與程式碼確認，正式資料庫為 Microsoft SQL Server；測試設定則使用 SQLite 記憶體資料庫。

系統前端主要由 Jinja2 template、Bootstrap/AdminLTE 樣式與 JavaScript 組成。任務頁面、流程編輯器、Mapping 頁面、比對頁面與管理後台皆以 server-rendered HTML 為主，互動型功能則使用 JSON API，例如檔案樹、流程版本、執行狀態與 NAS 瀏覽。

| 層級 | 組成 | 說明 |
|---|---|---|
| Web 層 | `app/__init__.py`、`app/blueprints/*` | 建立 Flask App、註冊 Blueprint、處理登入驗證、錯誤攔截與頁面/API 路由。 |
| 服務層 | `app/services/*` | 封裝任務、流程、執行佇列、Mapping metadata、權限、稽核、NAS 與系統設定邏輯。 |
| 資料層 | `app/models/*`、`migrations/versions/0001_baseline_schema.py` | 定義 ORM 模型與 Alembic baseline schema。 |
| 文件處理層 | `modules/workflow.py`、`modules/mapping_processor.py`、`modules/extract_*` | 執行 Word、PDF、Excel Mapping、圖片/表格擷取、模板合併與結果產生。 |
| 背景工作層 | `app/services/execution_service.py`、`app/jobs/executor.py` | 建立、鎖定、執行、取消、重試與清理背景工作。 |
| 檔案儲存 | `task_store/`、`output/`、`standard_update_store/` | 依任務保存來源檔、流程、結果、Mapping 執行紀錄與標準更新檔案。 |

依程式碼推測，主要資料流為：使用者先建立任務並指定 NAS 來源，系統建立 `tasks` 紀錄與 `task_store/<task_id>` 工作目錄，接著以背景工作同步來源檔案至 `files/`。使用者可在任務內建立流程定義或上傳 Mapping Excel。流程執行與 Mapping 執行均透過 `job_records` 建立背景工作，由 worker 取出工作後執行文件處理，輸出 `result.docx`、`log.json`、Mapping ZIP 或其他產物，並更新狀態紀錄供前端查詢。

系統使用 `task_execution_locks` 對同一任務的寫入型工作進行鎖定，避免流程與 Mapping 同時修改相同任務資料。工作狀態包含 `queued`、`claimed`、`running`、`completed`、`failed`、`canceled`、`timeout`。

### 待補充項目

- 此處需補充正式部署環境中的實際 Nginx 網域、服務名稱與網路拓撲圖。
- 此處需補充論文中需要呈現的系統架構圖。

## 2. Mapping 設計規範說明

### 主要依據檔案

- `modules/mapping_processor.py`
- `app/blueprints/tasks/mapping_routes.py`
- `app/blueprints/tasks/mapping_scheme_helpers.py`
- `app/models/mapping_metadata.py`
- `tests/test_mapping_processor_isolation.py`
- `tests/test_mapping_route_errors.py`
- `static/samples/mapping_example.xlsx`

Mapping 用於將 Excel 中的列資料轉換為文件處理步驟。系統會讀取使用者上傳或由流程匯出的 Mapping 檔，依列定義來源檔案、擷取段落或操作、類型、是否包含標題、輸出路徑、輸出檔名、模板檔與插入段落，最後產生 Word 文件、複製檔案/資料夾或封裝輸出 ZIP。

依 `modules/mapping_processor.py` 確認，新版 Mapping 必須有表頭，且只掃描前 10 列尋找表頭。主要欄位與別名如下。

| 邏輯欄位 | 支援表頭名稱 | 說明 |
|---|---|---|
| source | `檔案名稱/資料夾名稱/文字內容`、`輸入檔案名稱/資料夾名稱/文字內容`、`來源檔案` | 來源檔案、資料夾或文字內容。 |
| operation | `擷取段落/操作`、`擷取段落` | 章節、Figure/Table 條件或新增文字/標題等操作。 |
| item_type | `類型`、`Type`、`擷取類型` | 指定 All、Figure、Table、Copy File、Copy Folder、Add Text 等類型。 |
| include_title | `包含標題`、`是否包含標題`、`顯示標題`、`Include Title`、`Include Caption` | 控制章節標題或圖表 caption 是否納入。 |
| out_path | `檔案路徑`、`輸出路徑`、`章節名稱`、`輸出資料夾名稱` | 輸出子目錄。 |
| out_name | `檔案名稱`、`輸出檔案名稱` | 輸出檔名，文件輸出需為 `.docx`。 |
| template | `模板文件` | 可指定 Word 模板。 |
| insert | `插入段落名稱`、`插入段落`、`插入段落名稱/目的資料夾名稱` | 模板段落或複製目的資料夾。 |
| insert_mode | `插入方式`、`模板插入方式`、`Template Mode`、`Insert Mode` | 模板插入模式。 |

系統依 `item_type` 與 `operation` 判斷工作步驟。支援類型包含全文擷取、章節擷取、Figure、Table、表格包圖、PDF 轉圖片、加入圖片、加入文字、羅馬/項目符號/阿拉伯數字標題、複製檔案與複製資料夾。未指定類型時，系統會依操作欄內容推測章節擷取或圖表擷取。

系統提供引用文件檢查與擷取條件檢查，並將 Mapping 方案狀態記錄於 `mapping_schemes`。若任務檔案版本已更新，已驗證方案可能變成 `needs_review`。常見錯誤包含找不到新版 Mapping 表頭、缺少輸出檔名、輸出檔名非 `.docx`、缺少來源檔案、來源解析失敗、模板不存在、插入段落不存在、Figure/Table 條件不足與複製失敗。

舊版無表頭 Mapping 格式已停用。系統依 Excel 第一個工作表處理 Mapping；多工作表 Mapping 規則此處需補充。相似度自動對應主要見於標準對應模組，非一般文件 Mapping 核心流程。

### 待補充項目

- 此處需補充正式交付使用的 Mapping 範本欄位說明與範例列。
- 此處需補充 Mapping 欄位允許值完整清單與業務規則說明。

## 3. 任務管理模組說明

### 主要依據檔案

- `app/blueprints/tasks/task_routes.py`
- `app/blueprints/tasks/nas_routes.py`
- `app/services/task_service.py`
- `app/models/task.py`
- `app/models/execution.py`
- `app/templates/partials/sidebar_tasks.html`
- `tests/test_task_service_source_files.py`
- `tests/test_task_listing_pins.py`

任務管理模組提供文件處理工作的基本容器。每一個任務對應一組來源檔案、流程定義、輸出資料夾、Mapping 方案、執行紀錄與任務 metadata。系統以任務為單位隔離檔案與處理結果，避免不同文件處理工作互相污染。

使用者於 `/tasks` 建立任務，輸入任務名稱、描述與 NAS 路徑。系統驗證 NAS 路徑需存在且位於允許根目錄內，任務名稱不可重複，任務名稱與描述長度上限依程式碼為 50 字。建立後系統會產生 8 字元任務 ID、建立 `task_store/<task_id>/files` 與輸出資料夾，寫入 `meta.json`，並新增 `tasks` 資料庫紀錄。

任務來源同步狀態寫入 `meta.json`，包含 `source_sync_status`、`source_sync_job_id`、`source_sync_error`、`source_sync_started_at`、`source_sync_completed_at` 與 `source_sync_file_count`。依程式碼確認，來源同步中的狀態包含 `queued`、`running`；可處理狀態為空值或 `completed`。失敗狀態文字此處需補充，因任務同步 worker 完整狀態需結合執行結果判讀。

| 位置或資料表 | 用途 |
|---|---|
| `tasks` | 保存任務 ID、名稱、描述、建立者、NAS 路徑、輸出路徑與建立時間。 |
| `task_store/<task_id>/meta.json` | 保存任務顯示資料、來源同步狀態、最後編輯者與時間。 |
| `task_store/<task_id>/files/` | 保存從 NAS 同步或任務內管理的來源檔案。 |
| `task_store/<task_id>/flows/` | 保存流程 JSON 與流程版本資料。 |
| `task_store/<task_id>/jobs/` | 保存流程執行結果與 log。 |
| `task_store/<task_id>/mapping_job/` | 保存 Mapping 執行結果與 log。 |
| `task_store/<task_id>/output/` | 任務輸出根目錄。 |

建立、複製、刪除、重新命名、更新描述與 NAS 同步等操作會透過 `record_audit` 寫入 `audit_logs`。刪除權限依 `task_service.can_delete_task` 判斷，細節此處需補充，因本次掃描未完整展開該函式全部條件。

### 待補充項目

- 此處需補充任務刪除權限的正式政策描述。
- 此處需補充任務狀態在畫面上的完整顯示規格。

## 4. 檔案匯入模組說明

### 主要依據檔案

- `app/services/task_service.py`
- `app/services/nas_service.py`
- `app/blueprints/tasks/task_routes.py`
- `app/blueprints/tasks/nas_routes.py`
- `app/blueprints/flows/flow_file_routes.py`
- `app/blueprints/standard_updates/routes.py`
- `tests/test_nas_browse_api.py`
- `tests/test_flow_file_browser_create_folder.py`

系統主要以 NAS 允許根目錄作為任務來源。使用者建立任務時指定相對於允許根目錄的路徑，系統解析後將來源資料夾同步至任務工作目錄。流程檔案瀏覽 API 也允許在任務 `files` 與 `output` 範圍內建立、重新命名、刪除與下載檔案或資料夾。

標準更新模組另支援上傳 Word、標準 Excel、法規 Excel 與歐盟採用標準 Excel，對應路徑為 `/standards/<task_id>/upload-*`。

依 `task_service.allowed_file` 與前端流程檔案選擇規則，系統支援 `.docx`、`.pdf`、`.zip`、`.xlsx`、`.xls`、`.png`、`.jpg`、`.jpeg`、`.bmp`、`.gif`。Mapping 中新增圖片另支援 `.tif`、`.tiff`。

NAS 路徑不可為絕對路徑，不可包含 `..` 跳脫允許根目錄。若系統設定不允許遞迴，路徑不可包含子層級。系統會檢查指定路徑是否存在、是否位於允許根目錄內，以及是否為資料夾。複製來源時會檢查大小限制，預設或系統設定可由 `NAS_MAX_COPY_FILE_SIZE_MB` 與 `system_settings.nas_max_copy_file_size_mb` 控制。

任務內檔案 API 使用相對路徑解析，並阻擋根目錄重新命名或刪除。若存取超出任務 root，會回傳權限或路徑錯誤。

### 待補充項目

- 此處需補充正式環境 NAS 掛載點與允許根目錄清單。
- 此處需補充各類上傳檔案大小限制的正式設定值。

## 5. 匯入流程說明

### 主要依據檔案

- `app/blueprints/tasks/task_routes.py`
- `app/blueprints/tasks/nas_routes.py`
- `app/services/task_service.py`
- `app/services/execution_service.py`
- `app/jobs/executor.py`
- `app/services/mapping_metadata_service.py`

依程式碼推測，任務檔案匯入流程如下：使用者於 `/tasks` 指定 NAS 根目錄索引與相對路徑；`nas_service.resolve_nas_path` 驗證路徑格式、允許根目錄與實體存在性；系統建立任務 ID、任務資料夾、`files/`、`output/` 與 `meta.json`；`task_service.record_task_in_db` 新增 `tasks` 資料表紀錄；`enqueue_task_source_sync_job` 建立 `task_source_sync` 背景工作；worker 將來源資料同步至 `task_store/<task_id>/files/`；同步過程更新 `meta.json` 中的來源同步狀態；同步完成後任務可進入流程執行、Mapping 檢查或來源比對。

任務建立初期先寫入 `meta.json` 與資料庫任務紀錄，再建立背景工作。若資料庫紀錄建立失敗，系統會刪除已建立的任務資料夾；若背景工作建立失敗，系統會刪除任務紀錄與任務資料夾，避免留下不完整任務。

匯入結果主要呈現在任務列表與任務 metadata。檔案同步完成後，流程與 Mapping 模組可讀取 `files/` 內的來源檔。任務檔案版本變動會影響 Mapping 方案狀態，相關版本號記錄於 `task_file_states.files_revision`。

### 待補充項目

- 此處需補充 `task_source_sync` worker 內部複製規則的完整設計說明。
- 此處需補充同步失敗時是否保留部分檔案與重試策略。

## 6. 文件處理模組說明

### 主要依據檔案

- `modules/workflow.py`
- `modules/extract_word_chapter.py`
- `modules/extract_word_all_content.py`
- `modules/extract_specific_figure_xml.py`
- `modules/extract_specific_table_xml.py`
- `modules/extract_pdf_img.py`
- `modules/docx_merger.py`
- `modules/docx_provenance.py`
- `app/jobs/executor.py`

文件處理模組負責依流程或 Mapping 轉換後的步驟執行 Word/PDF 內容擷取、圖片或表格擷取、文字與標題插入、圖片插入、檔案複製、模板合併與後處理。此模組是系統產生交付文件與處理結果的核心。

輸入來源通常為任務 `files/` 內的 Word、PDF、圖片、資料夾或 Mapping 指定文字。輸出包含 `result.docx`、流程 `log.json`、Mapping 輸出 DOCX、複製後檔案/資料夾與 ZIP 封裝。流程執行結果保存在 `task_store/<task_id>/jobs/<job_id>/`，Mapping 結果保存在 `task_store/<task_id>/mapping_job/<run_id>/`。

依 `modules/workflow.py` 的 `SUPPORTED_STEPS`，目前支援擷取 PDF 標籤圖片、擷取 Word 全部內容、擷取 Word 指定章節/標題、插入 Word 指定章節/標題的特定圖片、插入 Word 指定章節/標題的特定表格、插入純文字段落、插入羅馬數字標題、插入項目符號標題、插入阿拉伯數字標題、插入圖片檔、複製檔案與複製資料夾。註解中曾出現但未啟用的步驟不列為已確認功能。

背景工作負責包裝流程執行狀態。若任一步驟產生 `status=error`，系統會記錄工作失敗與錯誤摘要。流程亦支援取消檢查，取消時會以 `JobCanceledError` 中斷。常見例外包含來源檔案不存在、PDF ZIP 無效、圖片路徑無效、關鍵字未檢索到檔案或資料夾、未知步驟類型與文件 XML 結構缺失。

### 待補充項目

- 此處需補充每一種文件處理步驟的業務輸入範例。
- 此處需補充處理大型文件時的效能限制與逾時設定。

## 7. 文件流程生成引擎

### 主要依據檔案

- `app/blueprints/flows/execution_routes.py`
- `app/services/flow_definition_service.py`
- `app/services/flow_validation_service.py`
- `app/services/flow_service.py`
- `app/blueprints/flows/templates/flows/flow.html`
- `modules/workflow.py`

文件流程生成引擎負責將前端流程編輯器或既有流程 JSON 轉換成可執行的工作步驟。流程可被保存、另存、建立版本、執行或匯出為 Mapping。此設計使文件處理程序不必硬編碼於後端，而能由使用者或維護者以流程定義組合不同處理步驟。

流程節點對應 `SUPPORTED_STEPS` 中的 step type。每個節點包含 `type` 與 `params`，執行前會依 schema 驗證必填欄位、檔案路徑與輸出檔名。若指定模板，系統會解析模板段落並把模板資訊附加至執行設定。

`build_workflow_from_form` 會從表單欄位建立 workflow list。`validate_flow_submission` 與 `validate_saved_flow_run` 負責檢查流程名稱、輸出檔名、必要欄位與步驟合法性。執行時，`_resolve_runtime_step_params` 將任務相對路徑解析為實體檔案路徑。

流程執行後產生 `result.docx` 與 `log.json`。若設定輸出路徑，worker 會將結果發布至任務 output，並記錄來源 provenance。流程結果頁可查詢、下載、取消、重試或刪除執行紀錄。

### 待補充項目

- 此處需補充流程編輯器 UI 與流程 JSON schema 的正式規格。
- 此處需補充流程節點之間是否允許條件式分支；目前程式碼未確認有分支流程。

## 8. Excel Mapping 自動對應引擎

### 主要依據檔案

- `modules/mapping_processor.py`
- `app/blueprints/tasks/mapping_routes.py`
- `app/blueprints/tasks/standard_mapping_routes.py`
- `app/services/standard_mapping_service.py`
- `tests/test_standard_mapping_priority_rules.py`
- `tests/test_flow_export_mapping_excel.py`

一般文件 Mapping 引擎會讀取 Excel 表頭別名，將每列轉換為 workflow step。依程式碼確認，此處的「自動對應」主要是表頭別名辨識、類型正規化、操作欄解析與來源檔案解析，不是以語意相似度自動推論任意欄位。

標準對應模組使用 `standard_mapping_service.py`，其規則包含標準名稱正規化、年份擷取、ISO/EN/BS 等標準層級優先順序、歐盟採用標準比對、標題欄位候選與標準表欄位辨識。程式碼中使用 `difflib.SequenceMatcher`，可推測部分比對會使用字串相似度；具體門檻值與完整人工確認流程此處需補充。

依程式碼推測，標準對應頁面提供欄位檢查、預覽與下載結果流程，若未符合兩欄格式或未完成手動對應欄位設定，系統會要求先完成欄位設定後再下載。一般 Mapping 方案則需先通過引用文件檢查與擷取條件檢查，狀態為 ready 後才可執行或設為排程。

失敗情境包含 Excel 暫存檔、Excel 無法讀取、缺少必要表頭、找不到 Mapping 方案、Mapping 方案需要重新檢查、Mapping 方案尚未通過檢查、標準對應欄位格式不符與下載失敗。自動對應結果仍依來源資料品質與欄位命名而定。

### 待補充項目

- 此處需補充 Excel Mapping「相似度或比對門檻」的正式設計值。
- 此處需補充人工確認畫面截圖與欄位對應操作設計。

## 9. 處理記錄

### 主要依據檔案

- `app/models/execution.py`
- `app/models/mapping_metadata.py`
- `app/models/auth.py`
- `app/services/audit_service.py`
- `app/services/execution_service.py`
- `app/services/mapping_metadata_service.py`
- `app/blueprints/flows/results_routes.py`
- `app/services/auth_admin_service.py`

系統記錄包含背景工作紀錄、工作事件、工作產物、Mapping 方案與執行紀錄、稽核紀錄、系統錯誤紀錄、流程版本 metadata 與任務 metadata。這些紀錄分散於資料庫與任務檔案目錄，兼顧查詢與產物追溯。

`job_records` 記錄工作 ID、工作類型、佇列、任務 ID、目標名稱、狀態、payload、result、artifact root、嘗試次數、worker、錯誤摘要、建立者與時間欄位。`job_events` 記錄事件訊息，`job_artifacts` 記錄工作產物相對路徑與大小。

`audit_logs` 記錄使用者操作，例如任務建立、流程執行、Mapping 檢查、Mapping 執行、標準更新與管理後台操作。`system_error_logs` 記錄系統錯誤，包含 level、component、message、error_type、detail 與 task_id。若資料庫寫入失敗，稽核會 fallback 至 JSONL，系統錯誤會 fallback 至 `system-error-fallback.jsonl`。

流程結果頁、Mapping 結果頁與管理後台可查詢執行紀錄、下載結果、取消、重試或刪除紀錄。管理後台提供 audit log 與 system error log 查詢與下載。

### 待補充項目

- 此處需補充正式維運時 log 查詢與保留政策。
- 此處需補充稽核紀錄對應論文或驗收需求的保存年限。

## 10. 文件處理流程說明

### 主要依據檔案

- `app/blueprints/tasks/task_routes.py`
- `app/blueprints/flows/execution_routes.py`
- `app/blueprints/tasks/mapping_routes.py`
- `app/jobs/executor.py`
- `app/services/execution_service.py`
- `modules/workflow.py`
- `modules/mapping_processor.py`

依程式碼推測，文件處理的完整流程為：建立任務並同步來源檔案、建立流程或 Mapping 方案、驗證來源與擷取條件、建立背景工作、worker 執行文件處理、產生 Word/ZIP/log 產物、記錄工作狀態與稽核紀錄、由結果頁提供查詢與下載。

流程執行可由 `/tasks/<task_id>/flows/run` 直接使用目前表單內容執行，也可由 `/tasks/<task_id>/flows/execute/<flow_name>` 執行已保存流程。系統會先解析流程、驗證欄位與檔案，然後建立 `flow_single` 工作。worker 執行 `run_workflow` 後保存結果。

Mapping 頁 `/tasks/<task_id>/mapping` 可上傳 Mapping、檢查引用文件、檢查擷取條件、建立 Mapping 方案與執行方案。Mapping 執行會建立 `mapping_operation` 或 `mapping_scheme_run` 工作，最後產生 Mapping log、輸出檔案與 ZIP。

流程結果保存在 `task_store/<task_id>/jobs/<job_id>`；Mapping 結果保存在 `task_store/<task_id>/mapping_job/<run_id>`。資料庫同時保存工作狀態與 Mapping 執行摘要，支援結果列表查詢、批次下載、刪除、取消與重試。

### 待補充項目

- 此處需補充文件處理流程圖。
- 此處需補充各流程節點的業務名稱與論文用語對應。

## 11. 版本控制說明

### 主要依據檔案

- `app/services/flow_version_service.py`
- `app/blueprints/flows/version_routes.py`
- `app/blueprints/flows/execution_routes.py`
- `app/blueprints/tasks/compare_routes.py`
- `app/services/flow_service.py`
- `app/blueprints/flows/templates/flows/flow.html`

系統版本控制主要用於流程定義與比對結果版本保存。流程版本讓使用者在修改流程時保留可回復的 JSON snapshot；比對結果版本則保存比較/預覽後的文件版本與 metadata。

流程版本來源包含 `auto_save`、`before_restore` 與 `manual_snapshot`。手動版本由使用者建立，回復前會自動建立 `before_restore` 備份。流程版本內容以 JSON 檔保存在 `task_store/<task_id>/flows/_versions/<flow_name>/`，metadata 記錄版本 ID、名稱、建立者、來源與內容雜湊。

流程版本以 `metadata.json` 搭配各版本 JSON 檔保存。系統以 SHA-256 內容雜湊判斷版本內容是否相同。手動 snapshot 上限為 20 筆，超過時會刪除較舊手動版本。依程式碼確認，版本資料不是獨立資料表，而是保存在任務檔案目錄。

版本來源會轉換為顯示名稱：`auto_save` 為自動保存，`before_restore` 為回復前備份，`manual_snapshot` 為手動版本。版本清單 API 會回傳可回復、重新命名、刪除與下載的 URL。

### 待補充項目

- 此處需補充比對結果版本與流程版本在驗收文件中的分類方式。
- 此處需補充版本保存上限是否符合正式需求。

## 12. 版本切換與回復功能說明

### 主要依據檔案

- `app/blueprints/flows/version_routes.py`
- `app/services/flow_version_service.py`
- `app/blueprints/flows/templates/flows/flow.html`

流程版本可透過 `/tasks/<task_id>/flows/<flow_name>/versions/<version_id>/restore` 回復。系統回復前會讀取目前流程 JSON 與目標版本 JSON，並先建立 `before_restore` 備份，再以目標版本內容覆寫目前流程檔。

若流程檔不存在，回傳 `Flow not found`；版本不存在，回傳 `Version not found`；版本檔無法解析，回傳 `Version file is invalid`。刪除與重新命名僅允許 `manual_snapshot`，手動版本以外不可刪除或重新命名。

回復流程會將目前流程內容保存在回復前備份，以便撤銷上次回復。回復後系統更新任務最後編輯時間。依程式碼推測，此功能只回復流程定義，不會回復已產生的執行結果、Mapping 結果、任務來源檔或資料庫工作紀錄。

回復會直接覆寫目前流程 JSON，因此可能使尚未保存的修改消失。前端回復按鈕有確認文字，指出目前內容會先自動備份。回復成功時顯示已成功回復或已成功撤銷上次回復。

### 待補充項目

- 此處需補充正式需求是否要支援任務資料或產物的完整回復。
- 此處需補充回復操作的權限限制；目前未看到版本回復專屬權限。

## 13. 來源比對模組說明

### 主要依據檔案

- `app/blueprints/tasks/compare_routes.py`
- `app/blueprints/tasks/compare_helpers.py`
- `modules/docx_provenance.py`
- `app/services/flow_output_provenance.py`
- `tests/test_compare_view.py`
- `tests/test_docx_provenance.py`

來源比對模組用於將流程輸出文件與來源文件關係可視化。系統會根據流程 log、來源檔案、章節擷取資訊與 provenance 資料建立預覽，協助確認輸出內容來自哪些來源章節、圖片或表格。

比對頁使用 `result.docx` 與 `log.json` 作為主要來源。系統會解析每個 workflow step，整理來源檔案、章節、標題、Figure/Table 條件與 PDF 預覽。若有 provenance 預覽資料，會優先使用 provenance trace；否則退回以段落與物件候選建立 trace。

依程式碼確認，比對頁會建立 PDF 與 HTML 預覽，並可加入來源標記。來源關聯包含章節來源、source URL、頁面來源 map 與 object trace candidates。若 LibreOffice 不存在或轉換失敗，系統會顯示預覽錯誤訊息。

若找不到 `result.docx` 或 `log.json`，比對頁回傳 404。預覽錯誤包含找不到要預覽的文件、找不到 LibreOffice、LibreOffice 轉 PDF/HTML 失敗與建立來源標記預覽失敗。

### 待補充項目

- 此處需補充來源比對畫面的截圖與使用情境說明。
- 此處需補充差異標示的正式圖例；目前程式碼較偏向來源追蹤與預覽，不是傳統文字 diff。

## 14. 權限控管說明

### 主要依據檔案

- `app/models/auth.py`
- `app/services/auth_service.py`
- `app/services/authn_service.py`
- `app/services/authz_service.py`
- `app/services/auth_hooks_service.py`
- `app/services/auth_admin_service.py`
- `app/blueprints/auth/routes.py`
- `app/templates/403.html`

系統使用 Flask-Login 與 Flask-LDAP3-Login。當 `AUTH_ENABLED` 為 true 時，除 `/auth/login`、`/auth/logout` 與 static asset 外，所有路由皆需登入。登入使用 LDAP 表單，依 `LDAP_HOST`、`LDAP_BASE_DN`、`LDAP_BIND_DN`、`LDAP_BIND_PASSWORD` 與 `LDAP_USER_LOGIN_ATTR` 查驗使用者。

| 角色 | 可使用功能 | 權限限制 | 備註 |
|---|---|---|---|
| `admin` | 系統管理、使用者管理、一般文件處理功能 | 具備 `user:manage` 權限 | 初始管理員可由 `INITIAL_ADMIN_WORK_IDS` 建立。 |
| `editor` | 文件任務、流程、Mapping 等一般功能 | 不具備使用者管理權限 | 未指定角色的已授權使用者登入時，程式會補 editor 角色。 |

目前程式碼只確認 `user:manage` 權限對應 admin。其他功能主要依登入狀態開放，是否需細分任務/流程/Mapping 權限此處需補充。

登入時系統先驗證 LDAP 憑證，再檢查 AD 群組 gate。若使用者不在允許群組、資料庫找不到使用者、帳號停用或 LDAP 設定錯誤，登入會失敗。登入成功後更新 `last_login_at` 並記錄 audit。

未登入使用者會被導向 `/auth/login?next=...`。權限不足時管理後台相關 view 會 `abort(403)`，系統以 `403.html` 顯示。登入失敗訊息包含憑證無效、不在允許登入群組、未獲授權、帳號已停用與登入失敗請聯絡管理員。

### 待補充項目

- 此處需補充是否需要任務層級資料存取限制。
- 此處需補充 AD 群組 DN 與正式角色指派流程。

## 15. 程式設計

### 主要依據檔案

- `app/__init__.py`
- `app/blueprints/*`
- `app/services/*`
- `app/models/*`
- `modules/*`
- `app/jobs/*`
- `tests/*`
- `pyproject.toml`

專案採 Flask application factory 模式。`create_app` 建立 web app，`create_job_app` 建立 worker app。兩者共用設定、資料庫與服務初始化，但 web app 會額外初始化登入、註冊 Blueprint 與錯誤 handler。

| 目錄 | 用途 |
|---|---|
| `app/blueprints/` | 功能路由與 template，包含 auth、tasks、flows、nas、standard_updates。 |
| `app/services/` | 商業邏輯與跨路由服務，例如任務、流程、執行佇列、權限、稽核、標準更新。 |
| `app/models/` | SQLAlchemy ORM 模型與 schema 補齊函式。 |
| `app/jobs/` | worker 執行器與背景工作處理。 |
| `modules/` | 文件處理、Word/PDF/Excel 解析、模板與 RBAC 輔助模組。 |
| `app/templates/` | 共用 layout、管理後台與 partial。 |
| `static/` | CSS、JS、圖示、vendor asset 與範例 Mapping。 |
| `tests/` | Pytest 測試，覆蓋流程、Mapping、權限、部署設定與文件處理細節。 |

主要頁面使用 Jinja2 template 渲染。互動功能以 JSON API 支援，例如 `/api/tasks/<task_id>/flow-files`、`/api/tasks/<task_id>/flows/<flow_name>/versions`、`/tasks/<task_id>/flows/runs/<job_id>/status` 與 Mapping status API。

核心資料模型包含 `TaskRecord`、`JobRecord`、`MappingSchemeRecord`、`MappingRunRecord`、`User`、`Role`、`AuditLog` 與 `SystemErrorLog`。核心處理函式包含 `run_workflow`、`process_mapping_excel`、`enqueue_job`、`record_audit`、`record_system_error`、`snapshot_flow_version` 與 `resolve_nas_path`。

### 待補充項目

- 此處需補充正式程式模組責任分工圖。
- 此處需補充重要函式的時序圖。

## 16. 資料庫設計

### 主要依據檔案

- `app/models/auth.py`
- `app/models/task.py`
- `app/models/execution.py`
- `app/models/mapping_metadata.py`
- `app/models/nas.py`
- `app/models/settings.py`
- `app/models/standard_update.py`
- `migrations/versions/0001_baseline_schema.py`

系統以 SQLAlchemy ORM 定義資料表，Alembic baseline migration 以 `db.metadata.create_all` 建立目前 metadata 中的所有資料表。正式環境設定以 Microsoft SQL Server 為主，測試環境使用 SQLite。部分 model 仍保留 `ensure_schema` 用於舊環境欄位補齊，但正式部署文件建議使用 migration 與 schema preflight。

| 資料表名稱 | 用途 | 主要欄位 | 關聯資料表 | 備註 |
| ----- | -- | ---- | ----- | -- |
| `tasks` | 任務主檔 | `id`, `name`, `description`, `creator`, `nas_path`, `output_path`, `created_at` | `job_records`, `mapping_schemes`, `mapping_runs` 以 `task_id` 邏輯關聯 | 未定義外鍵。 |
| `job_records` | 背景工作主檔 | `job_id`, `job_type`, `queue_name`, `task_id`, `status`, `payload_json`, `result_json`, `error_summary` | `job_artifacts`, `job_events`, `task_execution_locks` | 多個索引支援狀態與任務查詢。 |
| `job_artifacts` | 工作產物 | `id`, `job_id`, `artifact_type`, `rel_path`, `size_bytes` | `job_records` | 未定義外鍵。 |
| `job_events` | 工作事件 | `id`, `job_id`, `event_type`, `message`, `payload_json` | `job_records` | 未定義外鍵。 |
| `task_execution_locks` | 任務寫入鎖 | `task_id`, `lock_type`, `job_id`, `expires_at` | `job_records` | 防止同任務並行寫入。 |
| `task_file_states` | 任務檔案版本 | `task_id`, `files_revision`, `updated_at` | `tasks` | 用於 Mapping 方案是否需重新檢查。 |
| `mapping_schemes` | Mapping 方案 | `scheme_id`, `task_id`, `name`, `mapping_file`, `reference_ok`, `extract_ok`, `status_key` | `tasks`, `mapping_runs` | 保存驗證狀態與來源檔。 |
| `mapping_runs` | Mapping 執行紀錄 | `run_id`, `task_id`, `scheme_id`, `status`, `output_count`, `zip_file`, `log_file`, `error` | `mapping_schemes`, `job_records` | `run_id` 通常對應 job id。 |
| `users` | 使用者 | `id`, `work_id`, `display_name`, `email`, `is_active`, `last_login_at` | `user_roles` | `work_id` 唯一。 |
| `roles` | 角色 | `id`, `name` | `user_roles` | 內建 admin/editor。 |
| `user_roles` | 使用者角色 | `user_id`, `role_id` | `users`, `roles` | 一位使用者限制一個角色。 |
| `audit_logs` | 稽核紀錄 | `id`, `created_at`, `action`, `work_id`, `detail`, `task_id` | `users`, `tasks` 邏輯關聯 | detail 為 JSON 字串。 |
| `system_error_logs` | 系統錯誤 | `id`, `created_at`, `level`, `component`, `message`, `error_type`, `detail`, `task_id` | `tasks` 邏輯關聯 | 可由管理後台查詢。 |
| `nas_roots` | NAS 允許根目錄 | `id`, `path`, `env`, `platform`, `active`, `created_at` | 無 | `env + platform + path` 唯一。 |
| `system_settings` | 系統設定 | `id`, `email_batch_notify_enabled`, `nas_max_copy_file_size_mb`, `regulation_download_page_url`, `regulation_download_link_text` | 無 | 單筆預設設定。 |
| `regulation_sync_states` | 法規更新同步狀態 | `id`, `source_key`, `last_filename`, `last_uuid`, `last_url` | 無 | `source_key` 唯一。 |
| `standard_update_tasks` | 標準更新任務 | `id`, `name`, `status`, `word_file_path`, `standard_excel_path`, `regulation_excel_path`, `last_run_status` | `harmonised_releases` 邏輯關聯 | 保存標準更新流程狀態。 |
| `harmonised_releases` | 歐盟採用標準版本 | `id`, `source_url`, `file_name`, `nas_path`, `version_label`, `checksum`, `is_active`, `download_status` | `standard_update_tasks` 邏輯關聯 | 記錄下載版本與 active 狀態。 |

任務建立後產生 `tasks` 紀錄與任務目錄；流程與 Mapping 執行建立 `job_records` 及相關事件/產物。Mapping 方案與執行摘要可刪除。稽核與系統錯誤紀錄可依 retention CLI 清理。任務刪除會移除任務資料夾與資料庫任務紀錄；其是否同步刪除所有關聯 job 紀錄此處需補充。

### 待補充項目

- 此處需補充正式 ERD。
- 此處需補充資料表清理與備份政策。

## 17. 錯誤代碼列表及處理說明

### 主要依據檔案

- `app/__init__.py`
- `app/utils.py`
- `app/blueprints/auth/routes.py`
- `app/blueprints/flows/version_routes.py`
- `app/blueprints/flows/flow_file_routes.py`
- `app/blueprints/tasks/mapping_routes.py`
- `modules/mapping_processor.py`
- `modules/workflow.py`
- `app/services/nas_service.py`
- `app/services/audit_service.py`

程式碼未定義集中式業務錯誤代碼表。以下以 HTTP 狀態、系統回傳訊息或 log 前綴整理為可驗收的錯誤清單；若錯誤代碼欄標示為「程式未定義」，表示目前只能由訊息文字、HTTP 狀態或 log 內容辨識。

| 錯誤代碼 | 錯誤訊息 | 發生原因 | 使用者處理方式 | 管理者處理方式 |
| ---- | ---- | ---- | ------- | ------- |
| `HTTP-403` | 權限不足頁面 | 未具備管理或存取權限 | 以具權限帳號登入 | 檢查使用者角色與 `user_roles`。 |
| `HTTP-404` | Job not found or failed / Not Found | 任務、流程、版本、執行紀錄或檔案不存在 | 確認連結與任務狀態 | 檢查檔案目錄與資料庫紀錄是否一致。 |
| `HTTP-500` | Internal Server Error | 未處理例外 | 稍後重試並回報管理者 | 查詢 `system_error_logs` 與 app log。 |
| 程式未定義 | 憑證無效 | LDAP 表單驗證失敗或帳密錯誤 | 確認工號與密碼 | 檢查 LDAP 設定與 AD 狀態。 |
| 程式未定義 | 您的帳號不在允許的登入群組中 | AD group gate 未通過 | 聯絡管理者申請權限 | 檢查 `ALLOWED_GROUP_DN` 與群組成員。 |
| 程式未定義 | 您的帳號未獲得授權 | LDAP 通過但 DB 無使用者 | 聯絡管理者開通 | 於管理後台新增使用者與角色。 |
| 程式未定義 | 您的帳號已被停用 | `users.is_active` 為 false | 聯絡管理者 | 依管理政策啟用帳號。 |
| 程式未定義 | 指定的 NAS 路徑不是資料夾 | 任務建立指定來源非資料夾 | 改選資料夾 | 檢查 NAS 掛載與權限。 |
| 程式未定義 | 任務名稱已存在 | 建立或重新命名任務時名稱重複 | 使用其他名稱 | 檢查是否需合併或清理任務。 |
| 程式未定義 | 尚未設定允許的根目錄 | NAS root 未設定 | 聯絡管理者 | 設定 `ALLOWED_NAS_ROOTS_*` 或 `nas_roots`。 |
| 程式未定義 | 路徑不可包含 .. 或跳脫允許的根目錄 | 路徑安全檢查失敗 | 使用允許根目錄下相對路徑 | 檢查前端輸入與 root 設定。 |
| 程式未定義 | 找不到指定的路徑，或不在允許的根目錄內 | NAS 路徑不存在或不允許 | 檢查路徑 | 檢查掛載、權限與 root 設定。 |
| 程式未定義 | 輸出檔名不合法 | 檔名含非法字元或格式不符 | 修正檔名 | 檢查前端驗證是否一致。 |
| 程式未定義 | 輸出路徑不合法 | 輸出相對路徑含非法字元或跳脫 | 修正輸出路徑 | 檢查路徑正規化規則。 |
| `ERROR` log 前綴 | 找不到新版 Mapping 表頭 | Excel 缺少新版表頭 | 使用系統範例格式 | 確認 Mapping 範本版本。 |
| `ERROR` log 前綴 | 缺少輸出文件檔名 | Mapping 列未填輸出檔名 | 補齊輸出檔名 | 檢查 Mapping 欄位說明。 |
| `ERROR` log 前綴 | 輸出文件檔名需包含 .docx 副檔名 | Mapping 文件輸出非 docx | 改為 `.docx` | 檢查範本驗證。 |
| `ERROR` log 前綴 | 缺少輸入文件檔名 | Mapping 列缺少來源 | 補齊來源 | 確認來源檔是否在任務 files。 |
| `ERROR` log 前綴 | 來源檔案解析失敗 | 找不到檔案或資料夾 | 檢查來源名稱 | 檢查同步結果與大小寫/副檔名。 |
| `ERROR` log 前綴 | 未找到模板文件 | 指定模板不存在 | 重新選擇模板 | 檢查任務 files 與模板解析。 |
| `ERROR` log 前綴 | 插入段落未找到 | 模板中找不到指定段落 | 修正插入段落名稱 | 檢查模板段落解析結果。 |
| 程式未定義 | 缺少流程名稱 | 保存或建立版本未提供名稱 | 填入流程名稱 | 檢查前端欄位驗證。 |
| 程式未定義 | 流程名稱已存在 | 另存或重新命名重複 | 更換名稱 | 檢查流程檔案目錄。 |
| 程式未定義 | 找不到模板檔案，請重新載入 | 模板相對路徑失效 | 重新選擇模板 | 檢查任務檔案與流程 JSON。 |
| HTTP 404 | Flow not found | 流程版本 API 找不到流程檔 | 重新整理頁面 | 檢查 flow JSON 是否存在。 |
| HTTP 404 | Version not found | 指定版本不存在 | 選擇有效版本 | 檢查 `_versions` metadata。 |
| HTTP 400 | 手動版本以外的版本不可刪除 | 刪除非 manual snapshot | 不刪除系統版本 | 此為設計限制。 |
| 程式未定義 | Unknown step type | 流程 step type 不在支援清單 | 重新建立流程 | 檢查流程 JSON 或匯入來源。 |
| 程式未定義 | 未檢索到與關鍵字相符的檔案/資料夾 | 複製步驟 keyword 無匹配 | 調整關鍵字 | 檢查來源目錄內容。 |

### 待補充項目

- 此處需補充正式錯誤代碼命名規範；目前程式碼未集中定義錯誤碼。
- 此處需補充管理者 SOP 與截圖。

## 附錄 A. 技術細節補充

### A.1 Blueprint 與實際路由群組

本系統以多個 Blueprint 組成。依 `/home/NE025/UO_MDR/.venv` 測試環境產生的 Flask `url_map`，主要路由群組如下。

| Blueprint | URL 範圍 | 主要責任 |
|---|---|---|
| `auth_bp` | `/auth/login`、`/auth/logout` | LDAP 登入、登出與登入失敗稽核。 |
| `tasks_bp` | `/`、`/tasks*`、`/standards-legacy` | 任務列表、任務建立、任務詳細頁、Mapping、標準 Mapping、來源比對與任務輸出下載。 |
| `flow_builder_bp` | `/tasks/<task_id>/flows` | 流程編輯器與流程頁面容器。 |
| `flow_execution_bp` | `/tasks/<task_id>/flows/run`、`/execute/<flow_name>` | 流程保存、另存、建立版本與執行。 |
| `flow_crud_bp` | `/tasks/<task_id>/flows/delete/*`、`rename/*`、`export/*`、`import` | 流程檔案 CRUD、流程匯入匯出與流程轉 Mapping。 |
| `flow_results_bp` | `/tasks/<task_id>/flows/runs/*` | 流程執行狀態、結果明細、取消、重試、刪除與批次下載。 |
| `flow_file_bp` | `/api/tasks/<task_id>/flow-files*` | 任務 files/output 檔案樹 API、下載、資料夾與檔案管理。 |
| `flow_version_api_bp` / `flow_version_bp` | `/api/tasks/<task_id>/flows/<flow_name>/versions*`、`/tasks/<task_id>/flows/<flow_name>/versions/*` | 流程版本清單、建立、重新命名、刪除、下載與回復。 |
| `mapping_run_bp` | `/tasks/<task_id>/mapping/runs/*` | Mapping 執行紀錄取消、重試、刪除、狀態查詢與批次下載。 |
| `nas_bp` | `/nas/*` | NAS root 管理與 NAS 目錄瀏覽。 |
| `standard_updates_bp` | `/standards*` | 標準更新任務、輸入檔上傳、鎖定與標準更新 Mapping。 |
| `global_batch_bp` | `/batch/global*` | 全域批次流程執行與結果下載。 |

### A.2 文件處理步驟明細

| Step type | 中文標籤 | 主要輸入 | 驗證重點 |
|---|---|---|---|
| `extract_pdf_pages_as_images` | 擷取 PDF 標籤圖片 | PDF 檔、模板插入位置 | `input_file` 必填且需為 PDF。 |
| `extract_word_all_content` | 擷取 Word 全部內容 | DOCX、忽略目錄/頁首頁尾設定 | `input_file` 必填。 |
| `extract_word_chapter` | 擷取 Word 指定章節/標題 | DOCX、章節編號、章節標題、子標題、結束章節 | 章節編號與章節標題為組合必填；啟用子標題或連續擷取時有額外必填欄位。 |
| `extract_specific_figure_from_word` | 插入特定圖片 | DOCX、章節條件、caption、title、index | 圖片檢索條件至少需填 caption、title 或 index 之一。 |
| `extract_specific_table_from_word` | 插入特定表格 | DOCX、章節條件、caption、title、index | 表格檢索條件至少需填 caption、title 或 index 之一。 |
| `insert_text` | 插入純文字段落 | 文字、對齊、粗體、字級 | `text` 必填。 |
| `insert_roman_heading` | 插入羅馬數字標題 | 文字、層級、字級 | `text` 必填，編號由系統依序產生。 |
| `insert_bulleted_heading` | 插入項目符號標題 | 文字、字級 | `text` 必填。 |
| `insert_numbered_heading` | 插入阿拉伯數字標題 | 文字、層級、字級 | `text` 必填，編號由系統由 1、1.1、1.1.1 依序產生。 |
| `insert_image` | 插入圖片檔 | 圖片檔、對齊 | `input_file` 必填且需為圖片。 |
| `copy_files` | 複製檔案 | 來源、目的資料夾、關鍵字、目標名稱 | `source_dir` 必填；關鍵字留白代表複製全部檔案。 |
| `copy_directory` | 複製資料夾 | 來源資料夾、目的資料夾、關鍵字、目標名稱 | `source_dir` 必填；關鍵字留白代表複製整個來源資料夾。 |

### A.3 Mapping 轉換規則明細

| Mapping 類型 | 操作欄規則 | 轉換後 workflow step | 主要驗證 |
|---|---|---|---|
| `All` / `擷取全文` | 可留白或填 `All` | `extract_word_all_content` | 來源需解析為 Word 檔。 |
| 未填類型且操作欄為章節 | 例如 `1.2 標題`，可搭配子標題 | `extract_word_chapter` | 來源需解析為 Word 檔，章節條件需可定位。 |
| `Figure` | caption、`title=` 或 `index=` | `extract_specific_figure_from_word` | Figure 條件至少一項，index 必須為正整數。 |
| `Table` | caption、`title=` 或 `index=` | `extract_specific_table_from_word` | Table 條件至少一項，index 必須為正整數。 |
| `Figure Table` | 以 Figure 條件擷取表格內圖片 | `extract_specific_figure_from_word` 並啟用 table container | 類型與 caption kind 不可矛盾。 |
| `PDF Image` | 操作欄需留白 | `extract_pdf_pages_as_images` | 來源副檔名需為 `.pdf`。 |
| `Add Image` | 可指定插入參數 | `insert_image` | 圖片副檔名需在允許清單內。 |
| `Add Text` | 操作欄解析對齊、粗體、字級等參數 | `insert_text` | 文字內容不可空白。 |
| `Numbered/Roman/Bulleted Heading` | 操作欄解析標題層級與樣式 | `insert_numbered_heading`、`insert_roman_heading`、`insert_bulleted_heading` | 標題文字不可空白。 |
| `Copy File` | 插入欄可作為目的資料夾，關鍵字用於篩選 | 複製檔案流程 | 來源需為檔案或可搜尋目錄。 |
| `Copy Folder` | 插入欄可作為目的資料夾，關鍵字用於篩選 | 複製資料夾流程 | 來源需為資料夾。 |

### A.4 背景工作狀態

| 狀態 | 意義 | 可轉換狀態 |
|---|---|---|
| `queued` | 已建立、等待 worker claim | `claimed`、`canceled` |
| `claimed` | 已由 worker 取得但尚未正式執行 | `running`、`canceled`、`queued` |
| `running` | 執行中，worker 定期更新 heartbeat | `completed`、`failed`、`canceled`、`timeout`、`queued` |
| `completed` | 已完成 | 終止狀態 |
| `failed` | 執行失敗 | 終止狀態 |
| `canceled` | 使用者或系統取消 | 終止狀態 |
| `timeout` | 逾時或 stale job 處理結果 | 終止狀態 |

### A.5 程式分層對照

| 功能 | 路由檔案 | 服務檔案 | 模型檔案 |
|---|---|---|---|
| 登入與權限 | `app/blueprints/auth/routes.py`、`app/services/auth_admin_service.py` | `auth_service.py`、`authn_service.py`、`authz_service.py`、`auth_hooks_service.py` | `auth.py` |
| 任務管理 | `task_routes.py`、`nas_routes.py` | `task_service.py`、`nas_service.py` | `task.py`、`nas.py` |
| 流程編輯與執行 | `execution_routes.py`、`flow_crud_routes.py`、`results_routes.py` | `flow_definition_service.py`、`flow_validation_service.py`、`flow_service.py`、`execution_service.py` | `execution.py` |
| 流程版本 | `version_routes.py` | `flow_version_service.py` | 檔案型 metadata，無獨立資料表 |
| Mapping | `mapping_routes.py`、`mapping_scheme_helpers.py` | `mapping_metadata_service.py` | `mapping_metadata.py` |
| 標準對應/標準更新 | `standard_mapping_routes.py`、`standard_updates/routes.py` | `standard_mapping_service.py`、`standard_update_service.py` | `standard_update.py`、`settings.py` |
| 系統記錄 | 管理後台 view | `audit_service.py`、`operations_service.py`、`system_service.py` | `auth.py`、`settings.py` |

### A.6 資料關聯與索引補充

`user_roles` 對 `users` 與 `roles` 有外鍵，且以 unique constraint 限制一位使用者只有一個角色。其他多數業務關聯以 `task_id`、`job_id`、`scheme_id` 進行邏輯關聯，未在 ORM 層定義外鍵，原因依程式碼推測與任務檔案系統產物、舊環境相容性及刪除彈性有關。

`job_records` 具有狀態/優先權/建立時間、任務/狀態/建立時間、類型/狀態/建立時間等索引，支援 worker claim、結果列表與管理查詢。`mapping_schemes` 具有任務/更新時間與任務/狀態/更新時間索引，支援 Mapping 方案列表與狀態篩選。`mapping_runs` 具有任務/開始時間與任務/狀態/開始時間索引，支援 Mapping 執行歷程查詢。

### A.7 權限矩陣補充

| 功能範圍 | 未登入 | `editor` | `admin` | 依據 |
|---|---|---|---|---|
| 任務列表與任務功能 | 不可使用 | 可使用 | 可使用 | 全站登入檢查；未見任務功能專屬角色限制。 |
| 流程編輯與執行 | 不可使用 | 可使用 | 可使用 | 全站登入檢查；未見流程功能專屬角色限制。 |
| Mapping | 不可使用 | 可使用 | 可使用 | 全站登入檢查；未見 Mapping 功能專屬角色限制。 |
| 標準更新 | 不可使用 | 可使用 | 可使用 | 全站登入檢查；未見標準更新專屬角色限制。 |
| 使用者管理 | 不可使用 | 不可使用 | 可使用 | `user:manage` 權限只允許 admin。 |
| Audit/System error 管理頁 | 不可使用 | 不可使用 | 可使用 | 管理後台 view 以 `user_is_admin` 控制。 |

### A.8 版本回復範圍補充

流程版本回復只作用於流程 JSON。既有 job 目錄、`job_records`、`job_events`、`mapping_runs`、任務來源檔、任務 output 與標準更新任務不會跟著回復。因此，回復後再次執行流程會產生新的執行紀錄，而不會修改歷史執行結果。

### A.9 來源比對補充

比對模組會從 workflow log 建立章節來源、來源 URL、段落 trace、物件 trace candidates 與 page source map。章節來源用於整理每一個輸出章節對應的來源檔與章節條件；段落 trace 用於建立輸出段落與來源內容的對應；物件 trace candidates 用於圖片或表格等非純文字物件的來源候選；page source map 則用於 PDF 預覽頁面層級的來源標示。

## 附錄 B. 驗收項目對照表

完整表格請見 [18-驗收項目對照表.md](/home/NE025/UO_MDR/docs/system-description/18-驗收項目對照表.md)。

| 驗收項目 | 對應章節 | 主要程式或資料來源 | 待補充 |
|---|---|---|---|
| 核心資料程式 | 第 1、3、9、15、16 章 | `app/models/*`、`app/services/*` | 此處需補充「核心資料」正式範圍。 |
| 任務管理與檔案匯入程式 | 第 3、4、5 章 | `task_routes.py`、`task_service.py`、`nas_service.py` | 此處需補充來源同步 worker 完整規則。 |
| 流程管理與步驟程式 | 第 6、7、10、15 章 | `execution_routes.py`、`flow_definition_service.py`、`modules/workflow.py` | 此處需補充正式流程 JSON schema。 |
| Mapping 對應與 Excel 模板程式 | 第 2、7、8 章 | `modules/mapping_processor.py`、`mapping_routes.py`、`static/samples/mapping_example.xlsx` | 此處需補充正式 Mapping 範本。 |
| 翻譯模組程式 | 第 6、10、15 章 | `modules/translate_with_bedrock.py`、`tasks_bp.task_translate` | 此處需補充正式翻譯設定與資料外送規範。 |
| 來源比對／線上編輯與檔核程式 | 第 13 章 | `compare_routes.py`、`compare_helpers.py`、`docx_provenance.py` | 此處需補充「線上編輯」與「檔核」驗收定義。 |
| 版本控制與中繼資料管理程式 | 第 9、11、12、16 章 | `flow_version_service.py`、`mapping_metadata.py`、`execution.py` | 此處需補充是否要求任務產物整體版本化。 |
| 失敗與例外處理 | 第 9、17 章 | `audit_service.py`、`execution_service.py`、`mapping_processor.py` | 此處需補充正式錯誤代碼規範。 |
| 其它相關程式 | 第 14、15、16 章 | `auth_*`、`standard_update_service.py`、`operations_service.py` | 此處需補充驗收方「其它」範圍。 |

## 附錄 C. 系統圖與流程圖

完整 Mermaid 圖請見 [19-系統圖與流程圖.md](/home/NE025/UO_MDR/docs/system-description/19-系統圖與流程圖.md)。目前已補上：

- 系統架構圖
- 任務匯入與來源同步流程
- 流程執行流程
- Mapping 檢查與執行流程
- 流程版本回復流程
- ERD 摘要

## 附錄 D. API 與路由清單

完整路由清單請見 [20-API與路由清單.md](/home/NE025/UO_MDR/docs/system-description/20-API與路由清單.md)。該清單由 `/home/NE025/UO_MDR/.venv` 測試環境呼叫 `create_app('testing')` 後的 Flask `url_map` 產生，排除 static 檔案路由。當 `AUTH_ENABLED=True` 時，除 `/auth/login`、`/auth/logout` 與 static asset 外，皆需登入。

## 附錄 E. 資料表欄位字典

完整欄位字典請見 [21-資料表欄位字典.md](/home/NE025/UO_MDR/docs/system-description/21-資料表欄位字典.md)。該字典由 SQLAlchemy metadata 整理，涵蓋：

- `users`、`roles`、`user_roles`
- `tasks`
- `job_records`、`job_events`、`job_artifacts`、`task_execution_locks`
- `mapping_schemes`、`mapping_runs`、`task_file_states`
- `audit_logs`、`system_error_logs`
- `nas_roots`、`system_settings`、`regulation_sync_states`
- `standard_update_tasks`、`harmonised_releases`

## 本文件依據資料來源

- `SYSTEM_ARCHITECTURE.md`
- `SYSTEM_ENVIRONMENT.md`
- `ENVIRONMENT.md`
- `OPERATIONS.md`
- `pyproject.toml`
- `app/__init__.py`
- `app/config.py`
- `app/blueprints/auth/routes.py`
- `app/blueprints/tasks/task_routes.py`
- `app/blueprints/tasks/nas_routes.py`
- `app/blueprints/tasks/mapping_routes.py`
- `app/blueprints/tasks/compare_routes.py`
- `app/blueprints/tasks/standard_mapping_routes.py`
- `app/blueprints/flows/execution_routes.py`
- `app/blueprints/flows/version_routes.py`
- `app/blueprints/flows/flow_file_routes.py`
- `app/blueprints/flows/results_routes.py`
- `app/blueprints/standard_updates/routes.py`
- `app/models/auth.py`
- `app/models/task.py`
- `app/models/execution.py`
- `app/models/mapping_metadata.py`
- `app/models/nas.py`
- `app/models/settings.py`
- `app/models/standard_update.py`
- `app/services/auth_service.py`
- `app/services/authn_service.py`
- `app/services/authz_service.py`
- `app/services/auth_hooks_service.py`
- `app/services/auth_admin_service.py`
- `app/services/audit_service.py`
- `app/services/task_service.py`
- `app/services/nas_service.py`
- `app/services/execution_service.py`
- `app/services/flow_definition_service.py`
- `app/services/flow_validation_service.py`
- `app/services/flow_service.py`
- `app/services/flow_version_service.py`
- `app/services/mapping_metadata_service.py`
- `app/services/standard_mapping_service.py`
- `app/jobs/executor.py`
- `modules/workflow.py`
- `modules/mapping_processor.py`
- `modules/extract_word_chapter.py`
- `modules/extract_word_all_content.py`
- `modules/extract_specific_figure_xml.py`
- `modules/extract_specific_table_xml.py`
- `modules/extract_pdf_img.py`
- `modules/docx_provenance.py`
- `migrations/versions/0001_baseline_schema.py`
- `tests/test_mapping_processor_isolation.py`
- `tests/test_mapping_route_errors.py`
- `tests/test_standard_mapping_priority_rules.py`
- `tests/test_flow_export_mapping_excel.py`
- `tests/test_compare_view.py`
- `tests/test_docx_provenance.py`
- `tests/test_task_service_source_files.py`
- `.venv` 測試環境下 `create_app('testing')` 產生之 Flask `url_map`
- `.venv` 測試環境下 SQLAlchemy `db.metadata`

## 待補充項目總表

| 章節 | 待補充項目 |
|---|---|
| 1 | 正式部署環境中的實際 Nginx 網域、服務名稱與網路拓撲圖。 |
| 1 | 論文中需要呈現的系統架構圖。 |
| 2 | 正式交付使用的 Mapping 範本欄位說明與範例列。 |
| 2 | Mapping 欄位允許值完整清單與業務規則說明。 |
| 3 | 任務刪除權限的正式政策描述。 |
| 3 | 任務狀態在畫面上的完整顯示規格。 |
| 4 | 正式環境 NAS 掛載點與允許根目錄清單。 |
| 4 | 各類上傳檔案大小限制的正式設定值。 |
| 5 | `task_source_sync` worker 內部複製規則的完整設計說明。 |
| 5 | 同步失敗時是否保留部分檔案與重試策略。 |
| 6 | 每一種文件處理步驟的業務輸入範例。 |
| 6 | 處理大型文件時的效能限制與逾時設定。 |
| 7 | 流程編輯器 UI 與流程 JSON schema 的正式規格。 |
| 7 | 流程節點之間是否允許條件式分支。 |
| 8 | Excel Mapping「相似度或比對門檻」的正式設計值。 |
| 8 | 人工確認畫面截圖與欄位對應操作設計。 |
| 9 | 正式維運時 log 查詢與保留政策。 |
| 9 | 稽核紀錄對應論文或驗收需求的保存年限。 |
| 10 | 文件處理流程圖。 |
| 10 | 各流程節點的業務名稱與論文用語對應。 |
| 11 | 比對結果版本與流程版本在驗收文件中的分類方式。 |
| 11 | 版本保存上限是否符合正式需求。 |
| 12 | 正式需求是否要支援任務資料或產物的完整回復。 |
| 12 | 回復操作的權限限制。 |
| 13 | 來源比對畫面的截圖與使用情境說明。 |
| 13 | 差異標示的正式圖例。 |
| 14 | 是否需要任務層級資料存取限制。 |
| 14 | AD 群組 DN 與正式角色指派流程。 |
| 15 | 正式程式模組責任分工圖。 |
| 15 | 重要函式的時序圖。 |
| 16 | 正式 ERD。 |
| 16 | 資料表清理與備份政策。 |
| 17 | 正式錯誤代碼命名規範。 |
| 17 | 管理者 SOP 與截圖。 |
