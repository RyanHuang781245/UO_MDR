# 2. Mapping 設計規範說明

## 主要依據檔案

- `modules/mapping_processor.py`
- `app/blueprints/tasks/mapping_routes.py`
- `app/blueprints/tasks/mapping_scheme_helpers.py`
- `app/models/mapping_metadata.py`
- `tests/test_mapping_processor_isolation.py`
- `tests/test_mapping_route_errors.py`
- `static/samples/mapping_example.xlsx`

## Mapping 用途

Mapping 用於將 Excel 中的列資料轉換為文件處理步驟。系統會讀取使用者上傳或由流程匯出的 Mapping 檔，依列定義來源檔案、擷取段落或操作、類型、是否包含標題、輸出路徑、輸出檔名、模板檔與插入段落，最後產生 Word 文件、複製檔案/資料夾或封裝輸出 ZIP。

## 欄位設計

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

## 對應規則

系統依 `item_type` 與 `operation` 判斷工作步驟。支援類型包含全文擷取、章節擷取、Figure、Table、表格包圖、PDF 轉圖片、加入圖片、加入文字、羅馬/項目符號/阿拉伯數字標題、複製檔案與複製資料夾。未指定類型時，系統會依操作欄內容推測章節擷取或圖表擷取。

Figure/Table 可透過 caption、title 或 index 指定目標；若使用 `title` 或 `index` 參數，必須明確指定 Figure 或 Table 標籤。Table 擷取至少需提供 caption、title 或 index 其中之一；Figure 擷取亦同。

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

## 方案狀態與資料更新

Mapping 方案儲存在 `mapping_schemes`。`reference_ok` 表示引用文件檢查通過，`extract_ok` 表示擷取條件檢查通過，`validated_against_revision` 對應任務檔案版本。當任務來源檔案異動時，`task_file_states.files_revision` 會更新；若方案驗證版本落後於目前版本，狀態會被推導為 `needs_review`，代表需重新檢查後才適合執行。

Mapping 執行紀錄儲存在 `mapping_runs`，包含狀態、輸出數量、ZIP、log、錯誤訊息、檢查結果與來源類型。實際執行產物保存在任務的 `mapping_job/<run_id>` 目錄。

## 驗證與異常

系統提供引用文件檢查與擷取條件檢查，並將 Mapping 方案狀態記錄於 `mapping_schemes`。若任務檔案版本已更新，已驗證方案可能變成 `needs_review`。常見錯誤包含找不到新版 Mapping 表頭、缺少輸出檔名、輸出檔名非 `.docx`、缺少來源檔案、來源解析失敗、模板不存在、插入段落不存在、Figure/Table 條件不足與複製失敗。

## 限制

舊版無表頭 Mapping 格式已停用。系統依 Excel 第一個工作表處理 Mapping；多工作表 Mapping 規則此處需補充。相似度自動對應主要見於標準對應模組，非一般文件 Mapping 核心流程。

## 待補充項目

- 此處需補充正式交付使用的 Mapping 範本欄位說明與範例列。
- 此處需補充 Mapping 欄位允許值完整清單與業務規則說明。
