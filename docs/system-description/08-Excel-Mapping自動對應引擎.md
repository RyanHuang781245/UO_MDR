# 8. Excel Mapping 自動對應引擎

## 主要依據檔案

- `modules/mapping_processor.py`
- `app/blueprints/tasks/mapping_routes.py`
- `app/blueprints/tasks/standard_mapping_routes.py`
- `app/services/standard_mapping_service.py`
- `tests/test_standard_mapping_priority_rules.py`
- `tests/test_flow_export_mapping_excel.py`

## 一般文件 Mapping 引擎

一般文件 Mapping 引擎會讀取 Excel 表頭別名，將每列轉換為 workflow step。依程式碼確認，此處的「自動對應」主要是表頭別名辨識、類型正規化、操作欄解析與來源檔案解析，不是以語意相似度自動推論任意欄位。

## 標準對應引擎

標準對應模組使用 `standard_mapping_service.py`，其規則包含標準名稱正規化、年份擷取、ISO/EN/BS 等標準層級優先順序、歐盟採用標準比對、標題欄位候選與標準表欄位辨識。程式碼中使用 `difflib.SequenceMatcher`，可推測部分比對會使用字串相似度；具體門檻值與完整人工確認流程此處需補充。

## 人工確認機制

依程式碼推測，標準對應頁面提供欄位檢查、預覽與下載結果流程，若未符合兩欄格式或未完成手動對應欄位設定，系統會要求先完成欄位設定後再下載。一般 Mapping 方案則需先通過引用文件檢查與擷取條件檢查，狀態為 ready 後才可執行或設為排程。

## 失敗情境與限制

失敗情境包含 Excel 暫存檔、Excel 無法讀取、缺少必要表頭、找不到 Mapping 方案、Mapping 方案需要重新檢查、Mapping 方案尚未通過檢查、標準對應欄位格式不符與下載失敗。自動對應結果仍依來源資料品質與欄位命名而定。

## 標準對應正規化規則

標準對應服務會將標準編號轉為較穩定的查詢鍵，處理大小寫、全形/半形符號、空白、冒號、斜線、加號與括號等差異。年份欄位只接受 1900 到 2099 的四位數年份。歐盟採用標準比對會從參考檔文字中擷取 EN、ISO、BS、DIN、IEC、ASTM 等開頭的標準項目，再建立可查詢 key。

## 優先順序

依程式碼確認，標準層級包含 `BS EN ISO`、`BS EN`、`EN`、`EN ISO`、`BS ISO`、`ISO`、`BS`。預設啟用的歐盟優先層級為 `BS EN ISO`、`BS EN`、`EN`、`EN ISO`，且預設偏好最新 EN 變體。此設計用於在多個候選標準中選擇較符合歐盟採用標準情境的結果。

## 欄位辨識

標準對應會辨識 `Standards`、`Issued Year`、`EU Harmonised Standards under MDR 2017/745 (YES/NO)`、`Title` 等表頭，並提供 aliases，例如 `Standard No`、`Issue Year`、`EU Harmonized Standards under MDR 2017/745`、`Standard Title`。若 Excel 表頭不符合預期，使用者需進行手動欄位對應或補充欄位設定。

## 待補充項目

- 此處需補充 Excel Mapping「相似度或比對門檻」的正式設計值。
- 此處需補充人工確認畫面截圖與欄位對應操作設計。
