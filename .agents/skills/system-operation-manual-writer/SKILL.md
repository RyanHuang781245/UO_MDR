---
name: system-operation-manual-writer
description: Use this skill when the user asks Codex to create, revise, or complete a system operation manual, user manual, admin manual, deployment manual, or maintenance document based on source code, routes, controllers, UI pages, database schema, README files, API documentation, screenshots, or workflow notes.
---

# System Operation Manual Writer

## Goal

Create a complete, formal, and technically accurate system operation manual for the current software project.

The manual should explain how users operate the system, how administrators manage data, and how maintainers understand basic deployment and troubleshooting procedures.

## When to Use

Use this skill when the user asks for any of the following:

- 系統操作手冊
- 使用者手冊
- 管理者操作手冊
- 後台操作說明
- 系統維護手冊
- 部署與維護文件
- 功能操作說明
- User manual
- Admin manual
- Operation manual

## Primary Sources to Inspect

Before writing the manual, inspect available project materials in this priority order:

1. README, docs, specification files, and existing manuals
2. Frontend routes, pages, views, components, templates, and UI text
3. Backend routes, controllers, services, API handlers, serializers, and middleware
4. Database schema, migrations, models, seed data, and enum definitions
5. Authentication, authorization, role, and permission logic
6. Configuration files, environment variables, deployment scripts, Docker files, and CI files
7. Error messages, validation rules, logs, and exception handling
8. Test files that reveal expected behavior

Do not invent unsupported functions. If a detail cannot be confirmed from the project files, write 「此處需補充」.

## Output Language and Style

- Use Traditional Chinese.
- Use formal technical documentation style.
- Avoid casual wording.
- Prefer complete paragraphs and clear procedural steps.
- Avoid unnecessary English terminology.
- If an English technical term is necessary, write the Chinese term first and the English term in parentheses.
- Do not fabricate screenshots, test results, system features, performance numbers, or unsupported API behavior.
- If the behavior is inferred from source code rather than explicit documentation, state 「依程式碼推測」.

## Standard Manual Structure

Unless the user specifies another format, produce the manual using this structure:

# 系統操作手冊

## 1. 系統概述

Explain:
- System purpose
- Main users
- Main business or research scenario
- Core modules
- Data flow overview

## 2. 系統環境需求

Include when available:
- Operating system
- Runtime environment
- Database
- Required services
- Browser requirements
- Required environment variables
- Deployment dependencies

If unknown, mark as 「此處需補充」。

## 3. 使用者角色與權限

Explain all confirmed roles and permissions.

Use a table:

| 角色 | 可使用功能 | 權限限制 | 備註 |
|---|---|---|---|

If roles are not found, write 「此處需補充使用者角色與權限設定」。

## 4. 系統登入與登出

Include:
- Login path
- Required fields
- Login steps
- Login success behavior
- Login failure behavior
- Logout steps
- Session or token behavior if available

## 5. 主畫面與導覽說明

Include:
- Main dashboard purpose
- Navigation menu
- Major buttons
- Status indicators
- Common UI elements

If screenshots are needed, insert:
「此處建議補充畫面截圖：主畫面」。

## 6. 功能模組操作說明

For each confirmed module, use this format:

### 6.x 模組名稱

#### 6.x.1 功能目的
Explain what this module is used for.

#### 6.x.2 操作路徑
Write the page path, menu path, route, or API path if available.

#### 6.x.3 操作步驟
Use numbered steps.

1. 使用者進入……
2. 使用者輸入……
3. 使用者點選……
4. 系統顯示……

#### 6.x.4 欄位說明

| 欄位名稱 | 必填 | 說明 | 格式或限制 |
|---|---|---|---|

#### 6.x.5 系統回應
Explain success behavior, displayed results, saved data, or generated output.

#### 6.x.6 注意事項
List constraints, validation rules, permission limits, or known edge cases.

#### 6.x.7 錯誤處理
List possible errors and how users should respond.

| 錯誤情境 | 可能原因 | 處理方式 |
|---|---|---|

## 7. 資料查詢、新增、修改與刪除

If CRUD functions exist, explain:
- 查詢條件
- 新增資料流程
- 修改資料流程
- 刪除資料流程
- 刪除前確認機制
- 資料驗證規則
- 權限限制

## 8. 檔案上傳與下載

If file handling exists, explain:
- Supported file types
- File size limits if found
- Upload path
- Download path
- Parsing or validation behavior
- Common upload errors

If file behavior is not found, omit this section or mark as 「此處需補充」。

## 9. 報表、匯出與列印

If export/report functions exist, explain:
- Export format
- Export path
- Filter criteria
- Generated file naming
- Report content

## 10. 錯誤訊息與排除方式

Summarize confirmed errors from code, validation messages, logs, or API responses.

| 錯誤訊息 | 發生原因 | 使用者處理方式 | 管理者處理方式 |
|---|---|---|---|

## 11. 系統維護與備份

Include only confirmed information:
- Database backup
- Log location
- Service restart
- Environment configuration
- Scheduled jobs
- External services
- Deployment notes

If unavailable, write 「此處需補充系統維護與備份程序」。

## 12. 常見問題

Write FAQ based on actual system behavior.

## 13. 附錄

Include when available:
- API list
- Database table summary
- Role permission table
- Environment variable table
- Route list
- Glossary

## Evidence and Traceability Rules

When possible, mention the source file path near the relevant section in comments or notes, for example:

「依據 `src/routes/task.py` 與 `src/templates/tasks.html` 整理。」

Do not expose unnecessary implementation details to end users unless the manual is intended for administrators or maintainers.

## Final Quality Check

Before finalizing the manual, verify:

- Every described function exists in the project files.
- Every operation has a clear path and ordered steps.
- Missing details are marked as 「此處需補充」.
- No unsupported screenshots, results, or features are invented.
- Role and permission descriptions are consistent with the code.
- Error handling is included where validation or exceptions exist.
- The writing style is formal and suitable for a thesis appendix, project handover, or internal documentation.