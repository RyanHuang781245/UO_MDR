# 附錄 D. API 與路由清單

## 主要依據檔案

- `.venv` 測試環境下 `create_app('testing')` 產生之 Flask `url_map`
- `app/blueprints/*`
- `app/services/auth_hooks_service.py`

## 路由清單

以下清單由 Flask `url_map` 產生，排除 static 檔案路由。當 `AUTH_ENABLED=True` 時，除 `/auth/login`、`/auth/logout` 與 static asset 外，皆需登入。

| 方法 | 路徑 | Endpoint |
|---|---|---|
| `GET` | `/` | `tasks_bp.launcher` |
| `GET` | `/admin/` | `admin.index` |
| `GET,POST` | `/admin/ad-search/` | `ad_search.index` |
| `GET` | `/admin/audit-logs/` | `audit_logs.index` |
| `GET` | `/admin/audit-logs/download` | `audit_logs.download` |
| `GET` | `/admin/system-error-logs/` | `system_error_logs.index` |
| `GET,POST` | `/admin/system-settings/` | `system_settings.index` |
| `GET` | `/admin/user/` | `user.index_view` |
| `POST` | `/admin/user/action/` | `user.action_view` |
| `GET` | `/admin/user/ajax/lookup/` | `user.ajax_lookup` |
| `POST` | `/admin/user/ajax/update/` | `user.ajax_update` |
| `POST` | `/admin/user/delete/` | `user.delete_view` |
| `GET` | `/admin/user/details/` | `user.details_view` |
| `GET,POST` | `/admin/user/edit/` | `user.edit_view` |
| `GET` | `/admin/user/export/<export_type>/` | `user.export` |
| `GET,POST` | `/admin/user/new/` | `user.create_view` |
| `GET` | `/api/tasks/<task_id>/flow-files` | `flow_file_bp.api_flow_list_task_files` |
| `POST` | `/api/tasks/<task_id>/flow-files/clear` | `flow_file_bp.api_flow_clear_task_scope` |
| `GET` | `/api/tasks/<task_id>/flow-files/download` | `flow_file_bp.api_flow_download_task_file` |
| `GET` | `/api/tasks/<task_id>/flow-files/download-zip` | `flow_file_bp.api_flow_download_task_scope_zip` |
| `POST` | `/api/tasks/<task_id>/flow-files/entries/delete` | `flow_file_bp.api_flow_delete_task_entry` |
| `POST` | `/api/tasks/<task_id>/flow-files/entries/rename` | `flow_file_bp.api_flow_rename_task_entry` |
| `POST` | `/api/tasks/<task_id>/flow-files/folders` | `flow_file_bp.api_flow_create_task_folder` |
| `POST` | `/api/tasks/<task_id>/flow-files/folders/delete` | `flow_file_bp.api_flow_delete_task_folder` |
| `POST` | `/api/tasks/<task_id>/flow-files/folders/rename` | `flow_file_bp.api_flow_rename_task_folder` |
| `GET` | `/api/tasks/<task_id>/flows/<flow_name>/versions` | `flow_version_api_bp.list_flow_versions` |
| `POST` | `/api/tasks/<task_id>/flows/<flow_name>/versions` | `flow_version_api_bp.create_flow_version` |
| `POST` | `/api/tasks/<task_id>/flows/<flow_name>/versions/<version_id>/delete` | `flow_version_api_bp.delete_flow_version` |
| `POST` | `/api/tasks/<task_id>/flows/<flow_name>/versions/<version_id>/rename` | `flow_version_api_bp.rename_flow_version` |
| `GET,POST` | `/auth/login` | `auth_bp.login` |
| `GET` | `/auth/logout` | `auth_bp.logout` |
| `GET` | `/batch/global` | `global_batch_bp.global_batch_page` |
| `POST` | `/batch/global/<batch_id>/delete` | `global_batch_bp.delete_global_batch` |
| `POST` | `/batch/global/<batch_id>/download` | `global_batch_bp.download_global_batch` |
| `GET` | `/batch/global/<batch_id>/status` | `global_batch_bp.global_batch_status` |
| `POST` | `/batch/global/run` | `global_batch_bp.run_global_batch` |
| `POST` | `/nas/add-root` | `nas_bp.add_nas_root_route` |
| `GET` | `/nas/dirs` | `nas_bp.api_nas_list_dirs` |
| `POST` | `/nas/remove-root` | `nas_bp.remove_nas_root_route` |
| `GET,POST` | `/standards` | `standard_updates_bp.list` |
| `GET` | `/standards-legacy` | `tasks_bp.standards` |
| `GET` | `/standards/<task_id>` | `standard_updates_bp.detail` |
| `POST` | `/standards/<task_id>/delete` | `standard_updates_bp.delete` |
| `POST` | `/standards/<task_id>/description` | `standard_updates_bp.update_description` |
| `POST` | `/standards/<task_id>/download` | `standard_updates_bp.download_result` |
| `POST` | `/standards/<task_id>/files/delete` | `standard_updates_bp.delete_input_file` |
| `POST` | `/standards/<task_id>/lock/refresh` | `standard_updates_bp.refresh_lock` |
| `POST` | `/standards/<task_id>/lock/release` | `standard_updates_bp.release_lock` |
| `POST` | `/standards/<task_id>/lock/takeover` | `standard_updates_bp.takeover_lock` |
| `GET,POST` | `/standards/<task_id>/mapping` | `standard_updates_bp.mapping` |
| `POST` | `/standards/<task_id>/rename` | `standard_updates_bp.rename` |
| `POST` | `/standards/<task_id>/upload-harmonised-excel` | `standard_updates_bp.upload_harmonised_excel` |
| `POST` | `/standards/<task_id>/upload-regulation-excel` | `standard_updates_bp.upload_regulation_excel` |
| `POST` | `/standards/<task_id>/upload-standard-excel` | `standard_updates_bp.upload_standard_excel` |
| `POST` | `/standards/<task_id>/upload-word` | `standard_updates_bp.upload_word` |
| `POST` | `/standards/<task_id>/use-latest-harmonised` | `standard_updates_bp.use_latest_harmonised` |
| `GET` | `/tasks` | `tasks_bp.tasks` |
| `POST` | `/tasks` | `tasks_bp.create_task` |
| `GET` | `/tasks/<task_id>` | `tasks_bp.task_detail` |
| `GET` | `/tasks/<task_id>/compare/<job_id>` | `tasks_bp.task_compare` |
| `POST` | `/tasks/<task_id>/compare/<job_id>/delete/<version_id>` | `tasks_bp.task_compare_delete_version` |
| `POST` | `/tasks/<task_id>/compare/<job_id>/restore/<version_id>` | `tasks_bp.task_compare_restore_version` |
| `POST` | `/tasks/<task_id>/compare/<job_id>/save` | `tasks_bp.task_compare_save` |
| `POST` | `/tasks/<task_id>/compare/<job_id>/save-as` | `tasks_bp.task_compare_save_as` |
| `POST` | `/tasks/<task_id>/copy` | `tasks_bp.copy_task` |
| `POST` | `/tasks/<task_id>/delete` | `tasks_bp.delete_task` |
| `POST` | `/tasks/<task_id>/description` | `tasks_bp.update_task_description` |
| `GET` | `/tasks/<task_id>/download/<job_id>/<kind>` | `tasks_bp.task_download` |
| `GET` | `/tasks/<task_id>/download/<job_id>/version/<version_id>` | `tasks_bp.task_download_version` |
| `GET` | `/tasks/<task_id>/flows` | `flow_builder_bp.flow_builder` |
| `GET` | `/tasks/<task_id>/flows/<flow_name>/versions/<version_id>/download` | `flow_version_bp.download_flow_version` |
| `POST` | `/tasks/<task_id>/flows/<flow_name>/versions/<version_id>/restore` | `flow_version_bp.restore_flow_version` |
| `POST` | `/tasks/<task_id>/flows/delete/<flow_name>` | `flow_crud_bp.delete_flow` |
| `POST` | `/tasks/<task_id>/flows/execute/<flow_name>` | `flow_execution_bp.execute_flow` |
| `GET` | `/tasks/<task_id>/flows/export-mapping/<flow_name>` | `flow_crud_bp.export_flow_mapping` |
| `POST` | `/tasks/<task_id>/flows/export-mapping/merged` | `flow_crud_bp.export_merged_flow_mapping` |
| `GET` | `/tasks/<task_id>/flows/export/<flow_name>` | `flow_crud_bp.export_flow` |
| `POST` | `/tasks/<task_id>/flows/import` | `flow_crud_bp.import_flow` |
| `POST` | `/tasks/<task_id>/flows/rename/<flow_name>` | `flow_crud_bp.rename_flow` |
| `GET` | `/tasks/<task_id>/flows/results` | `flow_results_bp.flow_results` |
| `POST` | `/tasks/<task_id>/flows/run` | `flow_execution_bp.run_flow` |
| `POST` | `/tasks/<task_id>/flows/runs/<job_id>/cancel` | `flow_results_bp.cancel_flow_run` |
| `POST` | `/tasks/<task_id>/flows/runs/<job_id>/delete` | `flow_results_bp.delete_flow_run` |
| `GET` | `/tasks/<task_id>/flows/runs/<job_id>/detail` | `flow_results_bp.flow_run_detail` |
| `POST` | `/tasks/<task_id>/flows/runs/<job_id>/retry` | `flow_results_bp.retry_flow_run` |
| `GET` | `/tasks/<task_id>/flows/runs/<job_id>/status` | `flow_results_bp.flow_run_status` |
| `GET` | `/tasks/<task_id>/flows/runs/active` | `flow_results_bp.flow_run_active` |
| `POST` | `/tasks/<task_id>/flows/runs/delete` | `flow_results_bp.delete_flow_runs_bulk` |
| `POST` | `/tasks/<task_id>/flows/runs/download` | `flow_results_bp.download_flow_runs_bulk` |
| `POST` | `/tasks/<task_id>/flows/update-format/<flow_name>` | `flow_execution_bp.update_flow_format` |
| `GET,POST` | `/tasks/<task_id>/mapping` | `tasks_bp.task_mapping` |
| `GET` | `/tasks/<task_id>/mapping/example` | `tasks_bp.task_download_mapping_example` |
| `GET` | `/tasks/<task_id>/mapping/ops/<op_id>/status` | `tasks_bp.task_mapping_op_status` |
| `GET` | `/tasks/<task_id>/mapping/ops/active` | `tasks_bp.task_mapping_op_active` |
| `POST` | `/tasks/<task_id>/mapping/runs/<run_id>/cancel` | `mapping_run_bp.cancel_mapping_run` |
| `POST` | `/tasks/<task_id>/mapping/runs/<run_id>/delete` | `mapping_run_bp.delete_mapping_run` |
| `POST` | `/tasks/<task_id>/mapping/runs/<run_id>/retry` | `mapping_run_bp.retry_mapping_run` |
| `GET` | `/tasks/<task_id>/mapping/runs/<run_id>/status` | `mapping_run_bp.mapping_run_status` |
| `POST` | `/tasks/<task_id>/mapping/runs/delete` | `mapping_run_bp.delete_mapping_runs_bulk` |
| `POST` | `/tasks/<task_id>/mapping/runs/download` | `mapping_run_bp.download_mapping_runs_bulk` |
| `GET` | `/tasks/<task_id>/mapping/schemes/<scheme_id>/download` | `tasks_bp.task_download_mapping_scheme` |
| `GET` | `/tasks/<task_id>/mapping/schemes/<scheme_id>/logs/<kind>` | `tasks_bp.task_download_mapping_scheme_log` |
| `GET` | `/tasks/<task_id>/mapping/validation/<run_id>/logs/<kind>` | `tasks_bp.task_download_mapping_validation_log` |
| `GET` | `/tasks/<task_id>/nas-diff` | `tasks_bp.task_nas_diff` |
| `GET` | `/tasks/<task_id>/output/<path:filename>` | `tasks_bp.task_download_output` |
| `GET` | `/tasks/<task_id>/output/download` | `tasks_bp.task_download_output_query` |
| `POST` | `/tasks/<task_id>/rename` | `tasks_bp.rename_task` |
| `GET` | `/tasks/<task_id>/result/<job_id>` | `tasks_bp.task_result` |
| `GET,POST` | `/tasks/<task_id>/standard-mapping` | `tasks_bp.task_standard_mapping` |
| `POST` | `/tasks/<task_id>/standard-mapping/download` | `tasks_bp.task_standard_mapping_download` |
| `POST` | `/tasks/<task_id>/sync-nas` | `tasks_bp.sync_task_nas` |
| `POST` | `/tasks/<task_id>/templates/parse` | `tasks_bp.parse_template_doc` |
| `GET` | `/tasks/<task_id>/translate/<job_id>` | `tasks_bp.task_translate` |
| `GET` | `/tasks/<task_id>/view/<job_id>/<path:filename>` | `tasks_bp.task_view_file` |

## 待補充項目

- 此處需補充各 POST 路由的表單欄位、JSON request/response schema 與畫面觸發來源。

