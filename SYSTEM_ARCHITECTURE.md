# System Architecture

## Overview
UO_MDR is a Flask-based document processing workstation that hosts task-specific workspaces, runs configurable workflows, and surfaces results through server-rendered HTML templates. The application is organized around discrete task directories that hold uploaded files, workflow outputs, and versioned comparison artifacts.

## Runtime topology
- **Web layer (Flask, `app.py`)**: Handles task lifecycle (create, rename, delete), file management, workflow execution, translation, and document comparison endpoints. Views render HTML templates for task dashboards, workflow editors, and result viewers.
- **Task storage (`tasks/`)**: Each task directory contains a `files/` subfolder for uploads, optional `meta.json` descriptors, and per-job subfolders that store workflow outputs, comparison HTML, and versioned copies of edited files. The `output/` directory mirrors downloadable artifacts keyed by task ID.
- **Workflow engine (`modules/workflow.py`)**: Exposes a registry of supported workflow steps and executes them in order, producing a combined Word document plus a structured log for downstream UI display. Steps include PDF/Word extraction, heading insertion, content insertion, file copying, and figure/table renumbering.
- **Document processing utilities**: Specialized modules provide focused capabilities:
  - `modules/Extract_AllFile_to_FinalWord.py` supplies extraction helpers and formatting utilities used during workflow runs.
  - `modules/Edit_Word.py` contributes heading and paragraph insertion helpers that the workflow engine invokes.
  - `modules/file_copier.py` copies files whose names match user-provided keywords, used by both the workflow engine and the copy-files UI.
  - `modules/mapping_processor.py` transforms mapping spreadsheets into packaged outputs against a task’s `files/` directory.
  - `modules/translate_with_bedrock.py` streams text from DOCX/PDF/TXT sources through AWS Bedrock for translation with retry-aware chunking.
- **Presentation layer (`templates/`, `static/`)**: Bootstrap-flavored HTML templates drive task pages, workflow editors, comparison views, and mapping/copy-file utilities. Static assets support styling and client-side interactions.

## Request flow highlights
1. **Task creation**: Users upload a ZIP archive; the server extracts it under `tasks/<id>/files/` and records metadata so the task appears on the landing page. Subsequent uploads or renames update the same folder.
2. **Workflow execution**: Task-specific flow definitions (stored as JSON under `tasks/<id>/flows/`) are run via `/tasks/<task_id>/flows/run`. The engine streams document edits into a single Word file and writes a `log.json` detailing step parameters, statuses, and captured titles. Results are surfaced under `output/<task_id>/` for download and comparison.
3. **File utilities**: The copy-files route lets users create directories and copy matched files within a task workspace, while the mapping route processes uploaded Excel mappings against task files and emits packaged outputs.
4. **Translation**: Uploaded or workflow-generated DOCX/PDF/TXT files can be translated through AWS Bedrock models, with chunked requests and retry logic ensuring robustness against transient failures.
5. **Comparison and versioning**: When workflows generate comparison HTML, users can clean the content, hide captured titles, and save labeled versions. Saved versions live under the task’s job folder with downloadable ZIP bundles for traceability.

## Key interactions
- Routes in `app.py` orchestrate filesystem operations, marshal parameters into workflow steps, and persist artifacts so subsequent endpoints (download, view, compare) can serve them directly from disk.
- Workflow steps read from the task’s `files/` tree and write images, Word documents, and logs into job-specific directories, enabling iterative runs without cross-task contamination.
- Utility modules are deliberately stateless and accept explicit file paths, making it straightforward to reuse them in new workflow steps or CLIs without additional global configuration.
