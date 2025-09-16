import os
import uuid
import json
import zipfile
import re
from datetime import datetime
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    send_file,
    send_from_directory,
    abort,
)
from werkzeug.utils import secure_filename
from modules.workflow import SUPPORTED_STEPS, run_workflow
from modules.Extract_AllFile_to_FinalWord import (
    center_table_figure_paragraphs,
    apply_basic_style,
    remove_hidden_runs,
)
from modules.Edit_Word import renumber_figures_tables_file
from modules.translate_with_bedrock import translate_file
from modules.file_copier import copy_files

app = Flask(__name__, instance_relative_config=True)
app.config["SECRET_KEY"] = "dev-secret"
BASE_DIR = os.path.dirname(__file__)
app.config["OUTPUT_FOLDER"] = os.path.join(BASE_DIR, "output")
app.config["TASK_FOLDER"] = os.path.join(BASE_DIR, "tasks")
os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)
os.makedirs(app.config["TASK_FOLDER"], exist_ok=True)

ALLOWED_DOCX = {".docx"}
ALLOWED_PDF = {".pdf"}
ALLOWED_ZIP = {".zip"}

DOCUMENT_FORMAT_PRESETS = {
    "default": {
        "label": "Times New Roman / 新細明體（12 pt）",
        "western_font": "Times New Roman",
        "east_asian_font": "新細明體",
        "font_size": 12,
        "space_before": 6,
        "space_after": 6,
    },
    "modern": {
        "label": "Calibri / 微軟正黑體（12 pt）",
        "western_font": "Calibri",
        "east_asian_font": "微軟正黑體",
        "font_size": 12,
        "space_before": 6,
        "space_after": 6,
    },
}
DEFAULT_DOCUMENT_FORMAT_KEY = "default"
DEFAULT_LINE_SPACING = 1.5
LINE_SPACING_CHOICES = [
    ("1", "單行（1.0）"),
    ("1.15", "1.15 倍行距"),
    ("1.5", "1.5 倍行距"),
    ("2", "雙行（2.0）"),
]


def normalize_document_format(key: str) -> str:
    if not key or key not in DOCUMENT_FORMAT_PRESETS:
        return DEFAULT_DOCUMENT_FORMAT_KEY
    return key


def coerce_line_spacing(value) -> float:
    try:
        spacing = float(value)
        if spacing <= 0:
            raise ValueError
        return spacing
    except (TypeError, ValueError):
        return DEFAULT_LINE_SPACING


def allowed_file(filename, kinds=("docx", "pdf", "zip")):
    ext = os.path.splitext(filename)[1].lower()
    if "docx" in kinds and ext in ALLOWED_DOCX:
        return True
    if "pdf" in kinds and ext in ALLOWED_PDF:
        return True
    if "zip" in kinds and ext in ALLOWED_ZIP:
        return True
    return False


def list_files(base_dir):
    files = []
    for root, _, fns in os.walk(base_dir):
        for fn in fns:
            rel = os.path.relpath(os.path.join(root, fn), base_dir)
            files.append(rel)
    return sorted(files)


def build_file_tree(base_dir):
    tree = {"dirs": {}, "files": []}
    for root, dirs, files in os.walk(base_dir):
        rel = os.path.relpath(root, base_dir)
        node = tree
        if rel != ".":
            for part in rel.split(os.sep):
                node = node["dirs"].setdefault(part, {"dirs": {}, "files": []})
        node["files"].extend(sorted(files))
    return tree


def list_dirs(base_dir):
    dirs = []
    for root, dirnames, _ in os.walk(base_dir):
        rel_root = os.path.relpath(root, base_dir)
        for d in dirnames:
            path = os.path.normpath(os.path.join(rel_root, d))
            dirs.append(path)
    return sorted(dirs)


def task_name_exists(name, exclude_id=None):
    for tid in os.listdir(app.config["TASK_FOLDER"]):
        if exclude_id and tid == exclude_id:
            continue
        tdir = os.path.join(app.config["TASK_FOLDER"], tid)
        if not os.path.isdir(tdir):
            continue
        meta_path = os.path.join(tdir, "meta.json")
        tname = tid
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                tname = json.load(f).get("name", tid)
        if tname == name:
            return True
    return False

@app.route("/tasks/<task_id>/copy-files", methods=["GET", "POST"], endpoint="task_copy_files")
def task_copy_files(task_id):
    base = os.path.join(app.config["TASK_FOLDER"], task_id, "files")
    if not os.path.isdir(base):
        abort(404)

    def _safe_path(rel: str) -> str:
        norm = os.path.normpath(rel)
        if not rel or os.path.isabs(norm) or norm.startswith(".."):
            raise ValueError("資料夾名稱不合法")
        return os.path.join(base, norm)

    message = ""
    if request.method == "POST":
        action = request.form.get("action")
        if action == "create_dir":
            new_rel = request.form.get("new_dir", "").strip()
            try:
                os.makedirs(_safe_path(new_rel), exist_ok=True)
                message = f"已建立資料夾 {os.path.normpath(new_rel)}"
            except ValueError:
                message = "資料夾名稱不合法"
        else:
            source_rel = request.form.get("source_dir", "").strip()
            dest_rel = request.form.get("dest_dir", "").strip()
            keywords_raw = request.form.get("keywords", "")
            keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
            if not source_rel or not dest_rel or not keywords:
                message = "請完整輸入資料"
            else:
                try:
                    src = _safe_path(source_rel)
                    dest = _safe_path(dest_rel)
                    copied = copy_files(src, dest, keywords)
                    message = f"已複製 {len(copied)} 個檔案"
                except ValueError:
                    message = "資料夾名稱不合法"
                except Exception as e:
                    message = str(e)
    dirs = list_dirs(base)
    dirs.insert(0, ".")
    return render_template("copy_files.html", dirs=dirs, message=message, task_id=task_id)


@app.route("/tasks/<task_id>/mapping", methods=["GET", "POST"], endpoint="task_mapping")
def task_mapping(task_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    files_dir = os.path.join(tdir, "files")
    out_dir = os.path.join(app.config["OUTPUT_FOLDER"], task_id)
    messages = []
    outputs = []
    if request.method == "POST":
        f = request.files.get("mapping_file")
        if not f or not f.filename:
            messages.append("請選擇檔案")
        else:
            path = os.path.join(tdir, secure_filename(f.filename))
            f.save(path)
            try:
                from modules.mapping_processor import process_mapping_excel
                result = process_mapping_excel(path, files_dir, out_dir)
                messages = result["logs"]
                outputs = result["outputs"]
            except Exception as e:
                messages = [str(e)]
    rel_outputs = [os.path.basename(p) for p in outputs]
    return render_template("mapping.html", task_id=task_id, messages=messages, outputs=rel_outputs)


@app.get("/tasks/<task_id>/output/<filename>")
def task_download_output(task_id, filename):
    out_dir = os.path.join(app.config["OUTPUT_FOLDER"], task_id)
    file_path = os.path.join(out_dir, filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(out_dir, filename, as_attachment=True)


@app.get("/")
def tasks():
    task_list = []
    for tid in os.listdir(app.config["TASK_FOLDER"]):
        tdir = os.path.join(app.config["TASK_FOLDER"], tid)
        if os.path.isdir(tdir):
            meta_path = os.path.join(tdir, "meta.json")
            name = tid
            description = ""
            created = None
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    name = meta.get("name", tid)
                    description = meta.get("description", "")
                    created = meta.get("created")
            if not created:
                created = datetime.fromtimestamp(os.path.getmtime(tdir)).strftime("%Y-%m-%d %H:%M")
            task_list.append({"id": tid, "name": name, "description": description, "created": created})
    task_list.sort(key=lambda x: x["created"], reverse=True)
    return render_template("tasks.html", tasks=task_list)


@app.post("/tasks")
def create_task():
    f = request.files.get("task_zip")
    if not f or not f.filename or not allowed_file(f.filename, kinds=("zip",)):
        return "請上傳 ZIP 檔", 400
    task_name = request.form.get("task_name", "").strip() or "未命名任務"
    task_desc = request.form.get("task_desc", "").strip()
    if task_name_exists(task_name):
        return "任務名稱已存在", 400
    tid = str(uuid.uuid4())[:8]
    tdir = os.path.join(app.config["TASK_FOLDER"], tid)
    files_dir = os.path.join(tdir, "files")
    os.makedirs(files_dir, exist_ok=True)
    zpath = os.path.join(tdir, "source.zip")
    f.save(zpath)
    with zipfile.ZipFile(zpath, "r") as zf:
        zf.extractall(files_dir)
    with open(os.path.join(tdir, "meta.json"), "w", encoding="utf-8") as meta:
        json.dump(
            {
                "name": task_name,
                "description": task_desc,
                "created" : datetime.now().strftime("%Y-%m-%d %H:%M"),
            },
            meta,
            ensure_ascii=False,
            indent=2,
        )
    return redirect(url_for("task_detail", task_id=tid))


@app.post("/tasks/<task_id>/delete")
def delete_task(task_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    if os.path.isdir(tdir):
        import shutil
        shutil.rmtree(tdir)
    return redirect(url_for("tasks"))


@app.post("/tasks/<task_id>/rename")
def rename_task(task_id):
    new_name = request.form.get("name", "").strip()
    if not new_name:
        return "缺少名稱", 400
    if task_name_exists(new_name, exclude_id=task_id):
        return "任務名稱已存在", 400
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    if not os.path.isdir(tdir):
        abort(404)
    meta_path = os.path.join(tdir, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    meta["name"] = new_name
    if "created" not in meta:
        meta["created"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return redirect(url_for("tasks"))


@app.get("/tasks/<task_id>")
def task_detail(task_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    meta_path = os.path.join(tdir, "meta.json")
    name = task_id
    description = ""
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
            name = meta.get("name", task_id)
            description = meta.get("description", "")
    tree = build_file_tree(files_dir)
    return render_template(
        "task_detail.html",
        task={"id": task_id, "name": name, "description": description},
        files_tree=tree,
    )


@app.post("/tasks/<task_id>/files")
def upload_task_file(task_id):
    """Upload additional files to an existing task."""
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)

    f = request.files.get("task_file")
    if not f or not f.filename or not allowed_file(f.filename):
        return "請上傳 DOCX、PDF 或 ZIP 檔", 400

    filename = secure_filename(f.filename)
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".zip":
        tmp_path = os.path.join(files_dir, filename)
        f.save(tmp_path)
        with zipfile.ZipFile(tmp_path, "r") as zf:
            zf.extractall(files_dir)
        os.remove(tmp_path)
    else:
        f.save(os.path.join(files_dir, filename))

    return redirect(url_for("task_detail", task_id=task_id))

def gather_available_files(files_dir):
    mapping = {"docx": [], "pdf": [], "zip": [], "dir": []}
    for rel in list_files(files_dir):
        ext = os.path.splitext(rel)[1].lower()
        if ext == ".docx":
            mapping["docx"].append(rel)
        elif ext == ".pdf":
            mapping["pdf"].append(rel)
        elif ext == ".zip":
            mapping["zip"].append(rel)
    dirs = list_dirs(files_dir)
    dirs.insert(0, ".")
    mapping["dir"] = dirs
    return mapping


@app.get("/tasks/<task_id>/flows")
def flow_builder(task_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    flows = []
    for fn in os.listdir(flow_dir):
        if fn.endswith(".json"):
            path = os.path.join(flow_dir, fn)
            created = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M")
            has_copy = False
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    steps_data = data.get("steps", [])
                    created = data.get("created", created)
                else:
                    steps_data = data
                has_copy = any(
                    isinstance(s, dict) and s.get("type") == "copy_files"
                    for s in steps_data
                )
            except Exception:
                pass
            flows.append({"name": os.path.splitext(fn)[0], "created": created, "has_copy": has_copy})
    preset = None
    center_titles = True
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing = DEFAULT_LINE_SPACING
    loaded_name = request.args.get("flow")
    if loaded_name:
        p = os.path.join(flow_dir, f"{loaded_name}.json")
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                steps_data = data.get("steps", [])
                center_titles = data.get("center_titles", True) or any(
                    isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in steps_data
                )
                document_format = normalize_document_format(data.get("document_format"))
                line_spacing = coerce_line_spacing(data.get("line_spacing", DEFAULT_LINE_SPACING))
            else:
                steps_data = data
                center_titles = True
            preset = [
                s for s in steps_data
                if isinstance(s, dict) and s.get("type") in SUPPORTED_STEPS
            ]
    avail = gather_available_files(files_dir)
    tree = build_file_tree(files_dir)
    return render_template(
        "flow.html",
        task={"id": task_id},
        steps=SUPPORTED_STEPS,
        files=avail,
        flows=flows,
        preset=preset,
        loaded_name=loaded_name,
        center_titles=center_titles,
        format_presets=DOCUMENT_FORMAT_PRESETS,
        selected_format=document_format,
        line_spacing_choices=LINE_SPACING_CHOICES,
        selected_line_spacing=f"{line_spacing:g}",
        line_spacing=line_spacing,
        files_tree=tree,
    )


@app.post("/tasks/<task_id>/flows/run")
def run_flow(task_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    action = request.form.get("action", "run")
    flow_name = request.form.get("flow_name", "").strip()
    ordered_ids = request.form.get("ordered_ids", "").split(",")
    center_titles = request.form.get("center_titles") == "on"
    document_format = normalize_document_format(request.form.get("document_format"))
    line_spacing = coerce_line_spacing(request.form.get("line_spacing"))
    workflow = []
    for sid in ordered_ids:
        sid = sid.strip()
        if not sid:
            continue
        stype = request.form.get(f"step_{sid}_type", "")
        if not stype or stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = {}
        for k in schema.get("inputs", []):
            field = f"step_{sid}_{k}"
            val = request.form.get(field, "")
            params[k] = val
        workflow.append({"type": stype, "params": params})
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    if action == "save":
        if not flow_name:
            return "缺少流程名稱", 400
        path = os.path.join(flow_dir, f"{flow_name}.json")
        created = datetime.now().strftime("%Y-%m-%d %H:%M")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and "created" in data:
                    created = data["created"]
            except Exception:
                pass
        data = {
            "created": created,
            "steps": workflow,
            "center_titles": center_titles,
            "document_format": document_format,
            "line_spacing": line_spacing,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return redirect(url_for("flow_builder", task_id=task_id))

    runtime_steps = []
    for step in workflow:
        stype = step["type"]
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = {}
        for k, v in step["params"].items():
            accept = schema.get("accepts", {}).get(k, "text")
            if accept.startswith("file") and v:
                params[k] = os.path.join(files_dir, v)
            else:
                params[k] = v
        runtime_steps.append({"type": stype, "params": params})

    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(tdir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    run_workflow(runtime_steps, workdir=job_dir)
    result_path = os.path.join(job_dir, "result.docx")
    renumber_figures_tables_file(result_path)
    if center_titles:
        center_table_figure_paragraphs(result_path)
    remove_hidden_runs(result_path)
    format_spec = DOCUMENT_FORMAT_PRESETS.get(document_format, DOCUMENT_FORMAT_PRESETS[DEFAULT_DOCUMENT_FORMAT_KEY])
    apply_basic_style(
        result_path,
        western_font=format_spec["western_font"],
        east_asian_font=format_spec["east_asian_font"],
        font_size=format_spec["font_size"],
        line_spacing=line_spacing,
        space_before=format_spec.get("space_before", 6),
        space_after=format_spec.get("space_after", 6),
    )
    return redirect(url_for("task_result", task_id=task_id, job_id=job_id))


@app.post("/tasks/<task_id>/flows/execute/<flow_name>")
def execute_flow(task_id, flow_name):
    """Execute a previously saved flow."""
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    files_dir = os.path.join(tdir, "files")
    if not os.path.isdir(files_dir):
        abort(404)
    flow_path = os.path.join(tdir, "flows", f"{flow_name}.json")
    if not os.path.exists(flow_path):
        abort(404)
    with open(flow_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    document_format = DEFAULT_DOCUMENT_FORMAT_KEY
    line_spacing = DEFAULT_LINE_SPACING
    if isinstance(data, dict):
        workflow = data.get("steps", [])
        center_titles = data.get("center_titles", True) or any(
            isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in workflow
        )
        document_format = normalize_document_format(data.get("document_format"))
        line_spacing = coerce_line_spacing(data.get("line_spacing", DEFAULT_LINE_SPACING))
    else:
        workflow = data
        center_titles = True
    runtime_steps = []
    for step in workflow:
        stype = step.get("type")
        if stype not in SUPPORTED_STEPS:
            continue
        schema = SUPPORTED_STEPS.get(stype, {})
        params = {}
        for k, v in step.get("params", {}).items():
            accept = schema.get("accepts", {}).get(k, "text")
            if accept.startswith("file") and v:
                params[k] = os.path.join(files_dir, v)
            else:
                params[k] = v
        runtime_steps.append({"type": stype, "params": params})
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(tdir, "jobs", job_id)
    os.makedirs(job_dir, exist_ok=True)
    run_workflow(runtime_steps, workdir=job_dir)
    result_path = os.path.join(job_dir, "result.docx")
    renumber_figures_tables_file(result_path)
    if center_titles:
        center_table_figure_paragraphs(result_path)
    remove_hidden_runs(result_path)
    format_spec = DOCUMENT_FORMAT_PRESETS.get(document_format, DOCUMENT_FORMAT_PRESETS[DEFAULT_DOCUMENT_FORMAT_KEY])
    apply_basic_style(
        result_path,
        western_font=format_spec["western_font"],
        east_asian_font=format_spec["east_asian_font"],
        font_size=format_spec["font_size"],
        line_spacing=line_spacing,
        space_before=format_spec.get("space_before", 6),
        space_after=format_spec.get("space_after", 6),
    )
    return redirect(url_for("task_result", task_id=task_id, job_id=job_id))


@app.post("/tasks/<task_id>/flows/delete/<flow_name>")
def delete_flow(task_id, flow_name):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    path = os.path.join(flow_dir, f"{flow_name}.json")
    if os.path.exists(path):
        os.remove(path)
    return redirect(url_for("flow_builder", task_id=task_id))


@app.post("/tasks/<task_id>/flows/rename/<flow_name>")
def rename_flow(task_id, flow_name):
    new_name = request.form.get("name", "").strip()
    if not new_name:
        return "缺少流程名稱", 400
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    old_path = os.path.join(flow_dir, f"{flow_name}.json")
    new_path = os.path.join(flow_dir, f"{new_name}.json")
    if not os.path.exists(old_path):
        abort(404)
    if os.path.exists(new_path):
        return "流程名稱已存在", 400
    os.rename(old_path, new_path)
    return redirect(url_for("flow_builder", task_id=task_id))

@app.get("/tasks/<task_id>/flows/export/<flow_name>")
def export_flow(task_id, flow_name):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    path = os.path.join(tdir, "flows", f"{flow_name}.json")
    if not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=f"{flow_name}.json")


@app.post("/tasks/<task_id>/flows/import")
def import_flow(task_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    os.makedirs(flow_dir, exist_ok=True)
    f = request.files.get("flow_file")
    if not f or not f.filename.endswith(".json"):
        return "請上傳 JSON 檔", 400
    name = os.path.splitext(secure_filename(f.filename))[0]
    path = os.path.join(flow_dir, f"{name}.json")
    f.save(path)
    return redirect(url_for("flow_builder", task_id=task_id))


@app.get("/tasks/<task_id>/result/<job_id>")
def task_result(task_id, job_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    if not os.path.exists(docx_path):
        return "Job not found or failed.", 404
    log_json_path = os.path.join(job_dir, "log.json")
    log_entries = []
    overall_status = "ok"
    if os.path.exists(log_json_path):
        with open(log_json_path, "r", encoding="utf-8") as f:
            log_entries = json.load(f)
        if any(e.get("status") == "error" for e in log_entries):
            overall_status = "error"
    return render_template(
        "run.html",
        job_id=job_id,
        docx_path=url_for("task_download", task_id=task_id, job_id=job_id, kind="docx"),
        log_path=url_for("task_download", task_id=task_id, job_id=job_id, kind="log"),
        translate_path=url_for("task_translate", task_id=task_id, job_id=job_id),
        compare_path=url_for("task_compare", task_id=task_id, job_id=job_id),
        back_link=url_for("flow_builder", task_id=task_id),
        log_entries=log_entries,
        overall_status=overall_status,
    )


@app.get("/tasks/<task_id>/translate/<job_id>")
def task_translate(task_id, job_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    src = os.path.join(job_dir, "result.docx")
    if not os.path.exists(src):
        abort(404)
    out_docx = os.path.join(job_dir, "translated.docx")
    if not os.path.exists(out_docx):
        tmp_md = os.path.join(job_dir, "translated.md")
        translate_file(src, tmp_md)
        import docx
        doc = docx.Document()
        with open(tmp_md, "r", encoding="utf-8") as f:
            for line in f.read().splitlines():
                doc.add_paragraph(line)
        doc.save(out_docx)
    return send_file(
        out_docx,
        as_attachment=True,
        download_name=f"translated_{job_id}.docx",
    )


@app.get("/tasks/<task_id>/compare/<job_id>")
def task_compare(task_id, job_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(docx_path) or not os.path.exists(log_path):
        abort(404)

    from spire.doc import Document, FileFormat

    html_name = "result.html"
    html_path = os.path.join(job_dir, html_name)
    if not os.path.exists(html_path):
        doc = Document()
        doc.LoadFromFile(docx_path)
        doc.HtmlExportOptions.ImageEmbedded = True
        doc.SaveToFile(html_path, FileFormat.Html)
        doc.Close()
        remove_hidden_runs(docx_path)

    chapter_sources = {}
    source_urls = {}
    converted_docx = {}
    current = None
    with open(log_path, "r", encoding="utf-8") as f:
        entries = json.load(f)
    for entry in entries:
        stype = entry.get("type")
        params = entry.get("params", {})
        if stype == "insert_roman_heading":
            current = params.get("text", "")
            chapter_sources.setdefault(current, [])
        elif stype == "extract_pdf_chapter_to_table":
            pdf_dir = os.path.join(job_dir, "pdfs_extracted")
            pdfs = []
            if os.path.isdir(pdf_dir):
                for fn in sorted(os.listdir(pdf_dir)):
                    if fn.lower().endswith(".pdf"):
                        pdfs.append(fn)
                        rel = os.path.join("pdfs_extracted", fn)
                        source_urls[fn] = url_for(
                            "task_view_file", task_id=task_id, job_id=job_id, filename=rel
                        )
            chapter_sources.setdefault(current or "未分類", []).extend(pdfs)
        elif stype == "extract_word_chapter":
            infile = params.get("input_file", "")
            base = os.path.basename(infile)
            sec = params.get("target_chapter_section", "")
            use_title = str(params.get("target_title", "")).lower() in ["1", "true", "yes", "on"]
            title = params.get("target_title_section", "") if use_title else ""
            info = base
            if sec:
                info += f" 章節 {sec}"
            if title:
                info += f" 標題 {title}"
            chapter_sources.setdefault(current or "未分類", []).append(info)
            if base not in converted_docx and infile and os.path.exists(infile):
                preview_dir = os.path.join(job_dir, "source_html")
                os.makedirs(preview_dir, exist_ok=True)
                html_name_src = f"{os.path.splitext(base)[0]}.html"
                html_rel = os.path.join("source_html", html_name_src)
                html_path_src = os.path.join(job_dir, html_rel)
                doc = Document()
                doc.LoadFromFile(infile)
                doc.HtmlExportOptions.ImageEmbedded = True
                doc.SaveToFile(html_path_src, FileFormat.Html)
                doc.Close()
                converted_docx[base] = html_rel
            if base in converted_docx:
                source_urls[info] = url_for(
                    "task_view_file", task_id=task_id, job_id=job_id, filename=converted_docx[base]
                )
        elif stype == "extract_word_all_content":
            infile = params.get("input_file", "")
            base = os.path.basename(infile)
            chapter_sources.setdefault(current or "未分類", []).append(base)
            if base not in converted_docx and infile and os.path.exists(infile):
                preview_dir = os.path.join(job_dir, "source_html")
                os.makedirs(preview_dir, exist_ok=True)
                html_name_src = f"{os.path.splitext(base)[0]}.html"
                html_rel = os.path.join("source_html", html_name_src)
                html_path_src = os.path.join(job_dir, html_rel)
                doc = Document()
                doc.LoadFromFile(infile)
                doc.HtmlExportOptions.ImageEmbedded = True
                doc.SaveToFile(html_path_src, FileFormat.Html)
                doc.Close()
                converted_docx[base] = html_rel
            if base in converted_docx:
                source_urls[base] = url_for(
                    "task_view_file", task_id=task_id, job_id=job_id, filename=converted_docx[base]
                )

    chapters = list(chapter_sources.keys())
    html_url = url_for("task_view_file", task_id=task_id, job_id=job_id, filename=html_name)
    return render_template(
        "compare.html",
        html_url=html_url,
        chapters=chapters,
        chapter_sources=chapter_sources,
        source_urls=source_urls,
        back_link=url_for("task_result", task_id=task_id, job_id=job_id),
        save_url=url_for("task_compare_save", task_id=task_id, job_id=job_id),
        download_url=url_for("task_download", task_id=task_id, job_id=job_id, kind="docx"),
    )


@app.post("/tasks/<task_id>/compare/<job_id>/save")
def task_compare_save(task_id, job_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    html_content = request.form.get("html")
    if not html_content:
        data = request.get_json(silent=True) or {}
        html_content = data.get("html", "")
    if not html_content:
        return "缺少內容", 400
    # Remove any hidden elements marked via CSS display:none to strip chapter titles
    html_content = re.sub(
        r'<(\w+)[^>]*style="[^"]*display\s*:\s*none[^"]*"[^>]*>.*?</\1>',
        '',
        html_content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # Drop empty paragraphs that may remain after removing hidden markers
    html_content = re.sub(
        r'<p[^>]*>(?:\s|&nbsp;|&#160;)*</p>',
        '',
        html_content,
        flags=re.IGNORECASE,
    )
    html_path = os.path.join(job_dir, "result.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    from spire.doc import Document, FileFormat

    doc = Document()
    doc.LoadFromFile(html_path, FileFormat.Html)
    doc.SaveToFile(os.path.join(job_dir, "result.docx"), FileFormat.Docx)
    doc.Close()
    result_docx = os.path.join(job_dir, "result.docx")
    remove_hidden_runs(result_docx)
    apply_basic_style(result_docx)
    return "OK"


@app.get("/tasks/<task_id>/view/<job_id>/<path:filename>")
def task_view_file(task_id, job_id, filename):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    safe_filename = filename.replace("\\", "/")
    file_path = os.path.join(job_dir, safe_filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(job_dir, safe_filename)


@app.get("/tasks/<task_id>/download/<job_id>/<kind>")
def task_download(task_id, job_id, kind):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    if kind == "docx":
        return send_file(
            os.path.join(job_dir, "result.docx"),
            as_attachment=True,
            download_name=f"result_{job_id}.docx",
        )
    elif kind == "log":
        return send_file(
            os.path.join(job_dir, "log.json"),
            as_attachment=True,
            download_name=f"log_{job_id}.json",
        )
    abort(404)


if __name__ == "__main__":
    app.run(debug=True)

