import os
import uuid
import json
import zipfile
from datetime import datetime
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    send_file,
    abort,
)
from werkzeug.utils import secure_filename
from modules.workflow import SUPPORTED_STEPS, run_workflow
from modules.Extract_AllFile_to_FinalWord import center_table_figure_paragraphs
from modules.translate_with_bedrock import translate_file

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

def gather_available_files(files_dir):
    mapping = {"docx": [], "pdf": [], "zip": []}
    for rel in list_files(files_dir):
        ext = os.path.splitext(rel)[1].lower()
        if ext == ".docx":
            mapping["docx"].append(rel)
        elif ext == ".pdf":
            mapping["pdf"].append(rel)
        elif ext == ".zip":
            mapping["zip"].append(rel)
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
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    created = data.get("created", created)
            except Exception:
                pass
            flows.append({"name": os.path.splitext(fn)[0], "created": created})
    preset = None
    center_titles = True
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
            else:
                steps_data = data
                center_titles = True
            preset = [
                s for s in steps_data
                if isinstance(s, dict) and s.get("type") in SUPPORTED_STEPS
            ]
    avail = gather_available_files(files_dir)
    return render_template(
        "flow.html",
        task={"id": task_id},
        steps=SUPPORTED_STEPS,
        files=avail,
        flows=flows,
        preset=preset,
        loaded_name=loaded_name,
        center_titles=center_titles,
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
        data = {"created": created, "steps": workflow, "center_titles": center_titles}
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
    if center_titles:
        center_table_figure_paragraphs(os.path.join(job_dir, "result.docx"))
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
    if isinstance(data, dict):
        workflow = data.get("steps", [])
        center_titles = data.get("center_titles", True) or any(
            isinstance(s, dict) and s.get("type") == "center_table_figure_paragraphs" for s in workflow
        )
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
    if center_titles:
        center_table_figure_paragraphs(os.path.join(job_dir, "result.docx"))
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
    return render_template(
        "run.html",
        job_id=job_id,
        docx_path=url_for("task_download", task_id=task_id, job_id=job_id, kind="docx"),
        log_path=url_for("task_download", task_id=task_id, job_id=job_id, kind="log"),
        translate_path=url_for("task_translate", task_id=task_id, job_id=job_id),
        back_link=url_for("flow_builder", task_id=task_id),
        compare_path=url_for("task_compare", task_id=task_id, job_id=job_id),
    )


@app.get("/tasks/<task_id>/compare/<job_id>")
def task_compare(task_id, job_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(docx_path) or not os.path.exists(log_path):
        abort(404)

    with open(log_path, "r", encoding="utf-8") as f:
        log = json.load(f)

    headings = []
    mapping = {}
    current = None
    for entry in log:
        if entry.get("type") == "insert_roman_heading":
            current = entry.get("params", {}).get("text", "")
            headings.append(current)
            mapping[current] = []
        elif current:
            for v in entry.get("params", {}).values():
                if isinstance(v, str) and os.path.isfile(v):
                    mapping[current].append(v)

    import docx
    doc = docx.Document(docx_path)
    paras = [p.text.strip() for p in doc.paragraphs]
    sections = []
    search_start = 0
    for idx, title in enumerate(headings):
        start_idx = search_start
        for i in range(search_start, len(paras)):
            if paras[i] == title:
                start_idx = i
                break
        if idx + 1 < len(headings):
            next_title = headings[idx + 1]
            end_idx = len(paras)
            for j in range(start_idx + 1, len(paras)):
                if paras[j] == next_title:
                    end_idx = j
                    break
        else:
            end_idx = len(paras)
        content = [t for t in paras[start_idx + 1:end_idx] if t]
        sections.append({"title": title, "content": content, "sources": mapping.get(title, [])})
        search_start = end_idx

    def to_roman(num: int) -> str:
        vals = [
            (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
            (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
            (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
        ]
        res = ""
        for v, s in vals:
            res += s * (num // v)
            num %= v
        return res

    def read_file(path: str) -> str:
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext == ".docx":
                d = docx.Document(path)
                return "\n".join(p.text for p in d.paragraphs)
            if ext == ".pdf":
                import fitz
                pdf = fitz.open(path)
                text = "\n".join(page.get_text() for page in pdf)
                pdf.close()
                return text
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        except Exception as e:
            return f"(無法讀取: {e})"

    for i, sec in enumerate(sections, start=1):
        sec["roman"] = to_roman(i)
        unique_sources = []
        seen = set()
        for src in sec["sources"]:
            if src in seen:
                continue
            seen.add(src)
            unique_sources.append({
                "name": os.path.basename(src),
                "text": read_file(src),
            })
        sec["sources"] = unique_sources

    return render_template("compare.html", sections=sections)


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

