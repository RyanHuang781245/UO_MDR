
import os, uuid
from flask import Flask, render_template, request, redirect, url_for, send_file
from werkzeug.utils import secure_filename
from modules.workflow import SUPPORTED_STEPS, run_workflow

app = Flask(__name__, instance_relative_config=True)
app.config["SECRET_KEY"] = "dev-secret"
app.config["OUTPUT_FOLDER"] = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)

ALLOWED_DOCX = {".docx"}
ALLOWED_PDF = {".pdf"}
ALLOWED_ZIP = {".zip"}

def allowed_file(filename, kinds=("docx","pdf","zip")):
    ext = os.path.splitext(filename)[1].lower()
    if "docx" in kinds and ext in ALLOWED_DOCX: return True
    if "pdf" in kinds and ext in ALLOWED_PDF: return True
    if "zip" in kinds and ext in ALLOWED_ZIP: return True
    return False

@app.get("/")
def index():
    return render_template("index.html", steps=SUPPORTED_STEPS)

@app.post("/build")
def build():
    ordered_ids = request.form.get("ordered_ids", "").split(",")
    workflow = []
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(app.config["OUTPUT_FOLDER"], job_id)
    os.makedirs(job_dir, exist_ok=True)
    upload_dir = os.path.join(job_dir, "uploads")
    os.makedirs(upload_dir, exist_ok=True)

    def collect_params(sid, stype):
        params = {}
        schema = SUPPORTED_STEPS.get(stype, {})
        for k in schema.get("inputs", []):
            accept = schema["accepts"].get(k, "text")
            field = f"step_{sid}_{k}"
            if accept == "text":
                params[k] = request.form.get(field, "")
            elif accept == "bool":
                raw = request.form.get(field, "")
                raw_l = raw.lower() if isinstance(raw, str) else ""
                if raw_l in ("true","false"):
                    params[k] = raw_l
                elif raw_l in ("on","off"):
                    params[k] = "true" if raw_l == "on" else "false"
                else:
                    params[k] = "true" if raw and raw != "0" else "false"
            elif accept in ("int","float"):
                params[k] = request.form.get(field, "0")
            elif accept.startswith("file"):
                f = request.files.get(field)
                if f and f.filename:
                    fn = secure_filename(f.filename)
                    kinds = ("docx",) if accept=="file:docx" else ("pdf",) if accept=="file:pdf" else ("zip",)
                    if allowed_file(fn, kinds=kinds):
                        save_path = os.path.join(upload_dir, f"{sid}_{fn}")
                        f.save(save_path)
                        params[k] = save_path
        return params

    for sid in ordered_ids:
        sid = sid.strip()
        if not sid: continue
        stype = request.form.get(f"step_{sid}_type","")
        if not stype: continue
        workflow.append({"type": stype, "params": collect_params(sid, stype)})

    run_workflow(workflow, workdir=job_dir)
    return redirect(url_for("result", job_id=job_id))

@app.get("/result/<job_id>")
def result(job_id):
    job_dir = os.path.join(app.config["OUTPUT_FOLDER"], job_id)
    docx_path = os.path.join(job_dir, "result.docx")
    if not os.path.exists(docx_path):
        return "Job not found or failed.", 404
    return render_template("run.html",
                           job_id=job_id,
                           docx_path=url_for("download", job_id=job_id, kind="docx"),
                           log_path=url_for("download", job_id=job_id, kind="log"))

@app.get("/download/<job_id>/<kind>")
def download(job_id, kind):
    job_dir = os.path.join(app.config["OUTPUT_FOLDER"], job_id)
    if kind == "docx":
        return send_file(os.path.join(job_dir, "result.docx"), as_attachment=True, download_name=f"result_{job_id}.docx")
    elif kind == "log":
        return send_file(os.path.join(job_dir, "log.json"), as_attachment=True, download_name=f"log_{job_id}.json")
    return "Not found", 404

if __name__ == "__main__":
    app.run(debug=True)
