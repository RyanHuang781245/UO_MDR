import os
import shutil
import uuid
import json
import zipfile
import re
import urllib.parse
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
    jsonify,
    flash,
)
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename
from modules.workflow import SUPPORTED_STEPS, run_workflow
from modules.Extract_AllFile_to_FinalWord import (
    center_table_figure_paragraphs,
    apply_basic_style,
    remove_hidden_runs,
    hide_paragraphs_with_text,
    remove_paragraphs_with_text,
)
from modules.Edit_Word import renumber_figures_tables_file
from modules.translate_with_bedrock import translate_file
from modules.file_copier import copy_files

app = Flask(__name__, instance_relative_config=True)
app.config["SECRET_KEY"] = "dev-secret"
os.makedirs(app.instance_path, exist_ok=True)


def build_sql_server_uri() -> str:
    """Build an ODBC connection string for SQL Server using environment variables."""

    server = os.getenv("SQLSERVER_HOST", "10.30.12.177")
    database = os.getenv("SQLSERVER_DATABASE", "RBAC_TEST")
    username = os.getenv("SQLSERVER_USER", "user")
    password = os.getenv("SQLSERVER_PASSWORD", "user")
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    trust_cert = os.getenv("SQLSERVER_TRUST_CERT", "yes")

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate={trust_cert};"
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"


app.config["SQLALCHEMY_DATABASE_URI"] = build_sql_server_uri()
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"
BASE_DIR = os.path.dirname(__file__)
app.config["OUTPUT_FOLDER"] = os.path.join(BASE_DIR, "output")
app.config["TASK_FOLDER"] = os.path.join(BASE_DIR, "tasks")
os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)
os.makedirs(app.config["TASK_FOLDER"], exist_ok=True)

ALLOWED_DOCX = {".docx"}
ALLOWED_PDF = {".pdf"}
ALLOWED_ZIP = {".zip"}


class Role(db.Model):
    __tablename__ = "roles"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(20), unique=True, nullable=False)
    users = db.relationship("User", backref="role", lazy=True)

    def __repr__(self):
        return f"<Role {self.name}>"


class User(UserMixin, db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role_id = db.Column(db.Integer, db.ForeignKey("roles.id"), nullable=False)

    def set_password(self, password: str) -> None:
        from werkzeug.security import generate_password_hash

        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        from werkzeug.security import check_password_hash

        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role and self.role.name == "Admin"


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


def ensure_default_roles_and_admin():
    created = False
    if not Role.query.filter_by(name="Admin").first():
        admin_role = Role(name="Admin")
        user_role = Role(name="User")
        db.session.add_all([admin_role, user_role])
        db.session.commit()
        created = True
    if not User.query.filter_by(username="admin").first():
        admin_role = Role.query.filter_by(name="Admin").first()
        if admin_role:
            admin_user = User(username="admin", role=admin_role)
            admin_user.set_password("1234")
            db.session.add(admin_user)
            db.session.commit()
            created = True
    return created


def admin_required(view_func):
    from functools import wraps

    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            return login_manager.unauthorized()
        if not current_user.is_admin:
            abort(403)
        return view_func(*args, **kwargs)

    return wrapper


@login_manager.unauthorized_handler
def handle_unauthorized():
    flash("請先登入以使用此功能。", "warning")
    return redirect(url_for("login", next=request.url))


_bootstrapped_auth = False


@app.before_request
def enforce_login():
    global _bootstrapped_auth
    if not _bootstrapped_auth:
        db.create_all()
        ensure_default_roles_and_admin()
        _bootstrapped_auth = True
    allowed_endpoints = {"login", "register", "static"}
    if request.endpoint in allowed_endpoints or (request.endpoint or "").startswith("static"):
        return None
    if not current_user.is_authenticated:
        return login_manager.unauthorized()
    return None


@app.cli.command("init-users")
def init_users():
    db.create_all()
    created = ensure_default_roles_and_admin()
    if created:
        print("已初始化預設角色與管理員帳號 (admin/1234)")
    else:
        print("角色與使用者已存在，無需重新建立")

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


def collect_titles_to_hide(entries):
    titles = []
    seen = set()
    if not isinstance(entries, list):
        return titles
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        captured = entry.get("captured_titles")
        if not captured:
            result_meta = entry.get("result")
            if isinstance(result_meta, dict):
                captured = result_meta.get("captured_titles")
        if not captured:
            continue
        for title in captured:
            if not isinstance(title, str):
                continue
            trimmed = title.strip()
            normalized = " ".join(trimmed.split())
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            titles.append(trimmed)
    return titles


def load_titles_to_hide_from_log(job_dir):
    log_path = os.path.join(job_dir, "log.json")
    if not os.path.exists(log_path):
        return []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            entries = json.load(f)
        return collect_titles_to_hide(entries)
    except Exception:
        return []


def clean_compare_html_content(html_content):
    html_content = re.sub(
        r'<(\w+)[^>]*style="[^"]*display\s*:\s*none[^"]*"[^>]*>.*?</\1>',
        "",
        html_content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    html_content = re.sub(
        r"<p[^>]*>(?:\s|&nbsp;|&#160;)*</p>",
        "",
        html_content,
        flags=re.IGNORECASE,
    )
    return html_content


def save_compare_output(
    job_dir,
    html_content,
    titles_to_hide,
    base_name="result",
    subdir=None,
):
    target_dir = job_dir if not subdir else os.path.join(job_dir, subdir)
    os.makedirs(target_dir, exist_ok=True)
    html_path = os.path.join(target_dir, f"{base_name}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    from spire.doc import Document, FileFormat

    doc = Document()
    doc.LoadFromFile(html_path, FileFormat.Html)
    doc.SaveToFile(os.path.join(target_dir, f"{base_name}.docx"), FileFormat.Docx)
    doc.Close()
    result_docx = os.path.join(target_dir, f"{base_name}.docx")
    remove_hidden_runs(result_docx, preserve_texts=titles_to_hide)
    apply_basic_style(result_docx)
    hide_paragraphs_with_text(result_docx, titles_to_hide)
    return html_path, result_docx


def load_version_metadata(versions_dir):
    metadata = {"versions": []}
    if not os.path.isdir(versions_dir):
        return metadata
    meta_path = os.path.join(versions_dir, "metadata.json")
    if not os.path.exists(meta_path):
        return metadata
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("versions"), list):
            metadata = data
    except Exception:
        metadata = {"versions": []}
    return metadata


def save_version_metadata(versions_dir, metadata):
    os.makedirs(versions_dir, exist_ok=True)
    meta_path = os.path.join(versions_dir, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


def sanitize_version_slug(name):
    if not name:
        return "version"
    slug = re.sub(r"[^\w\-]+", "_", name.strip(), flags=re.UNICODE)
    slug = slug.strip("_")
    if not slug:
        slug = "version"
    return slug[:60]


def build_version_context(task_id, job_id, job_dir):
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    context = []
    versions = metadata.get("versions", [])
    for item in sorted(versions, key=lambda v: v.get("created_at", ""), reverse=True):
        version_id = item.get("id")
        base_name = item.get("base_name")
        if not version_id or not base_name:
            continue
        html_rel = f"versions/{base_name}.html"
        docx_rel = os.path.join(versions_dir, f"{base_name}.docx")
        html_abs = os.path.join(versions_dir, f"{base_name}.html")
        if not os.path.exists(docx_rel) or not os.path.exists(html_abs):
            continue
        created_display = item.get("created_at", "")
        created_at = item.get("created_at")
        if created_at:
            try:
                created_display = datetime.fromisoformat(created_at).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                created_display = created_at
        context.append(
            {
                "id": version_id,
                "name": item.get("name") or version_id,
                "created_at_display": created_display,
                "html_url": url_for(
                    "task_view_file",
                    task_id=task_id,
                    job_id=job_id,
                    filename=html_rel,
                ),
                "docx_url": url_for(
                    "task_download_version",
                    task_id=task_id,
                    job_id=job_id,
                    version_id=version_id,
                ),
                "restore_url": url_for(
                    "task_compare_restore_version",
                    task_id=task_id,
                    job_id=job_id,
                    version_id=version_id,
                ),
                "delete_url": url_for(
                    "task_compare_delete_version",
                    task_id=task_id,
                    job_id=job_id,
                    version_id=version_id,
                ),
            }
        )
    return context


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


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        next_url = request.args.get("next")
        return redirect(next_url or url_for("tasks"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            login_user(user)
            next_url = request.args.get("next")
            return redirect(next_url or url_for("tasks"))
        flash("帳號或密碼錯誤", "danger")

    return render_template("auth/login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("tasks"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if not username or not password:
            flash("請輸入帳號與密碼", "warning")
            return render_template("auth/register.html", username=username)

        if User.query.filter_by(username=username).first():
            flash("該帳號已被註冊", "warning")
            return render_template("auth/register.html", username=username)

        role = Role.query.filter_by(name="User").first()
        if not role:
            role = Role(name="User")
            db.session.add(role)
            db.session.commit()

        new_user = User(username=username, role=role)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        flash("註冊成功！請登入。", "success")
        return redirect(url_for("login"))

    return render_template("auth/register.html")


@app.get("/logout")
@login_required
def logout():
    logout_user()
    flash("您已成功登出", "info")
    return redirect(url_for("login"))


@app.get("/")
@login_required
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
@login_required
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
@login_required
@admin_required
def delete_task(task_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    if os.path.isdir(tdir):
        import shutil
        shutil.rmtree(tdir)
    return redirect(url_for("tasks"))


@app.post("/tasks/<task_id>/rename")
@login_required
@admin_required
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
@login_required
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
@login_required
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
            document_format = DEFAULT_DOCUMENT_FORMAT_KEY
            line_spacing = DEFAULT_LINE_SPACING
            steps_data = []
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    steps_data = data.get("steps", [])
                    created = data.get("created", created)
                    document_format = normalize_document_format(data.get("document_format"))
                    line_spacing = coerce_line_spacing(data.get("line_spacing", DEFAULT_LINE_SPACING))
                elif isinstance(data, list):
                    steps_data = data
                has_copy = any(
                    isinstance(s, dict) and s.get("type") == "copy_files"
                    for s in steps_data
                )
            except Exception:
                pass
            line_spacing_value = f"{line_spacing:g}"
            spacing_label = next(
                (label for value, label in LINE_SPACING_CHOICES if value == line_spacing_value),
                f"自訂（{line_spacing_value}）",
            )
            format_spec = DOCUMENT_FORMAT_PRESETS.get(
                document_format, DOCUMENT_FORMAT_PRESETS[DEFAULT_DOCUMENT_FORMAT_KEY]
            )
            flows.append(
                {
                    "name": os.path.splitext(fn)[0],
                    "created": created,
                    "has_copy": has_copy,
                    "document_format": document_format,
                    "format_label": format_spec.get("label", document_format),
                    "line_spacing": line_spacing,
                    "line_spacing_value": line_spacing_value,
                    "line_spacing_label": spacing_label,
                }
            )
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
        line_spacing_values=[value for value, _ in LINE_SPACING_CHOICES],
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
    workflow_result = run_workflow(runtime_steps, workdir=job_dir)
    result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
    titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
    renumber_figures_tables_file(result_path)
    if center_titles:
        center_table_figure_paragraphs(result_path)
    remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
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
    hide_paragraphs_with_text(result_path, titles_to_hide)
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
    workflow_result = run_workflow(runtime_steps, workdir=job_dir)
    result_path = workflow_result.get("result_docx") or os.path.join(job_dir, "result.docx")
    titles_to_hide = collect_titles_to_hide(workflow_result.get("log_json", []))
    renumber_figures_tables_file(result_path)
    if center_titles:
        center_table_figure_paragraphs(result_path)
    remove_hidden_runs(result_path, preserve_texts=titles_to_hide)
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
    hide_paragraphs_with_text(result_path, titles_to_hide)
    return redirect(url_for("task_result", task_id=task_id, job_id=job_id))


@app.post("/tasks/<task_id>/flows/update-format/<flow_name>")
def update_flow_format(task_id, flow_name):
    """Update the document formatting metadata for a saved flow."""
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    flow_dir = os.path.join(tdir, "flows")
    flow_path = os.path.join(flow_dir, f"{flow_name}.json")
    if not os.path.exists(flow_path):
        abort(404)

    document_format = normalize_document_format(request.form.get("document_format"))
    line_spacing = coerce_line_spacing(request.form.get("line_spacing"))

    try:
        with open(flow_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return "流程檔案格式錯誤", 400
    except Exception:
        data = {}

    if isinstance(data, dict):
        payload = data
    elif isinstance(data, list):
        payload = {"steps": data}
    else:
        payload = {"steps": []}

    payload["document_format"] = document_format
    payload["line_spacing"] = line_spacing

    if "created" not in payload:
        created = datetime.fromtimestamp(os.path.getmtime(flow_path)).strftime("%Y-%m-%d %H:%M")
        payload["created"] = created
    payload.setdefault("center_titles", True)

    with open(flow_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return redirect(url_for("flow_builder", task_id=task_id))


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

    with open(log_path, "r", encoding="utf-8") as f:
        entries = json.load(f)
    titles_to_hide = collect_titles_to_hide(entries)

    html_name = "result.html"
    html_path = os.path.join(job_dir, html_name)
    if not os.path.exists(html_path):
        doc = Document()
        doc.LoadFromFile(docx_path)
        doc.HtmlExportOptions.ImageEmbedded = True
        doc.SaveToFile(html_path, FileFormat.Html)
        doc.Close()
        remove_hidden_runs(docx_path, preserve_texts=titles_to_hide)

    chapter_sources = {}
    source_urls = {}
    converted_docx = {}
    current = None
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
    versions = build_version_context(task_id, job_id, job_dir)
    return render_template(
        "compare.html",
        html_url=html_url,
        chapters=chapters,
        chapter_sources=chapter_sources,
        source_urls=source_urls,
        titles_to_hide=titles_to_hide,
        back_link=url_for("task_result", task_id=task_id, job_id=job_id),
        save_url=url_for("task_compare_save", task_id=task_id, job_id=job_id),
        save_as_url=url_for("task_compare_save_as", task_id=task_id, job_id=job_id),
        download_url=url_for("task_download", task_id=task_id, job_id=job_id, kind="docx"),
        versions=versions,
    )


@app.post("/tasks/<task_id>/compare/<job_id>/save")
def task_compare_save(task_id, job_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    titles_to_hide = load_titles_to_hide_from_log(job_dir)
    html_content = request.form.get("html")
    if not html_content:
        data = request.get_json(silent=True) or {}
        html_content = data.get("html", "")
    if not html_content:
        return "缺少內容", 400
    html_content = clean_compare_html_content(html_content)
    save_compare_output(job_dir, html_content, titles_to_hide)
    return "OK"


@app.post("/tasks/<task_id>/compare/<job_id>/save-as")
def task_compare_save_as(task_id, job_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    titles_to_hide = load_titles_to_hide_from_log(job_dir)
    payload = request.get_json(silent=True) or {}
    html_content = payload.get("html")
    name = payload.get("name") or ""
    if not html_content:
        html_content = request.form.get("html")
        name = request.form.get("name") or name
    if not html_content:
        return jsonify({"error": "缺少內容"}), 400
    version_name = (name or "").strip()
    if not version_name:
        return jsonify({"error": "缺少版本名稱"}), 400
    html_content = clean_compare_html_content(html_content)
    versions_dir = os.path.join(job_dir, "versions")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    version_id = f"{timestamp}_{unique_suffix}"
    slug = sanitize_version_slug(version_name)
    base_name = f"{version_id}_{slug}" if slug else version_id
    save_compare_output(
        job_dir,
        html_content,
        titles_to_hide,
        base_name=base_name,
        subdir="versions",
    )
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    versions = [v for v in versions if v.get("id") != version_id]
    created_ts = datetime.now()
    versions.append(
        {
            "id": version_id,
            "name": version_name,
            "slug": slug,
            "base_name": base_name,
            "created_at": created_ts.isoformat(timespec="seconds"),
        }
    )
    versions.sort(key=lambda v: v.get("created_at", ""), reverse=True)
    metadata["versions"] = versions
    save_version_metadata(versions_dir, metadata)
    version_payload = {
        "id": version_id,
        "name": version_name,
        "created_at_display": created_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "html_url": url_for(
            "task_view_file",
            task_id=task_id,
            job_id=job_id,
            filename=f"versions/{base_name}.html",
        ),
        "docx_url": url_for(
            "task_download_version",
            task_id=task_id,
            job_id=job_id,
            version_id=version_id,
        ),
        "restore_url": url_for(
            "task_compare_restore_version",
            task_id=task_id,
            job_id=job_id,
            version_id=version_id,
        ),
        "delete_url": url_for(
            "task_compare_delete_version",
            task_id=task_id,
            job_id=job_id,
            version_id=version_id,
        ),
    }
    return jsonify({"status": "ok", "version": version_payload})


@app.get("/tasks/<task_id>/view/<job_id>/<path:filename>")
def task_view_file(task_id, job_id, filename):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    safe_filename = filename.replace("\\", "/")
    file_path = os.path.join(job_dir, safe_filename)
    if not os.path.isfile(file_path):
        abort(404)
    return send_from_directory(job_dir, safe_filename)


@app.post("/tasks/<task_id>/compare/<job_id>/restore/<version_id>")
def task_compare_restore_version(task_id, job_id, version_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((v for v in versions if v.get("id") == version_id), None)
    if not version:
        return jsonify({"error": "找不到指定版本"}), 404
    base_name = version.get("base_name")
    if not base_name:
        return jsonify({"error": "版本資料不完整"}), 404
    html_src = os.path.join(versions_dir, f"{base_name}.html")
    docx_src = os.path.join(versions_dir, f"{base_name}.docx")
    if not os.path.exists(html_src) or not os.path.exists(docx_src):
        return jsonify({"error": "版本檔案不存在"}), 404
    shutil.copyfile(html_src, os.path.join(job_dir, "result.html"))
    shutil.copyfile(docx_src, os.path.join(job_dir, "result.docx"))
    return jsonify({"status": "ok"})


@app.post("/tasks/<task_id>/compare/<job_id>/delete/<version_id>")
def task_compare_delete_version(task_id, job_id, version_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((v for v in versions if v.get("id") == version_id), None)
    if not version:
        return jsonify({"error": "找不到指定版本"}), 404
    metadata["versions"] = [v for v in versions if v.get("id") != version_id]
    save_version_metadata(versions_dir, metadata)
    base_name = version.get("base_name")
    if base_name:
        for ext in ("html", "docx"):
            path = os.path.join(versions_dir, f"{base_name}.{ext}")
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass
    return jsonify({"status": "ok"})


@app.get("/tasks/<task_id>/download/<job_id>/version/<version_id>")
def task_download_version(task_id, job_id, version_id):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    versions_dir = os.path.join(job_dir, "versions")
    metadata = load_version_metadata(versions_dir)
    versions = metadata.get("versions", [])
    version = next((v for v in versions if v.get("id") == version_id), None)
    if not version:
        abort(404)
    base_name = version.get("base_name")
    if not base_name:
        abort(404)
    docx_src = os.path.join(versions_dir, f"{base_name}.docx")
    if not os.path.exists(docx_src):
        abort(404)
    slug = version.get("slug") or version_id
    download_name = f"{slug}_{version_id}.docx"
    return send_file(docx_src, as_attachment=True, download_name=download_name)


@app.get("/tasks/<task_id>/download/<job_id>/<kind>")
def task_download(task_id, job_id, kind):
    tdir = os.path.join(app.config["TASK_FOLDER"], task_id)
    job_dir = os.path.join(tdir, "jobs", job_id)
    if kind == "docx":
        result_path = os.path.join(job_dir, "result.docx")
        if not os.path.exists(result_path):
            abort(404)
        titles_to_remove = []
        log_path = os.path.join(job_dir, "log.json")
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    entries = json.load(f)
                titles_to_remove = collect_titles_to_hide(entries)
            except Exception:
                titles_to_remove = []

        download_path = os.path.join(job_dir, "result_download.docx")
        shutil.copyfile(result_path, download_path)
        if titles_to_remove:
            remove_paragraphs_with_text(download_path, titles_to_remove)
        remove_hidden_runs(download_path)
        return send_file(
            download_path,
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


# if __name__ == "__main__":
#     app.run(debug=True)

