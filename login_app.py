from flask import Flask, render_template, request, redirect, url_for, flash, abort
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import urllib

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # 用於 Session 加密，請設定一個複雜的字串

# 資料庫連線設定
params = urllib.parse.quote_plus(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=localhost;'       
    'DATABASE=RBAC_TEST;'       
    'UID=user;'                 
    'PWD=user;'     
    'TrustServerCertificate=yes;' 
)

# 使用 mssql+pyodbc 驅動
app.config['SQLALCHEMY_DATABASE_URI'] = f"mssql+pyodbc:///?odbc_connect={params}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# 設定 Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # 若使用者未登入，將重導向至此 view

# 定義 Role 模型
class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(20), unique=True, nullable=False)
    users = db.relationship('User', backref='role', lazy=True)

    def __repr__(self):
        return f'<Role {self.name}>'

# 定義 User 模型
# 繼承 UserMixin 會自動加入 is_authenticated, is_active 等屬性
class User(UserMixin, db.Model):
    __tablename__ = 'users' # 資料表名稱
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False) # 存放加密後的密碼

    # 新增外鍵指向 role 表
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'), nullable=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    @property
    def is_admin(self):
        return self.role.name == 'Admin'

# Flask-Login 需要這個 callback 來根據 ID 載入使用者
@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 確認有登入
        if not current_user.is_authenticated:
            return redirect(url_for('login'))
        
        # 確認角色是否為 Admin
        if current_user.role.name != 'Admin':
            abort(403) # 拋出 403 禁止訪問錯誤
            
        return f(*args, **kwargs)
    return decorated_function

# 路由 (Routes)
# 初始化資料庫 (僅第一次執行時使用，用於建立資料表)
@app.cli.command("init-db")
def init_db():
    db.create_all()

    # 建立基本角色
    if not Role.query.filter_by(name='Admin').first():
        admin_role = Role(name='Admin')
        user_role = Role(name='User')
        db.session.add_all([admin_role, user_role])
        db.session.commit()

    if not User.query.filter_by(username='admin').first():
        admin_role = Role.query.filter_by(name='Admin').first()
        new_user = User(username='admin', role=admin_role)
        new_user.set_password('1234')
        db.session.add(new_user)
        db.session.commit()

@app.route('/')
def home():
    # 直接導向登入頁
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        existing_user = User.query.filter_by(username=username).first()
        if existing_user:
            flash('該帳號已被註冊')
            return redirect(url_for('register'))
        
        user_role = Role.query.filter_by(name='User').first()
        new_user = User(username=username, role=user_role)
        new_user.set_password(password)
        
        try:
            db.session.add(new_user)
            db.session.commit()
            flash('註冊成功！請登入。')
            return redirect(url_for('login'))
        except Exception as e:
            db.session.rollback()
            flash(f'錯誤: {e}')

    # 修改這裡：使用 render_template
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()

        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for('dashboard'))
        else:
            flash('帳號或密碼錯誤')

    # 修改這裡：使用 render_template
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    # 修改這裡：傳遞變數 name 給模板
    return render_template('dashboard.html', name=current_user.username)

@app.route('/admin')
@login_required
@admin_required
def admin_panel():
    return "<h1>管理員後台<h1>"

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('您已成功登出')
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(debug=True)