# 載入環境
cd /home/NE025/UO_MDR
set -a
source .env
set +a

export FLASK_APP=app.py
export ALEMBIC_CONFIG_NAME=production
export ALEMBIC_DATABASE_URL="$DATABASE_URL"

# 建表
/home/NE025/UO_MDR/.venv/bin/alembic upgrade head

# 驗證 schema
/home/NE025/UO_MDR/.venv/bin/flask --app app.py schema-preflight

# 初始化預設資料
/home/NE025/UO_MDR/.venv/bin/flask --app app.py seed-bootstrap