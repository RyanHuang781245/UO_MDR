#!/usr/bin/env bash
set -euo pipefail

APP_NAME="uo_regulations"
WORKER_SERVICE="uo_regulations_jobs_worker"
TIMER_SERVICE="adoption-standard-update.timer"
APP_DIR="/home/NE025/UO_MDR"
ENV_FILE="$APP_DIR/.env"
APP_ROOT="$APP_DIR"
BRANCH="${DEPLOY_BRANCH:-main}"
RUN_GIT_PULL="${RUN_GIT_PULL:-0}"
RUN_DB_BACKUP="${RUN_DB_BACKUP:-0}"
INSTALL_SYSTEMD_UNITS="${INSTALL_SYSTEMD_UNITS:-1}"
WEB_WORKERS="${WEB_WORKERS:-2}"
WEB_BIND="${WEB_BIND:-127.0.0.1:8000}"
UPDATE_ON_CALENDAR="${UPDATE_ON_CALENDAR:-daily}"
NGINX_FILE="${NGINX_FILE:-$APP_DIR/deploy/nginx.conf}"
NGINX_SITE_NAME="${NGINX_SITE_NAME:-$APP_NAME}"
ENABLE_NGINX="${ENABLE_NGINX:-0}"
UV_BIN="${UV_BIN:-uv}"
UV_SYNC_ARGS="${UV_SYNC_ARGS:---frozen}"
VENV_PYTHON="$APP_DIR/.venv/bin/python"
ALEMBIC_BIN="$APP_DIR/.venv/bin/alembic"
FLASK_BIN="$APP_DIR/.venv/bin/flask"

log() {
  printf '\n[%s] %s\n' "$(date '+%F %T')" "$1"
}

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "Required file not found: $path" >&2
    exit 66
  fi
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Command not found: $cmd" >&2
    exit 127
  fi
}

log "進入專案目錄"
cd "$APP_DIR"

require_file "$ENV_FILE"

log "載入正式環境變數"
set -a
source "$ENV_FILE"
set +a

export FLASK_APP=app.py
export ALEMBIC_CONFIG_NAME="${ALEMBIC_CONFIG_NAME:-production}"
export ALEMBIC_DATABASE_URL="${ALEMBIC_DATABASE_URL:-${DATABASE_URL:-}}"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is empty after loading $ENV_FILE" >&2
  exit 64
fi

if [[ -z "${ALEMBIC_DATABASE_URL:-}" ]]; then
  echo "ALEMBIC_DATABASE_URL is empty after loading $ENV_FILE" >&2
  exit 64
fi

if [[ "$RUN_GIT_PULL" == "1" ]]; then
  require_cmd git
  log "更新程式碼"
  git pull origin "$BRANCH"
fi

log "建立或同步 Python uv 虛擬環境"
require_cmd "$UV_BIN"
"$UV_BIN" sync $UV_SYNC_ARGS

require_file "$VENV_PYTHON"
require_file "$ALEMBIC_BIN"
require_file "$FLASK_BIN"

if [[ "$RUN_DB_BACKUP" == "1" ]]; then
  log "執行部署前資料庫備份"
  bash "$APP_DIR/scripts/backup_mssql_full.sh"
fi

log "停止排程 timer，避免部署中觸發"
sudo systemctl stop "$TIMER_SERVICE" || true

log "停止應用服務"
sudo systemctl stop "$WORKER_SERVICE" || true
sudo systemctl stop "$APP_NAME" || true

if [[ "$INSTALL_SYSTEMD_UNITS" == "1" ]]; then
  log "安裝或更新 systemd units"
  sudo bash "$APP_DIR/scripts/install_systemd_units.sh" \
    --install \
    --app-root "$APP_ROOT" \
    --env-file "$ENV_FILE" \
    --web-bind "$WEB_BIND" \
    --web-workers "$WEB_WORKERS" \
    --update-on-calendar "$UPDATE_ON_CALENDAR"
fi

if [[ "$ENABLE_NGINX" == "1" ]]; then
  require_cmd nginx
  require_file "$NGINX_FILE"
  log "複製 Nginx 設定"
  sudo cp "$NGINX_FILE" "/etc/nginx/sites-available/$NGINX_SITE_NAME"
  sudo ln -sf "/etc/nginx/sites-available/$NGINX_SITE_NAME" "/etc/nginx/sites-enabled/$NGINX_SITE_NAME"
  log "測試 Nginx 設定"
  sudo nginx -t
fi

log "執行資料庫 migration"
"$ALEMBIC_BIN" upgrade head

log "驗證 schema"
"$FLASK_BIN" --app app.py schema-preflight

log "初始化預設資料"
"$FLASK_BIN" --app app.py seed-bootstrap

log "啟動 Web service"
sudo systemctl restart "$APP_NAME"

log "啟動 Worker service"
sudo systemctl restart "$WORKER_SERVICE"

log "啟動 Timer"
sudo systemctl restart "$TIMER_SERVICE"

if [[ "$ENABLE_NGINX" == "1" ]]; then
  log "重新載入 Nginx"
  sudo systemctl reload nginx
fi

log "查看服務狀態"
sudo systemctl status "$APP_NAME" --no-pager
sudo systemctl status "$WORKER_SERVICE" --no-pager
sudo systemctl status "$TIMER_SERVICE" --no-pager

log "部署完成"
