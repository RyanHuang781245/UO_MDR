#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMPLATE_DIR="$APP_ROOT/deploy/systemd"
OUTPUT_DIR=""
INSTALL_MODE=0
UNIT_TARGET_DIR="/etc/systemd/system"
APP_USER="${APP_USER:-}"
APP_USER_EXPLICIT=0
ENV_FILE="$APP_ROOT/.env"
ENV_FILE_EXPLICIT=0
WEB_BIND="unix:uo_regulations.sock"
WEB_WORKERS="4"
UPDATE_ON_CALENDAR="*-*-* 8:00:00"
CLEANUP_ON_CALENDAR="*-*-* 23:00:00"
BACKUP_ON_CALENDAR="*-*-* 23:00:00"
SYSTEMCTL_BIN="${SYSTEMCTL_BIN:-systemctl}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/install_systemd_units.sh [options]

Options:
  --output-dir PATH         Render unit files into PATH. Default: ./build/systemd
  --install                 Install rendered unit files into /etc/systemd/system
  --unit-target-dir PATH    Override install directory. Default: /etc/systemd/system
  --app-root PATH           Application root. Default: repo root
  --app-user USER           systemd User=. Default: owner of app root
  --env-file PATH           EnvironmentFile=. Default: <app-root>/.env
  --web-bind TARGET         Gunicorn bind. Default: unix:uo_regulations.sock
  --web-workers N           Gunicorn worker count. Default: 4
  --update-on-calendar EXPR systemd timer OnCalendar. Default: *-*-* 8:00:00
  --cleanup-on-calendar EXPR systemd metadata cleanup timer OnCalendar. Default: *-*-* 03:30:00
  --backup-on-calendar EXPR systemd backup timer OnCalendar. Default: *-*-* 02:00:00
  --help                    Show this help

Examples:
  bash scripts/install_systemd_units.sh --output-dir /tmp/systemd-units
  sudo bash scripts/install_systemd_units.sh --install --update-on-calendar 'Mon..Fri 03:00' --cleanup-on-calendar '*-*-* 03:30:00' --backup-on-calendar '*-*-* 02:00:00'
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --install)
      INSTALL_MODE=1
      shift
      ;;
    --unit-target-dir)
      UNIT_TARGET_DIR="$2"
      shift 2
      ;;
    --app-root)
      APP_ROOT="$2"
      shift 2
      ;;
    --app-user)
      APP_USER="$2"
      APP_USER_EXPLICIT=1
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      ENV_FILE_EXPLICIT=1
      shift 2
      ;;
    --web-bind)
      WEB_BIND="$2"
      shift 2
      ;;
    --web-workers)
      WEB_WORKERS="$2"
      shift 2
      ;;
    --update-on-calendar)
      UPDATE_ON_CALENDAR="$2"
      shift 2
      ;;
    --cleanup-on-calendar)
      CLEANUP_ON_CALENDAR="$2"
      shift 2
      ;;
    --backup-on-calendar)
      BACKUP_ON_CALENDAR="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

TEMPLATE_DIR="$APP_ROOT/deploy/systemd"

if [[ "$APP_USER_EXPLICIT" -eq 0 && -z "$APP_USER" ]]; then
  APP_USER="$(stat -c '%U' "$APP_ROOT")"
fi

if [[ "$ENV_FILE_EXPLICIT" -eq 0 ]]; then
  ENV_FILE="$APP_ROOT/.env"
fi

if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$APP_ROOT/build/systemd"
fi

require_path() {
  local path="$1"
  local label="$2"
  if [[ ! -e "$path" ]]; then
    echo "$label not found: $path" >&2
    exit 66
  fi
}

require_path "$TEMPLATE_DIR" "Template directory"
require_path "$APP_ROOT/.venv/bin/gunicorn" "Gunicorn executable"
require_path "$APP_ROOT/.venv/bin/flask" "Flask executable"
require_path "$APP_ROOT/.venv/bin/python" "Python executable"
require_path "$ENV_FILE" "Environment file"

mkdir -p "$OUTPUT_DIR"

export APP_ROOT APP_USER ENV_FILE WEB_BIND WEB_WORKERS UPDATE_ON_CALENDAR CLEANUP_ON_CALENDAR BACKUP_ON_CALENDAR TEMPLATE_DIR OUTPUT_DIR

python3 - <<'PY'
from __future__ import annotations

import os
from pathlib import Path

template_dir = Path(os.environ["TEMPLATE_DIR"])
output_dir = Path(os.environ["OUTPUT_DIR"])
mapping = {
    "APP_ROOT": os.environ["APP_ROOT"],
    "APP_USER": os.environ["APP_USER"],
    "ENV_FILE": os.environ["ENV_FILE"],
    "WEB_BIND": os.environ["WEB_BIND"],
    "WEB_WORKERS": os.environ["WEB_WORKERS"],
    "UPDATE_ON_CALENDAR": os.environ["UPDATE_ON_CALENDAR"],
    "CLEANUP_ON_CALENDAR": os.environ["CLEANUP_ON_CALENDAR"],
    "BACKUP_ON_CALENDAR": os.environ["BACKUP_ON_CALENDAR"],
}

for template_path in sorted(template_dir.glob("*.template")):
    content = template_path.read_text(encoding="utf-8")
    for key, value in mapping.items():
        content = content.replace(f"{{{{{key}}}}}", value)
    target_name = template_path.name.removesuffix(".template")
    (output_dir / target_name).write_text(content, encoding="utf-8")
    print(output_dir / target_name)
PY

if [[ "$INSTALL_MODE" -eq 1 ]]; then
  mkdir -p "$UNIT_TARGET_DIR"
  install -m 0644 "$OUTPUT_DIR"/uo_regulations.service "$UNIT_TARGET_DIR"/uo_regulations.service
  install -m 0644 "$OUTPUT_DIR"/uo_regulations_jobs_worker.service "$UNIT_TARGET_DIR"/uo_regulations_jobs_worker.service
  install -m 0644 "$OUTPUT_DIR"/uo_regulations_flow_worker.service "$UNIT_TARGET_DIR"/uo_regulations_flow_worker.service
  install -m 0644 "$OUTPUT_DIR"/uo_regulations_batch_worker.service "$UNIT_TARGET_DIR"/uo_regulations_batch_worker.service
  install -m 0644 "$OUTPUT_DIR"/uo_regulations_metadata_cleanup.service "$UNIT_TARGET_DIR"/uo_regulations_metadata_cleanup.service
  install -m 0644 "$OUTPUT_DIR"/uo_regulations_metadata_cleanup.timer "$UNIT_TARGET_DIR"/uo_regulations_metadata_cleanup.timer
  install -m 0644 "$OUTPUT_DIR"/uo_regulations_backup.service "$UNIT_TARGET_DIR"/uo_regulations_backup.service
  install -m 0644 "$OUTPUT_DIR"/uo_regulations_backup.timer "$UNIT_TARGET_DIR"/uo_regulations_backup.timer
  install -m 0644 "$OUTPUT_DIR"/adoption-standard-update.service "$UNIT_TARGET_DIR"/adoption-standard-update.service
  install -m 0644 "$OUTPUT_DIR"/adoption-standard-update.timer "$UNIT_TARGET_DIR"/adoption-standard-update.timer
  "$SYSTEMCTL_BIN" daemon-reload
  echo "Installed unit files into $UNIT_TARGET_DIR"
  echo "Rendered with:"
  echo "  APP_ROOT=$APP_ROOT"
  echo "  APP_USER=$APP_USER"
  echo "  ENV_FILE=$ENV_FILE"
  echo "Installed units:"
  echo "  uo_regulations.service"
  echo "  uo_regulations_jobs_worker.service"
  echo "  uo_regulations_flow_worker.service"
  echo "  uo_regulations_batch_worker.service"
  echo "  uo_regulations_metadata_cleanup.service"
  echo "  uo_regulations_metadata_cleanup.timer"
  echo "  uo_regulations_backup.service"
  echo "  uo_regulations_backup.timer"
  echo "  adoption-standard-update.service"
  echo "  adoption-standard-update.timer"
  echo "Next:"
  echo "  sudo systemctl enable uo_regulations uo_regulations_jobs_worker uo_regulations_flow_worker uo_regulations_batch_worker uo_regulations_metadata_cleanup.timer uo_regulations_backup.timer adoption-standard-update.timer"
  echo "  sudo systemctl start uo_regulations uo_regulations_jobs_worker uo_regulations_flow_worker uo_regulations_batch_worker uo_regulations_metadata_cleanup.timer uo_regulations_backup.timer adoption-standard-update.timer"
  echo "  sudo systemctl status uo_regulations_metadata_cleanup.service --no-pager"
  echo "  sudo systemctl status uo_regulations_backup.service --no-pager"
  echo "  sudo systemctl status adoption-standard-update.service --no-pager"
  echo "Run metadata cleanup immediately only when needed:"
  echo "  sudo systemctl start uo_regulations_metadata_cleanup.service"
  echo "Run backup immediately only when needed:"
  echo "  sudo systemctl start uo_regulations_backup.service"
  echo "Run adoption update immediately only when needed:"
  echo "  sudo systemctl start adoption-standard-update.service"
else
  echo "Rendered unit files into $OUTPUT_DIR"
fi
