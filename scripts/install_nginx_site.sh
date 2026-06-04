#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NGINX_TEMPLATE="$APP_ROOT/deploy/nginx-site.conf.template"
NGINX_SITE_NAME="uo_regulations"
OUTPUT_FILE=""
INSTALL_MODE=0
SITES_AVAILABLE_DIR="/etc/nginx/sites-available"
SITES_ENABLED_DIR="/etc/nginx/sites-enabled"
NGINX_BIN="${NGINX_BIN:-nginx}"
SYSTEMCTL_BIN="${SYSTEMCTL_BIN:-systemctl}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/install_nginx_site.sh [options]

Options:
  --output-file PATH       Render site config into PATH. Default: <app-root>/build/nginx/<site-name>
  --install                Install rendered site config into nginx sites-available/sites-enabled
  --app-root PATH          Application root. Default: repo root
  --template PATH          Nginx site template. Default: <app-root>/deploy/nginx-site.conf.template
  --site-name NAME         Nginx site name. Default: uo_regulations
  --sites-available PATH   sites-available directory. Default: /etc/nginx/sites-available
  --sites-enabled PATH     sites-enabled directory. Default: /etc/nginx/sites-enabled
  --help                   Show this help

Examples:
  bash scripts/install_nginx_site.sh --output-file /tmp/uo_regulations
  sudo bash scripts/install_nginx_site.sh --install --app-root /home/NE025/UO_MDR
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-file)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --install)
      INSTALL_MODE=1
      shift
      ;;
    --app-root)
      APP_ROOT="$2"
      shift 2
      ;;
    --template)
      NGINX_TEMPLATE="$2"
      shift 2
      ;;
    --site-name)
      NGINX_SITE_NAME="$2"
      shift 2
      ;;
    --sites-available)
      SITES_AVAILABLE_DIR="$2"
      shift 2
      ;;
    --sites-enabled)
      SITES_ENABLED_DIR="$2"
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

if [[ -z "$OUTPUT_FILE" ]]; then
  OUTPUT_FILE="$APP_ROOT/build/nginx/$NGINX_SITE_NAME"
fi

require_path() {
  local path="$1"
  local label="$2"
  if [[ ! -e "$path" ]]; then
    echo "$label not found: $path" >&2
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

require_path "$APP_ROOT" "Application root"
require_path "$NGINX_TEMPLATE" "Nginx template"
mkdir -p "$(dirname "$OUTPUT_FILE")"

export APP_ROOT NGINX_TEMPLATE OUTPUT_FILE

python3 - <<'PY'
from __future__ import annotations

import os
from pathlib import Path

template_path = Path(os.environ["NGINX_TEMPLATE"])
output_path = Path(os.environ["OUTPUT_FILE"])
content = template_path.read_text(encoding="utf-8")
content = content.replace("{{APP_ROOT}}", os.environ["APP_ROOT"])
output_path.write_text(content, encoding="utf-8")
print(output_path)
PY

echo "Rendered with:"
echo "  APP_ROOT=$APP_ROOT"
echo "  NGINX_TEMPLATE=$NGINX_TEMPLATE"
echo "  OUTPUT_FILE=$OUTPUT_FILE"
echo "  NGINX_SITE_NAME=$NGINX_SITE_NAME"

if [[ "$INSTALL_MODE" -eq 1 ]]; then
  require_cmd "$NGINX_BIN"
  mkdir -p "$SITES_AVAILABLE_DIR" "$SITES_ENABLED_DIR"
  install -m 0644 "$OUTPUT_FILE" "$SITES_AVAILABLE_DIR/$NGINX_SITE_NAME"
  ln -sf "$SITES_AVAILABLE_DIR/$NGINX_SITE_NAME" "$SITES_ENABLED_DIR/$NGINX_SITE_NAME"
  "$NGINX_BIN" -t
  echo "Installed nginx site:"
  echo "  $SITES_AVAILABLE_DIR/$NGINX_SITE_NAME"
  echo "  $SITES_ENABLED_DIR/$NGINX_SITE_NAME -> $SITES_AVAILABLE_DIR/$NGINX_SITE_NAME"
  if command -v "$SYSTEMCTL_BIN" >/dev/null 2>&1; then
    "$SYSTEMCTL_BIN" reload nginx
    echo "Reloaded nginx"
  fi
else
  echo "Rendered nginx site config into $OUTPUT_FILE"
fi
