#!/usr/bin/env bash
set -euo pipefail

SQLCMD_BIN="${SQLCMD_BIN:-sqlcmd}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="${APP_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
ENV_FILE="${ENV_FILE:-$APP_ROOT/.env}"
SQLCMD_SERVER="${SQLCMD_SERVER:-}"
SQLCMD_USER="${SQLCMD_USER:-}"
SQLCMD_PASSWORD="${SQLCMD_PASSWORD:-}"
MSSQL_DATABASE="${MSSQL_DATABASE:-}"
MSSQL_BACKUP_DIR="${MSSQL_BACKUP_DIR:-}"
SQLCMD_TRUST_CERT="${SQLCMD_TRUST_CERT:-1}"

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: $name" >&2
    exit 64
  fi
}

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer: $value" >&2
    exit 64
  fi
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$1"
}

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

identifier_escape() {
  printf "%s" "$1" | sed 's/]/]]/g'
}

rotate_local_mssql_backups() {
  local keep_count="$1"
  local database_name="$2"
  local backup_dir="$3"
  local -a backups=()
  local entry
  local backup
  local index

  if [[ ! -d "$backup_dir" ]]; then
    log "MSSQL backup rotation skipped; directory is not locally accessible: $backup_dir"
    return 0
  fi

  while IFS= read -r -d '' entry; do
    backups+=("${entry#* }")
  done < <(find "$backup_dir" -maxdepth 1 -type f -name "${database_name}_*_copyonly_full.bak" -printf '%T@ %p\0' | sort -zrn)

  for ((index = keep_count; index < ${#backups[@]}; index++)); do
    backup="${backups[$index]}"
    rm -f "$backup"
    log "Removed old MSSQL backup: $backup"
  done
}

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ -n "${DATABASE_URL:-${RBAC_DATABASE_URL:-}}" ]]; then
  PYTHON_BIN="${PYTHON_BIN:-}"
  if [[ -z "$PYTHON_BIN" && -x "$APP_ROOT/.venv/bin/python" ]]; then
    PYTHON_BIN="$APP_ROOT/.venv/bin/python"
  fi
  PYTHON_BIN="${PYTHON_BIN:-python3}"
  eval "$("$PYTHON_BIN" "$SCRIPT_DIR/mssql_url_to_sqlcmd_env.py")"
fi

MSSQL_BACKUP_RETENTION_COUNT="${MSSQL_BACKUP_RETENTION_COUNT:-${BACKUP_RETENTION_COUNT:-3}}"

command -v "$SQLCMD_BIN" >/dev/null 2>&1 || {
  echo "sqlcmd not found: $SQLCMD_BIN" >&2
  exit 127
}

require_var SQLCMD_SERVER
require_var SQLCMD_USER
require_var SQLCMD_PASSWORD
require_var MSSQL_DATABASE
require_var MSSQL_BACKUP_DIR
require_positive_integer MSSQL_BACKUP_RETENTION_COUNT "$MSSQL_BACKUP_RETENTION_COUNT"

timestamp="$(date +%F_%H%M%S)"
backup_file_name="${BACKUP_FILE_NAME:-${MSSQL_DATABASE}_${timestamp}_copyonly_full.bak}"
backup_path="${MSSQL_BACKUP_DIR%/}/${backup_file_name}"
escaped_backup_path="$(sql_escape "$backup_path")"
escaped_database_name="$(identifier_escape "$MSSQL_DATABASE")"

query="
SET NOCOUNT ON;
BACKUP DATABASE [$escaped_database_name]
TO DISK = N'$escaped_backup_path'
WITH COPY_ONLY, INIT, COMPRESSION, CHECKSUM, STATS = 10;
RESTORE VERIFYONLY
FROM DISK = N'$escaped_backup_path'
WITH CHECKSUM;
"

sqlcmd_args=(
  -S "$SQLCMD_SERVER"
  -U "$SQLCMD_USER"
  -P "$SQLCMD_PASSWORD"
  -d master
  -b
  -Q "$query"
)

if [[ "$SQLCMD_TRUST_CERT" == "1" ]]; then
  sqlcmd_args+=(-C)
fi

"$SQLCMD_BIN" "${sqlcmd_args[@]}"

rotate_local_mssql_backups "$MSSQL_BACKUP_RETENTION_COUNT" "$MSSQL_DATABASE" "$MSSQL_BACKUP_DIR"

printf 'backup_file=%s\n' "$backup_path"
