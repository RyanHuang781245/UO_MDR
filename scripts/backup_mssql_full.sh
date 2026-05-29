#!/usr/bin/env bash
set -euo pipefail

SQLCMD_BIN="${SQLCMD_BIN:-sqlcmd}"
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

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

identifier_escape() {
  printf "%s" "$1" | sed 's/]/]]/g'
}

command -v "$SQLCMD_BIN" >/dev/null 2>&1 || {
  echo "sqlcmd not found: $SQLCMD_BIN" >&2
  exit 127
}

require_var SQLCMD_SERVER
require_var SQLCMD_USER
require_var SQLCMD_PASSWORD
require_var MSSQL_DATABASE
require_var MSSQL_BACKUP_DIR

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

printf 'backup_file=%s\n' "$backup_path"
