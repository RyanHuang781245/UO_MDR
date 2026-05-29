#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--yes" ]]; then
  echo "Usage: $0 --yes" >&2
  echo "This script performs an in-place RESTORE DATABASE ... WITH REPLACE." >&2
  exit 64
fi

SQLCMD_BIN="${SQLCMD_BIN:-sqlcmd}"
SQLCMD_SERVER="${SQLCMD_SERVER:-}"
SQLCMD_USER="${SQLCMD_USER:-}"
SQLCMD_PASSWORD="${SQLCMD_PASSWORD:-}"
MSSQL_DATABASE="${MSSQL_DATABASE:-}"
MSSQL_BACKUP_FILE="${MSSQL_BACKUP_FILE:-}"
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
require_var MSSQL_BACKUP_FILE

escaped_backup_file="$(sql_escape "$MSSQL_BACKUP_FILE")"
escaped_database_name="$(identifier_escape "$MSSQL_DATABASE")"

query="
SET NOCOUNT ON;
BEGIN TRY
    ALTER DATABASE [$escaped_database_name] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    RESTORE DATABASE [$escaped_database_name]
    FROM DISK = N'$escaped_backup_file'
    WITH REPLACE, RECOVERY, CHECKSUM, STATS = 10;
    ALTER DATABASE [$escaped_database_name] SET MULTI_USER;
END TRY
BEGIN CATCH
    BEGIN TRY
        ALTER DATABASE [$escaped_database_name] SET MULTI_USER;
    END TRY
    BEGIN CATCH
    END CATCH;
    THROW;
END CATCH;
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

printf 'restored_database=%s\n' "$MSSQL_DATABASE"
printf 'backup_file=%s\n' "$MSSQL_BACKUP_FILE"
