#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--yes" ]]; then
  echo "Usage: $0 --yes" >&2
  echo "This script performs an in-place RESTORE DATABASE ... WITH REPLACE." >&2
  exit 64
fi

SQLCMD_BIN="${SQLCMD_BIN:-sqlcmd}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="${APP_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
ENV_FILE="${ENV_FILE:-$APP_ROOT/.env}"
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
escaped_database_literal="$(sql_escape "$MSSQL_DATABASE")"
escaped_database_name="$(identifier_escape "$MSSQL_DATABASE")"

query="
SET NOCOUNT ON;
PRINT N'login=' + SUSER_SNAME();
PRINT N'system_user=' + SYSTEM_USER;
PRINT N'is_sysadmin=' + CONVERT(nvarchar(10), IS_SRVROLEMEMBER('sysadmin'));
PRINT N'is_dbcreator=' + CONVERT(nvarchar(10), IS_SRVROLEMEMBER('dbcreator'));
IF DB_ID(N'$escaped_database_literal') IS NULL
BEGIN
    PRINT N'target_database_exists=0';
END
ELSE
BEGIN
    DECLARE @state_desc nvarchar(60);
    SELECT @state_desc = state_desc FROM sys.databases WHERE name = N'$escaped_database_literal';
    PRINT N'target_database_exists=1';
    PRINT N'target_database_state=' + COALESCE(@state_desc, N'UNKNOWN');
END;
BEGIN TRY
    IF DB_ID(N'$escaped_database_literal') IS NOT NULL
    BEGIN
        ALTER DATABASE [$escaped_database_name] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    END
    ELSE
    BEGIN
        PRINT N'target_database_missing_skip_single_user=1';
    END;
    RESTORE DATABASE [$escaped_database_name]
    FROM DISK = N'$escaped_backup_file'
    WITH REPLACE, RECOVERY, CHECKSUM, STATS = 10;
    ALTER DATABASE [$escaped_database_name] SET MULTI_USER;
END TRY
BEGIN CATCH
    BEGIN TRY
        IF DB_ID(N'$escaped_database_literal') IS NOT NULL
        BEGIN
            ALTER DATABASE [$escaped_database_name] SET MULTI_USER;
        END
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
