#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/home/NE025/UO_MDR}"
BACKUP_ROOT="${BACKUP_ROOT:-$APP_ROOT/backups/files}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
HOSTNAME_SHORT="${HOSTNAME_SHORT:-$(hostname -s)}"
STAMP="$(date +%F_%H%M%S)"
ARCHIVE_NAME="${ARCHIVE_NAME:-${HOSTNAME_SHORT}_files_${STAMP}.tar.gz}"
ARCHIVE_PATH="$BACKUP_ROOT/$ARCHIVE_NAME"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"
TMP_INCLUDE_FILE="$(mktemp)"

cleanup() {
  rm -f "$TMP_INCLUDE_FILE"
}
trap cleanup EXIT

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$1"
}

require_path() {
  local path="$1"
  if [[ ! -e "$path" ]]; then
    echo "Required path not found: $path" >&2
    exit 66
  fi
}

mkdir -p "$BACKUP_ROOT"
require_path "$APP_ROOT"

cat >"$TMP_INCLUDE_FILE" <<'EOF'
.env
task_store
standard_update_store
harmonised_store
deploy/systemd
EOF

log "Creating archive: $ARCHIVE_PATH"
tar -czf "$ARCHIVE_PATH" \
  --exclude='task_store/*/files/*' \
  --exclude='task_store/*/jobs/*' \
  --exclude='task_store/*/mapping_job/*' \
  -C "$APP_ROOT" \
  -T "$TMP_INCLUDE_FILE"

sha256sum "$ARCHIVE_PATH" > "$CHECKSUM_PATH"

log "Archive created"
log "Checksum written: $CHECKSUM_PATH"

find "$BACKUP_ROOT" -type f \( -name "*.tar.gz" -o -name "*.tar.gz.sha256" \) -mtime +"$RETENTION_DAYS" -delete

log "Retention cleanup complete"
printf 'backup_file=%s\n' "$ARCHIVE_PATH"
