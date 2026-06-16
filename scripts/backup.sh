#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/home/NE025/UO_MDR}"
BACKUP_ROOT="${BACKUP_ROOT:-$APP_ROOT/backups/files}"
BACKUP_RETENTION_COUNT="${BACKUP_RETENTION_COUNT:-3}"
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

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer: $value" >&2
    exit 64
  fi
}

rotate_file_backups() {
  local keep_count="$1"
  local -a archives=()
  local entry
  local archive
  local index

  while IFS= read -r -d '' entry; do
    archives+=("${entry#* }")
  done < <(find "$BACKUP_ROOT" -maxdepth 1 -type f -name "*.tar.gz" -printf '%T@ %p\0' | sort -zrn)

  for ((index = keep_count; index < ${#archives[@]}; index++)); do
    archive="${archives[$index]}"
    rm -f "$archive" "${archive}.sha256"
    log "Removed old backup: $archive"
  done
}

require_positive_integer BACKUP_RETENTION_COUNT "$BACKUP_RETENTION_COUNT"
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
  --exclude='task_store/*/mappings' \
  --exclude='task_store/*/mappings/' \
  --exclude='task_store/*/mappings/*' \
  --exclude='task_store/*/mapping_job' \
  --exclude='task_store/*/mapping_job/' \
  --exclude='task_store/*/mapping_job/*' \
  -C "$APP_ROOT" \
  -T "$TMP_INCLUDE_FILE"

sha256sum "$ARCHIVE_PATH" > "$CHECKSUM_PATH"

log "Archive created"
log "Checksum written: $CHECKSUM_PATH"

rotate_file_backups "$BACKUP_RETENTION_COUNT"

log "Backup rotation complete; kept latest $BACKUP_RETENTION_COUNT archive(s)"
printf 'backup_file=%s\n' "$ARCHIVE_PATH"
