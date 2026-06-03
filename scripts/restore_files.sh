#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/home/NE025/UO_MDR}"
BACKUP_ROOT="${BACKUP_ROOT:-$APP_ROOT/backups/files}"
SKIP_PRE_RESTORE_BACKUP="${SKIP_PRE_RESTORE_BACKUP:-0}"
RESTORE_ARCHIVE="${RESTORE_ARCHIVE:-${1:-}}"
CONFIRM="${2:-}"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$1"
}

usage() {
  cat >&2 <<'EOF'
Usage:
  scripts/restore_files.sh PATH_TO_FILES_BACKUP.tar.gz --yes

Environment:
  APP_ROOT=/home/NE025/UO_MDR
  BACKUP_ROOT=$APP_ROOT/backups/files
  RESTORE_ARCHIVE=PATH_TO_FILES_BACKUP.tar.gz
  SKIP_PRE_RESTORE_BACKUP=1  Skip creating a current-state file backup before restore
EOF
}

if [[ -z "$RESTORE_ARCHIVE" || "$CONFIRM" != "--yes" ]]; then
  usage
  exit 64
fi

if [[ ! -d "$APP_ROOT" ]]; then
  echo "APP_ROOT not found: $APP_ROOT" >&2
  exit 66
fi

if [[ ! -f "$RESTORE_ARCHIVE" ]]; then
  echo "Restore archive not found: $RESTORE_ARCHIVE" >&2
  exit 66
fi

case "$RESTORE_ARCHIVE" in
  *.tar.gz) ;;
  *)
    echo "Restore archive must be a .tar.gz file: $RESTORE_ARCHIVE" >&2
    exit 64
    ;;
esac

CHECKSUM_PATH="${RESTORE_ARCHIVE}.sha256"
if [[ -f "$CHECKSUM_PATH" ]]; then
  log "Verifying checksum: $CHECKSUM_PATH"
  expected_hash="$(awk '{print $1; exit}' "$CHECKSUM_PATH")"
  actual_hash="$(sha256sum "$RESTORE_ARCHIVE" | awk '{print $1; exit}')"
  if [[ -z "$expected_hash" || "$expected_hash" != "$actual_hash" ]]; then
    echo "Checksum verification failed: $RESTORE_ARCHIVE" >&2
    exit 65
  fi
else
  log "Checksum file not found; skipping checksum verification"
fi

log "Validating archive structure"
tar -tzf "$RESTORE_ARCHIVE" >/dev/null

if [[ "$SKIP_PRE_RESTORE_BACKUP" != "1" ]]; then
  log "Creating current-state backup before restore"
  APP_ROOT="$APP_ROOT" BACKUP_ROOT="$BACKUP_ROOT" bash "$(dirname "${BASH_SOURCE[0]}")/backup.sh"
fi

log "Restoring files into: $APP_ROOT"
tar -xzf "$RESTORE_ARCHIVE" -C "$APP_ROOT"

log "File restore complete"
printf 'restored_archive=%s\n' "$RESTORE_ARCHIVE"
