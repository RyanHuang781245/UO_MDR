#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${HOME}/.local/share/fonts/noto-cjk"
FORCE=0

usage() {
  cat <<'EOF'
Usage: scripts/install_noto_cjk_fonts.sh [--force] [--install-dir DIR]

Install the Traditional Chinese Noto CJK fonts required by LibreOffice
comparison previews. The script is idempotent and safe to re-run.

Options:
  --force            Re-download fonts even if files already exist
  --install-dir DIR  Override the target font directory
  -h, --help         Show this help
EOF
}

run_as_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    echo "Missing sudo; please install required system packages manually." >&2
    return 1
  fi
}

install_system_dependencies() {
  local packages=()

  if ! command -v curl >/dev/null 2>&1; then
    packages+=(curl)
  fi

  if ! command -v fc-cache >/dev/null 2>&1; then
    if command -v apt-get >/dev/null 2>&1; then
      packages+=(fontconfig)
    elif command -v dnf >/dev/null 2>&1 || command -v yum >/dev/null 2>&1; then
      packages+=(fontconfig)
    else
      echo "fc-cache is required to refresh the font cache." >&2
      echo "Please install fontconfig manually and re-run this script." >&2
      exit 1
    fi
  fi

  if [[ ${#packages[@]} -eq 0 ]]; then
    return
  fi

  echo "Installing system dependencies: ${packages[*]}"
  if command -v apt-get >/dev/null 2>&1; then
    run_as_root apt-get update
    run_as_root apt-get install -y "${packages[@]}"
  elif command -v dnf >/dev/null 2>&1; then
    run_as_root dnf install -y "${packages[@]}"
  elif command -v yum >/dev/null 2>&1; then
    run_as_root yum install -y "${packages[@]}"
  else
    echo "No supported package manager found. Please install: ${packages[*]}" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=1
      shift
      ;;
    --install-dir)
      INSTALL_DIR="${2:?missing value for --install-dir}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

install_system_dependencies

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required to download Noto CJK fonts." >&2
  exit 1
fi

if ! command -v fc-cache >/dev/null 2>&1; then
  echo "fc-cache is required to refresh the font cache." >&2
  exit 1
fi

mkdir -p "${INSTALL_DIR}"

download_font() {
  local target_name="$1"
  local url="$2"
  local target_path="${INSTALL_DIR}/${target_name}"

  if [[ ${FORCE} -eq 0 && -f "${target_path}" ]]; then
    echo "Keeping existing font: ${target_path}"
    return
  fi

  echo "Downloading ${target_name}"
  curl -L --fail --show-error "${url}" -o "${target_path}"
}

download_font \
  "NotoSansCJKtc-Regular.otf" \
  "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Regular.otf"

download_font \
  "NotoSansCJKtc-Bold.otf" \
  "https://raw.githubusercontent.com/notofonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Bold.otf"

fc-cache -f "${INSTALL_DIR}"

if command -v fc-match >/dev/null 2>&1; then
  echo "Resolved font family:"
  fc-match "Noto Sans CJK TC"
fi

echo
echo "Traditional Chinese preview fonts are installed in:"
echo "  ${INSTALL_DIR}"
echo
echo "Recommended app setting:"
echo "  PROVENANCE_PREVIEW_LABEL_EAST_ASIA_FONT=Noto Sans CJK TC"
