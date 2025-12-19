from __future__ import annotations

import os
from pathlib import Path


def _load_env_file_simple(env_path: Path) -> None:
    """
    Minimal .env loader (no quotes/expansion support).

    - Ignores blank lines and comments (#...)
    - Parses KEY=VALUE
    - Does not override existing os.environ values
    """
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        os.environ.setdefault(key, value)


def load_dotenv_if_present(base_dir: str | os.PathLike | None = None, filename: str = ".env") -> bool:
    """
    Load environment variables from `.env` if present.

    Preference:
    1) python-dotenv (if installed)
    2) fallback simple parser

    Returns True if a file was found and processed.
    """
    base = Path(base_dir) if base_dir else Path.cwd()
    env_path = base / filename
    if not env_path.exists():
        return False

    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(dotenv_path=env_path, override=False)
        return True
    except Exception:
        _load_env_file_simple(env_path)
        return True

