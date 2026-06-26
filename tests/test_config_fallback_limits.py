from __future__ import annotations

from app.config import _resolve_system_error_fallback_limits


def test_system_error_fallback_limit_uses_mb_env(monkeypatch):
    monkeypatch.setenv("SYSTEM_ERROR_FALLBACK_MAX_MB", "5")
    monkeypatch.delenv("SYSTEM_ERROR_FALLBACK_MAX_BYTES", raising=False)

    assert _resolve_system_error_fallback_limits() == (5, 5 * 1024 * 1024)


def test_system_error_fallback_limit_accepts_legacy_bytes_env(monkeypatch):
    monkeypatch.delenv("SYSTEM_ERROR_FALLBACK_MAX_MB", raising=False)
    monkeypatch.setenv("SYSTEM_ERROR_FALLBACK_MAX_BYTES", "5242880")

    assert _resolve_system_error_fallback_limits() == (5, 5 * 1024 * 1024)
