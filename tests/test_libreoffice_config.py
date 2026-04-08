from pathlib import Path

from app.blueprints.tasks import compare_compat
from app.blueprints.tasks import compare_helpers


def test_find_libreoffice_binary_prefers_configured_env_path(monkeypatch, tmp_path: Path) -> None:
    fake_soffice = tmp_path / "soffice"
    fake_soffice.write_text("", encoding="utf-8")

    monkeypatch.setenv("LIBREOFFICE_BIN", str(fake_soffice))
    monkeypatch.setattr(compare_helpers.shutil, "which", lambda _candidate: None)

    assert compare_compat._find_libreoffice_binary() == str(fake_soffice)


def test_find_libreoffice_binary_falls_back_to_path_lookup(monkeypatch) -> None:
    monkeypatch.delenv("LIBREOFFICE_BIN", raising=False)
    monkeypatch.setattr(
        compare_helpers.shutil,
        "which",
        lambda candidate: "/usr/bin/soffice" if candidate == "soffice" else None,
    )
    monkeypatch.setattr(compare_helpers.os.path, "isfile", lambda path: path == "/usr/bin/soffice")

    assert compare_compat._find_libreoffice_binary() == "/usr/bin/soffice"


def test_build_libreoffice_env_appends_required_linux_paths(monkeypatch) -> None:
    monkeypatch.setattr(compare_helpers.os, "name", "posix")
    monkeypatch.setattr(compare_helpers.os, "pathsep", ":")
    monkeypatch.setenv("PATH", "/custom/bin")

    env = compare_helpers._build_libreoffice_env()

    assert env["PATH"].startswith("/custom/bin")
    assert "/usr/bin" in env["PATH"].split(":")
    assert "/bin" in env["PATH"].split(":")


def test_build_libreoffice_env_keeps_windows_path_unchanged(monkeypatch) -> None:
    monkeypatch.setattr(compare_helpers.os, "name", "nt")
    monkeypatch.setenv("PATH", r"C:\Windows\System32")

    env = compare_helpers._build_libreoffice_env()

    assert env["PATH"] == r"C:\Windows\System32"
