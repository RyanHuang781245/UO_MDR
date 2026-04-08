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
