import pytest


def test_api_nas_list_dirs_lists_only_directories(tmp_path, app, client):
    original_nas_roots = app.config.get("NAS_ALLOWED_ROOTS")
    original_allowed_nas_roots = app.config.get("ALLOWED_NAS_ROOTS")
    original_recursive = app.config.get("NAS_ALLOW_RECURSIVE")

    root = tmp_path / "nas"
    (root / "a" / "x").mkdir(parents=True)
    (root / "b").mkdir()
    (root / "file.txt").write_text("content")

    try:
        app.config["NAS_ALLOWED_ROOTS"] = [str(root)]
        app.config["ALLOWED_NAS_ROOTS"] = [str(root)]
        app.config["NAS_ALLOW_RECURSIVE"] = True

        resp = client.get("/api/nas/dirs?root_index=0")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["path"] == ""
        assert [d["name"] for d in data["dirs"]] == ["a", "b"]

        resp = client.get("/api/nas/dirs?root_index=0&path=a")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["path"] == "a"
        assert [d["name"] for d in data["dirs"]] == ["x"]
        assert data["dirs"][0]["path"] == "a/x"
    finally:
        app.config["NAS_ALLOWED_ROOTS"] = original_nas_roots
        app.config["ALLOWED_NAS_ROOTS"] = original_allowed_nas_roots
        app.config["NAS_ALLOW_RECURSIVE"] = original_recursive


def test_api_nas_list_dirs_respects_recursive_setting(tmp_path, app, client):
    original_nas_roots = app.config.get("NAS_ALLOWED_ROOTS")
    original_allowed_nas_roots = app.config.get("ALLOWED_NAS_ROOTS")
    original_recursive = app.config.get("NAS_ALLOW_RECURSIVE")

    root = tmp_path / "nas"
    (root / "a" / "x").mkdir(parents=True)

    try:
        app.config["NAS_ALLOWED_ROOTS"] = [str(root)]
        app.config["ALLOWED_NAS_ROOTS"] = [str(root)]
        app.config["NAS_ALLOW_RECURSIVE"] = False

        resp = client.get("/api/nas/dirs?root_index=0&path=a/x")
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data
    finally:
        app.config["NAS_ALLOWED_ROOTS"] = original_nas_roots
        app.config["ALLOWED_NAS_ROOTS"] = original_allowed_nas_roots
        app.config["NAS_ALLOW_RECURSIVE"] = original_recursive


def test_api_nas_list_dirs_invalid_root_index(tmp_path, app, client):
    original_nas_roots = app.config.get("NAS_ALLOWED_ROOTS")
    original_allowed_nas_roots = app.config.get("ALLOWED_NAS_ROOTS")

    root = tmp_path / "nas"
    root.mkdir()

    try:
        app.config["NAS_ALLOWED_ROOTS"] = [str(root)]
        app.config["ALLOWED_NAS_ROOTS"] = [str(root)]

        resp = client.get("/api/nas/dirs?root_index=5")
        assert resp.status_code == 400
        data = resp.get_json()
        assert data["error"] == "Invalid NAS root index"
    finally:
        app.config["NAS_ALLOWED_ROOTS"] = original_nas_roots
        app.config["ALLOWED_NAS_ROOTS"] = original_allowed_nas_roots

