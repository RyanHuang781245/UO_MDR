import os
import json
from app import app


def test_mapping_page_get(tmp_path):
    app.config['TASK_FOLDER'] = str(tmp_path)
    task_id = 'abc123'
    tdir = tmp_path / task_id / 'files'
    tdir.mkdir(parents=True)
    client = app.test_client()
    resp = client.get(f'/tasks/{task_id}/mapping')
    assert resp.status_code == 200
    assert b'mapping_file' in resp.data
