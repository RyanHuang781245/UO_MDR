import os
from app import app

def test_get_mapping_upload_page(tmp_path):
    app.config['TASK_FOLDER'] = str(tmp_path)
    task_id = 'task1'
    files_dir = tmp_path / task_id / 'files'
    files_dir.mkdir(parents=True)
    client = app.test_client()
    resp = client.get(f'/tasks/{task_id}/mapping')
    assert resp.status_code == 200
    assert '上傳 Mapping'.encode('utf-8') in resp.data
