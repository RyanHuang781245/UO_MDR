import os, json, sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app


def test_task_compare_save_accepts_json(tmp_path, monkeypatch):
    task_id = 'tid'
    job_id = 'jid'
    job_dir = tmp_path / task_id / 'jobs' / job_id
    job_dir.mkdir(parents=True)
    app.config['TASK_FOLDER'] = str(tmp_path)

    def fake_apply_basic_style(path):
        pass
    monkeypatch.setattr('app.apply_basic_style', fake_apply_basic_style)

    class DummyDoc:
        def LoadFromFile(self, path, fmt):
            self.path = path
        def SaveToFile(self, path, fmt):
            with open(path, 'w', encoding='utf-8') as f:
                f.write('doc')
        def Close(self):
            pass
    monkeypatch.setattr('spire.doc.Document', DummyDoc)
    class DummyFmt:
        Html = 0
        Docx = 1
    monkeypatch.setattr('spire.doc.FileFormat', DummyFmt)

    client = app.test_client()
    html = '<html><body><p>hi</p></body></html>'
    resp = client.post(f'/tasks/{task_id}/compare/{job_id}/save', json={'html': html})
    assert resp.status_code == 200
    saved_html = (job_dir / 'result.html').read_text(encoding='utf-8')
    assert saved_html == html
    saved_doc = (job_dir / 'result.docx').read_text(encoding='utf-8')
    assert saved_doc == 'doc'
