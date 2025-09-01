import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import build_chapter_segments

def test_build_chapter_segments():
    entries = [
        {"type": "insert_roman_heading", "params": {"text": "Chapter A"}},
        {"type": "extract_word_chapter", "params": {"input_file": os.path.join('dir','a.docx')}, "titles": [{"title": "1.1", "index": 3}]},
        {"type": "extract_word_chapter", "params": {"input_file": os.path.join('dir','b.docx')}, "titles": [{"title": "1.2", "index": 7}]}
    ]
    segs = build_chapter_segments(entries)
    assert "Chapter A" in segs
    assert segs["Chapter A"] == [
        {"index": 3, "source": "a.docx"},
        {"index": 7, "source": "b.docx"},
    ]
