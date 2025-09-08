from spire.doc import Document
from modules.mapping_processor import insert_title


def _style_name(p):
    return p.ListFormat.CustomStyleName


def test_insert_title_numbered():
    doc = Document()
    sec = doc.AddSection()
    p = insert_title(sec, "Heading")
    assert _style_name(p) == "outlineHeading"


def test_insert_title_roman():
    doc = Document()
    sec = doc.AddSection()
    p = insert_title(sec, "I. Scope")
    assert _style_name(p) == "romanHeading"


def test_insert_title_bullet():
    doc = Document()
    sec = doc.AddSection()
    p = insert_title(sec, "⚫ Item")
    assert _style_name(p) == "bulletHeading"


def test_insert_title_strips_chapter_number():
    doc = Document()
    sec = doc.AddSection()
    p = insert_title(sec, "6.4.2 Heading")
    assert _style_name(p) == "outlineHeading"
    assert "6.4.2" not in p.Text
    assert p.Text.strip() == "Heading"
