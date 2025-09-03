from spire.doc import Document, BuiltinStyle
from modules.Edit_Word import renumber_figures_tables


def _paragraph_text(p):
    # Helper to extract visible text from a paragraph
    return p.Text.strip()


def test_renumber_ignores_list_entries():
    doc = Document()
    sec = doc.AddSection()

    # Simulate list-of-figures and list-of-tables entries that appear before captions
    p0 = sec.AddParagraph()
    p0.AppendText('Figure 3 Foo')
    p0.ApplyStyle(BuiltinStyle.TableOfFigures)

    p1 = sec.AddParagraph()
    p1.AppendText('Table 2 Bar')
    p1.ApplyStyle(BuiltinStyle.TableOfFigures)

    # Actual captions with the same old numbers
    p2 = sec.AddParagraph()
    p2.AppendText('Figure 3 Foo')
    p2.ApplyStyle(BuiltinStyle.Caption)

    p3 = sec.AddParagraph()
    p3.AppendText('Table 2 Bar')
    p3.ApplyStyle(BuiltinStyle.Caption)

    renumber_figures_tables(doc)

    assert _paragraph_text(p2).startswith('Figure 1')
    assert _paragraph_text(p3).startswith('Table 1')
    # List entries should also reflect renumbered values
    assert _paragraph_text(p0).startswith('Figure 1')
    assert _paragraph_text(p1).startswith('Table 1')


def test_reference_updated_before_caption():
    doc = Document()
    sec = doc.AddSection()

    # In-text reference appears before the actual caption
    ref = sec.AddParagraph()
    ref.AppendText('The appearance is shown in Figure 3.')

    cap = sec.AddParagraph()
    cap.AppendText('Figure 3 Sample figure caption')

    renumber_figures_tables(doc)

    assert 'Figure 1' in _paragraph_text(ref)
    assert _paragraph_text(cap).startswith('Figure 1')
