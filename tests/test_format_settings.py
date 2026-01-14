from app.services.flow_service import (
    DOCUMENT_FORMAT_PRESETS,
    DEFAULT_DOCUMENT_FORMAT_KEY,
    DEFAULT_LINE_SPACING,
    coerce_line_spacing,
    normalize_document_format,
)


def test_normalize_document_format_handles_unknown_keys():
    assert normalize_document_format(None) == DEFAULT_DOCUMENT_FORMAT_KEY
    assert normalize_document_format("") == DEFAULT_DOCUMENT_FORMAT_KEY
    assert normalize_document_format("unknown") == DEFAULT_DOCUMENT_FORMAT_KEY
    for key in DOCUMENT_FORMAT_PRESETS:
        assert normalize_document_format(key) == key


def test_coerce_line_spacing_returns_valid_float():
    assert coerce_line_spacing("2") == 2.0
    assert coerce_line_spacing("1.25") == 1.25
    assert coerce_line_spacing(1.1) == 1.1


def test_coerce_line_spacing_defaults_on_invalid_values():
    assert coerce_line_spacing(None) == DEFAULT_LINE_SPACING
    assert coerce_line_spacing("not-a-number") == DEFAULT_LINE_SPACING
    assert coerce_line_spacing(0) == DEFAULT_LINE_SPACING
    assert coerce_line_spacing(-1) == DEFAULT_LINE_SPACING
