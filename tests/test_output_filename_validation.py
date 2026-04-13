from app.utils import normalize_docx_output_filename, normalize_docx_output_path


def test_normalize_output_filename_accepts_empty() -> None:
    normalized, error = normalize_docx_output_filename("")
    assert error is None
    assert normalized == ""


def test_normalize_output_filename_auto_appends_docx() -> None:
    normalized, error = normalize_docx_output_filename("QA_Report")
    assert error is None
    assert normalized == "QA_Report.docx"


def test_normalize_output_filename_accepts_docx_extension() -> None:
    normalized, error = normalize_docx_output_filename("QA_Report.docx")
    assert error is None
    assert normalized == "QA_Report.docx"


def test_normalize_output_filename_rejects_invalid_extension() -> None:
    normalized, error = normalize_docx_output_filename("QA_Report.pdf")
    assert normalized == ""
    assert error == "輸出檔名副檔名僅支援 .docx"


def test_normalize_output_filename_rejects_invalid_chars() -> None:
    normalized, error = normalize_docx_output_filename("QA/Report")
    assert normalized == ""
    assert error == '輸出檔名不可包含 \\ / : * ? " < > |'


def test_normalize_output_path_accepts_relative_folder_and_auto_appends_docx() -> None:
    normalized, error = normalize_docx_output_path("test/QA_Report")
    assert error is None
    assert normalized == "test/QA_Report.docx"


def test_normalize_output_path_rejects_parent_traversal() -> None:
    normalized, error = normalize_docx_output_path("../QA_Report")
    assert normalized == ""
    assert error == "輸出路徑不合法"

