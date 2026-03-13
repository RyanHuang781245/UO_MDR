from app.blueprints.flows.routes import _validate_flow_name


def test_validate_flow_name_accepts_normal_text() -> None:
    assert _validate_flow_name("My Flow 01") is None


def test_validate_flow_name_rejects_slash() -> None:
    assert _validate_flow_name("QA/Review") == '流程名稱不可包含 \\ / : * ? " < > |'


def test_validate_flow_name_rejects_backslash() -> None:
    assert _validate_flow_name(r"QA\Review") == '流程名稱不可包含 \\ / : * ? " < > |'
