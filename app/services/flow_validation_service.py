from __future__ import annotations

from typing import Any

from app.utils import parse_bool


FLOW_VALIDATION_RULES = {
    "required_by_action": {
        "save": ["flow_name"],
        "save_as": ["flow_name", "save_as_name"],
        "run": ["flow_name"],
        "save_version": ["flow_name", "version_name"],
    },
    "conditional_rules": [
        {
            "actions": ["save", "save_as", "run"],
            "when": {"field": "enable_output_filename", "equals": True},
            "fields": ["output_filename"],
            "message": "已勾選輸出檔案路徑時，請輸入輸出檔案路徑",
        },
        {
            "actions": ["run"],
            "when": {"field": "_has_any_template_index", "equals": True},
            "fields": ["template_file"],
            "message": "步驟已指定模板段落時，執行流程前必須先載入模板",
        },
    ],
}

FLOW_REQUIRED_MESSAGES = {
    "flow_name": "請輸入流程名稱",
    "save_as_name": "請輸入流程名稱",
    "version_name": "請輸入版本名稱",
}

STEP_REQUIRED_MESSAGES = {
    "input_file": "請選擇輸入檔案",
    "text": "請輸入文字內容",
    "source_dir": "請選擇來源檔案或資料夾",
}


def _has_text(value: Any) -> bool:
    return bool(str(value or "").strip())


def _raw_form_value(form, key: str) -> str:
    return str(form.get(key, "") or "").strip()


def _iter_submitted_steps(form, supported_steps: dict):
    ordered_ids = [item.strip() for item in str(form.get("ordered_ids", "") or "").split(",") if item.strip()]
    for step_id in ordered_ids:
        step_type = _raw_form_value(form, f"step_{step_id}_type")
        if not step_type or step_type not in supported_steps:
            continue
        schema = supported_steps.get(step_type, {})
        yield step_id, step_type, schema


def _submitted_step_values(form, step_id: str, schema: dict) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in schema.get("inputs", []):
        values[key] = _raw_form_value(form, f"step_{step_id}_{key}")
    # UI-only proxy fields used by validation rules.
    for key in ("chapter_ref", "continuous_extract", "continuous_end_ref", "use_chapter_title"):
        values[key] = _raw_form_value(form, f"step_{step_id}_{key}")
    return values


def _step_uses_template_index(values: dict[str, str]) -> bool:
    return _has_text(values.get("template_index"))


def _evaluate_condition(values: dict[str, str], condition: dict[str, Any]) -> bool:
    field = str(condition.get("field") or "").strip()
    if not field:
        return False
    actual = values.get(field, "")
    if not actual and field == "continuous_extract":
        actual = "true" if any(_has_text(values.get(name)) for name in ("explicit_end_number", "explicit_end_title", "continuous_end_ref")) else "false"
    expected = condition.get("equals")
    if isinstance(expected, bool):
        return parse_bool(actual, False) is expected
    return str(actual) == str(expected)


def _validate_step(step_id: str, step_type: str, schema: dict, values: dict[str, str]) -> list[dict[str, str | None]]:
    validation = schema.get("validation") or {}
    errors: list[dict[str, str | None]] = []

    for field in validation.get("required", []):
        if _has_text(values.get(field)):
            continue
        errors.append(
            {
                "scope": "step",
                "step_id": step_id,
                "step_type": step_type,
                "field": field,
                "message": f"步驟「{schema.get('label') or step_type}」{STEP_REQUIRED_MESSAGES.get(field, '缺少必填欄位')}",
            }
        )

    for rule in validation.get("composite_required", []):
        fields = [str(item) for item in rule.get("fields", []) if str(item).strip()]
        if fields and all(_has_text(values.get(field)) for field in fields):
            continue
        errors.append(
            {
                "scope": "step",
                "step_id": step_id,
                "step_type": step_type,
                "field": str(rule.get("proxy_field") or (fields[0] if fields else "")),
                "message": str(rule.get("message") or f"步驟「{schema.get('label') or step_type}」缺少必填欄位"),
            }
        )

    for rule in validation.get("conditional_required", []):
        condition = rule.get("when") or {}
        if not _evaluate_condition(values, condition):
            continue
        fields = [str(item) for item in rule.get("fields", []) if str(item).strip()]
        if fields and all(_has_text(values.get(field)) for field in fields):
            continue
        errors.append(
            {
                "scope": "step",
                "step_id": step_id,
                "step_type": step_type,
                "field": str(rule.get("proxy_field") or (fields[0] if fields else "")),
                "message": str(rule.get("message") or f"步驟「{schema.get('label') or step_type}」缺少必填欄位"),
            }
        )

    for rule in validation.get("at_least_one_of", []):
        fields = [str(item) for item in rule.get("fields", []) if str(item).strip()]
        if fields and any(_has_text(values.get(field)) for field in fields):
            continue
        errors.append(
            {
                "scope": "step",
                "step_id": step_id,
                "step_type": step_type,
                "field": str(rule.get("proxy_field") or (fields[0] if fields else "")),
                "message": str(rule.get("message") or f"步驟「{schema.get('label') or step_type}」缺少必填欄位"),
            }
        )

    return errors


def validate_flow_submission(action: str, form, supported_steps: dict) -> list[dict[str, str | None]]:
    errors: list[dict[str, str | None]] = []
    step_validation_actions = {"save", "save_as", "run"}

    required_fields = FLOW_VALIDATION_RULES.get("required_by_action", {}).get(action, [])
    for field in required_fields:
        if _has_text(_raw_form_value(form, field)):
            continue
        errors.append(
            {
                "scope": "flow",
                "step_id": None,
                "step_type": None,
                "field": field,
                "message": FLOW_REQUIRED_MESSAGES.get(field, "缺少必填欄位"),
            }
        )

    has_any_template_index = False
    if action in step_validation_actions:
        for step_id, step_type, schema in _iter_submitted_steps(form, supported_steps):
            values = _submitted_step_values(form, step_id, schema)
            if _step_uses_template_index(values):
                has_any_template_index = True
            errors.extend(_validate_step(step_id, step_type, schema, values))

    flow_values = {
        "enable_output_filename": "true" if parse_bool(form.get("enable_output_filename"), False) else "false",
        "output_filename": _raw_form_value(form, "output_filename"),
        "template_file": _raw_form_value(form, "template_file"),
        "_has_any_template_index": "true" if has_any_template_index else "false",
    }
    for rule in FLOW_VALIDATION_RULES.get("conditional_rules", []):
        if action not in set(rule.get("actions", [])):
            continue
        if not _evaluate_condition(flow_values, rule.get("when") or {}):
            continue
        for field in rule.get("fields", []):
            if _has_text(flow_values.get(str(field))):
                continue
            errors.append(
                {
                    "scope": "flow",
                    "step_id": None,
                    "step_type": None,
                    "field": str(field),
                    "message": str(rule.get("message") or "缺少必填欄位"),
                }
            )

    return errors


def validate_saved_flow_run(payload, supported_steps: dict) -> list[dict[str, str | None]]:
    errors: list[dict[str, str | None]] = []
    if isinstance(payload, dict):
        workflow = payload.get("steps", [])
        template_file = str(payload.get("template_file", "") or "").strip()
        output_filename = str(payload.get("output_filename", "") or "").strip()
    else:
        workflow = payload if isinstance(payload, list) else []
        template_file = ""
        output_filename = ""

    has_any_template_index = False
    for index, step in enumerate(workflow, start=1):
        if not isinstance(step, dict):
            continue
        step_type = str(step.get("type") or "").strip()
        if not step_type or step_type not in supported_steps:
            continue
        schema = supported_steps.get(step_type, {})
        params = step.get("params", {}) if isinstance(step.get("params"), dict) else {}
        values = {key: str(params.get(key, "") or "").strip() for key in schema.get("inputs", [])}
        values["chapter_ref"] = " ".join(part for part in (values.get("target_chapter_section", ""), values.get("target_chapter_title", "")) if part).strip()
        values["continuous_end_ref"] = " ".join(part for part in (values.get("explicit_end_number", ""), values.get("explicit_end_title", "")) if part).strip()
        if _step_uses_template_index(values):
            has_any_template_index = True
        step_errors = _validate_step(f"saved_{index}", step_type, schema, values)
        errors.extend(step_errors)

    flow_values = {
        "enable_output_filename": "true" if output_filename else "false",
        "output_filename": output_filename,
        "template_file": template_file,
        "_has_any_template_index": "true" if has_any_template_index else "false",
    }
    for rule in FLOW_VALIDATION_RULES.get("conditional_rules", []):
        if "run" not in set(rule.get("actions", [])):
            continue
        if not _evaluate_condition(flow_values, rule.get("when") or {}):
            continue
        for field in rule.get("fields", []):
            if _has_text(flow_values.get(str(field))):
                continue
            errors.append(
                {
                    "scope": "flow",
                    "step_id": None,
                    "step_type": None,
                    "field": str(field),
                    "message": str(rule.get("message") or "缺少必填欄位"),
                }
            )

    return errors
