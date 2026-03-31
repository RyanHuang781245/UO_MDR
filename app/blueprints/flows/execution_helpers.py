from __future__ import annotations

from app.jobs.executor import enqueue_single_flow_job

from .flow_file_helpers import _resolve_task_file_path


def _resolve_runtime_step_params(files_dir: str, schema: dict, raw_params: dict) -> dict:
    params = {}
    for key, value in raw_params.items():
        accept = schema.get("accepts", {}).get(key, "text")
        if isinstance(accept, str) and accept.startswith("file") and value:
            expect_dir = (
                True if accept.endswith(":dir")
                else False if accept.endswith(":docx") or accept.endswith(":pdf") or accept.endswith(":zip")
                else None
            )
            params[key] = _resolve_task_file_path(files_dir, str(value), expect_dir=expect_dir)
        else:
            params[key] = value
    return params


def _queue_single_flow_job(
    task_id: str,
    runtime_steps: list[dict],
    template_cfg: dict | None,
    document_format: str,
    line_spacing: float,
    apply_formatting: bool,
    actor: dict,
    flow_name: str,
    output_filename: str = "",
    source: str = "manual",
) -> str:
    return enqueue_single_flow_job(
        task_id=task_id,
        runtime_steps=runtime_steps,
        template_cfg=template_cfg,
        document_format=document_format,
        line_spacing=line_spacing,
        apply_formatting=apply_formatting,
        actor=actor,
        flow_name=flow_name,
        output_filename=output_filename,
        source=source,
    )
