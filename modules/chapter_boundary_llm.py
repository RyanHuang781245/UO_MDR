import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def _llm_log(message: str) -> None:
    print(f"[chapter-boundary-llm] {message}")


def _extract_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return data if isinstance(data, dict) else None


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if isinstance(item, dict):
            text = item.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "\n".join(parts)


def choose_next_heading_boundary_candidate(
    *,
    start_heading_text: str,
    start_number: str,
    candidates: list[dict[str, Any]],
    model_id: str | None = None,
) -> int | None:
    endpoint = (os.getenv("AZURE_OPENAI_ENDPOINT") or "").strip().rstrip("/")
    api_key = (os.getenv("AZURE_OPENAI_API_KEY") or "").strip()
    deployment = (model_id or os.getenv("AZURE_OPENAI_DEPLOYMENT") or "").strip()
    api_version = (os.getenv("AZURE_OPENAI_API_VERSION") or "").strip()

    missing: list[str] = []
    if not endpoint:
        missing.append("AZURE_OPENAI_ENDPOINT")
    if not api_key:
        missing.append("AZURE_OPENAI_API_KEY")
    if not deployment:
        missing.append("AZURE_OPENAI_DEPLOYMENT")
    if not api_version:
        missing.append("AZURE_OPENAI_API_VERSION")
    if missing:
        _llm_log(f"skip api call: missing {', '.join(missing)}")
        return None
    if not candidates:
        _llm_log("skip api call: no candidates")
        return None

    valid_indexes = {item["block_index"] for item in candidates if isinstance(item.get("block_index"), int)}
    if not valid_indexes:
        _llm_log("skip api call: candidate set is empty after validation")
        return None

    payload = {
        "start_heading_text": start_heading_text,
        "start_number": start_number,
        "task": (
            "Choose the most likely next chapter heading block after the current section. "
            "Return JSON only."
        ),
        "rules": [
            "Prefer short heading-like text over sentence-like body text.",
            "Use style_id, ilvl, heading_depth, numbering_prefix, is_bold, and xml_excerpt as structural clues.",
            "Only choose a block_index from the provided candidates.",
            "If none is convincing, return candidate_block_index as null.",
        ],
        "candidates": candidates,
        "response_schema": {
            "candidate_block_index": "integer or null",
            "reason": "short string",
        },
    }

    body = {
        "messages": [
            {
                "role": "system",
                "content": (
                    "You detect Word heading boundaries from simplified XML signals. "
                    "Respond with a single JSON object only."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(payload, ensure_ascii=False),
            },
        ],
        "temperature": 0,
        "max_tokens": 200,
    }

    url = (
        f"{endpoint}/openai/deployments/{urllib.parse.quote(deployment)}/chat/completions"
        f"?api-version={urllib.parse.quote(api_version)}"
    )
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "api-key": api_key,
        },
        method="POST",
    )

    try:
        _llm_log(
            f"calling Azure OpenAI deployment={deployment} candidates={len(candidates)}"
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            detail = ""
        _llm_log(f"api error: HTTP {exc.code} {detail[:300]}")
        return None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        _llm_log(f"api error: {type(exc).__name__}: {exc}")
        return None

    try:
        content = response_payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        _llm_log("api response missing choices/message/content")
        return None

    parsed = _extract_json_object(_message_text(content))
    if not parsed:
        _llm_log("api response did not contain valid JSON object")
        return None

    block_index = parsed.get("candidate_block_index")
    if isinstance(block_index, int) and block_index in valid_indexes:
        _llm_log(f"api selected block_index={block_index}")
        return block_index
    _llm_log(f"api returned invalid candidate_block_index={block_index!r}")
    return None
