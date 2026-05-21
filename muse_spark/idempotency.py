from __future__ import annotations

import hashlib
import json
from typing import Any, Optional


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def request_hash(body: dict[str, Any]) -> str:
    return sha256_text(canonical_json(body))


def normalized_latest_user_turn(messages: list[dict[str, Any]]) -> str:
    for message in reversed(messages):
        if str(message.get("role") or "").lower() != "user":
            continue
        content = message.get("content")
        if isinstance(content, str):
            return " ".join(content.split())
        return canonical_json(content)
    return ""


def resolve_idempotency_key(header_value: Optional[str], body: dict[str, Any]) -> str:
    if header_value and header_value.strip():
        return header_value.strip()
    conversation_id = str(body.get("conversation_id") or "new")
    latest_user = normalized_latest_user_turn(body.get("messages") or [])
    body_hash = request_hash(body)
    return "muse-auto-" + sha256_text(f"{conversation_id}\n{latest_user}\n{body_hash}")
