from __future__ import annotations

import json
import os
from typing import Any

from secret_refs import read_openclaw_config, resolve_secret_ref

DEFAULT_GATEWAY_BASE_URL = "http://127.0.0.1:18789/v1"
DEFAULT_GATEWAY_MODEL = "openclaw/default"


def resolve_openclaw_enrich_model_config() -> dict[str, Any]:
    if not _openclaw_enrich_enabled():
        return {}

    token = os.environ.get("OPENCLAW_GATEWAY_TOKEN", "").strip()
    if not token:
        token = _read_openclaw_token_from_config().strip()
    if not token:
        return {}

    config: dict[str, Any] = {
        "provider": "openclaw",
        "base_url": os.environ.get("OPENCLAW_GATEWAY_BASE_URL", DEFAULT_GATEWAY_BASE_URL).strip() or DEFAULT_GATEWAY_BASE_URL,
        "api_key": token,
        "model": os.environ.get("OPENCLAW_ENRICH_MODEL", DEFAULT_GATEWAY_MODEL).strip() or DEFAULT_GATEWAY_MODEL,
    }
    upstream_model = os.environ.get("OPENCLAW_UPSTREAM_MODEL", "").strip()
    if upstream_model:
        config["openclaw_model"] = upstream_model
    return config


def write_model_config(path: Path, model_config: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model_config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _openclaw_enrich_enabled() -> bool:
    mode = os.environ.get("OPENCLAW_ENRICH_MODE", "auto").strip().lower()
    return mode not in {"0", "false", "off", "disabled"}


def _read_openclaw_token_from_config() -> str:
    payload = read_openclaw_config()
    token = (((payload.get("gateway") or {}).get("auth") or {}).get("token"))
    if isinstance(token, str):
        return token
    return resolve_secret_ref(token, payload)
