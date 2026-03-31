from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


def read_openclaw_config() -> dict[str, Any]:
    config_path = (
        Path(os.environ["OPENCLAW_CONFIG_PATH"]).expanduser()
        if os.environ.get("OPENCLAW_CONFIG_PATH")
        else Path.home() / ".openclaw" / "openclaw.json"
    )
    if not config_path.exists() or not config_path.is_file():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_secret_ref(ref: Any, config: dict[str, Any]) -> str:
    if isinstance(ref, str):
        return ref
    if not isinstance(ref, dict):
        return ""

    source = str(ref.get("source", "")).strip()
    provider = str(ref.get("provider", "")).strip()
    ref_id = str(ref.get("id", "")).strip()
    if not source or not provider or not ref_id:
        return ""

    providers = ((config.get("secrets") or {}).get("providers") or {})
    provider_config = providers.get(provider, {})
    if source == "env":
        return os.environ.get(ref_id, "").strip()
    if source == "file":
        return _resolve_file_secret_ref(ref_id, provider_config)
    if source == "exec":
        return _resolve_exec_secret_ref(provider, ref_id, provider_config)
    return ""


def _resolve_file_secret_ref(ref_id: str, provider_config: dict[str, Any]) -> str:
    file_path = str(provider_config.get("path", "")).strip()
    if not file_path:
        return ""
    path = Path(file_path).expanduser()
    if not path.exists() or not path.is_file():
        return ""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return ""
    mode = str(provider_config.get("mode", "json")).strip() or "json"
    if mode == "singleValue":
        return text.rstrip("\r\n")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return ""
    value = _read_json_pointer(payload, ref_id)
    return value if isinstance(value, str) else ""


def _resolve_exec_secret_ref(provider: str, ref_id: str, provider_config: dict[str, Any]) -> str:
    command = str(provider_config.get("command", "")).strip()
    if not command:
        return ""
    args = [str(value) for value in provider_config.get("args", [])]
    payload = json.dumps({"protocolVersion": 1, "provider": provider, "ids": [ref_id]})
    try:
        completed = subprocess.run(
            [command, *args],
            input=payload,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
            timeout=float(provider_config.get("timeoutMs", 5000)) / 1000.0,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return ""
    if completed.returncode != 0:
        return ""
    try:
        response = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return ""
    if not isinstance(response, dict) or response.get("protocolVersion") != 1:
        return ""
    values = response.get("values")
    if not isinstance(values, dict):
        return ""
    value = values.get(ref_id)
    return value.strip() if isinstance(value, str) else ""


def _read_json_pointer(payload: Any, pointer: str) -> Any:
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        return None
    current = payload
    for token in pointer[1:].split("/"):
        decoded = token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            try:
                index = int(decoded)
            except ValueError:
                return None
            if index < 0 or index >= len(current):
                return None
            current = current[index]
            continue
        if not isinstance(current, dict) or decoded not in current:
            return None
        current = current[decoded]
    return current
