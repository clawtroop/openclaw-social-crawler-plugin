from __future__ import annotations

from typing import Any

from run_models import WorkItem
from worker_state import WorkerStateStore


AUTH_ERROR_CODES = {
    "AUTH_REQUIRED",
    "AUTH_EXPIRED",
    "AUTH_INTERACTIVE_TIMEOUT",
    "AUTH_SESSION_EXPORT_FAILED",
    "AUTH_AUTO_LOGIN_FAILED",
    "CAPTCHA",
}


class AuthOrchestrator:
    def __init__(self, state_store: WorkerStateStore, *, retry_after_seconds: int) -> None:
        self.state_store = state_store
        self.retry_after_seconds = retry_after_seconds

    def handle_errors(self, item: WorkItem, errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
        auth_pending: list[dict[str, Any]] = []
        for error in errors:
            if str(error.get("error_code") or "") not in AUTH_ERROR_CODES:
                continue
            normalized_error = self._normalize_error(item, error)
            self.state_store.upsert_auth_pending(
                item,
                normalized_error,
                retry_after_seconds=self.retry_after_seconds,
            )
            auth_pending.append({
                "item_id": item.item_id,
                "url": item.url,
                "platform": item.platform,
                "error_code": normalized_error.get("error_code"),
                "next_action": normalized_error.get("next_action"),
                "public_url": normalized_error.get("public_url"),
                "login_url": normalized_error.get("login_url"),
            })
        return auth_pending

    def clear_if_recovered(self, item: WorkItem) -> None:
        self.state_store.clear_auth_pending(item.item_id)

    def _normalize_error(self, item: WorkItem, error: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(error)
        error_code = str(normalized.get("error_code") or "")
        next_action = str(normalized.get("next_action") or "").strip()
        public_url = str(normalized.get("public_url") or "").strip()
        login_url = str(normalized.get("login_url") or "").strip()

        if error_code == "CAPTCHA" and next_action in {"", "notify_user"}:
            normalized["next_action"] = "complete login in auto-browser and retry"

        if not public_url:
            normalized["public_url"] = login_url or item.url
        if error_code == "CAPTCHA" and not login_url:
            normalized["login_url"] = public_url or item.url
        return normalized
