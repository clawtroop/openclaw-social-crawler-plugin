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
            self.state_store.upsert_auth_pending(
                item,
                error,
                retry_after_seconds=self.retry_after_seconds,
            )
            auth_pending.append({
                "item_id": item.item_id,
                "url": item.url,
                "platform": item.platform,
                "error_code": error.get("error_code"),
                "next_action": error.get("next_action"),
                "public_url": error.get("public_url"),
                "login_url": error.get("login_url"),
            })
        return auth_pending

    def clear_if_recovered(self, item: WorkItem) -> None:
        self.state_store.clear_auth_pending(item.item_id)
