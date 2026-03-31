from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from run_models import WorkItem


class WorkerStateStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._backlog_path = self.root / "backlog.json"
        self._auth_pending_path = self.root / "auth_pending.json"
        self._submit_pending_path = self.root / "submit_pending.json"
        self._dataset_cursors_path = self.root / "dataset_cursors.json"

    def load_backlog(self) -> list[WorkItem]:
        return [WorkItem.from_dict(item) for item in self._read_list(self._backlog_path)]

    def enqueue_backlog(self, items: list[WorkItem]) -> None:
        payload = self._read_list(self._backlog_path)
        merged = {str(item.get("item_id") or ""): item for item in payload if item.get("item_id")}
        for item in items:
            merged[item.item_id] = item.to_dict()
        self._write_json(self._backlog_path, list(merged.values()))

    def pop_backlog(self, limit: int) -> list[WorkItem]:
        items = self.load_backlog()
        popped = items[:limit]
        remaining = items[limit:]
        self._write_json(self._backlog_path, [item.to_dict() for item in remaining])
        return popped

    def load_auth_pending(self) -> list[dict[str, Any]]:
        return self._read_list(self._auth_pending_path)

    def upsert_auth_pending(self, item: WorkItem, error: dict[str, Any], *, retry_after_seconds: int) -> None:
        payload = self._read_list(self._auth_pending_path)
        merged = {str(entry.get("item_id") or ""): entry for entry in payload if entry.get("item_id")}
        merged[item.item_id] = {
            "item_id": item.item_id,
            "item": item.to_dict(),
            "error": dict(error),
            "available_at": int(time.time()) + max(0, retry_after_seconds),
            "updated_at": int(time.time()),
        }
        self._write_json(self._auth_pending_path, list(merged.values()))

    def clear_auth_pending(self, item_id: str) -> None:
        payload = [entry for entry in self._read_list(self._auth_pending_path) if str(entry.get("item_id")) != item_id]
        self._write_json(self._auth_pending_path, payload)

    def pop_due_auth_pending(self, limit: int, *, now: int | None = None) -> list[WorkItem]:
        current = int(time.time()) if now is None else now
        payload = self._read_list(self._auth_pending_path)
        due: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        for entry in payload:
            available_at = int(entry.get("available_at") or 0)
            if available_at <= current and len(due) < limit:
                due.append(entry)
            else:
                remaining.append(entry)
        self._write_json(self._auth_pending_path, remaining)
        return [WorkItem.from_dict(dict(entry.get("item") or {})) for entry in due]

    def enqueue_submit_pending(self, item: WorkItem, payload: dict[str, Any]) -> None:
        entries = self._read_list(self._submit_pending_path)
        entries.append({
            "item_id": item.item_id,
            "item": item.to_dict(),
            "payload": payload,
            "updated_at": int(time.time()),
        })
        self._write_json(self._submit_pending_path, entries)

    def load_submit_pending(self) -> list[dict[str, Any]]:
        return self._read_list(self._submit_pending_path)

    def clear_submit_pending(self, item_id: str) -> None:
        payload = [entry for entry in self._read_list(self._submit_pending_path) if str(entry.get("item_id")) != item_id]
        self._write_json(self._submit_pending_path, payload)

    def should_schedule_dataset(self, dataset_id: str, *, min_interval_seconds: int, now: int | None = None) -> bool:
        current = int(time.time()) if now is None else now
        cursors = self._read_object(self._dataset_cursors_path)
        last_run = int((cursors.get(dataset_id) or {}).get("last_scheduled_at") or 0)
        return current - last_run >= max(0, min_interval_seconds)

    def mark_dataset_scheduled(self, dataset_id: str, *, now: int | None = None) -> None:
        current = int(time.time()) if now is None else now
        cursors = self._read_object(self._dataset_cursors_path)
        cursors[dataset_id] = {"last_scheduled_at": current}
        self._write_json(self._dataset_cursors_path, cursors)

    def _read_list(self, path: Path) -> list[dict[str, Any]]:
        payload = self._read_json(path)
        return payload if isinstance(payload, list) else []

    def _read_object(self, path: Path) -> dict[str, Any]:
        payload = self._read_json(path)
        return payload if isinstance(payload, dict) else {}

    def _read_json(self, path: Path) -> Any:
        if not path.exists():
            return [] if path.suffix == ".json" and path.name != "dataset_cursors.json" else {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_json(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
