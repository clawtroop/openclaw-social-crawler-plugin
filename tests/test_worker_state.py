from __future__ import annotations

from run_models import WorkItem
from worker_state import WorkerStateStore


def _item(item_id: str) -> WorkItem:
    return WorkItem(
        item_id=item_id,
        source="backend_claim",
        url="https://example.com",
        dataset_id="dataset-1",
        platform="generic",
        resource_type="page",
        record={"platform": "generic", "resource_type": "page", "url": "https://example.com"},
    )


def test_worker_state_store_round_trips_backlog(workspace_tmp_path) -> None:
    store = WorkerStateStore(workspace_tmp_path / "state")
    store.enqueue_backlog([_item("a"), _item("b")])

    popped = store.pop_backlog(1)

    assert [item.item_id for item in popped] == ["a"]
    assert [item.item_id for item in store.load_backlog()] == ["b"]


def test_worker_state_store_manages_auth_pending(workspace_tmp_path) -> None:
    store = WorkerStateStore(workspace_tmp_path / "state")
    item = _item("auth-1")
    store.upsert_auth_pending(item, {"error_code": "AUTH_REQUIRED", "public_url": "https://vrd.example"}, retry_after_seconds=0)

    due = store.pop_due_auth_pending(5, now=9999999999)

    assert [entry.item_id for entry in due] == ["auth-1"]
    assert store.load_auth_pending() == []


def test_worker_state_store_tracks_dataset_schedule(workspace_tmp_path) -> None:
    store = WorkerStateStore(workspace_tmp_path / "state")

    assert store.should_schedule_dataset("dataset-1", min_interval_seconds=300, now=1000) is True
    store.mark_dataset_scheduled("dataset-1", now=1000)
    assert store.should_schedule_dataset("dataset-1", min_interval_seconds=300, now=1100) is False
    assert store.should_schedule_dataset("dataset-1", min_interval_seconds=300, now=1401) is True
