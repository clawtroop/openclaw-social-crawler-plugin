from __future__ import annotations

from task_sources import (
    BackendClaimSource,
    DatasetDiscoverySource,
    build_follow_up_items_from_discovery,
    claimed_task_from_payload,
    claimed_task_to_work_item,
    infer_platform_task,
    local_task_from_payload,
    task_to_work_item,
)
from worker_state import WorkerStateStore


class FakeClient:
    def fetch_core_submission(self, submission_id: str) -> dict:
        return {
            "dataset_id": "dataset-1",
            "original_url": "https://www.linkedin.com/in/test-user/",
        }

    def list_datasets(self) -> list[dict]:
        return [
            {
                "id": "dataset-1",
                "source_domains": ["en.wikipedia.org", "www.linkedin.com"],
            }
        ]


class BrokenClaimClient(FakeClient):
    def claim_repeat_crawl_task(self) -> dict:
        return {"id": "repeat-1", "submission_id": "sub-missing"}

    def claim_refresh_task(self) -> dict:
        return {"id": "refresh-1", "dataset_id": "dataset-1", "url": "https://arxiv.org/abs/2401.12345"}

    def fetch_core_submission(self, submission_id: str) -> dict:
        raise RuntimeError(f"submission not found: {submission_id}")


def test_claimed_repeat_task_enriches_url_from_submission() -> None:
    task = claimed_task_from_payload(
        "repeat_crawl",
        {"id": "task-1", "submission_id": "sub-1"},
        client=FakeClient(),
    )

    assert task.dataset_id == "dataset-1"
    assert task.platform == "linkedin"
    assert task.url == "https://www.linkedin.com/in/test-user/"


def test_claimed_task_converts_to_backend_work_item() -> None:
    task = claimed_task_from_payload(
        "refresh",
        {"id": "task-2", "dataset_id": "dataset-1", "url": "https://arxiv.org/abs/2401.12345"},
        client=FakeClient(),
    )
    item = claimed_task_to_work_item(task)

    assert item.claim_task_id == "task-2"
    assert item.claim_task_type == "refresh"
    assert item.record["platform"] == "arxiv"


def test_dataset_discovery_source_builds_seed_items(workspace_tmp_path) -> None:
    store = WorkerStateStore(workspace_tmp_path / "state")
    source = DatasetDiscoverySource(FakeClient(), store)

    items = source.collect(min_interval_seconds=0)

    assert len(items) == 2
    assert all(item.crawler_command == "discover-crawl" for item in items)


def test_build_follow_up_items_from_discovery_uses_canonical_urls() -> None:
    parent = claimed_task_to_work_item(
        claimed_task_from_payload(
            "refresh",
            {"id": "task-3", "dataset_id": "dataset-1", "url": "https://example.com"},
            client=FakeClient(),
        )
    )
    followups = build_follow_up_items_from_discovery(
        parent,
        [
            {"canonical_url": "https://en.wikipedia.org/wiki/Artificial_intelligence", "platform": "wikipedia", "resource_type": "article"},
            {"canonical_url": "https://www.linkedin.com/company/openai/", "platform": "linkedin", "resource_type": "company"},
        ],
    )

    assert [item.crawler_command for item in followups] == ["run", "run"]
    assert [item.platform for item in followups] == ["wikipedia", "linkedin"]


def test_claimed_and_local_tasks_normalize_parity() -> None:
    claimed = claimed_task_from_payload(
        "refresh",
        {"id": "task-4", "dataset_id": "dataset-1", "url": "https://arxiv.org/abs/2401.12345"},
        client=FakeClient(),
    )
    claimed_item = task_to_work_item(claimed)
    local = local_task_from_payload(
        {
            "task_id": "local-1",
            "task_type": "local_refresh",
            "url": "https://arxiv.org/abs/2401.12345",
            "dataset_id": "dataset-1",
        }
    )
    local_item = task_to_work_item(local)

    assert claimed_item.dataset_id == local_item.dataset_id
    assert claimed_item.platform == local_item.platform
    assert claimed_item.resource_type == local_item.resource_type
    assert claimed_item.url == local_item.url


def test_infer_platform_task_falls_back_to_generic() -> None:
    platform, resource_type, fields = infer_platform_task("https://example.com/path")

    assert platform == "generic"
    assert resource_type == "page"
    assert fields["url"] == "https://example.com/path"


def test_backend_claim_source_isolates_broken_repeat_task() -> None:
    source = BackendClaimSource(BrokenClaimClient())

    items = source.collect()

    assert len(items) == 1
    assert items[0].claim_task_type == "refresh"
    assert any("repeat_crawl task repeat-1 skipped" in error for error in source.last_errors)
