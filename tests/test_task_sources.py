from __future__ import annotations

from task_sources import (
    DatasetDiscoverySource,
    build_follow_up_items_from_discovery,
    claimed_task_from_payload,
    claimed_task_to_work_item,
    infer_platform_task,
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


def test_infer_platform_task_falls_back_to_generic() -> None:
    platform, resource_type, fields = infer_platform_task("https://example.com/path")

    assert platform == "generic"
    assert resource_type == "page"
    assert fields["url"] == "https://example.com/path"
