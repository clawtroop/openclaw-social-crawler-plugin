from __future__ import annotations

from pathlib import Path

from pow_solver import UnsupportedChallenge
from run_artifacts import RunArtifactWriter
from run_models import CrawlerRunResult, WorkItem
import httpx


CRAWLER_WORKTREE = (Path(__file__).resolve().parents[2] / ".worktrees" / "social-data-crawler-e2e-run-once").resolve()


def test_run_artifact_writer_persists_challenge_payload(workspace_tmp_path) -> None:
    writer = RunArtifactWriter(workspace_tmp_path / "run-1")
    challenge = {"id": "pow-1", "question_type": "unknown", "prompt": "solve me"}

    writer.write_json("preflight/challenge.json", challenge)

    stored = (workspace_tmp_path / "run-1" / "preflight" / "challenge.json").read_text(encoding="utf-8")
    assert '"id": "pow-1"' in stored


def test_unsupported_challenge_is_explicit() -> None:
    error = UnsupportedChallenge("unknown")

    assert str(error) == "unsupported challenge type: unknown"


class _FakeClient:
    def send_unified_heartbeat(self, *, client_name: str, ip_address: str = "") -> dict:
        return {"ok": True}

    def send_miner_heartbeat(self, *, client_name: str) -> None:
        return None

    def check_url_occupancy(self, dataset_id: str, url: str) -> dict:
        return {"occupied": False}

    def submit_preflight(self, dataset_id: str, epoch_id: str) -> dict:
        return {"data": {"allowed": True, "challenge": {"id": "pow-1", "question_type": "unknown"}}}


class _FakeRunner:
    def __init__(self, result: CrawlerRunResult, *, output_root: Path | None = None) -> None:
        self.result = result
        self.output_root = output_root or result.output_dir.parent

    def run_item(self, item: WorkItem, command: str) -> CrawlerRunResult:
        return self.result


class _FakeLocalClient(_FakeClient):
    def submit_preflight(self, dataset_id: str, epoch_id: str) -> dict:
        return {"data": {"allowed": True}}


class _UnauthorizedClient(_FakeClient):
    def claim_repeat_crawl_task(self) -> dict:
        request = httpx.Request("POST", "http://example.test/api/mining/v1/repeat-crawl-tasks/claim")
        response = httpx.Response(401, request=request)
        raise httpx.HTTPStatusError("unauthorized", request=request, response=response)

    def claim_refresh_task(self) -> None:
        return None

    def list_datasets(self) -> list[dict]:
        return []


def test_run_once_returns_explicit_unsolved_challenge_state(workspace_tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_CRAWLER_ROOT",
        str(CRAWLER_WORKTREE),
    )
    from agent_runtime import run_single_item_for_test

    result = CrawlerRunResult(
        output_dir=workspace_tmp_path / "crawl-out",
        records=[{"canonical_url": "https://example.com", "structured": {}, "plain_text": "hello"}],
        errors=[],
        summary={},
        exit_code=0,
        argv=["python", "-m", "crawler"],
    )
    item = WorkItem(
        item_id="backend_claim:repeat_crawl:task-1",
        source="backend_claim",
        url="https://example.com",
        dataset_id="dataset-1",
        platform="generic",
        resource_type="page",
        record={"url": "https://example.com", "platform": "generic", "resource_type": "page"},
        claim_task_id="task-1",
        claim_task_type="repeat_crawl",
        metadata={"epoch_id": "epoch-1"},
    )

    summary = run_single_item_for_test(
        item=item,
        client=_FakeClient(),
        runner=_FakeRunner(result),
        root=workspace_tmp_path,
    )

    assert summary["terminal_state"] == "challenge_received_but_unsolved"
    assert (workspace_tmp_path / "run-artifacts" / "preflight" / "challenge.json").exists()


def test_process_task_payload_accepts_local_task_envelope(workspace_tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_CRAWLER_ROOT",
        str(CRAWLER_WORKTREE),
    )
    from agent_runtime import AgentWorker, _build_test_config

    result = CrawlerRunResult(
        output_dir=workspace_tmp_path / "crawl-out",
        records=[{"canonical_url": "https://example.com", "structured": {}, "plain_text": "hello"}],
        errors=[],
        summary={},
        exit_code=0,
        argv=["python", "-m", "crawler"],
    )
    worker = AgentWorker(
        client=_FakeLocalClient(),
        runner=_FakeRunner(result),
        config=_build_test_config(workspace_tmp_path),
    )

    message = worker.process_task_payload(
        "local_crawl",
        {
            "task_id": "local-1",
            "url": "https://example.com",
        },
    )

    assert "processed local_crawl:local-1" in message


def test_run_iteration_surfaces_claim_error_without_crashing(workspace_tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_CRAWLER_ROOT",
        str(CRAWLER_WORKTREE),
    )
    from agent_runtime import AgentWorker, _build_test_config

    result = CrawlerRunResult(
        output_dir=workspace_tmp_path / "crawl-out",
        records=[],
        errors=[],
        summary={},
        exit_code=0,
        argv=["python", "-m", "crawler"],
    )
    worker = AgentWorker(
        client=_UnauthorizedClient(),
        runner=_FakeRunner(result),
        config=_build_test_config(workspace_tmp_path),
    )

    summary = worker.run_iteration(1)

    assert any("claim source failed" in error for error in summary["errors"])


def test_run_once_returns_claim_error_before_no_task_message(workspace_tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_CRAWLER_ROOT",
        str(CRAWLER_WORKTREE),
    )
    from agent_runtime import AgentWorker, _build_test_config

    result = CrawlerRunResult(
        output_dir=workspace_tmp_path / "crawl-out",
        records=[],
        errors=[],
        summary={},
        exit_code=0,
        argv=["python", "-m", "crawler"],
    )
    worker = AgentWorker(
        client=_UnauthorizedClient(),
        runner=_FakeRunner(result),
        config=_build_test_config(workspace_tmp_path),
    )

    message = worker.run_once()

    assert "claim source failed" in message
    assert (workspace_tmp_path / "_run_once" / "last-summary.json").exists()
