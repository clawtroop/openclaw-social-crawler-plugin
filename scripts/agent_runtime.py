from __future__ import annotations

import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from signer import WalletSigner

from auth_orchestrator import AUTH_ERROR_CODES, AuthOrchestrator
from common import inject_crawler_root, resolve_wallet_config
from crawl_mode_planner import CrawlModePlanner
from run_models import CrawlerRunResult, WorkItem, WorkerConfig, WorkerIterationSummary
from task_sources import (
    BackendClaimSource,
    DatasetDiscoverySource,
    ResumeQueueSource,
    build_follow_up_items_from_discovery,
    build_report_payload,
    optional_string,
)
from worker_state import WorkerStateStore

CRAWLER_ROOT = inject_crawler_root()

from crawler.io import read_json_file, read_jsonl_file  # noqa: E402
from crawler.submission_export import build_submission_request  # noqa: E402


class SkipItemError(RuntimeError):
    pass


class PlatformClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        miner_id: str,
        signer: "WalletSigner | None" = None,
    ) -> None:
        self.miner_id = miner_id
        self._signer = signer
        self._max_retries = 3
        headers = {
            "Content-Type": "application/json",
        }
        if token.strip():
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=30.0,
            headers=headers,
        )

    def send_miner_heartbeat(self, *, client_name: str) -> None:
        self._request("POST", "/api/mining/v1/miners/heartbeat", {"client": client_name})

    def claim_repeat_crawl_task(self) -> dict[str, Any] | None:
        return self._claim("/api/mining/v1/repeat-crawl-tasks/claim")

    def claim_refresh_task(self) -> dict[str, Any] | None:
        return self._claim("/api/mining/v1/refresh-tasks/claim")

    def report_repeat_crawl_task_result(self, task_id: str, payload: dict[str, Any]) -> None:
        return self._request("POST", f"/api/mining/v1/repeat-crawl-tasks/{task_id}/report", payload)

    def report_refresh_task_result(self, task_id: str, payload: dict[str, Any]) -> None:
        return self._request("POST", f"/api/mining/v1/refresh-tasks/{task_id}/report", payload)

    def submit_core_submissions(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/api/core/v1/submissions", payload)

    def fetch_core_submission(self, submission_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/api/core/v1/submissions/{submission_id}", None)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError(f"unexpected submission payload for {submission_id}")
        return data

    def fetch_dataset(self, dataset_id: str) -> dict[str, Any]:
        payload = self._request("GET", f"/api/core/v1/datasets/{dataset_id}", None)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ValueError(f"unexpected dataset payload for {dataset_id}")
        return data

    def list_datasets(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/api/core/v1/datasets", None)
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            items = data.get("items")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        return []

    def send_unified_heartbeat(self, *, client_name: str, ip_address: str = "") -> dict[str, Any]:
        return self._request("POST", "/api/mining/v1/heartbeat", {
            "client": client_name,
            "ip_address": ip_address,
        })

    def submit_preflight(self, dataset_id: str, epoch_id: str) -> dict[str, Any]:
        return self._request("POST", "/api/mining/v1/miners/preflight", {
            "dataset_id": dataset_id,
            "epoch_id": epoch_id,
        })

    def answer_pow_challenge(self, challenge_id: str, answer: str) -> dict[str, Any]:
        return self._request("POST", f"/api/mining/v1/pow-challenges/{challenge_id}/answer", {
            "answer": answer,
        })

    def check_url_occupancy(self, dataset_id: str, url: str) -> dict[str, Any]:
        from urllib.parse import quote
        encoded_url = quote(url, safe="")
        resp = self._request(
            "GET",
            f"/api/core/v1/url-occupancies/check?dataset_id={dataset_id}&url={encoded_url}",
            None,
        )
        data = resp.get("data")
        return data if isinstance(data, dict) else {}

    def _claim(self, path: str) -> dict[str, Any] | None:
        try:
            payload = self._request("POST", path, None)
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 404:
                return None
            raise
        data = payload.get("data")
        if data in (None, {}, []):
            return None
        if not isinstance(data, dict):
            raise ValueError(f"unexpected claim response shape for {path}")
        return data

    def _request(self, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if payload is not None:
            kwargs["json"] = payload
        if self._signer is not None:
            kwargs["headers"] = self._signer.build_auth_headers(method, path)
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._client.request(method, path, **kwargs)
                response.raise_for_status()
                if not response.content:
                    return {}
                body = response.json()
                if not isinstance(body, dict):
                    raise ValueError(f"unexpected response payload for {path}")
                return body
            except httpx.HTTPStatusError as error:
                last_error = error
                status_code = error.response.status_code
                if status_code < 500 or attempt >= self._max_retries:
                    raise
                time.sleep(0.5 * attempt)
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"request failed for {method} {path}")


class CrawlerRunner:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.output_root = config.output_root
        self.default_backend = config.default_backend

    def run_item(self, item: WorkItem, command: str) -> CrawlerRunResult:
        output_dir = Path(item.output_dir) if item.output_dir else (self.output_root / item.source / _safe_path_segment(item.item_id))
        output_dir.mkdir(parents=True, exist_ok=True)
        input_path = output_dir / "task-input.jsonl"
        input_path.write_text(json.dumps(item.record, ensure_ascii=False) + "\n", encoding="utf-8")
        argv = [self.config.python_bin, "-m", "crawler", command, "--input", str(input_path), "--output", str(output_dir), "--auto-login"]
        if item.resume:
            argv.append("--resume")
        if command == "discover-crawl":
            argv.extend(["--max-depth", str(self.config.discovery_max_depth), "--max-pages", str(self.config.discovery_max_pages)])
        elif self.default_backend:
            argv.extend(["--backend", self.default_backend])
        completed = subprocess.run(
            argv,
            cwd=self.config.crawler_root,
            capture_output=True,
            text=True,
        )
        records_path = output_dir / "records.jsonl"
        errors_path = output_dir / "errors.jsonl"
        records = read_jsonl_file(records_path) if records_path.exists() else []
        errors = read_jsonl_file(errors_path) if errors_path.exists() else []
        summary_path = output_dir / "summary.json"
        summary = read_json_file(summary_path) if summary_path.exists() else {}
        if not isinstance(summary, dict):
            summary = {}
        return CrawlerRunResult(
            output_dir=output_dir,
            records=records,
            errors=errors,
            summary=summary,
            exit_code=completed.returncode,
            argv=argv,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


def _solve_pow_challenge(challenge: dict[str, Any]) -> str:
    """Solve a PoW challenge.  Returns the answer string.

    Placeholder implementation — returns the prompt directly.
    The actual solving strategy depends on ``question_type`` values
    the backend sends (LLM-answerable questions, hash puzzles, etc.).
    """
    return str(challenge.get("prompt", ""))


class AgentWorker:
    def __init__(self, *, client: PlatformClient, runner: CrawlerRunner, config: WorkerConfig) -> None:
        self.client = client
        self.runner = runner
        self.config = config
        self.state_store = WorkerStateStore(config.state_root)
        self.resume_source = ResumeQueueSource(self.state_store)
        self.backend_source = BackendClaimSource(self.client)
        self.dataset_source = DatasetDiscoverySource(self.client, self.state_store)
        self.crawl_mode_planner = CrawlModePlanner()
        self.auth_orchestrator = AuthOrchestrator(
            self.state_store,
            retry_after_seconds=config.auth_retry_interval_seconds,
        )

    def run_once(self) -> str:
        summary = self.run_iteration(1)
        if summary["messages"]:
            return "; ".join(summary["messages"])
        if summary["auth_pending"]:
            first = summary["auth_pending"][0]
            return json.dumps(first, ensure_ascii=False)
        return "no task available"

    def process_task_payload(self, task_type: str, payload: dict[str, Any]) -> str:
        from task_sources import claimed_task_from_payload, claimed_task_to_work_item

        item = claimed_task_to_work_item(claimed_task_from_payload(task_type, payload, client=self.client))
        summary = WorkerIterationSummary(iteration=1)
        self._process_items([item], summary)
        result = summary.to_dict()
        if result["messages"]:
            return "; ".join(result["messages"])
        if result["errors"]:
            return "; ".join(result["errors"])
        return json.dumps(result, ensure_ascii=False)

    def run_iteration(self, iteration: int) -> dict[str, Any]:
        summary = WorkerIterationSummary(iteration=iteration)
        self._send_heartbeats(summary)
        self._drain_submit_pending(summary)
        work_items = self._collect_work_items(summary)
        if not work_items:
            summary.messages.append("no task available")
            summary.retry_pending = len(self.state_store.load_backlog()) + len(self.state_store.load_auth_pending())
            return summary.to_dict()
        self._process_items(work_items, summary)
        summary.retry_pending = len(self.state_store.load_backlog()) + len(self.state_store.load_auth_pending())
        return summary.to_dict()

    def run_loop(self, *, interval: int = 60, max_iterations: int = 0) -> str:
        """Run continuous mining loop.

        Args:
            interval: Seconds between iterations (default 60)
            max_iterations: Stop after N iterations, 0 = infinite
        """
        iteration = 0
        consecutive_empty = 0
        while max_iterations == 0 or iteration < max_iterations:
            iteration += 1
            try:
                summary = self.run_iteration(iteration)
                result = json.dumps(summary, ensure_ascii=False)
                if not summary["processed_items"] and not summary["discovery_items"] and not summary["claimed_items"] and not summary["resumed_items"]:
                    consecutive_empty += 1
                    wait = min(interval * (2 ** min(consecutive_empty, 3)), 300)
                else:
                    consecutive_empty = 0
                    wait = interval
                print(f"[worker] iteration {iteration}: {result}")
            except KeyboardInterrupt:
                print(f"[worker] stopped after {iteration} iterations")
                return f"stopped after {iteration} iterations"
            except Exception as e:
                print(f"[worker] iteration {iteration} error: {e}")
                wait = min(interval * 2, 120)

            try:
                time.sleep(wait)
            except KeyboardInterrupt:
                print(f"[worker] stopped after {iteration} iterations")
                return f"stopped after {iteration} iterations"

        return f"completed {iteration} iterations"

    def run_worker(self, *, interval: int = 60, max_iterations: int = 1) -> dict[str, Any]:
        iterations: list[dict[str, Any]] = []
        iteration = 0
        while max_iterations == 0 or iteration < max_iterations:
            iteration += 1
            iterations.append(self.run_iteration(iteration))
            if max_iterations == 1:
                break
            time.sleep(interval)
        return {
            "completed_iterations": iteration,
            "iterations": iterations,
            "state": {
                "backlog": len(self.state_store.load_backlog()),
                "auth_pending": self.state_store.load_auth_pending(),
                "submit_pending": len(self.state_store.load_submit_pending()),
            },
        }

    def _send_heartbeats(self, summary: WorkerIterationSummary) -> None:
        try:
            self.client.send_unified_heartbeat(client_name=self.config.client_name)
            summary.unified_heartbeat_sent = True
        except Exception as exc:
            summary.errors.append(f"unified heartbeat failed: {exc}")
        try:
            self.client.send_miner_heartbeat(client_name=self.config.client_name)
            summary.heartbeat_sent = True
        except Exception as exc:
            summary.errors.append(f"miner heartbeat failed: {exc}")

    def _collect_work_items(self, summary: WorkerIterationSummary) -> list[WorkItem]:
        items: list[WorkItem] = []
        resumed = self.resume_source.collect(limit=self.config.max_parallel)
        summary.resumed_items = len(resumed)
        items.extend(resumed)
        claimed = self.backend_source.collect()
        summary.claimed_items = len(claimed)
        items.extend(claimed)
        discoveries = self.dataset_source.collect(min_interval_seconds=self.config.dataset_refresh_seconds)
        summary.discovery_items = len(discoveries)
        items.extend(discoveries)
        merged: dict[str, WorkItem] = {}
        for item in items:
            merged[item.item_id] = item
        return list(merged.values())[: self.config.max_parallel]

    def _process_items(self, items: list[WorkItem], summary: WorkerIterationSummary) -> None:
        with ThreadPoolExecutor(max_workers=max(1, self.config.max_parallel)) as executor:
            futures = {executor.submit(self._run_item, item): item for item in items}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                except SkipItemError as exc:
                    summary.skipped_items += 1
                    summary.messages.append(f"skipped {item.item_id}: {exc}")
                    continue
                except Exception as exc:
                    summary.errors.append(f"{item.item_id}: {exc}")
                    retryable_item = _clone_item(item, resume=True)
                    self.state_store.enqueue_backlog([retryable_item])
                    continue
                self._handle_result(item, result, summary)

    def _run_item(self, item: WorkItem) -> CrawlerRunResult:
        command = self.crawl_mode_planner.choose_command(item)
        self._preflight_item(item, command=command)
        return self.runner.run_item(item, command)

    def _preflight_item(self, item: WorkItem, *, command: str) -> None:
        if item.dataset_id and command != "discover-crawl":
            try:
                occupancy = self.client.check_url_occupancy(item.dataset_id, item.url)
                if occupancy.get("occupied"):
                    raise SkipItemError(f"URL already occupied for {item.url}")
            except SkipItemError:
                raise
            except Exception:
                pass

        epoch_id = optional_string(item.metadata.get("epoch_id"))
        if item.dataset_id and epoch_id:
            try:
                preflight = self.client.submit_preflight(item.dataset_id, epoch_id)
                preflight_data = preflight.get("data", {})
                if isinstance(preflight_data, dict) and not preflight_data.get("allowed", True):
                    reason = preflight_data.get("reason", "unknown")
                    raise SkipItemError(f"preflight rejected: {reason}")
                challenge = preflight_data.get("challenge") if isinstance(preflight_data, dict) else None
                if isinstance(challenge, dict) and challenge:
                    answer = _solve_pow_challenge(challenge)
                    challenge_id = optional_string(challenge.get("id")) or optional_string(preflight_data.get("challenge_id")) or ""
                    if challenge_id and answer:
                        pow_result = self.client.answer_pow_challenge(challenge_id, answer)
                        pow_data = pow_result.get("data", {})
                        if isinstance(pow_data, dict) and not pow_data.get("passed"):
                            raise SkipItemError(f"PoW challenge failed for {item.item_id}")
            except SkipItemError:
                raise
            except Exception:
                pass

    def _handle_result(self, item: WorkItem, result: CrawlerRunResult, summary: WorkerIterationSummary) -> None:
        auth_pending = self.auth_orchestrator.handle_errors(item, result.errors)
        if auth_pending:
            summary.auth_pending.extend(auth_pending)

        retryable_errors = [
            error for error in result.errors
            if bool(error.get("retryable")) and str(error.get("error_code") or "") not in AUTH_ERROR_CODES
        ]
        if retryable_errors:
            self.state_store.enqueue_backlog([_clone_item(item, resume=True, output_dir=result.output_dir)])

        command = self.crawl_mode_planner.choose_command(item)
        if command == "discover-crawl":
            followups = build_follow_up_items_from_discovery(item, result.records)
            if followups:
                self.state_store.enqueue_backlog(followups)
            summary.discovered_followups += len(followups)
            summary.processed_items += 1
            summary.messages.append(f"discovered {len(followups)} follow-up URLs from {item.url}")
            return

        if not result.records:
            for error in result.errors:
                summary.errors.append(f"{item.item_id}: {error.get('error_code') or 'UNKNOWN'}")
            return

        record = result.records[0]
        self.auth_orchestrator.clear_if_recovered(item)
        summary.processed_items += 1

        report_result: dict[str, Any] | None = None
        if item.claim_task_id and item.claim_task_type:
            report_payload = build_report_payload(item, record)
            if item.claim_task_type == "repeat_crawl":
                report_result = self.client.report_repeat_crawl_task_result(item.claim_task_id, report_payload)
            elif item.claim_task_type == "refresh":
                report_result = self.client.report_refresh_task_result(item.claim_task_id, report_payload)

        if item.dataset_id:
            try:
                export_path, _response_path = _export_and_submit_core_submissions_for_task(
                    self.client,
                    result.output_dir,
                    record,
                    item,
                    report_result=report_result,
                )
                summary.submitted_items += 1
                summary.messages.append(f"processed {item.item_id} in {result.output_dir}; exported core submissions to {export_path}")
            except Exception as exc:
                self.state_store.enqueue_submit_pending(item, {"record": record, "report_result": report_result})
                summary.errors.append(f"submit deferred for {item.item_id}: {exc}")
        else:
            summary.messages.append(f"processed {item.item_id} in {result.output_dir}")

    def _drain_submit_pending(self, summary: WorkerIterationSummary) -> None:
        for entry in self.state_store.load_submit_pending():
            item_payload = entry.get("item")
            payload = entry.get("payload")
            if not isinstance(item_payload, dict) or not isinstance(payload, dict):
                continue
            item = WorkItem.from_dict(item_payload)
            record = payload.get("record")
            report_result = payload.get("report_result")
            if not isinstance(record, dict):
                continue
            output_dir = Path(item.output_dir) if item.output_dir else (self.runner.output_root / item.source / _safe_path_segment(item.item_id))
            try:
                _export_and_submit_core_submissions_for_task(
                    self.client,
                    output_dir,
                    record,
                    item,
                    report_result=report_result if isinstance(report_result, dict) else None,
                )
            except Exception:
                continue
            self.state_store.clear_submit_pending(item.item_id)
            summary.submitted_items += 1


def build_worker_from_env() -> AgentWorker:
    from signer import WalletSigner

    output_root = Path(os.environ.get("CRAWLER_OUTPUT_ROOT", str(CRAWLER_ROOT / "output" / "agent-runs"))).resolve()
    python_bin = os.environ.get("PYTHON_BIN") or os.environ.get("PLUGIN_PYTHON_BIN") or "python"
    state_root = Path(os.environ.get("WORKER_STATE_ROOT", str(output_root / "_worker_state"))).resolve()
    config = WorkerConfig(
        base_url=os.environ["PLATFORM_BASE_URL"],
        token=os.environ.get("PLATFORM_TOKEN", ""),
        miner_id=os.environ["MINER_ID"],
        output_root=output_root,
        crawler_root=CRAWLER_ROOT,
        python_bin=python_bin,
        state_root=state_root,
        default_backend=(os.environ.get("DEFAULT_BACKEND") or None),
        max_parallel=max(1, int(os.environ.get("WORKER_MAX_PARALLEL", "3"))),
        dataset_refresh_seconds=max(60, int(os.environ.get("DATASET_REFRESH_SECONDS", "900"))),
        discovery_max_pages=max(1, int(os.environ.get("DISCOVERY_MAX_PAGES", "25"))),
        discovery_max_depth=max(0, int(os.environ.get("DISCOVERY_MAX_DEPTH", "1"))),
        auth_retry_interval_seconds=max(30, int(os.environ.get("AUTH_RETRY_INTERVAL_SECONDS", "300"))),
    )

    wallet_bin, wallet_token = resolve_wallet_config()
    signer: WalletSigner | None = None
    if wallet_token.strip():
        signer = WalletSigner(wallet_bin=wallet_bin, session_token=wallet_token)

    client = PlatformClient(
        base_url=config.base_url,
        token=config.token,
        miner_id=config.miner_id,
        signer=signer,
    )
    runner = CrawlerRunner(config)
    return AgentWorker(client=client, runner=runner, config=config)


def export_core_submissions(input_path: str, output_path: str, dataset_id: str) -> Path:
    input_file = Path(input_path)
    records = read_jsonl_file(input_file)
    generated_at = None
    manifest_path = input_file.parent / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json_file(manifest_path)
        if isinstance(manifest, dict):
            generated_at = optional_string(manifest.get("generated_at"))
    payload = build_submission_request(records, dataset_id=dataset_id, generated_at=generated_at)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def _export_core_submissions_for_task(output_dir: Path, record: dict[str, Any], item: WorkItem) -> Path:
    dataset_id = optional_string(item.dataset_id)
    if not dataset_id:
        raise RuntimeError(f"item {item.item_id} is missing dataset_id for core submission export")
    export_path = output_dir / "core-submissions.json"
    generated_at = None
    manifest_path = output_dir / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json_file(manifest_path)
        if isinstance(manifest, dict):
            generated_at = optional_string(manifest.get("generated_at"))
    payload = build_submission_request([record], dataset_id=dataset_id, generated_at=generated_at)
    export_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return export_path


def _export_and_submit_core_submissions_for_task(
    client: PlatformClient,
    output_dir: Path,
    record: dict[str, Any],
    item: WorkItem,
    *,
    report_result: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    export_path = _export_core_submissions_for_task(output_dir, record, item)
    payload = read_json_file(export_path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid core submission export payload at {export_path}")
    dataset_id = optional_string(item.dataset_id)
    fetch_dataset = getattr(client, "fetch_dataset", None)
    if dataset_id and callable(fetch_dataset):
        dataset = fetch_dataset(dataset_id)
        _augment_submission_payload_for_dataset(payload, dataset=dataset, record=record, item=item)
        export_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    response_path = output_dir / "core-submissions-response.json"
    submission_id = _extract_submission_id(report_result)
    if submission_id:
        response_data = _resolve_existing_submission_response(client, submission_id=submission_id, report_result=report_result)
        response_path.write_text(json.dumps(response_data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return export_path, response_path
    response = client.submit_core_submissions(payload)
    response_path.write_text(json.dumps(response, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return export_path, response_path


def _augment_submission_payload_for_dataset(
    payload: dict[str, Any],
    *,
    dataset: dict[str, Any],
    record: dict[str, Any],
    item: WorkItem,
) -> None:
    schema = dataset.get("schema")
    entries = payload.get("entries")
    if not isinstance(schema, dict) or not isinstance(entries, list):
        return
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        original_structured_data = entry.get("structured_data")
        if not isinstance(original_structured_data, dict):
            original_structured_data = {}
        structured_data: dict[str, Any] = {}
        for field_name, spec in schema.items():
            if field_name in original_structured_data and original_structured_data[field_name] not in (None, ""):
                structured_data[field_name] = original_structured_data[field_name]
                continue
            if isinstance(spec, dict) and not bool(spec.get("required")) and field_name not in original_structured_data:
                continue
            value = _resolve_schema_field_value(
                field_name,
                entry=entry,
                record=record,
                item=item,
                structured_data=original_structured_data,
            )
            if value not in (None, ""):
                structured_data[field_name] = value
        entry["structured_data"] = structured_data


def _resolve_schema_field_value(
    field_name: str,
    *,
    entry: dict[str, Any],
    record: dict[str, Any],
    item: WorkItem,
    structured_data: dict[str, Any],
) -> Any:
    record_metadata = record.get("metadata")
    metadata = record_metadata if isinstance(record_metadata, dict) else {}
    cleaned_data = entry.get("cleaned_data") or record.get("plain_text") or record.get("cleaned_data") or record.get("markdown")
    candidate_values = {
        "url": entry.get("url") or record.get("canonical_url") or record.get("url") or item.url,
        "title": structured_data.get("title") or record.get("title") or metadata.get("title") or metadata.get("page_title"),
        "content": structured_data.get("content") or cleaned_data,
        "cleaned_data": cleaned_data,
        "canonical_url": record.get("canonical_url") or item.url,
    }
    if field_name in candidate_values:
        return candidate_values[field_name]
    if field_name in structured_data:
        return structured_data[field_name]
    if field_name in metadata:
        return metadata[field_name]
    return record.get(field_name)


def _extract_submission_id(report_result: dict[str, Any] | None) -> str | None:
    if not isinstance(report_result, dict):
        return None
    data = report_result.get("data")
    if isinstance(data, dict):
        return optional_string(data.get("submission_id"))
    return optional_string(report_result.get("submission_id"))


def _resolve_existing_submission_response(
    client: PlatformClient,
    *,
    submission_id: str,
    report_result: dict[str, Any] | None,
) -> dict[str, Any]:
    fetch_core_submission = getattr(client, "fetch_core_submission", None)
    if callable(fetch_core_submission):
        try:
            submission = fetch_core_submission(submission_id)
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 404:
                raise
        else:
            return {"data": [submission]}
    if isinstance(report_result, dict):
        return report_result
    return {"data": [{"id": submission_id}]}


def _safe_path_segment(value: str) -> str:
    slug = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)
    return slug or "item"


def _clone_item(item: WorkItem, *, resume: bool, output_dir: Path | None = None) -> WorkItem:
    payload = item.to_dict()
    payload["resume"] = resume
    if output_dir is not None:
        payload["output_dir"] = str(output_dir)
    return WorkItem.from_dict(payload)
