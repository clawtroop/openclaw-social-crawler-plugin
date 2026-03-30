from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx

from common import inject_crawler_root

CRAWLER_ROOT = inject_crawler_root()

from crawler.cli import main as crawler_main  # noqa: E402
from crawler.io import read_json_file, read_jsonl_file  # noqa: E402
from crawler.submission_export import build_submission_request  # noqa: E402


@dataclass(frozen=True, slots=True)
class ClaimedTask:
    task_id: str
    task_type: str
    url: str
    dataset_id: str | None
    platform: str
    resource_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


def claimed_task_from_payload(
    task_type: str,
    payload: dict[str, Any],
    *,
    client: "PlatformClient | None" = None,
) -> ClaimedTask:
    enriched_payload = _enrich_task_payload(task_type, payload, client=client)
    task_id = str(payload.get("id") or "").strip()
    if not task_id:
        raise ValueError("task payload is missing id")
    url = str(enriched_payload.get("url") or enriched_payload.get("target_url") or "").strip()
    if not url:
        raise ValueError(f"task {task_id} is missing url")
    platform, resource_type, discovered_fields = _infer_platform_task(url)
    metadata = dict(enriched_payload)
    metadata.pop("id", None)
    metadata.pop("url", None)
    metadata.pop("target_url", None)
    return ClaimedTask(
        task_id=task_id,
        task_type=task_type,
        url=url,
        dataset_id=_optional_string(enriched_payload.get("dataset_id")),
        platform=_optional_string(enriched_payload.get("platform")) or platform,
        resource_type=_optional_string(enriched_payload.get("resource_type")) or resource_type,
        metadata=metadata,
    )


def claimed_task_to_crawl_record(task: ClaimedTask) -> dict[str, Any]:
    record: dict[str, Any] = _build_platform_record(task)
    for key, value in task.metadata.items():
        if key not in {"dataset_id", "platform", "resource_type"} and value not in (None, ""):
            record[key] = value
    return record


def build_report_payload(task: ClaimedTask, record: dict[str, Any]) -> dict[str, Any]:
    cleaned_data = record.get("plain_text")
    if cleaned_data in (None, ""):
        cleaned_data = record.get("cleaned_data")
    if cleaned_data in (None, ""):
        cleaned_data = record.get("markdown")
    return {
        "cleaned_data": "" if cleaned_data is None else str(cleaned_data),
        "canonical_url": record.get("canonical_url") or record.get("url") or task.url,
        "structured_data": record.get("structured") if isinstance(record.get("structured"), dict) else {},
        "crawl_timestamp": _optional_string(record.get("crawl_timestamp")),
    }


class PlatformClient:
    def __init__(self, *, base_url: str, token: str, miner_id: str) -> None:
        self.miner_id = miner_id
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
        kwargs = {}
        if payload is not None:
            kwargs["json"] = payload
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


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    base_url: str
    token: str
    miner_id: str
    output_root: Path
    default_backend: str | None = None
    client_name: str = "social-crawler-agent/0.1"


class CrawlerRunner:
    def __init__(self, output_root: Path, *, default_backend: str | None = None) -> None:
        self.output_root = output_root
        self.default_backend = default_backend

    def run_task(self, task: ClaimedTask) -> tuple[Path, dict[str, Any], dict[str, Any]]:
        output_dir = self.output_root / task.task_type / task.task_id
        output_dir.mkdir(parents=True, exist_ok=True)
        input_path = output_dir / "task-input.jsonl"
        input_path.write_text(json.dumps(claimed_task_to_crawl_record(task), ensure_ascii=False) + "\n", encoding="utf-8")
        argv = ["crawl", "--input", str(input_path), "--output", str(output_dir)]
        if self.default_backend:
            argv.extend(["--backend", self.default_backend])
        exit_code = crawler_main(argv)
        if exit_code != 0:
            raise RuntimeError(f"crawler exited with code {exit_code} for task {task.task_id}")
        records = read_jsonl_file(output_dir / "records.jsonl")
        if not records:
            raise RuntimeError(f"crawler produced no records for task {task.task_id}")
        summary_path = output_dir / "summary.json"
        summary = read_json_file(summary_path) if summary_path.exists() else {}
        if not isinstance(summary, dict):
            summary = {}
        return output_dir, records[0], summary


class AgentWorker:
    def __init__(self, *, client: PlatformClient, runner: CrawlerRunner, config: WorkerConfig) -> None:
        self.client = client
        self.runner = runner
        self.config = config

    def run_once(self) -> str:
        self.client.send_miner_heartbeat(client_name=self.config.client_name)

        repeat_payload = self.client.claim_repeat_crawl_task()
        if repeat_payload:
            return self.process_task_payload("repeat_crawl", repeat_payload)

        refresh_payload = self.client.claim_refresh_task()
        if refresh_payload:
            return self.process_task_payload("refresh", refresh_payload)

        return "no task available"

    def process_task_payload(self, task_type: str, payload: dict[str, Any]) -> str:
        task = claimed_task_from_payload(task_type, payload, client=self.client)
        output_dir, record, _summary = self.runner.run_task(task)
        report_payload = build_report_payload(task, record)
        if task.task_type == "repeat_crawl":
            report_result = self.client.report_repeat_crawl_task_result(task.task_id, report_payload)
        elif task.task_type == "refresh":
            report_result = self.client.report_refresh_task_result(task.task_id, report_payload)
        else:
            raise ValueError(f"unsupported task type {task.task_type}")
        export_path, _response_path = _export_and_submit_core_submissions_for_task(
            self.client,
            output_dir,
            record,
            task,
            report_result=report_result if isinstance(report_result, dict) else None,
        )
        return f"processed {task.task_type} task {task.task_id} in {output_dir}; exported core submissions to {export_path}"


def build_worker_from_env() -> AgentWorker:
    output_root = Path(os.environ.get("CRAWLER_OUTPUT_ROOT", str(CRAWLER_ROOT / "output" / "agent-runs"))).resolve()
    config = WorkerConfig(
        base_url=os.environ["PLATFORM_BASE_URL"],
        token=os.environ.get("PLATFORM_TOKEN", ""),
        miner_id=os.environ["MINER_ID"],
        output_root=output_root,
        default_backend=(os.environ.get("DEFAULT_BACKEND") or None),
    )
    client = PlatformClient(base_url=config.base_url, token=config.token, miner_id=config.miner_id)
    runner = CrawlerRunner(output_root=config.output_root, default_backend=config.default_backend)
    return AgentWorker(client=client, runner=runner, config=config)


def export_core_submissions(input_path: str, output_path: str, dataset_id: str) -> Path:
    input_file = Path(input_path)
    records = read_jsonl_file(input_file)
    generated_at = None
    manifest_path = input_file.parent / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json_file(manifest_path)
        if isinstance(manifest, dict):
            generated_at = _optional_string(manifest.get("generated_at"))
    payload = build_submission_request(records, dataset_id=dataset_id, generated_at=generated_at)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _enrich_task_payload(
    task_type: str,
    payload: dict[str, Any],
    *,
    client: PlatformClient | None,
) -> dict[str, Any]:
    enriched = dict(payload)
    if enriched.get("url") or enriched.get("target_url"):
        return enriched
    submission_id = _optional_string(enriched.get("submission_id"))
    if task_type == "repeat_crawl" and submission_id and client is not None:
        submission = client.fetch_core_submission(submission_id)
        enriched.setdefault("dataset_id", submission.get("dataset_id"))
        enriched.setdefault("url", submission.get("original_url") or submission.get("normalized_url"))
    return enriched


def _infer_platform_task(url: str) -> tuple[str, str, dict[str, str]]:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path

    if host.endswith("en.wikipedia.org") and path.startswith("/wiki/"):
        title = unquote(path.split("/wiki/", 1)[1]).replace("_", " ")
        return "wikipedia", "article", {"title": title}

    if host.endswith("arxiv.org") and path.startswith("/abs/"):
        arxiv_id = path.split("/abs/", 1)[1].strip("/")
        return "arxiv", "paper", {"arxiv_id": arxiv_id}

    if host.endswith("www.linkedin.com"):
        linkedin_patterns = (
            (r"^/in/([^/]+)/?$", "profile", "public_identifier"),
            (r"^/company/([^/]+)/?$", "company", "company_slug"),
            (r"^/jobs/view/(\d+)/?$", "job", "job_id"),
            (r"^/feed/update/([^/]+)/?$", "post", "activity_urn"),
        )
        for pattern, resource_type, field_name in linkedin_patterns:
            match = re.match(pattern, path)
            if match:
                return "linkedin", resource_type, {field_name: match.group(1)}

    if host.endswith("www.amazon.com"):
        dp_match = re.search(r"/dp/([A-Z0-9]{10})(?:/|$)", path)
        if dp_match:
            return "amazon", "product", {"asin": dp_match.group(1)}

    if host.endswith("basescan.org") or host.endswith("base.org"):
        for prefix, resource_type, field_name in (
            ("/address/", "address", "address"),
            ("/tx/", "transaction", "tx_hash"),
            ("/token/", "token", "contract_address"),
        ):
            if path.startswith(prefix):
                return "base", resource_type, {field_name: path.split(prefix, 1)[1].strip("/")}

    return "generic", "page", {"url": url}


def _build_platform_record(task: ClaimedTask) -> dict[str, Any]:
    platform, resource_type, discovered_fields = _infer_platform_task(task.url)
    platform = task.platform or platform
    resource_type = task.resource_type or resource_type
    record: dict[str, Any] = {
        "platform": platform,
        "resource_type": resource_type,
    }
    if platform == "generic":
        record["url"] = task.url
    else:
        record.update(discovered_fields)
    return record


def _export_core_submissions_for_task(output_dir: Path, record: dict[str, Any], task: ClaimedTask) -> Path:
    dataset_id = _optional_string(task.dataset_id)
    if not dataset_id:
        raise RuntimeError(f"task {task.task_id} is missing dataset_id for core submission export")
    export_path = output_dir / "core-submissions.json"
    generated_at = None
    manifest_path = output_dir / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json_file(manifest_path)
        if isinstance(manifest, dict):
            generated_at = _optional_string(manifest.get("generated_at"))
    payload = build_submission_request([record], dataset_id=dataset_id, generated_at=generated_at)
    export_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return export_path


def _export_and_submit_core_submissions_for_task(
    client: PlatformClient,
    output_dir: Path,
    record: dict[str, Any],
    task: ClaimedTask,
    *,
    report_result: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    export_path = _export_core_submissions_for_task(output_dir, record, task)
    payload = read_json_file(export_path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid core submission export payload at {export_path}")
    dataset_id = _optional_string(task.dataset_id)
    fetch_dataset = getattr(client, "fetch_dataset", None)
    if dataset_id and callable(fetch_dataset):
        dataset = fetch_dataset(dataset_id)
        _augment_submission_payload_for_dataset(payload, dataset=dataset, record=record, task=task)
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
    task: ClaimedTask,
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
                task=task,
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
    task: ClaimedTask,
    structured_data: dict[str, Any],
) -> Any:
    record_metadata = record.get("metadata")
    metadata = record_metadata if isinstance(record_metadata, dict) else {}
    cleaned_data = entry.get("cleaned_data") or record.get("plain_text") or record.get("cleaned_data") or record.get("markdown")
    candidate_values = {
        "url": entry.get("url") or record.get("canonical_url") or record.get("url") or task.url,
        "title": structured_data.get("title") or record.get("title") or metadata.get("title") or metadata.get("page_title"),
        "content": structured_data.get("content") or cleaned_data,
        "cleaned_data": cleaned_data,
        "canonical_url": record.get("canonical_url") or task.url,
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
        return _optional_string(data.get("submission_id"))
    return _optional_string(report_result.get("submission_id"))


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
