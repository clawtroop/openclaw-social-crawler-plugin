from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class TaskEnvelope:
    task_id: str
    task_source: str
    task_type: str
    url: str
    dataset_id: str | None
    platform: str
    resource_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WorkItem:
    item_id: str
    source: str
    url: str
    dataset_id: str | None
    platform: str
    resource_type: str
    record: dict[str, Any]
    crawler_command: str | None = None
    claim_task_id: str | None = None
    claim_task_type: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    resume: bool = False
    output_dir: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "source": self.source,
            "url": self.url,
            "dataset_id": self.dataset_id,
            "platform": self.platform,
            "resource_type": self.resource_type,
            "record": dict(self.record),
            "crawler_command": self.crawler_command,
            "claim_task_id": self.claim_task_id,
            "claim_task_type": self.claim_task_type,
            "metadata": dict(self.metadata),
            "resume": self.resume,
            "output_dir": self.output_dir,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "WorkItem":
        return cls(
            item_id=str(payload.get("item_id") or ""),
            source=str(payload.get("source") or ""),
            url=str(payload.get("url") or ""),
            dataset_id=_optional_string(payload.get("dataset_id")),
            platform=str(payload.get("platform") or "generic"),
            resource_type=str(payload.get("resource_type") or "page"),
            record=dict(payload.get("record") or {}),
            crawler_command=_optional_string(payload.get("crawler_command")),
            claim_task_id=_optional_string(payload.get("claim_task_id")),
            claim_task_type=_optional_string(payload.get("claim_task_type")),
            metadata=dict(payload.get("metadata") or {}),
            resume=bool(payload.get("resume", False)),
            output_dir=_optional_string(payload.get("output_dir")),
        )


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    base_url: str
    token: str
    miner_id: str
    output_root: Path
    crawler_root: Path
    python_bin: str
    state_root: Path
    default_backend: str | None = None
    client_name: str = "social-crawler-agent/0.2"
    max_parallel: int = 3
    dataset_refresh_seconds: int = 900
    discovery_max_pages: int = 25
    discovery_max_depth: int = 1
    auth_retry_interval_seconds: int = 300
    openclaw_enrich_enabled: bool = False
    openclaw_model_config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CrawlerRunResult:
    output_dir: Path
    records: list[dict[str, Any]]
    errors: list[dict[str, Any]]
    summary: dict[str, Any]
    exit_code: int
    argv: list[str]
    stdout: str = ""
    stderr: str = ""


@dataclass(slots=True)
class WorkerIterationSummary:
    iteration: int
    heartbeat_sent: bool = False
    unified_heartbeat_sent: bool = False
    claimed_items: int = 0
    discovery_items: int = 0
    resumed_items: int = 0
    processed_items: int = 0
    submitted_items: int = 0
    discovered_followups: int = 0
    skipped_items: int = 0
    retry_pending: int = 0
    auth_pending: list[dict[str, Any]] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "iteration": self.iteration,
            "heartbeat_sent": self.heartbeat_sent,
            "unified_heartbeat_sent": self.unified_heartbeat_sent,
            "claimed_items": self.claimed_items,
            "discovery_items": self.discovery_items,
            "resumed_items": self.resumed_items,
            "processed_items": self.processed_items,
            "submitted_items": self.submitted_items,
            "discovered_followups": self.discovered_followups,
            "skipped_items": self.skipped_items,
            "retry_pending": self.retry_pending,
            "auth_pending": list(self.auth_pending),
            "messages": list(self.messages),
            "errors": list(self.errors),
        }


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
