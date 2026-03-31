from __future__ import annotations

from crawl_mode_planner import CrawlModePlanner
from run_models import WorkItem


def _item(source: str, command: str | None = None) -> WorkItem:
    return WorkItem(
        item_id=source,
        source=source,
        url="https://example.com",
        dataset_id=None,
        platform="generic",
        resource_type="page",
        record={"platform": "generic", "resource_type": "page", "url": "https://example.com"},
        crawler_command=command,
    )


def test_crawl_mode_planner_prefers_explicit_command() -> None:
    planner = CrawlModePlanner()

    assert planner.choose_command(_item("backend_claim", "crawl")) == "crawl"


def test_crawl_mode_planner_routes_discovery_sources() -> None:
    planner = CrawlModePlanner()

    assert planner.choose_command(_item("dataset_discovery")) == "discover-crawl"


def test_crawl_mode_planner_defaults_to_run() -> None:
    planner = CrawlModePlanner()

    assert planner.choose_command(_item("backend_claim")) == "run"
