# OpenClaw Social Crawler Agent Plugin

This is the OpenClaw plugin layer for the `social-data-crawler` skill and Python crawler project.

## Separation of concerns

- `social-data-crawler`
  skill + crawler engine
- `openclaw-social-crawler-plugin`
  OpenClaw-native plugin that registers tools and calls the crawler project

## Files

- `openclaw.plugin.json`
  native OpenClaw manifest
- `package.json`
  package metadata plus plugin entrypoint declaration
- `index.ts`
  OpenClaw registration entry
- `src/tools.ts`
  tool registration and Python bridge launcher
- `scripts/run_tool.py`
  Python bridge entry
- `scripts/agent_runtime.py`
  worker runtime that talks to Platform Service and the crawler CLI

## Required plugin config

- `crawlerRoot`
- `platformBaseUrl`
- `minerId`

Optional:

- `platformToken`
- `pythonBin`
- `outputRoot`
- `defaultBackend`

## Registered tools

- `social_crawler_heartbeat`
- `social_crawler_run_once`
- `social_crawler_process_task_file`
- `social_crawler_export_core_submissions`

## Python runtime

The plugin shells out to `scripts/run_tool.py`, which loads the `social-data-crawler`
project from `crawlerRoot` and reuses its existing CLI / submission export code.

`run-once` now performs the full local integration chain:

- send mining heartbeat
- claim one repeat-crawl or refresh task
- run `social-data-crawler`
- report cleaned data back to Mining API
- export Core submission payload to `core-submissions.json`
- if `report` already returns `submission_id`, treat that as the authoritative Core creation path and persist the lookup/result to `core-submissions-response.json`
- otherwise submit the exported payload to `/api/core/v1/submissions`

Before exporting/submitting, the plugin now normalizes `structured_data` against the target dataset schema:

- fill required fields such as `title`, `content`, and `url` from crawler output when possible
- drop schema-external fields that the Core API currently rejects

If the remote `claim` endpoint is currently unavailable but you already have a task payload
from task creation or another control plane, use `social_crawler_process_task_file` or
`python scripts/run_tool.py process-task-file <taskType> <taskJsonPath>` to run the same
pipeline without waiting on claim. The task file reader accepts UTF-8 with BOM as well as plain UTF-8 JSON.

## OpenClaw config example

See [`openclaw.config.example.jsonc`](./openclaw.config.example.jsonc) for a minimal local plugin entry.

The important point is that OpenClaw config points at this plugin directory, while the plugin config points at the separate `social-data-crawler` project through `crawlerRoot`.

## Local verification

```bash
python -m pytest tests/test_agent_runtime.py tests/test_run_tool.py -v
python scripts/run_tool.py --help
```
