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

- `social_crawler_worker`
- `social_crawler_heartbeat`
- `social_crawler_run_once`
- `social_crawler_run_loop`
- `social_crawler_process_task_file`
- `social_crawler_export_core_submissions`

## Python runtime

The plugin shells out to `scripts/run_tool.py`, which loads the `social-data-crawler`
project from `crawlerRoot` and reuses its existing CLI / submission export code.

When the local OpenClaw Gateway is already running, the plugin now auto-detects Gateway auth and injects a temporary crawler `--model-config` for `run` / `enrich` execution:

- no manual provider key entry in the enrich flow
- prefers `OPENCLAW_GATEWAY_TOKEN` when present
- otherwise reads `~/.openclaw/openclaw.json` or `OPENCLAW_CONFIG_PATH`
- supports OpenClaw `SecretRef` token sources: `env`, `file`, and `exec`

This keeps OpenClaw-specific enrich wiring in the plugin layer instead of making `social-data-crawler` auto-switch providers by default.

`social_crawler_worker` is now the primary entrypoint. It drives a single Worker state machine that can:

- send unified + miner heartbeats
- resume backlog / auth-pending / submit-pending work
- claim repeat-crawl and refresh tasks
- discover autonomous dataset seeds from active datasets
- choose `discover-crawl`, `run`, or `crawl`
- pass `--auto-login` through to `social-data-crawler`
- keep other items running while auth-required items move into pending/retry state
- export and submit Core payloads

`run-once` remains as a compatibility/debug entry and still performs the local integration chain:

- send mining heartbeat
- claim one repeat-crawl or refresh task
- run `social-data-crawler`
- report cleaned data back to Mining API
- export Core submission payload to `core-submissions.json`
- if `report` already returns `submission_id`, treat that as the authoritative Core creation path and persist the lookup/result to `core-submissions-response.json`
- otherwise submit the exported payload to `/api/core/v1/submissions`

`run-loop` and `social_crawler_worker` build on top of the same worker pipeline and keep repeating:

- heartbeat
- claim one task if available
- crawl / report / submit
- sleep for the configured interval
- continue until interrupted or `maxIterations` is reached

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

Additional worker-oriented config:

- `workerStateRoot`
- `workerMaxParallel`
- `datasetRefreshSeconds`
- `discoveryMaxPages`
- `discoveryMaxDepth`
- `authRetryIntervalSeconds`

Optional process env overrides for local Gateway enrich:

- `OPENCLAW_ENRICH_MODE=off` to disable plugin-side auto injection
- `OPENCLAW_GATEWAY_BASE_URL` to override `http://127.0.0.1:18789/v1`
- `OPENCLAW_ENRICH_MODEL` to override `openclaw/default`
- `OPENCLAW_UPSTREAM_MODEL` to send `x-openclaw-model` for upstream model selection

## Local verification

```bash
python scripts/run_tool.py --help
python scripts/run_tool.py run-worker 60 1
python scripts/run_tool.py run-loop 60 1
```
