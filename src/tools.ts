import path from "node:path";
import { spawn } from "node:child_process";
import { Type } from "@sinclair/typebox";
import type { OpenClawPluginApi } from "../api.js";

type PluginConfig = {
  crawlerRoot?: string;
  pythonBin?: string;
  platformBaseUrl?: string;
  platformToken?: string;
  minerId?: string;
  outputRoot?: string;
  defaultBackend?: string;
};

function resolvePluginConfig(api: OpenClawPluginApi): Required<Pick<PluginConfig, "crawlerRoot" | "platformBaseUrl" | "minerId">> & PluginConfig {
  const cfg = (api.pluginConfig ?? {}) as PluginConfig;
  if (!cfg.crawlerRoot?.trim()) {
    throw new Error("plugins.entries.social-crawler-agent.config.crawlerRoot is required");
  }
  if (!cfg.platformBaseUrl?.trim()) {
    throw new Error("plugins.entries.social-crawler-agent.config.platformBaseUrl is required");
  }
  if (!cfg.minerId?.trim()) {
    throw new Error("plugins.entries.social-crawler-agent.config.minerId is required");
  }
  return {
    ...cfg,
    crawlerRoot: cfg.crawlerRoot,
    platformBaseUrl: cfg.platformBaseUrl,
    minerId: cfg.minerId,
  };
}

async function runPythonTool(
  api: OpenClawPluginApi,
  command: string,
  extraArgs: string[] = [],
): Promise<string> {
  const cfg = resolvePluginConfig(api);
  const pythonBin = cfg.pythonBin?.trim() || "python";
  const scriptPath = path.join(api.rootDir ?? ".", "scripts", "run_tool.py");
  const env = {
    ...process.env,
    SOCIAL_CRAWLER_ROOT: cfg.crawlerRoot,
    PLATFORM_BASE_URL: cfg.platformBaseUrl,
    PLATFORM_TOKEN: cfg.platformToken ?? "",
    MINER_ID: cfg.minerId,
    CRAWLER_OUTPUT_ROOT: cfg.outputRoot ?? path.join(cfg.crawlerRoot, "output", "agent-runs"),
    DEFAULT_BACKEND: cfg.defaultBackend ?? "",
  };

  return await new Promise((resolve, reject) => {
    const child = spawn(pythonBin, [scriptPath, command, ...extraArgs], {
      cwd: api.rootDir,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", (error) => reject(error));
    child.on("close", (code) => {
      if (code === 0) {
        resolve(stdout.trim() || "ok");
        return;
      }
      reject(new Error(stderr.trim() || `python helper exited with code ${code}`));
    });
  });
}

export function createHeartbeatTool(api: OpenClawPluginApi) {
  return {
    name: "social_crawler_heartbeat",
    label: "Social Crawler Heartbeat",
    description: "Send one miner heartbeat to Platform Service using the configured crawler worker identity.",
    parameters: Type.Object({}),
    async execute() {
      const text = await runPythonTool(api, "heartbeat");
      return { content: [{ type: "text", text }] };
    },
  };
}

export function createRunOnceTool(api: OpenClawPluginApi) {
  return {
    name: "social_crawler_run_once",
    label: "Social Crawler Run Once",
    description: "Send heartbeat, claim one repeat-crawl or refresh task, execute social-data-crawler, and report the result.",
    parameters: Type.Object({}),
    async execute() {
      const text = await runPythonTool(api, "run-once");
      return { content: [{ type: "text", text }] };
    },
  };
}

export function createProcessTaskFileTool(api: OpenClawPluginApi) {
  return {
    name: "social_crawler_process_task_file",
    label: "Process Task File",
    description:
      "Process one refresh or repeat-crawl task payload JSON through crawl, report, and core submission export/submit. Useful when remote claim is unavailable.",
    parameters: Type.Object({
      taskType: Type.Union(
        [Type.Literal("refresh"), Type.Literal("repeat_crawl")],
        { description: "Platform task type." },
      ),
      taskPath: Type.String({ description: "Absolute or plugin-relative path to a task payload JSON file." }),
    }),
    async execute(_id: string, params: Record<string, unknown>) {
      const taskType = typeof params.taskType === "string" ? params.taskType : "";
      const taskPath = typeof params.taskPath === "string" ? params.taskPath : "";
      if (!taskType.trim() || !taskPath.trim()) {
        throw new Error("taskType and taskPath are required");
      }
      const text = await runPythonTool(api, "process-task-file", [taskType, taskPath]);
      return { content: [{ type: "text", text }] };
    },
  };
}

export function createExportCoreSubmissionsTool(api: OpenClawPluginApi) {
  return {
    name: "social_crawler_export_core_submissions",
    label: "Export Core Submissions",
    description: "Convert crawler records.jsonl into Platform Service Core submission payload JSON.",
    parameters: Type.Object({
      inputPath: Type.String({ description: "Absolute or plugin-relative path to records.jsonl." }),
      outputPath: Type.String({ description: "Absolute or plugin-relative path for the exported JSON." }),
      datasetId: Type.String({ description: "Platform Service dataset id." }),
    }),
    async execute(_id: string, params: Record<string, unknown>) {
      const inputPath = typeof params.inputPath === "string" ? params.inputPath : "";
      const outputPath = typeof params.outputPath === "string" ? params.outputPath : "";
      const datasetId = typeof params.datasetId === "string" ? params.datasetId : "";
      if (!inputPath.trim() || !outputPath.trim() || !datasetId.trim()) {
        throw new Error("inputPath, outputPath, and datasetId are required");
      }
      const text = await runPythonTool(api, "export-core-submissions", [inputPath, outputPath, datasetId]);
      return { content: [{ type: "text", text }] };
    },
  };
}
