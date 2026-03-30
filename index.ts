import { definePluginEntry, type AnyAgentTool, type OpenClawPluginApi } from "./api.js";
import {
  createRunOnceTool,
  createHeartbeatTool,
  createExportCoreSubmissionsTool,
  createProcessTaskFileTool,
} from "./src/tools.js";

export default definePluginEntry({
  id: "social-crawler-agent",
  name: "Social Crawler Agent",
  description: "Runs social-data-crawler jobs from OpenClaw tools and worker triggers.",
  register(api: OpenClawPluginApi) {
    api.registerTool(createHeartbeatTool(api) as unknown as AnyAgentTool, { optional: true });
    api.registerTool(createRunOnceTool(api) as unknown as AnyAgentTool, { optional: true });
    api.registerTool(createProcessTaskFileTool(api) as unknown as AnyAgentTool, { optional: true });
    api.registerTool(createExportCoreSubmissionsTool(api) as unknown as AnyAgentTool, {
      optional: true,
    });
  },
});
