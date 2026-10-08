import type {
  AdapterExecutionContext,
  AdapterExecutionResult,
  AdapterEnvironmentTestResult,
} from "@paperclipai/adapter-utils";
import { readAdapterExecutionTarget } from "@paperclipai/adapter-utils/execution-target";
import { parseObject } from "@paperclipai/adapter-utils/server-utils";

export const CLAWROUTER_CONFIGURATION_ERROR = "claude_clawrouter_configuration_invalid";

type RouteResult =
  | { config: Record<string, unknown>; error?: never }
  | { config?: never; error: string };

/** Resolve only an explicit provider selection, using trusted server configuration. */
export function resolveClaudeClawRouterRoute(
  config: Record<string, unknown>,
  remote = false,
  serverEnv: NodeJS.ProcessEnv = process.env,
): RouteResult {
  const model = config.model;
  if (typeof model !== "string" || !model.startsWith("clawrouter/")) return { config };
  if (remote) return { error: "ClawRouter Claude routing requires a local execution target." };
  const nativeModel = model.slice("clawrouter/".length).trim();
  if (!nativeModel) return { error: "ClawRouter Claude routing requires a model name." };
  const env = parseObject(config.env);
  const effectiveEnv = { ...serverEnv, ...env };
  const providerMode = ["CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY", "CLAUDE_CODE_USE_MANTLE", "CLAUDE_CODE_USE_ANTHROPIC_AWS"]
    .some((name) => effectiveEnv[name] === "1" || effectiveEnv[name] === "true");
  if (providerMode || effectiveEnv.ANTHROPIC_BEDROCK_BASE_URL || effectiveEnv.CLAUDE_CODE_OAUTH_TOKEN) {
    return { error: "ClawRouter Claude routing cannot be combined with another provider mode or subscription OAuth token." };
  }
  const baseUrl = serverEnv.PAPERCLIP_CLAWROUTER_BASE_URL?.trim();
  const key = serverEnv.CLAWROUTER_PROXY_KEY;
  if (!baseUrl || !key?.trim()) {
    return { error: "ClawRouter Claude routing requires server PAPERCLIP_CLAWROUTER_BASE_URL and CLAWROUTER_PROXY_KEY." };
  }
  try {
    const url = new URL(baseUrl);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
      throw new Error();
    }
  } catch {
    return { error: "Server PAPERCLIP_CLAWROUTER_BASE_URL must be an HTTP(S) URL without credentials, query, or fragment." };
  }
  return {
    config: {
      ...config,
      model: nativeModel,
      env: {
        ...env,
        ANTHROPIC_BASE_URL: baseUrl,
        ANTHROPIC_AUTH_TOKEN: key,
        ANTHROPIC_API_KEY: "",
        CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST: "1",
        CLAUDE_CODE_SUBPROCESS_ENV_SCRUB: "1",
      },
    },
  };
}

export function resolveClaudeExecutionRoute(ctx: AdapterExecutionContext): RouteResult {
  const target = readAdapterExecutionTarget({
    executionTarget: ctx.executionTarget,
    legacyRemoteExecution: ctx.executionTransport?.remoteExecution,
  });
  return resolveClaudeClawRouterRoute(ctx.config, target?.kind === "remote");
}

export function claudeRouteExecutionFailure(error: string): AdapterExecutionResult {
  return {
    exitCode: 1,
    signal: null,
    timedOut: false,
    errorCode: CLAWROUTER_CONFIGURATION_ERROR,
    errorMessage: error,
  };
}

export function claudeRouteProbeFailure(adapterType: string, error: string): AdapterEnvironmentTestResult {
  return {
    adapterType,
    status: "fail",
    checks: [{ code: CLAWROUTER_CONFIGURATION_ERROR, level: "error", message: error }],
    testedAt: new Date().toISOString(),
  };
}
