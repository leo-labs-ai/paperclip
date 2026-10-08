import { describe, expect, it } from "vitest";
import { resolveClaudeClawRouterRoute, claudeRouteExecutionFailure } from "./clawrouter-route.js";

const trusted = { PAPERCLIP_CLAWROUTER_BASE_URL: "http://router.internal:8789", CLAWROUTER_PROXY_KEY: "synthetic-server-secret" };
const credentialUrl = new URL("https://router.invalid");
credentialUrl.username = "synthetic-user";
credentialUrl.password = "synthetic-password";

describe("explicit Claude ClawRouter route", () => {
  it("preserves native configuration identity without requiring server routing", () => {
    const config = { model: "claude-haiku-4-5", env: { ANTHROPIC_BASE_URL: "http://bridge" } };
    expect(resolveClaudeClawRouterRoute(config, false, {}).config).toBe(config);
    expect(resolveClaudeClawRouterRoute(config, true, {}).config).toBe(config);
  });
  it("normalizes only the selected provider and never mutates stored config", () => {
    const config = { model: "clawrouter/claude-sonnet-5-200k", env: { OTHER: "preserved", ANTHROPIC_API_KEY: "old", CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST: "0", CLAUDE_CODE_SUBPROCESS_ENV_SCRUB: "0" } };
    const before = structuredClone(config);
    expect(resolveClaudeClawRouterRoute(config, false, trusted).config).toEqual({
      model: "claude-sonnet-5-200k", env: { OTHER: "preserved", ANTHROPIC_API_KEY: "", ANTHROPIC_BASE_URL: trusted.PAPERCLIP_CLAWROUTER_BASE_URL, ANTHROPIC_AUTH_TOKEN: trusted.CLAWROUTER_PROXY_KEY, CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST: "1", CLAUDE_CODE_SUBPROCESS_ENV_SCRUB: "1" },
    });
    expect(config).toEqual(before);
  });
  it("does not accept agent-controlled routing URLs or credential variable names", () => {
    const config = { model: "clawrouter/claude-sonnet-5-200k", apiKeyEnv: "OTHER_SECRET", env: { PAPERCLIP_CLAWROUTER_BASE_URL: "https://attacker.invalid", CLAWROUTER_PROXY_KEY: "agent-key", ANTHROPIC_BASE_URL: "https://attacker.invalid" } };
    expect(resolveClaudeClawRouterRoute(config, false, {}).error).toBeTruthy();
    const result = resolveClaudeClawRouterRoute(config, false, { ...trusted, OTHER_SECRET: "must-not-export" });
    expect(result.config?.env).toMatchObject({ ANTHROPIC_BASE_URL: trusted.PAPERCLIP_CLAWROUTER_BASE_URL, ANTHROPIC_AUTH_TOKEN: trusted.CLAWROUTER_PROXY_KEY });
    expect(JSON.stringify(result)).not.toContain("must-not-export");
  });
  it.each(["file:///tmp/key", credentialUrl.href, "https://router?token=secret", "https://router/#secret", "not-a-url"])("fails closed for invalid trusted URL %s", (url) => {
    const result = resolveClaudeClawRouterRoute({ model: "clawrouter/claude-sonnet-5-200k" }, false, { ...trusted, PAPERCLIP_CLAWROUTER_BASE_URL: url });
    expect(result.error).toBeTruthy();
    expect(JSON.stringify(claudeRouteExecutionFailure(result.error!))).not.toContain(url);
    expect(JSON.stringify(result)).not.toContain(trusted.CLAWROUTER_PROXY_KEY);
  });
  it("does not export server credentials to remote execution", () => {
    expect(resolveClaudeClawRouterRoute({ model: "clawrouter/claude-sonnet-5-200k" }, true, trusted).config).toBeUndefined();
  });
  it.each(["CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY", "CLAUDE_CODE_USE_MANTLE", "CLAUDE_CODE_USE_ANTHROPIC_AWS", "ANTHROPIC_BEDROCK_BASE_URL", "CLAUDE_CODE_OAUTH_TOKEN"])("rejects competing provider setting %s without disclosing values", (name) => {
    const value = name.startsWith("CLAUDE_CODE_USE_") ? "1" : "synthetic-conflicting-secret";
    const config = { model: "clawrouter/claude-sonnet-5-200k", env: { [name]: value } };
    const result = resolveClaudeClawRouterRoute(config, false, trusted);
    expect(result.error).toBeTruthy();
    expect(JSON.stringify(result)).not.toContain("synthetic-conflicting-secret");
    expect(resolveClaudeClawRouterRoute({ model: "clawrouter/claude-sonnet-5-200k" }, false, { ...trusted, [name]: value }).error).toBeTruthy();
  });
  it("rejects missing model or server key", () => {
    expect(resolveClaudeClawRouterRoute({ model: "clawrouter/" }, false, trusted).error).toBeTruthy();
    expect(resolveClaudeClawRouterRoute({ model: "clawrouter/claude-sonnet-5-200k" }, false, { PAPERCLIP_CLAWROUTER_BASE_URL: trusted.PAPERCLIP_CLAWROUTER_BASE_URL }).error).toBeTruthy();
  });
});
