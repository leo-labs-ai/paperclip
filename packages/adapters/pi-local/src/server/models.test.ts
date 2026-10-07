import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as serverUtils from "@paperclipai/adapter-utils/server-utils";
import {
  discoverPiModels,
  discoverPiModelsCached,
  ensurePiModelConfiguredAndAvailable,
  findClosePiModelMatches,
  formatPiModelUnavailableMessage,
  listPiModels,
  validatePiModelForPersistence,
  piModelsCacheSizeForTests,
  resetPiModelsCacheForTests,
} from "./models.js";

async function writeExecutable(directory: string, source: string): Promise<string> {
  const command = path.join(directory, "fake-pi");
  await writeFile(command, `#!/bin/sh\n${source}`);
  await chmod(command, 0o755);
  return command;
}

describe("pi models", () => {
  afterEach(() => {
    delete process.env.PAPERCLIP_PI_COMMAND;
    resetPiModelsCacheForTests();
    vi.restoreAllMocks();
  });

  it("allows cold model discovery enough time under runtime contention", async () => {
    const runChildProcess = vi.spyOn(serverUtils, "runChildProcess").mockResolvedValue({
      exitCode: 0,
      signal: null,
      timedOut: false,
      stdout: "provider  model\nbridge  claude-opus-4-8\n",
      stderr: "",
      pid: null,
      startedAt: new Date().toISOString(),
    });

    await discoverPiModels({ command: "pi", cwd: "/tmp" });

    expect(runChildProcess).toHaveBeenCalledWith(
      expect.any(String),
      "pi",
      ["--list-models"],
      expect.objectContaining({ timeoutSec: 60, graceSec: 3 }),
    );
  });

  it("returns an empty list when discovery command is unavailable", async () => {
    process.env.PAPERCLIP_PI_COMMAND = "__paperclip_missing_pi_command__";
    await expect(listPiModels()).resolves.toEqual([]);
  });

  it("rejects when model is missing", async () => {
    await expect(
      ensurePiModelConfiguredAndAvailable({ model: "" }),
    ).rejects.toThrow("Pi requires `adapterConfig.model`");
  });

  it("rejects when discovery cannot run for configured model", async () => {
    process.env.PAPERCLIP_PI_COMMAND = "__paperclip_missing_pi_command__";
    await expect(
      ensurePiModelConfiguredAndAvailable({
        model: "xai/grok-4",
        env: { PI_CODING_AGENT_DIR: "/nonexistent-pi-agent-dir" },
      }),
    ).rejects.toThrow();
  });

  it("answers the preflight from the agent-config models.json without spawning pi", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-agent-config-"));
    try {
      const runChildProcess = vi.spyOn(serverUtils, "runChildProcess");
      await writeFile(
        path.join(directory, "models.json"),
        JSON.stringify({
          providers: { clawrouter: { models: [{ id: "claude-opus-5-200k" }, "claude-sonnet-5" ] } },
        }),
      );

      await expect(
        ensurePiModelConfiguredAndAvailable({
          model: "clawrouter/claude-sonnet-5",
          command: "pi",
          env: { PI_CODING_AGENT_DIR: directory },
        }),
      ).resolves.toEqual([
        { id: "clawrouter/claude-opus-5-200k", label: "clawrouter/claude-opus-5-200k" },
        { id: "clawrouter/claude-sonnet-5", label: "clawrouter/claude-sonnet-5" },
      ]);
      expect(runChildProcess).not.toHaveBeenCalled();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("falls back to spawn discovery when models.json does not list the model", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-agent-config-"));
    try {
      await writeFile(
        path.join(directory, "models.json"),
        JSON.stringify({ providers: { clawrouter: { models: ["claude-sonnet-5"] } } }),
      );
      const runChildProcess = vi.spyOn(serverUtils, "runChildProcess").mockResolvedValue({
        exitCode: 0,
        signal: null,
        timedOut: false,
        stdout: "provider  model\nbridge  claude-opus-4-8\n",
        stderr: "",
        pid: null,
        startedAt: new Date().toISOString(),
      });

      await expect(
        ensurePiModelConfiguredAndAvailable({
          model: "bridge/claude-opus-4-8",
          command: "pi",
          cwd: "/tmp",
          env: { PI_CODING_AGENT_DIR: directory },
        }),
      ).resolves.toEqual([{ id: "bridge/claude-opus-4-8", label: "bridge/claude-opus-4-8" }]);
      expect(runChildProcess).toHaveBeenCalledTimes(1);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("skips the model preflight when discovery times out", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(serverUtils, "runChildProcess").mockResolvedValue({
      exitCode: null,
      signal: "SIGKILL",
      timedOut: true,
      stdout: "",
      stderr: "",
      pid: null,
      startedAt: new Date().toISOString(),
    });

    await expect(
      ensurePiModelConfiguredAndAvailable({
        model: "xai/grok-4",
        command: "pi",
        cwd: "/tmp",
        env: { PI_CODING_AGENT_DIR: "/nonexistent-pi-agent-dir" },
      }),
    ).resolves.toEqual([]);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("Pi model discovery unavailable"));
  });

  it("still rejects a configured model that a completed discovery does not list", async () => {
    vi.spyOn(serverUtils, "runChildProcess").mockResolvedValue({
      exitCode: 0,
      signal: null,
      timedOut: false,
      stdout: "provider  model\nbridge  claude-opus-4-8\n",
      stderr: "",
      pid: null,
      startedAt: new Date().toISOString(),
    });

    await expect(
      ensurePiModelConfiguredAndAvailable({
        model: "xai/grok-4",
        command: "pi",
        cwd: "/tmp",
        env: { PI_CODING_AGENT_DIR: "/nonexistent-pi-agent-dir" },
      }),
    ).rejects.toThrow("Configured Pi model is unavailable");
  });

  it("does not re-run discovery for every run while a timeout cooldown is active", async () => {
    const runChildProcess = vi.spyOn(serverUtils, "runChildProcess").mockResolvedValue({
      exitCode: null,
      signal: "SIGKILL",
      timedOut: true,
      stdout: "",
      stderr: "",
      pid: null,
      startedAt: new Date().toISOString(),
    });

    await expect(discoverPiModelsCached({ command: "pi", cwd: "/tmp" })).rejects.toThrow(
      "timed out",
    );
    await expect(discoverPiModelsCached({ command: "pi", cwd: "/tmp" })).rejects.toThrow(
      "timed out",
    );

    expect(runChildProcess).toHaveBeenCalledTimes(1);
  });

  it("coalesces concurrent refreshes for the same workspace", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-models-"));
    try {
      const calls = path.join(directory, "calls");
      const command = await writeExecutable(
        directory,
        `[ -z "$PAPERCLIP_RUN_ID" ] || exit 2\nprintf x >> "${calls}"\nprintf 'provider  model\\nbridge  claude-opus-4-8\\n'\n`,
      );

      await Promise.all([
        discoverPiModelsCached({ command, cwd: directory, env: { PAPERCLIP_RUN_ID: "first" } }),
        discoverPiModelsCached({ command, cwd: directory, env: { PAPERCLIP_RUN_ID: "second" } }),
      ]);

      await expect(readFile(calls, "utf8")).resolves.toBe("x");
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("uses the last successful model list when a refresh fails", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-models-"));
    const now = vi.spyOn(Date, "now");
    try {
      const failureMarker = path.join(directory, "fail");
      const command = await writeExecutable(
        directory,
        `if [ -f "${failureMarker}" ]; then echo refresh-failed >&2; exit 1; fi\ntouch "${failureMarker}"\nprintf 'provider  model\\nbridge  claude-opus-4-8\\n'\n`,
      );
      now.mockReturnValue(1_000);
      await expect(discoverPiModelsCached({ command })).resolves.toEqual([
        { id: "bridge/claude-opus-4-8", label: "bridge/claude-opus-4-8" },
      ]);

      now.mockReturnValue(301_001);
      await expect(discoverPiModelsCached({ command })).resolves.toEqual([
        { id: "bridge/claude-opus-4-8", label: "bridge/claude-opus-4-8" },
      ]);

      now.mockReturnValue(86_401_001);
      await expect(discoverPiModelsCached({ command })).rejects.toThrow("refresh-failed");
    } finally {
      now.mockRestore();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("keeps model registries scoped to their workspace", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-models-"));
    try {
      const firstWorkspace = path.join(directory, "first");
      const secondWorkspace = path.join(directory, "second");
      await Promise.all([mkdir(firstWorkspace), mkdir(secondWorkspace)]);
      const command = await writeExecutable(
        directory,
        "printf 'provider  model\\nbridge  %s\\n' \"$(basename \"$(pwd)\")\"\n",
      );

      await expect(discoverPiModelsCached({ command, cwd: firstWorkspace })).resolves.toEqual([
        { id: "bridge/first", label: "bridge/first" },
      ]);
      await expect(discoverPiModelsCached({ command, cwd: secondWorkspace })).resolves.toEqual([
        { id: "bridge/second", label: "bridge/second" },
      ]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("parses stdout when a successful discovery also emits a warning", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-models-"));
    try {
      const command = await writeExecutable(
        directory,
        "echo model-load-warning >&2\nprintf 'provider  model\\nbridge  claude-opus-4-8\\n'\n",
      );

      await expect(discoverPiModelsCached({ command })).resolves.toEqual([
        { id: "bridge/claude-opus-4-8", label: "bridge/claude-opus-4-8" },
      ]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("separates model registries with different effective environments", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-models-"));
    const previousProviders = process.env.PAPERCLIP_PI_PROVIDERS;
    try {
      const calls = path.join(directory, "calls");
      const command = await writeExecutable(
        directory,
        `printf x >> "${calls}"\nprintf 'provider  model\\nbridge  claude-opus-4-8\\n'\n`,
      );

      process.env.PAPERCLIP_PI_PROVIDERS = "first";
      await discoverPiModelsCached({ command });
      process.env.PAPERCLIP_PI_PROVIDERS = "second";
      await discoverPiModelsCached({ command });

      await expect(readFile(calls, "utf8")).resolves.toBe("xx");
    } finally {
      if (previousProviders === undefined) delete process.env.PAPERCLIP_PI_PROVIDERS;
      else process.env.PAPERCLIP_PI_PROVIDERS = previousProviders;
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("bounds retained discovery environments", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "paperclip-pi-models-"));
    try {
      const command = await writeExecutable(
        directory,
        "printf 'provider  model\\nbridge  claude-opus-4-8\\n'\n",
      );
      for (let index = 0; index < 65; index += 1) {
        await discoverPiModelsCached({ command, env: { PAPERCLIP_PI_PROVIDERS: String(index) } });
      }

      expect(piModelsCacheSizeForTests()).toBe(64);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("lists close matches first and every available model in the unavailable error", () => {
    const models = Array.from({ length: 20 }, (_, i) => ({ id: `aaa/model-${String(i).padStart(2, "0")}`, label: "" }));
    models.push({ id: "clawrouter/gpt-5.6-luna-200k", label: "" }, { id: "clawrouter/gpt-5.6-sol-200k", label: "" });
    const message = formatPiModelUnavailableMessage("clawrouter/gpt-5.6-luna", models);
    expect(message).toContain("Did you mean: clawrouter/gpt-5.6-luna-200k");
    expect(message).toContain("Available models (22):");
    for (const entry of models) expect(message).toContain(entry.id);
    expect(message).not.toContain("...");
  });

  it("finds a model under another provider prefix and ignores unrelated ids", () => {
    const models = [
      { id: "clawrouter/gpt-5.6-sol-200k", label: "" },
      { id: "bridge/claude-opus-4-8", label: "" },
    ];
    expect(findClosePiModelMatches("openai/gpt-5.6-sol-200k", models)).toEqual(["clawrouter/gpt-5.6-sol-200k"]);
    expect(findClosePiModelMatches("zzz/qqqq", models)).toEqual([]);
  });

  it("validates a model for persistence against the discovered catalog", async () => {
    vi.spyOn(serverUtils, "runChildProcess").mockResolvedValue({
      exitCode: 0,
      signal: null,
      timedOut: false,
      stdout: "provider  model\nclawrouter  gpt-5.6-luna-200k\n",
      stderr: "",
      pid: null,
      startedAt: new Date().toISOString(),
    });
    const env = { PI_CODING_AGENT_DIR: "/nonexistent-pi-agent-dir" };
    await expect(
      validatePiModelForPersistence({ model: "clawrouter/gpt-5.6-luna-200k", command: "pi", cwd: "/tmp", env }),
    ).resolves.toEqual({ status: "valid" });
    const invalid = await validatePiModelForPersistence({ model: "clawrouter/gpt-5.6-luna", command: "pi", cwd: "/tmp", env });
    expect(invalid.status).toBe("invalid");
    expect(invalid.status === "invalid" && invalid.message).toContain("Did you mean: clawrouter/gpt-5.6-luna-200k");
  });

  it("accepts a model listed only in the agent-config models.json without spawning pi", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "pi-persist-"));
    try {
      await writeFile(
        path.join(dir, "models.json"),
        JSON.stringify({ providers: { clawrouter: { models: [{ id: "gpt-5.6-sol-200k" }] } } }),
      );
      const run = vi.spyOn(serverUtils, "runChildProcess");
      await expect(
        validatePiModelForPersistence({ model: "clawrouter/gpt-5.6-sol-200k", env: { PI_CODING_AGENT_DIR: dir } }),
      ).resolves.toEqual({ status: "valid" });
      expect(run).not.toHaveBeenCalled();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("fails open as unverified when the catalog cannot be read", async () => {
    vi.spyOn(serverUtils, "runChildProcess").mockResolvedValue({
      exitCode: null,
      signal: "SIGTERM",
      timedOut: true,
      stdout: "",
      stderr: "",
      pid: null,
      startedAt: new Date().toISOString(),
    });
    const result = await validatePiModelForPersistence({
      model: "clawrouter/anything",
      command: "pi",
      cwd: "/tmp",
      env: { PI_CODING_AGENT_DIR: "/nonexistent-pi-agent-dir" },
    });
    expect(result.status).toBe("unverified");
  });
});
