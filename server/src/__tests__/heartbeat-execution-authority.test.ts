import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { and, eq } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  agents, agentWakeupRequests, companies, companyMemberships, createDb, heartbeatRuns, issues,
} from "@paperclipai/db";
import { registerServerAdapter, unregisterServerAdapter } from "../adapters/index.js";
import { heartbeatService } from "../services/heartbeat.js";
import { NativeRunnerOwnershipUnverifiedError } from "../services/native-runtime/native-runner-ownership.js";
import { getRunLogStore } from "../services/run-log-store.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";

// Seams inside the executeRun pipeline. Each wrapper counts the call and runs an
// optional hook first, so a test can move run ownership to another executor at an
// exact point and then observe what the losing executor still does.
const hooks = vi.hoisted(() => ({
  beforeGithubEnvironment: null as null | (() => Promise<void>),
  beforeLedgerScope: null as null | (() => Promise<void>),
  beforeSessionResolution: null as null | (() => Promise<void>),
  calls: { githubEnvironment: 0, sessionResolution: 0, runtimeServices: 0, nativeSelection: 0 },
}));

// The last unconditional step of run preparation after workspace preparation has
// succeeded and before the H49 context write (executeRun.runtimeServices.contextSnapshot).
// Moving ownership here makes the H49 write the first write that can lose.
vi.mock("@paperclipai/adapter-utils/execution-target", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@paperclipai/adapter-utils/execution-target")>();
  return {
    ...actual,
    prepareGitHubExecutionEnvironment: async (...args: Parameters<typeof actual.prepareGitHubExecutionEnvironment>) => {
      hooks.calls.githubEnvironment += 1;
      await hooks.beforeGithubEnvironment?.();
      return actual.prepareGitHubExecutionEnvironment(...args);
    },
  };
});

vi.mock("../services/heartbeat/run-state.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../services/heartbeat/run-state.js")>();
  return {
    ...actual,
    resolveLedgerScopeForRun: async (...args: Parameters<typeof actual.resolveLedgerScopeForRun>) => {
      await hooks.beforeLedgerScope?.();
      return actual.resolveLedgerScopeForRun(...args);
    },
  };
});

vi.mock("../services/heartbeat/session-preparation.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../services/heartbeat/session-preparation.js")>();
  return {
    ...actual,
    resolveHeartbeatAdapterSession: async (...args: Parameters<typeof actual.resolveHeartbeatAdapterSession>) => {
      hooks.calls.sessionResolution += 1;
      await hooks.beforeSessionResolution?.();
      return actual.resolveHeartbeatAdapterSession(...args);
    },
  };
});

vi.mock("../services/heartbeat/runtime-services.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../services/heartbeat/runtime-services.js")>();
  return {
    ...actual,
    startHeartbeatRuntimeServices: async (...args: Parameters<typeof actual.startHeartbeatRuntimeServices>) => {
      hooks.calls.runtimeServices += 1;
      return actual.startHeartbeatRuntimeServices(...args);
    },
  };
});

const support = await getEmbeddedPostgresTestSupport();
const describePostgres = support.supported ? describe : describe.skip;
if (!support.supported) console.warn(`Skipping heartbeat execution authority tests: ${support.reason}`);

const STOLEN_OWNER = "stolen-by-another-executor";

describePostgres("heartbeat execution authority", () => {
  const adapterType = "execution_authority_test";
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  let heartbeat: ReturnType<typeof heartbeatService>;
  let adapterLaunches = 0;
  let ledgerScopeSeenByAdapter: unknown;

  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("heartbeat-execution-authority-");
    db = createDb(temporary.connectionString);
    heartbeat = heartbeatService(db);
    registerServerAdapter({
      type: adapterType, supportsLocalAgentJwt: false,
      execute: async (input) => {
        adapterLaunches += 1;
        const [row] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, input.runId));
        ledgerScopeSeenByAdapter = (row?.usageJson as Record<string, unknown> | null)?.ledgerScope;
        return { exitCode: 0, signal: null, timedOut: false, resultJson: {} };
      },
      testEnvironment: async () => ({ adapterType, status: "pass", checks: [], testedAt: new Date().toISOString() }),
    });
  }, 30_000);
  beforeEach(() => {
    adapterLaunches = 0;
    ledgerScopeSeenByAdapter = undefined;
    hooks.beforeGithubEnvironment = null;
    hooks.beforeLedgerScope = null;
    hooks.beforeSessionResolution = null;
    hooks.calls.githubEnvironment = 0;
    hooks.calls.sessionResolution = 0;
    hooks.calls.runtimeServices = 0;
    hooks.calls.nativeSelection = 0;
  });
  afterEach(() => vi.restoreAllMocks());
  afterAll(async () => {
    await heartbeat?.drainActiveRunExecutions();
    unregisterServerAdapter(adapterType);
    await db?.$client.end({ timeout: 0 });
    await temporary?.cleanup();
  });

  async function seed() {
    const companyId = randomUUID(), agentId = randomUUID(), issueId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "Authority fixture", issuePrefix: `A${companyId.slice(0, 6)}` });
    await db.insert(agents).values({ id: agentId, companyId, name: "Authority writer", adapterType, status: "idle" });
    await db.insert(issues).values({ id: issueId, companyId, title: "Authority fixture", status: "todo", assigneeAgentId: agentId, responsibleUserId: "authority-user", issueNumber: 1 });
    return { companyId, agentId, issueId };
  }

  // One-shot: later pipeline stages that reach the same seam must not re-steal.
  function clearTakeoverHooks() {
    hooks.beforeGithubEnvironment = null;
    hooks.beforeLedgerScope = null;
    hooks.beforeSessionResolution = null;
  }

  // Simulates another executor taking over the run: the stored owner and fence
  // change, so every write this executor made from its captured claim must lose.
  async function stealRunOwnership(agentId: string) {
    clearTakeoverHooks();
    const stolen = await db.update(heartbeatRuns)
      .set({ ownerToken: STOLEN_OWNER, fence: 9_000_000_000 })
      .where(and(eq(heartbeatRuns.agentId, agentId), eq(heartbeatRuns.status, "running")))
      .returning({ id: heartbeatRuns.id });
    // A steal that matched no running row would make every assertion below vacuous.
    expect(stolen).toHaveLength(1);
  }

  async function runToCompletion(f: Awaited<ReturnType<typeof seed>>) {
    const run = await heartbeat.wakeup(f.agentId, {
      source: "on_demand", triggerDetail: "manual", reason: "manual",
      payload: { issueId: f.issueId }, contextSnapshot: { issueId: f.issueId },
    });
    expect(run).not.toBeNull();
    await heartbeat.drainActiveRunExecutions();
    const [row] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run!.id));
    expect(row).toBeDefined();
    return row!;
  }

  it("control: a run that keeps ownership prepares, stamps the ledger scope and launches the adapter", async () => {
    const f = await seed();
    const row = await runToCompletion(f);
    expect(row.status, row.error ?? undefined).toBe("succeeded");
    expect(adapterLaunches).toBe(1);
    expect(hooks.calls.githubEnvironment).toBe(1);
    expect(hooks.calls.sessionResolution).toBe(1);
    expect(hooks.calls.runtimeServices).toBe(1);
    expect(ledgerScopeSeenByAdapter).toBeTruthy();
  });

  it("H51: a lost session-start write stops the run before runtime services and the adapter start", async () => {
    const f = await seed();
    // Session resolution runs after the context write and immediately before the
    // session-start write, so ownership is lost exactly at the H51 write.
    hooks.beforeSessionResolution = () => stealRunOwnership(f.agentId);
    const row = await runToCompletion(f);
    expect(hooks.calls.sessionResolution).toBe(1);
    expect(hooks.calls.runtimeServices).toBe(0);
    expect(adapterLaunches).toBe(0);
    // The new owner's claim is untouched: the losing executor wrote nothing terminal.
    expect(row.ownerToken).toBe(STOLEN_OWNER);
    expect(row.status).toBe("running");
  });

  it("H49: a lost context write stops the run before session preparation, runtime services and the adapter", async () => {
    const f = await seed();
    // Workspace preparation (and its own fenced context write) has already
    // succeeded when GitHub environment preparation runs; the next fenced write
    // is H49, so ownership is lost exactly there.
    hooks.beforeGithubEnvironment = () => stealRunOwnership(f.agentId);
    const row = await runToCompletion(f);
    // The takeover seam was reached, i.e. the run got past workspace preparation.
    expect(hooks.calls.githubEnvironment).toBe(1);
    // Session resolution lies after the H49 write and before the H51 write, so
    // zero calls proves the run stopped at H49 instead of continuing on stale state.
    expect(hooks.calls.sessionResolution).toBe(0);
    expect(hooks.calls.runtimeServices).toBe(0);
    expect(adapterLaunches).toBe(0);
    expect(row.ownerToken).toBe(STOLEN_OWNER);
    expect(row.status).toBe("running");
  });

  // Native runs carry no claim/controller pair: their write authority is "the row
  // is still running in native mode", so the winning competitor is Stop (or a
  // terminal settlement) rather than an owner takeover. The fixture uses the
  // persisted-native recovery shape (queued row already resolved to native for a
  // paperclip_runner agent), whose provider boundary is stubbed to fail closed.
  describe("native runs", () => {
    const STOP_MARKER = "authority_fixture_stop";
    let paperclipHome: string;
    let previousPaperclipHome: string | undefined;

    beforeEach(async () => {
      previousPaperclipHome = process.env.PAPERCLIP_HOME;
      paperclipHome = await fs.mkdtemp(path.join(os.tmpdir(), "authority-native-"));
      process.env.PAPERCLIP_HOME = paperclipHome;
    });
    afterEach(async () => {
      if (previousPaperclipHome === undefined) delete process.env.PAPERCLIP_HOME;
      else process.env.PAPERCLIP_HOME = previousPaperclipHome;
      // Native preparation can materialize read-only bundles in the owned temporary home.
      try { execFileSync("chmod", ["-R", "u+rwX", paperclipHome]); } catch { /* nothing materialized */ }
      await fs.rm(paperclipHome, { recursive: true, force: true });
    });

    async function seedNative() {
      const companyId = randomUUID(), agentId = randomUUID(), issueId = randomUUID();
      const runId = randomUUID(), wakeupRequestId = randomUUID();
      const issuePrefix = `N${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`;
      await db.insert(companies).values({
        id: companyId, name: "Native authority fixture", issuePrefix,
        defaultResponsibleUserId: "responsible-user", requireBoardApprovalForNewAgents: false,
      });
      await db.insert(companyMemberships).values({
        companyId, principalType: "user", principalId: "responsible-user", status: "active", membershipRole: "member",
      });
      await db.insert(agents).values({
        id: agentId, companyId, name: "Native authority writer", role: "engineer", status: "idle",
        adapterType: "paperclip_runner", adapterConfig: { provider: "codex", model: "gpt-5.6-luna" },
        runtimeConfig: { heartbeat: { wakeOnDemand: true, maxConcurrentRuns: 1 } }, permissions: {},
      });
      await db.insert(agentWakeupRequests).values({
        id: wakeupRequestId, companyId, agentId, source: "assignment", triggerDetail: "system",
        reason: "issue_assigned", payload: { issueId }, status: "queued", runId,
      });
      await db.insert(heartbeatRuns).values({
        id: runId, companyId, agentId, invocationSource: "assignment", triggerDetail: "system",
        status: "queued", wakeupRequestId, contextSnapshot: { issueId, taskId: issueId, wakeReason: "issue_assigned" },
      });
      await db.insert(issues).values({
        id: issueId, companyId, title: "Native authority fixture", status: "in_progress", priority: "medium",
        assigneeAgentId: agentId, checkoutRunId: runId, executionRunId: runId,
        responsibleUserId: "responsible-user", issueNumber: 1, identifier: `${issuePrefix}-1`, startedAt: new Date(),
      });
      await db.update(heartbeatRuns).set({
        scopeKind: "issue", issueId, runtimeMode: "native", runtimeModeResolvedAt: new Date(), nativeIssueId: issueId,
      }).where(eq(heartbeatRuns.id, runId));
      return { companyId, agentId, issueId, runId };
    }

    // Stop wins: the run is terminal before this executor's next fenced write.
    async function stopNativeRun(runId: string) {
      clearTakeoverHooks();
      const stopped = await db.update(heartbeatRuns)
        .set({ status: "cancelled", errorCode: STOP_MARKER, finishedAt: new Date() })
        .where(and(eq(heartbeatRuns.id, runId), eq(heartbeatRuns.status, "running"), eq(heartbeatRuns.runtimeMode, "native")))
        .returning({ id: heartbeatRuns.id });
      // A stop that matched no running native row would make every assertion vacuous.
      expect(stopped).toHaveLength(1);
    }

    async function runNative(runId: string) {
      // Stops at the provider boundary without spawning anything.
      const providerBackend = vi.fn((_execution: { workspace: { cwd: string } }) => {
        throw new NativeRunnerOwnershipUnverifiedError();
      });
      const nativeHeartbeat = heartbeatService(db, {
        nativeSessionBackendFactory: providerBackend,
        beforeNativeRuntimeSelection: async () => { hooks.calls.nativeSelection += 1; },
      });
      await nativeHeartbeat.resumeQueuedRuns();
      await nativeHeartbeat.drainActiveRunExecutions();
      const [row] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
      expect(row).toBeDefined();
      return { row: row!, providerBackend };
    }

    it("control: a native run that keeps authority passes H49 and H51 and reaches native runtime selection", async () => {
      const f = await seedNative();
      const { row, providerBackend } = await runNative(f.runId);
      expect(row.runtimeMode).toBe("native");
      expect(hooks.calls.githubEnvironment).toBe(1);
      expect(hooks.calls.sessionResolution).toBe(1);
      expect(hooks.calls.nativeSelection).toBe(1);
      expect(providerBackend).toHaveBeenCalledTimes(1);
      expect(adapterLaunches).toBe(0);
    });

    it("H49: Stop winning at the context write stops a native run before session preparation and selection", async () => {
      const f = await seedNative();
      hooks.beforeGithubEnvironment = () => stopNativeRun(f.runId);
      const { row, providerBackend } = await runNative(f.runId);
      // The seam sits after workspace preparation, directly before the H49 write.
      expect(hooks.calls.githubEnvironment).toBe(1);
      // Session resolution lies between H49 and H51, so zero calls isolates H49.
      expect(hooks.calls.sessionResolution).toBe(0);
      expect(hooks.calls.runtimeServices).toBe(0);
      expect(hooks.calls.nativeSelection).toBe(0);
      expect(providerBackend).not.toHaveBeenCalled();
      expect(adapterLaunches).toBe(0);
      expect(row).toMatchObject({ status: "cancelled", errorCode: STOP_MARKER, runtimeMode: "native" });
    });

    it("H51: Stop winning at the session-start write stops a native run before runtime services and selection", async () => {
      const f = await seedNative();
      hooks.beforeSessionResolution = () => stopNativeRun(f.runId);
      const { row, providerBackend } = await runNative(f.runId);
      // H49 already passed (the hook fires after it); the next fenced write is H51.
      expect(hooks.calls.githubEnvironment).toBe(1);
      expect(hooks.calls.sessionResolution).toBe(1);
      expect(hooks.calls.runtimeServices).toBe(0);
      expect(hooks.calls.nativeSelection).toBe(0);
      expect(providerBackend).not.toHaveBeenCalled();
      expect(adapterLaunches).toBe(0);
      expect(row).toMatchObject({ status: "cancelled", errorCode: STOP_MARKER, runtimeMode: "native" });
    });
  });

  it("ledger: a superseded executor cannot stamp ledger scope on the run", async () => {
    const f = await seed();
    hooks.beforeLedgerScope = () => stealRunOwnership(f.agentId);
    const row = await runToCompletion(f);
    expect((row.usageJson as Record<string, unknown> | null)?.ledgerScope).toBeUndefined();
    expect(adapterLaunches).toBe(0);
    expect(hooks.calls.sessionResolution).toBe(0);
    expect(row.ownerToken).toBe(STOLEN_OWNER);
  });

  it("cold tier: readLog passes the persisted s3 tier to the run-log reader", async () => {
    const f = await seed();
    const runId = randomUUID();
    const logRef = `run-logs/${f.companyId}/${runId}.ndjson.gz`;
    await db.insert(heartbeatRuns).values({
      id: runId, companyId: f.companyId, agentId: f.agentId,
      invocationSource: "assignment", triggerDetail: "system", status: "succeeded",
      logStore: "s3", logRef,
    });
    const read = vi.spyOn(getRunLogStore(), "read").mockResolvedValue({ content: "archived line\n", nextOffset: 14 });

    const result = await heartbeat.readLog(runId, { offset: 0, limitBytes: 1024 });

    expect(read).toHaveBeenCalledTimes(1);
    expect(read).toHaveBeenCalledWith({ store: "s3", logRef }, { offset: 0, limitBytes: 1024 });
    expect(result).toMatchObject({ runId, store: "s3", logRef, content: "archived line\n" });
  });

  it("cold tier: a hot local_file run still reads the local tier", async () => {
    const f = await seed();
    const read = vi.spyOn(getRunLogStore(), "read").mockResolvedValue({ content: "hot\n", nextOffset: 4 });
    await heartbeat.readLog({ id: randomUUID(), companyId: f.companyId, logStore: "local_file", logRef: "hot/ref.ndjson" });
    expect(read).toHaveBeenCalledWith({ store: "local_file", logRef: "hot/ref.ndjson" }, undefined);
  });
});
