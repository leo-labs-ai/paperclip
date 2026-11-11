import type { Db } from "@paperclipai/db";
import { randomUUID } from "node:crypto";
import { activityLog, agents, agentWakeupRequests, authUsers, companies, companyMemberships, createDb, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS, heartbeatRunEvents, heartbeatRuns, issues, issueThreadInteractions } from "@paperclipai/db";
import { eq, sql } from "drizzle-orm";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { DETACHED_PROCESS_ERROR_CODE } from "./recovery.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "../../__tests__/helpers/embedded-postgres.js";
import type { CurrentUserRedactionOptions } from "../../log-redaction.js";
import { publishLiveEvent } from "../live-events.js";
import { withRunAuthFailureRepairLock } from "../connection-intents.js";
import { getHeartbeatRunRuntimeStatus } from "../heartbeat-run-runtime-status.js";
import { createHeartbeatRunState } from "./run-state.js";
import { createHeartbeatRunPreparation } from "./run-preparation.js";
import { createHeartbeatLifecycle, persistHeartbeatRunProcessMetadata, type HeartbeatLifecycleDependencies, type HeartbeatRunLivenessAuthority } from "./run-lifecycle.js";

vi.mock("../live-events.js", () => ({ publishLiveEvent: vi.fn() }));
vi.mock("../connection-intents.js", async importOriginal => {
  const original = await importOriginal<typeof import("../connection-intents.js")>();
  return { ...original, withRunAuthFailureRepairLock: vi.fn(original.withRunAuthFailureRepairLock) };
});

type Run = typeof heartbeatRuns.$inferSelect;
function callbacks(db: Db) {
  const state = createHeartbeatRunState(db);
  return {
    ...state,
    ...createHeartbeatRunPreparation(db),
    getRun: vi.fn(state.getRun),
    issuesSvc: {
      addComment: vi.fn<HeartbeatLifecycleDependencies["issuesSvc"]["addComment"]>(),
      listReviewAttention: vi.fn<HeartbeatLifecycleDependencies["issuesSvc"]["listReviewAttention"]>(),
    },
    recovery: { escalateStrandedAssignedIssue: vi.fn<HeartbeatLifecycleDependencies["recovery"]["escalateStrandedAssignedIssue"]>() },
    companySkills: { completeTestRunForIssue: vi.fn<HeartbeatLifecycleDependencies["companySkills"]["completeTestRunForIssue"]>() },
    budgets: { getInvocationBlock: vi.fn(async () => null) },
    treeControlSvc: { getActivePauseHoldGate: vi.fn(async () => null) },
    enqueueWakeup: vi.fn<HeartbeatLifecycleDependencies["enqueueWakeup"]>(),
    getCurrentUserRedactionOptions: vi.fn(async (): Promise<CurrentUserRedactionOptions> => ({})),
    getAgentInvokability: vi.fn<HeartbeatLifecycleDependencies["getAgentInvokability"]>(async () => ({ invokable: true })),
    emitTerminalAgentTaskRun: vi.fn<HeartbeatLifecycleDependencies["emitTerminalAgentTaskRun"]>(),
    budgetHooks: {},
  } satisfies HeartbeatLifecycleDependencies;
}
function guardedDatabase() {
  const access = vi.fn(() => { throw new Error("Unexpected lifecycle database access"); });
  return { db: new Proxy({}, { get: access }) as Db, access };
}

describe("heartbeat lifecycle module boundary", () => {
  it("constructs without database work or reporting effects", () => {
    const database = guardedDatabase();
    const deps = callbacks(database.db);
    createHeartbeatLifecycle(database.db, deps);
    expect(database.access).not.toHaveBeenCalled();
    for (const callback of Object.values(deps)) if (vi.isMockFunction(callback)) expect(callback).not.toHaveBeenCalled();
  });

  it("rejects process metadata without an original controller before database access", async () => {
    const database = guardedDatabase();
    expect(await persistHeartbeatRunProcessMetadata(database.db, "run", {
      pid: 123456789, processGroupId: null, startedAt: "2026-10-09T00:00:00.000Z", targetKind: "remote",
    }, {
      kind: "legacy", executionClaim: { ownerToken: "owner", fence: 7 }, executionControllerBootId: "",
    })).toBeNull();
    expect(database.access).not.toHaveBeenCalled();
  });

  it("rejects missing or fabricated liveness authority before database access", async () => {
    const database = guardedDatabase();
    const lifecycle = createHeartbeatLifecycle(database.db, callbacks(database.db));
    const run = { id: "unowned", status: "failed" } as Run;
    // @ts-expect-error Authority is mandatory, including during caller migration.
    expect(await lifecycle.classifyAndPersistRunLiveness(run)).toBeNull();
    expect(await lifecycle.classifyAndPersistRunLiveness(run, { kind: "terminal" } as HeartbeatRunLivenessAuthority)).toBeNull();
    expect(lifecycle.terminalRunLivenessAuthority(run)).toBeNull();
    expect(database.access).not.toHaveBeenCalled();
  });

  it("ignores absent wake requests without accessing the database", async () => {
    const database = guardedDatabase();
    const lifecycle = createHeartbeatLifecycle(database.db, callbacks(database.db));
    await lifecycle.setWakeupStatus(null, "completed");
    await lifecycle.setWakeupStatus(undefined, "completed");
    expect(database.access).not.toHaveBeenCalled();
  });

  it.each([
    { livenessState: "empty_response", contextSnapshot: { goalControlRequestId: "goal" } },
    { livenessState: "plan_only", contextSnapshot: { resumeSessionGoalHeartbeat: true } },
    { livenessState: "productive", contextSnapshot: { issueId: "issue" } },
    { livenessState: "empty_response", contextSnapshot: {} },
  ])("keeps ineligible continuations out of wake admission (%j)", async (input) => {
    const database = guardedDatabase();
    const deps = callbacks(database.db);
    await createHeartbeatLifecycle(database.db, deps).handleRunLivenessContinuation(input as unknown as Run);
    expect(database.access).not.toHaveBeenCalled();
    expect(deps.enqueueWakeup).not.toHaveBeenCalled();
    expect(deps.budgets.getInvocationBlock).not.toHaveBeenCalled();
  });

  it("preserves retained native ownership before lease-release teardown reads or writes", async () => {
    const database = guardedDatabase();
    const deps = callbacks(database.db);
    const run = { runtimeMode: "native", status: "running", nativePhase: "terminal_failure", errorCode: "native_execution_ownership_unverified" } as Run;
    expect(await createHeartbeatLifecycle(database.db, deps).terminalizeRunOnLeaseRelease(run)).toBe(run);
    expect(database.access).not.toHaveBeenCalled();
    expect(deps.emitTerminalAgentTaskRun).not.toHaveBeenCalled();
  });

  it("rejects progress without original controller authority before reading a replacement row", async () => {
    const database = guardedDatabase();
    const deps = callbacks(database.db);
    const run = { id: "late-progress", companyId: "company", agentId: "agent", status: "running", contextSnapshot: {} } as Run;
    expect(await createHeartbeatLifecycle(database.db, deps).recordCurrentHeartbeatRunRuntimeProgress(run,
      { phase: "finalize", message: "late update" }, null,
      { kind: "legacy", executionClaim: { ownerToken: "original", fence: 7 }, executionControllerBootId: "" },
    )).toBeNull();
    expect(deps.getRun).not.toHaveBeenCalled();
    expect(getHeartbeatRunRuntimeStatus(run.id)).toBeNull();
    expect(database.access).not.toHaveBeenCalled();
  });
});

const support = await getEmbeddedPostgresTestSupport();
if (!support.supported) console.warn(`Skipping lifecycle module database tests: ${support.reason}`);
describe.skipIf(!support.supported)("heartbeat lifecycle database wiring", () => {
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: Db;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("paperclip-heartbeat-lifecycle-module");
    db = createDb(database.connectionString);
  }, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS);
  afterAll(async () => {
    await db?.$client.end();
    await database?.cleanup();
  });
  beforeEach(async () => {
    vi.mocked(publishLiveEvent).mockClear();
    await db.execute(sql`truncate table companies restart identity cascade`);
  });

  async function fixture(patch: Partial<typeof heartbeatRuns.$inferInsert> = {}) {
    const [company] = await db.insert(companies).values({ name: "Lifecycle module", issuePrefix: "LM" }).returning();
    const [agent] = await db.insert(agents).values({ companyId: company.id, name: "Lifecycle agent" }).returning();
    const [wake] = await db.insert(agentWakeupRequests).values({ companyId: company.id, agentId: agent.id, source: "assignment", status: "claimed" }).returning();
    const [run] = await db.insert(heartbeatRuns).values({ companyId: company.id, agentId: agent.id, wakeupRequestId: wake.id, status: "running", runtimeMode: "native", ...patch }).returning();
    return { company, agent, wake, run };
  }

  // Real interleaving seam: hold the row lock so the activity operation's observation read
  // completes, its fenced write blocks behind the lock, and the intervening write commits
  // between the two. Nothing is mocked; the stale comparison runs in PostgreSQL.
  async function reportAcrossInterveningWrite(
    run: Run,
    intervene: (tx: Parameters<Parameters<Db["transaction"]>[0]>[0]) => Promise<unknown>,
  ) {
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    let pending!: ReturnType<typeof lifecycle.reportAuthenticatedRunActivity>;
    await db.transaction(async tx => {
      await tx.execute(sql`select 1 from heartbeat_runs where id = ${run.id} for update`);
      const [holder] = (await tx.execute(sql`select pg_backend_pid() as pid`)) as unknown as Array<{ pid: number }>;
      pending = lifecycle.reportAuthenticatedRunActivity(run.id, { companyId: run.companyId, agentId: run.agentId });
      let blocked = false;
      for (let attempt = 0; attempt < 500 && !blocked; attempt++) {
        const rows = (await tx.execute(
          sql`select 1 from pg_stat_activity where ${holder.pid} = any(pg_blocking_pids(pid))`,
        )) as unknown as Array<unknown>;
        blocked = rows.length > 0;
        if (!blocked) await new Promise(resolve => setTimeout(resolve, 10));
      }
      expect(blocked).toBe(true);
      await intervene(tx);
    });
    return pending;
  }
  async function exactUpdatedAt(runId: string) {
    const [row] = (await db.execute(
      sql`select to_char(updated_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') as value from heartbeat_runs where id = ${runId}`,
    )) as unknown as Array<{ value: string }>;
    return row.value;
  }
  async function detachedLegacyFixture(patch: Partial<typeof heartbeatRuns.$inferInsert> = {}) {
    return fixture({ runtimeMode: "legacy", ownerToken: "diagnostic-owner", fence: 7,
      errorCode: DETACHED_PROCESS_ERROR_CODE, error: "detached", ...patch });
  }
  async function expectDiagnosticsRetained(runId: string) {
    const [current] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
    expect(current.errorCode).toBe(DETACHED_PROCESS_ERROR_CODE);
    expect(current.error).toBe("detached");
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, runId))).toHaveLength(0);
  }

  it("clears authenticated activity diagnostics without renewing execution authority", async () => {
    const leaseExpiresAt = new Date("2025-01-01T00:00:00Z");
    const { run } = await detachedLegacyFixture({ leaseExpiresAt });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const updated = await lifecycle.reportAuthenticatedRunActivity(run.id, { companyId: run.companyId, agentId: run.agentId });
    expect(updated?.errorCode).toBeNull();
    expect(updated?.ownerToken).toBe(run.ownerToken);
    expect(updated?.fence).toBe(7);
    expect(updated?.leaseExpiresAt).toEqual(leaseExpiresAt);
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toHaveLength(1);
    expect(getHeartbeatRunRuntimeStatus(run.id)).toBeNull();
    expect(lifecycle.terminalRunLivenessAuthority(updated!)).toBeNull();
  });

  it("clears diagnostics when the stored updated_at carries sub-millisecond precision", async () => {
    // A JavaScript Date cannot hold these microseconds, so a Date equality predicate never matches.
    const { run } = await detachedLegacyFixture();
    await db.execute(sql`update heartbeat_runs set updated_at = '2026-01-01T00:00:00.123456Z' where id = ${run.id}`);
    expect(await exactUpdatedAt(run.id)).toBe("2026-01-01T00:00:00.123456");
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const updated = await lifecycle.reportAuthenticatedRunActivity(run.id, { companyId: run.companyId, agentId: run.agentId });
    expect(updated?.errorCode).toBeNull();
    expect(updated?.ownerToken).toBe("diagnostic-owner");
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toHaveLength(1);
  });

  it("rejects diagnostic activity from a different agent", async () => {
    const { run } = await fixture({ errorCode: DETACHED_PROCESS_ERROR_CODE, error: "detached" });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    expect(await lifecycle.reportAuthenticatedRunActivity(run.id, { companyId: run.companyId, agentId: run.companyId })).toBeNull();
    await expectDiagnosticsRetained(run.id);
  });

  it("does not clear a replacement executor's warning from stale activity evidence", async () => {
    const { run } = await detachedLegacyFixture({ ownerToken: "old" });
    const result = await reportAcrossInterveningWrite(run, tx =>
      tx.update(heartbeatRuns).set({ ownerToken: "replacement", fence: 8 }).where(eq(heartbeatRuns.id, run.id)));
    expect(result).toBeNull();
    const [current] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(current.ownerToken).toBe("replacement");
    expect(current.fence).toBe(8);
    await expectDiagnosticsRetained(run.id);
  });

  it("does not clear diagnostics after a same-owner write intervenes", async () => {
    const { run } = await detachedLegacyFixture();
    const before = await exactUpdatedAt(run.id);
    const result = await reportAcrossInterveningWrite(run, tx =>
      tx.update(heartbeatRuns).set({ resultJson: { progressed: true }, updatedAt: new Date() }).where(eq(heartbeatRuns.id, run.id)));
    expect(result).toBeNull();
    expect(await exactUpdatedAt(run.id)).not.toBe(before);
    await expectDiagnosticsRetained(run.id);
  });

  it("does not clear diagnostics after a same-owner write that differs only below a millisecond", async () => {
    const { run } = await detachedLegacyFixture();
    await db.execute(sql`update heartbeat_runs set updated_at = '2026-01-01T00:00:00.123456Z' where id = ${run.id}`);
    const result = await reportAcrossInterveningWrite(run, tx =>
      tx.execute(sql`update heartbeat_runs set updated_at = '2026-01-01T00:00:00.123789Z' where id = ${run.id}`));
    expect(result).toBeNull();
    expect(await exactUpdatedAt(run.id)).toBe("2026-01-01T00:00:00.123789");
    await expectDiagnosticsRetained(run.id);
  });

  it("does not clear diagnostics after a same-owner write that leaves updated_at unchanged", async () => {
    const { run } = await detachedLegacyFixture();
    const before = await exactUpdatedAt(run.id);
    const result = await reportAcrossInterveningWrite(run, tx =>
      tx.update(heartbeatRuns).set({ resultJson: { silent: true } }).where(eq(heartbeatRuns.id, run.id)));
    expect(result).toBeNull();
    expect(await exactUpdatedAt(run.id)).toBe(before);
    await expectDiagnosticsRetained(run.id);
  });

  it("does not clear diagnostics after the run leaves running status", async () => {
    const { run } = await detachedLegacyFixture();
    const result = await reportAcrossInterveningWrite(run, tx =>
      tx.update(heartbeatRuns).set({ status: "failed", updatedAt: new Date() }).where(eq(heartbeatRuns.id, run.id)));
    expect(result).toBeNull();
    const [current] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(current.errorCode).toBe(DETACHED_PROCESS_ERROR_CODE);
  });

  it("records a terminal reconciliation observation without minting execution or liveness authority", async () => {
    const { run } = await fixture({ runtimeMode: "legacy", status: "failed", ownerToken: null });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    await lifecycle.appendRunReconciliationEvent(run, { eventType: "lifecycle", message: "Retry evaluated" });
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toHaveLength(1);
    expect(lifecycle.terminalRunLivenessAuthority(run)).toBeNull();
    expect(getHeartbeatRunRuntimeStatus(run.id)).toBeNull();
  });

  it.each([
    { status: "running", ownerToken: "replacement" },
    { resultJson: { changed: true } },
    { contextSnapshot: { changed: true } },
    { fence: 9 },
  ])("rejects a stale reconciliation observation after %j", async patch => {
    const { run } = await fixture({ runtimeMode: "legacy", status: "failed", ownerToken: null });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    await db.update(heartbeatRuns).set(patch).where(eq(heartbeatRuns.id, run.id));
    await lifecycle.appendRunReconciliationEvent(run, { eventType: "lifecycle", message: "Stale retry observation" });
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toHaveLength(0);
  });

  // Seam: real lifecycle settlement and persisted repair/card/activity state, not collaborator calls.
  async function authFailureFixture() {
    const controller = "00000000-0000-4000-8000-000000000001";
    const claim = Object.freeze({ ownerToken: "auth-owner", fence: 7 });
    const seeded = await fixture({ runtimeMode: "legacy", ...claim, controllerBootId: controller });
    const userId = randomUUID();
    await db.insert(authUsers).values({ id: userId, name: "Repair user", email: `${userId}@example.test`, createdAt: new Date(), updatedAt: new Date() });
    await db.insert(companyMemberships).values({ companyId: seeded.company.id, principalType: "user", principalId: userId, status: "active", membershipRole: "member" });
    const [issue] = await db.insert(issues).values({ companyId: seeded.company.id, title: "Repair authentication", status: "in_progress", assigneeAgentId: seeded.agent.id }).returning();
    await db.update(agents).set({ adapterType: "codex_local" }).where(eq(agents.id, seeded.agent.id));
    const [run] = await db.update(heartbeatRuns).set({ contextSnapshot: { issueId: issue.id }, responsibleUserId: userId }).where(eq(heartbeatRuns.id, seeded.run.id)).returning();
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const won = await lifecycle.setRunStatusIfRunning(run.id, "failed", { errorCode: "codex_auth_required", error: "Sign in required" }, undefined, claim, controller);
    expect(won.updated).toBe(true);
    if (!won.run) throw new Error("Missing failed winner");
    const authority = lifecycle.terminalRunLivenessAuthority(won.run);
    if (!authority) throw new Error("Missing liveness receipt");
    vi.mocked(publishLiveEvent).mockClear();
    return { ...seeded, run: won.run, issue, lifecycle, authority, claim, controller };
  }

  async function expectNoRepair(run: Run) {
    expect(await db.select().from(issueThreadInteractions).where(eq(issueThreadInteractions.sourceRunId, run.id))).toEqual([]);
    expect(await db.select().from(activityLog).where(eq(activityLog.runId, run.id))).toEqual([]);
    const [stored] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(stored.livenessState).toBeNull();
    expect(publishLiveEvent).not.toHaveBeenCalled();
  }

  it("classifies without auth repair when the credential mutex cannot be acquired", async () => {
    const { run, lifecycle, authority } = await authFailureFixture();
    vi.mocked(withRunAuthFailureRepairLock).mockRejectedValueOnce(new Error("credential lock permission denied"));
    const updated = await lifecycle.classifyAndPersistRunLiveness(run, authority);
    expect(updated?.id).toBe(run.id);
    expect(updated?.livenessState).not.toBeNull();
    expect(await db.select().from(issueThreadInteractions).where(eq(issueThreadInteractions.sourceRunId, run.id))).toHaveLength(0);
  });

  it("does not replay settlement after entering the credential mutex callback", async () => {
    const { run, lifecycle, authority } = await authFailureFixture();
    const failure = new Error("lock release failed after settlement");
    vi.mocked(withRunAuthFailureRepairLock).mockImplementationOnce(async (_companyId, work) => {
      await work();
      throw failure;
    });
    const transaction = vi.spyOn(db, "transaction");
    try {
      await expect(lifecycle.classifyAndPersistRunLiveness(run, authority)).rejects.toBe(failure);
      expect(transaction).toHaveBeenCalledOnce();
    } finally {
      transaction.mockRestore();
    }
  });

  it("commits same-company winner liveness and authentication card together", async () => {
    const { run, lifecycle, authority } = await authFailureFixture();
    const updated = await lifecycle.classifyAndPersistRunLiveness(run, authority);
    expect(updated?.livenessState).toBe("blocked");
    const cards = await db.select().from(issueThreadInteractions).where(eq(issueThreadInteractions.sourceRunId, run.id));
    expect(cards).toHaveLength(1);
    expect(cards[0]).toMatchObject({ companyId: run.companyId, status: "pending", kind: "connection_intent" });
    expect(await db.select().from(activityLog).where(eq(activityLog.runId, run.id))).toHaveLength(1);
    expect(publishLiveEvent).toHaveBeenCalledWith(expect.objectContaining({ companyId: run.companyId, type: "activity.logged" }));
    expect(lifecycle.terminalRunLivenessAuthority({ ...run })).toBeNull();
  });

  it.each([
    { name: "owner", patch: { ownerToken: "replacement" } },
    { name: "fence", patch: { fence: 8 } },
    { name: "controller", patch: { controllerBootId: "00000000-0000-4000-8000-000000000002" } },
    { name: "terminal receipt", patch: { executionStatusDeliveryId: "00000000-0000-4000-8000-000000000003" } },
  ])("rejects a terminal winner after $name changes without repair or publication", async ({ patch }) => {
    const { run, lifecycle, authority } = await authFailureFixture();
    await db.update(heartbeatRuns).set(patch).where(eq(heartbeatRuns.id, run.id));
    expect(await lifecycle.classifyAndPersistRunLiveness(run, authority)).toBeNull();
    await expectNoRepair(run);
  });

  it("does not turn a failed row or a matching fence into executor authority", async () => {
    const { run, lifecycle, claim, controller } = await authFailureFixture();
    const [fresh] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(lifecycle.terminalRunLivenessAuthority(fresh)).toBeNull();
    for (const ownerToken of [claim.ownerToken, "forged-token"]) {
      expect(await lifecycle.classifyAndPersistRunLiveness(fresh, { kind: "legacy", executionClaim: { ...claim, ownerToken }, executionControllerBootId: controller })).toBeNull();
    }
    expect(await lifecycle.classifyAndPersistRunLiveness(fresh, { kind: "native" })).toBeNull();
    await expectNoRepair(run);
  });

  it("rejects cross-company reuse of a winning receipt", async () => {
    const { run, lifecycle, authority } = await authFailureFixture();
    expect(await lifecycle.classifyAndPersistRunLiveness({ ...run, companyId: randomUUID() }, authority)).toBeNull();
    await expectNoRepair(run);
  });

  it.each([
    { name: "unchanged", patch: {}, accepted: true },
    { name: "token", patch: { ownerToken: "replacement" }, accepted: false },
    { name: "fence", patch: { fence: 8 }, accepted: false },
    { name: "controller", patch: { controllerBootId: "00000000-0000-4000-8000-000000000002" }, accepted: false },
  ])("fences live legacy liveness with the original $name", async ({ patch, accepted }) => {
    const controller = "00000000-0000-4000-8000-000000000001";
    const claim = Object.freeze({ ownerToken: "live-owner", fence: 7 });
    const { run } = await fixture({ runtimeMode: "legacy", ...claim, controllerBootId: controller });
    if (!accepted) await db.update(heartbeatRuns).set(patch).where(eq(heartbeatRuns.id, run.id));
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const updated = await lifecycle.classifyAndPersistRunLiveness(run, { kind: "legacy", executionClaim: claim, executionControllerBootId: controller });
    if (accepted) expect(updated).toMatchObject({ id: run.id, livenessState: expect.any(String) });
    else {
      expect(updated).toBeNull();
      await expectNoRepair(run);
    }
  });

  it("keeps native classification independent of legacy claims", async () => {
    const { run } = await fixture({ status: "succeeded", runtimeMode: "native" });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const updated = await lifecycle.classifyAndPersistRunLiveness(run, { kind: "native" });
    expect(updated?.id).toBe(run.id);
    expect(updated?.livenessState).not.toBeNull();
    expect(updated?.ownerToken).toBeNull();
  });

  it.each(["standard", "planning"] as const)("retains persisted issue work mode in classification: %s", async workMode => {
    const { run } = await fixture({ status: "succeeded", runtimeMode: "native" });
    const [issue] = await db.insert(issues).values({ companyId: run.companyId, title: "Plan work", status: "in_progress", assigneeAgentId: run.agentId, workMode }).returning();
    const [bound] = await db.update(heartbeatRuns).set({ contextSnapshot: { issueId: issue.id } }).where(eq(heartbeatRuns.id, run.id)).returning();
    const updated = await createHeartbeatLifecycle(db, callbacks(db)).classifyAndPersistRunLiveness(bound, { kind: "native" }, { summary: "I will inspect the repo next." });
    expect(updated?.livenessState).toBe(workMode === "planning" ? "advanced" : "plan_only");
  });

  it("keeps operator Stop authority separate from executor authority", async () => {
    const { run } = await fixture({ runtimeMode: "legacy", status: "queued" });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const stopped = await lifecycle.cancelRunFromControlPlane(run, ["queued"], {});
    expect(stopped.updated).toBe(true);
    if (!stopped.updated || !stopped.run) throw new Error("Missing Stop winner");
    const authority = lifecycle.terminalRunLivenessAuthority(stopped.run);
    if (!authority) throw new Error("Missing Stop receipt");
    expect((await lifecycle.classifyAndPersistRunLiveness(stopped.run, authority))?.status).toBe("cancelled");
    await stopped.appendCancellationEvent?.({ eventType: "lifecycle", message: "Operator Stop" });
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toHaveLength(1);
  });

  it("rolls back auth repair and emits nothing when the final liveness write loses", async () => {
    const { run, lifecycle, authority } = await authFailureFixture();
    // A deterministic database veto occurs after repair inserts, without sleeps or mock SQL.
    await db.execute(sql`create or replace function reject_liveness_write() returns trigger language plpgsql as $$
      begin
        if new.liveness_state is not null then
          if new.liveness_state <> 'blocked' or not exists (
            select 1 from issue_thread_interactions where source_run_id = new.id and kind = 'connection_intent'
          ) then raise exception 'Expected authentication repair before liveness veto'; end if;
          return null;
        end if;
        return new;
      end $$`);
    await db.execute(sql`create trigger reject_liveness_write before update on heartbeat_runs for each row execute function reject_liveness_write()`);
    try {
      expect(await lifecycle.classifyAndPersistRunLiveness(run, authority)).toBeNull();
      await expectNoRepair(run);
    } finally {
      await db.execute(sql`drop trigger reject_liveness_write on heartbeat_runs`);
      await db.execute(sql`drop function reject_liveness_write()`);
    }
    // The accepted receipt remains usable after rollback; repair wasn't silently consumed.
    expect((await lifecycle.classifyAndPersistRunLiveness(run, authority))?.livenessState).toBe("blocked");
  });

  it("preserves sandbox process birth instead of inspecting a colliding controller PID", async () => {
    const { run } = await fixture();
    const remoteBirth = "2026-10-09T00:00:00.000Z";
    const persisted = await persistHeartbeatRunProcessMetadata(db, run.id, {
      pid: process.pid, processGroupId: null, startedAt: remoteBirth, targetKind: "remote",
    }, { kind: "native" });
    expect(persisted?.processStartedAt?.toISOString()).toBe(remoteBirth);
    expect(persisted?.processPid).toBe(process.pid);
  });

  it.each([
    { name: "owner", patch: { ownerToken: "replacement-owner" } },
    { name: "fence", patch: { fence: 8 } },
    { name: "controller", patch: { controllerBootId: "00000000-0000-4000-8000-000000000002" } },
    { name: "terminal status", patch: { status: "succeeded" } },
    { name: "native mode", patch: { runtimeMode: "native" } },
  ])("rejects stale legacy process metadata after changing $name", async ({ patch }) => {
    const { run } = await fixture({ runtimeMode: "legacy", ownerToken: "original-owner", fence: 7, controllerBootId: "00000000-0000-4000-8000-000000000001" });
    const authority = {
      kind: "legacy" as const,
      executionClaim: { ownerToken: "original-owner", fence: 7 },
      executionControllerBootId: "00000000-0000-4000-8000-000000000001",
    };
    const meta = { pid: 123456789, processGroupId: null, startedAt: "2026-10-09T00:00:00.000Z", targetKind: "remote" as const };
    expect(await persistHeartbeatRunProcessMetadata(db, run.id, meta, authority)).not.toBeNull();
    const deps = callbacks(db);
    const lifecycle = createHeartbeatLifecycle(db, deps);
    await lifecycle.recordCurrentHeartbeatRunRuntimeProgress(run, { phase: "config_sync", message: "original progress" }, null, authority);
    await db.update(heartbeatRuns).set(patch).where(eq(heartbeatRuns.id, run.id));
    vi.mocked(publishLiveEvent).mockClear();
    expect(await lifecycle.recordCurrentHeartbeatRunRuntimeProgress(run, { phase: "finalize", message: "stale progress" }, null, authority)).toBeNull();
    expect(getHeartbeatRunRuntimeStatus(run.id)?.message).toBe("original progress");
    await lifecycle.appendRunEvent(run, { eventType: "assistant.message", message: "stale event" }, authority.executionClaim, false, authority.executionControllerBootId);
    expect(await lifecycle.setRunStatusIfRunning(run.id, "failed", { error: "stale failure" }, undefined, authority.executionClaim, authority.executionControllerBootId))
      .toMatchObject({ updated: false });
    expect(deps.emitTerminalAgentTaskRun).not.toHaveBeenCalled();
    expect(publishLiveEvent).not.toHaveBeenCalled();
    expect(await persistHeartbeatRunProcessMetadata(db, run.id, { ...meta, pid: 987654321 }, authority)).toBeNull();
    const [stored] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(stored.processPid).toBe(meta.pid);
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toEqual([]);
  });

  it("allows only the captured terminal winner to append after ownership release", async () => {
    const controller = "00000000-0000-4000-8000-000000000001";
    const claim = { ownerToken: "terminal-owner", fence: 7 };
    const { run } = await fixture({ runtimeMode: "legacy", ...claim, controllerBootId: controller });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const won = await lifecycle.setRunStatusIfRunning(run.id, "succeeded", {}, undefined, claim, controller);
    expect(won.updated).toBe(true);
    if (!won.run) throw new Error("missing winning terminal row");
    await lifecycle.appendRunEvent(won.run, { eventType: "lifecycle", message: "winner" }, claim, true, controller);
    await lifecycle.appendRunEvent(won.run, { eventType: "lifecycle", message: "not a winner" }, claim, false, controller);
    await lifecycle.appendRunEvent(won.run, { eventType: "lifecycle", message: "missing controller" }, claim, true);
    await db.update(heartbeatRuns).set({ controllerBootId: "00000000-0000-4000-8000-000000000002" }).where(eq(heartbeatRuns.id, run.id));
    await lifecycle.appendRunEvent(won.run, { eventType: "lifecycle", message: "replaced controller" }, claim, true, controller);
    expect(await db.select({ message: heartbeatRunEvents.message, fence: heartbeatRunEvents.fence }).from(heartbeatRunEvents)
      .where(eq(heartbeatRunEvents.runId, run.id))).toEqual([{ message: "winner", fence: 7 }]);
  });

  it("fails closed for ownerless running legacy transitions", async () => {
    const { run } = await fixture({ runtimeMode: "legacy" });
    const deps = callbacks(db);
    const lifecycle = createHeartbeatLifecycle(db, deps);
    expect(await lifecycle.setRunStatusIfRunning(run.id, "failed")).toMatchObject({ updated: false });
    expect(await lifecycle.terminalizeRunOnLeaseRelease(run)).toBe(run);
    expect(deps.emitTerminalAgentTaskRun).not.toHaveBeenCalled();
    expect(publishLiveEvent).not.toHaveBeenCalled();
  });

  it.each(["clear", "held", "cancellation_fence_lost", "already_completed"] as const)(
    "keeps native control-plane cancellation independent of legacy ownership: %s",
    async (state) => {
      const { run } = await fixture(state === "held"
        ? { errorCode: "native_execution_ownership_unverified", nativePhase: "terminal_failure" }
        : {});
      if (state === "already_completed") {
        await db.update(heartbeatRuns).set({ status: "succeeded" }).where(eq(heartbeatRuns.id, run.id));
      }
      const deps = callbacks(db);
      const result = await createHeartbeatLifecycle(db, deps).cancelRunFromControlPlane(
        run, ["running"], { finishedAt: new Date() },
        state === "cancellation_fence_lost" ? sql`false` : undefined,
      );
      expect(result.updated).toBe(state === "clear");
      if (result.updated) await result.appendCancellationEvent({ eventType: "lifecycle", message: "operator stop" });
      else expect("appendCancellationEvent" in result).toBe(false);
      const events = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id));
      expect(events.map(event => event.message)).toEqual(state === "clear" ? ["operator stop"] : []);
      expect(deps.emitTerminalAgentTaskRun).toHaveBeenCalledTimes(state === "clear" ? 1 : 0);
    },
  );

  it.each(["receipt", "fence", "controller"] as const)("rejects a Stop event after its winning %s changes", async (changed) => {
    const { run } = await fixture({
      runtimeMode: "legacy", ownerToken: "stop-owner", fence: 7,
      controllerBootId: "00000000-0000-4000-8000-000000000001",
    });
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    const result = await lifecycle.cancelRunFromControlPlane(run, ["running"], {});
    expect(result.updated).toBe(true);
    if (!result.updated) throw new Error("Stop did not win");
    await db.update(heartbeatRuns).set(changed === "receipt"
      ? { executionStatusDeliveryId: "00000000-0000-4000-8000-000000000003" }
      : changed === "fence" ? { fence: 8 }
      : { controllerBootId: "00000000-0000-4000-8000-000000000002" },
    ).where(eq(heartbeatRuns.id, run.id));
    await result.appendCancellationEvent({ eventType: "lifecycle", message: "stale stop event" });
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toEqual([]);
  });

  it("does not upgrade a queued operator snapshot to a newly admitted execution", async () => {
    const { run } = await fixture({ status: "queued", runtimeMode: "legacy" });
    await db.update(heartbeatRuns).set({
      status: "running", ownerToken: "successor", fence: 1,
      controllerBootId: "00000000-0000-4000-8000-000000000001", startedAt: new Date(),
    }).where(eq(heartbeatRuns.id, run.id));
    const deps = callbacks(db);
    const result = await createHeartbeatLifecycle(db, deps).cancelRunFromControlPlane(
      run, ["queued", "running"], {},
    );
    expect(result).toMatchObject({ updated: false, run: { status: "running", ownerToken: "successor" } });
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id))).toEqual([]);
    expect(deps.emitTerminalAgentTaskRun).not.toHaveBeenCalled();
  });

  it("does not use explicit native authority to mutate a legacy process", async () => {
    const { run } = await fixture({ runtimeMode: "legacy", ownerToken: "original-owner", fence: 7 });
    expect(await persistHeartbeatRunProcessMetadata(db, run.id, {
      pid: 123456789, processGroupId: null, startedAt: "2026-10-09T00:00:00.000Z", targetKind: "remote",
    }, { kind: "native" })).toBeNull();
  });

  it("keeps one terminal outcome and reports only the winner of competing writes", async () => {
    const { run } = await fixture();
    const deps = callbacks(db);
    const lifecycle = createHeartbeatLifecycle(db, deps);
    const results = await Promise.all([
      lifecycle.setRunStatusIfRunning(run.id, "succeeded", { finishedAt: new Date() }),
      lifecycle.setRunStatusIfRunning(run.id, "failed", { errorCode: "late_failure", finishedAt: new Date() }),
    ]);
    const winner = results.find(result => result.updated)!;
    expect(results.filter(result => result.updated)).toHaveLength(1);
    const [persisted] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(persisted.status).toBe(winner.run?.status);
    expect(results.every(result => result.run?.status === persisted.status)).toBe(true);
    expect(deps.emitTerminalAgentTaskRun).toHaveBeenCalledTimes(1);
    expect(deps.emitTerminalAgentTaskRun).toHaveBeenCalledWith(winner.run, "running", undefined);
    expect(vi.mocked(publishLiveEvent).mock.calls.filter(([event]) => event.type === "heartbeat.run.status")).toHaveLength(1);
  });

  it("rechecks native ownership atomically before a terminal write", async () => {
    const { run } = await fixture({ errorCode: "native_execution_ownership_unverified", nativePhase: "terminal_failure" });
    const deps = callbacks(db);
    const result = await createHeartbeatLifecycle(db, deps).setRunStatusIfRunning(run.id, "failed");
    expect(result).toMatchObject({ updated: false, run: { status: "running", errorCode: run.errorCode } });
    expect(deps.emitTerminalAgentTaskRun).not.toHaveBeenCalled();
    expect(publishLiveEvent).not.toHaveBeenCalled();
  });

  it.each(["setRunStatus", "setRunStatusIfRunning"] as const)("preserves accounting metadata while %s enriches usage", async (operation) => {
    const { run } = await fixture({ usageJson: { accountingReceiptReady: false, usageSource: "per_run", inputTokens: 5 } });
    await createHeartbeatLifecycle(db, callbacks(db))[operation](run.id, "succeeded", { usageJson: { inputTokens: 8, outputTokens: 3 } });
    const [persisted] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(persisted.usageJson).toEqual({ accountingReceiptReady: false, usageSource: "per_run", inputTokens: 8, outputTokens: 3 });
  });

  it.each(["claimed", "cancelled"] as const)("settles a %s wake without reviving a revoked request", async (status) => {
    const { wake } = await fixture();
    await db.update(agentWakeupRequests).set({ status }).where(eq(agentWakeupRequests.id, wake.id));
    await createHeartbeatLifecycle(db, callbacks(db)).setWakeupStatus(wake.id, "completed", { reason: "settled" });
    const [persisted] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, wake.id));
    expect(persisted.status).toBe(status === "cancelled" ? "cancelled" : "completed");
    expect(persisted.reason).toBe(status === "cancelled" ? wake.reason : "settled");
  });

  it("publishes the persisted sequence for concurrent event writes", async () => {
    const { run } = await fixture();
    const lifecycle = createHeartbeatLifecycle(db, callbacks(db));
    await Promise.all(["first", "second"].map(message => lifecycle.appendRunEvent(run, { eventType: "lifecycle", message })));
    const rows = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id)).orderBy(heartbeatRunEvents.seq);
    expect(rows.map(row => row.seq)).toEqual([1, 2]);
    const published = vi.mocked(publishLiveEvent).mock.calls.map(([event]) => event).filter(event => event.type === "heartbeat.run.event");
    expect(published).toHaveLength(2);
    for (const row of rows) expect(published.find(event => event.payload?.seq === row.seq)?.payload).toMatchObject({ runId: run.id, message: row.message });
  });

  it("reads current redaction settings for each event and persists the published sanitized payload", async () => {
    const { run } = await fixture();
    const deps = callbacks(db);
    deps.getCurrentUserRedactionOptions.mockResolvedValueOnce({ enabled: true, userNames: ["alice"], homeDirs: ["/Users/alice"] });
    deps.getCurrentUserRedactionOptions.mockResolvedValueOnce({ enabled: true, userNames: ["bob"], homeDirs: ["/Users/bob"] });
    const lifecycle = createHeartbeatLifecycle(db, deps);
    for (const name of ["alice", "bob"]) await lifecycle.appendRunEvent(run, {
      eventType: "lifecycle", message: `Opening /Users/${name}/project`, payload: { path: `/Users/${name}/project`, token: "test-credential-material" },
    });
    const rows = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, run.id)).orderBy(heartbeatRunEvents.seq);
    expect(deps.getCurrentUserRedactionOptions).toHaveBeenCalledTimes(2);
    expect(rows).toHaveLength(2);
    const published = vi.mocked(publishLiveEvent).mock.calls.map(([event]) => event).filter(event => event.type === "heartbeat.run.event");
    expect(published).toHaveLength(2);
    for (const row of rows) {
      const text = JSON.stringify({ message: row.message, payload: row.payload });
      expect(text).not.toContain("alice");
      expect(text).not.toContain("bob");
      expect(text).not.toContain("test-credential-material");
      expect(published.find(event => event.payload?.seq === row.seq)?.payload).toMatchObject({ message: row.message, payload: row.payload });
    }
  });
});
