import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  activityLog,
  agents,
  agentWakeupRequests,
  companies,
  createDb,
  heartbeatRunEvents,
  heartbeatRunWatchdogDecisions,
  heartbeatRuns,
  issueComments,
  issueRecoveryActions,
  issueRelations,
  issues,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { persistActivity, publishActivity } from "../services/activity-log.js";
import { appendHeartbeatRunEvent } from "../services/heartbeat-run-events.js";
import { subscribeCompanyLiveEvents } from "../services/live-events.js";
import {
  ACTIVE_RUN_OUTPUT_CONTINUE_REARM_MS,
  ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS,
  ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS,
  recoveryService,
} from "../services/recovery/service.js";

vi.mock("../services/heartbeat-run-events.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../services/heartbeat-run-events.js")>();
  return { ...actual, appendHeartbeatRunEvent: vi.fn(actual.appendHeartbeatRunEvent) };
});

vi.mock("../services/activity-log.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../services/activity-log.js")>();
  return {
    ...actual,
    persistActivity: vi.fn(actual.persistActivity),
    publishActivity: vi.fn(actual.publishActivity),
  };
});

const activityLogActual = await vi.importActual<typeof import("../services/activity-log.js")>(
  "../services/activity-log.js",
);
const mockedAppendHeartbeatRunEvent = vi.mocked(appendHeartbeatRunEvent);
const mockedPersistActivity = vi.mocked(persistActivity);
const mockedPublishActivity = vi.mocked(publishActivity);

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres active-run output watchdog tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
  );
}

function errorHasPostgresCode(error: unknown, code: string): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < 4; depth += 1) {
    if (!current || typeof current !== "object") return false;
    const record = current as { code?: unknown; cause?: unknown };
    if (record.code === code) return true;
    current = record.cause;
  }
  return false;
}

async function truncateCompaniesWithDeadlockRetry(db: ReturnType<typeof createDb>) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await db.execute(sql.raw(`TRUNCATE TABLE "companies" CASCADE`));
      return;
    } catch (error) {
      if (!errorHasPostgresCode(error, "40P01") || attempt === 4) throw error;
      await new Promise((resolve) => setTimeout(resolve, 50 * (attempt + 1)));
    }
  }
}

describeEmbeddedPostgres("active-run output watchdog", () => {
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;
  let db: ReturnType<typeof createDb>;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-active-run-output-watchdog-");
    db = createDb(tempDb.connectionString);
  }, 30_000);

  afterEach(async () => {
    mockedAppendHeartbeatRunEvent.mockClear();
    mockedPersistActivity.mockReset();
    mockedPersistActivity.mockImplementation(activityLogActual.persistActivity);
    mockedPublishActivity.mockReset();
    mockedPublishActivity.mockImplementation(activityLogActual.publishActivity);
    await truncateCompaniesWithDeadlockRetry(db);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedRunningRun(opts: {
    now: Date;
    ageMs: number;
    withOutput?: boolean;
    sourceStatus?: "in_progress" | "blocked" | "done" | "cancelled";
    sourceOriginKind?: string;
    sameRunTerminalEvidence?: boolean;
  }) {
    const companyId = randomUUID();
    const managerId = randomUUID();
    const coderId = randomUUID();
    const issueId = randomUUID();
    const runId = randomUUID();
    const issuePrefix = `W${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`;
    const startedAt = new Date(opts.now.getTime() - opts.ageMs);
    const lastOutputAt = opts.withOutput ? new Date(opts.now.getTime() - 5 * 60 * 1000) : null;
    const sourceStatus = opts.sourceStatus ?? "in_progress";
    const terminalEvidenceAt = new Date(startedAt.getTime() + 10 * 60 * 1000);

    await db.insert(companies).values({
      id: companyId,
      name: "Watchdog Co",
      issuePrefix,
      defaultResponsibleUserId: "responsible-user",
      requireBoardApprovalForNewAgents: false,
    });
    await db.insert(agents).values([
      {
        id: managerId,
        companyId,
        name: "CTO",
        role: "cto",
        status: "idle",
        adapterType: "codex_local",
        adapterConfig: {},
        runtimeConfig: {},
        permissions: {},
      },
      {
        id: coderId,
        companyId,
        name: "Coder",
        role: "engineer",
        status: "running",
        reportsTo: managerId,
        adapterType: "codex_local",
        adapterConfig: {},
        runtimeConfig: {},
        permissions: {},
      },
    ]);
    await db.insert(issues).values({
      id: issueId,
      companyId,
      title: "Long running implementation",
      status: sourceStatus,
      priority: "medium",
      assigneeAgentId: coderId,
      issueNumber: 1,
      identifier: `${issuePrefix}-1`,
      originKind: opts.sourceOriginKind ?? "manual",
      completedAt: sourceStatus === "done" ? terminalEvidenceAt : null,
      cancelledAt: sourceStatus === "cancelled" ? terminalEvidenceAt : null,
      updatedAt: startedAt,
      createdAt: startedAt,
    });
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId: coderId,
      status: "running",
      invocationSource: "assignment",
      triggerDetail: "system",
      startedAt,
      processStartedAt: startedAt,
      lastOutputAt,
      lastOutputSeq: opts.withOutput ? 3 : 0,
      lastOutputStream: opts.withOutput ? "stdout" : null,
      contextSnapshot: { issueId },
      logBytes: 0,
    });
    await db.update(issues).set({ executionRunId: runId }).where(eq(issues.id, issueId));

    if (opts.sameRunTerminalEvidence) {
      await db.insert(activityLog).values({
        companyId,
        actorType: "agent",
        actorId: coderId,
        agentId: coderId,
        runId,
        action: "issue.updated",
        entityType: "issue",
        entityId: issueId,
        details: {
          identifier: `${issuePrefix}-1`,
          status: sourceStatus,
          _previous: { status: "in_progress" },
        },
        createdAt: terminalEvidenceAt,
      });
    }

    return { companyId, managerId, coderId, issueId, runId, issuePrefix, startedAt };
  }

  async function seedCoderOwnedManagerDecision(now: Date) {
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
    });
    const evaluationIssueId = randomUUID();
    const managerRunId = randomUUID();
    await db.insert(issues).values({
      id: evaluationIssueId,
      companyId: seeded.companyId,
      title: "Manager review of a coder-owned run",
      status: "todo",
      priority: "high",
      assigneeAgentId: seeded.managerId,
      issueNumber: 2,
      identifier: `${seeded.issuePrefix}-2`,
      originKind: "stale_active_run_evaluation",
      originId: seeded.runId,
      originRunId: seeded.runId,
      originFingerprint: `stale_active_run:${seeded.companyId}:${seeded.runId}`,
    });
    await db.insert(heartbeatRuns).values({
      id: managerRunId,
      companyId: seeded.companyId,
      agentId: seeded.managerId,
      status: "succeeded",
      invocationSource: "assignment",
      triggerDetail: "system",
      startedAt: now,
      finishedAt: now,
      processStartedAt: now,
      lastOutputAt: now,
      lastOutputSeq: 1,
      lastOutputStream: "stdout",
      contextSnapshot: { issueId: evaluationIssueId },
      logBytes: 0,
    });
    return { ...seeded, evaluationIssueId, managerRunId };
  }

  function createRecovery() {
    const enqueueWakeup = vi.fn();
    return { enqueueWakeup, recovery: recoveryService(db, { enqueueWakeup }) };
  }

  async function buildSummary(runId: string, now: Date) {
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
    if (!run) throw new Error(`Missing test run ${runId}`);
    return recoveryService(db, { enqueueWakeup: vi.fn() }).buildRunOutputSilence(run, now);
  }

  async function expectNoReviewArtifacts(input: {
    companyId: string;
    issueId: string;
    coderId: string;
    managerId: string;
  }) {
    const [evaluations, comments, relations, actions, wakes, source, coder, manager] = await Promise.all([
      db.select().from(issues).where(and(
        eq(issues.companyId, input.companyId),
        eq(issues.originKind, "stale_active_run_evaluation"),
      )),
      db.select().from(issueComments).where(eq(issueComments.issueId, input.issueId)),
      db.select().from(issueRelations).where(eq(issueRelations.companyId, input.companyId)),
      db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.companyId, input.companyId)),
      db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, input.companyId)),
      db.select().from(issues).where(eq(issues.id, input.issueId)).then((rows) => rows[0]),
      db.select().from(agents).where(eq(agents.id, input.coderId)).then((rows) => rows[0]),
      db.select().from(agents).where(eq(agents.id, input.managerId)).then((rows) => rows[0]),
    ]);

    expect(evaluations).toHaveLength(0);
    expect(comments).toHaveLength(0);
    expect(relations).toHaveLength(0);
    expect(actions).toHaveLength(0);
    expect(wakes).toHaveLength(0);
    expect(source?.assigneeAgentId).toBe(input.coderId);
    expect(coder?.status).toBe("running");
    expect(manager?.status).toBe("idle");
  }

  it.each(["stale_active_run_evaluation", "issue_productivity_review"])("keeps blocked and %s sources artifact-free", async (originKind) => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const blocked = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
      sourceStatus: "blocked",
    });
    const recursive = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
      sourceOriginKind: originKind,
    });
    const { enqueueWakeup, recovery } = createRecovery();

    await expect(recovery.scanSilentActiveRuns({ now, companyId: blocked.companyId }))
      .resolves.toMatchObject({ created: 0, skipped: 1 });
    await expect(recovery.scanSilentActiveRuns({ now, companyId: recursive.companyId }))
      .resolves.toMatchObject({ created: 0, skipped: 1 });
    await expectNoReviewArtifacts(blocked);

    const recursiveIssues = await db.select().from(issues).where(eq(issues.companyId, recursive.companyId));
    expect(recursiveIssues).toHaveLength(1);
    expect(recursiveIssues[0]?.id).toBe(recursive.issueId);
    expect(await db.select().from(issueComments).where(eq(issueComments.issueId, recursive.issueId))).toHaveLength(0);
    expect(await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.companyId, recursive.companyId))).toHaveLength(0);
    expect(await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, recursive.companyId))).toHaveLength(0);
    expect(enqueueWakeup).not.toHaveBeenCalled();
  });

  it("scopes candidates, readers, and writers to one company", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const companyA = await seedRunningRun({ now, ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000 });
    const companyB = await seedRunningRun({ now, ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000 });
    const healthyRunId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: healthyRunId,
      companyId: companyA.companyId,
      agentId: companyA.coderId,
      status: "running",
      invocationSource: "assignment",
      triggerDetail: "system",
      startedAt: now,
      processStartedAt: now,
      lastOutputAt: now,
      lastOutputSeq: 1,
      lastOutputStream: "stdout",
      contextSnapshot: {},
      logBytes: 0,
    });
    const { recovery } = createRecovery();

    const result = await recovery.scanSilentActiveRuns({ now, companyId: companyA.companyId });

    // The company filter, the SQL timestamp expression, and the healthy-run
    // exclusion together keep the scan to the one silent run in company A.
    expect(result.scanned).toBe(1);

    const evaluationIssueIdInCompanyB = randomUUID();
    await db.insert(issues).values({
      id: evaluationIssueIdInCompanyB,
      companyId: companyB.companyId,
      title: "Evaluation issue in the other company",
      status: "todo",
      priority: "medium",
      assigneeAgentId: companyB.managerId,
      issueNumber: 2,
      identifier: `${companyB.issuePrefix}-2`,
      originKind: "stale_active_run_evaluation",
      originId: companyB.runId,
      originRunId: companyB.runId,
      originFingerprint: `stale_active_run:${companyB.companyId}:${companyB.runId}`,
    });

    await expect(recovery.recordWatchdogDecision({
      runId: companyA.runId,
      actor: { type: "agent", agentId: companyA.managerId },
      decision: "continue",
      evaluationIssueId: evaluationIssueIdInCompanyB,
      reason: "Cross-company evaluation issue must be rejected",
      now,
    })).rejects.toMatchObject({ status: 404 });

    await expect(recovery.recordWatchdogDecision({
      runId: companyA.runId,
      actor: { type: "board" },
      decision: "continue",
      reason: "Cross-company createdByRunId must be rejected",
      createdByRunId: companyB.runId,
      now,
    })).rejects.toMatchObject({ status: 403 });
  });

  it("leaves no partial state when the fold transaction fails", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
      sourceStatus: "done",
      sameRunTerminalEvidence: true,
    });
    const evaluationIssueId = randomUUID();
    await db.insert(issues).values({
      id: evaluationIssueId,
      companyId: seeded.companyId,
      title: "Existing stale evaluation",
      status: "todo",
      priority: "high",
      assigneeAgentId: seeded.managerId,
      issueNumber: 2,
      identifier: `${seeded.issuePrefix}-2`,
      originKind: "stale_active_run_evaluation",
      originId: seeded.runId,
      originRunId: seeded.runId,
      originFingerprint: `stale_active_run:${seeded.companyId}:${seeded.runId}`,
    });
    await db.insert(issueRecoveryActions).values({
      companyId: seeded.companyId,
      sourceIssueId: seeded.issueId,
      recoveryIssueId: evaluationIssueId,
      kind: "active_run_watchdog",
      status: "active",
      ownerType: "agent",
      ownerAgentId: seeded.managerId,
      cause: "active_run_watchdog",
      fingerprint: `active-run-watchdog:${seeded.companyId}:${seeded.runId}:${seeded.issueId}`,
      evidence: { runId: seeded.runId },
      nextAction: "Review stale active run",
    });
    const { recovery } = createRecovery();
    mockedAppendHeartbeatRunEvent.mockRejectedValueOnce(new Error("injected fold transaction fault"));

    await expect(recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId }))
      .rejects.toThrow("injected fold transaction fault");

    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, seeded.runId));
    expect(run?.status).toBe("running");
    expect(await db.select().from(heartbeatRunWatchdogDecisions).where(eq(
      heartbeatRunWatchdogDecisions.runId,
      seeded.runId,
    ))).toHaveLength(0);
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, seeded.runId))).toHaveLength(0);
    // The seed itself planted one activity-log row as the fake same-run
    // terminal evidence; the fold must not add a second one.
    expect(await db.select().from(activityLog).where(eq(activityLog.runId, seeded.runId))).toHaveLength(1);
    const [source] = await db.select().from(issues).where(eq(issues.id, seeded.issueId));
    const [evaluation] = await db.select().from(issues).where(eq(issues.id, evaluationIssueId));
    const [action] = await db
      .select()
      .from(issueRecoveryActions)
      .where(eq(issueRecoveryActions.sourceIssueId, seeded.issueId));
    expect(source?.executionRunId).toBe(seeded.runId);
    expect(evaluation?.status).toBe("todo");
    expect(await db.select().from(issueComments).where(eq(issueComments.issueId, evaluationIssueId))).toHaveLength(0);
    expect(action).toMatchObject({ status: "active", outcome: null });
    const [agent] = await db.select().from(agents).where(eq(agents.id, seeded.coderId));
    expect(agent?.status).toBe("running");
  });


  it("folds a terminal source with same-run evidence without creating review work", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
      sourceStatus: "done",
      sameRunTerminalEvidence: true,
    });
    const { enqueueWakeup, recovery } = createRecovery();

    const result = await recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId });
    expect(result).toMatchObject({ created: 0, folded: 1, skipped: 0 });
    expect(enqueueWakeup).not.toHaveBeenCalled();

    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, seeded.runId));
    const [source] = await db.select().from(issues).where(eq(issues.id, seeded.issueId));
    const [agent] = await db.select().from(agents).where(eq(agents.id, seeded.coderId));
    expect(run?.status).toBe("succeeded");
    expect(run?.resultJson).toMatchObject({
      sourceResolvedWatchdogFold: {
        sourceIssueId: seeded.issueId,
        sourceIssueStatus: "done",
        evaluationIssueId: null,
        cleanup: { outcome: "no_process_metadata" },
      },
    });
    expect(source?.executionRunId).toBeNull();
    expect(agent?.status).toBe("idle");
    expect(await db.select().from(issues).where(and(
      eq(issues.companyId, seeded.companyId),
      eq(issues.originKind, "stale_active_run_evaluation"),
    ))).toHaveLength(0);
    expect(await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.companyId, seeded.companyId))).toHaveLength(0);
  });

  it("does not fold or create review work for a terminal source without same-run evidence", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
      sourceStatus: "done",
    });
    const { enqueueWakeup, recovery } = createRecovery();

    await expect(recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId }))
      .resolves.toMatchObject({ created: 0, folded: 0, skipped: 1 });
    const [run] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, seeded.runId));
    expect(run?.status).toBe("running");
    expect(enqueueWakeup).not.toHaveBeenCalled();
    expect(await db.select().from(issues).where(and(
      eq(issues.companyId, seeded.companyId),
      eq(issues.originKind, "stale_active_run_evaluation"),
    ))).toHaveLength(0);
  });

  it("folds existing legacy evaluation and recovery rows idempotently", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
      sourceStatus: "done",
      sameRunTerminalEvidence: true,
    });
    const evaluationIssueId = randomUUID();
    await db.insert(issues).values({
      id: evaluationIssueId,
      companyId: seeded.companyId,
      title: "Existing stale evaluation",
      status: "todo",
      priority: "high",
      assigneeAgentId: seeded.managerId,
      issueNumber: 2,
      identifier: `${seeded.issuePrefix}-2`,
      originKind: "stale_active_run_evaluation",
      originId: seeded.runId,
      originRunId: seeded.runId,
      originFingerprint: `stale_active_run:${seeded.companyId}:${seeded.runId}`,
    });
    await db.insert(issueRelations).values({
      companyId: seeded.companyId,
      issueId: evaluationIssueId,
      relatedIssueId: seeded.issueId,
      type: "blocks",
    });
    await db.insert(issueRecoveryActions).values({
      companyId: seeded.companyId,
      sourceIssueId: seeded.issueId,
      recoveryIssueId: evaluationIssueId,
      kind: "active_run_watchdog",
      status: "active",
      ownerType: "agent",
      ownerAgentId: seeded.managerId,
      cause: "active_run_watchdog",
      fingerprint: `active-run-watchdog:${seeded.companyId}:${seeded.runId}:${seeded.issueId}`,
      evidence: { runId: seeded.runId },
      nextAction: "Review stale active run",
    });
    const { recovery } = createRecovery();

    await expect(recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId }))
      .resolves.toMatchObject({ created: 0, folded: 1 });
    await expect(recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId }))
      .resolves.toMatchObject({ scanned: 0, created: 0, folded: 0 });
    const [evaluation] = await db.select().from(issues).where(eq(issues.id, evaluationIssueId));
    const [action] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.sourceIssueId, seeded.issueId));
    expect(evaluation?.status).toBe("done");
    expect(action).toMatchObject({ status: "resolved", outcome: "false_positive" });
    expect(await db.select().from(heartbeatRunWatchdogDecisions).where(eq(
      heartbeatRunWatchdogDecisions.runId,
      seeded.runId,
    ))).toHaveLength(1);
    expect(await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, seeded.runId))).toHaveLength(1);
  });

  it("keeps open legacy evaluations readable without refreshing or reprioritizing them", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
    });
    const evaluationIssueId = randomUUID();
    const evaluationUpdatedAt = new Date("2026-04-20T12:00:00.000Z");
    await db.insert(issues).values({
      id: evaluationIssueId,
      companyId: seeded.companyId,
      title: "Legacy silent-run evaluation",
      status: "todo",
      priority: "medium",
      assigneeAgentId: seeded.managerId,
      issueNumber: 2,
      identifier: `${seeded.issuePrefix}-2`,
      originKind: "stale_active_run_evaluation",
      originId: seeded.runId,
      originRunId: seeded.runId,
      originFingerprint: `stale_active_run:${seeded.companyId}:${seeded.runId}`,
      updatedAt: evaluationUpdatedAt,
    });
    const { enqueueWakeup, recovery } = createRecovery();

    await expect(buildSummary(seeded.runId, now)).resolves.toMatchObject({
      level: "critical",
      evaluationIssueId,
      evaluationIssueIdentifier: `${seeded.issuePrefix}-2`,
      evaluationIssueAssigneeAgentId: seeded.managerId,
    });
    await expect(recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId }))
      .resolves.toMatchObject({ created: 0, existing: 1, escalated: 0 });
    const [evaluation] = await db.select().from(issues).where(eq(issues.id, evaluationIssueId));
    expect(evaluation).toMatchObject({ status: "todo", priority: "medium", assigneeAgentId: seeded.managerId });
    expect(evaluation?.updatedAt.toISOString()).toBe(evaluationUpdatedAt.toISOString());
    expect(await db.select().from(issueComments).where(eq(issueComments.issueId, evaluationIssueId))).toHaveLength(0);
    expect(await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, seeded.companyId))).toHaveLength(0);
    expect(enqueueWakeup).not.toHaveBeenCalled();

    await expect(recovery.recordWatchdogDecision({
      runId: seeded.runId,
      actor: { type: "agent", agentId: seeded.managerId },
      decision: "continue",
      evaluationIssueId,
      reason: "Resolve through the legacy review",
      now,
    })).resolves.toMatchObject({ evaluationIssueId, createdByAgentId: seeded.managerId });
    const [watchedAfterDecision] = await db
      .select({ agentId: heartbeatRuns.agentId })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, seeded.runId));
    expect(watchedAfterDecision?.agentId).toBe(seeded.coderId);
    await expect(recovery.recordWatchdogDecision({
      runId: seeded.runId,
      actor: { type: "agent", agentId: randomUUID() },
      decision: "continue",
      evaluationIssueId,
      reason: "Not assigned",
      now,
    })).rejects.toMatchObject({ status: 403 });
  });

  it("records one watched-run activity when the recovery manager owns a different run", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedCoderOwnedManagerDecision(now);
    const { recovery } = createRecovery();
    const published: Array<{ type: string; payload: Record<string, unknown> }> = [];
    const unsubscribe = subscribeCompanyLiveEvents(seeded.companyId, (event) => {
      published.push({ type: event.type, payload: event.payload ?? {} });
    });

    try {
      await expect(recovery.recordWatchdogDecision({
        runId: seeded.runId,
        actor: { type: "agent", agentId: seeded.managerId, runId: seeded.managerRunId },
        decision: "continue",
        evaluationIssueId: seeded.evaluationIssueId,
        reason: "Coder is still making progress",
        now,
      })).resolves.toMatchObject({
        runId: seeded.runId,
        evaluationIssueId: seeded.evaluationIssueId,
        decision: "continue",
        createdByAgentId: seeded.managerId,
        createdByUserId: null,
        createdByRunId: seeded.managerRunId,
      });
    } finally {
      unsubscribe();
    }

    const [watched] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, seeded.runId));
    const [creator] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, seeded.managerRunId));
    expect(watched?.agentId).toBe(seeded.coderId);
    expect(creator?.agentId).toBe(seeded.managerId);
    expect(seeded.managerRunId).not.toBe(seeded.runId);

    const decisions = await db.select().from(heartbeatRunWatchdogDecisions).where(eq(
      heartbeatRunWatchdogDecisions.runId,
      seeded.runId,
    ));
    expect(decisions).toHaveLength(1);
    expect(decisions[0]).toMatchObject({
      companyId: seeded.companyId,
      runId: seeded.runId,
      decision: "continue",
      createdByAgentId: seeded.managerId,
      createdByUserId: null,
      createdByRunId: seeded.managerRunId,
      snoozedUntil: new Date(now.getTime() + ACTIVE_RUN_OUTPUT_CONTINUE_REARM_MS),
    });

    const activities = await db.select().from(activityLog).where(eq(activityLog.companyId, seeded.companyId));
    expect(activities).toHaveLength(1);
    expect(activities[0]).toMatchObject({
      companyId: seeded.companyId,
      actorType: "system",
      actorId: "system",
      agentId: null,
      action: "heartbeat.watchdog_decision_recorded",
      entityType: "heartbeat_run",
      entityId: seeded.runId,
      runId: seeded.runId,
      details: {
        source: "recovery.record_watchdog_decision",
        decision: "continue",
        evaluationIssueId: seeded.evaluationIssueId,
        reason: "Coder is still making progress",
        recoveryActorType: "agent",
        createdByAgentId: seeded.managerId,
        createdByUserId: null,
        createdByRunId: seeded.managerRunId,
        snoozedUntil: new Date(now.getTime() + ACTIVE_RUN_OUTPUT_CONTINUE_REARM_MS).toISOString(),
      },
    });
    expect(published).toEqual([
      expect.objectContaining({
        type: "activity.logged",
        payload: expect.objectContaining({
          actorType: "system",
          actorId: "system",
          action: "heartbeat.watchdog_decision_recorded",
          entityType: "heartbeat_run",
          entityId: seeded.runId,
          runId: seeded.runId,
        }),
      }),
    ]);
  });

  it("rolls back the decision and skips publication when activity insertion fails", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedCoderOwnedManagerDecision(now);
    const { recovery } = createRecovery();
    const published: string[] = [];
    const unsubscribe = subscribeCompanyLiveEvents(seeded.companyId, (event) => {
      published.push(event.type);
    });
    mockedPersistActivity.mockImplementationOnce(async (executor, input) => {
      await activityLogActual.persistActivity(executor, input);
      throw new Error("injected activity insert fault");
    });

    try {
      await expect(recovery.recordWatchdogDecision({
        runId: seeded.runId,
        actor: { type: "agent", agentId: seeded.managerId, runId: seeded.managerRunId },
        decision: "continue",
        evaluationIssueId: seeded.evaluationIssueId,
        reason: "Coder is still making progress",
        now,
      })).rejects.toThrow("injected activity insert fault");
    } finally {
      unsubscribe();
    }

    expect(published).toEqual([]);
    expect(mockedPublishActivity).not.toHaveBeenCalled();
    const [watched] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, seeded.runId));
    expect(watched?.agentId).toBe(seeded.coderId);
    expect(await db.select().from(heartbeatRunWatchdogDecisions).where(eq(
      heartbeatRunWatchdogDecisions.companyId,
      seeded.companyId,
    ))).toHaveLength(0);
    expect(await db.select().from(activityLog).where(eq(activityLog.companyId, seeded.companyId))).toHaveLength(0);
  });

  it("returns the committed decision when activity publication throws", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedCoderOwnedManagerDecision(now);
    const { recovery } = createRecovery();
    mockedPublishActivity.mockImplementationOnce(() => {
      throw new Error("injected publication fault");
    });

    await expect(recovery.recordWatchdogDecision({
      runId: seeded.runId,
      actor: { type: "agent", agentId: seeded.managerId, runId: seeded.managerRunId },
      decision: "continue",
      evaluationIssueId: seeded.evaluationIssueId,
      reason: "Coder is still making progress",
      now,
    })).resolves.toMatchObject({
      runId: seeded.runId,
      createdByAgentId: seeded.managerId,
      createdByRunId: seeded.managerRunId,
    });

    expect(await db.select().from(heartbeatRunWatchdogDecisions).where(eq(
      heartbeatRunWatchdogDecisions.runId,
      seeded.runId,
    ))).toHaveLength(1);
    const activities = await db.select().from(activityLog).where(eq(activityLog.companyId, seeded.companyId));
    expect(activities).toHaveLength(1);
    expect(activities[0]).toMatchObject({
      runId: seeded.runId,
      entityId: seeded.runId,
      actorType: "system",
    });
  });

  it("does not recreate or auto-dismiss a closed legacy evaluation", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000,
    });
    const evaluationIssueId = randomUUID();
    await db.insert(issues).values({
      id: evaluationIssueId,
      companyId: seeded.companyId,
      title: "Closed legacy evaluation",
      status: "done",
      priority: "medium",
      assigneeAgentId: seeded.managerId,
      issueNumber: 2,
      identifier: `${seeded.issuePrefix}-2`,
      originKind: "stale_active_run_evaluation",
      originId: seeded.runId,
      originRunId: seeded.runId,
      originFingerprint: `stale_active_run:${seeded.companyId}:${seeded.runId}`,
    });
    const { recovery } = createRecovery();

    await expect(recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId }))
      .resolves.toMatchObject({ created: 0, existing: 0, skipped: 1 });
    expect(await db.select().from(issues).where(eq(issues.companyId, seeded.companyId))).toHaveLength(2);
    expect(await db.select().from(heartbeatRunWatchdogDecisions).where(eq(
      heartbeatRunWatchdogDecisions.runId,
      seeded.runId,
    ))).toHaveLength(0);
  });

  it("ignores healthy runs that produced recent output", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const seeded = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000,
      withOutput: true,
    });
    const { recovery } = createRecovery();

    await expect(buildSummary(seeded.runId, now)).resolves.toMatchObject({ level: "ok" });
    await expect(recovery.scanSilentActiveRuns({ now, companyId: seeded.companyId }))
      .resolves.toMatchObject({ scanned: 0, created: 0 });
  });

  it("buildRunOutputSilenceMap matches per-run buildRunOutputSilence results for a batch of runs", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const suspicious = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000,
    });
    const { companyId, coderId, issuePrefix } = suspicious;
    const recovery = recoveryService(db, { enqueueWakeup: vi.fn() });

    // Second run in the same company: aged past the critical threshold, later snoozed
    // so the batch exercises the "snoozed" branch alongside "suspicious" and "ok".
    const criticalIssueId = randomUUID();
    const criticalRunId = randomUUID();
    const criticalStartedAt = new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_CRITICAL_THRESHOLD_MS + 60_000));
    await db.insert(issues).values({
      id: criticalIssueId,
      companyId,
      title: "Second long running implementation",
      status: "in_progress",
      priority: "medium",
      assigneeAgentId: coderId,
      issueNumber: 2,
      identifier: `${issuePrefix}-2`,
      originKind: "manual",
      updatedAt: criticalStartedAt,
      createdAt: criticalStartedAt,
    });
    await db.insert(heartbeatRuns).values({
      id: criticalRunId,
      companyId,
      agentId: coderId,
      status: "running",
      invocationSource: "assignment",
      triggerDetail: "system",
      startedAt: criticalStartedAt,
      processStartedAt: criticalStartedAt,
      lastOutputAt: null,
      lastOutputSeq: 0,
      lastOutputStream: null,
      contextSnapshot: { issueId: criticalIssueId },
      logBytes: 0,
    });
    await db.update(issues).set({ executionRunId: criticalRunId }).where(eq(issues.id, criticalIssueId));

    // Third run in the same company: recent output, so it stays "ok".
    const okIssueId = randomUUID();
    const okRunId = randomUUID();
    const okStartedAt = new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000));
    const okLastOutputAt = new Date(now.getTime() - 5 * 60 * 1000);
    await db.insert(issues).values({
      id: okIssueId,
      companyId,
      title: "Third noisy implementation",
      status: "in_progress",
      priority: "medium",
      assigneeAgentId: coderId,
      issueNumber: 3,
      identifier: `${issuePrefix}-3`,
      originKind: "manual",
      updatedAt: okStartedAt,
      createdAt: okStartedAt,
    });
    await db.insert(heartbeatRuns).values({
      id: okRunId,
      companyId,
      agentId: coderId,
      status: "running",
      invocationSource: "assignment",
      triggerDetail: "system",
      startedAt: okStartedAt,
      processStartedAt: okStartedAt,
      lastOutputAt: okLastOutputAt,
      lastOutputSeq: 3,
      lastOutputStream: "stdout",
      contextSnapshot: { issueId: okIssueId },
      logBytes: 0,
    });
    await db.update(issues).set({ executionRunId: okRunId }).where(eq(issues.id, okIssueId));

    // Upstream's scan no longer auto-creates evaluation issues, so snooze the
    // critical run as the board (no evaluation issue required) to exercise
    // the "snoozed" branch.
    const snoozedUntil = new Date(now.getTime() + 60 * 60 * 1000);
    await recovery.recordWatchdogDecision({
      runId: criticalRunId,
      actor: { type: "board" },
      decision: "snooze",
      reason: "Investigating separately",
      snoozedUntil,
      now,
    });

    const runs = [
      {
        id: suspicious.runId,
        companyId,
        status: "running" as const,
        lastOutputAt: null,
        lastOutputSeq: 0,
        lastOutputStream: null,
        processStartedAt: new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000)),
        startedAt: new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000)),
        createdAt: new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000)),
      },
      {
        id: criticalRunId,
        companyId,
        status: "running" as const,
        lastOutputAt: null,
        lastOutputSeq: 0,
        lastOutputStream: null,
        processStartedAt: criticalStartedAt,
        startedAt: criticalStartedAt,
        createdAt: criticalStartedAt,
      },
      {
        id: okRunId,
        companyId,
        status: "running" as const,
        lastOutputAt: okLastOutputAt,
        lastOutputSeq: 3,
        lastOutputStream: "stdout" as const,
        processStartedAt: okStartedAt,
        startedAt: okStartedAt,
        createdAt: okStartedAt,
      },
    ];

    const batched = await recovery.buildRunOutputSilenceMap(companyId, runs, now);
    expect(batched.size).toBe(3);

    for (const run of runs) {
      const singular = await recovery.buildRunOutputSilence(run, now);
      expect(batched.get(run.id)).toEqual(singular);
    }

    expect(batched.get(suspicious.runId)?.level).toBe("suspicious");
    expect(batched.get(criticalRunId)?.level).toBe("snoozed");
    expect(batched.get(okRunId)?.level).toBe("ok");
  });

  it("resolves the same evaluation via buildRunOutputSilence and buildRunOutputSilenceMap when a run has multiple open stale-run evaluation issues", async () => {
    const now = new Date("2026-04-22T20:00:00.000Z");
    const { companyId, coderId, runId, issuePrefix } = await seedRunningRun({
      now,
      ageMs: ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000,
    });
    const recovery = recoveryService(db, { enqueueWakeup: vi.fn() });

    // `issues_active_stale_run_evaluation_uq` normally guarantees at most one open
    // evaluation per run. Drop it for this test only, to reproduce the multi-row
    // state the single-run and batch lookups must resolve identically regardless
    // of scan order.
    await db.execute(sql.raw(`DROP INDEX IF EXISTS issues_active_stale_run_evaluation_uq`));

    const olderEvaluationId = randomUUID();
    const newerEvaluationId = randomUUID();
    try {
      const olderUpdatedAt = new Date(now.getTime() - 30 * 60 * 1000);
      await db.insert(issues).values({
        id: olderEvaluationId,
        companyId,
        title: "Review silent active run (older)",
        status: "todo",
        priority: "medium",
        assigneeAgentId: coderId,
        issueNumber: 90,
        identifier: `${issuePrefix}-90`,
        originKind: "stale_active_run_evaluation",
        originId: runId,
        updatedAt: olderUpdatedAt,
        createdAt: olderUpdatedAt,
      });

      const newerUpdatedAt = new Date(now.getTime() - 5 * 60 * 1000);
      await db.insert(issues).values({
        id: newerEvaluationId,
        companyId,
        title: "Review silent active run (newer)",
        status: "in_progress",
        priority: "high",
        assigneeAgentId: coderId,
        issueNumber: 91,
        identifier: `${issuePrefix}-91`,
        originKind: "stale_active_run_evaluation",
        originId: runId,
        updatedAt: newerUpdatedAt,
        createdAt: newerUpdatedAt,
      });

      const run = {
        id: runId,
        companyId,
        status: "running" as const,
        lastOutputAt: null,
        lastOutputSeq: 0,
        lastOutputStream: null,
        processStartedAt: new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000)),
        startedAt: new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000)),
        createdAt: new Date(now.getTime() - (ACTIVE_RUN_OUTPUT_SUSPICION_THRESHOLD_MS + 60_000)),
      };

      const singular = await recovery.buildRunOutputSilence(run, now);
      const batched = await recovery.buildRunOutputSilenceMap(companyId, [run], now);

      expect(singular.evaluationIssueId).toBe(newerEvaluationId);
      expect(batched.get(run.id)).toEqual(singular);
    } finally {
      // This test intentionally left two open evaluation issues for the same
      // run, which the dropped index normally forbids. Clear them before
      // recreating the index, or CREATE UNIQUE INDEX fails on the existing
      // violation.
      await db.delete(issues).where(eq(issues.id, olderEvaluationId));
      await db.delete(issues).where(eq(issues.id, newerEvaluationId));
      // Restore the partial unique index so later tests in this file (and any
      // appended after this one) still run under the real schema constraint.
      await db.execute(
        sql.raw(`
          CREATE UNIQUE INDEX IF NOT EXISTS issues_active_stale_run_evaluation_uq
            ON issues USING btree (company_id, origin_kind, origin_id)
            WHERE origin_kind = 'stale_active_run_evaluation'
              AND origin_id IS NOT NULL
              AND hidden_at IS NULL
              AND status NOT IN ('done', 'cancelled')
        `),
      );
    }
  });
});
