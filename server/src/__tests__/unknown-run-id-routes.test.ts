import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { afterEach, expect, it } from "vitest";
import { eq, sql } from "drizzle-orm";
import {
  activityLog,
  agents,
  companies,
  companyMemberships,
  heartbeatRuns,
  issues,
  principalPermissionGrants,
} from "@paperclipai/db";
import { HttpError } from "../errors.js";
import { errorHandler } from "../middleware/error-handler.js";
import { issueRoutes } from "../routes/issues.js";
import { logActivity } from "../services/activity-log.js";
import { setBeforeScopedHeartbeatWrite } from "../services/heartbeat-run-scope.js";
import { setBeforeUnscopedCheckoutAdoption } from "../services/issues.js";
import {
  describeEmbeddedPostgres,
  seedCompanyWithBoardAccess,
  useEmbeddedPostgres,
} from "./helpers/route-test-harness.js";

const UNKNOWN_RUN_ID = "d26b3c44-1111-4111-8111-111111111111";

describeEmbeddedPostgres("unknown X-Paperclip-Run-Id on checkout and PATCH", () => {
  afterEach(() => {
    setBeforeUnscopedCheckoutAdoption(null);
    setBeforeScopedHeartbeatWrite(null);
  });

  const ctx = useEmbeddedPostgres("paperclip-unknown-run-id-", {
    resetEach: async (db) => {
      await db.delete(activityLog);
      await db.delete(issues);
      await db.delete(heartbeatRuns);
      await db.delete(agents);
      await db.delete(principalPermissionGrants);
      await db.delete(companyMemberships);
      await db.delete(companies);
    },
  });

  async function seedAgent(companyId: string, name: string) {
    const agentId = randomUUID();
    await ctx.db.insert(agents).values({
      id: agentId,
      companyId,
      name,
      role: "engineer",
      status: "idle",
      adapterType: "process",
      adapterConfig: {},
      runtimeConfig: {},
      permissions: {},
    });
    return agentId;
  }

  async function seedRun(companyId: string, agentId: string) {
    const runId = randomUUID();
    await ctx.db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      invocationSource: "on_demand",
      status: "running",
    });
    return runId;
  }

  async function seedIssue(companyId: string, assigneeAgentId: string) {
    const issueId = randomUUID();
    await ctx.db.insert(issues).values({
      id: issueId,
      companyId,
      title: "Write back",
      status: "todo",
      priority: "medium",
      assigneeAgentId,
    });
    return issueId;
  }

  function appFor(actor: Record<string, unknown>) {
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      (req as express.Request & { actor: typeof actor }).actor = actor;
      next();
    });
    app.use("/api", issueRoutes(ctx.db, {} as never));
    app.use(errorHandler);
    return app;
  }

  function agentActor(companyId: string, agentId: string, runId: string | null) {
    return {
      type: "agent" as const,
      source: "agent_key" as const,
      agentId,
      companyId,
      runId: runId ?? undefined,
    };
  }

  function expectUnknownRun(res: { status: number; body: unknown }) {
    const serialized = JSON.stringify(res.body);
    expect(res.status).toBe(422);
    expect(res.body).toMatchObject({
      error: "unknown_run_id",
      code: "unknown_run_id",
    });
    expect(serialized).not.toContain("Failed query");
    expect(serialized).not.toContain("params");
    expect(serialized).not.toContain("23503");
    expect(serialized).not.toContain("select ");
    expect(serialized).not.toContain("insert ");
  }

  const teardownPatch = {
    assigneeAdapterOverrides: {
      adapterConfig: {
        workspaceStrategy: {
          type: "git_worktree",
          teardownCommand: "rm -rf /tmp/paperclip-rce",
        },
      },
    },
  };

  it("prefers 403 for an agent host-command patch and keeps 422 for an authorized field", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const issueId = await seedIssue(company.companyId, agentId);
    const unknownAgent = agentActor(company.companyId, agentId, UNKNOWN_RUN_ID);

    const forbidden = await request(appFor(unknownAgent))
      .patch(`/api/issues/${issueId}`)
      .send(teardownPatch);
    expect(forbidden.status).toBe(403);
    expect(forbidden.body.error).toContain("host-executed workspace commands");
    expect(JSON.stringify(forbidden.body)).not.toContain("unknown_run_id");

    const title = await request(appFor(unknownAgent))
      .patch(`/api/issues/${issueId}`)
      .send({ title: "should-not-stick" });
    expectUnknownRun(title);

    const board = await request(appFor({ ...company.actor, runId: UNKNOWN_RUN_ID }))
      .patch(`/api/issues/${issueId}`)
      .send(teardownPatch);
    expectUnknownRun(board);

    const runId = await seedRun(company.companyId, agentId);
    const inScope = await request(appFor(agentActor(company.companyId, agentId, runId)))
      .patch(`/api/issues/${issueId}`)
      .send(teardownPatch);
    expect(inScope.status).toBe(403);
    expect(inScope.body.error).toContain("host-executed workspace commands");

    await ctx.db.delete(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
    const deletedField = await request(appFor(agentActor(company.companyId, agentId, runId)))
      .patch(`/api/issues/${issueId}`)
      .send({ title: "deleted-run" });
    expectUnknownRun(deletedField);

    const deletedCommand = await request(appFor(agentActor(company.companyId, agentId, runId)))
      .patch(`/api/issues/${issueId}`)
      .send(teardownPatch);
    expect(deletedCommand.status).toBe(403);
    expect(deletedCommand.body.error).toContain("host-executed workspace commands");

    const row = await ctx.db.query.issues.findFirst({
      where: (table, { eq: whereEq }) => whereEq(table.id, issueId),
    });
    expect(row?.title).toBe("Write back");
    expect(row?.assigneeAdapterOverrides ?? null).toBeNull();
    const written = await ctx.db
      .select({ id: activityLog.id })
      .from(activityLog)
      .where(eq(activityLog.entityId, issueId));
    expect(written).toEqual([]);
  });

  it("rejects a missing run, a cross-company run, and another agent's run on checkout and PATCH", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const other = await seedCompanyWithBoardAccess(ctx.db, "Other");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const otherAgentId = await seedAgent(company.companyId, "Other agent");
    const foreignAgentId = await seedAgent(other.companyId, "Foreign agent");
    const issueId = await seedIssue(company.companyId, agentId);
    const foreignRunId = await seedRun(other.companyId, foreignAgentId);
    const siblingRunId = await seedRun(company.companyId, otherAgentId);

    for (const runId of [UNKNOWN_RUN_ID, foreignRunId, siblingRunId]) {
      const actor = agentActor(company.companyId, agentId, runId);
      const checkout = await request(appFor(actor))
        .post(`/api/issues/${issueId}/checkout`)
        .send({ agentId, expectedStatuses: ["todo"] });
      expectUnknownRun(checkout);

      const patched = await request(appFor(actor))
        .patch(`/api/issues/${issueId}`)
        .send({ title: `should-not-stick-${runId}` });
      expectUnknownRun(patched);
    }

    const row = await ctx.db.query.issues.findFirst({
      where: (table, { eq }) => eq(table.id, issueId),
    });
    expect(row?.title).toBe("Write back");
    expect(row?.status).toBe("todo");
    expect(row?.checkoutRunId).toBeNull();
  });

  it("still checks out and patches when the run is in company and agent scope", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const runId = await seedRun(company.companyId, agentId);
    const issueId = await seedIssue(company.companyId, agentId);
    const actor = agentActor(company.companyId, agentId, runId);

    const checkout = await request(appFor(actor))
      .post(`/api/issues/${issueId}/checkout`)
      .send({ agentId, expectedStatuses: ["todo"] });
    expect(checkout.status).toBe(200);
    expect(checkout.body.checkoutRunId).toBe(runId);
    expect(checkout.body.status).toBe("in_progress");

    const patched = await request(appFor(actor))
      .patch(`/api/issues/${issueId}`)
      .send({ title: "Persisted" });
    expect(patched.status).toBe(200);
    expect(patched.body.title).toBe("Persisted");
    expect(JSON.stringify(patched.body)).not.toContain("Failed query");

    const activity = await ctx.db
      .select({ action: activityLog.action, runId: activityLog.runId, details: activityLog.details })
      .from(activityLog)
      .where(eq(activityLog.entityId, issueId));
    expect(activity).toContainEqual(
      expect.objectContaining({
        action: "issue.updated",
        runId,
        details: expect.objectContaining({ title: "Persisted" }),
      }),
    );
  });

  it("rejects a board actor attaching another company's run and accepts an in-company run", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const other = await seedCompanyWithBoardAccess(ctx.db, "Other");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const foreignAgentId = await seedAgent(other.companyId, "Foreign agent");
    const issueId = await seedIssue(company.companyId, agentId);
    const foreignRunId = await seedRun(other.companyId, foreignAgentId);
    const homeRunId = await seedRun(company.companyId, agentId);

    const foreign = await request(appFor({ ...company.actor, runId: foreignRunId }))
      .patch(`/api/issues/${issueId}`)
      .send({ title: "cross-company" });
    expectUnknownRun(foreign);

    const home = await request(appFor({ ...company.actor, runId: homeRunId }))
      .patch(`/api/issues/${issueId}`)
      .send({ title: "board-ok" });
    expect(home.status).toBe(200);
    expect(home.body.title).toBe("board-ok");
  });

  it("maps a missing or cross-company run on the activity write itself to 422 without SQL", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const other = await seedCompanyWithBoardAccess(ctx.db, "Other");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const foreignAgentId = await seedAgent(other.companyId, "Foreign agent");
    const foreignRunId = await seedRun(other.companyId, foreignAgentId);

    await expect(logActivity(ctx.db, {
      companyId: company.companyId,
      actorType: "agent",
      actorId: agentId,
      agentId,
      action: "issue.updated",
      entityType: "issue",
      entityId: randomUUID(),
      runId: UNKNOWN_RUN_ID,
    })).rejects.toMatchObject({ status: 422, message: "unknown_run_id" });

    try {
      await logActivity(ctx.db, {
        companyId: company.companyId,
        actorType: "agent",
        actorId: agentId,
        agentId,
        action: "issue.updated",
        entityType: "issue",
        entityId: randomUUID(),
        runId: foreignRunId,
      });
      throw new Error("cross-company run id was accepted");
    } catch (error) {
      expect(error).toBeInstanceOf(HttpError);
      expect((error as HttpError).status).toBe(422);
      expect(JSON.stringify(error)).not.toContain("Failed query");
      expect(JSON.stringify(error)).not.toContain("params");
      expect((error as Error).message).not.toContain(foreignRunId);
    }
  });

  async function waitUntilBlocked(
    blockerPid: number,
    pending: Promise<{ status: number; body: unknown }>,
  ) {
    let early: { status: number; body: unknown } | undefined;
    pending.then((result) => {
      early = result;
    }).catch(() => {});
    for (let attempt = 0; attempt < 200; attempt += 1) {
      if (early) {
        throw new Error(`request finished before blocking: ${early.status} ${JSON.stringify(early.body)}`);
      }
      const [state] = await ctx.db.execute(sql`select exists (
        select 1 from pg_stat_activity where ${blockerPid} = any(pg_blocking_pids(pid))
      ) as waiting`) as unknown as Array<{ waiting: boolean }>;
      if (state?.waiting) return;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error(`timed out waiting for pid ${blockerPid} to block a request`);
  }

  async function deleteRunWhileRequestBlocks(
    runId: string,
    startRequest: () => Promise<{ status: number; body: unknown }>,
    mutate?: (tx: Parameters<Parameters<typeof ctx.db.transaction>[0]>[0]) => Promise<void>,
  ) {
    let pending!: Promise<{ status: number; body: unknown }>;
    await ctx.db.transaction(async (tx) => {
      await tx.execute(sql`select ${heartbeatRuns.id} from ${heartbeatRuns} where ${heartbeatRuns.id} = ${runId} for update`);
      const [backend] = await tx.execute(sql`select pg_backend_pid() as pid`) as unknown as Array<{ pid: number }>;
      pending = startRequest();
      await waitUntilBlocked(Number(backend?.pid), pending);
      if (mutate) await mutate(tx);
      else await tx.delete(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
    });
    return pending;
  }

  it("rolls back checkout when a real run row is deleted while the write waits on its lock", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const checkoutRunId = await seedRun(company.companyId, agentId);
    const issueId = await seedIssue(company.companyId, agentId);

    const checkout = await deleteRunWhileRequestBlocks(checkoutRunId, () =>
      request(appFor(agentActor(company.companyId, agentId, checkoutRunId)))
        .post(`/api/issues/${issueId}/checkout`)
        .send({ agentId, expectedStatuses: ["todo"] }));
    expectUnknownRun(checkout);

    const row = await ctx.db.query.issues.findFirst({
      where: (table, { eq: whereEq }) => whereEq(table.id, issueId),
    });
    expect(row?.title).toBe("Write back");
    expect(row?.status).toBe("todo");
    expect(row?.checkoutRunId).toBeNull();
  });

  it("rolls back a PATCH when the run is deleted after the unlocked check and before the write lock", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const runId = await seedRun(company.companyId, agentId);
    const issueId = await seedIssue(company.companyId, agentId);

    setBeforeScopedHeartbeatWrite(async () => {
      await ctx.db.delete(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
    });

    const patched = await request(appFor(agentActor(company.companyId, agentId, runId)))
      .patch(`/api/issues/${issueId}`)
      .send({ title: "should-roll-back" });
    expectUnknownRun(patched);

    const row = await ctx.db.query.issues.findFirst({
      where: (table, { eq: whereEq }) => whereEq(table.id, issueId),
    });
    expect(row?.title).toBe("Write back");
    expect(row?.status).toBe("todo");
    const written = await ctx.db
      .select({ id: activityLog.id })
      .from(activityLog)
      .where(eq(activityLog.entityId, issueId));
    expect(written).toEqual([]);
  });

  it("does not keep a same-UUID run that was reinserted under another company", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const other = await seedCompanyWithBoardAccess(ctx.db, "Other");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const foreignAgentId = await seedAgent(other.companyId, "Foreign agent");
    const runId = await seedRun(company.companyId, agentId);
    const issueId = await seedIssue(company.companyId, agentId);

    const checkout = await deleteRunWhileRequestBlocks(
      runId,
      () => request(appFor(agentActor(company.companyId, agentId, runId)))
        .post(`/api/issues/${issueId}/checkout`)
        .send({ agentId, expectedStatuses: ["todo"] }),
      async (tx) => {
        await tx.delete(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
        await tx.insert(heartbeatRuns).values({
          id: runId,
          companyId: other.companyId,
          agentId: foreignAgentId,
          invocationSource: "on_demand",
          status: "running",
        });
      },
    );
    expectUnknownRun(checkout);

    const row = await ctx.db.query.issues.findFirst({
      where: (table, { eq: whereEq }) => whereEq(table.id, issueId),
    });
    expect(row?.checkoutRunId).toBeNull();
    expect(row?.status).toBe("todo");
    const moved = await ctx.db.query.heartbeatRuns.findFirst({
      where: (table, { eq: whereEq }) => whereEq(table.id, runId),
    });
    expect(moved?.companyId).toBe(other.companyId);
  });

  it("rejects stale-execution adoption when the actor run is deleted and reinserted in another company", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const other = await seedCompanyWithBoardAccess(ctx.db, "Other");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const foreignAgentId = await seedAgent(other.companyId, "Foreign agent");
    const actorRunId = await seedRun(company.companyId, agentId);
    const executionRunId = await seedRun(company.companyId, agentId);
    const issueId = randomUUID();
    await ctx.db.insert(issues).values({
      id: issueId,
      companyId: company.companyId,
      title: "Stale execution",
      status: "todo",
      priority: "medium",
      assigneeAgentId: agentId,
      executionRunId,
    });

    setBeforeUnscopedCheckoutAdoption(async () => {
      await ctx.db
        .update(heartbeatRuns)
        .set({ status: "failed", finishedAt: new Date() })
        .where(eq(heartbeatRuns.id, executionRunId));
      await ctx.db.delete(heartbeatRuns).where(eq(heartbeatRuns.id, actorRunId));
      await ctx.db.insert(heartbeatRuns).values({
        id: actorRunId,
        companyId: other.companyId,
        agentId: foreignAgentId,
        invocationSource: "on_demand",
        status: "running",
      });
    });

    const checkout = await request(appFor(agentActor(company.companyId, agentId, actorRunId)))
      .post(`/api/issues/${issueId}/checkout`)
      .send({ agentId, expectedStatuses: ["todo"] });
    expectUnknownRun(checkout);

    const row = await ctx.db.query.issues.findFirst({
      where: (table, { eq: whereEq }) => whereEq(table.id, issueId),
    });
    expect(row?.status).toBe("todo");
    expect(row?.checkoutRunId).toBeNull();
    expect(row?.executionRunId).toBe(executionRunId);
    expect(row?.assigneeAgentId).toBe(agentId);
  });

  it("rejects stale checkout adoption when the actor run is reinserted under another company", async () => {
    const company = await seedCompanyWithBoardAccess(ctx.db, "Home");
    const other = await seedCompanyWithBoardAccess(ctx.db, "Other");
    const agentId = await seedAgent(company.companyId, "Home agent");
    const foreignAgentId = await seedAgent(other.companyId, "Foreign agent");
    const actorRunId = await seedRun(company.companyId, agentId);
    const staleCheckoutRunId = await seedRun(company.companyId, agentId);
    const executionRunId = await seedRun(company.companyId, agentId);
    await ctx.db
      .update(heartbeatRuns)
      .set({ status: "failed", finishedAt: new Date() })
      .where(eq(heartbeatRuns.id, staleCheckoutRunId));
    const issueId = randomUUID();
    await ctx.db.insert(issues).values({
      id: issueId,
      companyId: company.companyId,
      title: "Stale checkout",
      status: "in_progress",
      priority: "medium",
      assigneeAgentId: agentId,
      checkoutRunId: staleCheckoutRunId,
      executionRunId,
    });

    setBeforeUnscopedCheckoutAdoption(async () => {
      await ctx.db.delete(heartbeatRuns).where(eq(heartbeatRuns.id, actorRunId));
      await ctx.db.insert(heartbeatRuns).values({
        id: actorRunId,
        companyId: other.companyId,
        agentId: foreignAgentId,
        invocationSource: "on_demand",
        status: "running",
      });
    });

    const checkout = await request(appFor(agentActor(company.companyId, agentId, actorRunId)))
      .post(`/api/issues/${issueId}/checkout`)
      .send({ agentId, expectedStatuses: ["in_progress", "todo"] });
    expectUnknownRun(checkout);

    const row = await ctx.db.query.issues.findFirst({
      where: (table, { eq: whereEq }) => whereEq(table.id, issueId),
    });
    expect(row?.status).toBe("in_progress");
    expect(row?.checkoutRunId).toBe(staleCheckoutRunId);
    expect(row?.executionRunId).toBe(executionRunId);
  });
});
