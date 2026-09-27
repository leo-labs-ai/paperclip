import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { expect, it } from "vitest";
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
import {
  describeEmbeddedPostgres,
  seedCompanyWithBoardAccess,
  useEmbeddedPostgres,
} from "./helpers/route-test-harness.js";

const UNKNOWN_RUN_ID = "d26b3c44-1111-4111-8111-111111111111";

describeEmbeddedPostgres("unknown X-Paperclip-Run-Id on checkout and PATCH", () => {
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
});
