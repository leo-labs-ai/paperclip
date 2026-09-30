import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { and, eq, sql } from "drizzle-orm";
import { agents, companies, createDb, heartbeatRuns, issueRecoveryActions, issues } from "@paperclipai/db";
import {
  conversationRecoveryActionPredicate,
  getConversationOwnershipBlocker,
  runIdMatchesEvidence,
  runIssueLinkPredicate,
} from "../services/conversation-continuation.js";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";

const support = await getEmbeddedPostgresTestSupport();
const describeDb = support.supported ? describe : describe.skip;

describeDb("conversation continuation indexable predicates", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-conversation-continuation-queries-");
    db = createDb(tempDb.connectionString);
  }, 30_000);
  afterAll(async () => { await tempDb?.cleanup(); });

  async function seedCompany() {
    const companyId = randomUUID();
    const agentId = randomUUID();
    await db.insert(companies).values({
      id: companyId, name: "Paperclip", issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
    });
    await db.insert(agents).values({
      id: agentId, companyId, name: "Coder", role: "engineer", status: "idle", adapterType: "codex_local",
      adapterConfig: {}, runtimeConfig: {}, permissions: {},
    });
    return { companyId, agentId };
  }
  const newIssue = async (companyId: string) => {
    const id = randomUUID();
    await db.insert(issues).values({ id, companyId, title: "t" });
    return id;
  };
  const liveRun = (companyId: string, agentId: string, v: Partial<typeof heartbeatRuns.$inferInsert>) => ({
    companyId, agentId, status: "failed", runtimeMode: "legacy", processPid: process.pid,
    runnerProfileJson: { adapterDispatch: { adapterType: "codex_local" } }, ...v,
  });

  it("ownership blocker: nativeIssueId takes precedence over context issueId; context only when native is null", async () => {
    const { companyId, agentId } = await seedCompany();
    const [native, ctxOnly, shadow] = await Promise.all([newIssue(companyId), newIssue(companyId), newIssue(companyId)]);
    // native points at `native`, context claims `shadow`: only `native` is blocked.
    await db.insert(heartbeatRuns).values(liveRun(companyId, agentId, { nativeIssueId: native, contextSnapshot: { issueId: shadow } }));
    // no native id: context issue applies.
    await db.insert(heartbeatRuns).values(liveRun(companyId, agentId, { contextSnapshot: { issueId: ctxOnly } }));

    expect((await getConversationOwnershipBlocker(db, companyId, native))?.runId).toBeTruthy();
    expect(await getConversationOwnershipBlocker(db, companyId, shadow)).toBeNull();
    expect((await getConversationOwnershipBlocker(db, companyId, ctxOnly))?.runId).toBeTruthy();
  });

  it("ownership blocker respects the company boundary", async () => {
    const a = await seedCompany();
    const b = await seedCompany();
    const issueId = await newIssue(a.companyId);
    await db.insert(heartbeatRuns).values(liveRun(a.companyId, a.agentId, { nativeIssueId: issueId }));
    expect(await getConversationOwnershipBlocker(db, a.companyId, issueId)).not.toBeNull();
    expect(await getConversationOwnershipBlocker(db, b.companyId, issueId)).toBeNull();
  });

  it("evidence matching is safe for malformed/non-canonical runIds and company-scoped", async () => {
    const a = await seedCompany();
    const b = await seedCompany();
    const issueIds = await Promise.all(Array.from({ length: 7 }, () => newIssue(a.companyId)));
    const issueId = issueIds[6];
    const [run] = await db.insert(heartbeatRuns).values(liveRun(a.companyId, a.agentId, {
      nativeIssueId: issueId, runtimeMode: "legacy", status: "interrupted",
    })).returning();
    const evidences: Array<Record<string, unknown>> = [
      { runId: "not-a-uuid" }, { runId: run.id.toUpperCase() }, { runId: 12345 }, {}, { runId: "" },
      { runId: `${run.id}x` }, { runId: run.id },
    ];
    const ids: string[] = [];
    for (const [i, evidence] of evidences.entries()) {
      const [row] = await db.insert(issueRecoveryActions).values({
        companyId: a.companyId, sourceIssueId: issueIds[i],
        kind: "active_run_watchdog", cause: "legacy_execution_requires_reconciliation",
        fingerprint: randomUUID(), evidence, nextAction: "n",
      }).returning();
      ids.push(row.id);
    }
    const matched = await db.select({ id: issueRecoveryActions.id }).from(issueRecoveryActions)
      .innerJoin(heartbeatRuns, and(
        eq(heartbeatRuns.companyId, issueRecoveryActions.companyId),
        runIdMatchesEvidence(issueRecoveryActions.evidence),
      ));
    expect(matched.map((r) => r.id)).toEqual([ids[ids.length - 1]]);
    // Whole predicate runs without a cast error over malformed evidence.
    await expect(db.select({ id: issueRecoveryActions.id }).from(issueRecoveryActions)
      .where(conversationRecoveryActionPredicate())).resolves.toBeDefined();
    // Cross-company run with same id never matches.
    const crossed = await db.select({ id: issueRecoveryActions.id }).from(issueRecoveryActions)
      .innerJoin(heartbeatRuns, and(
        eq(heartbeatRuns.companyId, b.companyId),
        eq(issueRecoveryActions.companyId, a.companyId),
        eq(heartbeatRuns.companyId, issueRecoveryActions.companyId),
        runIdMatchesEvidence(issueRecoveryActions.evidence),
      ));
    expect(crossed).toEqual([]);
  });

  it("issue linkage uses an index, not a heap scan, with many irrelevant rows", async () => {
    const { companyId, agentId } = await seedCompany();
    const issueId = await newIssue(companyId);
    await db.execute(sql`
      insert into heartbeat_runs (company_id, agent_id, status, native_issue_id)
      select ${companyId}::uuid, ${agentId}::uuid, 'failed', gen_random_uuid() from generate_series(1, 3000)`);
    await db.insert(heartbeatRuns).values(liveRun(companyId, agentId, { nativeIssueId: issueId }));
    await db.execute(sql`analyze heartbeat_runs`);
    const plan = await db.transaction(async (tx) => {
      await tx.execute(sql`set local enable_seqscan = off`);
      const rows = await tx.execute(sql`explain select id from heartbeat_runs
        where company_id = ${companyId}::uuid and ${runIssueLinkPredicate(issueId)}`);
      return (rows as unknown as Array<Record<string, string>>).map((r) => r["QUERY PLAN"]).join("\n");
    });
    expect(plan).not.toMatch(/Seq Scan on heartbeat_runs/);
    expect(plan).toMatch(/heartbeat_runs_company_native_issue/);
    const runId = randomUUID();
    const pkPlan = await db.execute(sql`explain select 1 from heartbeat_runs where ${runIdMatchesEvidence(sql`${JSON.stringify({ runId })}::jsonb`)}`);
    expect((pkPlan as unknown as Array<Record<string, string>>).map((r) => r["QUERY PLAN"]).join("\n")).toMatch(/heartbeat_runs_pkey/);
  });
});
