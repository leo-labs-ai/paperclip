import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { eq, sql } from "drizzle-orm";
import { agents, companies, createDb, heartbeatRunEvents, heartbeatRuns, issues } from "@paperclipai/db";
import { appendHeartbeatRunEvent } from "../services/heartbeat-run-events.js";
import { acquireRunSlotLock, admitRunSlotInTransaction } from "../services/heartbeat-run-slot.js";
import { sqlMillisInterval } from "../services/run-ownership-store.js";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "./helpers/embedded-postgres.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

describeEmbeddedPostgres("claim-fenced run events and slot lock order", () => {
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  let otherDb: ReturnType<typeof createDb>;
  const companyId = "40000000-0000-4000-8000-000000000001";
  const agentId = "40000000-0000-4000-8000-000000000002";
  let ordinal = 10;

  const makeRun = async (patch: Partial<typeof heartbeatRuns.$inferInsert> = {}) => {
    const id = `40000000-0000-4000-8000-${String(ordinal++).padStart(12, "0")}`;
    await db.insert(heartbeatRuns).values({
      id, companyId, agentId, status: "running", ownerToken: "owner-a", fence: 3,
      leaseExpiresAt: new Date(Date.now() + 1_000), ...patch,
    });
    return id;
  };
  const readRun = async (id: string) => (await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, id)))[0]!;
  const readEvents = (id: string) => db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, id));
  const base = (runId: string) => ({ companyId, runId, agentId, eventType: "log", message: "m" });
  const stale = { ownerToken: "owner-b", fence: 3 };
  const held = { ownerToken: "owner-a", fence: 3 };
  const native = (n: number) => ({
    sourceInstanceId: "src", sourceEventId: `e${n}`, sourceSeq: n, protocolSchemaVersion: 1, canonicalPayload: { n },
  });

  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("paperclip-claim-fencing-");
    db = createDb(temporary.connectionString);
    otherDb = createDb(temporary.connectionString);
    await db.insert(companies).values({ id: companyId, name: "Fencing", issuePrefix: "FNC" });
    await db.insert(agents).values({ id: agentId, companyId, name: "Fence agent" });
  }, 60_000);
  afterAll(async () => { await temporary?.cleanup(); });

  it("rejects a stale claim on every append path without touching sequence, events or lease", async () => {
    const runId = await makeRun();
    const before = await readRun(runId);
    const retryExhaustion = { retryReason: "r", scheduledRetryAttempt: 1, maxAttempts: 2 };
    const attempts = [
      { ...base(runId), claim: stale },
      { ...base(runId), claim: stale, retryExhaustion },
      { ...base(runId), claim: stale, nativeSource: native(1) },
      { ...base(runId), claim: stale, retryExhaustion, nativeSource: native(2) },
    ];
    for (const input of attempts) {
      expect((await appendHeartbeatRunEvent(db, input)).disposition).toBe("rejected");
    }
    const after = await readRun(runId);
    expect(after.nextEventSeq).toBe(before.nextEventSeq);
    expect(after.leaseExpiresAt?.getTime()).toBe(before.leaseExpiresAt?.getTime());
    expect(await readEvents(runId)).toHaveLength(0);
  });

  it("rejects an append for a no-longer-running run even when the token matches", async () => {
    const runId = await makeRun({ status: "cancelled" });
    for (const extra of [{}, { nativeSource: native(1) }]) {
      expect((await appendHeartbeatRunEvent(db, { ...base(runId), claim: held, ...extra })).disposition).toBe("rejected");
    }
    expect(await readEvents(runId)).toHaveLength(0);
  });

  it("commits held-claim appends on the slow path, stamps the fence and renews the lease", async () => {
    const runId = await makeRun();
    const before = await readRun(runId);
    const nativeResult = await appendHeartbeatRunEvent(db, { ...base(runId), claim: held, nativeSource: native(1) });
    const exhausted = await appendHeartbeatRunEvent(db, {
      ...base(runId), claim: held, eventType: "lifecycle", message: "Bounded retry exhausted",
      retryExhaustion: { retryReason: "r", scheduledRetryAttempt: 1, maxAttempts: 2 },
    });
    expect(nativeResult.disposition).toBe("committed");
    expect(exhausted.disposition).toBe("committed");
    expect((await readEvents(runId)).map((e) => e.fence)).toEqual([3, 3]);
    expect((await readRun(runId)).leaseExpiresAt!.getTime()).toBeGreaterThan(before.leaseExpiresAt!.getTime() + 30_000);
  });

  it("keeps unclaimed native appends working so native lease authority is unchanged", async () => {
    const runId = await makeRun({ ownerToken: null, fence: null });
    const result = await appendHeartbeatRunEvent(db, { ...base(runId), nativeSource: native(1) });
    expect(result.disposition).toBe("committed");
    expect((await readEvents(runId))[0]!.fence).toBeNull();
  });

  it("binds the lease interval as a parameter", async () => {
    const [row] = await db.execute<{ ms: string }>(sql`select extract(epoch from ${sqlMillisInterval(90_000)}) * 1000 as ms`);
    expect(Number(row!.ms)).toBe(90_000);
    const [injected] = await db.execute<{ ms: string }>(sql`select extract(epoch from ${sqlMillisInterval(Number.NaN)}) as ms`);
    expect(Number(injected!.ms)).toBe(0);
  });

  it("takes the slot lock before row locks so a row-first waiter cannot deadlock admission", async () => {
    // Claim tx: slot lock first, then the issue row. A concurrent queue editor
    // (issue row, no slot lock) must simply wait, never form a cycle.
    const issueId = "40000000-0000-4000-8000-0000000000aa";
    await db.insert(issues).values({ id: issueId, companyId, title: "lock order" });
    const runId = await makeRun({ status: "queued", ownerToken: null, fence: null });
    let editorStarted!: () => void;
    const editorReady = new Promise<void>((resolve) => { editorStarted = resolve; });
    const claim = db.transaction(async (tx) => {
      await acquireRunSlotLock(tx as never, agentId);
      await editorReady;
      await new Promise((resolve) => setTimeout(resolve, 150));
      await tx.select({ id: issues.id }).from(issues).where(eq(issues.id, issueId)).for("update");
      const decision = await admitRunSlotInTransaction(tx as never, { runId, agentId, maxConcurrentRuns: 5 });
      return decision.admitted;
    });
    const editor = otherDb.transaction(async (tx) => {
      editorStarted();
      await tx.select({ id: issues.id }).from(issues).where(eq(issues.id, issueId)).for("update");
      await tx.select({ id: heartbeatRuns.id }).from(heartbeatRuns).where(eq(heartbeatRuns.id, runId)).for("update");
    });
    await expect(Promise.race([
      Promise.all([claim, editor]),
      new Promise((_, reject) => setTimeout(() => reject(new Error("deadlock/timeout")), 10_000)),
    ])).resolves.toEqual([true, undefined]);
  });
});
