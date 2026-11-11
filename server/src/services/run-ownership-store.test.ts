import { randomUUID } from "node:crypto";
import { eq, sql } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { agents, companies, createDb, heartbeatRunEvents, heartbeatRuns } from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "../__tests__/helpers/embedded-postgres.js";
import { claimHeartbeatRunSlot } from "./heartbeat-run-slot.js";
import {
  appendFencedRunEvent,
  claimExpiredLease,
  findExpiredLeaseRuns,
  isClaimStale,
  releaseRunOwnership,
  renewLease,
  writeFencedRunPatch,
  type RunClaim,
} from "./run-ownership-store.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

describeEmbeddedPostgres("run ownership store", () => {
  // Two independent connections against the same database, standing in for
  // two Paperclip replicas. Using the query-builder `db` for one side and a
  // second, separately-created `db2` for the other ensures a "peer" claim in
  // these tests goes through a real second connection/session, not just a
  // second in-process call reusing the same client.
  let db!: ReturnType<typeof createDb>;
  let db2!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-run-ownership-");
    db = createDb(tempDb.connectionString);
    db2 = createDb(tempDb.connectionString);
  }, 30_000);

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedCompanyAgent() {
    const companyId = randomUUID();
    const agentId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Ownership test company",
      issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
      defaultResponsibleUserId: "responsible-user",
    });
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "Ownership test agent",
      role: "engineer",
      status: "active",
      adapterType: "test_adapter",
      adapterConfig: {},
      runtimeConfig: { heartbeat: { maxConcurrentRuns: 5 } },
      permissions: {},
    });
    return { companyId, agentId };
  }

  async function cleanup(companyId: string, agentId: string) {
    await db.delete(heartbeatRunEvents).where(eq(heartbeatRunEvents.agentId, agentId));
    await db.delete(heartbeatRuns).where(eq(heartbeatRuns.agentId, agentId));
    await db.delete(agents).where(eq(agents.id, agentId));
    await db.delete(companies).where(eq(companies.id, companyId));
  }

  async function seedRunningRun(input: {
    companyId: string;
    agentId: string;
    leaseExpiresAt: Date;
    ownerToken?: string | null;
    fence?: number | null;
  }) {
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId: input.companyId,
      agentId: input.agentId,
      status: "running",
      invocationSource: "assignment",
      contextSnapshot: {},
      ownerToken: input.ownerToken ?? randomUUID(),
      fence: input.fence ?? null,
      leaseExpiresAt: input.leaseExpiresAt,
      leaseRenewedAt: new Date(),
    });
    return runId;
  }

  it("mints a unique, monotonic fence and owner token on claim", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const runIds = [randomUUID(), randomUUID()];
    await db.insert(heartbeatRuns).values(
      runIds.map((id) => ({
        id,
        companyId,
        agentId,
        status: "queued",
        invocationSource: "assignment",
        contextSnapshot: {},
      })),
    );

    try {
      const first = await claimHeartbeatRunSlot(db, {
        runId: runIds[0],
        agentId,
        startedAt: new Date(),
        responsibleUserId: "responsible-user",
      });
      const second = await claimHeartbeatRunSlot(db, {
        runId: runIds[1],
        agentId,
        startedAt: new Date(),
        responsibleUserId: "responsible-user",
      });

      expect(first?.ownerToken).toBeTruthy();
      expect(second?.ownerToken).toBeTruthy();
      expect(first?.ownerToken).not.toBe(second?.ownerToken);
      expect(typeof first?.fence).toBe("number");
      expect(typeof second?.fence).toBe("number");
      expect(second!.fence!).toBeGreaterThan(first!.fence!);
      expect(first?.leaseExpiresAt).toBeInstanceOf(Date);
      expect(first?.claimAttempt).toBe(1);
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("renewLease extends the lease only for the current owner+fence", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "queued",
      invocationSource: "assignment",
      contextSnapshot: {},
    });

    try {
      const claimed = await claimHeartbeatRunSlot(db, {
        runId,
        agentId,
        startedAt: new Date(),
        responsibleUserId: "responsible-user",
      });
      expect(claimed).toBeTruthy();
      const claim: RunClaim = { ownerToken: claimed!.ownerToken!, fence: claimed!.fence };

      const staleToken = await renewLease(db, { runId, claim: { ownerToken: "not-the-real-token", fence: claim.fence } });
      expect(staleToken).toBeNull();

      const staleFence = await renewLease(db, { runId, claim: { ownerToken: claim.ownerToken, fence: (claim.fence ?? 0) + 999 } });
      expect(staleFence).toBeNull();

      const renewed = await renewLease(db, { runId, claim });
      expect(renewed).toBeTruthy();
      expect(renewed!.leaseExpiresAt!.getTime()).toBeGreaterThan(claimed!.leaseExpiresAt!.getTime());
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("releaseRunOwnership clears owner_token but preserves the fence for audit", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "queued",
      invocationSource: "assignment",
      contextSnapshot: {},
    });

    try {
      const claimed = await claimHeartbeatRunSlot(db, {
        runId,
        agentId,
        startedAt: new Date(),
        responsibleUserId: "responsible-user",
      });
      const claim: RunClaim = { ownerToken: claimed!.ownerToken!, fence: claimed!.fence };
      const released = await releaseRunOwnership(db, { runId, claim });
      expect(released?.ownerToken).toBeNull();
      expect(released?.leaseExpiresAt).toBeNull();
      expect(released?.fence).toBe(claimed!.fence);
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("findExpiredLeaseRuns surfaces only running rows past their lease, oldest first, by DB clock", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const now = new Date();
    const freshRunId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(now.getTime() + 60_000) });
    const expiredRunId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(now.getTime() - 5_000) });
    const veryExpiredRunId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(now.getTime() - 30_000) });

    try {
      const expired = await findExpiredLeaseRuns(db, {});
      const expiredIds = expired.map((run) => run.id);
      expect(expiredIds).toEqual([veryExpiredRunId, expiredRunId]);
      expect(expiredIds).not.toContain(freshRunId);
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("claimExpiredLease takes over only a genuinely expired lease and mints a strictly higher fence", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const now = new Date();
    // Seed each run's fence from a real draw of the shared global sequence
    // (rather than a hardcoded literal) so "the takeover mints a strictly
    // higher fence" is a real assertion regardless of how many other tests
    // in this file have already advanced the sequence.
    const [{ nextval: seedFenceRaw }] = (await db.execute(
      sql`select nextval('heartbeat_run_fence_seq')`,
    )) as unknown as Array<{ nextval: string | number }>;
    const seedFence = Number(seedFenceRaw);
    const liveRunId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(now.getTime() + 60_000), fence: seedFence });
    const expiredRunId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(now.getTime() - 5_000), fence: seedFence });

    try {
      const liveAttempt = await claimExpiredLease(db, { runId: liveRunId });
      expect(liveAttempt).toBeNull();

      const priorOwnerToken = (await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, expiredRunId)))[0]!.ownerToken;
      const takeover = await claimExpiredLease(db, { runId: expiredRunId });
      expect(takeover).toBeTruthy();
      expect(takeover!.ownerToken).toBeTruthy();
      expect(takeover!.ownerToken).not.toBe(priorOwnerToken);
      expect(takeover!.fence!).toBeGreaterThan(seedFence);
      expect(takeover!.leaseExpiresAt!.getTime()).toBeGreaterThan(now.getTime());
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("requires both the durable run lease and controller lease to expire", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const runId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(0) });
    try {
      await db.update(heartbeatRuns).set({
        controllerBootId: "controller-a",
        controllerLeaseExpiresAt: sql`now() + interval '1 hour'`,
      }).where(eq(heartbeatRuns.id, runId));
      expect(await claimExpiredLease(db2, { runId })).toBeNull();

      // A controller without an expiry is not proof of a dead controller.
      await db.update(heartbeatRuns).set({ controllerLeaseExpiresAt: null })
        .where(eq(heartbeatRuns.id, runId));
      expect(await claimExpiredLease(db2, { runId })).toBeNull();

      await db.update(heartbeatRuns).set({
        controllerLeaseExpiresAt: sql`now() - interval '1 second'`,
        leaseExpiresAt: sql`now() + interval '1 hour'`,
      }).where(eq(heartbeatRuns.id, runId));
      expect(await claimExpiredLease(db2, { runId })).toBeNull();

      await db.update(heartbeatRuns).set({ leaseExpiresAt: sql`now() - interval '1 second'` })
        .where(eq(heartbeatRuns.id, runId));
      const takeover = await claimExpiredLease(db2, { runId });
      expect(takeover?.ownerToken).toBeTruthy();
      expect(takeover?.fence).not.toBeNull();
      expect(await claimExpiredLease(db, { runId })).toBeNull();
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("two connections racing claimExpiredLease: exactly one wins, with a higher fence than the original", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const now = new Date();
    const runId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(now.getTime() - 5_000), fence: 5 });

    try {
      const [resultA, resultB] = await Promise.all([
        claimExpiredLease(db, { runId }),
        claimExpiredLease(db2, { runId }),
      ]);
      const winners = [resultA, resultB].filter((r): r is NonNullable<typeof r> => r !== null);
      expect(winners).toHaveLength(1);
      expect(winners[0]!.fence!).toBeGreaterThan(5);
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("appendFencedRunEvent atomically checks the claim and inserts, and rejects a stale/superseded claim", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "queued",
      invocationSource: "assignment",
      contextSnapshot: {},
    });

    try {
      const claimed = await claimHeartbeatRunSlot(db, {
        runId,
        agentId,
        startedAt: new Date(),
        responsibleUserId: "responsible-user",
      });
      const claim: RunClaim = { ownerToken: claimed!.ownerToken!, fence: claimed!.fence };

      const inserted = await appendFencedRunEvent(db, {
        runId,
        claim,
        companyId,
        agentId,
        seq: 1,
        eventType: "lifecycle",
        message: "run started",
      });
      expect(inserted).toBeTruthy();
      expect(inserted!.fence).toBe(claim.fence);

      const events = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, runId));
      expect(events).toHaveLength(1);
      expect(events[0]!.fence).toBe(claim.fence);

      // A peer takes over via db2, invalidating the original claim.
      const takeover = await claimExpiredLease(db2, { runId, graceMs: -1_000_000 });
      expect(takeover).toBeTruthy();

      const staleWrite = await appendFencedRunEvent(db, {
        runId,
        claim,
        companyId,
        agentId,
        seq: 2,
        eventType: "lifecycle",
        message: "stale write attempt after takeover",
      });
      expect(staleWrite).toBeNull();

      // The stale write must not have landed.
      const eventsAfter = await db.select().from(heartbeatRunEvents).where(eq(heartbeatRunEvents.runId, runId));
      expect(eventsAfter).toHaveLength(1);

      // The new owner's claim, however, can append.
      const newClaim: RunClaim = { ownerToken: takeover!.ownerToken!, fence: takeover!.fence };
      const wonWrite = await appendFencedRunEvent(db2, {
        runId,
        claim: newClaim,
        companyId,
        agentId,
        seq: 2,
        eventType: "lifecycle",
        message: "written by new owner after takeover",
      });
      expect(wonWrite).toBeTruthy();
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("crash simulation: a run whose owner vanished without releasing is only recoverable via a real takeover, never by reusing its old claim", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const now = new Date();
    // Simulate the crashed executor: claimed, then lease left to lapse with
    // no release/renewal ever happening (process died mid-run).
    const crashedOwnerToken = randomUUID();
    const runId = await seedRunningRun({
      companyId,
      agentId,
      leaseExpiresAt: new Date(now.getTime() - 10_000),
      ownerToken: crashedOwnerToken,
      fence: 1,
    });

    try {
      // The dead owner's own (now-stale-by-definition) claim can no longer
      // renew or write — it has no way to tell it crashed, but the store
      // still refuses it once another party's takeover succeeds.
      const takeover = await claimExpiredLease(db2, { runId });
      expect(takeover).toBeTruthy();
      expect(takeover!.ownerToken).not.toBe(crashedOwnerToken);

      const deadClaim: RunClaim = { ownerToken: crashedOwnerToken, fence: 1 };
      const deadRenew = await renewLease(db, { runId, claim: deadClaim });
      expect(deadRenew).toBeNull();

      const deadEvent = await appendFencedRunEvent(db, {
        runId,
        claim: deadClaim,
        companyId,
        agentId,
        seq: 1,
        eventType: "lifecycle",
        message: "should never land",
      });
      expect(deadEvent).toBeNull();
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("writeFencedRunPatch applies the patch and renews the lease for the current owner+fence, and rejects a stale claim", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "queued",
      invocationSource: "assignment",
      contextSnapshot: {},
    });

    try {
      const claimed = await claimHeartbeatRunSlot(db, {
        runId,
        agentId,
        startedAt: new Date(),
        responsibleUserId: "responsible-user",
      });
      expect(claimed).toBeTruthy();
      const claim: RunClaim = { ownerToken: claimed!.ownerToken!, fence: claimed!.fence };

      const patched = await writeFencedRunPatch(db, {
        runId,
        claim,
        patch: { contextSnapshot: { wakeReason: "test" } },
      });
      expect(patched).toBeTruthy();
      expect(patched!.contextSnapshot).toEqual({ wakeReason: "test" });
      // Lease renewal uses now() at write time, so it must be strictly later
      // than the lease minted at claim time, not merely non-decreasing.
      expect(patched!.leaseExpiresAt!.getTime()).toBeGreaterThan(claimed!.leaseExpiresAt!.getTime());

      // A peer takes over via db2, invalidating the original claim.
      const takeover = await claimExpiredLease(db2, { runId, graceMs: -1_000_000 });
      expect(takeover).toBeTruthy();

      const staleWrite = await writeFencedRunPatch(db, {
        runId,
        claim,
        patch: { contextSnapshot: { wakeReason: "stale" } },
      });
      expect(staleWrite).toBeNull();

      // The stale write must not have landed.
      const afterStale = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, runId));
      expect(afterStale[0]!.contextSnapshot).toEqual({ wakeReason: "test" });
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("isClaimStale reports true only on an actual owner/fence mismatch, false on a missing row", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "queued",
      invocationSource: "assignment",
      contextSnapshot: {},
    });

    try {
      const claimed = await claimHeartbeatRunSlot(db, {
        runId,
        agentId,
        startedAt: new Date(),
        responsibleUserId: "responsible-user",
      });
      const claim: RunClaim = { ownerToken: claimed!.ownerToken!, fence: claimed!.fence };

      expect(await isClaimStale(db, { runId, claim })).toBe(false);

      const takeover = await claimExpiredLease(db2, { runId, graceMs: -1_000_000 });
      expect(takeover).toBeTruthy();

      expect(await isClaimStale(db, { runId, claim })).toBe(true);

      // A row that no longer exists is not an ownership dispute.
      expect(await isClaimStale(db, { runId: randomUUID(), claim })).toBe(false);
    } finally {
      await cleanup(companyId, agentId);
    }
  });

  it("reaper takeover branch: a fenced terminal write under the reaper's claim releases ownership, and a lost takeover race is not treated as a stale write", async () => {
    const { companyId, agentId } = await seedCompanyAgent();
    const now = new Date();
    const runId = await seedRunningRun({ companyId, agentId, leaseExpiresAt: new Date(now.getTime() - 5_000) });

    try {
      // Mirrors reapOrphanedRuns: take over the expired lease, then finalize
      // under the freshly minted reaper claim.
      const takeover = await claimExpiredLease(db, { runId });
      expect(takeover).toBeTruthy();
      const reaperClaim: RunClaim = { ownerToken: takeover!.ownerToken!, fence: takeover!.fence };

      const finalized = await db
        .update(heartbeatRuns)
        .set({ status: "failed", ownerToken: null, leaseExpiresAt: null, finishedAt: now, updatedAt: now })
        .where(sql`${heartbeatRuns.id} = ${runId} and owner_token = ${reaperClaim.ownerToken} and fence = ${reaperClaim.fence}`)
        .returning()
        .then((rows) => rows[0] ?? null);
      expect(finalized).toBeTruthy();
      expect(finalized!.status).toBe("failed");
      expect(finalized!.ownerToken).toBeNull();

      // A second reaper racing the same already-taken-over lease loses:
      // claimExpiredLease matches 0 rows (status is no longer "running") and
      // returns null, which reapOrphanedRuns treats as "lost the race, skip"
      // rather than as a stale write to report.
      const raced = await claimExpiredLease(db2, { runId });
      expect(raced).toBeNull();
    } finally {
      await cleanup(companyId, agentId);
    }
  });
});
