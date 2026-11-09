import { and, eq, gte, lt, ne, notInArray, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { agents, heartbeatRuns } from "@paperclipai/db";
import { parseHeartbeatPolicy } from "./heartbeat-policy.js";
import { DEFAULT_LEASE_TTL_MS, mintOwnerToken } from "./run-ownership-store.js";

type HeartbeatRunSlotInput = {
  runId: string;
  agentId: string;
  startedAt: Date;
  responsibleUserId: string | null;
  leaseTtlMs?: number;
};

type DailyRunCapBlock = {
  reason: "heartbeat.daily_run_limit";
  observed: number;
  limit: number;
};

type HeartbeatRunSlotClaim = {
  claimed: typeof heartbeatRuns.$inferSelect | null;
  dailyCapBlock: DailyRunCapBlock | null;
};

/**
 * Column patch that mints durable ownership for a queued -> running claim:
 * a fresh owner_token, a fence pulled from the global heartbeat_run_fence_seq
 * sequence, and an initial lease. Spread this into the claim UPDATE so the
 * ownership columns land in the same statement as the status transition.
 */
export function runOwnershipClaimPatch(input: { startedAt: Date; leaseTtlMs?: number }) {
  return {
    ownerToken: mintOwnerToken(),
    fence: sql`nextval('heartbeat_run_fence_seq')`,
    leaseExpiresAt: new Date(input.startedAt.getTime() + (input.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS)),
    leaseRenewedAt: input.startedAt,
    claimAttempt: sql`${heartbeatRuns.claimAttempt} + 1`,
  };
}

function currentUtcDayWindow(now: Date) {
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 0, 0, 0));
  return { start, end };
}

/**
 * Claim one queued heartbeat run without exceeding the agent's concurrency cap.
 *
 * The in-process start lock is useful for reducing duplicate scheduler work, but
 * it cannot serialize separate Paperclip workers. Keep the capacity check and
 * queued -> running transition in one transaction protected by a per-agent
 * advisory lock so the limit remains strict across workers and restarts.
 *
 * The same transaction mints durable ownership: a fresh owner_token, a fence
 * pulled from the global heartbeat_run_fence_seq sequence (monotonic across
 * every run, so any later takeover is trivially comparable), and an initial
 * lease. See run-ownership-store.ts for how the rest of the run lifecycle
 * fences its writes against these columns.
 */
export async function claimHeartbeatRunSlot(
  db: Db,
  input: HeartbeatRunSlotInput,
): Promise<typeof heartbeatRuns.$inferSelect | null> {
  return claimHeartbeatRunSlotInternal(db, input).then(({ claimed }) => claimed);
}

/**
 * Atomically enforce the daily run cap with the queued -> running transition.
 *
 * Callers may perform a cheaper preflight before this function, but admission
 * must use this transaction because separate workers can otherwise all observe
 * the same remaining daily capacity.
 */
export async function claimHeartbeatRunSlotWithDailyCap(
  db: Db,
  input: HeartbeatRunSlotInput & { dailyRunLimit: number | null },
): Promise<HeartbeatRunSlotClaim> {
  return claimHeartbeatRunSlotInternal(db, input);
}

type SlotAdmissionInput = {
  runId: string;
  agentId: string;
  dailyRunLimit?: number | null;
};

type SlotAdmissionDecision =
  | { admitted: true }
  | { admitted: false; dailyCapBlock: DailyRunCapBlock | null };

/**
 * Admission checks that must run inside the SAME transaction as the queued ->
 * running UPDATE: the per-agent advisory lock, the strict daily-run cap and the
 * concurrency cap. Callers that already own a claim transaction (the queue's
 * claim, the native review claim) call this immediately before their UPDATE
 * instead of opening a nested transaction, so the lock, the cap reads and the
 * ownership-minting UPDATE commit or roll back together. Separate Paperclip
 * workers serialize on the advisory lock, so the caps stay strict across
 * replicas and restarts.
 *
 * `maxConcurrentRuns` is supplied by the caller so the scheduler's own policy
 * resolution (including per-role concurrency overrides) stays the single source
 * of truth; it falls back to the shared heartbeat policy when omitted.
 */
/**
 * Take the per-agent slot lock for the rest of the transaction.
 *
 * LOCK ORDER (every admission path, no exceptions): this slot lock is the
 * FIRST lock a claim transaction takes. The order is
 *   slot advisory (this) < issue row < wakeup row < run row < external-admission advisory.
 * A transaction that already holds issue/wake/run row locks must never wait
 * for this lock, because the lock holder next waits for those rows. The lock
 * is re-entrant within a transaction, so `admitRunSlotInTransaction` calling it
 * again after the early acquire is a no-op.
 */
export async function acquireRunSlotLock(txDb: Db, agentId: string): Promise<void> {
  await txDb.execute(
    sql`select pg_advisory_xact_lock(hashtextextended(${`heartbeat-run-slot:${agentId}`}, 0))`,
  );
}

export async function admitRunSlotInTransaction(
  txDb: Db,
  input: SlotAdmissionInput & { maxConcurrentRuns?: number },
): Promise<SlotAdmissionDecision> {
  await acquireRunSlotLock(txDb, input.agentId);

  let maxConcurrentRuns = input.maxConcurrentRuns;
  if (maxConcurrentRuns === undefined) {
    const agent = await txDb
      .select()
      .from(agents)
      .where(eq(agents.id, input.agentId))
      .then((rows) => rows[0] ?? null);
    if (!agent) return { admitted: false, dailyCapBlock: null };
    maxConcurrentRuns = parseHeartbeatPolicy(agent).maxConcurrentRuns;
  }

  if (input.dailyRunLimit !== null && input.dailyRunLimit !== undefined) {
    const { start, end } = currentUtcDayWindow(new Date());
    const [row] = await txDb
      .select({ count: sql<number>`count(*)` })
      .from(heartbeatRuns)
      .where(
        and(
          eq(heartbeatRuns.agentId, input.agentId),
          gte(heartbeatRuns.startedAt, start),
          lt(heartbeatRuns.startedAt, end),
          notInArray(heartbeatRuns.status, ["queued", "scheduled_retry"]),
          ne(heartbeatRuns.id, input.runId),
        ),
      );
    const observed = Number(row?.count ?? 0);
    if (observed >= input.dailyRunLimit) {
      return {
        admitted: false,
        dailyCapBlock: {
          reason: "heartbeat.daily_run_limit",
          observed,
          limit: input.dailyRunLimit,
        },
      };
    }
  }

  const [{ count }] = await txDb
    .select({ count: sql<number>`count(*)` })
    .from(heartbeatRuns)
    .where(and(eq(heartbeatRuns.agentId, input.agentId), eq(heartbeatRuns.status, "running")));
  if (Number(count ?? 0) >= maxConcurrentRuns) {
    return { admitted: false, dailyCapBlock: null };
  }
  return { admitted: true };
}

/**
 * Run `claimWrite` inside a transaction that holds the per-agent advisory
 * lock and has verified the daily-run cap and the concurrency cap. The
 * callback performs the actual queued -> running UPDATE (which may carry
 * caller-specific bookkeeping) and returns the claimed row or null when the
 * compare-and-set lost.
 */
export async function withHeartbeatRunSlotAdmission(
  db: Db,
  input: SlotAdmissionInput,
  claimWrite: (tx: Db) => Promise<typeof heartbeatRuns.$inferSelect | null>,
): Promise<HeartbeatRunSlotClaim> {
  return db.transaction(async (tx) => {
    const txDb = tx as unknown as Db;
    const decision = await admitRunSlotInTransaction(txDb, input);
    if (!decision.admitted) return { claimed: null, dailyCapBlock: decision.dailyCapBlock };
    const claimed = await claimWrite(txDb);
    return { claimed, dailyCapBlock: null };
  });
}

async function claimHeartbeatRunSlotInternal(
  db: Db,
  input: HeartbeatRunSlotInput & { dailyRunLimit?: number | null },
): Promise<HeartbeatRunSlotClaim> {
  return withHeartbeatRunSlotAdmission(db, input, (txDb) =>
    txDb
      .update(heartbeatRuns)
      .set({
        status: "running",
        responsibleUserId: input.responsibleUserId,
        startedAt: input.startedAt,
        updatedAt: input.startedAt,
        ...runOwnershipClaimPatch(input),
      })
      .where(
        and(
          eq(heartbeatRuns.id, input.runId),
          eq(heartbeatRuns.agentId, input.agentId),
          eq(heartbeatRuns.status, "queued"),
        ),
      )
      .returning()
      .then((rows) => rows[0] ?? null),
  );
}
