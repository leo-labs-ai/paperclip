import { and, asc, eq, getTableColumns, or, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { heartbeatRunEvents, heartbeatRuns } from "@paperclipai/db";
import { nativeSha256 } from "./native-runtime/canonical.js";
import { DEFAULT_LEASE_TTL_MS, sqlMillisInterval, type RunClaim } from "./run-ownership-store.js";

export interface AppendHeartbeatRunEventInput {
  companyId: string;
  runId: string;
  agentId: string;
  eventType: string;
  stream?: string | null;
  level?: string | null;
  color?: string | null;
  message?: string | null;
  payload?: Record<string, unknown> | null;
  /** Reuse an existing exhaustion receipt, including receipts from older builds. */
  retryExhaustion?: {
    retryReason: string;
    scheduledRetryAttempt: number;
    maxAttempts: number;
  };
  nativeSource?: {
    sourceInstanceId: string;
    sourceEventId: string;
    sourceSeq: number;
    protocolSchemaVersion: number;
    canonicalPayload: Record<string, unknown>;
  };
  /**
   * Fork (active-active): fence the append against the run's durable
   * ownership. When set, the insert only lands if the run row still carries
   * exactly this (ownerToken, fence) pair and is `running`; the same
   * statement renews the lease (PostgreSQL clock) and stamps the row's fence
   * onto the event. A stale claim returns `disposition: "rejected"` instead
   * of inserting an unfenced event. See run-ownership-store.ts.
   */
  claim?: RunClaim | null;
  leaseTtlMs?: number;
}

export interface AppendHeartbeatRunEventResult {
  row: typeof heartbeatRunEvents.$inferSelect;
  disposition: "committed" | "duplicate" | "rejected";
  highestContiguousSourceSeq: number;
}

export class HeartbeatRunEventConflictError extends Error {
  readonly code = "native_event_replay_conflict" as const;
  constructor() {
    super("native_event_replay_conflict");
    this.name = "HeartbeatRunEventConflictError";
  }
}

/**
 * Atomically reserve the next event sequence on the run row. Both native PRP
 * ingestion and legacy/direct-adapter writers use this allocator so the
 * database uniqueness invariant cannot turn a concurrent log/cancel/recovery
 * race into a failed run.
 */
export async function allocateHeartbeatRunEventSeq(
  db: Db,
  runId: string,
): Promise<number> {
  return db.transaction(async (tx) => {
    const [run] = await tx.select({ id: heartbeatRuns.id }).from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, runId)).for("update").limit(1);
    if (!run) throw new Error("heartbeat_run_event_binding_mismatch");
    // Read persisted evidence after acquiring the lock, in a new statement.
    // A writer we waited for may have committed beyond the stored counter.
    const [updated] = await tx
      .update(heartbeatRuns)
      .set({
        nextEventSeq: sql`greatest(${heartbeatRuns.nextEventSeq}, coalesce((
          select max(${heartbeatRunEvents.seq}) + 1 from ${heartbeatRunEvents}
          where ${heartbeatRunEvents.runId} = ${runId}
        ), 1)) + 1`,
        updatedAt: new Date(),
      })
      .where(eq(heartbeatRuns.id, runId))
      .returning({ nextEventSeq: heartbeatRuns.nextEventSeq });
    return Number(updated!.nextEventSeq) - 1;
  });
}

export async function appendHeartbeatRunEvent(
  db: Db,
  input: AppendHeartbeatRunEventInput,
): Promise<AppendHeartbeatRunEventResult> {
  if (!input.nativeSource && !input.retryExhaustion) {
    // Fork: a claimed append must land only while the run row still carries
    // exactly this (ownerToken, fence) pair and is running. The ownership
    // predicate lives in the same UPDATE that allocates the sequence, so a
    // takeover committing between "check" and "insert" cannot admit a stale
    // executor's event, and the lease renews in that same statement.
    const claim = input.claim ?? null;
    const claimPredicate = claim
      ? sql`and ${heartbeatRuns.status} = 'running'
        and ${heartbeatRuns.ownerToken} = ${claim.ownerToken}
        and ${claim.fence === null ? sql`${heartbeatRuns.fence} is null` : sql`${heartbeatRuns.fence} = ${claim.fence}`}`
      : sql``;
    const leaseRenewal = claim
      ? sql`, lease_expires_at = now() + ${sqlMillisInterval(input.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS)},
          lease_renewed_at = now()`
      : sql``;
    const allocated = db.$with("allocated_run_event", {}).as(sql`
      update ${heartbeatRuns}
      set next_event_seq = greatest(${heartbeatRuns.nextEventSeq}, coalesce((
            select max(${heartbeatRunEvents.seq}) + 1 from ${heartbeatRunEvents}
            where ${heartbeatRunEvents.runId} = ${input.runId}
          ), 1)) + 1,
          updated_at = ${sql.param(new Date(), heartbeatRuns.updatedAt)}${leaseRenewal}
      where ${heartbeatRuns.id} = ${input.runId}
        and ${heartbeatRuns.companyId} = ${input.companyId}
        and ${heartbeatRuns.agentId} = ${input.agentId}
        ${claimPredicate}
      returning company_id, id as run_id, agent_id, next_event_seq - 1 as seq, fence as event_fence
    `);
    const appended = db.$with("appended_run_event", getTableColumns(heartbeatRunEvents)).as(sql`
      insert into ${heartbeatRunEvents}
        (company_id, run_id, agent_id, seq, event_type, stream, level, color, message, payload, fence)
      select company_id, run_id, agent_id, seq,
        ${input.eventType}, ${input.stream ?? null}, ${input.level ?? null},
        ${input.color ?? null}, ${input.message ?? null},
        ${sql.param(input.payload ?? null, heartbeatRunEvents.payload)},
        ${claim ? sql`event_fence` : sql`null`}
      from ${allocated}
      returning *
    `);
    const [row] = await db.with(allocated, appended).select().from(appended);
    if (!row) {
      if (claim) {
        const [boundRun] = await db
          .select({ id: heartbeatRuns.id })
          .from(heartbeatRuns)
          .where(
            and(
              eq(heartbeatRuns.id, input.runId),
              eq(heartbeatRuns.companyId, input.companyId),
              eq(heartbeatRuns.agentId, input.agentId),
            ),
          )
          .limit(1);
        if (!boundRun) throw new Error("heartbeat_run_event_binding_mismatch");
        return {
          row: null as unknown as typeof heartbeatRunEvents.$inferSelect,
          disposition: "rejected",
          highestContiguousSourceSeq: 0,
        };
      }
      throw new Error("heartbeat_run_event_binding_mismatch");
    }
    return { row, disposition: "committed", highestContiguousSourceSeq: 0 };
  }
  return db.transaction(async (tx) => {
    const run = await tx
      .select()
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, input.runId))
      .for("update")
      .limit(1)
      .then((rows) => rows[0] ?? null);
    if (!run || run.companyId !== input.companyId || run.agentId !== input.agentId) {
      throw new Error("heartbeat_run_event_binding_mismatch");
    }

    // Fork ownership fence: the row is locked FOR UPDATE above, so this
    // check + the lease renewal + the insert below are atomic with respect
    // to any concurrent takeover (claimExpiredLease also updates the row).
    let eventFence: number | null = null;
    if (input.claim) {
      const claimHeld =
        run.status === "running" &&
        run.ownerToken === input.claim.ownerToken &&
        (input.claim.fence === null ? run.fence === null : run.fence === input.claim.fence);
      if (!claimHeld) {
        return {
          row: null as unknown as typeof heartbeatRunEvents.$inferSelect,
          disposition: "rejected" as const,
          highestContiguousSourceSeq: 0,
        };
      }
      const leaseTtlMs = input.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;
      await tx
        .update(heartbeatRuns)
        .set({
          leaseExpiresAt: sql`now() + ${sqlMillisInterval(leaseTtlMs)}`,
          leaseRenewedAt: sql`now()`,
        })
        .where(eq(heartbeatRuns.id, input.runId));
      eventFence = run.fence;
    }

    if (input.retryExhaustion && !input.nativeSource) {
      // The run lock also serializes concurrent recovery checks across server
      // instances. Reusing the receipt must not allocate a sequence or publish
      // another live event on each scheduler tick.
      const existing = await tx
        .select()
        .from(heartbeatRunEvents)
        .where(and(
          eq(heartbeatRunEvents.companyId, input.companyId),
          eq(heartbeatRunEvents.runId, input.runId),
          eq(heartbeatRunEvents.agentId, input.agentId),
          eq(heartbeatRunEvents.eventType, "lifecycle"),
          sql`${heartbeatRunEvents.message} like 'Bounded retry exhausted%'`,
          sql`${heartbeatRunEvents.payload} @> ${JSON.stringify(input.retryExhaustion)}::jsonb`,
        ))
        .limit(1)
        .then((rows) => rows[0] ?? null);
      if (existing) {
        return {
          row: existing,
          disposition: "duplicate" as const,
          highestContiguousSourceSeq: 0,
        };
      }
    }

    const sourceHash = input.nativeSource
      ? nativeSha256(input.nativeSource.canonicalPayload)
      : null;
    if (input.nativeSource) {
      const existing = await tx
        .select()
        .from(heartbeatRunEvents)
        .where(and(
          eq(heartbeatRunEvents.runId, input.runId),
          or(
            eq(heartbeatRunEvents.sourceEventId, input.nativeSource.sourceEventId),
            and(
              eq(heartbeatRunEvents.sourceInstanceId, input.nativeSource.sourceInstanceId),
              eq(heartbeatRunEvents.sourceSeq, input.nativeSource.sourceSeq),
            ),
          ),
        ))
        .limit(1)
        .then((rows) => rows[0] ?? null);
      if (existing) {
        if (existing.sourcePayloadSha256 !== sourceHash) {
          throw new HeartbeatRunEventConflictError();
        }
        return {
          row: existing,
          disposition: "duplicate" as const,
          highestContiguousSourceSeq: await contiguousCursor(
            tx as unknown as Db,
            input.runId,
            input.nativeSource.sourceInstanceId,
          ),
        };
      }
    }

    const seq = await allocateHeartbeatRunEventSeq(
      tx as unknown as Db,
      input.runId,
    );
    const [row] = await tx.insert(heartbeatRunEvents).values({
      companyId: input.companyId,
      runId: input.runId,
      agentId: input.agentId,
      seq,
      eventType: input.eventType,
      stream: input.stream ?? null,
      level: input.level ?? null,
      color: input.color ?? null,
      message: input.message ?? null,
      payload: input.payload ?? null,
      sourceInstanceId: input.nativeSource?.sourceInstanceId ?? null,
      sourceEventId: input.nativeSource?.sourceEventId ?? null,
      sourceSeq: input.nativeSource?.sourceSeq ?? null,
      sourcePayloadSha256: sourceHash,
      protocolSchemaVersion: input.nativeSource?.protocolSchemaVersion ?? null,
      fence: eventFence,
    }).returning();
    if (!row) throw new Error("heartbeat_run_event_not_persisted");
    return {
      row,
      disposition: "committed" as const,
      highestContiguousSourceSeq: input.nativeSource
        ? await contiguousCursor(
            tx as unknown as Db,
            input.runId,
            input.nativeSource.sourceInstanceId,
          )
        : 0,
    };
  });
}

async function contiguousCursor(db: Db, runId: string, sourceInstanceId: string): Promise<number> {
  const rows = await db
    .select({ sourceSeq: heartbeatRunEvents.sourceSeq })
    .from(heartbeatRunEvents)
    .where(and(
      eq(heartbeatRunEvents.runId, runId),
      eq(heartbeatRunEvents.sourceInstanceId, sourceInstanceId),
    ))
    .orderBy(asc(heartbeatRunEvents.sourceSeq));
  let cursor = 0;
  for (const row of rows) {
    if (row.sourceSeq === cursor + 1) cursor += 1;
    else if (row.sourceSeq !== null && row.sourceSeq > cursor + 1) break;
  }
  return cursor;
}
