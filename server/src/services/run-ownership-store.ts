import { randomUUID } from "node:crypto";
import { and, eq, lt, asc, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { heartbeatRuns } from "@paperclipai/db";

/**
 * Durable leased ownership + monotonic fencing for heartbeat runs
 * (active-active reforge plan 003).
 *
 * The in-process `runningProcesses`/`activeRunExecutions` maps and
 * `withAgentStartLock` can only coordinate within one Paperclip process. This
 * store makes ownership of a running heartbeat run a fact in PostgreSQL:
 *
 * - `ownerToken` identifies the executor that currently holds the run.
 * - `fence` is a value minted from the global `heartbeat_run_fence_seq`
 *   sequence on every claim/takeover. Fence values are strictly increasing
 *   across the whole table.
 * - `leaseExpiresAt`/`leaseRenewedAt` bound how long a claim is presumed
 *   live. Expiry is a signal for reconciliation to investigate and make the
 *   loss operator-visible; it is NOT proof the previous holder stopped
 *   writing (a wedged process can outlive its lease). That is why every
 *   mutation is fenced on (ownerToken, fence) rather than trusting expiry
 *   alone.
 *
 * ## Correctness contract
 *
 * A caller is authorized to mutate a run's durable state ONLY while holding
 * an `{ ownerToken, fence }` pair that was minted for it — by
 * `claimHeartbeatRunSlot` (fresh claim) or `claimExpiredLease` (takeover) —
 * and never mutated afterward by the caller. Every write helper here
 * compares BOTH columns: `owner_token = $ownerToken AND fence = $fence`.
 * Comparing fence in addition to the (already globally-unique) owner token
 * is deliberate defense in depth: it also catches a caller that reused a
 * stale in-memory `run` row whose `ownerToken` field it forgot to refresh,
 * since the row's `fence` moves on every takeover even when a bug elsewhere
 * left the `ownerToken` field aliased.
 *
 * A caller MUST capture `{ ownerToken, fence }` exactly once, at the moment
 * it wins a claim/takeover, into an immutable binding, and thread that same
 * value through every subsequent write for the run's execution — never
 * re-read `ownerToken`/`fence` off a later `SELECT` of the row and pass that
 * back in as if it were still "mine". A later `SELECT` reflects whoever
 * currently owns the run, which after a takeover is no longer the caller.
 *
 * Every mutation helper here returns null (not throw) on a 0-row CAS
 * result, mirroring the compare-and-swap idiom already used by
 * heartbeat.ts's setRunStatusFromLive. Never throwing) means "I was
 * superseded" is an expected, cheap-to-check outcome, not an exception path.
 */

export const DEFAULT_LEASE_TTL_MS = 90_000;

/**
 * A bound-parameter millisecond interval. Replaces string interpolation into
 * `'<n> milliseconds'` casts: the value travels as an integer parameter and the
 * server multiplies a fixed `interval '1 millisecond'`, so nothing is spliced
 * into the SQL text and non-integer or negative input is normalised here.
 */
export function sqlMillisInterval(ms: number) {
  const whole = Number.isFinite(ms) ? Math.max(0, Math.trunc(ms)) : 0;
  return sql`(${whole}::bigint * interval '1 millisecond')`;
}

export function mintOwnerToken(): string {
  return randomUUID();
}

/**
 * Redact an owner token for logs/telemetry. Tokens are bearer-equivalent
 * within a lease window (holding one authorizes durable writes), so full
 * values must never reach logs, error payloads, or metrics labels — only
 * enough of the value to correlate two log lines about the same claim.
 */
export function redactOwnerToken(token: string | null | undefined): string {
  if (!token) return "none";
  return `${token.slice(0, 8)}…`;
}

export type HeartbeatRunRow = typeof heartbeatRuns.$inferSelect;

/** The claim identity a caller must capture once and thread immutably. */
export type RunClaim = { ownerToken: string; fence: number | null };

// db.execute(sql`...`) bypasses drizzle's column mapping entirely: rows come
// back keyed by raw Postgres column names (snake_case, e.g. `owner_token`),
// not the camelCase JS field names (`ownerToken`) every other helper in this
// file — and every caller typed against HeartbeatRunRow — expects. It also
// leaves bigint (oid 20) and timestamptz columns as raw strings, since
// heartbeat_runs declares fence/logBytes/lastOutputBytes as
// `bigint({ mode: "number" })` and every *At column as `timestamp`, a
// conversion only the query builder (select/update/.returning()) applies.
// Every raw-SQL helper below must normalize its rows through this or every
// consumer silently gets undefined fields, numeric-looking strings, or
// strings where a Date is expected.
function toCamelCase(key: string): string {
  return key.replace(/_([a-z0-9])/g, (_, c: string) => c.toUpperCase());
}

const BIGINT_NUMBER_FIELDS = new Set(["fence", "logBytes", "lastOutputBytes"]);

function normalizeHeartbeatRunRow(row: Record<string, unknown>): HeartbeatRunRow {
  const normalized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(row)) {
    const camelKey = toCamelCase(key);
    if (BIGINT_NUMBER_FIELDS.has(camelKey) && typeof value === "string") {
      normalized[camelKey] = Number(value);
    } else if (camelKey.endsWith("At") && typeof value === "string") {
      normalized[camelKey] = new Date(value);
    } else {
      normalized[camelKey] = value;
    }
  }
  return normalized as HeartbeatRunRow;
}

function claimCondition(runId: string, claim: RunClaim) {
  return and(
    eq(heartbeatRuns.id, runId),
    eq(heartbeatRuns.ownerToken, claim.ownerToken),
    claim.fence === null ? sql`${heartbeatRuns.fence} is null` : eq(heartbeatRuns.fence, claim.fence),
  );
}

/**
 * Renew the lease for the run currently held by `claim`. No-ops (returns
 * null) if the run is no longer running or ownership/fence moved on.
 */
export async function renewLease(
  db: Db,
  input: { runId: string; claim: RunClaim; leaseTtlMs?: number; now?: Date },
): Promise<HeartbeatRunRow | null> {
  const now = input.now ?? new Date();
  const leaseExpiresAt = new Date(now.getTime() + (input.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS));
  return db
    .update(heartbeatRuns)
    .set({ leaseExpiresAt, leaseRenewedAt: now, updatedAt: now })
    .where(and(claimCondition(input.runId, input.claim), eq(heartbeatRuns.status, "running")))
    .returning()
    .then((rows) => rows[0] ?? null);
}

/**
 * Release ownership on graceful finalization. Clears owner_token/lease so a
 * finished run reads as unowned, but deliberately leaves `fence` untouched —
 * fence is a monotonic audit trail, not a reset-per-run counter.
 */
export async function releaseRunOwnership(
  db: Db,
  input: { runId: string; claim: RunClaim },
): Promise<HeartbeatRunRow | null> {
  return db
    .update(heartbeatRuns)
    .set({ ownerToken: null, leaseExpiresAt: null, updatedAt: new Date() })
    .where(claimCondition(input.runId, input.claim))
    .returning()
    .then((rows) => rows[0] ?? null);
}

/**
 * Real expired-lease takeover: atomically hand ownership to a new claimant
 * with a strictly higher fence, but ONLY if the lease is expired according
 * to PostgreSQL's own clock (`now()`), not the caller's wall clock. Using
 * the DB clock is load-bearing: two replicas racing this takeover must agree
 * on "is the lease actually expired" from a single canonical clock, or clock
 * skew between them could let a replica take over a lease that has not
 * really lapsed, or fail to take over one that has.
 *
 * This is a single conditional UPDATE, so at most one racing caller can win
 * it — the loser's UPDATE matches 0 rows and gets null back, exactly like
 * every other CAS helper in this file. The winner's new `{ ownerToken,
 * fence }` becomes the immutable claim it must thread through the rest of
 * that run's execution.
 */
export async function claimExpiredLease(
  db: Db,
  input: { runId: string; graceMs?: number; leaseTtlMs?: number },
): Promise<HeartbeatRunRow | null> {
  const graceMs = input.graceMs ?? 0;
  const leaseTtlMs = input.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;
  const ownerToken = mintOwnerToken();
  const rows = (await db.execute(sql`
    UPDATE heartbeat_runs
    SET owner_token = ${ownerToken},
        fence = nextval('heartbeat_run_fence_seq'),
        lease_expires_at = now() + ${sqlMillisInterval(leaseTtlMs)},
        lease_renewed_at = now(),
        claim_attempt = claim_attempt + 1,
        updated_at = now()
    WHERE id = ${input.runId}
      AND status = 'running'
      AND lease_expires_at is not null
      AND lease_expires_at < now() - ${sqlMillisInterval(graceMs)}
    RETURNING *
  `)) as unknown as Array<Record<string, unknown>>;
  return rows[0] ? normalizeHeartbeatRunRow(rows[0]) : null;
}

/**
 * Reconciliation read: running rows whose lease has lapsed by more than
 * `graceMs`, oldest first (using PostgreSQL's clock via `now()`, for the
 * same clock-skew reason as claimExpiredLease). Read-only by design — this
 * store does not decide how to recover a lost run. Local-child-process
 * adapters cannot be safely adopted from a fresh executor (no durable
 * idempotency/status probe exists yet), so callers must not use this to
 * auto-restart execution; it exists to make loss operator-visible.
 */
export async function findExpiredLeaseRuns(
  db: Db,
  input: { graceMs?: number; limit?: number } = {},
): Promise<HeartbeatRunRow[]> {
  const graceMs = input.graceMs ?? 0;
  return db
    .select()
    .from(heartbeatRuns)
    .where(
      and(
        eq(heartbeatRuns.status, "running"),
        sql`${heartbeatRuns.leaseExpiresAt} is not null`,
        lt(heartbeatRuns.leaseExpiresAt, sql`now() - ${sqlMillisInterval(graceMs)}`),
      ),
    )
    .orderBy(asc(heartbeatRuns.leaseExpiresAt))
    .limit(input.limit ?? 100);
}

/**
 * Atomically verify the caller still holds the run AND append a fenced
 * event row in one statement. This replaces the earlier two-round-trip
 * "renew lease, then separately insert" sequence: between those two
 * round trips a takeover could land, and the second write (the insert)
 * had nothing left to check against, so a stale owner's event could still
 * land after it had already lost ownership. Here the INSERT only executes
 * against rows produced by the lease-renewal CTE, so the check and the
 * insert are one atomic unit — there is no window for a concurrent
 * takeover to slip between them.
 *
 * Returns null when the caller's claim no longer matches the row (stale
 * owner or wrong fence) — the caller should drop the event rather than
 * insert it unfenced.
 */
export async function appendFencedRunEvent(
  db: Db,
  input: {
    runId: string;
    claim: RunClaim;
    companyId: string;
    agentId: string;
    seq: number;
    eventType: string;
    stream?: string | null;
    level?: string | null;
    color?: string | null;
    message?: string | null;
    payload?: Record<string, unknown> | null;
    leaseTtlMs?: number;
  },
): Promise<{ eventId: number; fence: number | null } | null> {
  const leaseTtlMs = input.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;
  const fenceMatch =
    input.claim.fence === null ? sql`fence is null` : sql`fence = ${input.claim.fence}`;
  const rows = (await db.execute(sql`
    WITH lease AS (
      UPDATE heartbeat_runs
      SET lease_expires_at = now() + ${sqlMillisInterval(leaseTtlMs)},
          lease_renewed_at = now(),
          updated_at = now()
      WHERE id = ${input.runId}
        AND owner_token = ${input.claim.ownerToken}
        AND ${fenceMatch}
        AND status = 'running'
      RETURNING fence
    )
    INSERT INTO heartbeat_run_events
      (company_id, run_id, agent_id, seq, event_type, stream, level, color, message, payload, fence)
    SELECT ${input.companyId}, ${input.runId}, ${input.agentId}, ${input.seq}, ${input.eventType},
           ${input.stream ?? null}, ${input.level ?? null}, ${input.color ?? null}, ${input.message ?? null},
           ${input.payload ? JSON.stringify(input.payload) : null}::jsonb, lease.fence
    FROM lease
    RETURNING id, fence
  `)) as unknown as Array<{ id: number | string; fence: number | string | null }>;
  const row = rows[0];
  if (!row) return null;
  return {
    eventId: typeof row.id === "string" ? Number(row.id) : row.id,
    fence: typeof row.fence === "string" ? Number(row.fence) : row.fence,
  };
}

/**
 * Fenced mid-execution patch: any non-terminal write executeRun makes to a
 * run's row (contextSnapshot, session bookkeeping, log handle, output
 * progress, ...) while it executes, conditioned on the same immutable claim
 * as every other mutation. Returns null on a lost/superseded claim instead
 * of writing unconditionally — callers must not blindly fold the write's
 * absence into local state (e.g. reassigning a local `run` variable) without
 * checking for null first.
 *
 * Also renews the lease, using PostgreSQL's own clock (`now()`), the same as
 * every other lease-touching helper here — this is one of the two places
 * (with `appendFencedRunEvent`) an in-progress execution keeps its lease
 * alive between explicit `renewLease` calls, so a run that is actively
 * mutating its context/session state must not have its lease reaped out from
 * under it.
 */
export async function writeFencedRunPatch(
  db: Db,
  input: { runId: string; claim: RunClaim; patch: Partial<typeof heartbeatRuns.$inferInsert>; leaseTtlMs?: number },
): Promise<HeartbeatRunRow | null> {
  const leaseTtlMs = input.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;
  return db
    .update(heartbeatRuns)
    .set({
      ...input.patch,
      leaseExpiresAt: sql`now() + ${sqlMillisInterval(leaseTtlMs)}`,
      leaseRenewedAt: sql`now()`,
      updatedAt: sql`now()`,
    })
    .where(claimCondition(input.runId, input.claim))
    .returning()
    .then((rows) => rows[0] ?? null);
}

/**
 * Reread the row and report whether `claim` is actually stale (owner_token
 * and/or fence no longer match what's on the row right now). A 0-row CAS
 * result is not on its own proof of a stale claim — e.g.
 * `setRunStatusFromLive`'s `fromStatuses` guard can also cause it, and a row
 * that no longer exists is not an ownership dispute either. Callers must
 * check this before logging `stale_write_rejected`, or the signal stops
 * meaning "another owner took this run" and starts meaning "any CAS miss for
 * any reason", which is not alertable.
 */
export async function isClaimStale(db: Db, input: { runId: string; claim: RunClaim }): Promise<boolean> {
  const [row] = await db
    .select({ ownerToken: heartbeatRuns.ownerToken, fence: heartbeatRuns.fence })
    .from(heartbeatRuns)
    .where(eq(heartbeatRuns.id, input.runId));
  if (!row) return false;
  return row.ownerToken !== input.claim.ownerToken || row.fence !== input.claim.fence;
}

/**
 * Stale-owner rejection telemetry. Kept as a dedicated call (rather than an
 * inline `logger.warn`) so it is one grep target and one alertable event
 * name, distinct from ordinary status-transition-lost-the-race logging —
 * an operator watching for active-active correctness regressions should be
 * able to alert on this event name alone. Never logs the raw token.
 */
export function describeStaleOwnershipRejection(input: {
  runId: string;
  claim: RunClaim;
  context: string;
}): { event: string; fields: Record<string, unknown> } {
  return {
    event: "heartbeat.run_ownership.stale_write_rejected",
    fields: {
      runId: input.runId,
      ownerToken: redactOwnerToken(input.claim.ownerToken),
      fence: input.claim.fence,
      context: input.context,
    },
  };
}
