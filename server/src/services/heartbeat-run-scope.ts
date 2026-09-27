import { and, eq, type SQL } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { heartbeatRuns } from "@paperclipai/db";
import { isRunId } from "@paperclipai/shared";
import { HttpError } from "../errors.js";

/**
 * Documented client code for a syntactically valid run id that is not a
 * heartbeat run in the caller's company, and for an agent actor that agent's
 * own run. Mirrors `invalid_run_id` (malformed header) but uses 422 because
 * the UUID shape is valid.
 *
 * Response shape from the error handler:
 * `{ error: "unknown_run_id", code: "unknown_run_id", details: { code, source } }`
 * The body must not include SQL, query text, or bind parameters.
 */
export const UNKNOWN_RUN_ID_ERROR = "unknown_run_id";

const HEARTBEAT_RUN_FK =
  /(?:checkout_run_id|execution_run_id|run_id)_heartbeat_runs_id_fk/;

export function unknownRunIdError(): HttpError {
  return new HttpError(422, UNKNOWN_RUN_ID_ERROR, {
    code: UNKNOWN_RUN_ID_ERROR,
    source: "header",
  });
}

type CauseCarrier = {
  code?: unknown;
  constraint?: unknown;
  constraint_name?: unknown;
  message?: unknown;
  cause?: unknown;
};

function constraintText(candidate: CauseCarrier): string {
  const named = candidate.constraint ?? candidate.constraint_name;
  const parts = [
    typeof named === "string" ? named : "",
    typeof candidate.message === "string" ? candidate.message : "",
  ];
  return parts.filter(Boolean).join(" ");
}

/**
 * True when Postgres rejected a write because checkout_run_id, execution_run_id,
 * or activity run_id does not reference heartbeat_runs. Other 23503 violations
 * (company, agent, issue) must not be relabeled.
 */
export function isHeartbeatRunReferenceViolation(error: unknown): boolean {
  let current: unknown = error;
  const seen = new Set<unknown>();
  for (let depth = 0; depth < 6 && current && typeof current === "object"; depth += 1) {
    if (seen.has(current)) break;
    seen.add(current);
    const candidate = current as CauseCarrier;
    const text = constraintText(candidate);
    if (candidate.code === "23503" && HEARTBEAT_RUN_FK.test(text)) return true;
    if (HEARTBEAT_RUN_FK.test(text) && text.includes("23503")) return true;
    current = candidate.cause;
  }
  return false;
}

export type HeartbeatRunScope = {
  runId: string | null | undefined;
  companyId: string;
  /**
   * When set, the run must belong to this agent. Board and system actors pass
   * null so only the company boundary is enforced.
   */
  agentId: string | null;
};

type RunReader = Pick<Db, "select">;
type RunTx = Parameters<Parameters<Db["transaction"]>[0]>[0];

function scopeConditions(input: HeartbeatRunScope & { runId: string }): SQL[] {
  const conditions: SQL[] = [
    eq(heartbeatRuns.id, input.runId),
    eq(heartbeatRuns.companyId, input.companyId),
  ];
  if (input.agentId) conditions.push(eq(heartbeatRuns.agentId, input.agentId));
  return conditions;
}

function normalizeRunId(runId: string | null | undefined): string | null {
  if (typeof runId !== "string") return null;
  const normalized = runId.trim().toLowerCase();
  if (!normalized) return null;
  // Same canonical shape as the auth header parser. A non-UUID must not reach
  // Postgres (invalid uuid input is a 500 that can echo the statement).
  if (!isRunId(normalized)) throw unknownRunIdError();
  return normalized;
}

/**
 * Unlocked existence check. Use it before side effects such as workspace
 * reopen. It is not a lock: callers that write the id must also take
 * {@link lockScopedHeartbeatRun} in the write transaction or map the FK race.
 */
export async function assertScopedHeartbeatRun(
  db: RunReader,
  input: HeartbeatRunScope,
): Promise<void> {
  const runId = normalizeRunId(input.runId);
  if (!runId) return;
  const row = await db
    .select({ id: heartbeatRuns.id })
    .from(heartbeatRuns)
    .where(and(...scopeConditions({ ...input, runId })))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!row) throw unknownRunIdError();
}

/**
 * Locks the in-scope run row against delete and primary-key reuse until the
 * surrounding transaction commits. `FOR KEY SHARE` does not block ordinary
 * heartbeat status updates. A missing or out-of-scope id throws
 * `unknown_run_id` and must not be followed by a write of that id.
 */
export async function lockScopedHeartbeatRun(
  tx: RunTx,
  input: HeartbeatRunScope & { runId: string },
): Promise<void> {
  const runId = normalizeRunId(input.runId);
  if (!runId) return;
  const row = await tx
    .select({ id: heartbeatRuns.id })
    .from(heartbeatRuns)
    .where(and(...scopeConditions({ ...input, runId })))
    .for("key share")
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!row) throw unknownRunIdError();
}

/**
 * Runs `write` in a transaction that holds the scoped run lock when a run id
 * is present. Maps a heartbeat-run FK race to `unknown_run_id` so a delete
 * between an earlier unlocked check and the write cannot surface as a 500.
 */
export async function withScopedHeartbeatRun<T>(
  db: Db,
  input: HeartbeatRunScope,
  write: (tx: Db) => Promise<T>,
): Promise<T> {
  const runId = normalizeRunId(input.runId);
  if (!runId) return write(db);
  try {
    return await db.transaction(async (tx) => {
      await lockScopedHeartbeatRun(tx, { ...input, runId });
      return write(tx as unknown as Db);
    });
  } catch (error) {
    if (error instanceof HttpError) throw error;
    if (isHeartbeatRunReferenceViolation(error)) throw unknownRunIdError();
    throw error;
  }
}
