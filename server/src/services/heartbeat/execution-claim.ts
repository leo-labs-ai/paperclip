import { AsyncLocalStorage } from "node:async_hooks";
import { and, eq, sql } from "drizzle-orm";
import { heartbeatRuns } from "@paperclipai/db";
import type { RunClaim } from "../run-ownership-store.js";

/**
 * Execution-scoped ownership for one `executeRun` call.
 *
 * The `RunClaim` is captured exactly once, from the row returned by the
 * queued -> running admission (or adopted from the dispatcher that performed
 * that admission), and is never re-read from a later SELECT: after a takeover a
 * later SELECT reflects the new owner, and replaying it as "mine" would let a
 * superseded executor keep writing under someone else's identity.
 *
 * The executor, run-lifecycle writers and event appender live in different
 * modules, so the claim travels with the async execution context instead of
 * through every call signature. A scope only answers for its own run id, so
 * operator, recovery and reaper writes (which run outside the scope, or for a
 * different run) stay unfenced by it and keep their own authority.
 *
 * `released` flips when the executor's own fenced terminal write commits. From
 * that point the row is terminal and nobody can take it over, so the executor's
 * trailing bookkeeping (final events, liveness patches) must not be rejected
 * merely because the terminal write cleared the owner token.
 */
export type ExecutionClaimScope = {
  runId: string;
  claim: RunClaim | undefined;
  released: boolean;
};

const storage = new AsyncLocalStorage<ExecutionClaimScope>();

export function runInExecutionClaimScope<T>(runId: string, fn: (scope: ExecutionClaimScope) => T): T {
  const scope: ExecutionClaimScope = { runId, claim: undefined, released: false };
  return storage.run(scope, () => fn(scope));
}

export function bindExecutionClaim(scope: ExecutionClaimScope, claim: RunClaim | undefined) {
  scope.claim = claim;
}

/** The immutable claim the current execution holds for `runId`, if any. */
export function currentExecutionClaim(runId: string): RunClaim | undefined {
  const scope = storage.getStore();
  if (!scope || scope.runId !== runId || scope.released) return undefined;
  return scope.claim;
}

/** Called after this execution's own fenced terminal write committed. */
export function releaseExecutionClaim(runId: string) {
  const scope = storage.getStore();
  if (scope && scope.runId === runId) scope.released = true;
}

/** The claim to fence with: an explicit one wins over the ambient one. */
export function resolveRunClaim(runId: string, explicit?: RunClaim | null): RunClaim | undefined {
  return explicit ?? currentExecutionClaim(runId);
}

/** Build a claim from a row read at claim time (the successful admission). */
export function runClaimFrom(run: { ownerToken: string | null; fence: number | null }): RunClaim | undefined {
  return run.ownerToken ? { ownerToken: run.ownerToken, fence: run.fence } : undefined;
}

export function runClaimWhere(runId: string, claim: RunClaim | undefined) {
  return claim
    ? and(
        eq(heartbeatRuns.id, runId),
        eq(heartbeatRuns.ownerToken, claim.ownerToken),
        claim.fence === null ? sql`${heartbeatRuns.fence} is null` : eq(heartbeatRuns.fence, claim.fence),
      )
    : eq(heartbeatRuns.id, runId);
}

/** Terminal status clears ownership in the same statement as the status write. */
export function ownershipReleasePatch(terminal: boolean) {
  return terminal ? { ownerToken: null, leaseExpiresAt: null } : {};
}
