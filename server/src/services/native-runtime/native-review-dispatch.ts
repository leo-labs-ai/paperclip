import { withExternalAdmissionGuard, assertNoExternalOverlap } from "../../modules/external-agents/index.js";
import { agentWakeupRequests, heartbeatRuns, issues, type Db } from "@paperclipai/db";
import { and, eq } from "drizzle-orm";
import type { PgUpdateSetSource } from "drizzle-orm/pg-core";
import { claimNativeReviewExecutionLock, readNativeReviewAssignmentContext } from "./native-review-participant.js";

/** The run, wake, and issue lock form one reviewer admission claim. */
export async function claimQueuedNativeReviewRun(db: Db, input: {
  run: typeof heartbeatRuns.$inferSelect;
  agentNameKey: string | null;
  claimedAt: Date;
  claimValues: PgUpdateSetSource<typeof heartbeatRuns>;
  /**
   * Runs inside this claim transaction after the row locks (the slot lock itself was
   * taken first via `lockSlot`), so
   * slot admission (advisory lock, strict caps) commits atomically with the
   * queued -> running UPDATE. Returning false leaves the run queued.
   */
  admit?: (tx: Db) => Promise<boolean>;
  /** Takes the per-agent slot lock; runs first, before the issue/wake/run row locks (see acquireRunSlotLock). */
  lockSlot?: (tx: Db) => Promise<void>;
}): Promise<typeof heartbeatRuns.$inferSelect | null> {
  const context = input.run.contextSnapshot as Record<string, unknown> | null;
  const issueId = context?.issueId;
  if (typeof issueId !== "string" || !readNativeReviewAssignmentContext(context)) return null;
  return db.transaction(async (tx) => {
    await input.lockSlot?.(tx as unknown as Db);
    // Match the queue editor's lock order: issue, wake, then run.
    const [issue] = await tx.select({ id: issues.id }).from(issues).where(and(
      eq(issues.id, issueId), eq(issues.companyId, input.run.companyId),
    )).for("update");
    if (!issue) return null;
    if (input.run.wakeupRequestId) {
      const [wake] = await tx.select().from(agentWakeupRequests).where(and(
        eq(agentWakeupRequests.id, input.run.wakeupRequestId),
        eq(agentWakeupRequests.companyId, input.run.companyId),
        eq(agentWakeupRequests.agentId, input.run.agentId),
      )).for("update");
      if (!wake || wake.status !== "queued" || wake.runId !== input.run.id) return null;
    }
    const [run] = await tx.select().from(heartbeatRuns).where(and(
      eq(heartbeatRuns.id, input.run.id), eq(heartbeatRuns.companyId, input.run.companyId),
      eq(heartbeatRuns.agentId, input.run.agentId),
    )).for("update");
    if (!run || run.status !== "queued" || run.wakeupRequestId !== input.run.wakeupRequestId) return null;
    if ((run.contextSnapshot as Record<string, unknown> | null)?.issueId !== issueId) return null;
    await withExternalAdmissionGuard(tx, run.companyId, run.agentId, () => assertNoExternalOverlap(tx, run.companyId, run.agentId));
    if (input.admit && !(await input.admit(tx as unknown as Db))) return null;
    const locked = await claimNativeReviewExecutionLock(tx as unknown as Db, {
      companyId: run.companyId, issueId, agentId: run.agentId, runId: run.id,
      contextSnapshot: context, agentNameKey: input.agentNameKey, claimedAt: input.claimedAt,
    });
    if (!locked) return null;
    const [claimed] = await tx.update(heartbeatRuns).set({
      ...input.claimValues, status: "running", startedAt: run.startedAt ?? input.claimedAt,
      updatedAt: input.claimedAt,
    }).where(eq(heartbeatRuns.id, run.id)).returning();
    if (run.wakeupRequestId) await tx.update(agentWakeupRequests).set({
      status: "claimed", claimedAt: input.claimedAt, updatedAt: input.claimedAt,
    }).where(eq(agentWakeupRequests.id, run.wakeupRequestId));
    return claimed ?? null;
  });
}
