import { and, eq } from "drizzle-orm";
import { isDeepStrictEqual } from "node:util";
import { activityLog, heartbeatRuns, issues, type Db } from "@paperclipai/db";
import { conflict, forbidden, HttpError } from "../errors.js";
import { logActivity, persistActivity, type ActivityPublication } from "./activity-log.js";
import {
  resolveTaskWatchdogMutationScope,
  taskWatchdogScopeAllowsIssueMutation,
} from "./task-watchdog-scope.js";
import { taskWatchdogService } from "./task-watchdogs.js";

type Transaction = Parameters<Parameters<Db["transaction"]>[0]>[0];
type RecoveryActor = { type: "agent"; agentId: string; companyId: string; runId: string };
type WatchdogScope = Extract<Awaited<ReturnType<typeof resolveTaskWatchdogMutationScope>>, { kind: "watchdog" }>;

export async function recordTaskWatchdogRecoveryStale(
  db: Db,
  actor: { agentId: string; runId: string },
  scope: WatchdogScope,
  observedStopFingerprint: string,
  reason: string,
) {
  await logActivity(db, {
    companyId: scope.companyId,
    actorType: "agent",
    actorId: actor.agentId,
    agentId: actor.agentId,
    runId: actor.runId,
    action: "issue.task_watchdog_recovery_stale",
    entityType: "issue",
    entityId: scope.watchdogIssueId ?? scope.watchedIssueId,
    details: {
      watchdogId: scope.watchdogId,
      sourceIssueId: scope.watchedIssueId,
      watchdogIssueId: scope.watchdogIssueId,
      stopFingerprint: observedStopFingerprint,
      reason,
    },
  });
}

export async function applyTaskWatchdogRecovery<T>(
  db: Db,
  input: {
    actor: RecoveryActor;
    issueId: string;
    observedStopFingerprint: string;
    expectedIssue: typeof issues.$inferSelect;
    mutations: ("status" | "comment")[];
  },
  apply: (tx: Transaction) => Promise<T>,
  publications: ActivityPublication[],
) {
  let recoveryScope: WatchdogScope | null = null;
  try {
    return await db.transaction(async (tx) => {
      const [run] = await tx.select().from(heartbeatRuns).where(and(
        eq(heartbeatRuns.id, input.actor.runId),
        eq(heartbeatRuns.companyId, input.actor.companyId),
        eq(heartbeatRuns.agentId, input.actor.agentId),
      )).for("update");
      if (!run || run.status !== "running") {
        throw forbidden("Watchdog recovery requires this agent's running heartbeat.");
      }
      const [priorRecovery] = await tx.select({ id: activityLog.id }).from(activityLog).where(and(
        eq(activityLog.companyId, input.actor.companyId),
        eq(activityLog.runId, run.id),
        eq(activityLog.action, "issue.task_watchdog_recovery_applied"),
      )).limit(1);
      if (priorRecovery) {
        throw conflict("This watchdog run has already committed its recovery.");
      }
      const scope = await resolveTaskWatchdogMutationScope(tx as unknown as Db, input.actor);
      if (scope.kind !== "watchdog") {
        throw forbidden("Recovery requires an active persisted task watchdog.");
      }
      recoveryScope = scope;
      const [issue] = await tx.select().from(issues).where(and(
        eq(issues.id, input.issueId),
        eq(issues.companyId, scope.companyId),
      )).for("update");
      if (!issue || (await taskWatchdogScopeAllowsIssueMutation(
        tx as unknown as Db, scope, issue, { allowWatchdogIssue: false },
      )).kind !== "watchdog") {
        throw forbidden("Recovery must target the watched issue subtree.");
      }
      if (Object.entries(issue).some(([key, value]) =>
        !isDeepStrictEqual(value, input.expectedIssue[key as keyof typeof issue])
      )) {
        throw conflict("The recovery target changed during authorization; read it again.", {
          code: "task_watchdog_recovery_stale",
        });
      }
      const revalidated = await taskWatchdogService(tx as unknown as Db).revalidateMutationScope({
        ...scope,
        stopFingerprint: input.observedStopFingerprint,
      });
      if (!revalidated.allowed) {
        throw conflict(revalidated.reason, {
          code: "task_watchdog_recovery_stale",
          watchdogId: scope.watchdogId,
          watchedIssueId: scope.watchedIssueId,
          watchdogIssueId: scope.watchdogIssueId,
          observedStopFingerprint: input.observedStopFingerprint,
          currentState: revalidated.classification?.state ?? null,
        });
      }
      const result = await apply(tx);
      const { publication } = await persistActivity(tx as unknown as Db, {
        companyId: scope.companyId,
        actorType: "agent",
        actorId: input.actor.agentId,
        agentId: input.actor.agentId,
        runId: run.id,
        action: "issue.task_watchdog_recovery_applied",
        entityType: "issue",
        entityId: issue.id,
        details: {
          watchdogId: scope.watchdogId,
          sourceIssueId: scope.watchedIssueId,
          watchdogIssueId: scope.watchdogIssueId,
          stopFingerprint: input.observedStopFingerprint,
          runStopFingerprint: scope.stopFingerprint,
          mutations: input.mutations,
        },
      });
      publications.push(publication);
      const finalState = await taskWatchdogService(tx as unknown as Db).revalidateMutationScope({
        ...scope,
        stopFingerprint: input.observedStopFingerprint,
      });
      const before = revalidated.classification;
      const after = finalState.classification;
      // A status change made by this batch is expected; any other material change is stale.
      const unchangedLeaves = (leaves: typeof before.stopSnapshot.materialLeaves) =>
        leaves.map((leaf) => ({
          ...leaf,
          status: leaf.issueId === issue.id ? null : leaf.status,
        }));
      if (
        after?.state !== "stopped" ||
        !isDeepStrictEqual(before.includedIssueIds, after.includedIssueIds) ||
        !isDeepStrictEqual(unchangedLeaves(before.stopSnapshot.materialLeaves), unchangedLeaves(after.stopSnapshot.materialLeaves)) ||
        !isDeepStrictEqual(before.stopSnapshot.waitsByIssueId, after.stopSnapshot.waitsByIssueId)
      ) {
        throw conflict("The watched subtree changed while recovery was being applied.", {
          code: "task_watchdog_recovery_stale",
        });
      }
      return result;
    });
  } catch (error) {
    const databaseCode = (error as { code?: string; cause?: { code?: string } })?.code
      ?? (error as { cause?: { code?: string } })?.cause?.code;
    const stale = error instanceof HttpError &&
      (error.details as { code?: string } | undefined)?.code === "task_watchdog_recovery_stale";
    if (recoveryScope && (stale || databaseCode === "40001")) {
      await recordTaskWatchdogRecoveryStale(
        db, input.actor, recoveryScope, input.observedStopFingerprint,
        stale ? (error as Error).message : "Concurrent state change aborted the recovery transaction.",
      );
    }
    if (databaseCode === "40001") {
      throw conflict("Concurrent state change aborted recovery; observe the subtree again.");
    }
    throw error;
  }
}
