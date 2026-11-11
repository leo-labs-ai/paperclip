import { and, eq, isNull, sql } from "drizzle-orm";
import { logger } from "../../middleware/logger.js";
import { DEFAULT_LEASE_TTL_MS } from "../run-ownership-store.js";
import { heartbeatRuns, type Db } from "@paperclipai/db";
import type { AdapterInvocationMeta, AdapterRuntimeEvent } from "../../adapters/index.js";
import { redactCurrentUserText, type CurrentUserRedactionOptions } from "../../log-redaction.js";
import type { createAgentIdentityRedactor } from "../agent-identity-redaction.js";
import { touchHeartbeatRunRuntimeStatus } from "../heartbeat-run-runtime-status.js";
import { publishLiveEvent } from "../live-events.js";
import type { RunLogHandle, RunLogStore } from "../run-log-store.js";
import { appendExcerpt, compactRunLogChunk } from "./run-log.js";
import { isHeartbeatRunRuntimeStatusActive, publishHeartbeatRunRuntimeProgress, type createHeartbeatLifecycle, type HeartbeatExecutionWriteAuthority } from "./run-lifecycle.js";

const MAX_LIVE_LOG_CHUNK_BYTES = 8 * 1024;
export const ACTIVE_RUN_OUTPUT_PROGRESS_FLUSH_INTERVAL_MS = 60 * 1000;
export const ACTIVE_RUN_LOG_RUNTIME_STATUS_REFRESH_INTERVAL_MS = 5 * 1000;

type Run = typeof heartbeatRuns.$inferSelect;

export interface HeartbeatRunOutputInput {
  run: Run;
  authority: HeartbeatExecutionWriteAuthority;
  providerFallbackMetadata?: Record<string, unknown> | null;
  runId: string;
  issueId: string | null;
  runLogStore: Pick<RunLogStore, "begin" | "append">;
  identityRedactor: Pick<ReturnType<typeof createAgentIdentityRedactor>, "chunk" | "redact">;
  secretKeys: Set<string>;
  appendRunEvent: ReturnType<typeof createHeartbeatLifecycle>["appendRunEvent"];
  getCurrentUserRedactionOptions: () => Promise<CurrentUserRedactionOptions>;
}

/** One recorder per execution attempt; the log store itself stays service-scoped. */
export function createHeartbeatRunOutput(db: Db, input: HeartbeatRunOutputInput) {
  const { run, runId, issueId, runLogStore, identityRedactor, secretKeys, appendRunEvent, getCurrentUserRedactionOptions } = input;
  const authority = input.authority.kind === "legacy"
    ? { ...input.authority, executionClaim: { ...input.authority.executionClaim } }
    : input.authority;
  const executionClaim = authority.kind === "legacy" ? authority.executionClaim : undefined;
  const controllerBootId = authority.kind === "legacy" ? authority.executionControllerBootId : undefined;
  const ownedRun = and(
    eq(heartbeatRuns.id, run.id), eq(heartbeatRuns.companyId, run.companyId), eq(heartbeatRuns.agentId, run.agentId),
    eq(heartbeatRuns.status, "running"),
    authority.kind === "native" ? eq(heartbeatRuns.runtimeMode, "native") : and(
      sql`${heartbeatRuns.runtimeMode} is distinct from 'native'`,
      executionClaim?.ownerToken && controllerBootId ? and(
        eq(heartbeatRuns.ownerToken, executionClaim.ownerToken),
        executionClaim.fence === null ? isNull(heartbeatRuns.fence) : eq(heartbeatRuns.fence, executionClaim.fence),
        eq(heartbeatRuns.controllerBootId, controllerBootId),
      ) : sql`false`,
    ),
  );
  let handle: RunLogHandle | null = null;
  let stdoutExcerpt = "";
  let stderrExcerpt = "";
  let outputSeq = Number(run.lastOutputSeq ?? 0);
  let lastOutputFlushAt: Date | null = run.lastOutputAt ?? null;
  let lastLogRuntimeStatusTouchMs = 0;
  const outputProgressState: {
    pending: {
      at: Date;
      seq: number;
      stream: "stdout" | "stderr";
      bytes: number;
    } | null;
  } = { pending: null };
  let persistedLogBytes = Number(run.logBytes ?? 0);
  const flushOutputProgress = async (opts?: { force?: boolean }) => {
    const pendingOutputProgress = outputProgressState.pending;
    if (!pendingOutputProgress) return;
    const shouldFlush =
      opts?.force === true ||
      !lastOutputFlushAt ||
      pendingOutputProgress.at.getTime() - lastOutputFlushAt.getTime() >=
        ACTIVE_RUN_OUTPUT_PROGRESS_FLUSH_INTERVAL_MS;
    if (!shouldFlush) return;
    try {
      const [written] = await db
      .update(heartbeatRuns)
      .set({
        lastOutputAt: pendingOutputProgress.at,
        lastOutputSeq: pendingOutputProgress.seq,
        lastOutputStream: pendingOutputProgress.stream,
        lastOutputBytes: pendingOutputProgress.bytes,
        ...(authority.kind === "legacy" ? {
          leaseExpiresAt: sql`now() + (${DEFAULT_LEASE_TTL_MS}::text || ' milliseconds')::interval`,
          leaseRenewedAt: sql`now()`,
        } : {}),
        updatedAt: new Date(),
      })
      .where(ownedRun).returning({ id: heartbeatRuns.id });
      if (!written) return;
      lastOutputFlushAt = pendingOutputProgress.at;
      if (outputProgressState.pending === pendingOutputProgress) outputProgressState.pending = null;
    } catch (error) {
      logger.warn({ err: error, runId }, "failed to flush run output progress");
      if (opts?.force !== true) lastOutputFlushAt = new Date();
    }
  };
  // The caller's captured admission remains authoritative across all callbacks.
  async function begin(currentRun: Run) {
    const run = currentRun;
    handle = await db.transaction(async tx => {
      const [owned] = await tx.select({ id: heartbeatRuns.id }).from(heartbeatRuns)
        .where(ownedRun).for("update");
      if (!owned) throw new Error("Run output ownership changed before log binding");
      const admittedHandle = await runLogStore.begin({
        companyId: run.companyId,
        agentId: run.agentId,
        runId,
      });
      const [bound] = await tx.update(heartbeatRuns).set({
        logStore: admittedHandle.store,
        logRef: admittedHandle.logRef,
        updatedAt: new Date(),
      }).where(ownedRun).returning({ id: heartbeatRuns.id });
      if (!bound) throw new Error("Run output ownership changed before log binding");
      return admittedHandle;
    });

    const currentUserRedactionOptions =
      await getCurrentUserRedactionOptions();
    const appendIdentityRedactedLog = async (stream: "stdout" | "stderr", chunk: string) => {
      const sanitizedChunk = compactRunLogChunk(
        redactCurrentUserText(chunk, currentUserRedactionOptions),
      );
      let appendFailed = false;
      const recorded = await db.transaction(async tx => {
        const [owned] = await tx.select({ id: heartbeatRuns.id }).from(heartbeatRuns).where(ownedRun).for("update");
        if (!owned) return null;
        if (stream === "stdout")
          stdoutExcerpt = appendExcerpt(stdoutExcerpt, sanitizedChunk);
        if (stream === "stderr")
          stderrExcerpt = appendExcerpt(stderrExcerpt, sanitizedChunk);
        const ts = new Date().toISOString();

        outputSeq += 1;
        const chunkSeq = outputSeq;
        let appendedBytes = 0;
        if (handle) {
          try {
            appendedBytes = await runLogStore.append(handle, {
              stream,
              chunk: sanitizedChunk,
              ts,
              seq: chunkSeq,
            });
          } catch (error) {
            appendFailed = true;
            throw error;
          }
          persistedLogBytes += appendedBytes;
        }
        outputProgressState.pending = {
          at: new Date(ts),
          seq: chunkSeq,
          stream,
          bytes: persistedLogBytes,
        };
        return { ts, chunkSeq };
      }).catch((error: unknown) => {
        if (appendFailed) throw error;
        logger.warn({ err: error, runId }, "could not authorize run output; chunk publication skipped");
        return null;
      });
      if (!recorded) return;
      const { ts, chunkSeq } = recorded;
      await flushOutputProgress();

      // Streamed CLI output is real run activity: keep the in-memory
      // runtime status ("Working... / X ago") fresh between structured
      // events so sandbox runs with mid-run log streaming never show a
      // minutes-stale timestamp. Throttled to avoid churning the live
      // event stream on every 250ms tail chunk.
      const logActivityAt = new Date(ts);
      if (
        isHeartbeatRunRuntimeStatusActive(run.status) &&
        logActivityAt.getTime() - lastLogRuntimeStatusTouchMs >=
          ACTIVE_RUN_LOG_RUNTIME_STATUS_REFRESH_INTERVAL_MS
      ) {
        lastLogRuntimeStatusTouchMs = logActivityAt.getTime();
        const touchedStatus = touchHeartbeatRunRuntimeStatus({
          companyId: run.companyId,
          issueId,
          agentId: run.agentId,
          runId: run.id,
          at: logActivityAt,
        });
        if (touchedStatus)
          publishHeartbeatRunRuntimeProgress(touchedStatus);
      }

      const payloadChunk =
        sanitizedChunk.length > MAX_LIVE_LOG_CHUNK_BYTES
          ? sanitizedChunk.slice(
              sanitizedChunk.length - MAX_LIVE_LOG_CHUNK_BYTES,
            )
          : sanitizedChunk;

      publishLiveEvent({
        companyId: run.companyId,
        type: "heartbeat.run.log",
        payload: {
          runId: run.id,
          agentId: run.agentId,
          issueId,
          ts,
          seq: chunkSeq,
          stream,
          chunk: payloadChunk,
          truncated: payloadChunk.length !== sanitizedChunk.length,
        },
      });
    };
    const onLog = (stream: "stdout" | "stderr", chunk: string) =>
      appendIdentityRedactedLog(stream, identityRedactor.chunk(stream, chunk));
    const onAdapterMeta = async (meta: AdapterInvocationMeta) => {
      meta = identityRedactor.redact(meta);
      if (meta.env && secretKeys.size > 0) {
        for (const key of secretKeys) {
          if (key in meta.env) meta.env[key] = "***REDACTED***";
        }
      }
      await appendRunEvent(currentRun, {
        eventType: "adapter.invoke",
        stream: "system",
        level: "info",
        message: "adapter invocation",
        payload: {
          ...meta,
          ...(input.providerFallbackMetadata ? { providerFallback: input.providerFallbackMetadata } : {}),
        },
      }, executionClaim, false, controllerBootId);
    };

    const onAdapterEvent = async (event: AdapterRuntimeEvent) => {
      event = identityRedactor.redact(event);
      const eventType = event.eventType.trim();
      if (!eventType) return;
      await appendRunEvent(currentRun, {
        eventType: eventType.slice(0, 120),
        stream: event.stream,
        level: event.level,
        color: event.color,
        message: event.message,
        payload: event.payload,
      }, executionClaim, false, controllerBootId);
    };

    return { currentUserRedactionOptions, appendIdentityRedactedLog, onLog, onAdapterMeta, onAdapterEvent };
  }

  return {
    begin,
    get handle() { return handle; },
    outputProgressState,
    flushOutputProgress,
    // Finalization awaits I/O and can receive more output before persisting excerpts.
    get stdoutExcerpt() { return stdoutExcerpt; },
    get stderrExcerpt() { return stderrExcerpt; },
  };
}
