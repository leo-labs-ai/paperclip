/**
 * PluginJobScheduler — tick-based scheduler for plugin scheduled jobs.
 *
 * The scheduler is the central coordinator for all plugin cron jobs. It
 * periodically ticks (default every 30 seconds), queries the `plugin_jobs`
 * table for jobs whose `nextRunAt` has passed, dispatches `runJob` RPC calls
 * to the appropriate worker processes, records each execution in the
 * `plugin_job_runs` table, and advances the scheduling pointer.
 *
 * ## Responsibilities
 *
 * 1. **Tick loop** — A `setInterval`-based loop fires every `tickIntervalMs`
 *    (default 30s). Each tick scans for due jobs and dispatches them.
 *
 * 2. **Cron parsing & next-run calculation** — Uses the lightweight built-in
 *    cron parser ({@link parseCron}, {@link nextCronTick}) to compute the
 *    `nextRunAt` timestamp after each run or when a new job is registered.
 *
 * 3. **Overlap prevention** — Before dispatching a job, the scheduler checks
 *    for an existing `running` run for the same job. If one exists, the job
 *    is skipped for that tick.
 *
 * 4. **Job run recording** — Every execution creates a `plugin_job_runs` row:
 *    `queued` → `running` → `succeeded` | `failed`. Duration and error are
 *    captured.
 *
 * 5. **Lifecycle integration** — The scheduler exposes `registerPlugin()` and
 *    `unregisterPlugin()` so the host lifecycle manager can wire up job
 *    scheduling when plugins start/stop. On registration, the scheduler
 *    computes `nextRunAt` for all active jobs that don't already have one.
 *
 * 6. **Expiry reconciliation** — A second `setInterval` loop (default 60s,
 *    independent of the tick loop) sweeps occurrences whose lease has
 *    genuinely expired (DB clock, plus a grace period) and takes each one
 *    over via `takeoverExpiredOccurrence`, settling it at the terminal
 *    `"unknown"` outcome. This never re-dispatches the job — see
 *    plugin-job-claims-store.ts's module doc for why blind replay is unsafe.
 *    Without this sweep, an occurrence whose worker crashed mid-lease (no
 *    completion write, no acknowledgement-revoke) would stay `running`
 *    forever. Dig 2026-09-02: lease churn under the old 90s TTL + 10-way
 *    concurrency is mitigated by a longer occurrence TTL, slightly lower
 *    maxConcurrentJobs, and richer takeover metrics (never silent unknown).
 *
 * @see PLUGIN_SPEC.md §17 — Scheduled Jobs
 * @see ./plugin-job-store.ts — Persistence layer
 * @see ./cron.ts — Cron parsing utilities
 */

import type { Db } from "@paperclipai/db";
import { pluginJobs } from "@paperclipai/db";
import { and, eq, isNull } from "drizzle-orm";
import type { PluginJobStore } from "./plugin-job-store.js";
import type { PluginWorkerManager } from "./plugin-worker-manager.js";
import { parseCron, nextCronTick, validateCron } from "./cron.js";
import type pino from "pino";
import { logger } from "../middleware/logger.js";
import {
  acknowledgeOccurrence,
  claimDueOccurrences,
  claimManualOccurrence,
  completeOccurrence,
  DEFAULT_OCCURRENCE_LEASE_TTL_MS,
  describeStaleOccurrenceRejection,
  findExpiredOccurrences,
  isOccurrenceClaimStale,
  renewOccurrenceLease,
  revokeUnacknowledgedOccurrences,
  takeoverExpiredOccurrence,
  type ClaimedOccurrence,
  type OccurrenceClaim,
  type PluginJobOccurrenceRow,
  type PluginJobRunRow,
} from "./plugin-job-claims-store.js";
import type { PluginJobRunStatus } from "@paperclipai/shared";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default interval between scheduler ticks (30 seconds). */
const DEFAULT_TICK_INTERVAL_MS = 30_000;

/** Default timeout for a runJob RPC call (5 minutes). */
const DEFAULT_JOB_TIMEOUT_MS = 5 * 60 * 1_000;

/**
 * Maximum number of concurrent job executions across all plugins.
 * Dig 2026-09-02: 10 concurrent runJob RPCs on one event loop correlated with
 * occurrence-lease renewal slips → expired-lease takeover churn. Slightly
 * lower default reduces renewal timer starvation; override via options if a
 * host needs more throughput.
 */
const DEFAULT_MAX_CONCURRENT_JOBS = 6;

/** Default interval between expiry-reconciliation sweeps (60 seconds). */
const DEFAULT_RECONCILIATION_INTERVAL_MS = 60_000;

/**
 * Default grace period added on top of an occurrence's own lease expiry
 * before reconciliation will take it over (30 seconds). Slack for
 * clock/scheduling jitter around the lease-renewal cadence
 * (`leaseTtlMs / 3`, i.e. every ~60s for the default 180s occurrence TTL) —
 * a genuinely healthy renewal that lands a beat late must not be raced by
 * reconciliation. Dig 2026-09-02 / Captain: keep occurrence TTL at 180s as
 * the intentional lease slack; do not double grace to 60s — that widens the
 * expired-lease overlap where a new claim can start beside a still-running
 * job.
 */
const DEFAULT_RECONCILIATION_GRACE_MS = 30_000;

/** Bounded batch size per reconciliation sweep. */
const DEFAULT_RECONCILIATION_BATCH_LIMIT = 100;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Options for creating a PluginJobScheduler.
 */
export interface PluginJobSchedulerOptions {
  /** Suppress periodic database work while an unclaimed Cloud app stands by. */
  isBackgroundWorkEnabled?: () => boolean;
  /** Drizzle database instance. */
  db: Db;
  /** Persistence layer for jobs and runs. */
  jobStore: PluginJobStore;
  /** Worker process manager for RPC calls. */
  workerManager: PluginWorkerManager;
  /** Interval between scheduler ticks in ms (default: 30s). */
  tickIntervalMs?: number;
  /** Timeout for individual job RPC calls in ms (default: 5min). */
  jobTimeoutMs?: number;
  /** Maximum number of concurrent job executions (default: 6). */
  maxConcurrentJobs?: number;
  /** Interval between expiry-reconciliation sweeps in ms (default: 60s). */
  reconciliationIntervalMs?: number;
  /**
   * Grace period beyond an occurrence's own lease expiry before
   * reconciliation will take it over, in ms (default: 30s).
   */
  reconciliationGraceMs?: number;
}

/**
 * Result of a manual job trigger.
 */
export interface TriggerJobResult {
  /** The created run ID. */
  runId: string;
  /** The job ID that was triggered. */
  jobId: string;
}

/**
 * Diagnostic information about the scheduler.
 */
export interface SchedulerDiagnostics {
  /** Whether the tick loop is running. */
  running: boolean;
  /** Number of jobs currently executing. */
  activeJobCount: number;
  /** Set of job IDs currently in-flight. */
  activeJobIds: string[];
  /** Total number of ticks executed since start. */
  tickCount: number;
  /** Timestamp of the last tick (ISO 8601). */
  lastTickAt: string | null;
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/**
 * The public interface of the job scheduler.
 */
export interface PluginJobScheduler {
  /**
   * Start the scheduler tick loop.
   *
   * Safe to call multiple times — subsequent calls are no-ops.
   */
  start(): void;

  /**
   * Stop the scheduler tick loop.
   *
   * In-flight job runs are NOT cancelled — they are allowed to finish
   * naturally. The tick loop simply stops firing.
   */
  stop(): void;

  /**
   * Register a plugin with the scheduler.
   *
   * Computes `nextRunAt` for all active jobs that are missing it. This is
   * typically called after a plugin's worker process starts and
   * `syncJobDeclarations()` has been called.
   *
   * @param pluginId - UUID of the plugin
   */
  registerPlugin(pluginId: string): Promise<void>;

  /**
   * Unregister a plugin from the scheduler.
   *
   * Cancels any in-flight runs for the plugin and removes tracking state.
   *
   * @param pluginId - UUID of the plugin
   */
  unregisterPlugin(pluginId: string): Promise<void>;

  /**
   * Manually trigger a specific job (outside of the cron schedule).
   *
   * Creates a run with `trigger: "manual"` and dispatches immediately,
   * respecting the overlap prevention check.
   *
   * @param jobId - UUID of the job to trigger
   * @param trigger - What triggered this run (default: "manual")
   * @returns The created run info
   * @throws {Error} if the job is not found, not active, or already running
   */
  triggerJob(jobId: string, trigger?: "manual" | "retry"): Promise<TriggerJobResult>;

  /**
   * Run a single scheduler tick immediately (for testing).
   *
   * @internal
   */
  tick(): Promise<void>;

  /**
   * Run a single expiry-reconciliation sweep immediately (for testing).
   * Also runs automatically on `reconciliationIntervalMs` once `start()` is
   * called — see I7 in the active-active reforge remediation.
   *
   * @internal
   */
  reconcile(): Promise<void>;

  /**
   * Get diagnostic information about the scheduler state.
   */
  diagnostics(): SchedulerDiagnostics;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/**
 * Create a new PluginJobScheduler.
 *
 * @example
 * ```ts
 * const scheduler = createPluginJobScheduler({
 *   db,
 *   jobStore,
 *   workerManager,
 * });
 *
 * // Start the tick loop
 * scheduler.start();
 *
 * // When a plugin comes online, register it
 * await scheduler.registerPlugin(pluginId);
 *
 * // Manually trigger a job
 * const { runId } = await scheduler.triggerJob(jobId);
 *
 * // On server shutdown
 * scheduler.stop();
 * ```
 */
export function createPluginJobScheduler(
  options: PluginJobSchedulerOptions,
): PluginJobScheduler {
  const {
    db,
    jobStore,
    workerManager,
    tickIntervalMs = DEFAULT_TICK_INTERVAL_MS,
    jobTimeoutMs = DEFAULT_JOB_TIMEOUT_MS,
    maxConcurrentJobs = DEFAULT_MAX_CONCURRENT_JOBS,
    reconciliationIntervalMs = DEFAULT_RECONCILIATION_INTERVAL_MS,
    reconciliationGraceMs = DEFAULT_RECONCILIATION_GRACE_MS,
  } = options;

  const log = logger.child({ service: "plugin-job-scheduler" });

  // -----------------------------------------------------------------------
  // State
  // -----------------------------------------------------------------------

  /** Timer handle for the tick loop. */
  let tickTimer: ReturnType<typeof setInterval> | null = null;

  /** Timer handle for the expiry-reconciliation sweep. */
  let reconciliationTimer: ReturnType<typeof setInterval> | null = null;

  /** Whether the scheduler is running. */
  let running = false;

  /** Set of job IDs currently being executed (for overlap prevention). */
  const activeJobs = new Set<string>();

  /** Total number of ticks since start. */
  let tickCount = 0;

  /** Timestamp of the last tick. */
  let lastTickAt: Date | null = null;

  /** Guard against concurrent tick execution. */
  let tickInProgress = false;

  /** Guard against concurrent reconciliation sweep execution. */
  let reconciliationInProgress = false;

  // -----------------------------------------------------------------------
  // Core: tick
  // -----------------------------------------------------------------------

  /**
   * A single scheduler tick. Atomically claims a bounded batch of due
   * occurrences (durable, fenced — see plugin-job-claims-store.ts) and
   * dispatches them. `nextRunAt` is reserved as part of the same claim
   * transaction, so a crash between claiming and dispatching cannot lose or
   * duplicate the schedule pointer advance.
   */
  async function tick(): Promise<void> {
    if (options.isBackgroundWorkEnabled?.() === false) return;
    // Prevent overlapping ticks (in case a tick takes longer than the interval)
    if (tickInProgress) {
      log.debug("skipping tick — previous tick still in progress");
      return;
    }

    tickInProgress = true;
    tickCount++;
    lastTickAt = new Date();

    try {
      const now = new Date();

      // Bounded durable-claim batch: the local concurrency cap and worker
      // liveness are "local pressure" checks (this replica's own in-memory
      // state), applied as an eligibility filter on locked candidate rows
      // before any of them are claimed — see claimDueOccurrences's isEligible.
      const capacity = maxConcurrentJobs - activeJobs.size;
      if (capacity <= 0) {
        log.warn(
          { maxConcurrentJobs, activeJobCount: activeJobs.size },
          "max concurrent jobs reached, skipping tick claim",
        );
        return;
      }

      let claims: ClaimedOccurrence[];
      try {
        claims = await claimDueOccurrences(db, {
          now,
          limit: capacity,
          isEligible: (job) => {
            if (activeJobs.has(job.id)) {
              log.debug(
                { jobId: job.id, jobKey: job.jobKey, pluginId: job.pluginId },
                "skipping job — already running (overlap prevention)",
              );
              return false;
            }
            if (!workerManager.isRunning(job.pluginId)) {
              log.debug({ jobId: job.id, pluginId: job.pluginId }, "skipping job — worker not running");
              return false;
            }
            if (!job.schedule) {
              log.warn({ jobId: job.id, jobKey: job.jobKey }, "skipping job — no schedule defined");
              return false;
            }
            return true;
          },
          computeNextRunAt: (job) => computeNextRunAtForJob(job, now),
        });
      } catch (err) {
        log.error({ err: err instanceof Error ? err.message : String(err) }, "failed to claim due occurrences");
        return;
      }

      if (claims.length === 0) {
        return;
      }

      log.debug({ count: claims.length }, "claimed due occurrences");

      await Promise.allSettled(claims.map((claimed) => dispatchJob(claimed)));
    } catch (err) {
      log.error(
        { err: err instanceof Error ? err.message : String(err) },
        "scheduler tick error",
      );
    } finally {
      tickInProgress = false;
    }
  }

  // -----------------------------------------------------------------------
  // Core: reconcile occurrences whose lease has genuinely expired
  // -----------------------------------------------------------------------

  /**
   * Bounded expiry-reconciliation sweep (I7). Finds occurrences whose lease
   * has lapsed by more than `reconciliationGraceMs` and takes each over —
   * this NEVER re-dispatches or replays the job; it settles the occurrence
   * (and its run) at the terminal `"unknown"` outcome, per the module's
   * never-blind-replay rule (see plugin-job-claims-store.ts's module doc and
   * `takeoverExpiredOccurrence`). This is what actually recovers a job whose
   * worker crashed, was killed, or whose replica died mid-lease — without
   * this sweep, such an occurrence would sit `running` forever (nothing else
   * in this scheduler ever un-claims a lease that hasn't been explicitly
   * completed or acknowledgement-revoked).
   *
   * Runs on its own timer, independent of and non-blocking for the main tick
   * loop. Bounded to `DEFAULT_RECONCILIATION_BATCH_LIMIT` rows per sweep;
   * a backlog beyond that is picked up on the next sweep. Each takeover is
   * independently try/caught so one failing row cannot block the rest of the
   * batch, and the whole function never throws — it is driven by a bare
   * `setInterval` callback with no caller to catch a rejection.
   */
  async function reconcile(): Promise<void> {
    if (reconciliationInProgress) {
      log.debug("skipping reconciliation sweep — previous sweep still in progress");
      return;
    }
    reconciliationInProgress = true;
    try {
      let expired: PluginJobOccurrenceRow[];
      try {
        expired = await findExpiredOccurrences(db, {
          graceMs: reconciliationGraceMs,
          limit: DEFAULT_RECONCILIATION_BATCH_LIMIT,
        });
      } catch (err) {
        log.error(
          { err: err instanceof Error ? err.message : String(err) },
          "failed to query expired occurrences for reconciliation",
        );
        return;
      }

      if (expired.length === 0) {
        return;
      }

      log.warn(
        {
          count: expired.length,
          reconciliationGraceMs,
          leaseTtlMs: DEFAULT_OCCURRENCE_LEASE_TTL_MS,
          maxConcurrentJobs,
        },
        "reconciling expired occurrence leases",
      );

      for (const occurrence of expired) {
        try {
          const result = await takeoverExpiredOccurrence(db, {
            occurrenceId: occurrence.id,
            graceMs: reconciliationGraceMs,
          });
          if (result) {
            const leaseExpiresAt = occurrence.leaseExpiresAt
              ? new Date(occurrence.leaseExpiresAt)
              : null;
            const leaseRenewedAt = occurrence.leaseRenewedAt
              ? new Date(occurrence.leaseRenewedAt)
              : null;
            // Always attach lease metrics when marking unknown — dig 2026-09-02
            // takeover churn was hard to diagnose from id-only warn lines.
            log.warn(
              {
                occurrenceId: occurrence.id,
                jobId: occurrence.jobId,
                pluginId: occurrence.pluginId,
                runId: result.run?.id ?? null,
                kind: occurrence.kind,
                statusBeforeTakeover: occurrence.status,
                claimAttempt: occurrence.claimAttempt,
                acknowledgedAt: occurrence.acknowledgedAt
                  ? new Date(occurrence.acknowledgedAt).toISOString()
                  : null,
                leaseExpiresAt: leaseExpiresAt ? leaseExpiresAt.toISOString() : null,
                leaseRenewedAt: leaseRenewedAt ? leaseRenewedAt.toISOString() : null,
                leaseExpiredAgeMs: leaseExpiresAt ? Date.now() - leaseExpiresAt.getTime() : null,
                reconciliationGraceMs,
                leaseTtlMs: DEFAULT_OCCURRENCE_LEASE_TTL_MS,
              },
              "took over expired occurrence lease — marked unknown, not replayed",
            );
          }
        } catch (err) {
          log.error(
            {
              occurrenceId: occurrence.id,
              jobId: occurrence.jobId,
              err: err instanceof Error ? err.message : String(err),
            },
            "failed to take over expired occurrence",
          );
        }
      }
    } finally {
      reconciliationInProgress = false;
    }
  }

  // -----------------------------------------------------------------------
  // Core: renew an in-flight occurrence's lease while its RPC is pending
  // -----------------------------------------------------------------------

  /**
   * Start periodic DB-clock lease renewal for an occurrence while its
   * `runJob` RPC is in flight. Long-running jobs (up to `jobTimeoutMs`, by
   * default well beyond the occurrence's lease TTL) must not have their
   * lease reaped by reconciliation out from under them. Renewal failure
   * (claim went stale — e.g. a reconciler already took the occurrence over)
   * is logged, not thrown: the RPC call cannot be cancelled mid-flight, and
   * the fenced `completeOccurrence` write at the end will reject on its own
   * if ownership truly moved on.
   */
  function startLeaseRenewal(
    occurrenceId: string,
    claim: OccurrenceClaim,
    leaseTtlMs: number,
    jobLog: pino.Logger,
  ): () => void {
    const intervalMs = Math.max(5_000, Math.floor(leaseTtlMs / 3));
    const timer = setInterval(() => {
      // Renewal runs detached from any caller's await chain (the timer
      // fires on its own), so a rejection here (DB connection error, etc.)
      // would otherwise become an unhandled promise rejection — it MUST be
      // caught, not just have its resolved-null case handled.
      renewOccurrenceLease(db, { occurrenceId, claim, leaseTtlMs })
        .then((renewed) => {
          if (!renewed) {
            const rejection = describeStaleOccurrenceRejection({ occurrenceId, claim, context: "lease_renewal" });
            jobLog.warn(rejection.fields, rejection.event);
          }
        })
        .catch((err: unknown) => {
          jobLog.error(
            { occurrenceId, err: err instanceof Error ? err.message : String(err) },
            "lease renewal threw — will retry on next interval",
          );
        });
    }, intervalMs);
    return () => clearInterval(timer);
  }

  /**
   * Report a rejected completion/acknowledge write. Gated on
   * `isOccurrenceClaimStale` so the alert-worthy "stale_completion_rejected"
   * warning is only fired for a genuine ownership dispute (claim no longer
   * matches the row) — a 0-row CAS can also mean the occurrence id itself is
   * gone, which is unexpected and escalated separately rather than folded
   * into the same routine-sounding warning.
   */
  async function reportRejectedOccurrenceWrite(
    jobLog: pino.Logger,
    occurrenceId: string,
    claim: OccurrenceClaim,
    context: string,
  ): Promise<void> {
    const rejection = describeStaleOccurrenceRejection({ occurrenceId, claim, context });
    let stale: boolean;
    try {
      stale = await isOccurrenceClaimStale(db, { occurrenceId, claim });
    } catch (err) {
      jobLog.error(
        { ...rejection.fields, err: err instanceof Error ? err.message : String(err) },
        "failed to check claim staleness after rejected write",
      );
      return;
    }
    if (stale) {
      jobLog.warn(rejection.fields, rejection.event);
    } else {
      jobLog.error(rejection.fields, `${rejection.event}:occurrence_missing`);
    }
  }

  /**
   * Fenced completion write that can never throw out of the caller — this
   * runs at the end of `executeClaimedRun`'s success path and inside its
   * catch block, so a DB error here (not just a stale-claim null) must be
   * swallowed-and-logged rather than propagate, or a fire-and-forget caller
   * (`triggerJob`'s `void executeClaimedRun(...)`) would produce an
   * unhandled rejection.
   */
  async function safeCompleteOccurrence(
    jobLog: pino.Logger,
    input: {
      occurrenceId: string;
      claim: OccurrenceClaim;
      runId: string;
      status: Extract<PluginJobRunStatus, "succeeded" | "failed">;
      error?: string | null;
      durationMs: number;
    },
  ): Promise<{ occurrence: unknown; run: PluginJobRunRow } | null> {
    try {
      const completed = await completeOccurrence(db, input);
      if (!completed) {
        // Another executor already resolved this occurrence (e.g.
        // reconciliation took it over as "unknown" while this RPC was still
        // in flight). Must not blindly overwrite whatever the current owner
        // recorded.
        await reportRejectedOccurrenceWrite(
          jobLog,
          input.occurrenceId,
          input.claim,
          input.status === "succeeded" ? "complete:succeeded" : "complete:failed",
        );
        return null;
      }
      return completed;
    } catch (err) {
      jobLog.error(
        {
          occurrenceId: input.occurrenceId,
          runId: input.runId,
          err: err instanceof Error ? err.message : String(err),
        },
        "failed to write occurrence completion",
      );
      return null;
    }
  }

  // -----------------------------------------------------------------------
  // Core: execute a claimed occurrence
  // -----------------------------------------------------------------------

  /**
   * Execute one already-claimed occurrence: acknowledge the claim (making
   * it ineligible for lifecycle revoke), call the worker, and fence the
   * completion write against the same claim captured at claim time.
   *
   * `nextRunAt` is NOT touched here — it was already reserved atomically in
   * the same transaction that claimed the occurrence, so a crash at any
   * point in this function cannot lose or duplicate the schedule advance.
   */
  async function executeClaimedRun(
    claimed: ClaimedOccurrence,
    trigger: "schedule" | "manual" | "retry",
    scheduledAt: string,
  ): Promise<void> {
    const { job, run, occurrence, claim } = claimed;
    const { id: jobId, pluginId, jobKey } = job;
    const jobLog = log.child({ jobId, pluginId, jobKey, runId: run.id, occurrenceId: occurrence.id });

    activeJobs.add(jobId);
    const startedAt = Date.now();
    let stopRenewal: (() => void) | null = null;

    try {
      await jobStore.markRunning(run.id);

      // Acknowledge before dispatching: from this point on, lifecycle
      // revoke (disable/unload) must leave this occurrence to drain rather
      // than force-cancelling it. A null return means the claim already
      // went stale before we ever called the worker — do NOT dispatch.
      const acknowledged = await acknowledgeOccurrence(db, { occurrenceId: occurrence.id, claim });
      if (!acknowledged) {
        await reportRejectedOccurrenceWrite(jobLog, occurrence.id, claim, "dispatch:pre-ack");
        return;
      }

      stopRenewal = startLeaseRenewal(occurrence.id, claim, DEFAULT_OCCURRENCE_LEASE_TTL_MS, jobLog);

      jobLog.info("dispatching job");

      await workerManager.call(
        pluginId,
        "runJob",
        {
          job: {
            jobKey,
            runId: run.id,
            trigger,
            scheduledAt,
            occurrenceId: occurrence.id,
            fence: claim.fence,
          },
        },
        jobTimeoutMs,
      );

      stopRenewal();
      stopRenewal = null;

      const durationMs = Date.now() - startedAt;
      const completed = await safeCompleteOccurrence(jobLog, {
        occurrenceId: occurrence.id,
        claim,
        runId: run.id,
        status: "succeeded",
        durationMs,
      });
      if (completed) {
        jobLog.info({ durationMs }, "job completed successfully");
      }
    } catch (err) {
      stopRenewal?.();
      const durationMs = Date.now() - startedAt;
      const errorMessage = err instanceof Error ? err.message : String(err);
      jobLog.error({ durationMs, err: errorMessage }, "job execution failed");

      await safeCompleteOccurrence(jobLog, {
        occurrenceId: occurrence.id,
        claim,
        runId: run.id,
        status: "failed",
        error: errorMessage,
        durationMs,
      });
    } finally {
      stopRenewal?.();
      activeJobs.delete(jobId);
    }
  }

  async function dispatchJob(claimed: ClaimedOccurrence): Promise<void> {
    const scheduledAt = (claimed.occurrence.scheduledFor ?? new Date()).toISOString();
    await executeClaimedRun(claimed, "schedule", scheduledAt);
  }

  // -----------------------------------------------------------------------
  // Core: manual trigger
  // -----------------------------------------------------------------------

  async function triggerJob(
    jobId: string,
    trigger: "manual" | "retry" = "manual",
  ): Promise<TriggerJobResult> {
    const job = await jobStore.getJobById(jobId);
    if (!job) {
      throw new Error(`Job not found: ${jobId}`);
    }

    if (job.status !== "active") {
      throw new Error(
        `Job "${job.jobKey}" is not active (status: ${job.status})`,
      );
    }

    if (!workerManager.isRunning(job.pluginId)) {
      throw new Error(
        `Worker for plugin "${job.pluginId}" is not running — cannot trigger job`,
      );
    }

    // Atomic claim: refuses if the job already has a live claimed
    // occurrence (scheduled or manual) — replaces the prior in-memory-only
    // + best-effort DB overlap check with a single fenced transaction.
    const claimed = await claimManualOccurrence(db, { jobId, trigger });
    if (!claimed) {
      throw new Error(
        `Job "${job.jobKey}" is already running — cannot trigger while in progress`,
      );
    }

    // Dispatch in background — don't block the caller
    void executeClaimedRun(claimed, trigger, new Date().toISOString());

    return { runId: claimed.run.id, jobId };
  }

  // -----------------------------------------------------------------------
  // Schedule pointer management
  // -----------------------------------------------------------------------

  /**
   * Compute the next `nextRunAt` tick for a due job. Called from inside
   * `claimDueOccurrences`'s transaction (see tick()) so the schedule pointer
   * is reserved atomically alongside the occurrence claim — no separate
   * post-dispatch "advance the pointer" step exists anymore, closing the
   * crash window where a claim could survive but the pointer advance not
   * (or vice versa).
   */
  function computeNextRunAtForJob(job: typeof pluginJobs.$inferSelect, now: Date): Date | null {
    if (!job.schedule) return null;

    const validationError = validateCron(job.schedule);
    if (validationError) {
      log.warn(
        { jobId: job.id, schedule: job.schedule, error: validationError },
        "invalid cron schedule — cannot compute next run",
      );
      return null;
    }

    const cron = parseCron(job.schedule);
    return nextCronTick(cron, now);
  }

  /**
   * Bootstrap a first `nextRunAt` for active jobs that have never had one —
   * i.e. `next_run_at IS NULL` (a freshly declared job). Called when a
   * plugin is registered with the scheduler.
   *
   * This must NEVER touch a job that already has a pointer, even one that
   * is now overdue: `claimDueOccurrences` is the only path allowed to
   * advance `next_run_at`, because that's the only path that reserves the
   * occurrence atomically alongside the advance. An earlier version of this
   * function recomputed `nextRunAt` from `nextCronTick(cron, new Date())`
   * whenever the existing pointer was in the past, which silently skipped
   * the missed occurrence — the job's schedule pointer jumped straight to
   * the next future tick with no occurrence, no run, and no record that a
   * tick was ever due — instead of leaving it for `claimDueOccurrences` to
   * pick up on the next tick. It also always overwrote `lastRunAt`
   * (defaulting to the epoch when unset), clobbering a real prior run
   * timestamp with a synthetic one. Neither is done here: `lastRunAt` is
   * left untouched (this function only sets `nextRunAt`), and the write
   * itself is conditioned on `next_run_at IS NULL` so it is a no-op —
   * atomically, even if two replicas register the same plugin concurrently
   * — for any job that already has a pointer.
   */
  async function ensureNextRunTimestamps(pluginId: string): Promise<void> {
    const jobs = await jobStore.listJobs(pluginId, "active");

    for (const job of jobs) {
      // Only bootstrap jobs that have never been scheduled. A job with any
      // existing nextRunAt — future or overdue — is left strictly alone.
      if (job.nextRunAt !== null) {
        continue;
      }

      if (!job.schedule) {
        continue;
      }

      const validationError = validateCron(job.schedule);
      if (validationError) {
        log.warn(
          { jobId: job.id, jobKey: job.jobKey, schedule: job.schedule, error: validationError },
          "skipping job with invalid cron schedule",
        );
        continue;
      }

      const cron = parseCron(job.schedule);
      const nextRunAt = nextCronTick(cron, new Date());
      if (!nextRunAt) continue;

      // CAS on next_run_at IS NULL: makes this safe even if two replicas
      // call registerPlugin for the same plugin concurrently — only the
      // first write lands, the second is a harmless no-op.
      const [updated] = await db
        .update(pluginJobs)
        .set({ nextRunAt, updatedAt: new Date() })
        .where(and(eq(pluginJobs.id, job.id), isNull(pluginJobs.nextRunAt)))
        .returning();

      if (updated) {
        log.debug(
          { jobId: job.id, jobKey: job.jobKey, nextRunAt: nextRunAt.toISOString() },
          "bootstrapped nextRunAt for job",
        );
      }
    }
  }

  // -----------------------------------------------------------------------
  // Plugin registration
  // -----------------------------------------------------------------------

  async function registerPlugin(pluginId: string): Promise<void> {
    log.info({ pluginId }, "registering plugin with job scheduler");
    await ensureNextRunTimestamps(pluginId);
  }

  async function unregisterPlugin(pluginId: string): Promise<void> {
    log.info({ pluginId }, "unregistering plugin from job scheduler");

    // Revoke only occurrences the worker was never actually asked to run
    // (acknowledged_at IS NULL) — those are safe to force-cancel. Occurrences
    // already dispatched to the worker are deliberately left alone; they
    // drain via their own completion path (executeClaimedRun's
    // completeOccurrence call), same as if the plugin were still running.
    try {
      await revokeUnacknowledgedOccurrences(db, {
        pluginId,
        reason: "Plugin unregistered",
      });
    } catch (err) {
      log.error(
        {
          pluginId,
          err: err instanceof Error ? err.message : String(err),
        },
        "error revoking unacknowledged occurrences during unregister",
      );
    }

    // Deliberately does NOT touch `activeJobs` here. `activeJobs` tracks
    // in-flight executions, and only `executeClaimedRun`'s own try/finally
    // adds/removes an entry — it owns the full lifecycle of that jobId's
    // membership. An acknowledged (already-dispatched) occurrence is left to
    // drain above; forcibly deleting its jobId from `activeJobs` here would
    // let a concurrent tick's `isEligible` overlap check see it as free and
    // attempt to claim a new occurrence for the same job while the old one
    // is still actually running on the worker.
  }

  // -----------------------------------------------------------------------
  // Lifecycle: start / stop
  // -----------------------------------------------------------------------

  function start(): void {
    if (running) {
      log.debug("scheduler already running");
      return;
    }

    running = true;
    tickTimer = setInterval(() => {
      void tick();
    }, tickIntervalMs);
    reconciliationTimer = setInterval(() => {
      // `reconcile()` never throws (see its own try/finally) so this bare
      // fire-and-forget call cannot become an unhandled rejection, but the
      // `void` documents that intent explicitly.
      void reconcile();
    }, reconciliationIntervalMs);

    log.info(
      { tickIntervalMs, maxConcurrentJobs, reconciliationIntervalMs, reconciliationGraceMs },
      "plugin job scheduler started",
    );
  }

  function stop(): void {
    // Always clear timers defensively, even if `running` is already false,
    // to prevent leaked interval timers.
    if (tickTimer !== null) {
      clearInterval(tickTimer);
      tickTimer = null;
    }
    if (reconciliationTimer !== null) {
      clearInterval(reconciliationTimer);
      reconciliationTimer = null;
    }

    if (!running) return;
    running = false;

    log.info(
      { activeJobCount: activeJobs.size },
      "plugin job scheduler stopped",
    );
  }

  // -----------------------------------------------------------------------
  // Diagnostics
  // -----------------------------------------------------------------------

  function diagnostics(): SchedulerDiagnostics {
    return {
      running,
      activeJobCount: activeJobs.size,
      activeJobIds: [...activeJobs],
      tickCount,
      lastTickAt: lastTickAt?.toISOString() ?? null,
    };
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  return {
    start,
    stop,
    registerPlugin,
    unregisterPlugin,
    triggerJob,
    tick,
    reconcile,
    diagnostics,
  };
}
