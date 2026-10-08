# Paperclip true active-active reforge

**Date:** 2026-07-12

**Baseline:** `9cb229ec9` (`Fix runtime worktree policy gating (#75)`)

**Related:** [#6 — audit shared PVC state before true multi-replica HA](https://github.com/valkyriweb/paperclip/issues/6); [#57 — migration 0130 exceeds startup probe](https://github.com/valkyriweb/paperclip/issues/57)

**Executor package:** [`plans/README.md`](../../plans/README.md)

## Decision

Paperclip can become true active-active only by making PostgreSQL the durable coordination authority and moving every replica-local side effect behind an explicit **interface**, durable claim, or elected leader. **Do not simply set `replicas=2`.** That would duplicate timers, migrations, backup writers, plugin dispatch, event delivery, and host-local child-process assumptions while preserving no shared ownership protocol.

The target is an active-active control plane: any healthy API replica may accept a request; durable work has one fenced owner at a time; ephemeral delivery may be at-least-once; and locality-bound execution is routed to a compatible executor rather than assumed present on every replica.

## Vocabulary used by this package

- **module**: a cohesive implementation unit, such as `heartbeat.ts` or the plugin scheduler.
- **interface**: the typed contract that permits a replacement implementation without changing callers.
- **seam**: an existing boundary where the replacement can be introduced incrementally.
- **adapter**: an implementation of an interface for a particular backend or execution target.
- **depth**: how far a slice changes persistence, control flow, and operator behavior.
- **locality**: whether work requires a particular process, filesystem, terminal, or executor host.
- **leverage**: an existing seam whose reuse reduces migration risk.

## Evidence-led current-state synthesis

| Concern | Direct evidence at baseline | Active-active implication |
|---|---|---|
| Startup/migrations | `server/src/index.ts:158-205` inspects and applies migrations during each server start; `:324-329` selects external Postgres. `packages/db/src/client.ts:233-278` applies each file transactionally but has no cross-process advisory lock. | Two replicas can race schema work. Slice 001 introduces a session-scoped PostgreSQL advisory lock and separates readiness from migration completion; this directly addresses [#57](https://github.com/valkyriweb/paperclip/issues/57). |
| Process-local schedulers/recovery | `server/src/index.ts:786-966` starts startup reaping and a `setInterval` that ticks timers, routines, orphan recovery, watchdogs, and lock sweeping. | Every replica repeats the same scans. Durable claim rows/fencing replace process-local exclusivity in slices 003–005. |
| Backups | `server/src/index.ts:589-646` uses `databaseBackupInFlight` and `:969-985` uses a local interval. | That boolean protects only one Node process. Slice 007 uses a leader lease and object-storage manifest. |
| Local execution ownership | `packages/adapter-utils/src/server-utils.ts:80` exports `runningProcesses`; `:2859-2876` spawns and records it in a `Map`. `server/src/services/agent-start-lock.ts:3-44` serializes starts in a module-level `Map`. | An in-memory handle is not visible to another replica. Slice 003 replaces logical run ownership; slice 006 routes locality-bound work; slice 008 brokers terminal/session traffic. |
| Heartbeat queue/reaping | `server/src/services/heartbeat.ts:10002-10074` uses the local lock, reads queued runs, and calls `claimQueuedRun`; `:9738-9864` decides orphan status using in-memory handles and local PIDs. | The current queue has useful leverage, but claims and reaping are not fenced across replicas. |
| Plugin jobs | `server/src/services/plugin-job-scheduler.ts:260-337` selects due jobs then dispatches; `:344-443` creates and starts a run. `plugin-job-store.ts:353-381` inserts then unconditionally marks it running. | Two schedulers can observe the same due job. Slice 004 makes the DB claim atomic and leases execution. |
| Live events | `server/src/services/live-events.ts:7-42` is an in-memory `EventEmitter`; `realtime/live-events-ws.ts:170-215` subscribes each socket to that emitter. | Events published on replica A never reach WebSockets attached to B. Slice 005 supplies a durable outbox and shared fanout. |
| Logs and objects | `run-log-store.ts:1-25` says hot logs are local files and cold logs may be S3; `storage/index.ts:21-32` already creates provider-swappable storage. | Assets have an adapter seam, but hot logs and backup files retain filesystem locality. Slice 002 completes shared object storage. |
| Workspaces/environments | `workspace-runtime.ts:1576-1667` creates/reuses local git worktrees. `environment-run-orchestrator.ts:327-498` already realizes a workspace through `local`, `ssh`, or `sandbox` drivers and resolves an adapter target. | This is the leverage for slice 006: preserve local behavior behind a remote/shared workspace and execution adapter. |
| Terminal sessions | `environment-custom-image-terminal-ws.ts:484-574` owns an SSH terminal WebSocket and its connection registry in the serving process. | A reconnect through another replica cannot find the session. Slice 008 chooses sticky routing or a broker. |

## Representative baseline excerpts

`server/src/index.ts:869-873` runs replica-local periodic work:

```ts
setInterval(() => {
  const sweptRuntimeStatuses = heartbeat.sweepExpiredRuntimeStatuses();
  // timer, routine, recovery, and watchdog work follows
}, config.heartbeatSchedulerIntervalMs);
```

`server/src/services/agent-start-lock.ts:3-4` is only process-local:

```ts
const startLocksByAgent = new Map<string, { promise: Promise<void>; startedAtMs: number }>();
```

`server/src/services/live-events.ts:27-33` cannot cross a Node process:

```ts
const event = toLiveEvent(input);
emitter.emit(input.companyId, event);
return event;
```

`server/src/services/plugin-job-scheduler.ts:260-270` reads due work before a durable claim:

```ts
const dueJobs = await db.select().from(pluginJobs).where(
  and(eq(pluginJobs.status, "active"), lte(pluginJobs.nextRunAt, now)),
);
```

These excerpts establish why the plan introduces durable coordination rather than relying on more deployment replicas.

## Target architecture

```text
client ──> any API replica ──> PostgreSQL (system of record)
                  │              ├─ migrations/leader leases/work claims
                  │              ├─ heartbeat + plugin state + event outbox
                  │              └─ fencing tokens / audit history
                  ├─> shared object storage (assets, run logs, backup manifests)
                  ├─> fanout consumer/broker ──> WebSocket replicas
                  └─> execution adapter ──> selected local/SSH/sandbox executor
```

Rules:

1. PostgreSQL transaction boundaries create claims; a worker checks its fencing token before every state transition and external side effect acknowledgement.
2. A lease expiry is permission to *attempt takeover*, never proof that a former local process died. Side effects must be idempotent by run/job/outbox key.
3. Object keys are deterministic, company-scoped where tenant data is involved, immutable after commit, and recorded with checksum/version metadata.
4. UI events are durable before fanout. Consumers deduplicate by event ID and resume from a cursor; WebSocket delivery remains at-least-once.
5. Local child processes, raw worktrees, and terminal handles explicitly advertise locality. A replica may route, not impersonate, their owner.

## Ordered roadmap

| Slice | Deliverable | Depends on |
|---|---|---|
| [001](../../plans/001-external-postgresql-advisory-locked-migrations.md) | external PostgreSQL production contract and advisory-locked migrations | none |
| [002](../../plans/002-shared-object-storage-adapters.md) | shared object adapters for assets, run logs, and backups | 001 |
| [003](../../plans/003-fenced-durable-heartbeat-run-ownership.md) | fenced durable heartbeat ownership | 001 |
| [004](../../plans/004-atomic-plugin-job-claims.md) | atomic plugin-job claims | 001, 003 claim conventions |
| [005](../../plans/005-durable-event-outbox-fanout.md) | transactional outbox and shared fanout | 001 |
| [006](../../plans/006-remote-shared-workspace-execution-adapters.md) | remote/shared workspace and execution adapters | 001–003, 005 |
| [007](../../plans/007-leader-controlled-backups-recovery.md) | leader-controlled backups/recovery | 001, 002, 003 |
| [008](../../plans/008-terminal-session-routing-broker.md) | terminal/session routing or broker | 003, 005, 006 |
| [009](../../plans/009-multi-replica-canary-rollout.md) | canary, failure drills, and rollout | 001–008 |

## Global guardrails

- Preserve the company-scoped control-plane invariants and existing local-first mode. The shared path is additive until a documented cutover.
- Do not turn PostgreSQL into a long-running execution transport. DB rows coordinate; adapters execute.
- Do not make a side-effect retry safe merely by increasing timeout or adding a retry loop.
- Add metrics for claim contention, lease loss, fencing rejection, duplicate suppression, outbox lag, fanout cursor lag, and routing failures before enabling a slice.
- Every schema change uses normal Drizzle migration generation and `pnpm db:migrate`; the migration lock implementation itself must be tested against two real PostgreSQL connections.

## Architecture exit criteria

The work is ready for general multi-replica enablement only when two replicas can be killed and restarted during an active workload without duplicate heartbeat/plugin execution, lost terminal safety boundaries, silently missing events, divergent assets/logs/backups, or unbounded recovery; and slice 009's rollback and observability gates pass.
