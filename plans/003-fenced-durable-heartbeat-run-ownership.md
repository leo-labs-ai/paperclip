# 003 — Fenced durable heartbeat run ownership

## Executor preamble

Begin only after 001. At `9cb229ec9`, map every `heartbeatRuns` write, `claimQueuedRun` caller, `runningProcesses`, `activeRunExecutions`, and `withAgentStartLock` use. Read `server/src/services/heartbeat.ts:9738-9864,10002-10074`, `server/src/services/agent-start-lock.ts`, `packages/adapter-utils/src/server-utils.ts:80,2859-2876`, and `server/src/index.ts:786-966` before editing.

## Status metadata

- **Status:** TODO
- **Priority:** P0
- **Effort:** XL
- **Risk:** Critical — duplicate external agent execution.
- **Dependencies:** 001
- **Category:** durable ownership / execution safety
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6)
- **Baseline:** `9cb229ec9`

## Goal

Replace process-local heartbeat start/reap ownership with a durable leased claim and monotonic fencing token so one logical run has one authoritative executor during overlap or failure.

## Evidence and design

The current local `Map` lock and PID/process maps cannot coordinate replicas. Atomically claim queued rows with PostgreSQL, renew lease during execution, and require the owner token on every durable mutation/final acknowledgement. Expiry permits reconciliation, not proof that the old subprocess stopped.

## In scope

- `packages/db/src/schema/heartbeat_runs.ts`, `packages/db/src/schema/heartbeat_run_events.ts`, `packages/db/src/schema/index.ts`, `packages/db/drizzle/**` **(create migration)**
- `server/src/services/heartbeat.ts`, `server/src/services/agent-start-lock.ts`, `server/src/services/run-ownership-store.ts` **(create)**, `server/src/services/run-ownership-store.test.ts` **(create)**
- `server/src/services/heartbeat-run-runtime-status.ts`, `server/src/index.ts`, `packages/adapter-utils/src/server-utils.ts`
- `doc/operations/run-ownership.md` **(create)**

## Out of scope

- Remote routing (006), terminal brokering (008), issue checkout policy changes, and adopting a lost host-local subprocess.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Server ownership tests exit 0. |
| `pnpm db:generate && pnpm db:migrate` | Only reviewed claim/fence migration is generated and applied. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck` | Workspace typechecks exit 0. |

## Git workflow

Use `active-active/003-run-fencing`. Include the generated migration and only **In scope** paths; run `git diff --check` and scope diff before `feat(active-active): fence heartbeat ownership`. No push/deploy/multi-replica enablement.

## Implementation steps

### 1. Persist claims and owner tokens

Add run holder, lease expiry, monotonic fence, attempt, renewal, and idempotency fields/store. `claim` uses conditional update or `FOR UPDATE SKIP LOCKED`; return opaque owner token.

**Verify:** `pnpm test:run && pnpm db:migrate`
**Expected outcome:** concurrent claim returns one owner and takeover receives a higher fence.

### 2. Fence every execution mutation

Pass the token through start, renew, cancel, log, status, issue-lock, and finalization flows. Local lock may optimize only after DB claim; stale owner stops dispatch and records observed loss.

**Verify:** `pnpm test:run`
**Expected outcome:** stale owner cannot append, finalize, release, or mutate state after takeover.

### 3. Replace PID-based orphan decisions with reconciliation

Make expired leases drive adapter-specific idempotency/cancel/status reconciliation and partition/claim scans. Expose holder/lease/fence metrics and recovery audit records.

**Verify:** `pnpm test:run-run-runtime-status`
**Expected outcome:** crash between invoke/finalize is recoverable without duplicate spawn; locality-bound loss is visible.

## Test plan

Use two PostgreSQL-backed server instances for claim race, renewal, expiry/takeover, stale writes, crash window, and scan concurrency. Verify local adapter reports unrecoverable locality loss rather than adoption. Run Commands table and capture run/fence IDs.

## Done criteria

- [ ] Every running heartbeat has inspectable durable holder, lease, fence, and idempotency key.
- [ ] Stale replicas cannot mutate state/logs/issue locks after takeover.
- [ ] Recovery is safe without shared memory and emits an operator-visible path.
- [ ] All Commands table checks exit 0.

## STOP conditions

Stop before enabling HA if any durable mutation lacks fence enforcement, an adapter has irreversible work without idempotency/status probe, or expiry leaves an unbounded duplicate window. Escalate [#6](https://github.com/valkyriweb/paperclip/issues/6).

## Maintenance notes

All new heartbeat writers must accept owner token; enforce this in store APIs rather than convention. Re-run two-replica race tests after run status/schema, adapter semantics, or lease timing changes; alert on stale-fence rejections and lease-loss rate.
