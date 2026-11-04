# 004 — Atomic plugin-job claims

## Executor preamble

Start after 001 and after 003's claim/fence conventions are accepted. At `9cb229ec9`, read `server/src/services/plugin-job-scheduler.ts:260-443`, `plugin-job-store.ts:353-381`, `plugin-job-coordinator.ts:98-257`, and `server/src/app.ts:259-271,444-445`. Resolve the current `pending`/`queued` vocabulary before adding state.

## Status metadata

- **Status:** TODO
- **Priority:** P0
- **Effort:** L
- **Risk:** High — duplicate plugin side effects.
- **Dependencies:** 001; 003 claim conventions
- **Category:** scheduler / durable ownership
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6)
- **Baseline:** `9cb229ec9`

## Goal

Give scheduled and manual plugin jobs a durable, fenced claim so API replicas cannot dispatch the same logical occurrence independently.

## Evidence and design

Due rows are selected before local `activeJobs` suppression and the store unconditionally marks a run running. Reserve occurrence and advance schedule atomically; use 003-style lease/fence/idempotency and surface unknown execution rather than blind retry.

## In scope

- `packages/db/src/schema/plugin_jobs.ts`, `packages/db/src/schema/plugin_logs.ts`, `packages/db/src/schema/index.ts`, `packages/db/drizzle/**` **(create migration)**
- `server/src/services/plugin-job-store.ts`, `server/src/services/plugin-job-scheduler.ts`, `server/src/services/plugin-job-coordinator.ts`
- `server/src/app.ts`, `server/src/__tests__/plugin-job-claims.test.ts` **(create)**
- `doc/operations/plugin-job-reconciliation.md` **(create)**

## Out of scope

- Generic distributed queue, sandbox redesign, cron syntax changes, or automatic replay of irreversible plugin effects.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Plugin job tests exit 0. |
| `pnpm db:generate && pnpm db:migrate` | Reviewed occurrence/claim migration is generated and applies successfully. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck` | All workspace typechecks exit 0. |

## Git workflow

Create `active-active/004-plugin-job-claims` after 003. Keep migration/schema/store/scheduler changes together; run `git diff --check` and scope diff before `feat(active-active): claim plugin jobs`. Do not push or deploy.

## Implementation steps

### 1. Reserve occurrence and claim atomically

Add unique job/scheduled-time occurrence plus holder/lease/fence. Implement transactional `claimDueJobs`, `claimManualJob`, renewal, and fenced completion; reserve `nextRunAt` in the same transaction.

**Verify:** `pnpm test:run && pnpm db:migrate`
**Expected outcome:** two schedulers create one occurrence and a takeover has higher fence.

### 2. Make scheduler and worker protocol claim-aware

Tick a bounded durable-claim batch; retain `activeJobs` only for local pressure. Send run/occurrence/fence through worker RPC and reject stale callbacks.

**Verify:** `pnpm test:run`
**Expected outcome:** stale completion is rejected and pointer advancement survives a simulated crash.

### 3. Reconcile lifecycle and observability

Revoke future claims on disable/unload, drain only acknowledged owners, retain reconciliation history, and add lag/conflict/expiry/stale-completion metrics.

**Verify:** `pnpm test:run`
**Expected outcome:** disabled plugin cannot claim and existing lifecycle behavior remains intact.

## Test plan

Run two scheduler instances on one PostgreSQL DB for scheduled/manual races, renewal/takeover, crash before pointer advance, disable, and stale callback. Run Commands table; record occurrence and fence IDs.

## Done criteria

- [ ] One due occurrence creates one logical execution across replicas.
- [ ] Every worker RPC has durable run, occurrence, and fence before dispatch.
- [ ] Unknown external execution is visible for operator recovery, not silently replayed.
- [ ] All Commands table checks exit 0.

## STOP conditions

Stop if any plugin has irreversible work without idempotency/status probe, `nextRunAt` advances outside occurrence reservation, or completion cannot reject stale fences. Keep it local/single-replica and escalate [#6](https://github.com/valkyriweb/paperclip/issues/6).

## Maintenance notes

New plugin trigger/callback paths must use the claim store. Re-run race coverage after cron, worker RPC, plugin lifecycle, or schema changes; monitor claim conflicts and unknown outcomes.
