# 009 — Multi-replica canary and rollout

## Executor preamble

This is the only plan permitted to enable two replicas. Start only when 001–008 have accepted evidence, not merely merged code. From `9cb229ec9`, compare actual manifests/config to `server/src/index.ts:786-1029`, local event/lock evidence, and the [#6](https://github.com/valkyriweb/paperclip/issues/6)/[#57](https://github.com/valkyriweb/paperclip/issues/57) gates. A TODO/deviation is a block.

## Status metadata

- **Status:** TODO
- **Priority:** P0
- **Effort:** L
- **Risk:** Critical — production duplicate execution/data loss/security boundary failure.
- **Dependencies:** 001–008 accepted
- **Category:** rollout / operations
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6), [#57](https://github.com/valkyriweb/paperclip/issues/57)
- **Baseline:** `9cb229ec9`

## Goal

Prove true active-active behavior with an independent-filesystem two-replica canary, fault drills, observability gates, audit artifacts, and reversible one-replica rollback. Setting `replicas=2` alone is explicitly rejected.

## Evidence and design

Current startup runs heartbeat/recovery/backup/archive loops per process; local event fanout and locks do not cross replicas. The explicit `multi_replica` profile must reject missing prerequisite contracts before deployment, then promote only on drill evidence.

## In scope

- `server/src/config.ts`, `server/src/index.ts`, `server/src/services/active-active-preflight.ts` **(create)**, `server/src/services/active-active-preflight.test.ts` **(create)**
- `deploy/active-active/**` **(create canary manifests/config)**
- `scripts/active-active-canary.mjs` **(create)**, `scripts/active-active-chaos.mjs` **(create)**
- `doc/operations/active-active-canary.md` **(create)**, `doc/operations/active-active-rollback.md` **(create)**
- `plans/README.md`

## Out of scope

- Autoscaling policy, multi-region active-active, public SLA claim, default change for `local_trusted`, or production rollout without completed canary evidence.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Preflight unit/integration tests exit 0. |
| `pnpm db:migrate` | Disposable/canary DB migration exits 0 with one advisory-lock holder. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck && pnpm build` | Typecheck and production build exit 0. |
| `git diff --check` | Exit status 0; manifests/scripts/runbooks have no whitespace errors. |
| `git diff --name-only 9cb229ec9...HEAD` | Lists only the **In scope** rollout paths. |

## Git workflow

Use `active-active/009-canary-rollout`. Keep manifests/scripts/runbooks/preflight together; run `git diff --check` and baseline scope diff before `feat(active-active): add canary rollout gates`. Do not push, deploy, or change production replicas from this plan.

## Implementation steps

### 1. Build blocking preflight and artifacts

Reject missing direct migration URL, object-store probe, PVC classification, replica ID, fences/outbox, supported adapter locality, and startup probe budget; link each failure to #6/#57. Create two-replica independent-filesystem manifests, dashboards, alerts, rollback runbook, and synthetic runner.

**Verify:** `pnpm test:run`
**Expected outcome:** each missing prerequisite yields actionable failure and runner is invocable.

### 2. Establish one-replica baseline then canary workload

Enable contracts on one replica, record metrics and isolated restore; add second replica without shared process/PVC. Exercise concurrent heartbeats, plugin triggers, cross-replica events/logs/assets, backup, remote executor, and terminal reconnect.

**Verify:** `pnpm db:migrate`
**Expected outcome:** command supports required environment input and migration gate shows single holder; actual run archives workload IDs/metrics.

### 3. Execute fault drills and promotion/rollback gates

Kill worker/publisher/leader during claims, pause DB, restart WS owner, deny object storage, force migration wait, and drain replica. Gate promotion on no duplicate/stale fence, bounded lag, readable objects, restore proof, and owned sessions; otherwise revert feature flag to one replica.

**Verify:** `pnpm test:run`
**Expected outcome:** drill runner exposes documented scenarios and repository regression suite remains green; canary report records pass/fail per drill.

## Test plan

Run Commands table plus deployment-specific synthetic/chaos suite against two independent replicas, PostgreSQL, and object store. Archive run IDs, owner/fence/lag snapshots, object checksums, restore evidence, terminal route traces, and rollback result. Test failure cases before promotion, not after.

## Done criteria

- [ ] Preflight rejects every missing prerequisite and references #6/#57 as applicable.
- [ ] Canary drills show one logical heartbeat/plugin execution, no lost committed event/object, readable backup restore, and safe terminal routing.
- [ ] Operators can inspect owner/fence/lag/locality and drain or roll back to one replica.
- [ ] All Commands table checks exit 0 and artifacts are attached to release review.

## STOP conditions

Immediately stop promotion and return to one replica if any duplicate external execution, stale-fence durable write, cross-replica replay failure, committed-object read failure, migration probe overrun, or cross-session terminal risk appears. Do not compensate with more replicas, timeout, or retry rate; escalate #6 or #57 with artifacts.

## Maintenance notes

Run preflight and representative chaos drills before every deployment topology, DB/provider, proxy, or adapter change. Keep manifests, dashboards, SLO thresholds, and rollback commands versioned with runtime contracts; expire no audit artifact until release acceptance is complete.
