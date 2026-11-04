# 007 — Leader-controlled backups and recovery

## Executor preamble

Start after 001–003; do not enable shared backup until 002 has passed. At `9cb229ec9`, read `server/src/index.ts:589-646,780-1029`, `server/src/services/run-log-store.ts`, and storage providers. Inventory every startup `setInterval`/`setTimeout` owner and audit [#6](https://github.com/valkyriweb/paperclip/issues/6) PVC paths.

## Status metadata

- **Status:** TODO
- **Priority:** P0
- **Effort:** L
- **Risk:** Critical — lost backups, destructive retention, duplicate recovery.
- **Dependencies:** 001, 002, 003
- **Category:** operations / leadership
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6)
- **Baseline:** `9cb229ec9`

## Goal

Run backup, retention, log archival, and recovery scanning as named fenced leader duties with shared checksum manifests and operator-confirmed restore.

## Evidence and design

`databaseBackupInFlight` and startup intervals are process-local. Generalize 003 lease/fence by duty name/partition, record operation intent/outcome, and never mark backup successful until a fresh replica can read its manifest.

## In scope

- `packages/db/src/schema/leader_operations.ts` **(create)**, `packages/db/src/schema/index.ts`, `packages/db/drizzle/**` **(create migration)**
- `server/src/services/leadership-store.ts` **(create)**, `server/src/services/leadership-store.test.ts` **(create)**
- `server/src/index.ts`, `server/src/services/heartbeat.ts`, `server/src/services/run-log-store.ts`, `server/src/storage/service.ts`
- `doc/operations/backup-recovery.md` **(create)**

## Out of scope

- Automatic restore, PITR product, cross-region DR, and collapsing all schedulers into one process.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Leadership/backup/recovery tests exit 0. |
| `pnpm db:generate && pnpm db:migrate` | Reviewed leadership-operation migration is generated and applies. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck` | All workspace typechecks exit 0. |

## Git workflow

Use `active-active/007-leader-backups`; include only duty, storage, migration, and runbook paths. Run `git diff --check` and scope diff before `feat(active-active): fence backup leadership`. No production restore or deployment.

## Implementation steps

### 1. Implement named durable leader duties

Add acquire/renew/release by duty/partition with monotonic fence and operation rows containing trigger, holder, timing, outcome, manifest, and safe error summary.

**Verify:** `pnpm test:run && pnpm db:migrate`
**Expected outcome:** two replicas race to one leader and stale leader cannot commit/delete.

### 2. Fence backup lifecycle and restore evidence

Write intent, dump, object/manifest validation through 002, then commit success. A new leader reconciles incomplete intent; manual requests route to current leader or return `leader_unavailable`.

**Verify:** `pnpm test:run`
**Expected outcome:** crash before/after upload leaves auditable operation and only verified manifest is restorable.

### 3. Move singleton timers through leader duties

Wrap reaper/watchdog/archive/retention scans in bounded due scans; every candidate still uses own claim/fence. Publish drain/revoke/restore metrics and runbook.

**Verify:** `pnpm test:run`
**Expected outcome:** simultaneous scans cannot double-finalize runs or prune latest verified restore point.

## Test plan

Run two-replica duty race, leader crash at both upload boundaries, manual during scheduled backup, stale retention, recovery scan race, fresh-replica manifest read, and isolated restore. Run Commands table and retain operation/manifest/checksum evidence.

## Done criteria

- [ ] Exactly one fenced leader performs each singleton duty at a time.
- [ ] Every successful backup has readable shared checksum manifest and isolated restore proof.
- [ ] Failover leaves auditable incomplete/succeeded/failed operation, never ambiguous filesystem state.
- [ ] All Commands table checks exit 0.

## STOP conditions

Stop if success precedes fresh-replica manifest read, two leaders can prune same set, restore needs undocumented local secret/key/PVC, or recovery external work lacks own claim/fence. Escalate [#6](https://github.com/valkyriweb/paperclip/issues/6).

## Maintenance notes

Register every new singleton timer as a named duty and review its partitioning. Exercise restore and leader-failover drill after storage, retention, scheduler, or credential changes; keep last verified restore evidence with release records.
