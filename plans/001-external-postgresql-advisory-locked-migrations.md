# 001 — External PostgreSQL and advisory-locked migrations

## Executor preamble

Work only from baseline `9cb229ec9` (2026-07-12). This is a P0 prerequisite: do not start a second API replica until this plan is accepted. Read `server/src/index.ts:158-205,324-329,485-489`, `packages/db/src/client.ts:233-278`, and `doc/DATABASE.md`; if those seams drift, update this plan and obtain DB/operator review before editing.

## Status metadata

- **Status:** TODO
- **Priority:** P0
- **Effort:** L
- **Risk:** High — schema coordination can block startup or corrupt migrations.
- **Dependencies:** none
- **Category:** database / rollout safety
- **Planned at:** 2026-07-12
- **Issue:** [#57](https://github.com/valkyriweb/paperclip/issues/57)
- **Baseline:** `9cb229ec9`

## Goal

Require external PostgreSQL and serialize inspection, repair, apply, and final inspection through one session-scoped advisory lock in the multi-replica profile, while preserving embedded PostgreSQL local development.

## Evidence and design

Every server currently calls the migration path at startup; `packages/db/src/client.ts` makes each file transactional but has no cross-connection lock. The direct migration URL documented in `doc/DATABASE.md` is the leverage seam. Use a pinned direct PostgreSQL session and `pg_advisory_lock`; a waiter must re-inspect after lock acquisition, never reuse a stale pending list.

## In scope

- `packages/db/src/migration-runtime.ts` and `packages/db/src/migration-status.ts`
- `packages/db/src/migration-coordinator.ts` **(create)** and `packages/db/src/migration-coordinator.test.ts` **(create)**
- `packages/db/src/client.ts`, `packages/db/src/index.ts`, `packages/db/src/runtime-config.ts`
- `server/src/index.ts`, `server/src/config.ts`, `server/src/__tests__/environment-config.test.ts`
- `doc/DATABASE.md`

## Out of scope

- `packages/db/src/schema/**` (no claim/outbox schema), automatic online-migration framework, shared storage, and removal of embedded local development.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | DB unit/integration suite exits 0; coordinator race tests pass when PostgreSQL test configuration is present. |
| `pnpm db:generate` | Exits 0 and does not generate a schema migration for this coordination-only change. |
| `pnpm db:migrate` | Exits 0 against the configured disposable DB; only one holder runs migrations. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck` | All workspace typechecks exit 0. |

## Git workflow

Create `active-active/001-migration-lock`; keep edits limited to **In scope**. Before commit run `git diff --check` and `git diff --name-only 9cb229ec9...HEAD`; commit as `feat(active-active): coordinate migrations`. Do not push, deploy, or run a production migration.

## Implementation steps

### 1. Create the pinned-session coordinator

Add `MigrationCoordinator.withExclusiveMigrationLock()` returning holder, wait, duration, and state metadata. Hash a stable lock key for logs; acquire/release in `finally` on the same direct session.

**Verify:** `pnpm test:run`
**Expected outcome:** two real connections show one holder; release and failure both free the lock.

### 2. Place the full startup critical section behind it

Make `server/src/index.ts` run inspect, repair, apply, and final inspect only inside the coordinator. A waiter reports `waiting_for_migration_lock`, then re-inspects after acquisition; retain embedded mode behavior.

**Verify:** `pnpm test:run`
**Expected outcome:** a blocked holder prevents concurrent apply and a waiter succeeds after release.

### 3. Add HA configuration, readiness, and operator guidance

For `multi_replica`, require external `DATABASE_URL` and non-transaction-pooled `DATABASE_MIGRATION_URL`; bound waiting and expose `waiting_for_migration_lock`, `migrating`, `ready`, or failure without logging URLs. Document probe budget/rollback for [#57](https://github.com/valkyriweb/paperclip/issues/57).

**Verify:** `pnpm test:run && pnpm db:migrate`
**Expected outcome:** invalid HA config is rejected, valid config migrates once, and readiness states are deterministic.

## Test plan

- Add two-connection PostgreSQL integration coverage for blocked holder, waiter takeover, timeout, and failed migration release.
- Cover embedded mode unchanged and HA configuration rejection for missing/direct pooled URLs.
- Run the Commands table in order; attach lock wait/duration output and migration ID to review evidence.

## Done criteria

- [ ] Two concurrent startups produce exactly one migration executor; the waiter re-inspects before serving.
- [ ] A bounded wait is observable in readiness/logs and does not crashloop solely because another replica migrates.
- [ ] HA configuration cannot select embedded or transaction-pooled migration storage silently.
- [ ] `pnpm test:run`, `pnpm db:migrate`, `pnpm test:run`, and `pnpm -r typecheck` exit 0.

## STOP conditions

Stop and escalate [#57](https://github.com/valkyriweb/paperclip/issues/57) if the provider lacks session advisory locks, exposes only transaction pooling, migration duration exceeds agreed probe budget, or any test lets a waiter execute SQL concurrently. Do not begin 002–009.

## Maintenance notes

Keep the lock key stable across releases; version/configure timeout deliberately and never log connection strings. When migration startup, readiness, or provider topology changes, rerun the two-connection test and revise `doc/DATABASE.md` plus this plan's commands/evidence.
