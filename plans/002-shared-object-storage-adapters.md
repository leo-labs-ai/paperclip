# 002 — Shared object-storage adapters

## Executor preamble

Start after 001 is accepted. From `9cb229ec9`, reread `server/src/storage/index.ts:21-64`, `server/src/services/run-log-store.ts:1-25,220-278`, `server/src/index.ts:585-646,987-1029`, and `doc/DATABASE.md`. Inventory every writable PVC path before design; do not assume a shared filesystem is safe.

## Status metadata

- **Status:** TODO
- **Priority:** P0
- **Effort:** L
- **Risk:** High — data loss/corruption or retention deletion.
- **Dependencies:** 001
- **Category:** durable storage
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6)
- **Baseline:** `9cb229ec9`

## Goal

Make assets, active/archived run logs, and backup artifacts readable from a replacement replica by using one shared-object contract and durable integrity metadata.

## Evidence and design

Storage already has provider seams, but hot logs and backups use replica-local paths. Extend that seam rather than introduce a parallel S3 client. Commit immutable object metadata (provider/key/length/SHA-256/version) before publishing references; local fallback remains only outside HA.

## In scope

- `server/src/storage/types.ts`, `server/src/storage/index.ts`, `server/src/storage/service.ts`, `server/src/storage/local-disk-provider.ts`, `server/src/storage/s3-provider.ts`
- `server/src/storage/object-store.ts` **(create)** and `server/src/storage/object-store.test.ts` **(create)**
- `server/src/services/run-log-store.ts`, `server/src/index.ts`
- `packages/db/src/schema/external_objects.ts`, `packages/db/src/schema/index.ts`, `packages/db/drizzle/**` **(create migration)**
- `doc/DATABASE.md`, `doc/operations/object-storage.md` **(create)**

## Out of scope

- CDN/public object access, cross-region replication, encryption-key redesign, deleting local-first providers, and backup leader election (007).

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Server/storage tests exit 0. |
| `pnpm db:generate` | Generates only the reviewed object metadata migration, if schema changed. |
| `pnpm db:migrate` | Applies that migration to disposable DB with exit 0. |
| `pnpm test:run` | Repository test suite exits 0. |
| `pnpm -r typecheck` | All workspace typechecks exit 0. |

## Git workflow

Use `active-active/002-object-storage` after 001. Keep generated migration with its schema change; inspect `git diff --check` and `git diff --name-only 9cb229ec9...HEAD` before `feat(active-active): add shared object storage`. Do not push or alter `.pi/`.

## Implementation steps

### 1. Define and test the object contract

Require streaming put/get, head, conditional put/delete, checksum, length, provider, and version/etag metadata. Implement local-disk and S3-compatible adapters behind the existing registry.

**Verify:** `pnpm test:run`
**Expected outcome:** byte range, conditional conflict, interrupted upload, and checksum mismatch tests pass for both adapters.

### 2. Commit objects before durable references

Migrate assets, run logs, and backup writer paths to write immutable object then metadata/reference; stream large data. Add dual-read/copy-forward keyed by legacy path/checksum and visible corruption status.

**Verify:** `pnpm test:run && pnpm db:migrate`
**Expected outcome:** a second process reads newly committed logs/assets without a shared path and legacy fallback is auditable.

### 3. Define layout, lifecycle, and preflight

Use company prefixes and reserved system backup/log prefixes. Add retention only for committed objects and document lifecycle plus [#6](https://github.com/valkyriweb/paperclip/issues/6) PVC inventory.

**Verify:** `pnpm test:run`
**Expected outcome:** retention retains latest/verified manifests and preflight reports every writable local path.

## Test plan

Test two independent service instances with one object store; interrupted/conditional uploads, checksum failure, dual-read recovery, active log reads, and backup restore without shared filesystem. Run Commands table and retain object key/checksum evidence.

## Done criteria

- [ ] HA references never point only to a replica-local asset/log/backup path.
- [ ] Replacement replicas read committed objects and active/completed logs.
- [ ] Backup manifests are checksum-verified; retention deletes only committed, eligible objects.
- [ ] All Commands table checks exit 0.

## STOP conditions

Stop and escalate [#6](https://github.com/valkyriweb/paperclip/issues/6) if cross-replica reads fail, a reference publishes before durable integrity metadata, correctness needs a shared PVC, or retention can remove a referenced object.

## Maintenance notes

Treat object key layout and metadata as compatibility API. Re-run adapter compatibility and restore tests when changing S3 provider behavior, lifecycle rules, or log/backup serialization; periodically reconcile copy-forward failures and PVC inventory.
