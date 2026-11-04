# 006 — Remote/shared workspace and execution adapters

## Executor preamble

Start only after 001–003 and 005 are accepted. At `9cb229ec9`, reread `server/src/services/workspace-runtime.ts:1576-1760`, `environment-run-orchestrator.ts:327-498`, `workspace-file-resources.ts:320-372`, and `packages/adapter-utils/src/server-utils.ts:2859-3005`. Map every consumer that treats a physical cwd as a logical workspace.

## Status metadata

- **Status:** TODO
- **Priority:** P1
- **Effort:** XL
- **Risk:** High — path/secrets leakage or concurrent workspace mutation.
- **Dependencies:** 001, 002, 003, 005
- **Category:** execution routing / locality
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6)
- **Baseline:** `9cb229ec9`

## Goal

Separate logical workspace identity from host filesystem locality and route a fenced run only to a compatible local, SSH, or sandbox executor.

## Evidence and design

Existing orchestrator already realizes local/SSH/sandbox drivers, while workspace runtime and child maps leak local paths/handles. Promote adapters: logical references in control plane, physical cwd only inside adapter; local worktree/process remains explicitly `host-bound`.

## In scope

- `server/src/services/workspace-runtime.ts`, `workspace-realization.ts`, `execution-workspaces.ts`, `environment-run-orchestrator.ts`, `workspace-file-resources.ts`
- `server/src/services/workspace-adapter.ts` **(create)**, `server/src/services/execution-adapter.ts` **(create)**, `server/src/services/executor-registry.ts` **(create)**
- `packages/db/src/schema/execution_workspaces.ts`, `packages/db/src/schema/environment_leases.ts`, `packages/db/src/schema/index.ts`, `packages/db/drizzle/**` **(create migration)**
- `packages/adapter-utils/src/server-utils.ts`, `server/src/__tests__/execution-adapter-routing.test.ts` **(create)**
- `doc/operations/executor-routing.md` **(create)**

## Out of scope

- Universal network filesystem, Kubernetes mandate, remote file-browser redesign, and migration of every adapter/provider in this release.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Workspace/routing tests exit 0. |
| `pnpm db:generate && pnpm db:migrate` | Reviewed workspace/executor migration is generated and applies. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck` | All workspace typechecks exit 0. |

## Git workflow

Use `active-active/006-execution-routing`; keep 003 owner-token integration and generated migration in the same slice. Run `git diff --check` and baseline scope diff before `feat(active-active): route execution by locality`. Do not deploy executors or change `.pi/`.

## Implementation steps

### 1. Define logical workspace and capability contracts

Create `WorkspaceAdapter`/`ExecutionAdapter` with immutable logical ref, locality/capabilities, realization, cleanup, and reconciliation. Refactor current worktree behind explicit local adapter.

**Verify:** `pnpm test:run`
**Expected outcome:** existing local worktree regression passes while callers receive logical, not host, identity.

### 2. Promote remote adapter and register executors

Make SSH/sandbox realization idempotent, return remote cwd only to executor internals, report version/capability, and register health. Persist snapshot/ref/commit and use object store for artifacts/logs.

**Verify:** `pnpm test:run`
**Expected outcome:** two API replicas realize the same logical workspace remotely without path leak.

### 3. Bind routing to fenced ownership

Select live compatible executor, persist executor/target/realization on run, renew/reconcile with 003 token, and fail visibly if none is compatible.

**Verify:** `pnpm test:run`
**Expected outcome:** executor loss and incompatible routing do not adopt host-bound work; cleanup is idempotent.

## Test plan

Cover local regression, SSH/sandbox realization, independent replicas, local-path redaction, executor loss, incompatible capabilities, concurrent workspace isolation, and fence-aware retry. Run Commands table and record executor/run/fence IDs.

## Done criteria

- [ ] A replica without source worktree routes to a compatible registered executor.
- [ ] DB records logical workspace and execution locality without exposing unusable host paths.
- [ ] Host-bound adapters explicitly fail/reroute and cannot be adopted by another replica.
- [ ] All Commands table checks exit 0.

## STOP conditions

Stop if remote execution receives a local path, two executors can mutate one workspace without isolation/lock, executor identity cannot bind to run fence, or durable metadata contains secrets. Escalate [#6](https://github.com/valkyriweb/paperclip/issues/6).

## Maintenance notes

Treat capability/locality fields as versioned protocol. Every new adapter must declare locality and reconciliation behavior; rerun routing/isolation tests after driver, workspace policy, or owner-token changes.
