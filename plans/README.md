# True active-active executor plans

**Program issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6) · **Migration/probe issue:** [#57](https://github.com/valkyriweb/paperclip/issues/57) · **Baseline:** `9cb229ec9` · **Planned at:** 2026-07-12

This package executes the architecture in [`doc/plans/2026-07-12-paperclip-active-active-reforge.md`](../doc/plans/2026-07-12-paperclip-active-active-reforge.md). PostgreSQL is the coordination authority; shared object storage holds durable blobs; an executor owns work only through a durable lease and fencing token. **Never enable HA by only setting `replicas=2`.**

## Execution and status

| Status | Plan | Priority | Effort | Dependency / gate | Issue | Notes |
|---|---|---:|---:|---|---|---|
| TODO | [001](001-external-postgresql-advisory-locked-migrations.md) | P0 | L | none; establishes external DB/migration gate | [#57](https://github.com/valkyriweb/paperclip/issues/57) | Complete before any multi-replica process starts. |
| TODO | [002](002-shared-object-storage-adapters.md) | P0 | L | 001 | [#6](https://github.com/valkyriweb/paperclip/issues/6) | Required before HA logs or backups. |
| TODO | [003](003-fenced-durable-heartbeat-run-ownership.md) | P0 | XL | 001 | [#6](https://github.com/valkyriweb/paperclip/issues/6) | Defines claim/fence conventions used later. |
| TODO | [004](004-atomic-plugin-job-claims.md) | P0 | L | 001, 003 conventions | [#6](https://github.com/valkyriweb/paperclip/issues/6) | May start only after 003 contract review. |
| TODO | [005](005-durable-event-outbox-fanout.md) | P0 | L | 001 | [#6](https://github.com/valkyriweb/paperclip/issues/6) | Needed before cross-replica live-event claims. |
| TODO | [006](006-remote-shared-workspace-execution-adapters.md) | P1 | XL | 001–003, 005 | [#6](https://github.com/valkyriweb/paperclip/issues/6) | Do not adopt host-bound worktrees. |
| TODO | [007](007-leader-controlled-backups-recovery.md) | P0 | L | 001–003 | [#6](https://github.com/valkyriweb/paperclip/issues/6) | Also requires 002 before enabling backups. |
| TODO | [008](008-terminal-session-routing-broker.md) | P1 | XL | 003, 005, 006 | [#6](https://github.com/valkyriweb/paperclip/issues/6) | Keep a single gateway until its design is proven. |
| TODO | [009](009-multi-replica-canary-rollout.md) | P0 | L | 001–008 accepted | [#6](https://github.com/valkyriweb/paperclip/issues/6), [#57](https://github.com/valkyriweb/paperclip/issues/57) | The only plan that permits canary enablement. |

Execute in numeric order unless the dependency column explicitly permits parallel discovery. A TODO is not ready to start until its dependency gate has passed and its drift check is current.

## Shared executor rules

1. Start from `9cb229ec9`; read the named evidence and run the plan's drift check before editing. Stop if it no longer describes the code.
2. Work on one branch and one slice at a time. Do not mix a schema migration with unrelated cleanup. Do not alter `.pi/`.
3. A lease expiry permits reconciliation, not duplicate side effects. Persist provider/key/checksum before publishing an object; persist state and outbox event atomically.
4. Run every step verification, then the distinct test plan. Record command output, migration IDs, metrics, drill IDs, and deviations in the PR/issue.
5. STOP means no rollout and no dependent plan. Escalate the named issue with evidence; do not solve it by increasing replicas, timeout, or retries.

## Common Git workflow

| Command | Expected result |
|---|---|
| `git switch -c active-active/NNN-<slice>` | A branch for exactly one plan, based on `9cb229ec9` or the accepted dependency commit. |
| `git status --short` | Only intended slice files are listed; `.pi/` is absent. |
| `git diff --check` | Exit status 0; no whitespace errors. |
| `git diff --name-only 9cb229ec9...HEAD` | Only the plan's **In scope** paths (and generated migration) appear. |
| `git commit -am "feat(active-active): <slice>"` | A reviewable, single-slice commit after tests pass. |

Do not push, deploy, apply production migrations, or change replica count from these documents.

## Findings considered and rejected

| Finding / alternative | Decision | Reason |
|---|---|---|
| Set `replicas=2` around current local loops | Rejected | Local maps, timers, `EventEmitter`, PID handles, and backup booleans duplicate work. |
| Shared PVC as the HA coordination mechanism | Rejected | It hides locality and does not provide fencing, auditing, or safe ownership. Track inventory in [#6](https://github.com/valkyriweb/paperclip/issues/6). |
| Table-row-only migration lock | Rejected | It cannot guarantee a pinned PostgreSQL session across inspect/repair/apply; use advisory lock. |
| Exactly-once WebSocket delivery | Rejected | Use durable outbox plus cursor/deduplication; transport remains at-least-once. |
| Adopt a lost local subprocess from another replica | Rejected | Its handles/filesystem are host-bound; reconcile or route through a compatible executor. |

## Package maintenance

Keep this table and each plan's metadata in sync when dependencies, paths, commands, or issue gates change. Mark a row `DONE` only after its checkbox criteria and test plan are evidenced in review; mark it `BLOCKED` with the failed STOP condition otherwise. Preserve the architecture overview/evidence in the companion document rather than duplicating it in implementation PRs.
