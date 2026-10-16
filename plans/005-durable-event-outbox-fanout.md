# 005 — Durable event outbox and fanout

## Executor preamble

Start after 001. At `9cb229ec9`, inventory every `publishLiveEvent`/`publishGlobalLiveEvent` caller and classify it transactional state vs best-effort signal. Read `server/src/services/live-events.ts:7-42`, `server/src/realtime/live-events-ws.ts:170-215`, `server/src/index.ts:711-714`, and heartbeat producers before editing.

## Status metadata

- **Status:** TODO
- **Priority:** P0
- **Effort:** L
- **Risk:** High — lost authorization boundary or unbounded data growth.
- **Dependencies:** 001
- **Category:** events / realtime
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6)
- **Baseline:** `9cb229ec9`

## Goal

Persist company-scoped state events transactionally and fan them out across replicas with authorized replay, cursoring, and at-least-once deduplication.

## Evidence and design

The current event counter and `EventEmitter` are per-process and WebSockets subscribe locally. Use an outbox as source of truth; `LISTEN/NOTIFY` (or approved broker) is only wakeup. Consumers always read durable rows and checkpoint only after processing.

## In scope

- `packages/db/src/schema/live_event_outbox.ts` **(create)**, `packages/db/src/schema/index.ts`, `packages/db/drizzle/**` **(create migration)**
- `server/src/services/live-events.ts`, `server/src/services/domain-event-outbox.ts` **(create)**, `server/src/services/domain-event-outbox.test.ts` **(create)**
- `server/src/realtime/live-events-ws.ts`, `server/src/index.ts`, `server/src/services/heartbeat.ts`
- `doc/operations/live-event-replay.md` **(create)**

## Out of scope

- Exactly-once WebSocket delivery, event-sourcing rewrite, cross-company subscriptions, bulk replay UI, or synchronous broker publishing inside DB transactions.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Outbox/WebSocket tests exit 0. |
| `pnpm db:generate && pnpm db:migrate` | Reviewed outbox/cursor migration is generated and applies. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck` | All workspace typechecks exit 0. |

## Git workflow

Use `active-active/005-event-outbox`. Commit only producer/outbox/WS/migration paths after `git diff --check`; title `feat(active-active): persist live events`. No push, broker deployment, or global `EventEmitter` removal before vertical slice evidence.

## Implementation steps

### 1. Persist and consume immutable outbox rows

Define globally cursorable ID, company, aggregate/version, schema-versioned non-secret payload, timestamp, and consumer checkpoint. Write state and outbox row atomically.

**Verify:** `pnpm test:run && pnpm db:migrate`
**Expected outcome:** committed mutation remains deliverable after publisher restart.

### 2. Add multi-replica-safe fanout and replay

Use notification only to wake polling; consume/checkpoint safely. Accept bounded `afterEventId`, authorize before replay, then stream; clients dedupe IDs.

**Verify:** `pnpm test:run`
**Expected outcome:** socket on replica B receives authorized event from A and reconnect ordering is stable.

### 3. Migrate producers and retention observability

Move heartbeat first, then selected plugin/mutation producers. Add lag/replay/dedupe/poison metrics and retention after replay-window/consumer-lag policy.

**Verify:** `pnpm test:run`
**Expected outcome:** duplicate wakeup and poison payload do not skip subsequent authorized events.

## Test plan

Test commit while fanout down, A-to-B socket delivery, restart before/after checkpoint, replay authorization, duplicate notification, poison isolation, and retention boundary. Run Commands table and retain cursor/lag snapshots.

## Done criteria

- [ ] Committed event on any replica is eventually observable on every authorized socket path.
- [ ] Publisher exit cannot silently lose a committed state transition event.
- [ ] Clients safely deduplicate and resume from cursor.
- [ ] All Commands table checks exit 0.

## STOP conditions

Stop if payload contains credentials/large log chunks, checkpoint can skip unprocessed event, replay bypasses company auth, or no owner bounds storage. Do not globally replace `EventEmitter` until first producer/replay slice passes; escalate [#6](https://github.com/valkyriweb/paperclip/issues/6).

## Maintenance notes

Version payload schemas and keep replay authorization aligned with WebSocket upgrades. New transactional producers must use the outbox; monitor lag/poison rows and test retention on every cursor-policy change.
