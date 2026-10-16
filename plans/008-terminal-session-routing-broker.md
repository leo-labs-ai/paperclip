# 008 — Terminal/session routing or broker

## Executor preamble

Start only after 003, 005, and 006. At `9cb229ec9`, map terminal setup-session rows, token minting, connection registry, SSH connector, adapter session codecs, and all input/output authority paths. Read `server/src/realtime/environment-custom-image-terminal-ws.ts:484-574`, `server/src/index.ts:708-714`, and `packages/adapter-utils/src/server-utils.ts:80,2859-3005`.

## Status metadata

- **Status:** TODO
- **Priority:** P1
- **Effort:** XL
- **Risk:** Critical — cross-company shell access or stale input.
- **Dependencies:** 003, 005, 006
- **Category:** realtime / security / routing
- **Planned at:** 2026-07-12
- **Issue:** [#6](https://github.com/valkyriweb/paperclip/issues/6)
- **Baseline:** `9cb229ec9`

## Goal

Safely reconnect a terminal through a different API replica via an explicitly selected sticky gateway or broker, durable owner lease, generation fencing, and short-lived scoped reconnect token.

## Evidence and design

Terminal WebSocket, SSH shell, timer, and registry are process-local. First complete a bounded design spike: choose L7 sticky gateway only if proxy affinity/failure semantics are proven; otherwise use a dedicated broker/gateway. Neither option serializes arbitrary PTY state into PostgreSQL.

## In scope

- `server/src/realtime/environment-custom-image-terminal-ws.ts`, `server/src/realtime/live-events-ws.ts`, `server/src/index.ts`
- `server/src/services/terminal-session-route.ts` **(create)**, `server/src/services/terminal-gateway.ts` **(create)**, `server/src/services/terminal-session-route.test.ts` **(create)**
- `packages/db/src/schema/environment_custom_image_setup_sessions.ts`, `packages/db/src/schema/index.ts`, `packages/db/drizzle/**` **(create migration)**
- `packages/adapter-utils/src/server-utils.ts`, `doc/operations/terminal-routing.md` **(create)**

## Out of scope

- Serializing arbitrary PTY/SSH state, terminal UX redesign, cross-provider live session migration, or bypassing setup-session authorization.

## Commands

| Command | Expected result |
|---|---|
| `pnpm test:run` | Terminal route/auth tests exit 0. |
| `pnpm db:generate && pnpm db:migrate` | Reviewed registry/lease migration is generated and applies. |
| `pnpm test:run` | Repository suite exits 0. |
| `pnpm -r typecheck` | All workspace typechecks exit 0. |

## Git workflow

Use `active-active/008-terminal-routing`; commit the design decision with implementation, migration, tests, and runbook only. Run `git diff --check` and baseline scope diff before `feat(active-active): route terminal sessions`. Do not modify proxy production config or push.

## Implementation steps

### 1. Decide and record the gateway strategy

Compare sticky gateway and broker against current load balancer, failure domains, latency, and executor locality. Select one with explicit fallback; retain single gateway if proof is absent.

**Verify:** `pnpm test:run`
**Expected outcome:** selected strategy's route decision and unavailable state are deterministic and documented.

### 2. Persist owner route and fence input

Register session before shell open with company/session, gateway/executor, expiry, generation, lease/fence. Mint audience-bound, short-lived, single-purpose reconnect token; route before WebSocket upgrade.

**Verify:** `pnpm test:run`
**Expected outcome:** reconnect via B reaches owner A or safely returns unavailable; cross-company/expired token fails.

### 3. Handle drain, loss, and status propagation

Probe adapter on owner loss to resume or close, increment generation before replacement, reject stale input, emit close/availability through 005, and drain with bounded revocation/cleanup.

**Verify:** `pnpm test:run`
**Expected outcome:** reconnect race, drain, crash, stale generation, and SSH cleanup each leave one authoritative session.

## Test plan

Test A-connect/B-reconnect, cross-company/expired token, reconnect race, owner drain/crash, stale socket input, locality-bound unavailable executor, and cleanup. Run Commands table; retain route/generation/fence traces and load-balancer proof.

## Done criteria

- [ ] Reconnect never attaches wrong shell or creates silent duplicate logical terminal.
- [ ] Owner/generation/status is inspectable and durable close/availability events emit.
- [ ] Chosen strategy has tested load-balancer affinity and drain runbook.
- [ ] All Commands table checks exit 0.

## STOP conditions

Stop if company/session authorization is not provable, stale socket writes after ownership change, proxy cannot preserve required affinity, or executor cannot report session liveness. Keep terminals single-gateway and escalate [#6](https://github.com/valkyriweb/paperclip/issues/6).

## Maintenance notes

Treat reconnect token claims and generation semantics as security protocol; rotate/test them with auth changes. Re-run cross-replica reconnect and drain tests after proxy, adapter session codec, or terminal lifecycle changes.
