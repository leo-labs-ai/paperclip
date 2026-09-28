# Database

Paperclip uses PostgreSQL via [Drizzle ORM](https://orm.drizzle.team/). There are three ways to run the database, from simplest to most production-ready.

## 1. Embedded PostgreSQL — zero config

If you don't set `DATABASE_URL`, the server automatically starts an embedded PostgreSQL instance and manages a local data directory.

```sh
pnpm dev
```

That's it. On first start the server:

1. Creates a `~/.paperclip/instances/default/db/` directory for storage
2. Ensures the `paperclip` database exists
3. Runs migrations automatically for empty databases
4. Starts serving requests

Data persists across restarts in `~/.paperclip/instances/default/db/`. To reset local dev data, delete that directory.

If you need to apply pending migrations manually, run:

```sh
pnpm db:migrate
```

When `DATABASE_URL` is unset, this command targets the current embedded PostgreSQL instance for your active Paperclip config/instance.

Issue reference mentions follow the normal migration path: the schema migration creates the tracking table, but it does not backfill historical issue titles, descriptions, comments, or documents automatically.

To backfill existing content manually after migrating, run:

```sh
pnpm issue-references:backfill
# optional: limit to one company
pnpm issue-references:backfill -- --company <company-id>
```

Future issue, comment, and document writes sync references automatically without running the backfill command.

This mode is ideal for local development and one-command installs.

Docker note: the Docker quickstart image also uses embedded PostgreSQL by default. Persist `/paperclip` to keep DB state across container restarts (see `doc/DOCKER.md`).

## 2. Local PostgreSQL (Docker)

For a full PostgreSQL server locally, use the included Docker Compose setup:

```sh
docker compose up -d
```

This starts PostgreSQL 17 on `localhost:5432`. Then set the connection string:

```sh
cp .env.example .env
# .env already contains:
# DATABASE_URL=postgres://paperclip:paperclip@localhost:5432/paperclip
```

Run migrations:

```sh
DATABASE_URL=postgres://paperclip:paperclip@localhost:5432/paperclip \
  pnpm db:migrate
```

Start the server:

```sh
pnpm dev
```

## 3. Hosted PostgreSQL (Supabase)

For production, use a hosted PostgreSQL provider. [Supabase](https://supabase.com/) is a good option with a free tier.

### Setup

1. Create a project at [database.new](https://database.new)
2. Go to **Project Settings > Database > Connection string**
3. Copy the URI and replace the password placeholder with your database password

### Connection string

Supabase offers two connection modes:

**Direct connection** (port 5432) — use for migrations and one-off scripts:

```
postgres://postgres.[PROJECT-REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:5432/postgres
```

**Connection pooling via Supavisor** (port 6543) — use for the application:

```
postgres://postgres.[PROJECT-REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres
```

### Configure

For the application runtime, use a direct PostgreSQL connection unless the database client has explicit prepared-statement configuration for your pooling mode:

```sh
DATABASE_URL=postgres://postgres.[PROJECT-REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:5432/postgres
```

If you later run the app with a pooled runtime URL, set `DATABASE_MIGRATION_URL` to the direct connection URL. Paperclip uses it for startup schema checks/migrations and plugin namespace migrations, while the app continues to use `DATABASE_URL` for runtime queries:

```sh
DATABASE_URL=postgres://postgres.[PROJECT-REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres
DATABASE_MIGRATION_URL=postgres://postgres.[PROJECT-REF]:[PASSWORD]@aws-0-[REGION].pooler.supabase.com:5432/postgres
# Required with PAPERCLIP_DEPLOYMENT_PROFILE=multi_replica after verifying the endpoint contract:
DATABASE_MIGRATION_SESSION_CAPABLE=true
```

If your hosted database requires transaction-pooling-only connections (pgbouncer transaction mode, Supavisor port 6543, Neon `-pooler` endpoints), set `DATABASE_PREPARED_STATEMENTS=false` so the client does not rely on session-scoped prepared statements, and keep `DATABASE_MIGRATION_URL` on a direct connection. Do not edit database client source files as part of deployment setup.

Multi-replica deployments must also set `DATABASE_MIGRATION_SESSION_CAPABLE=true`. This is an explicit operator attestation that `DATABASE_MIGRATION_URL` reaches one stable PostgreSQL backend session for the lifetime of a connection and supports session advisory locks. PostgreSQL URLs do not encode pooling mode, so Paperclip fails unknown endpoints closed instead of guessing from provider hostnames or ports. Do not set this flag for PgBouncer/Supavisor transaction-pooling endpoints. `pnpm db:migrate` reads the same process environment, config-adjacent `.env`, and current-working-directory `.env` layers as server startup, with that precedence, including the same migration URL, profile, attestation, and lock-timeout contract. A defined blank or whitespace-only `DATABASE_URL` or `DATABASE_MIGRATION_URL` is an invalid override in both entrypoints; it fails closed instead of falling through to a lower-precedence database.

### Multi-replica migration coordination

Set `PAPERCLIP_DEPLOYMENT_PROFILE=multi_replica` before running more than one API replica. This profile refuses embedded PostgreSQL and requires both:

- `DATABASE_URL` for normal application queries; and
- `DATABASE_MIGRATION_URL` for a **direct, session-capable** PostgreSQL connection. Do not use a transaction-pooler endpoint for the migration URL; and
- `DATABASE_MIGRATION_SESSION_CAPABLE=true` as the explicit operator attestation for that endpoint contract.

Startup and `pnpm db:migrate` serialize migration inspection, migration-history repair, application, and final inspection with a stable PostgreSQL session advisory lock. A waiting replica re-inspects the schema after acquiring the lock; it never applies a stale pending-migration list. Logs expose only the hashed lock id and the states `waiting_for_migration_lock`, `migrating`, `ready`, or `failed`, never either connection string.

If the reserved PostgreSQL session closes, Paperclip records lost lock ownership, waits for the migration callback to settle, performs best-effort idempotent cleanup, and reports `failed` instead of issuing another query through the closed session. Reserved-session teardown uses postgres.js's forced one-second `end` timeout so backend-loss races cannot leave startup stuck in `migrating`. Migration callbacks are not generally cancellable, so session loss cannot stop DDL already running through separate clients; operators must treat the deployment as failed and verify database state before retrying.

`PAPERCLIP_MIGRATION_LOCK_TIMEOUT_MS` bounds how long startup waits (default: 10 minutes). Keep the Kubernetes startup-probe budget longer than this timeout plus the longest measured migration. If migration work fails, leave the deployment at one replica, fix or roll back the migration, and retry; do not bypass the lock or point the migration URL at a transaction pooler.

### Client tuning (optional)

All of these are optional; when unset, the driver defaults apply and behavior is unchanged — typical self-hosted setups need none of them:

```sh
DATABASE_PREPARED_STATEMENTS=false   # required for transaction-mode poolers; default: enabled
DATABASE_POOL_MAX=25                 # connection pool size; default: 10
DATABASE_IDLE_TIMEOUT_SECONDS=60     # close idle pooled connections; default: 60 (0 = keep open)
DATABASE_CONNECT_TIMEOUT_SECONDS=10  # default: 30
DATABASE_MAX_LIFETIME_SECONDS=1800   # recycle a pooled connection after this long; default: 30-60 min (random)
DATABASE_APPLICATION_NAME=paperclip  # application_name in pg_stat_activity; default: paperclip
```

### Push the schema

```sh
# Use the direct connection (port 5432) for schema changes
DATABASE_URL=postgres://postgres.[PROJECT-REF]:[PASSWORD]@...5432/postgres \
  pnpm db:migrate
```

### Free tier limits

- 500 MB database storage
- 200 concurrent connections
- Projects pause after 1 week of inactivity

See [Supabase pricing](https://supabase.com/pricing) for current details.

## Switching between modes

The database mode is controlled by `DATABASE_URL`:

| `DATABASE_URL` | Mode |
|---|---|
| Not set | Embedded PostgreSQL (`~/.paperclip/instances/default/db/`) |
| `postgres://...localhost...` | Local Docker PostgreSQL |
| `postgres://...supabase.com...` | Hosted Supabase |

Your Drizzle schema (`packages/db/src/schema/`) stays the same regardless of mode.

## Connection pool timeouts

The application pool (`createDb()`) closes idle connections so a database-side *smart*
shutdown (failover, maintenance, CloudNativePG switchover) does not have to wait on
Paperclip. postgres.js defaults `idle_timeout` to `null`, i.e. an idle session is never
closed, so a smart shutdown blocks for its full timeout before escalating — measured as a
~181s primary outage for what should be a ~2s failover.

| Env var | Default | Meaning |
|---|---|---|
| `PAPERCLIP_DB_IDLE_TIMEOUT_SEC` | `30` | Close a pooled connection after this many seconds idle. `0` disables (postgres.js default / pre-fix behaviour). |
| `PAPERCLIP_DB_MAX_LIFETIME_SEC` | `1800` | Recycle a connection after this many seconds, even if busy. `0` disables. |

The defaults match the measured workload (bursty 30s heartbeat ticks, 0–2 concurrent
queries against a default pool of 10), so the pool drains shortly after the app goes quiet
instead of holding sessions open indefinitely.

One caveat on `max_lifetime`: postgres.js already bounds it by default, to a
*per-connection* random 30–60 minutes. Pinning the fixed 1800s above trades that jitter for
determinism, so a pool opened at startup expires together. If you see herd reconnects,
lengthen `PAPERCLIP_DB_MAX_LIFETIME_SEC` or set it to `0` to fall back to postgres.js's
jittered default.

### HA-failover connection recovery (HOM-441)

A CloudNativePG primary switchover can leave a pooled connection's TCP session open on the
client side with neither a FIN nor an RST. The backend never received the in-flight query,
so PostgreSQL's own `statement_timeout` never fires for it — the client just waits forever
for a reply that will never come. Three layers close that gap, from the OS socket up to the
auth request itself:

| Env var | Default | Meaning |
|---|---|---|
| `PAPERCLIP_DB_KEEPALIVE_SEC` | `15` | postgres.js `keep_alive`: TCP keepalive idle delay before the OS sends the first probe on a pooled socket. `0` disables. How fast the kernel gives up after that first probe depends on `TCP_KEEPINTVL`/`TCP_KEEPCNT`, which Node does not expose per-socket — tuning those is the paired lue-kube (pod `sysctls`) change, not this one. |
| `PAPERCLIP_DB_SOCKET_TIMEOUT_MS` | derived from `statement_timeout` (comfortably above it; `10` minutes when `statement_timeout` is disabled) | Client-side backstop that does not depend on OS keepalive tuning at all: destroys a pooled socket after this many ms without a single byte *read* from the peer, forcing postgres.js's existing reconnect path to replace it on the next query. Reads only, not reads-or-writes: postgres.js pipelines, so counting outbound writes would let every further query written onto an already-dead socket postpone the backstop indefinitely. `0` disables. |
| `PAPERCLIP_DB_CONNECT_TIMEOUT_SEC` | `10` | postgres.js `connect_timeout`. The driver default (30s) is tuned for an occasionally-slow network, not a repeatedly-unreachable failed-over primary; a shorter budget means a doomed connection attempt fails, and the next attempt (which may hit a now-healthy replica-turned-primary) starts sooner. `0` restores the driver default (the option is left unset rather than passed through as `0`, which postgres.js would read as "no connect timer at all"). With the socket factory below installed this bound also covers the TCP dial itself, which the driver's own `connect_timeout` does not: postgres.js starts its connect timer only after the factory has returned a socket. The upstream `DATABASE_CONNECT_TIMEOUT_SECONDS` knob still wins when set. |
| `PAPERCLIP_AUTH_DB_TIMEOUT_MS` | `5000` | Application-level ceiling on a single Bearer/agent-key auth DB lookup in `server/src/middleware/auth.ts` (`authDbLookup`), including its own transient-connection retries. This is the layer that actually bounds the auth request: a connection recovery attempt that itself lands on another still-unreachable target has no bound of its own inside postgres.js (its handling of a first query on a brand-new connection retries silently until it either succeeds or the process exits), so this timeout is what turns a hung Bearer/agent-key request into a fast, clear failure instead of an indefinite hang. |

`PAPERCLIP_DB_SOCKET_TIMEOUT_MS` is implemented via a custom postgres.js `socket` factory
(`socketWithInactivityTimeout` in `packages/db/src/client.ts`) since there is no native
driver option for it. That factory is installed only for a **single-host** `DATABASE_URL`:
postgres.js's custom-socket branch skips its own dial, and with it the per-connection host
rotation (`hostIndex`) a comma-separated multi-host connection string relies on — a factory
shared by the whole pool cannot reproduce a per-connection cursor without scattering fresh
connections across hosts that may still be standbys. A multi-host URL therefore keeps the
driver's dial, its rotation, and OS keepalive (`PAPERCLIP_DB_KEEPALIVE_SEC`) plus
`PAPERCLIP_AUTH_DB_TIMEOUT_MS` as its failover backstops; the lue-kube topology this was
written for uses the single rw service, so it gets all three layers. See `packages/db/src/socket-inactivity-timeout.test.ts` for a
simulated-failover regression test (a fake wire-protocol TCP server that completes the
startup handshake and then goes silent, standing in for the dead switchover peer) and
`server/src/__tests__/auth-db-lookup.test.ts` for the same scenario proven bounded at the
auth layer, including the pathological case where the pool-level backstop's own retry
lands on a connection that never gives up on its own.

A dedicated, unauthenticated readiness endpoint exercises the same pool the Bearer/agent-key
auth path draws from: `GET /api/health/auth-db` runs one `SELECT 1` through `authDbLookup`
(bounded to the existing 3s health-probe timeout, not the general-purpose 5s default above)
and returns `200 {"status":"ready"}` or `503 {"status":"not_ready"}` with no further detail.
Wire this path into the k8s `readinessProbe` (paired lue-kube change) so a wedged auth-path
connection pulls the pod out of rotation instead of continuing to accept traffic it cannot
serve.

A Bearer/agent-key request whose auth lookup hits `PAPERCLIP_AUTH_DB_TIMEOUT_MS` answers
`503 {"error": …, "code": "AUTH_DB_TIMEOUT"}`, not a generic 500, so a caller (and anything
watching the API) can tell a transiently unreachable database from a server bug and retry.

## Migration authoring checklist

The 0126 issue comment attribution backfill showed the failure mode this checklist is meant to prevent: each batch looked for the next rows with an unindexed predicate, so PostgreSQL repeatedly scanned the same table and the migration became O(n²) as the table grew.

When authoring migrations or one-time backfills:

- Create the supporting index for the batch predicate before the backfill loop runs.
- Bound batches by an indexed key, such as an id range or keyset pagination cursor. Do not use `OFFSET` pagination or a query shape that re-scans already-visited rows each batch.
- Avoid unbounded full-table `UPDATE` or `DELETE` statements. Add a selective predicate and process rows in bounded batches when table size can be large.
- Use `CREATE INDEX CONCURRENTLY` for large existing tables when the migration can run outside a transaction and must avoid long write locks.
- Split schema changes, index creation, and data backfill into separate phases so each step has clear locking and rollback behavior.
- Treat the `check:migrations` CI gate as the enforcement backstop for these rules. If it flags a migration, rewrite the migration or add a suppression comment with the indexed predicate, batch bound, and reason the remaining scan is safe.

## Migration snapshots

`drizzle-kit generate` diffs `packages/db/src/schema/` against the newest snapshot in `packages/db/src/migrations/meta/`. That snapshot must describe the schema that every migration produces when they run in order. A snapshot that drifts from the schema makes the *next* migration wrong, because `generate` folds the drift into it. The drift can add a column that an earlier migration already created, which makes that migration fail on a fresh database. It can also drop a column that the schema still uses.

- Create every migration with `pnpm --filter @paperclipai/db generate`. Do not hand-write a snapshot.
- Do not hand-edit a snapshot to resolve a merge conflict. Renumber your migration and run `generate` again, as `packages/db/.gitattributes` describes.
- `packages/db/src/migration-snapshot-drift.test.ts` is the enforcement backstop. It repeats the diff that `generate` performs and fails when the newest snapshot no longer matches `packages/db/src/schema/`.

## Cloud runtime identity singleton

The private `instance_settings` row whose singleton key is
`cloud-runtime-identity/v1` records the immutable Cloud stack id, warm-pool
claim id, previous pool origin, canonical origin, and stack slug accepted from
Cloud's signed pre-activation assertion. It is separate from the normal
`default` settings row and never appears in the settings API. This is
intentionally instance-scoped rather than company-scoped: an instance has one
public identity, and the existing unique singleton-key index makes concurrent
or later attempts to replace it fail closed. The server loads the row before
constructing URL-dependent runtime services on every boot.

## Resource membership tables

Paperclip stores current-user sidebar membership state in:

- `project_memberships`
- `agent_memberships`

These rows are company-scoped and user-scoped. A missing row means the user is joined, so existing users keep seeing projects and agents in the sidebar until they explicitly leave them. Rows only control sidebar visibility; they do not affect project/agent detail access, all-pages, selectors, assignment flows, or existing company permissions.

Both tables use a unique key on `(company_id, user_id, resource_id)` and keep `state` as `joined` or `left`. Join/leave mutations are idempotent board-user `/me` operations and write activity entries when the effective state changes.

## Decision training snapshot retention

`decision_training_examples` stores a point-in-time copy of an issue, its comments, relevant runs, and the selected decision. Each row carries the `scrub_deleted_comments_v1` retention policy marker, and JSONL exports include that marker alongside the snapshot.

- Deleting a captured source comment transactionally replaces that comment in every affected snapshot with a content-free redaction tombstone. The original body, presentation, and metadata are not retained in the training record.
- Deleting an issue deletes its decision-training examples through the `issue_id` foreign-key cascade.
- Deleting a training example deletes only that example and does not mutate the source issue.

This policy makes training exports self-describing while keeping the decision record usable after a comment deletion without retaining content the author removed.

## Decision queues and triage provenance

The decisions desk stores queue membership, decide-by/snooze state, and retention state in `decision_queues`, `decision_queue_items`, `decision_triage`, and `decision_retention`. These sidecars use the stable attention identity `(source_kind, source_id)` so all attention source kinds can participate without copying source titles, bodies, projects, or other visibility-sensitive data.

`decision_triage_events` is append-only history for queue and triage changes. Current rows and history both carry server-derived user/agent, heartbeat run, API-key, and responsible-user attribution where applicable. Queue reads must resolve and authorize their source rows at read time; a sidecar row is never a visibility grant.

Triage writes serialize on the company and attention-source identity so concurrent partial updates preserve both fields and produce monotonic history versions.

`decision_retention` tracks the last observed source `activityAt`, Keep, reversible archive provenance, and monotonic source/archive versions. `decision_archive_notification_outbox` has a unique key over company, source identity, archive version, and immutable origin agent so repeated sweeps cannot enqueue duplicate notifications; delivery claims are retryable and coalesced per agent.

## Native runner persistence

Native runner state is additive to the existing heartbeat tables. Every existing
`heartbeat_runs` row defaults to `runtime_mode = 'legacy'`; adding these columns
does not select the native runtime or start a runner process. Native execution can
record its resolved runtime profile, provider session, driver, completion
contract, durable event cursor, and finalization phase on the run when a later
rollout explicitly selects it.

`completion_contracts`, `native_run_results`, `native_run_finalizations`,
`work_assessments`, `status_decisions`, and `status_decision_effects` form the
append-oriented evidence and status-decision chain. Unique fingerprints,
versions, ordinals, and idempotency keys make retries deterministic. Composite
foreign keys bind every contract, result, assessment, decision, effect, and
finalization to one company, issue, and run. The database rejects mixed-owner
evidence even when every referenced ID exists. Native source identities on
`heartbeat_run_events` are nullable so legacy events remain readable without
rewriting historical rows. Per-run native source identifiers are unique, while
the existing legacy sequence behavior remains unchanged. The hidden native
coordinator serializes on its bound `heartbeat_runs` row, allocates
`next_event_seq`, and commits a validated PRP event before the transport sends
its cumulative ACK. Byte-equivalent source retries return the existing cursor;
gaps and conflicting replays fail closed. Accepted structured results enter the
finalization ledger, whose retry time and owner lease are checked under a row
lock. None of these writes selects a runtime or changes a legacy run's execution
path.

Durable agent session goals are an additive projection on
`agent_task_sessions`, distinct from the business-goal hierarchy. The row stores
the negotiated goal capability, normalized snapshot and status, desired state,
provider source cursor, monotonic projection revision, and observation time.
`agent_session_goal_actions` is the control outbox: `(session_id, request_id)`
is unique, so retries return the original accepted action. Provider source
ordering fences duplicate and stale updates, and a cleared projection retains
its revision/cursor tombstone so an older provider event cannot resurrect it.

Issue `status_version` advances only when `status` changes. The JavaScript backup
path includes user-defined functions and triggers so a restored database keeps
that invariant. Removing or disabling a future native rollout flag must not
delete these records; persisted experimental runs remain available for recovery
and inspection.

`native_run_finalizations` also stores restart ownership and recovery state.
The controller owner is a server boot id, PID, operating-system process-start
timestamp, and monotonically increasing controller generation. Recovery writes
its correlated request id, current state, and a bounded JSON history. A
successor can take the lease immediately only when coordinated handoff or PID
and process-start evidence proves the prior controller is gone, or when the
lease expires. Recovery generation changes do not increment the independent
provider-attempt counter.

## Telegram private draft identities

`chat_telegram_draft_ids` is a content-free, instance-wide PostgreSQL sequence,
not a company-owned record. Telegram's native Stop callback carries a draft ID
but no actor or Paperclip generation. IDs therefore must not be recycled when
a transaction rolls back or an endpoint/company is deleted and its bot is
connected again. The sequence allocates positive 31-bit IDs without cycling;
exhaustion refuses new draft allocation rather than wrapping or falling back to
random IDs. Never reset it as part of chat cleanup.

The matching `chat_actions` entry remains company/endpoint-scoped and binds the
draft to its exact conversation, publication attempt, runtime, credential and
approved text. Stop can suppress that private draft's final publication; it
cannot cancel a task or model run. Logical backups preserve the sequence, but
restoring an older database may roll back its high-water mark: disaster recovery
must not assume stale provider Stop events are safe to reuse. That restore
boundary is not qualified by the rollback/concurrency regression.

## Attachment upload provenance

`issue_attachments.originating_run_id` records server-derived run attribution at
upload time. It is not writable through attachment or work-product update APIs.
Legacy attachments and uploads without a registered run keep a null value; the
migration deliberately does not infer attribution from mutable work products.
Deleting the originating run clears the reference and fails closed for automatic
chat handoff. An agent's external file selection must match the attachment's
company, task, agent, and originating run. Editing or recreating a work-product
record cannot reassign that authority to a later run.

## Question-response delivery receipts

`issue_question_response_deliveries` is the retry-safe, content-free outbox for
answered `ask_user_questions` interactions. Its unique interaction and correlation
indexes enforce one causal delivery per response. It records source and target
run/turn ids, payload digest, attempt/acknowledgement state, and one of `steered`,
`coalesced`, or `wake_fallback`; answer content remains only in
`issue_thread_interactions.result`. Deleting the interaction cascades its receipt,
while deleting a referenced run clears that run pointer without deleting history.

## Plugin database namespaces

The plugin runtime tracks plugin-owned database namespaces and migrations in `plugin_database_namespaces` and `plugin_migrations`. Hosted deployments that separate runtime and migration connections should set `DATABASE_MIGRATION_URL`; plugin namespace migration work uses the migration connection when present.

## Backups

Paperclip supports automatic and manual logical database backups. These dumps include
non-system database schemas such as `public`, the Drizzle migration journal, and
plugin-owned database schemas. See `doc/DEVELOPING.md` for the current
`paperclipai db:backup` / `pnpm db:backup` commands and backup retention
configuration.

Treat these logical dumps as a portable secondary recovery path, not as the primary
production disaster-recovery system. Production PostgreSQL should use a database-native
physical backup system with continuous WAL archiving, scheduled base backups, tested
point-in-time recovery, and independently enforced retention. For example, a
CloudNativePG deployment can use Barman Cloud to archive WAL and base backups to
S3-compatible object storage. Physical backups plus WAL are more space- and
recovery-efficient than repeatedly storing full logical dumps; the logical dump remains
valuable as a provider-independent escape hatch and for selective inspection.

Database backups do not include non-database instance files such as local-disk
uploads, workspace files, or the local encrypted secrets master key. Back those paths
up separately when you need full instance disaster recovery.

When shared object storage is configured, each completed local dump is also published
as an immutable system-scoped durable object followed by a verified JSON manifest. The
manifest records the exact object key, compressed byte length, SHA-256, and creation
time so another replica can materialize integrity-checked restore input without access
to the producing replica's backup directory. This publication does not replace WAL,
base-backup, or point-in-time-recovery tooling. Backup scheduling and retention ownership
remain part of the later HA fencing work.

## Durable object metadata

Shared object bytes are coordinated through `durable_objects`. Each committed row records
the configured backend identity, immutable object key, owning company or reserved system
scope, content type, byte length, SHA-256, optional backend version/ETag, and verification
state. Durable references may be published only after upload and verification commit.
Integrity failures move a row to `corrupt` with a visible reason. Successful backend deletion leaves a `deleted` tombstone so stale references cannot fall through to unversioned bytes at the same key.

See [Shared object storage](operations/object-storage.md) for provider requirements and the
remaining paths that keep multi-replica deployment blocked.

## Secret storage

Paperclip stores secret metadata and versions in:

- `user_secret_definitions`
- `user_secret_declarations`
- `company_secrets`
- `company_secret_versions`
- `company_secret_bindings`
- `secret_access_events`

Company secrets use `company_secrets.scope = 'company'` and are bound directly
through `company_secret_bindings`. User-specific secrets reuse the same provider
and version storage, but each value is a `company_secrets.scope = 'user'` row
with `owner_user_id` and `user_secret_definition_id` set. Definitions describe
the reusable company-level slot, declarations record where `user_secret_ref`
bindings are required, and the concrete value is selected later for the
responsible user.

Secret-aware env bindings are supported by agents, projects, and routines. Routine env lives in `routines.env`, is captured in `routine_revisions.snapshot`, and routine dispatches store `routine_runs.routine_revision_id` so runtime secret resolution uses the env snapshot that existed when the run was created. Routine secret refs bind with `target_type = 'routine'`, `target_id = routines.id`, and `config_path` values under `env.*`.

For local/default installs, the active provider is `local_encrypted`:

- Secret material is encrypted at rest with a local master key.
- Default key file: `~/.paperclip/instances/default/secrets/master.key` (auto-created if missing).
- CLI config location: `~/.paperclip/instances/default/config.json` under `secrets.localEncrypted.keyFilePath`.
- Backup/restore requires both the database metadata and the local master key file; either artifact alone is insufficient.
- The server best-effort enforces `0600` key file permissions and provider health reports permission warnings.
- User-scoped values use the same local encrypted provider path. Database
  backups preserve definitions, declarations, owner metadata, version metadata,
  and access events, but restored user-scoped values are decryptable only when
  the matching local master key is restored with the database.

Optional overrides:

- `PAPERCLIP_SECRETS_MASTER_KEY` (32-byte key as base64, hex, or raw 32-char string)
- `PAPERCLIP_SECRETS_MASTER_KEY_FILE` (custom key file path)

Strict mode to block new inline sensitive env values:

```sh
PAPERCLIP_SECRETS_STRICT_MODE=true
```

You can set strict mode and provider defaults via:

```sh
pnpm paperclipai configure --section secrets
```

Inline secret migration command:

```sh
npx paperclipai secrets migrate-inline-env --company-id <company-id> --apply

# direct database maintenance fallback
pnpm secrets:migrate-inline-env --apply
```

Hosted AWS provider notes live in [SECRETS-AWS-PROVIDER.md](./SECRETS-AWS-PROVIDER.md).

### Persistent agent conversations

Migration `0274_agent_chat.sql` adds conversation identity/state and session generation/boundary columns to `issues`, plus idempotent client request IDs and processed session-boundary generations to `issue_comments`. The company/agent/user unique index resolves concurrent first writes to one issue. A check constraint preserves the assigned-agent identity and prevents terminal conversation status. Comment request IDs are unique per issue and user. There is no separate chat/message store. Provider sessions continue to use `agent_task_sessions`; `/new` removes only the matching conversation session, and session writers fence stale generations against the issue row.

## Legacy controller ownership

Legacy run claims atomically record `controller_boot_id`, a database-clock
`controller_lease_expires_at`, and `execution_stage` before workspace provisioning.
The lease renews independently of output. A different container must not infer
controller death from its own process map or numeric PIDs. Expiration grants
cleanup authority; it does not prove that remote inference has stopped. Recovery
revokes the previous boot identity with a conditional update. Its own claim also
expires so another sweep can finish cleanup after a restart. Historical rows keep
null ownership fields and follow the previous recovery path.
