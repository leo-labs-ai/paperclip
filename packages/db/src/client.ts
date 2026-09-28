import { createHash } from "node:crypto";
import net from "node:net";
import { drizzle as drizzlePg } from "drizzle-orm/postgres-js";
import { migrate as migratePg } from "drizzle-orm/postgres-js/migrator";
import { readFile, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import postgres from "postgres";
import * as schema from "./schema/index.js";
import { withTransientWriteRetry } from "./transient-write-retry.js";

const MIGRATIONS_FOLDER = fileURLToPath(new URL("./migrations", import.meta.url));
const DRIZZLE_MIGRATIONS_TABLE = "__drizzle_migrations";
const MIGRATIONS_JOURNAL_JSON = fileURLToPath(new URL("./migrations/meta/_journal.json", import.meta.url));

function createUtilitySql(url: string) {
  return postgres(url, { max: 1, onnotice: () => {} });
}

type RegisteredPostgresClient = ReturnType<typeof postgres>;

/**
 * Derives a registry key from a connection URL's host and port only. We must
 * not retain or log the full URL, because it carries credentials.
 */
function hostPortKey(url: string): string {
  const parsed = new URL(url);
  return `${parsed.hostname}:${parsed.port || "5432"}`;
}

/**
 * Same as `hostPortKey`, but returns `null` instead of throwing when the URL
 * does not parse. `postgres(url)` tolerates a value `new URL()` rejects (an
 * empty string falls back to the `PG*` environment variables), so `createDb`
 * must tolerate it too: skip the registry entry and let the driver decide
 * the outcome, instead of throwing an error the driver itself would not.
 */
function hostPortKeyOrNull(url: string): string | null {
  try {
    return hostPortKey(url);
  } catch (error) {
    if (error instanceof TypeError && (error as NodeJS.ErrnoException).code === "ERR_INVALID_URL") return null;
    throw error;
  }
}

// Tracks every client `createDb` hands out, keyed by host and port, so a test
// fixture can end them before it stops the Postgres cluster they point at. A
// `WeakRef` plus `FinalizationRegistry` means a long-lived process (a real
// server) retains nothing extra: an unreferenced client is pruned on its own.
const clientsByHostPort = new Map<string, Set<WeakRef<RegisteredPostgresClient>>>();
const clientFinalizer = new FinalizationRegistry<{ hostPortKey: string; ref: WeakRef<RegisteredPostgresClient> }>(
  ({ hostPortKey, ref }) => {
    const refs = clientsByHostPort.get(hostPortKey);
    if (!refs) return;
    refs.delete(ref);
    if (refs.size === 0) clientsByHostPort.delete(hostPortKey);
  },
);

function registerClient(key: string, client: RegisteredPostgresClient): void {
  const ref = new WeakRef(client);
  let refs = clientsByHostPort.get(key);
  if (!refs) {
    refs = new Set();
    clientsByHostPort.set(key, refs);
  }
  refs.add(ref);
  clientFinalizer.register(client, { hostPortKey: key, ref }, ref);
}

/**
 * Ends every live client `createDb` handed out for the given URL's host and
 * port, then forgets them. Call this before stopping a Postgres cluster: a
 * client that outlives the cluster it points at can crash the process (a
 * reserved connection's deferred write firing after the socket is gone).
 * Swallows individual `end()` errors so one bad client cannot block the rest.
 */
export async function closeRegisteredClients(url: string): Promise<void> {
  const key = hostPortKey(url);
  const refs = clientsByHostPort.get(key);
  if (!refs) return;

  clientsByHostPort.delete(key);
  const clients: RegisteredPostgresClient[] = [];
  for (const ref of refs) {
    clientFinalizer.unregister(ref);
    const client = ref.deref();
    if (client) clients.push(client);
  }

  await Promise.all(clients.map((client) => client.end({ timeout: 1 }).catch(() => {})));
}

function isSafeIdentifier(value: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
}

function quoteIdentifier(value: string): string {
  if (!isSafeIdentifier(value)) throw new Error(`Unsafe SQL identifier: ${value}`);
  return `"${value.replaceAll("\"", "\"\"")}"`;
}

function quoteLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function splitMigrationStatements(content: string): string[] {
  return content
    .split("--> statement-breakpoint")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
}

export type MigrationState =
  | {
      status: "upToDate";
      tableCount: number;
      availableMigrations: string[];
      appliedMigrations: string[];
      journalEntryCount: number;
    }
  | {
      status: "needsMigrations";
      tableCount: number;
      availableMigrations: string[];
      appliedMigrations: string[];
      pendingMigrations: string[];
      journalEntryCount: number;
      reason: "no-migration-journal-empty-db" | "no-migration-journal-non-empty-db" | "pending-migrations";
    };

/**
 * Fork defaults for idle/lifetime pool bounds.
 *
 * postgres.js defaults `idle_timeout` to `null`, so an idle pooled connection is never
 * closed. That interacts badly with CloudNativePG failover: PostgreSQL's *smart* shutdown
 * waits for every client session to disconnect voluntarily, so idle pool connections hold
 * the old primary open for the full `smartShutdownTimeout` (measured: 180.9s, with 9 idle
 * `postgres.js` sessions) before CNPG escalates to a fast shutdown. Closing idle
 * connections bounds that wait by `idle_timeout` instead. Defaults match the measured
 * workload (bursty 30s heartbeat ticks, 0-2 active connections against a pool of 10).
 * `PAPERCLIP_DB_IDLE_TIMEOUT_SEC` / `PAPERCLIP_DB_MAX_LIFETIME_SEC` override; `0` disables.
 */
export const DEFAULT_DB_IDLE_TIMEOUT_SEC = 30;
export const DEFAULT_DB_MAX_LIFETIME_SEC = 30 * 60;

/**
 * Fork defaults for server-side session guards on the main pool.
 *
 * A single runaway maintenance statement (measured: a long unindexed DELETE)
 * can hold pool connections until every other query queues behind it, the
 * health probe times out, readiness drops the pod, and the whole app 503s.
 * Bounding statements and idle-in-transaction sessions at the application
 * pool level ends such runaways instead of letting them starve the pool.
 * Migrations, backups, and other utility clients use their own connections
 * (`createUtilitySql`) and are not affected.
 * `PAPERCLIP_DB_STATEMENT_TIMEOUT_MS` / `PAPERCLIP_DB_IDLE_IN_TX_TIMEOUT_MS`
 * override; `0` disables.
 */
export const DEFAULT_DB_STATEMENT_TIMEOUT_MS = 5 * 60 * 1000;
export const DEFAULT_DB_IDLE_IN_TX_TIMEOUT_MS = 2 * 60 * 1000;

/**
 * Fork defaults for HA-failover connection recovery (HOM-441).
 *
 * postgres.js's `keep_alive` enables TCP keepalive on every pooled socket
 * (driver default: 60s before the first probe). PostgreSQL's own
 * `statement_timeout` above never fires for a connection that goes silently
 * dead — a CloudNativePG primary switchover can leave the old primary's TCP
 * session open on the client side with neither a FIN nor an RST, so the
 * backend never receives the query it is supposedly running. Shortening
 * `keep_alive` gets the first OS-level probe out sooner; how fast the kernel
 * gives up after that depends on `TCP_KEEPINTVL`/`TCP_KEEPCNT`, which Node
 * does not expose per-socket — tuning those is the paired lue-kube (pod
 * `sysctls`) change, not this one.
 *
 * `socket_timeout` (implemented below via a custom postgres.js `socket`
 * factory; there is no native driver option for it) is the backstop that
 * does not depend on OS keepalive tuning at all: `net.Socket#setTimeout`
 * fires once neither a read nor a write has happened for that long,
 * destroying the socket and forcing postgres.js's existing reconnect path
 * (hardened against the null-socket-write race by
 * `patches/postgres@3.4.9.patch`, HOM-421) to replace it on the next query.
 * It must stay comfortably above `statement_timeout` — otherwise it would
 * abort a query PostgreSQL itself would still have let finish — so the
 * default derives from whatever `statement_timeout` resolves to instead of
 * being a fixed number; `DEFAULT_DB_SOCKET_TIMEOUT_MS` only applies when
 * `statement_timeout` is disabled (`0`).
 * `PAPERCLIP_DB_KEEPALIVE_SEC` / `PAPERCLIP_DB_SOCKET_TIMEOUT_MS` override;
 * `0` disables either.
 */
export const DEFAULT_DB_KEEPALIVE_SEC = 15;
export const DB_SOCKET_TIMEOUT_MARGIN_MS = 30 * 1000;
export const DEFAULT_DB_SOCKET_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Fork default for `connect_timeout`. The driver default (30s) is tuned for
 * an occasionally-slow network, not for a repeatedly-unreachable failed-over
 * primary; a shorter budget means a doomed connection attempt fails, and the
 * next attempt (which may hit a now-healthy replica-turned-primary) starts,
 * sooner. `PAPERCLIP_DB_CONNECT_TIMEOUT_SEC` overrides; `0` restores the
 * driver default (no timeout is not expressible here — see postgres.js
 * `timer()`). The upstream `DATABASE_CONNECT_TIMEOUT_SECONDS` knob still
 * wins when set, matching the idle-timeout precedence above.
 */
export const DEFAULT_DB_CONNECT_TIMEOUT_SEC = 10;
/** postgres.js's own `connect_timeout` default, applied when ours is unset or `0`. */
export const DRIVER_CONNECT_TIMEOUT_SEC = 30;

function parseNonNegativeIntEnv(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  const trimmed = value.trim();
  if (trimmed.length === 0) return fallback;
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0) return fallback;
  return Math.floor(parsed);
}

export interface DatabaseClientOptions {
  /**
   * postgres.js `prepare`. Set false when connecting through a
   * transaction-mode pooler (pgbouncer / Neon `-pooler` endpoints /
   * Supabase Supavisor transaction ports) so the client does not rely on
   * session-scoped prepared statements. Defaults to the driver default
   * (enabled), preserving existing behavior on direct connections.
   */
  prepare?: boolean;
  /** postgres.js `max` — connection pool size (driver default: 10). */
  maxConnections?: number;
  /** postgres.js `idle_timeout` in seconds (driver default: disabled). */
  idleTimeoutSeconds?: number;
  /** postgres.js `connect_timeout` in seconds (driver default: 30). */
  connectTimeoutSeconds?: number;
  /** postgres.js `max_lifetime` in seconds; 0 restores the driver's jittered default. */
  maxLifetimeSeconds?: number;
  /** Server-side `statement_timeout` in ms for pool sessions; 0 disables. */
  statementTimeoutMs?: number;
  /** Server-side `idle_in_transaction_session_timeout` in ms for pool sessions; 0 disables. */
  idleInTransactionTimeoutMs?: number;
  /** postgres.js `keep_alive` — TCP keepalive idle delay in seconds; 0 disables. */
  keepAliveSeconds?: number;
  /**
   * Destroys a pooled socket after this many ms without any read or write
   * activity (a client-side inactivity watchdog, not a PostgreSQL server-side
   * setting). 0 disables. See the HOM-441 comment above `DEFAULT_DB_KEEPALIVE_SEC`.
   */
  socketTimeoutMs?: number;
  /**
   * postgres.js `connection.application_name`, shown in
   * `pg_stat_activity.application_name`. Lets an operator tell Paperclip's
   * pool apart from other clients of the same database (driver default:
   * `postgres.js`).
   */
  applicationName?: string;
}

/**
 * Idle pooled connections close after this many seconds unless
 * `DATABASE_IDLE_TIMEOUT_SECONDS` says otherwise. The driver default keeps an
 * idle connection open forever, so a process that stops issuing queries still
 * holds every backend it ever opened. Set `DATABASE_IDLE_TIMEOUT_SECONDS=0`
 * to restore the driver default.
 */
export const DEFAULT_DATABASE_IDLE_TIMEOUT_SECONDS = 60;
/** `application_name` reported to PostgreSQL unless `DATABASE_APPLICATION_NAME` overrides it. */
export const DEFAULT_DATABASE_APPLICATION_NAME = "paperclip";

function envBoolean(env: NodeJS.ProcessEnv, name: string): boolean | undefined {
  const value = env[name]?.trim().toLowerCase();
  if (value === undefined || value === "") return undefined;
  if (value === "true" || value === "1") return true;
  if (value === "false" || value === "0") return false;
  throw new Error(`${name} must be "true" or "false", got: ${env[name]}`);
}

function envPositiveInteger(env: NodeJS.ProcessEnv, name: string): number | undefined {
  const value = env[name]?.trim();
  if (value === undefined || value === "") return undefined;
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error(`${name} must be a positive integer, got: ${env[name]}`);
  }
  return Number.parseInt(value, 10);
}

function envNonNegativeInteger(env: NodeJS.ProcessEnv, name: string): number | undefined {
  const value = env[name]?.trim();
  if (value === undefined || value === "") return undefined;
  if (!/^(?:0|[1-9]\d*)$/.test(value)) {
    throw new Error(`${name} must be a non-negative integer, got: ${env[name]}`);
  }
  return Number.parseInt(value, 10);
}

function envNonEmptyString(env: NodeJS.ProcessEnv, name: string): string | undefined {
  const value = env[name]?.trim();
  if (value === undefined || value === "") return undefined;
  return value;
}

/**
 * Database client tuning from the environment, so hosted deployments can
 * adapt to their connection topology (pooled endpoints, network latency)
 * without editing source. Every variable is optional. This function returns
 * only the values the environment sets; `resolveDatabaseClientOptions` adds
 * Paperclip's own defaults on top, and the driver defaults apply to the rest
 * — self-hosted setups need none of these.
 */
export function databaseClientOptionsFromEnv(env: NodeJS.ProcessEnv = process.env): DatabaseClientOptions {
  const options: DatabaseClientOptions = {};
  const prepare = envBoolean(env, "DATABASE_PREPARED_STATEMENTS");
  if (prepare !== undefined) options.prepare = prepare;
  const maxConnections = envPositiveInteger(env, "DATABASE_POOL_MAX");
  if (maxConnections !== undefined) options.maxConnections = maxConnections;
  // `0` is allowed here: it disables idle reaping (the driver default).
  const idleTimeoutSeconds = envNonNegativeInteger(env, "DATABASE_IDLE_TIMEOUT_SECONDS");
  if (idleTimeoutSeconds !== undefined) options.idleTimeoutSeconds = idleTimeoutSeconds;
  const connectTimeoutSeconds = envPositiveInteger(env, "DATABASE_CONNECT_TIMEOUT_SECONDS");
  if (connectTimeoutSeconds !== undefined) options.connectTimeoutSeconds = connectTimeoutSeconds;
  // Fork defaults: close idle connections and bound connection lifetime so CNPG
  // smart shutdowns are not held open by an otherwise-idle pool (see above).
  if (options.idleTimeoutSeconds === undefined) {
    options.idleTimeoutSeconds = parseNonNegativeIntEnv(env.PAPERCLIP_DB_IDLE_TIMEOUT_SEC, DEFAULT_DB_IDLE_TIMEOUT_SEC);
  }
  options.maxLifetimeSeconds = parseNonNegativeIntEnv(env.PAPERCLIP_DB_MAX_LIFETIME_SEC, DEFAULT_DB_MAX_LIFETIME_SEC);
  options.statementTimeoutMs = parseNonNegativeIntEnv(
    env.PAPERCLIP_DB_STATEMENT_TIMEOUT_MS,
    DEFAULT_DB_STATEMENT_TIMEOUT_MS,
  );
  options.idleInTransactionTimeoutMs = parseNonNegativeIntEnv(
    env.PAPERCLIP_DB_IDLE_IN_TX_TIMEOUT_MS,
    DEFAULT_DB_IDLE_IN_TX_TIMEOUT_MS,
  );
  // Upstream knob (DATABASE_MAX_LIFETIME_SECONDS) wins over the fork default when set.
  const maxLifetimeSeconds = envPositiveInteger(env, "DATABASE_MAX_LIFETIME_SECONDS");
  if (maxLifetimeSeconds !== undefined) options.maxLifetimeSeconds = maxLifetimeSeconds;
  const applicationName = envNonEmptyString(env, "DATABASE_APPLICATION_NAME");
  if (applicationName !== undefined) options.applicationName = applicationName;
  // Fork default: fail a doomed connection attempt sooner than the driver's
  // own 30s default (see DEFAULT_DB_CONNECT_TIMEOUT_SEC above).
  if (options.connectTimeoutSeconds === undefined) {
    options.connectTimeoutSeconds = parseNonNegativeIntEnv(
      env.PAPERCLIP_DB_CONNECT_TIMEOUT_SEC,
      DEFAULT_DB_CONNECT_TIMEOUT_SEC,
    );
  }
  // Fork default: shorten TCP keepalive so the OS notices a dead peer sooner
  // than the driver's own 60s default (see the HOM-441 comment above).
  options.keepAliveSeconds = parseNonNegativeIntEnv(env.PAPERCLIP_DB_KEEPALIVE_SEC, DEFAULT_DB_KEEPALIVE_SEC);
  // Must stay above statement_timeout (see the HOM-441 comment above) or this
  // would abort a query PostgreSQL itself would still have let finish.
  const derivedSocketTimeoutMs =
    options.statementTimeoutMs > 0 ? options.statementTimeoutMs + DB_SOCKET_TIMEOUT_MARGIN_MS : DEFAULT_DB_SOCKET_TIMEOUT_MS;
  options.socketTimeoutMs = parseNonNegativeIntEnv(env.PAPERCLIP_DB_SOCKET_TIMEOUT_MS, derivedSocketTimeoutMs);
  return options;
}

/**
 * Fills in Paperclip's defaults for the options the caller left unset: idle
 * connections are reaped after `DEFAULT_DATABASE_IDLE_TIMEOUT_SECONDS`, and the
 * pool identifies itself as `DEFAULT_DATABASE_APPLICATION_NAME`. Everything
 * else stays at the driver default. An explicit value (including
 * `idleTimeoutSeconds: 0`) always wins over the default.
 */
export function resolveDatabaseClientOptions(options: DatabaseClientOptions): DatabaseClientOptions {
  return {
    ...options,
    idleTimeoutSeconds: options.idleTimeoutSeconds ?? DEFAULT_DATABASE_IDLE_TIMEOUT_SECONDS,
    applicationName: options.applicationName ?? DEFAULT_DATABASE_APPLICATION_NAME,
  };
}

/**
 * A connection target as postgres.js hands it to a custom `socket` factory:
 * `host`/`port` are arrays (multi-host connection strings). The driver's own
 * per-connection host rotation (`hostIndex` in `connect()`) is skipped
 * entirely on the `options.socket` branch, so a factory cannot reproduce it;
 * the factory is therefore only installed for a single-host connection
 * string (see `postgresJsOptions`) and only ever dials `[0]`.
 */
interface SocketFactoryTarget {
  host: string[];
  port: number[];
}

/**
 * Builds a postgres.js `socket` factory (see `createSocket` in
 * postgres@3.4.9's `src/connection.js`) that dials the connection itself —
 * required by the custom-socket contract, which skips the driver's own
 * `socket.connect(port, host)` call entirely and assumes the factory
 * returns an already-connecting-or-connected socket — and destroys the
 * resulting raw TCP socket after `timeoutMs` without a single byte *read*
 * from the peer. Read silence, not the read-or-write silence
 * `net.Socket#setTimeout` measures: postgres.js pipelines, so every
 * subsequent query written onto an already-dead socket would keep pushing a
 * read-or-write window out and the backstop would never reclaim the socket.
 * Destroying it raises the same `error`/`close` sequence as any other
 * network-dropped connection, which postgres.js already turns into a
 * rejected, retryable `CONNECTION_CLOSED`-coded error and a reconnect on the
 * next query (see the HOM-441 comment above `DEFAULT_DB_KEEPALIVE_SEC`).
 *
 * `connectTimeoutMs` bounds the TCP dial itself. The driver's own
 * `connect_timeout` cannot: `connectTimer.start()` runs only *after*
 * `await options.socket(options)` resolves, so with a custom factory
 * installed the dial would otherwise be bounded only by the OS SYN retry
 * budget (~127s on Linux) — the opposite of a fast reconnect onto a
 * failed-over primary. `0` leaves the dial to the OS.
 */
export function socketWithInactivityTimeout(
  timeoutMs: number,
  connectTimeoutMs = 0,
): (target: SocketFactoryTarget) => Promise<net.Socket> {
  return (target) =>
    new Promise((resolve, reject) => {
      let settled = false;
      const socket = net.connect({ host: target.host[0], port: target.port[0] }, () =>
        settle(null),
      ) as net.Socket & { host?: string; port?: number };
      // postgres.js assigns these itself on its own dial branch and reads them
      // back in `secure()` as `servername: net.isIP(socket.host) ? undefined :
      // socket.host`. A custom factory returns before that branch runs, so
      // without this the TLS handshake carries no SNI — and an endpoint that
      // routes on SNI (Neon, Supavisor) rejects the connection outright.
      socket.host = target.host[0];
      socket.port = target.port[0];

      // Every path out of the dial has to settle this promise. postgres.js
      // awaits it inside `connect()` and only attaches its own `close`/
      // `error` listeners afterwards, so a promise left pending wedges that
      // pool slot permanently — repeated occurrences exhaust `max` and force
      // the pod restart this change exists to eliminate.
      function settle(err: Error | null): void {
        if (settled) return;
        settled = true;
        if (connectTimer) clearTimeout(connectTimer);
        socket.removeListener("error", onPreConnectError);
        socket.removeListener("close", onPreConnectClose);
        if (err) {
          clearInterval(readIdleTimer);
          socket.destroy();
          reject(err);
          return;
        }
        lastBytesRead = socket.bytesRead;
        lastReadAt = Date.now();
        resolve(socket);
      }

      const connectionClosed = (message: string) =>
        Object.assign(new Error(message), { code: "CONNECTION_CLOSED" });
      const onPreConnectError = (err: Error) => settle(err);
      const onPreConnectClose = () => settle(connectionClosed("Database socket closed before it finished connecting"));
      socket.once("error", onPreConnectError);
      socket.once("close", onPreConnectClose);

      const connectTimer =
        connectTimeoutMs > 0
          ? setTimeout(() => {
              settle(Object.assign(new Error(`Database socket did not connect within ${connectTimeoutMs}ms`), {
                code: "CONNECT_TIMEOUT",
              }));
            }, connectTimeoutMs).unref()
          : null;

      // Armed from the dial onwards and reset only when the socket's read
      // counter has moved, so a stream of outbound writes on a socket whose
      // peer has gone silent cannot postpone it.
      //
      // Sampled on a poll a tenth of the window long rather than on one timer
      // per window: a single timer can only observe the counter when it fires,
      // so the last sample before real silence starts always looks like
      // activity and the socket survives a further full window — twice the
      // documented reclaim time.
      //
      // `bytesRead`, not a `data` listener: once postgres.js's `secure()`
      // hands this socket to `tls.connect({ socket })`, the TLS wrapper takes
      // the reads over and the raw socket emits no further `data` events, so
      // a listener-driven window would expire on a perfectly healthy TLS
      // connection — every `sslmode=require` deployment — and destroy it
      // mid-query. The kernel-level byte counter keeps advancing under the
      // TLS wrap, which makes the same "nothing read from the peer" question
      // answerable on plaintext and TLS alike.
      let lastBytesRead = 0;
      let lastReadAt = Date.now();
      const readIdleTimer = setInterval(onReadIdle, Math.max(1, Math.ceil(timeoutMs / 10))).unref();
      socket.once("close", () => clearInterval(readIdleTimer));

      function onReadIdle(): void {
        if (socket.bytesRead !== lastBytesRead) {
          lastBytesRead = socket.bytesRead;
          lastReadAt = Date.now();
          return;
        }
        if (Date.now() - lastReadAt < timeoutMs) return;
        clearInterval(readIdleTimer);
        if (!settled) {
          settle(connectionClosed(`Database socket read nothing within ${timeoutMs}ms`));
          return;
        }
        // Destroy with no error argument. postgres.js's own `closed(hadError)`
        // handler only rejects the pending query itself when `hadError` is
        // false (`!hadError && (query || sent.length) && error(...)`), and it
        // nulls its socket reference in that same synchronous step. Passing an
        // Error to destroy() instead makes Node also emit a separate `error`
        // event first, which rejects the query slightly *before* `closed()`
        // runs — a query dispatched into that narrow window can still reach
        // `socket.write()` on the already-destroyed-but-not-yet-nulled socket
        // and fail with `ERR_SOCKET_CLOSED`, a race distinct from the
        // null-socket write that HOM-421 already guards against.
        socket.destroy();
      }
    });
}

/**
 * Counts the hosts in a connection URL the way postgres.js's own `parseUrl`
 * does: the authority between `://` (or the credentials' `@`) and the first
 * `/` or `?`, comma-separated. A URL the driver tolerates but `new URL()`
 * rejects (an empty string falls back to the `PG*` variables) counts as one.
 */
export function connectionUrlHostCount(url: string): number {
  const schemeless = url.slice(url.indexOf("://") + 3).split(/[?/]/)[0] ?? "";
  const authority = schemeless.slice(schemeless.indexOf("@") + 1);
  if (authority.length === 0) return 1;
  return authority.split(",").length;
}

/**
 * `url` decides whether the inactivity-timeout socket factory is installed:
 * a multi-host connection string keeps the driver's own dial (and with it
 * its per-connection host rotation, which the custom-socket branch of
 * `connect()` skips), so it keeps the OS-keepalive layer as its only
 * failover backstop. Omitted means single-host.
 */
export function postgresJsOptions(options: DatabaseClientOptions, url?: string): Record<string, unknown> {
  const driverOptions: Record<string, unknown> = {};
  if (options.prepare !== undefined) driverOptions.prepare = options.prepare;
  if (options.maxConnections !== undefined) driverOptions.max = options.maxConnections;
  if (options.idleTimeoutSeconds !== undefined) driverOptions.idle_timeout = options.idleTimeoutSeconds;
  // `connect_timeout: 0` would disable the driver's connect timer outright
  // (`timer()` treats a falsy value as "no timer"), so `0` means "restore the
  // driver's own 30s default" by leaving the option unset.
  if (options.connectTimeoutSeconds !== undefined && options.connectTimeoutSeconds > 0) {
    driverOptions.connect_timeout = options.connectTimeoutSeconds;
  }
  if (options.maxLifetimeSeconds !== undefined && options.maxLifetimeSeconds > 0) {
    driverOptions.max_lifetime = options.maxLifetimeSeconds;
  }
  if (options.keepAliveSeconds !== undefined) driverOptions.keep_alive = options.keepAliveSeconds;
  const singleHost = url === undefined || connectionUrlHostCount(url) === 1;
  if (options.socketTimeoutMs !== undefined && options.socketTimeoutMs > 0 && singleHost) {
    const connectTimeoutMs =
      options.connectTimeoutSeconds !== undefined && options.connectTimeoutSeconds > 0
        ? options.connectTimeoutSeconds * 1000
        : DRIVER_CONNECT_TIMEOUT_SEC * 1000;
    driverOptions.socket = socketWithInactivityTimeout(options.socketTimeoutMs, connectTimeoutMs);
  }
  const connection: Record<string, unknown> = {};
  if (options.statementTimeoutMs !== undefined && options.statementTimeoutMs > 0) {
    connection.statement_timeout = options.statementTimeoutMs;
  }
  if (options.idleInTransactionTimeoutMs !== undefined && options.idleInTransactionTimeoutMs > 0) {
    connection.idle_in_transaction_session_timeout = options.idleInTransactionTimeoutMs;
  }
  if (options.applicationName !== undefined) connection.application_name = options.applicationName;
  if (Object.keys(connection).length > 0) driverOptions.connection = connection;
  return driverOptions;
}

export function createDb(url: string, options?: DatabaseClientOptions) {
  const resolved = resolveDatabaseClientOptions(options ?? databaseClientOptionsFromEnv());
  const sql = postgres(url, postgresJsOptions(resolved, url));
  const key = hostPortKeyOrNull(url);
  if (key) registerClient(key, sql);
  // The registry keeps the real client (teardown must end the actual pool);
  // drizzle gets the retrying face so a pooler-recycled socket replays the
  // query instead of failing the request that happened to draw it.
  return drizzlePg(withTransientWriteRetry(sql), { schema });
}

export async function getPostgresDataDirectory(url: string): Promise<string | null> {
  const sql = createUtilitySql(url);
  try {
    const rows = await sql<{ data_directory: string | null }[]>`
      SELECT current_setting('data_directory', true) AS data_directory
    `;
    const actual = rows[0]?.data_directory;
    return typeof actual === "string" && actual.length > 0 ? actual : null;
  } catch {
    return null;
  } finally {
    await sql.end();
  }
}

async function listMigrationFiles(): Promise<string[]> {
  const entries = await readdir(MIGRATIONS_FOLDER, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sql"))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b));
}

type MigrationJournalFile = {
  entries?: Array<{ idx?: number; tag?: string; when?: number }>;
};

type JournalMigrationEntry = {
  fileName: string;
  folderMillis: number;
  order: number;
};

async function listJournalMigrationEntries(): Promise<JournalMigrationEntry[]> {
  try {
    const raw = await readFile(MIGRATIONS_JOURNAL_JSON, "utf8");
    const parsed = JSON.parse(raw) as MigrationJournalFile;
    if (!Array.isArray(parsed.entries)) return [];
    return parsed.entries
      .map((entry, entryIndex) => {
        if (typeof entry?.tag !== "string") return null;
        if (typeof entry?.when !== "number" || !Number.isFinite(entry.when)) return null;
        const order = Number.isInteger(entry.idx) ? Number(entry.idx) : entryIndex;
        return { fileName: `${entry.tag}.sql`, folderMillis: entry.when, order };
      })
      .filter((entry): entry is JournalMigrationEntry => entry !== null);
  } catch {
    return [];
  }
}

async function listJournalMigrationFiles(): Promise<string[]> {
  const entries = await listJournalMigrationEntries();
  return entries.map((entry) => entry.fileName);
}

async function readMigrationFileContent(migrationFile: string): Promise<string> {
  return readFile(new URL(`./migrations/${migrationFile}`, import.meta.url), "utf8");
}

async function orderMigrationsByJournal(migrationFiles: string[]): Promise<string[]> {
  const journalEntries = await listJournalMigrationEntries();
  const orderByFileName = new Map(journalEntries.map((entry) => [entry.fileName, entry.order]));
  return [...migrationFiles].sort((left, right) => {
    const leftOrder = orderByFileName.get(left);
    const rightOrder = orderByFileName.get(right);
    if (leftOrder === undefined && rightOrder === undefined) return left.localeCompare(right);
    if (leftOrder === undefined) return 1;
    if (rightOrder === undefined) return -1;
    if (leftOrder === rightOrder) return left.localeCompare(right);
    return leftOrder - rightOrder;
  });
}

type SqlExecutor = Pick<ReturnType<typeof postgres>, "unsafe">;

async function runInTransaction(sql: SqlExecutor, action: () => Promise<void>): Promise<void> {
  await sql.unsafe("BEGIN");
  try {
    await action();
    await sql.unsafe("COMMIT");
  } catch (error) {
    try {
      await sql.unsafe("ROLLBACK");
    } catch {
      // Ignore rollback failures and surface the original error.
    }
    throw error;
  }
}

async function latestMigrationCreatedAt(
  sql: SqlExecutor,
  qualifiedTable: string,
): Promise<number | null> {
  const rows = await sql.unsafe<{ created_at: string | number | null }[]>(
    `SELECT created_at FROM ${qualifiedTable} ORDER BY created_at DESC NULLS LAST LIMIT 1`,
  );
  const value = Number(rows[0]?.created_at ?? Number.NaN);
  return Number.isFinite(value) ? value : null;
}

function normalizeFolderMillis(value: number | null | undefined): number {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.trunc(value);
  }
  return Date.now();
}

async function ensureMigrationJournalTable(
  sql: ReturnType<typeof postgres>,
): Promise<{ migrationTableSchema: string; columnNames: Set<string> }> {
  let migrationTableSchema = await discoverMigrationTableSchema(sql);
  if (!migrationTableSchema) {
    const drizzleSchema = quoteIdentifier("drizzle");
    const migrationTable = quoteIdentifier(DRIZZLE_MIGRATIONS_TABLE);
    await sql.unsafe(`CREATE SCHEMA IF NOT EXISTS ${drizzleSchema}`);
    await sql.unsafe(
      `CREATE TABLE IF NOT EXISTS ${drizzleSchema}.${migrationTable} (id SERIAL PRIMARY KEY, hash text NOT NULL, created_at bigint)`,
    );
    migrationTableSchema = (await discoverMigrationTableSchema(sql)) ?? "drizzle";
  }

  const columnNames = await getMigrationTableColumnNames(sql, migrationTableSchema);
  return { migrationTableSchema, columnNames };
}

async function migrationHistoryEntryExists(
  sql: SqlExecutor,
  qualifiedTable: string,
  columnNames: Set<string>,
  migrationFile: string,
  hash: string,
): Promise<boolean> {
  const predicates: string[] = [];
  if (columnNames.has("hash")) predicates.push(`hash = ${quoteLiteral(hash)}`);
  if (columnNames.has("name")) predicates.push(`name = ${quoteLiteral(migrationFile)}`);
  if (predicates.length === 0) return false;

  const rows = await sql.unsafe<{ one: number }[]>(
    `SELECT 1 AS one FROM ${qualifiedTable} WHERE ${predicates.join(" OR ")} LIMIT 1`,
  );
  return rows.length > 0;
}

async function recordMigrationHistoryEntry(
  sql: SqlExecutor,
  qualifiedTable: string,
  columnNames: Set<string>,
  migrationFile: string,
  hash: string,
  folderMillis: number,
): Promise<void> {
  const insertColumns: string[] = [];
  const insertValues: string[] = [];

  if (columnNames.has("hash")) {
    insertColumns.push(quoteIdentifier("hash"));
    insertValues.push(quoteLiteral(hash));
  }
  if (columnNames.has("name")) {
    insertColumns.push(quoteIdentifier("name"));
    insertValues.push(quoteLiteral(migrationFile));
  }
  if (columnNames.has("created_at")) {
    const latestCreatedAt = await latestMigrationCreatedAt(sql, qualifiedTable);
    const createdAt = latestCreatedAt === null
      ? normalizeFolderMillis(folderMillis)
      : Math.max(latestCreatedAt + 1, normalizeFolderMillis(folderMillis));
    insertColumns.push(quoteIdentifier("created_at"));
    insertValues.push(quoteLiteral(String(createdAt)));
  }

  if (insertColumns.length === 0) return;

  await sql.unsafe(
    `INSERT INTO ${qualifiedTable} (${insertColumns.join(", ")}) VALUES (${insertValues.join(", ")})`,
  );
}

async function applyPendingMigrationsManually(
  url: string,
  pendingMigrations: string[],
): Promise<void> {
  if (pendingMigrations.length === 0) return;

  const orderedPendingMigrations = await orderMigrationsByJournal(pendingMigrations);
  const journalEntries = await listJournalMigrationEntries();
  const folderMillisByFileName = new Map(
    journalEntries.map((entry) => [entry.fileName, normalizeFolderMillis(entry.folderMillis)]),
  );

  const sql = createUtilitySql(url);
  try {
    const { migrationTableSchema, columnNames } = await ensureMigrationJournalTable(sql);
    const qualifiedTable = `${quoteIdentifier(migrationTableSchema)}.${quoteIdentifier(DRIZZLE_MIGRATIONS_TABLE)}`;

    for (const migrationFile of orderedPendingMigrations) {
      const migrationContent = await readMigrationFileContent(migrationFile);
      const hash = createHash("sha256").update(migrationContent).digest("hex");
      const existingEntry = await migrationHistoryEntryExists(
        sql,
        qualifiedTable,
        columnNames,
        migrationFile,
        hash,
      );
      if (existingEntry) continue;

      await runInTransaction(sql, async () => {
        for (const statement of splitMigrationStatements(migrationContent)) {
          await sql.unsafe(statement);
        }

        await recordMigrationHistoryEntry(
          sql,
          qualifiedTable,
          columnNames,
          migrationFile,
          hash,
          folderMillisByFileName.get(migrationFile) ?? Date.now(),
        );
      });
    }
  } finally {
    await sql.end();
  }
}

async function mapHashesToMigrationFiles(migrationFiles: string[]): Promise<Map<string, string>> {
  const mapped = new Map<string, string>();

  await Promise.all(
    migrationFiles.map(async (migrationFile) => {
      const content = await readMigrationFileContent(migrationFile);
      const hash = createHash("sha256").update(content).digest("hex");
      mapped.set(hash, migrationFile);
    }),
  );

  return mapped;
}

async function getMigrationTableColumnNames(
  sql: ReturnType<typeof postgres>,
  migrationTableSchema: string,
): Promise<Set<string>> {
  const columns = await sql.unsafe<{ column_name: string }[]>(
    `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = ${quoteLiteral(migrationTableSchema)}
        AND table_name = ${quoteLiteral(DRIZZLE_MIGRATIONS_TABLE)}
    `,
  );
  return new Set(columns.map((column) => column.column_name));
}

async function tableExists(
  sql: ReturnType<typeof postgres>,
  tableName: string,
): Promise<boolean> {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = ${tableName}
    ) AS exists
  `;
  return rows[0]?.exists ?? false;
}

async function columnExists(
  sql: ReturnType<typeof postgres>,
  tableName: string,
  columnName: string,
): Promise<boolean> {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = ${tableName}
        AND column_name = ${columnName}
    ) AS exists
  `;
  return rows[0]?.exists ?? false;
}

async function columnHasDataType(
  sql: ReturnType<typeof postgres>,
  tableName: string,
  columnName: string,
  dataType: string,
): Promise<boolean> {
  const rows = await sql<{ dataType: string; udtName: string }[]>`
    SELECT data_type AS "dataType", udt_name AS "udtName"
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = ${tableName}
      AND column_name = ${columnName}
  `;
  const expected = dataType.toLowerCase();
  return rows.some((row) => (
    row.dataType.toLowerCase() === expected || row.udtName.toLowerCase() === expected
  ));
}

async function indexExists(
  sql: ReturnType<typeof postgres>,
  indexName: string,
): Promise<boolean> {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public'
        AND c.relkind = 'i'
        AND c.relname = ${indexName}
    ) AS exists
  `;
  return rows[0]?.exists ?? false;
}

async function constraintExists(
  sql: ReturnType<typeof postgres>,
  constraintName: string,
): Promise<boolean> {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1
      FROM pg_constraint c
      JOIN pg_namespace n ON n.oid = c.connamespace
      WHERE n.nspname = 'public'
        AND c.conname = ${constraintName}
    ) AS exists
  `;
  return rows[0]?.exists ?? false;
}

async function functionExists(
  sql: ReturnType<typeof postgres>,
  functionName: string,
): Promise<boolean> {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = 'public'
        AND p.proname = ${functionName}
    ) AS exists
  `;
  return rows[0]?.exists ?? false;
}

async function triggerExists(
  sql: ReturnType<typeof postgres>,
  triggerName: string,
): Promise<boolean> {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public'
        AND t.tgname = ${triggerName}
        AND NOT t.tgisinternal
    ) AS exists
  `;
  return rows[0]?.exists ?? false;
}

async function heartbeatEventSequencesAreUnique(
  sql: ReturnType<typeof postgres>,
): Promise<boolean> {
  const rows = await sql<{ unique: boolean }[]>`
    SELECT NOT EXISTS (
      SELECT 1
      FROM heartbeat_run_events
      GROUP BY run_id, seq
      HAVING count(*) > 1
    ) AS unique
  `;
  return rows[0]?.unique ?? false;
}

async function heartbeatNextEventSequencesAreCurrent(
  sql: ReturnType<typeof postgres>,
): Promise<boolean> {
  const rows = await sql<{ current: boolean }[]>`
    SELECT NOT EXISTS (
      SELECT 1
      FROM heartbeat_runs run
      WHERE run.next_event_seq IS DISTINCT FROM COALESCE((
        SELECT max(event.seq) + 1
        FROM heartbeat_run_events event
        WHERE event.run_id = run.id
      ), 1)
    ) AS current
  `;
  return rows[0]?.current ?? false;
}

async function migrationStatementAlreadyApplied(
  sql: ReturnType<typeof postgres>,
  statement: string,
): Promise<boolean> {
  const normalized = statement
    .replace(/^\s*--.*$/gm, "")
    .replace(/\s+/g, " ")
    .trim();

  const createTableMatch = normalized.match(/^CREATE TABLE(?: IF NOT EXISTS)? "([^"]+)"/i);
  if (createTableMatch) {
    return tableExists(sql, createTableMatch[1]);
  }

  const addColumnMatch = normalized.match(
    /^ALTER TABLE "([^"]+)" ADD COLUMN(?: IF NOT EXISTS)? "([^"]+)"/i,
  );
  if (addColumnMatch) {
    return columnExists(sql, addColumnMatch[1], addColumnMatch[2]);
  }

  const alterColumnTypeMatch = normalized.match(
    /^ALTER TABLE "([^"]+)" ALTER COLUMN "([^"]+)" SET DATA TYPE ([A-Za-z0-9_]+)/i,
  );
  if (alterColumnTypeMatch) {
    return columnHasDataType(
      sql,
      alterColumnTypeMatch[1],
      alterColumnTypeMatch[2],
      alterColumnTypeMatch[3],
    );
  }

  const createIndexMatch = normalized.match(/^CREATE (?:UNIQUE )?INDEX(?: IF NOT EXISTS)? "([^"]+)"/i);
  if (createIndexMatch) {
    return indexExists(sql, createIndexMatch[1]);
  }

  const addConstraintMatch = normalized.match(/^ALTER TABLE "([^"]+)" ADD CONSTRAINT "([^"]+)"/i);
  if (addConstraintMatch) {
    return constraintExists(sql, addConstraintMatch[2]);
  }

  const createFunctionMatch = normalized.match(
    /^CREATE OR REPLACE FUNCTION "?([A-Za-z_][A-Za-z0-9_]*)"?\s*\(/i,
  );
  if (createFunctionMatch) {
    return functionExists(sql, createFunctionMatch[1]);
  }

  const createTriggerMatch = normalized.match(
    /^CREATE TRIGGER "?([A-Za-z_][A-Za-z0-9_]*)"?/i,
  );
  if (createTriggerMatch) {
    return triggerExists(sql, createTriggerMatch[1]);
  }

  // These native-runner repairs have persistent postconditions. Verify them
  // instead of replaying them when a restored database is missing only the
  // migration-history row.
  if (
    normalized.startsWith("WITH ranked AS (")
    && normalized.includes('UPDATE "heartbeat_run_events" AS event')
  ) {
    return heartbeatEventSequencesAreUnique(sql);
  }
  if (
    normalized.startsWith('UPDATE "heartbeat_runs" AS run')
    && normalized.includes('SET "next_event_seq" = COALESCE')
  ) {
    return heartbeatNextEventSequencesAreCurrent(sql);
  }

  // If we cannot reason about a statement safely, require manual migration.
  return false;
}

async function migrationContentAlreadyApplied(
  sql: ReturnType<typeof postgres>,
  migrationContent: string,
): Promise<boolean> {
  const statements = splitMigrationStatements(migrationContent);
  if (statements.length === 0) return false;

  for (const statement of statements) {
    const applied = await migrationStatementAlreadyApplied(sql, statement);
    if (!applied) return false;
  }

  return true;
}

async function loadAppliedMigrations(
  sql: ReturnType<typeof postgres>,
  migrationTableSchema: string,
  availableMigrations: string[],
): Promise<string[]> {
  const quotedSchema = quoteIdentifier(migrationTableSchema);
  const qualifiedTable = `${quotedSchema}.${quoteIdentifier(DRIZZLE_MIGRATIONS_TABLE)}`;
  const columnNames = await getMigrationTableColumnNames(sql, migrationTableSchema);

  if (columnNames.has("name")) {
    const rows = await sql.unsafe<{ name: string }[]>(`SELECT name FROM ${qualifiedTable} ORDER BY id`);
    return rows.map((row) => row.name).filter((name): name is string => Boolean(name));
  }

  if (columnNames.has("hash")) {
    const rows = await sql.unsafe<{ hash: string }[]>(`SELECT hash FROM ${qualifiedTable} ORDER BY id`);
    const hashesToMigrationFiles = await mapHashesToMigrationFiles(availableMigrations);
    const appliedFromHashes = rows
      .map((row) => hashesToMigrationFiles.get(row.hash))
      .filter((name): name is string => Boolean(name));

    if (appliedFromHashes.length > 0) {
      // Best-effort: when all hashes resolve, this is authoritative.
      if (appliedFromHashes.length === rows.length) return appliedFromHashes;

      // Partial hash resolution can happen when files have changed; return what we can trust.
      return appliedFromHashes;
    }

    // Fallback only when hashes are unavailable/unresolved.
    if (columnNames.has("created_at")) {
      const journalEntries = await listJournalMigrationEntries();
      if (journalEntries.length > 0) {
        const lastDbRows = await sql.unsafe<{ created_at: string | number | null }[]>(
          `SELECT created_at FROM ${qualifiedTable} ORDER BY created_at DESC LIMIT 1`,
        );
        const lastCreatedAt = Number(lastDbRows[0]?.created_at ?? -1);
        if (Number.isFinite(lastCreatedAt) && lastCreatedAt >= 0) {
          return journalEntries
            .filter((entry) => availableMigrations.includes(entry.fileName))
            .filter((entry) => entry.folderMillis <= lastCreatedAt)
            .map((entry) => entry.fileName)
            .slice(0, rows.length);
        }
      }
    }
  }

  const rows = await sql.unsafe<{ id: number }[]>(`SELECT id FROM ${qualifiedTable} ORDER BY id`);
  const journalMigrationFiles = await listJournalMigrationFiles();
  const appliedFromIds = rows
    .map((row) => journalMigrationFiles[row.id - 1])
    .filter((name): name is string => Boolean(name));
  if (appliedFromIds.length > 0) return appliedFromIds;

  return availableMigrations.slice(0, Math.max(0, rows.length));
}

export type MigrationHistoryReconcileResult = {
  repairedMigrations: string[];
  remainingMigrations: string[];
};

export async function reconcilePendingMigrationHistory(
  url: string,
): Promise<MigrationHistoryReconcileResult> {
  const state = await inspectMigrations(url);
  if (state.status !== "needsMigrations" || state.reason !== "pending-migrations") {
    return { repairedMigrations: [], remainingMigrations: [] };
  }

  const sql = createUtilitySql(url);
  const repairedMigrations: string[] = [];

  try {
    const journalEntries = await listJournalMigrationEntries();
    const folderMillisByFile = new Map(journalEntries.map((entry) => [entry.fileName, entry.folderMillis]));
    const migrationTableSchema = await discoverMigrationTableSchema(sql);
    if (!migrationTableSchema) {
      return { repairedMigrations, remainingMigrations: state.pendingMigrations };
    }

    const columnNames = await getMigrationTableColumnNames(sql, migrationTableSchema);
    const qualifiedTable = `${quoteIdentifier(migrationTableSchema)}.${quoteIdentifier(DRIZZLE_MIGRATIONS_TABLE)}`;

    for (const migrationFile of state.pendingMigrations) {
      const migrationContent = await readMigrationFileContent(migrationFile);
      const alreadyApplied = await migrationContentAlreadyApplied(sql, migrationContent);
      if (!alreadyApplied) break;

      const hash = createHash("sha256").update(migrationContent).digest("hex");
      const folderMillis = folderMillisByFile.get(migrationFile) ?? Date.now();
      const existingByHash = columnNames.has("hash")
        ? await sql.unsafe<{ created_at: string | number | null }[]>(
            `SELECT created_at FROM ${qualifiedTable} WHERE hash = ${quoteLiteral(hash)} ORDER BY created_at DESC LIMIT 1`,
          )
        : [];
      const existingByName = columnNames.has("name")
        ? await sql.unsafe<{ created_at: string | number | null }[]>(
            `SELECT created_at FROM ${qualifiedTable} WHERE name = ${quoteLiteral(migrationFile)} ORDER BY created_at DESC LIMIT 1`,
          )
        : [];
      if (existingByHash.length > 0 || existingByName.length > 0) {
        if (columnNames.has("created_at")) {
          const existingHashCreatedAt = Number(existingByHash[0]?.created_at ?? -1);
          if (existingByHash.length > 0 && Number.isFinite(existingHashCreatedAt) && existingHashCreatedAt < folderMillis) {
            await sql.unsafe(
              `UPDATE ${qualifiedTable} SET created_at = ${quoteLiteral(String(folderMillis))} WHERE hash = ${quoteLiteral(hash)} AND created_at < ${quoteLiteral(String(folderMillis))}`,
            );
          }

          const existingNameCreatedAt = Number(existingByName[0]?.created_at ?? -1);
          if (existingByName.length > 0 && Number.isFinite(existingNameCreatedAt) && existingNameCreatedAt < folderMillis) {
            await sql.unsafe(
              `UPDATE ${qualifiedTable} SET created_at = ${quoteLiteral(String(folderMillis))} WHERE name = ${quoteLiteral(migrationFile)} AND created_at < ${quoteLiteral(String(folderMillis))}`,
            );
          }
        }

        repairedMigrations.push(migrationFile);
        continue;
      }

      const insertColumns: string[] = [];
      const insertValues: string[] = [];

      if (columnNames.has("hash")) {
        insertColumns.push(quoteIdentifier("hash"));
        insertValues.push(quoteLiteral(hash));
      }
      if (columnNames.has("name")) {
        insertColumns.push(quoteIdentifier("name"));
        insertValues.push(quoteLiteral(migrationFile));
      }
      if (columnNames.has("created_at")) {
        insertColumns.push(quoteIdentifier("created_at"));
        insertValues.push(quoteLiteral(String(folderMillis)));
      }

      if (insertColumns.length === 0) break;

      await sql.unsafe(
        `INSERT INTO ${qualifiedTable} (${insertColumns.join(", ")}) VALUES (${insertValues.join(", ")})`,
      );
      repairedMigrations.push(migrationFile);
    }
  } finally {
    await sql.end();
  }

  const refreshed = await inspectMigrations(url);
  return {
    repairedMigrations,
    remainingMigrations:
      refreshed.status === "needsMigrations" ? refreshed.pendingMigrations : [],
  };
}

async function discoverMigrationTableSchema(sql: ReturnType<typeof postgres>): Promise<string | null> {
  const rows = await sql<{ schemaName: string }[]>`
    SELECT n.nspname AS "schemaName"
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = ${DRIZZLE_MIGRATIONS_TABLE} AND c.relkind = 'r'
  `;

  if (rows.length === 0) return null;

  const drizzleSchema = rows.find(({ schemaName }) => schemaName === "drizzle");
  if (drizzleSchema) return drizzleSchema.schemaName;

  const publicSchema = rows.find(({ schemaName }) => schemaName === "public");
  if (publicSchema) return publicSchema.schemaName;

  return rows[0]?.schemaName ?? null;
}

export async function inspectMigrations(url: string): Promise<MigrationState> {
  const sql = createUtilitySql(url);

  try {
    const availableMigrations = await listMigrationFiles();
    const tableCountResult = await sql<{ count: number }[]>`
      select count(*)::int as count
      from information_schema.tables
      where table_schema = 'public'
        and table_type = 'BASE TABLE'
    `;
    const tableCount = tableCountResult[0]?.count ?? 0;

    const migrationTableSchema = await discoverMigrationTableSchema(sql);
    if (!migrationTableSchema) {
      if (tableCount > 0) {
        return {
          status: "needsMigrations",
          tableCount,
          availableMigrations,
          appliedMigrations: [],
          pendingMigrations: availableMigrations,
          journalEntryCount: 0,
          reason: "no-migration-journal-non-empty-db",
        };
      }

      return {
        status: "needsMigrations",
        tableCount,
        availableMigrations,
        appliedMigrations: [],
        pendingMigrations: availableMigrations,
        journalEntryCount: 0,
        reason: "no-migration-journal-empty-db",
      };
    }

    const qualifiedMigrationTable = `${quoteIdentifier(migrationTableSchema)}.${quoteIdentifier(DRIZZLE_MIGRATIONS_TABLE)}`;
    const journalCountRows = await sql.unsafe<{ count: number }[]>(
      `SELECT count(*)::int AS count FROM ${qualifiedMigrationTable}`,
    );
    const journalEntryCount = journalCountRows[0]?.count ?? 0;
    const appliedMigrations = await loadAppliedMigrations(sql, migrationTableSchema, availableMigrations);
    const pendingMigrations = availableMigrations.filter((name) => !appliedMigrations.includes(name));
    if (pendingMigrations.length === 0) {
      return {
        status: "upToDate",
        tableCount,
        availableMigrations,
        appliedMigrations,
        journalEntryCount,
      };
    }

    return {
      status: "needsMigrations",
      tableCount,
      availableMigrations,
      appliedMigrations,
      pendingMigrations,
      journalEntryCount,
      reason: "pending-migrations",
    };
  } finally {
    await sql.end();
  }
}

export async function applyPendingMigrations(url: string): Promise<void> {
  const initialState = await inspectMigrations(url);
  if (initialState.status === "upToDate") return;

  if (initialState.reason === "no-migration-journal-empty-db") {
    const sql = createUtilitySql(url);
    try {
      const db = drizzlePg(sql);
      await migratePg(db, { migrationsFolder: MIGRATIONS_FOLDER });
    } finally {
      await sql.end();
    }

    let bootstrappedState = await inspectMigrations(url);
    if (bootstrappedState.status === "upToDate") return;
    if (bootstrappedState.reason === "pending-migrations") {
      const repair = await reconcilePendingMigrationHistory(url);
      if (repair.repairedMigrations.length > 0) {
        bootstrappedState = await inspectMigrations(url);
      }
      if (bootstrappedState.status === "needsMigrations" && bootstrappedState.reason === "pending-migrations") {
        await applyPendingMigrationsManually(url, bootstrappedState.pendingMigrations);
        bootstrappedState = await inspectMigrations(url);
      }
    }
    if (bootstrappedState.status === "upToDate") return;
    throw new Error(
      `Failed to bootstrap migrations: ${bootstrappedState.pendingMigrations.join(", ")}`,
    );
  }

  if (initialState.reason === "no-migration-journal-non-empty-db") {
    throw new Error(
      "Database has tables but no migration journal; automatic migration is unsafe. Initialize migration history manually.",
    );
  }

  let state = await inspectMigrations(url);
  if (state.status === "upToDate") return;

  const repair = await reconcilePendingMigrationHistory(url);
  if (repair.repairedMigrations.length > 0) {
    state = await inspectMigrations(url);
    if (state.status === "upToDate") return;
  }

  if (state.status !== "needsMigrations" || state.reason !== "pending-migrations") {
    throw new Error("Migrations are still pending after migration-history reconciliation; run inspectMigrations for details.");
  }

  await applyPendingMigrationsManually(url, state.pendingMigrations);

  const finalState = await inspectMigrations(url);
  if (finalState.status !== "upToDate") {
    throw new Error(
      `Failed to apply pending migrations: ${finalState.pendingMigrations.join(", ")}`,
    );
  }
}

export type MigrationBootstrapResult =
  | { migrated: true; reason: "migrated-empty-db"; tableCount: 0 }
  | { migrated: false; reason: "already-migrated"; tableCount: number }
  | { migrated: false; reason: "not-empty-no-migration-journal"; tableCount: number };

export async function migratePostgresIfEmpty(url: string): Promise<MigrationBootstrapResult> {
  const sql = createUtilitySql(url);

  try {
    const migrationTableSchema = await discoverMigrationTableSchema(sql);

    const tableCountResult = await sql<{ count: number }[]>`
      select count(*)::int as count
      from information_schema.tables
      where table_schema = 'public'
        and table_type = 'BASE TABLE'
    `;

    const tableCount = tableCountResult[0]?.count ?? 0;

    if (migrationTableSchema) {
      return { migrated: false, reason: "already-migrated", tableCount };
    }

    if (tableCount > 0) {
      return { migrated: false, reason: "not-empty-no-migration-journal", tableCount };
    }

    const db = drizzlePg(sql);
    await migratePg(db, { migrationsFolder: MIGRATIONS_FOLDER });

    return { migrated: true, reason: "migrated-empty-db", tableCount: 0 };
  } finally {
    await sql.end();
  }
}

export async function ensurePostgresDatabase(
  url: string,
  databaseName: string,
): Promise<"created" | "exists"> {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(databaseName)) {
    throw new Error(`Unsafe database name: ${databaseName}`);
  }

  const sql = createUtilitySql(url);
  try {
    const existing = await sql<{ one: number }[]>`
      select 1 as one from pg_database where datname = ${databaseName} limit 1
    `;
    if (existing.length > 0) return "exists";

    await sql.unsafe(`create database "${databaseName}" encoding 'UTF8' lc_collate 'C' lc_ctype 'C' template template0`);
    return "created";
  } finally {
    await sql.end();
  }
}

export async function resetPostgresDatabase(
  url: string,
  databaseName: string,
): Promise<"reset"> {
  const quotedDatabaseName = quoteIdentifier(databaseName);
  const sql = createUtilitySql(url);
  try {
    await sql`
      select pg_terminate_backend(pid)
      from pg_stat_activity
      where datname = ${databaseName}
        and pid <> pg_backend_pid()
    `;
    await sql.unsafe(`drop database if exists ${quotedDatabaseName}`);
    await sql.unsafe(`create database ${quotedDatabaseName} encoding 'UTF8' lc_collate 'C' lc_ctype 'C' template template0`);
    return "reset";
  } finally {
    await sql.end();
  }
}

export type Db = ReturnType<typeof createDb>;
