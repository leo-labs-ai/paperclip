import { describe, expect, it } from "vitest";
import {
  DB_SOCKET_TIMEOUT_MARGIN_MS,
  DEFAULT_DATABASE_APPLICATION_NAME,
  DEFAULT_DATABASE_IDLE_TIMEOUT_SECONDS,
  DEFAULT_DB_CONNECT_TIMEOUT_SEC,
  DEFAULT_DB_IDLE_TIMEOUT_SEC,
  DEFAULT_DB_IDLE_IN_TX_TIMEOUT_MS,
  DEFAULT_DB_KEEPALIVE_SEC,
  DEFAULT_DB_MAX_LIFETIME_SEC,
  DEFAULT_DB_STATEMENT_TIMEOUT_MS,
  databaseClientOptionsFromEnv,
  postgresJsOptions,
  resolveDatabaseClientOptions,
} from "./client.js";

// Fork: when nothing is set, idle/lifetime bounds and session guards default
// on instead of preserving the driver's unbounded behaviour (CNPG
// smart-shutdown fix; pool-starvation-503 fix; HOM-441 failover-reconnect fix).
const FORK_DEFAULTS = {
  idleTimeoutSeconds: DEFAULT_DB_IDLE_TIMEOUT_SEC,
  connectTimeoutSeconds: DEFAULT_DB_CONNECT_TIMEOUT_SEC,
  maxLifetimeSeconds: DEFAULT_DB_MAX_LIFETIME_SEC,
  statementTimeoutMs: DEFAULT_DB_STATEMENT_TIMEOUT_MS,
  idleInTransactionTimeoutMs: DEFAULT_DB_IDLE_IN_TX_TIMEOUT_MS,
  keepAliveSeconds: DEFAULT_DB_KEEPALIVE_SEC,
  socketTimeoutMs: DEFAULT_DB_STATEMENT_TIMEOUT_MS + DB_SOCKET_TIMEOUT_MARGIN_MS,
};

describe("databaseClientOptionsFromEnv", () => {
  it("returns only fork pool bounds when nothing is set", () => {
    expect(databaseClientOptionsFromEnv({})).toEqual(FORK_DEFAULTS);
    expect(postgresJsOptions(databaseClientOptionsFromEnv({}))).toEqual({
      idle_timeout: DEFAULT_DB_IDLE_TIMEOUT_SEC,
      connect_timeout: DEFAULT_DB_CONNECT_TIMEOUT_SEC,
      max_lifetime: DEFAULT_DB_MAX_LIFETIME_SEC,
      keep_alive: DEFAULT_DB_KEEPALIVE_SEC,
      connection: {
        statement_timeout: DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        idle_in_transaction_session_timeout: DEFAULT_DB_IDLE_IN_TX_TIMEOUT_MS,
      },
    });
  });

  it("ignores empty values", () => {
    expect(
      databaseClientOptionsFromEnv({
        DATABASE_PREPARED_STATEMENTS: "",
        DATABASE_POOL_MAX: "",
      }),
    ).toEqual(FORK_DEFAULTS);
  });

  it("parses prepared-statement toggles", () => {
    expect(databaseClientOptionsFromEnv({ DATABASE_PREPARED_STATEMENTS: "false" })).toEqual({ prepare: false, ...FORK_DEFAULTS });
    expect(databaseClientOptionsFromEnv({ DATABASE_PREPARED_STATEMENTS: "0" })).toEqual({ prepare: false, ...FORK_DEFAULTS });
    expect(databaseClientOptionsFromEnv({ DATABASE_PREPARED_STATEMENTS: "true" })).toEqual({ prepare: true, ...FORK_DEFAULTS });
    expect(databaseClientOptionsFromEnv({ DATABASE_PREPARED_STATEMENTS: "TRUE" })).toEqual({ prepare: true, ...FORK_DEFAULTS });
  });

  it("parses pool and timeout settings", () => {
    expect(
      databaseClientOptionsFromEnv({
        DATABASE_POOL_MAX: "25",
        DATABASE_IDLE_TIMEOUT_SECONDS: "60",
        DATABASE_CONNECT_TIMEOUT_SECONDS: "10",
        DATABASE_MAX_LIFETIME_SECONDS: "1800",
        DATABASE_APPLICATION_NAME: " paperclip-web ",
      }),
    ).toEqual({
      maxConnections: 25,
      idleTimeoutSeconds: 60,
      connectTimeoutSeconds: 10,
      maxLifetimeSeconds: 1800,
      statementTimeoutMs: DEFAULT_DB_STATEMENT_TIMEOUT_MS,
      idleInTransactionTimeoutMs: DEFAULT_DB_IDLE_IN_TX_TIMEOUT_MS,
      keepAliveSeconds: DEFAULT_DB_KEEPALIVE_SEC,
      socketTimeoutMs: DEFAULT_DB_STATEMENT_TIMEOUT_MS + DB_SOCKET_TIMEOUT_MARGIN_MS,
      applicationName: "paperclip-web",
    });
  });

  it("accepts DATABASE_IDLE_TIMEOUT_SECONDS=0 as an explicit opt-out of idle reaping", () => {
    expect(databaseClientOptionsFromEnv({ DATABASE_IDLE_TIMEOUT_SECONDS: "0" })).toEqual({
      ...FORK_DEFAULTS,
      idleTimeoutSeconds: 0,
    });
  });

  it("rejects malformed values instead of silently ignoring them", () => {
    expect(() => databaseClientOptionsFromEnv({ DATABASE_PREPARED_STATEMENTS: "maybe" })).toThrow(
      /DATABASE_PREPARED_STATEMENTS/,
    );
    expect(() => databaseClientOptionsFromEnv({ DATABASE_POOL_MAX: "0" })).toThrow(/DATABASE_POOL_MAX/);
    expect(() => databaseClientOptionsFromEnv({ DATABASE_POOL_MAX: "-3" })).toThrow(/DATABASE_POOL_MAX/);
    expect(() => databaseClientOptionsFromEnv({ DATABASE_CONNECT_TIMEOUT_SECONDS: "1.5" })).toThrow(
      /DATABASE_CONNECT_TIMEOUT_SECONDS/,
    );
    expect(() => databaseClientOptionsFromEnv({ DATABASE_IDLE_TIMEOUT_SECONDS: "-1" })).toThrow(
      /DATABASE_IDLE_TIMEOUT_SECONDS/,
    );
    expect(() => databaseClientOptionsFromEnv({ DATABASE_IDLE_TIMEOUT_SECONDS: "abc" })).toThrow(
      /DATABASE_IDLE_TIMEOUT_SECONDS/,
    );
    expect(() => databaseClientOptionsFromEnv({ DATABASE_MAX_LIFETIME_SECONDS: "0" })).toThrow(
      /DATABASE_MAX_LIFETIME_SECONDS/,
    );
    expect(() => databaseClientOptionsFromEnv({ DATABASE_MAX_LIFETIME_SECONDS: "NaN" })).toThrow(
      /DATABASE_MAX_LIFETIME_SECONDS/,
    );
  });

  it("maps to postgres.js option names", () => {
    expect(
      postgresJsOptions({
        prepare: false,
        maxConnections: 25,
        idleTimeoutSeconds: 60,
        connectTimeoutSeconds: 10,
        maxLifetimeSeconds: 1800,
        applicationName: "paperclip-web",
      }),
    ).toEqual({
      prepare: false,
      max: 25,
      idle_timeout: 60,
      connect_timeout: 10,
      max_lifetime: 1800,
      connection: { application_name: "paperclip-web" },
    });
  });
});

describe("resolveDatabaseClientOptions", () => {
  it("reaps idle connections and names the pool when the environment sets nothing", () => {
    expect(resolveDatabaseClientOptions({})).toEqual({
      idleTimeoutSeconds: DEFAULT_DATABASE_IDLE_TIMEOUT_SECONDS,
      applicationName: DEFAULT_DATABASE_APPLICATION_NAME,
    });
    // Fork: databaseClientOptionsFromEnv already sets the fork's tighter idle
    // timeout and session guards, so only application_name comes from here.
    expect(postgresJsOptions(resolveDatabaseClientOptions(databaseClientOptionsFromEnv({})))).toEqual({
      idle_timeout: DEFAULT_DB_IDLE_TIMEOUT_SEC,
      connect_timeout: DEFAULT_DB_CONNECT_TIMEOUT_SEC,
      max_lifetime: DEFAULT_DB_MAX_LIFETIME_SEC,
      keep_alive: DEFAULT_DB_KEEPALIVE_SEC,
      connection: {
        statement_timeout: DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        idle_in_transaction_session_timeout: DEFAULT_DB_IDLE_IN_TX_TIMEOUT_MS,
        application_name: DEFAULT_DATABASE_APPLICATION_NAME,
      },
    });
  });

  it("keeps every explicit value, including an idle timeout of 0", () => {
    expect(
      resolveDatabaseClientOptions({
        maxConnections: 3,
        idleTimeoutSeconds: 0,
        applicationName: "paperclip-cli",
      }),
    ).toEqual({ maxConnections: 3, idleTimeoutSeconds: 0, applicationName: "paperclip-cli" });
    expect(postgresJsOptions(resolveDatabaseClientOptions({ idleTimeoutSeconds: 0 }))).toMatchObject({
      idle_timeout: 0,
    });
  });
});

describe("postgresJsOptions HA-failover wiring (HOM-441)", () => {
  it("omits connect_timeout entirely when the knob is 0, restoring the driver default", () => {
    // Passing `connect_timeout: 0` through would disable the driver's connect
    // timer outright (postgres.js's `timer()` treats a falsy value as "no
    // timer"), the opposite of what `0` is documented to mean.
    const options = databaseClientOptionsFromEnv({ PAPERCLIP_DB_CONNECT_TIMEOUT_SEC: "0" });
    expect(options.connectTimeoutSeconds).toBe(0);
    expect(postgresJsOptions(options)).not.toHaveProperty("connect_timeout");
  });

  it("keeps the driver's own dial when no connection string is supplied", () => {
    // Without a URL the host count cannot be checked, and the custom factory
    // only ever dials host[0] -- installing it on an unverified target would
    // silently disable the driver's multi-host rotation.
    expect(postgresJsOptions(databaseClientOptionsFromEnv({}))).not.toHaveProperty("socket");
  });

  it("keeps the driver's own dial for a multi-host connection string", () => {
    // The custom-socket branch of postgres.js's `connect()` skips its
    // per-connection host rotation (`hostIndex`), which a shared factory
    // cannot reproduce, so a multi-host URL keeps the driver's dial.
    const options = databaseClientOptionsFromEnv({});
    expect(postgresJsOptions(options, "postgres://u:p@h1,h2:5432/db")).not.toHaveProperty("socket");
    expect(postgresJsOptions(options, "postgres://u:p@h1:5432/db")).toHaveProperty("socket", expect.any(Function));
  });
});
