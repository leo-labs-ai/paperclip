import net from "node:net";
import { closeRegisteredClients, createDb } from "@paperclipai/db";
import { sql as drizzleSql } from "drizzle-orm";
import { afterEach, describe, expect, it } from "vitest";

import { AuthDbTimeoutError, authDbLookup } from "../middleware/auth.ts";

/** The shape drizzle produces: a wrapper whose `cause` is the driver error. */
function driverClosedError(code: string): Error {
  const driver = Object.assign(new Error(`write ${code} db.example.internal:5432`), { code });
  return new Error("Failed query: select …", { cause: driver });
}

describe("authDbLookup", () => {
  const originalTimeoutEnv = process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS;

  afterEach(() => {
    if (originalTimeoutEnv === undefined) delete process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS;
    else process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS = originalTimeoutEnv;
  });

  it("resolves whatever the lookup resolves", async () => {
    await expect(authDbLookup(async () => "ok")).resolves.toBe("ok");
  });

  it("replays once on a transient closed-connection error, same as retryOnTransientDbConnectionError", async () => {
    let calls = 0;
    const result = await authDbLookup(async () => {
      calls += 1;
      if (calls === 1) throw driverClosedError("CONNECTION_CLOSED");
      return "ok";
    });
    expect(result).toBe("ok");
    expect(calls).toBe(2);
  });

  it("propagates a non-transient failure immediately, without waiting for the timeout", async () => {
    process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS = "5000";
    const startedAt = Date.now();
    await expect(authDbLookup(async () => {
      throw new Error("constraint violation");
    })).rejects.toThrow("constraint violation");
    expect(Date.now() - startedAt).toBeLessThan(1_000);
  });

  it("times out with AuthDbTimeoutError when the lookup never settles", async () => {
    process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS = "60";
    const startedAt = Date.now();
    await expect(authDbLookup(() => new Promise(() => {}))).rejects.toBeInstanceOf(AuthDbTimeoutError);
    const elapsedMs = Date.now() - startedAt;
    expect(elapsedMs).toBeGreaterThanOrEqual(50);
    expect(elapsedMs).toBeLessThan(1_000);
  });

  it("falls back to the default timeout for an unset or invalid env value", async () => {
    delete process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS;
    // Not asserting the exact 5s default here (that would make the suite
    // slow); asserting instead that an invalid value doesn't make the
    // lookup hang forever with no timeout at all.
    process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS = "not-a-number";
    await expect(authDbLookup(async () => "ok")).resolves.toBe("ok");
  });
});

describe("authDbLookup bounds a Bearer/agent-key DB call even when postgres.js's own reconnect never gives up", () => {
  // HOM-441: a CloudNativePG switchover can leave a pooled TCP session open
  // with neither a FIN nor an RST. `createDb()` also layers
  // `withTransientWriteRetry` (packages/db) on a `CONNECTION_CLOSED` write
  // failure, and that replay lands on a brand-new connection — postgres.js's
  // own handling of a first ("initial") query on a connection that keeps
  // completing the TCP handshake but then goes silent again has no bound of
  // its own (see packages/db/src/socket-inactivity-timeout.test.ts for the
  // narrower, pool-level backstop). `authDbLookup` is the caller-facing fix:
  // regardless of what postgres.js is still doing in the background, the
  // Bearer/agent-key auth path fails fast and the next request succeeds
  // once the failover completes — no process restart required.
  const authOk = Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0]);
  const readyForQuery = Buffer.from([0x5a, 0, 0, 0, 5, 0x49]);
  const emptyQueryReply = Buffer.concat([
    Buffer.from([0x31, 0, 0, 0, 4]), // ParseComplete
    Buffer.from([0x74, 0, 0, 0, 6, 0, 0]), // ParameterDescription, 0 params
    Buffer.from([0x54, 0, 0, 0, 6, 0, 0]), // RowDescription, 0 fields
    Buffer.from([0x32, 0, 0, 0, 4]), // BindComplete
    Buffer.from([0x43, 0, 0, 0, 0x0d, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x30, 0]), // CommandComplete
  ]);

  function startFlakyPrimary(): Promise<{ server: net.Server; port: number; alive: { value: boolean } }> {
    const alive = { value: true };
    const server = net.createServer((socket) => {
      let greeted = false;
      socket.on("data", () => {
        if (!greeted) {
          greeted = true;
          socket.write(Buffer.concat([authOk, readyForQuery]));
          return;
        }
        if (!alive.value) return; // silent, forever, like a dead switchover peer
        socket.write(Buffer.concat([emptyQueryReply, readyForQuery]));
      });
      socket.on("error", () => {});
    });
    return new Promise((resolve) => {
      server.listen(0, "127.0.0.1", () => {
        resolve({ server, port: (server.address() as net.AddressInfo).port, alive });
      });
    });
  }

  let server: net.Server | null = null;
  let url: string | null = null;

  afterEach(async () => {
    delete process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS;
    if (url) await closeRegisteredClients(url);
    if (server) await new Promise((resolve) => server!.close(resolve));
    server = null;
    url = null;
  });

  it("fails a stuck agent-key lookup fast instead of hanging indefinitely", async () => {
    process.env.PAPERCLIP_AUTH_DB_TIMEOUT_MS = "300";
    const started = await startFlakyPrimary();
    server = started.server;
    url = `postgres://test:test@127.0.0.1:${started.port}/test`;

    // A long pool-level socket timeout on purpose: this proves the *caller*
    // is bounded by authDbLookup itself, not by the pool-level backstop
    // happening to fire first.
    const db = createDb(url, {
      connectTimeoutSeconds: 5,
      prepare: false,
      maxConnections: 1,
      idleTimeoutSeconds: 0,
      socketTimeoutMs: 5_000,
    });

    await authDbLookup(() => db.execute(drizzleSql`select 1`));

    started.alive.value = false;
    const startedAt = Date.now();
    await expect(authDbLookup(() => db.execute(drizzleSql`select 1`))).rejects.toBeInstanceOf(AuthDbTimeoutError);
    const elapsedMs = Date.now() - startedAt;
    // Bounded to the configured ceiling, nowhere near postgres.js's own
    // unbounded internal reconnect loop underneath it.
    expect(elapsedMs).toBeLessThan(2_000);
  }, 10_000);
});
