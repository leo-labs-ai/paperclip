import net from "node:net";
import postgres from "postgres";
import { afterEach, describe, expect, it } from "vitest";
import { socketWithInactivityTimeout } from "./client.js";

/**
 * Regression coverage for HOM-441: a CloudNativePG primary switchover can
 * leave a pooled connection's TCP session open on the client side with
 * neither a FIN nor an RST, so a query already in flight on it waits
 * forever for a response that will never arrive. Neither `statement_timeout`
 * (server-side; the server never received the query) nor the driver's own
 * `connect_timeout` (only guards establishing a *new* connection) bounds
 * that wait. `socketWithInactivityTimeout` is the client-side backstop:
 * `net.Socket#setTimeout` fires once neither a read nor a write has
 * happened for the configured window, and destroying the socket on that
 * timeout raises the same `error`/`close` sequence postgres.js already
 * handles for a network-dropped connection, so the query rejects and the
 * pool reconnects on the next query — no process restart required.
 */
describe("socketWithInactivityTimeout", () => {
  let server: net.Server | null = null;
  const serverSockets: net.Socket[] = [];

  afterEach(async () => {
    for (const socket of serverSockets) socket.destroy();
    serverSockets.length = 0;
    if (server) await new Promise((resolve) => server!.close(resolve));
    server = null;
  });

  function startEchoServer(): Promise<number> {
    server = net.createServer((socket) => {
      serverSockets.push(socket);
      socket.on("error", () => {});
    });
    return new Promise((resolve) => {
      server!.listen(0, "127.0.0.1", () => resolve((server!.address() as net.AddressInfo).port));
    });
  }

  it("destroys the socket once it has been silent for the configured window", async () => {
    const port = await startEchoServer();
    const socket = await socketWithInactivityTimeout(80)({ host: ["127.0.0.1"], port: [port] });
    socket.on("error", () => {});

    expect(socket.destroyed).toBe(false);
    const destroyed = await new Promise<boolean>((resolve) => {
      socket.once("close", () => resolve(socket.destroyed));
    });
    expect(destroyed).toBe(true);
  });

  it("does not destroy a socket that keeps exchanging bytes within the window", async () => {
    server = net.createServer((socket) => {
      serverSockets.push(socket);
      socket.on("data", (chunk) => socket.write(chunk));
      socket.on("error", () => {});
    });
    const port = await new Promise<number>((resolve) => {
      server!.listen(0, "127.0.0.1", () => resolve((server!.address() as net.AddressInfo).port));
    });

    const socket = await socketWithInactivityTimeout(120)({ host: ["127.0.0.1"], port: [port] });
    socket.on("error", () => {});

    // Ping the echo server every 40ms — well inside the 120ms idle window —
    // for longer than that window, and confirm the socket survives.
    for (let i = 0; i < 6; i++) {
      socket.write("ping");
      await new Promise((resolve) => setTimeout(resolve, 40));
    }
    expect(socket.destroyed).toBe(false);
    socket.destroy();
  });
});

describe("a postgres.js client reconnects through a socket left dead by a silent failover", () => {
  const authOk = Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0]);
  const readyForQuery = Buffer.from([0x5a, 0, 0, 0, 5, 0x49]);
  const emptyQueryReply = Buffer.concat([
    Buffer.from([0x31, 0, 0, 0, 4]), // ParseComplete
    // ParameterDescription (0 params): the client's Describe('S', ...) targets
    // the statement, not just the portal, so it always expects this message
    // — even with zero parameters — before RowDescription/NoData. Omitting it
    // desyncs the client's parser on the very next byte, which postgres.js
    // treats as a protocol-level connection error and retries silently
    // forever (the `initial`-query reconnect path), not as a query failure.
    Buffer.from([0x74, 0, 0, 0, 6, 0, 0]),
    Buffer.from([0x54, 0, 0, 0, 6, 0, 0]), // RowDescription, zero fields
    Buffer.from([0x32, 0, 0, 0, 4]), // BindComplete
    Buffer.from([0x43, 0, 0, 0, 0x0d, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x30, 0]), // CommandComplete "SELECT 0"
  ]);

  /**
   * A fake primary whose responsiveness is controlled by `alive`, shared
   * across every connection it accepts. The startup handshake always
   * succeeds — including postgres.js's own internal array-types bootstrap
   * query that runs before a caller's first query ever reaches the wire —
   * so a connection can fully warm up while `alive` is still true. Flipping
   * `alive` to false then reproduces exactly what a CloudNativePG
   * switchover leaves behind: the TCP session stays open with neither a FIN
   * nor an RST, and any query already using it just never gets a reply.
   * Flipping it back to true and letting the dead connection get replaced
   * stands in for the failover completing underneath a reconnect.
   */
  function startFlakyPrimary(): Promise<{ server: net.Server; port: number; alive: { value: boolean }; connectionCount: () => number }> {
    let connections = 0;
    const alive = { value: true };
    const server = net.createServer((socket) => {
      connections += 1;
      let greeted = false;
      socket.on("data", () => {
        if (!greeted) {
          greeted = true;
          socket.write(Buffer.concat([authOk, readyForQuery]));
          return;
        }
        if (!alive.value) return; // silent: nothing already in flight ever gets a reply
        socket.write(Buffer.concat([emptyQueryReply, readyForQuery]));
      });
      socket.on("error", () => {});
    });
    return new Promise((resolve) => {
      server.listen(0, "127.0.0.1", () => {
        resolve({ server, port: (server.address() as net.AddressInfo).port, alive, connectionCount: () => connections });
      });
    });
  }

  let server: net.Server | null = null;
  let sql: postgres.Sql | null = null;

  afterEach(async () => {
    if (sql) await sql.end({ timeout: 0 });
    if (server) await new Promise((resolve) => server!.close(resolve));
    server = null;
    sql = null;
  });

  it("fails the hung query fast and serves the next one on a fresh connection", async () => {
    const started = await startFlakyPrimary();
    server = started.server;
    const url = `postgres://test:test@127.0.0.1:${started.port}/test`;

    // This exercises `socketWithInactivityTimeout` the same way
    // `postgresJsOptions()` wires it up, but through a raw postgres.js
    // client rather than `createDb()`: `createDb()` also layers
    // `withTransientWriteRetry` on top (see transient-write-retry.ts), whose
    // retry lands on a brand-new connection. Bounding *that* connection's
    // own hang is a separate concern — it never gets to reuse an already-
    // open socket, so it doesn't hit the mid-query-death path this backstop
    // targets — and is covered where the retry wrapper and the auth-path
    // timeout are exercised together, not here.
    // maxConnections: 1 forces the second query through the same pool slot,
    // proving it was actually recycled rather than served by a spare.
    // `socket` is postgres.js's documented custom-socket-factory option
    // (see the "Custom socket" section of its README), but it's not part of
    // the published `Options<{}>` type, hence the cast — the same shape
    // `postgresJsOptions()` builds in client.ts.
    sql = postgres(url, {
      connect_timeout: 5,
      prepare: false,
      max: 1,
      idle_timeout: 0,
      socket: socketWithInactivityTimeout(150),
    } as postgres.Options<Record<string, never>>);

    // Warm the pool's one connection up while the primary is healthy: this
    // clears postgres.js's own "initial connection" bootstrap (its internal
    // array-types query included), so the failover below hits an ordinary,
    // already-established query — the actual HOM-441 shape — rather than the
    // driver's separate, silently-retried-forever handling of a connection
    // that never finished connecting in the first place.
    await expect(sql.unsafe("select 1", [])).resolves.toBeDefined();

    // Simulate the switchover: the TCP session survives, but the peer that
    // would answer it is gone.
    started.alive.value = false;

    const startedAt = Date.now();
    await expect(sql.unsafe("select 1", [])).rejects.toBeInstanceOf(Error);
    const elapsedMs = Date.now() - startedAt;
    // Well under the suite's own timeout, and nowhere near "forever": proves
    // the query failed because the inactivity backstop fired, not because
    // something else eventually gave up.
    expect(elapsedMs).toBeLessThan(5_000);

    // Simulate the new primary being reachable, and the pool reconnecting
    // transparently: the same client handle, no process restart, no caller
    // ever touching the pool directly.
    started.alive.value = true;
    await expect(sql.unsafe("select 1", [])).resolves.toBeDefined();
    expect(started.connectionCount()).toBeGreaterThanOrEqual(2);
  }, 15_000);
});
