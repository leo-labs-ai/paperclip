import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import tls from "node:tls";
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
/** A throwaway self-signed pair so a real TLS handshake can run in-process. */
function selfSignedLocalhostCert(): { key: string; cert: string } {
  const dir = mkdtempSync(path.join(tmpdir(), "paperclip-db-tls-"));
  const keyPath = path.join(dir, "key.pem");
  const certPath = path.join(dir, "cert.pem");
  try {
    execFileSync(
      "openssl",
      // prettier-ignore
      ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", keyPath, "-out", certPath, "-days", "1", "-subj", "/CN=127.0.0.1"],
      { stdio: "ignore" },
    );
    return { key: readFileSync(keyPath, "utf8"), cert: readFileSync(certPath, "utf8") };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

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

  it("still destroys the socket when only outbound writes keep happening", async () => {
    // postgres.js pipelines: after a failover every further query is written
    // onto the already-dead socket. A read-or-write idle window (what
    // `net.Socket#setTimeout` measures) would be pushed out by each of those
    // writes, so the backstop would never reclaim the connection. The window
    // must therefore track reads only.
    const port = await startEchoServer(); // accepts, never answers
    const socket = await socketWithInactivityTimeout(120)({ host: ["127.0.0.1"], port: [port] });
    socket.on("error", () => {});

    const closed = new Promise<void>((resolve) => socket.once("close", () => resolve()));
    const writer = setInterval(() => {
      if (!socket.destroyed) socket.write("ping");
    }, 20);
    try {
      await closed;
    } finally {
      clearInterval(writer);
    }
    expect(socket.destroyed).toBe(true);
  });

  it("does not destroy a healthy connection once postgres.js wraps it in TLS", async () => {
    // `sslmode=require` makes postgres.js's `secure()` hand the socket this
    // factory dialled to `tls.connect({ socket })`. The TLS wrapper takes the
    // reads over, so the raw socket stops emitting `data` even while bytes
    // keep arriving — a backstop keyed on those events would expire on a
    // perfectly healthy connection and fail whatever query is on the wire.
    const { key, cert } = selfSignedLocalhostCert();
    const tlsServer = tls.createServer({ key, cert }, (socket) => {
      socket.on("data", (chunk) => socket.write(chunk));
      socket.on("error", () => {});
    });
    const port = await new Promise<number>((resolve) => {
      tlsServer.listen(0, "127.0.0.1", () => resolve((tlsServer.address() as net.AddressInfo).port));
    });

    try {
      const raw = await socketWithInactivityTimeout(120)({ host: ["127.0.0.1"], port: [port] });
      raw.on("error", () => {});
      const secured = tls.connect({ socket: raw, rejectUnauthorized: false });
      secured.on("error", () => {});
      await new Promise<void>((resolve, reject) => {
        secured.once("secureConnect", resolve);
        secured.once("error", reject);
      });

      // Exchange encrypted bytes for twice the idle window.
      for (let i = 0; i < 6; i++) {
        secured.write("ping");
        await new Promise((resolve) => setTimeout(resolve, 40));
      }

      expect(raw.destroyed).toBe(false);
      expect(secured.destroyed).toBe(false);
      secured.destroy();
    } finally {
      await new Promise((resolve) => tlsServer.close(resolve));
    }
  }, 10_000);

  it("reclaims a socket within its documented window, not twice it", async () => {
    // The backstop advertises a single `timeoutMs` window. Sampling the read
    // counter once per window cannot honour that: whatever the last sample
    // saw, silence that begins just after it is only noticed a full further
    // window later, so a connection that dies mid-query survives up to twice
    // the documented time before the query fails.
    const timeoutMs = 400;
    server = net.createServer((socket) => {
      serverSockets.push(socket);
      socket.on("error", () => {});
      socket.write("hello"); // one burst, then silence, like a dead primary
    });
    const port = await new Promise<number>((resolve) => {
      server!.listen(0, "127.0.0.1", () => resolve((server!.address() as net.AddressInfo).port));
    });

    const socket = await socketWithInactivityTimeout(timeoutMs)({ host: ["127.0.0.1"], port: [port] });
    socket.on("error", () => {});
    let lastDataAt = Date.now();
    socket.on("data", () => {
      lastDataAt = Date.now();
    });

    const closedAt = await new Promise<number>((resolve) => socket.once("close", () => resolve(Date.now())));
    expect(socket.destroyed).toBe(true);
    const elapsedMs = closedAt - lastDataAt;
    expect(elapsedMs).toBeGreaterThanOrEqual(timeoutMs * 0.9);
    expect(elapsedMs).toBeLessThan(timeoutMs * 1.5);
  }, 10_000);

  it("rejects instead of hanging when the dial never completes", async () => {
    // TEST-NET-1 (RFC 5737) is reserved and unrouted: the SYN either
    // blackholes or is refused by the local stack. Either way the factory's
    // promise has to settle — postgres.js awaits it inside `connect()` and
    // attaches its own listeners only afterwards, so a pending promise wedges
    // that pool slot forever.
    const startedAt = Date.now();
    await expect(
      socketWithInactivityTimeout(60_000, 150)({ host: ["192.0.2.1"], port: [5432] }),
    ).rejects.toBeInstanceOf(Error);
    expect(Date.now() - startedAt).toBeLessThan(5_000);
  }, 10_000);
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

  it("still sends TLS SNI for a non-IP host when the custom socket factory dials", async () => {
    // postgres.js derives the handshake's `servername` from `socket.host`,
    // which it sets on its own dial branch -- a branch the custom-socket
    // contract skips. An endpoint that routes on SNI (Neon, Supavisor, an
    // SNI-based ingress in front of the cluster) refuses a handshake that
    // carries none, so the factory has to supply it.
    const { key, cert } = selfSignedLocalhostCert();
    const secureContext = tls.createSecureContext({ key, cert });
    let sniServername: string | false | null = null;

    server = net.createServer((raw) => {
      raw.on("error", () => {});
      raw.once("data", () => {
        // SSLRequest: 'S' accepts, then the same socket carries the handshake.
        raw.write(Buffer.from("S"));
        const secured = new tls.TLSSocket(raw, { isServer: true, secureContext });
        secured.on("error", () => {});
        secured.once("secure", () => {
          sniServername = secured.servername;
        });
        let greeted = false;
        secured.on("data", () => {
          if (!greeted) {
            greeted = true;
            secured.write(Buffer.concat([authOk, readyForQuery]));
            return;
          }
          secured.write(Buffer.concat([emptyQueryReply, readyForQuery]));
        });
      });
    });
    // Bind every interface so the literal host `localhost` resolves onto it
    // whichever family the resolver prefers.
    const port = await new Promise<number>((resolve) => {
      server!.listen(0, () => resolve((server!.address() as net.AddressInfo).port));
    });

    sql = postgres(`postgres://test:test@localhost:${port}/test`, {
      ssl: { rejectUnauthorized: false },
      connect_timeout: 5,
      prepare: false,
      max: 1,
      idle_timeout: 0,
      socket: socketWithInactivityTimeout(5_000, 2_000),
    } as postgres.Options<Record<string, never>>);

    await expect(sql.unsafe("select 1", [])).resolves.toBeDefined();
    expect(sniServername).toBe("localhost");
  }, 15_000);

  it("reclaims the pool slot after a pre-connect dial failure instead of leaking it forever", async () => {
    // Grab an ephemeral port and immediately stop listening on it: the next
    // connect attempt against it gets ECONNREFUSED, which is the pre-connect
    // failure path in postgres.js's custom-socket branch of `createSocket()`
    // -- distinct from the mid-query silent-death path the rest of this
    // file covers. `socketWithInactivityTimeout`'s factory promise rejects
    // on that failure (see its `onPreConnectError` handler), and
    // postgres.js's own `createSocket()` (src/connection.js) only calls
    // `error(e)` on a rejected factory promise -- it never calls `onclose`,
    // so the connection object is left parked in the pool's `connecting`
    // queue forever with `error`/`close` listeners never attached to a real
    // socket. With `max: 1` that is the pool's only slot: every later query
    // queues behind it and hangs forever, reproducing the same class of
    // stuck-pod symptom HOM-441 exists to eliminate, just one layer earlier
    // (dial time instead of query time).
    const probe = net.createServer();
    const port = await new Promise<number>((resolve) => {
      probe.listen(0, "127.0.0.1", () => resolve((probe.address() as net.AddressInfo).port));
    });
    await new Promise((resolve) => probe.close(resolve));

    const url = `postgres://test:test@127.0.0.1:${port}/test`;
    sql = postgres(url, {
      connect_timeout: 5,
      prepare: false,
      max: 1,
      idle_timeout: 0,
      socket: socketWithInactivityTimeout(5_000, 200),
    } as postgres.Options<Record<string, never>>);

    await expect(sql.unsafe("select 1", [])).rejects.toBeInstanceOf(Error);

    // Bring up a real (minimal) server on the same port now, and prove the
    // *next* query succeeds quickly. If the pre-connect failure had leaked
    // the pool's one slot, this second query would sit behind it in
    // postgres.js's own pending-queries queue and never get a connection --
    // no timeout of its own would ever fire for it.
    server = net.createServer((socket) => {
      let greeted = false;
      socket.on("data", () => {
        if (!greeted) {
          greeted = true;
          socket.write(Buffer.concat([authOk, readyForQuery]));
          return;
        }
        socket.write(Buffer.concat([emptyQueryReply, readyForQuery]));
      });
      socket.on("error", () => {});
    });
    await new Promise<void>((resolve) => server!.listen(port, "127.0.0.1", () => resolve()));

    const startedAt = Date.now();
    await expect(sql.unsafe("select 1", [])).resolves.toBeDefined();
    expect(Date.now() - startedAt).toBeLessThan(2_000);
  }, 10_000);
});
