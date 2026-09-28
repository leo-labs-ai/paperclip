import { beforeEach, afterEach, describe, expect, it } from "vitest";
import { eq, sql } from "drizzle-orm";
import {
  createDrizzleHeartbeatResultRetentionDb,
  createHeartbeatResultRetention,
  resolveHeartbeatResultRetentionConfig,
  type HeartbeatResultRetentionConfig,
  type HeartbeatResultRetentionDb,
} from "../services/heartbeat-result-retention.ts";
import {
  HEARTBEAT_RUN_RESULT_OUTPUT_MAX_CHARS,
  HEARTBEAT_RUN_SAFE_RESULT_JSON_MAX_BYTES,
} from "../services/heartbeat-run-summary.ts";
import { loadConfig } from "../config.ts";
import { agents, companies, createDb, heartbeatRuns } from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";

const NOW = new Date("2026-08-20T10:00:00.000Z");
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const daysAgo = (n: number) => new Date(NOW.getTime() - n * MS_PER_DAY);
const silentLog = { info: () => {}, warn: () => {}, error: () => {} };

function cfg(over: Partial<HeartbeatResultRetentionConfig> = {}): HeartbeatResultRetentionConfig {
  return {
    retentionDays: 30,
    // Small caps so fixtures stay readable. A jsonb value under the ~2KB TOAST
    // threshold is stored uncompressed, so pg_column_size tracks raw size here
    // and a few hundred characters is reliably "oversized" for these tests.
    maxBytes: 200,
    keepOutputChars: 10,
    batchSize: 50,
    itemLimit: 100,
    ...over,
  };
}

describe("heartbeat result retention sweep loop", () => {
  it("walks the cursor past rows the update skips instead of re-selecting them forever", async () => {
    // The regression this guards: a row that is oversized for a reason the
    // sweeper cannot fix (a large field that is neither stdout nor stderr) is
    // never modified, so a plain LIMIT-only selector returns the same page on
    // every iteration and the sweep spins until itemLimit.
    //
    // Scope: this exercises the LOOP, against a fake that resolves the cursor
    // exactly. That the real driver's cursor IS exact — the other half of the
    // same guarantee — is covered against real Postgres by "pages forward ...
    // on rows carrying microsecond timestamps".
    const residue = Array.from({ length: 3 }, (_, i) => ({
      id: `res-${i}`,
      createdAtText: new Date(NOW.getTime() - (10 - i) * 1000).toISOString(),
    }));
    const seenCursors: Array<string | null> = [];
    const db: HeartbeatResultRetentionDb = {
      async selectOversizedPage({ cursor, limit }) {
        seenCursors.push(cursor?.id ?? null);
        const start = cursor ? residue.findIndex((r) => r.id === cursor.id) + 1 : 0;
        return residue.slice(start, start + limit);
      },
      // Nothing is trimmable: every candidate is residue.
      async trimResultJson() {
        return 0;
      },
    };

    const retention = createHeartbeatResultRetention({
      db,
      config: cfg({ batchSize: 1 }),
      now: () => NOW,
      log: silentLog,
    });
    const result = await retention.runSweep();

    expect(result.examined).toBe(3);
    expect(result.trimmed).toBe(0);
    expect(result.residue).toBe(3);
    // Terminated on an empty page, not by exhausting itemLimit.
    expect(result.batches).toBe(3);
    expect(seenCursors).toEqual([null, "res-0", "res-1", "res-2"]);
  });

  it("skips a tick while a previous sweep is still running", async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let selectCalls = 0;
    const db: HeartbeatResultRetentionDb = {
      async selectOversizedPage() {
        selectCalls += 1;
        await gate;
        return [];
      },
      async trimResultJson() {
        return 0;
      },
    };

    const retention = createHeartbeatResultRetention({
      db,
      config: cfg(),
      now: () => NOW,
      log: silentLog,
    });

    const first = retention.runSweep();
    const second = await retention.runSweep();
    expect(second.skipped).toBe(true);
    expect(second.reason).toBe("already_running");

    release();
    const firstResult = await first;
    expect(firstResult.skipped).toBe(false);
    expect(selectCalls).toBe(1);

    // The guard releases, so a later tick runs normally.
    const third = await retention.runSweep();
    expect(third.skipped).toBe(false);
  });

  it("stops at itemLimit batches", async () => {
    const db: HeartbeatResultRetentionDb = {
      async selectOversizedPage({ cursor }) {
        const n = cursor ? Number(cursor.id) + 1 : 0;
        return [{ id: String(n), createdAtText: new Date(NOW.getTime() - 1000).toISOString() }];
      },
      async trimResultJson() {
        return 1;
      },
    };
    const retention = createHeartbeatResultRetention({
      db,
      config: cfg({ batchSize: 1, itemLimit: 5 }),
      now: () => NOW,
      log: silentLog,
    });
    const result = await retention.runSweep();
    expect(result.batches).toBe(5);
    expect(result.examined).toBe(5);
  });
});

describe("heartbeat result retention config", () => {
  it("keeps enough headroom that a trimmed row clears the size gate", () => {
    // A trimmed row costs at most both outputs at the cap plus the stamped
    // metadata. If that ever approached maxBytes, every trimmed row would stay
    // a permanent candidate and the sweep would re-detoast it forever. Asserted
    // against the real constants so a future edit to either cannot quietly
    // erase the margin.
    //
    // The cap counts CHARACTERS (`left()`/`length()`) but the gate counts
    // BYTES (`pg_column_size`), so the worst case is every kept character being
    // a 4-byte one. Asserting 4096 chars as 4096 bytes would understate the
    // real figure fourfold and let a future edit slip through.
    const MAX_UTF8_BYTES_PER_CHAR = 4;
    const STAMPED_METADATA_BYTES = 512; // generous upper bound for the six keys
    const worstCaseTrimmedBytes =
      2 * HEARTBEAT_RUN_RESULT_OUTPUT_MAX_CHARS * MAX_UTF8_BYTES_PER_CHAR + STAMPED_METADATA_BYTES;
    // 33,280 against a 65,536 gate: a 1.97x margin, and that is the pessimistic
    // reading twice over, since the gate measures the COMPRESSED stored size
    // while this counts raw bytes. Doubling either constant breaks this.
    expect(worstCaseTrimmedBytes).toBeLessThan(HEARTBEAT_RUN_SAFE_RESULT_JSON_MAX_BYTES);
  });

  it("reads its window and batching from config", () => {
    const resolved = resolveHeartbeatResultRetentionConfig({
      runResultRetentionDays: 45,
      runResultRetentionBatchSize: 25,
      runResultRetentionItemLimit: 7,
    } as never);
    expect(resolved.retentionDays).toBe(45);
    expect(resolved.batchSize).toBe(25);
    expect(resolved.itemLimit).toBe(7);
    // The size gate and output cap are deliberately NOT configurable: they must
    // agree with the API's own projection of an oversized row.
    expect(resolved.maxBytes).toBe(HEARTBEAT_RUN_SAFE_RESULT_JSON_MAX_BYTES);
    expect(resolved.keepOutputChars).toBe(HEARTBEAT_RUN_RESULT_OUTPUT_MAX_CHARS);
  });
});

describe("PAPERCLIP_RUN_RESULT_RETENTION_ENABLED", () => {
  const KEY = "PAPERCLIP_RUN_RESULT_RETENTION_ENABLED";
  let saved: string | undefined;

  beforeEach(() => {
    saved = process.env[KEY];
  });

  afterEach(() => {
    if (saved === undefined) delete process.env[KEY];
    else process.env[KEY] = saved;
  });

  it("is on when unset", () => {
    delete process.env[KEY];
    expect(loadConfig().runResultRetentionEnabled).toBe(true);
  });

  it("is off only for the exact string \"false\"", () => {
    process.env[KEY] = "false";
    expect(loadConfig().runResultRetentionEnabled).toBe(false);
  });

  it("stays on for a typo'd value rather than silently disabling itself", () => {
    // The failure mode this guards: `!== "true"` would turn "False", "0", or a
    // stray space into a silent opt-out, and nobody notices a sweeper that
    // never runs until the table is 7GB again.
    for (const value of ["False", "FALSE", "0", "no", " false", ""]) {
      process.env[KEY] = value;
      expect(loadConfig().runResultRetentionEnabled).toBe(true);
    }
  });
});

describe("PAPERCLIP_RUN_RESULT_RETENTION_BATCH_SIZE", () => {
  const KEY = "PAPERCLIP_RUN_RESULT_RETENTION_BATCH_SIZE";
  // PostgreSQL's wire protocol caps a statement at 65,535 bind parameters, and
  // the trim UPDATE binds one per candidate id.
  const PG_MAX_BIND_PARAMS = 65_535;
  let saved: string | undefined;

  beforeEach(() => {
    saved = process.env[KEY];
  });

  afterEach(() => {
    if (saved === undefined) delete process.env[KEY];
    else process.env[KEY] = saved;
  });

  it("clamps a huge value below the bind-parameter limit instead of wedging the sweeper", () => {
    // The failure this guards is not a slow sweep, it is a permanently stuck
    // one. Over the bind-parameter limit every trim throws; the cursor is
    // function-local so the throw discards it, and the next tick restarts from
    // the beginning and dies at the same point — an error in the log every 24h
    // and nothing trimmed, forever, while the table keeps growing.
    process.env[KEY] = "100000";
    const batchSize = loadConfig().runResultRetentionBatchSize;
    expect(batchSize).toBeLessThan(PG_MAX_BIND_PARAMS);
    // Clamped to something useful rather than reset to the default: an operator
    // who asked for large batches still gets them.
    expect(batchSize).toBeGreaterThan(200);
  });

  it("leaves ordinary values alone", () => {
    process.env[KEY] = "500";
    expect(loadConfig().runResultRetentionBatchSize).toBe(500);
  });

  it("still floors at 1, so a zero or negative value cannot stall the walk", () => {
    for (const value of ["0", "-5"]) {
      process.env[KEY] = value;
      expect(loadConfig().runResultRetentionBatchSize).toBe(1);
    }
  });
});

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres heartbeat-result-retention tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
  );
}

// The SQL is the whole risk surface here: jsonb_set_lax NULL handling, the
// STRICT-jsonb_set trap, type coercion of non-string values, timestamp binding,
// and uuid array binding all fail only against a real server.
describeEmbeddedPostgres("createDrizzleHeartbeatResultRetentionDb (real Postgres)", () => {
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;

  beforeEach(async () => {
    database = await startEmbeddedPostgresTestDatabase("paperclip-heartbeat-result-retention-");
    db = createDb(database.connectionString);
  });

  afterEach(async () => {
    await database.cleanup();
  });

  async function seedCompanyAndAgent(): Promise<{ companyId: string; agentId: string }> {
    const [company] = await db.insert(companies).values({ name: "co-1" }).returning({ id: companies.id });
    const [agent] = await db
      .insert(agents)
      .values({ companyId: company!.id, name: "agent-1" })
      .returning({ id: agents.id });
    return { companyId: company!.id, agentId: agent!.id };
  }

  async function seedRun(over: {
    id: string;
    companyId: string;
    agentId: string;
    /**
     * Omit to let the schema's `defaultNow()` fire. That is the ONLY way to get
     * a row with the microsecond precision production rows actually carry — a
     * JS `Date` can only express milliseconds, so seeding one produces a
     * `.000000` timestamp that round-trips losslessly and hides any
     * cursor-precision bug. Pair it with `ageRuns` to move the row into the past.
     */
    createdAt?: Date;
    resultJson: Record<string, unknown> | null;
  }) {
    await db.insert(heartbeatRuns).values({
      id: over.id,
      companyId: over.companyId,
      agentId: over.agentId,
      status: "succeeded",
      ...(over.createdAt ? { createdAt: over.createdAt } : {}),
      resultJson: over.resultJson,
    });
  }

  /**
   * Move seeded runs to `days` before the fixed NOW, independent of the host
   * clock. One whole-second shift, anchored on the newest row, is applied to
   * every row, so each keeps its sub-second digits and their relative order.
   */
  async function ageRuns(ids: string[], days: number) {
    const idArray = sql`ARRAY[${sql.join(
      ids.map((id) => sql`${id}`),
      sql`, `,
    )}]::uuid[]`;
    await db.execute(
      sql`update heartbeat_runs
          set created_at = created_at + (
            ${daysAgo(days).toISOString()}::timestamptz
            - (select date_trunc('second', max(created_at)) from heartbeat_runs where id = any(${idArray}))
          )
          where id = any(${idArray})`,
    );
  }

  async function readRun(id: string) {
    const [row] = await db
      .select({
        resultJson: heartbeatRuns.resultJson,
        updatedAt: heartbeatRuns.updatedAt,
      })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, id));
    return row!;
  }

  const uuid = (n: number) => `${String(n).repeat(8)}-${String(n).repeat(4)}-${String(n).repeat(4)}-${String(n).repeat(4)}-${String(n).repeat(12)}`;

  it("trims oversized stdout/stderr while preserving every other key", async () => {
    const { companyId, agentId } = await seedCompanyAndAgent();
    const id = uuid(1);
    await seedRun({
      id,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: {
        sessionId: "session-abc",
        retryNotBefore: "2026-09-01T00:00:00.000Z",
        summary: "completed",
        stdout: "A".repeat(500),
        stderr: "B".repeat(500),
      },
    });

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    const page = await retentionDb.selectOversizedPage({
      cutoff: daysAgo(30),
      maxBytes: 200,
      limit: 10,
      cursor: null,
    });
    expect(page.map((r) => r.id)).toEqual([id]);

    const trimmed = await retentionDb.trimResultJson({ ids: [id], keepOutputChars: 10, maxBytes: 200 });
    expect(trimmed).toBe(1);

    const after = (await readRun(id)).resultJson as Record<string, unknown>;
    // The reads that made nulling the column unsafe still resolve.
    expect(after.sessionId).toBe("session-abc");
    expect(after.retryNotBefore).toBe("2026-09-01T00:00:00.000Z");
    expect(after.summary).toBe("completed");
    expect(after.stdout).toBe("A".repeat(10));
    expect(after.stderr).toBe("B".repeat(10));
    expect(after.truncated).toBe(true);
    expect(after.truncationReason).toBe("retention_trimmed");
    expect(after.stdoutTruncated).toBe(true);
    expect(after.stderrTruncated).toBe(true);
    expect(typeof after.originalSizeBytes).toBe("number");
    expect(after.originalSizeBytes as number).toBeGreaterThan(200);
  });

  it("leaves non-string and short output fields untouched and reports them as not truncated", async () => {
    const { companyId, agentId } = await seedCompanyAndAgent();
    const id = uuid(2);
    await seedRun({
      id,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: {
        // An object here must not be coerced to a string by the trim.
        stdout: { nested: true, blob: "C".repeat(400) },
        stderr: "short",
        sessionId: "session-xyz",
      },
    });

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    // Nothing is trimmable, so the row is residue: selected, but not updated.
    const page = await retentionDb.selectOversizedPage({
      cutoff: daysAgo(30),
      maxBytes: 200,
      limit: 10,
      cursor: null,
    });
    expect(page.map((r) => r.id)).toEqual([id]);

    const trimmed = await retentionDb.trimResultJson({ ids: [id], keepOutputChars: 10, maxBytes: 200 });
    expect(trimmed).toBe(0);

    const after = (await readRun(id)).resultJson as Record<string, unknown>;
    expect(after.stdout).toEqual({ nested: true, blob: "C".repeat(400) });
    expect(after.stderr).toBe("short");
    // Untouched means unstamped.
    expect(after.truncated).toBeUndefined();
  });

  it("keeps a JSON-null stdout as null rather than nulling the whole document", async () => {
    // jsonb_set is STRICT: a NULL new_value returns NULL for the entire
    // document, which would wipe sessionId and the retry bookkeeping. The
    // jsonb_set_lax + coalesce pair is what prevents that.
    const { companyId, agentId } = await seedCompanyAndAgent();
    const id = uuid(3);
    await seedRun({
      id,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: { stdout: null, stderr: "D".repeat(500), sessionId: "session-null" },
    });

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    expect(await retentionDb.trimResultJson({ ids: [id], keepOutputChars: 10, maxBytes: 200 })).toBe(1);

    const after = (await readRun(id)).resultJson as Record<string, unknown>;
    expect(after).not.toBeNull();
    expect(after.sessionId).toBe("session-null");
    expect(after.stdout).toBeNull();
    expect(after.stderr).toBe("D".repeat(10));
    expect(after.stdoutTruncated).toBe(false);
    expect(after.stderrTruncated).toBe(true);
  });

  it("does not bump updated_at", async () => {
    const { companyId, agentId } = await seedCompanyAndAgent();
    const id = uuid(4);
    await seedRun({
      id,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: { stdout: "E".repeat(500), sessionId: "s" },
    });
    const before = await readRun(id);

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    expect(await retentionDb.trimResultJson({ ids: [id], keepOutputChars: 10, maxBytes: 200 })).toBe(1);

    const after = await readRun(id);
    expect(after.updatedAt!.getTime()).toBe(before.updatedAt!.getTime());
  });

  it("respects the retention window and the size gate", async () => {
    const { companyId, agentId } = await seedCompanyAndAgent();
    const oldBig = uuid(5);
    const freshBig = uuid(6);
    const oldSmall = uuid(7);
    await seedRun({
      id: oldBig,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: { stdout: "F".repeat(500) },
    });
    await seedRun({
      id: freshBig,
      companyId,
      agentId,
      createdAt: daysAgo(2),
      resultJson: { stdout: "G".repeat(500) },
    });
    await seedRun({
      id: oldSmall,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: { stdout: "tiny" },
    });

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    // A plain Date at the port boundary: the raw sql template casts it via
    // toISOString()::timestamptz, because binding a bare Date throws in
    // postgres-js (the same scar the run-log archiver carries).
    const page = await retentionDb.selectOversizedPage({
      cutoff: daysAgo(30),
      maxBytes: 200,
      limit: 10,
      cursor: null,
    });
    expect(page.map((r) => r.id)).toEqual([oldBig]);
  });

  it("pages forward with the (created_at, id) cursor on rows carrying microsecond timestamps", async () => {
    // The rows are seeded through the schema default and then aged, so their
    // timestamps keep the microseconds `now()` gave them — exactly like
    // production, where 1997 of 2000 sampled rows have a non-zero
    // sub-millisecond component.
    //
    // This is the regression that matters: carry the cursor through a JS `Date`
    // and `toISOString()` truncates it below the row it came from, so
    // `(created_at, id) > (cursor)` stays true for that row and it heads every
    // subsequent page forever. Seeding a plain `Date` here would give a
    // `.000000` timestamp, round-trip losslessly, and prove nothing.
    const { companyId, agentId } = await seedCompanyAndAgent();
    const ids = [uuid(1), uuid(2), uuid(3)];
    for (const id of ids) {
      await seedRun({ id, companyId, agentId, resultJson: { stdout: "H".repeat(500) } });
    }
    // The database default uses the host clock while the fixture clock is
    // fixed; re-anchor the rows 60 days before NOW so they sit before the
    // fixed 30-day cutoff on any host date, keeping their microseconds.
    await ageRuns(ids, 60);

    const [{ subMs }] = (await db.execute(sql`
      select count(*) filter (
        where (date_part('microsecond', created_at)::bigint % 1000) <> 0
      )::int as "subMs"
      from heartbeat_runs
      where id = any(${sql`ARRAY[${sql.join(
        ids.map((id) => sql`${id}`),
        sql`, `,
      )}]::uuid[]`})
    `)) as unknown as Array<{ subMs: number }>;
    // Guards the guard: if the fixture ever stops producing microseconds this
    // test silently goes back to proving nothing.
    expect(subMs).toBe(ids.length);

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    const seen: string[] = [];
    let cursor: { createdAtText: string; id: string } | null = null;
    for (let i = 0; i < 5; i += 1) {
      const page = await retentionDb.selectOversizedPage({
        cutoff: daysAgo(30),
        maxBytes: 200,
        limit: 1,
        cursor,
      });
      if (page.length === 0) break;
      seen.push(...page.map((r) => r.id));
      const last = page[page.length - 1]!;
      cursor = { createdAtText: last.createdAtText, id: last.id };
    }

    // Four pages of one row, then an empty page. A truncating cursor returns
    // the third row on every iteration and never yields an empty page at all.
    expect(seen).toEqual(ids);
  });

  it("trims only the ids it is given", async () => {
    const { companyId, agentId } = await seedCompanyAndAgent();
    const target = uuid(1);
    const bystander = uuid(2);
    for (const id of [target, bystander]) {
      await seedRun({
        id,
        companyId,
        agentId,
        createdAt: daysAgo(40),
        resultJson: { stdout: "I".repeat(500) },
      });
    }

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    expect(await retentionDb.trimResultJson({ ids: [target], keepOutputChars: 10, maxBytes: 200 })).toBe(1);

    expect(((await readRun(target)).resultJson as Record<string, unknown>).stdout).toBe("I".repeat(10));
    expect(((await readRun(bystander)).resultJson as Record<string, unknown>).stdout).toBe("I".repeat(500));
  });

  it("is a no-op for an empty id list", async () => {
    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    expect(await retentionDb.trimResultJson({ ids: [], keepOutputChars: 10, maxBytes: 200 })).toBe(0);
  });

  it("is idempotent: a second sweep drops the row from the candidate set", async () => {
    // The trim must leave the row small enough to clear the size gate, or it
    // stays a candidate forever (harmless — the cursor still guarantees
    // progress — but it re-detoasts the row on every sweep for nothing).
    // Headroom is what buys that: the stamped metadata costs ~200 bytes on top
    // of the kept output, so the gate has to exceed
    // 2 * keepOutputChars + metadata. Production clears it by ~8x; see the
    // config invariant test below. Fixture stays under the ~2KB TOAST
    // threshold so pg_column_size reflects raw size and repetitive filler is
    // not compressed away.
    const { companyId, agentId } = await seedCompanyAndAgent();
    const id = uuid(1);
    await seedRun({
      id,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: { stdout: "J".repeat(1500), sessionId: "s" },
    });

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    const retention = createHeartbeatResultRetention({
      db: retentionDb,
      config: cfg({ maxBytes: 600, keepOutputChars: 10 }),
      now: () => NOW,
      log: silentLog,
    });

    const first = await retention.runSweep();
    expect(first.examined).toBe(1);
    expect(first.trimmed).toBe(1);

    const second = await retention.runSweep();
    expect(second.examined).toBe(0);
    expect(second.trimmed).toBe(0);
  });

  it("leaves a row that is oversized for another reason as stable residue", async () => {
    // Complement to the idempotency case: here the trim cannot bring the row
    // under the gate because the bulk is elsewhere. It must stay residue —
    // counted, never rewritten twice, and never blocking the cursor.
    const { companyId, agentId } = await seedCompanyAndAgent();
    const id = uuid(2);
    await seedRun({
      id,
      companyId,
      agentId,
      createdAt: daysAgo(40),
      resultJson: { stdout: "K".repeat(300), nestedHuge: { blob: "L".repeat(1200) } },
    });

    const retentionDb = createDrizzleHeartbeatResultRetentionDb(db);
    const retention = createHeartbeatResultRetention({
      db: retentionDb,
      config: cfg({ maxBytes: 600, keepOutputChars: 10 }),
      now: () => NOW,
      log: silentLog,
    });

    const first = await retention.runSweep();
    expect(first.trimmed).toBe(1);

    // Still a candidate, but nothing further to do: pure residue, and the
    // sweep still terminates rather than spinning on it.
    const second = await retention.runSweep();
    expect(second.examined).toBe(1);
    expect(second.trimmed).toBe(0);
    expect(second.residue).toBe(1);

    const after = (await readRun(id)).resultJson as Record<string, unknown>;
    expect(after.nestedHuge).toEqual({ blob: "L".repeat(1200) });
  });
});
