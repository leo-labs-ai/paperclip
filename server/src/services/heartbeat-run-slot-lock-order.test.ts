import { describe, expect, it, vi } from "vitest";
import { PgDialect } from "drizzle-orm/pg-core";
import { acquireRunSlotLock, admitRunSlotInTransaction } from "./heartbeat-run-slot.js";
import { sqlMillisInterval } from "./run-ownership-store.js";

const dialect = new PgDialect();

describe("sqlMillisInterval", () => {
  it("binds the duration as a parameter instead of splicing it into the SQL text", () => {
    const query = dialect.sqlToQuery(sqlMillisInterval(90_000));
    expect(query.sql).toContain("interval '1 millisecond'");
    expect(query.sql).not.toContain("90000");
    expect(query.params).toEqual([90_000]);
  });

  it("normalises fractional, negative and non-finite input", () => {
    expect(dialect.sqlToQuery(sqlMillisInterval(1500.9)).params).toEqual([1500]);
    expect(dialect.sqlToQuery(sqlMillisInterval(-5)).params).toEqual([0]);
    expect(dialect.sqlToQuery(sqlMillisInterval(Number.NaN)).params).toEqual([0]);
  });
});

describe("run slot lock order", () => {
  function recordingTx(maxRunning = 0) {
    const statements: string[] = [];
    const tx = {
      execute: vi.fn(async (query: unknown) => {
        statements.push(dialect.sqlToQuery(query as never).sql);
        return [];
      }),
      select: vi.fn(() => ({
        from: () => ({ where: () => Promise.resolve([{ count: maxRunning }]) }),
      })),
    };
    return { tx: tx as never, statements, execute: tx.execute };
  }

  it("acquireRunSlotLock takes the per-agent advisory lock with a single statement", async () => {
    const { tx, statements } = recordingTx();
    await acquireRunSlotLock(tx, "agent-1");
    expect(statements).toHaveLength(1);
    expect(statements[0]).toContain("pg_advisory_xact_lock");
  });

  it("admission re-acquires the same lock key, so an early acquire makes it re-entrant", async () => {
    const early = recordingTx();
    const late = recordingTx();
    await acquireRunSlotLock(early.tx, "agent-1");
    const decision = await admitRunSlotInTransaction(late.tx, { runId: "r", agentId: "agent-1", maxConcurrentRuns: 1 });
    expect(decision).toEqual({ admitted: true });
    expect(late.statements[0]).toBe(early.statements[0]);
    expect(late.execute).toHaveBeenCalled();
  });

  it("denies admission at the concurrency cap", async () => {
    const { tx } = recordingTx(2);
    expect(await admitRunSlotInTransaction(tx, { runId: "r", agentId: "a", maxConcurrentRuns: 2 }))
      .toEqual({ admitted: false, dailyCapBlock: null });
  });
});
