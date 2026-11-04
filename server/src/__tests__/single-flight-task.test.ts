import { describe, expect, it } from "vitest";
import { createSingleFlightTask } from "../lib/single-flight-task.js";

// Seam: scheduler work admission and its drainable promise, without DB or timer mocks.
function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

describe("single-flight scheduler task", () => {
  it("admits only one pass, skips rather than queues busy ticks, and admits the next tick", async () => {
    const run = createSingleFlightTask();
    const pending = deferred();
    let calls = 0;
    const work = () => { calls++; return pending.promise; };
    const first = run(work);
    expect(first).toBeInstanceOf(Promise);
    for (let tick = 0; tick < 10; tick++) expect(run(work)).toBeUndefined();
    await Promise.resolve();
    expect(calls).toBe(1);
    pending.resolve();
    await first;
    expect(calls).toBe(1);
    await run(work);
    expect(calls).toBe(2);
  });

  it("releases admission after an async rejection and preserves the error for the tracker", async () => {
    const run = createSingleFlightTask();
    const pending = deferred();
    const error = new Error("sweep failed");
    const first = run(() => pending.promise);
    const rejected = expect(first).rejects.toBe(error);
    expect(run(() => Promise.resolve())).toBeUndefined();
    pending.reject(error);
    await rejected;
    await expect(run(async () => "recovered")).resolves.toBe("recovered");
  });

  it("releases admission after a synchronous throw", async () => {
    const run = createSingleFlightTask();
    const error = new Error("synchronous failure");
    await expect(run(() => { throw error; })).rejects.toBe(error);
    await expect(run(async () => 42)).resolves.toBe(42);
  });

  it("reserves admission before invoking work, including reentrant starts", async () => {
    const run = createSingleFlightTask();
    await run(async () => {
      expect(run(async () => { throw new Error("must not start"); })).toBeUndefined();
    });
  });

  it("keeps independent maintenance lanes independent", async () => {
    const recovery = createSingleFlightTask();
    const retention = createSingleFlightTask();
    const pending = deferred();
    const recoveryWork = recovery(() => pending.promise);
    await expect(retention(async () => "retained")).resolves.toBe("retained");
    expect(recovery(async () => undefined)).toBeUndefined();
    pending.resolve();
    await recoveryWork;
  });

  it("returns the actual in-flight promise for shutdown to drain, with no hidden follow-up", async () => {
    const run = createSingleFlightTask();
    const pending = deferred();
    const tracked: Promise<unknown>[] = [];
    let calls = 0;
    const schedule = () => {
      const work = run(async () => { calls++; await pending.promise; });
      if (work) tracked.push(work);
    };
    schedule();
    schedule();
    let drained = false;
    const draining = Promise.allSettled(tracked).then(() => { drained = true; });
    await Promise.resolve();
    expect(drained).toBe(false);
    expect(tracked).toHaveLength(1);
    pending.resolve();
    await draining;
    expect(drained).toBe(true);
    expect(calls).toBe(1);
  });

  it("releases after a suppressed or stopped no-op pass", async () => {
    const run = createSingleFlightTask();
    await run(async () => undefined);
    await expect(run(async () => "next tick")).resolves.toBe("next tick");
  });
});
