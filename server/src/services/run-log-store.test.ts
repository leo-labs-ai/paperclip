import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { promises as fs } from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { createLocalFileRunLogStore } from "./run-log-store.js";

const beginInput = { companyId: "co1", agentId: "ag1", runId: "run1" };
let baseDir: string;
let previousCompressionSetting: string | undefined;

beforeEach(async () => {
  baseDir = await fs.mkdtemp(path.join(os.tmpdir(), "run-log-store-test-"));
  previousCompressionSetting = process.env.PAPERCLIP_RUN_LOG_COMPRESS;
  process.env.PAPERCLIP_RUN_LOG_COMPRESS = "true";
});

afterEach(async () => {
  await fs.rm(baseDir, { recursive: true, force: true });
  if (previousCompressionSetting === undefined) delete process.env.PAPERCLIP_RUN_LOG_COMPRESS;
  else process.env.PAPERCLIP_RUN_LOG_COMPRESS = previousCompressionSetting;
  vi.restoreAllMocks();
});

describe("createLocalFileRunLogStore retry and finalization behavior", () => {
  it("retains every attempt in the compressed log when a run retries", async () => {
    const store = createLocalFileRunLogStore(baseDir);
    const attemptIds: string[] = [];

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const handle = await store.begin(beginInput);
      expect(handle.attemptId).toBeTruthy();
      attemptIds.push(handle.attemptId!);
      await store.append(handle, {
        stream: "stderr",
        chunk: `attempt-${attempt}`,
        ts: "same-time",
        seq: attempt,
      });

      const summary = await store.finalize(handle);
      expect(summary.bytes).not.toBeNull();
      expect(summary.compressed).toBe(true);
      expect(summary.logRef).toBe(`${handle.logRef}.gz`);

      const lines = (await store.read(handle)).content
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { chunk: string; attemptId: string });
      expect(lines.map((line) => line.chunk)).toEqual(
        Array.from({ length: attempt }, (_, index) => `attempt-${index + 1}`),
      );
      expect(lines.map((line) => line.attemptId)).toEqual(attemptIds);
      expect(new Set(attemptIds).size).toBe(attempt);
    }
  });

  it.each(["finishes", "fails"] as const)(
    "waits for an accepted append that %s before finalizing the log",
    async (outcome) => {
      const store = createLocalFileRunLogStore(baseDir);
      const handle = await store.begin(beginInput);
      let markAppendEntered!: () => void;
      let releaseAppend!: () => void;
      const appendEntered = new Promise<void>((resolve) => { markAppendEntered = resolve; });
      const appendGate = new Promise<void>((resolve) => { releaseAppend = resolve; });
      const appendFile = fs.appendFile.bind(fs);
      const appendFileSpy = vi.spyOn(fs, "appendFile").mockImplementationOnce(async (...args) => {
        markAppendEntered();
        await appendGate;
        if (outcome === "fails") throw new Error("disk unavailable");
        await appendFile(...args);
      });

      const append = store.append(handle, {
        stream: "stderr",
        chunk: "accepted diagnostic",
        ts: "t1",
      });
      void append.catch(() => undefined);
      await appendEntered;

      let finalized = false;
      const finalize = store.finalize(handle).then((summary) => {
        finalized = true;
        return summary;
      });
      try {
        expect(finalized).toBe(false);
        releaseAppend();
        if (outcome === "fails") await expect(append).rejects.toThrow("disk unavailable");
        else await append;

        const summary = await finalize;
        const read = await store.read(handle);
        expect(summary.bytes).toBe(Buffer.byteLength(read.content, "utf8"));
        expect(summary.sha256).toBe(createHash("sha256").update(read.content).digest("hex"));
        expect(read.content).toBe(
          outcome === "fails"
            ? ""
            : `${JSON.stringify({
                ts: "t1",
                attemptId: handle.attemptId,
                stream: "stderr",
                chunk: "accepted diagnostic",
              })}\n`,
        );
        expect(appendFileSpy).toHaveBeenCalledOnce();
      } finally {
        releaseAppend();
        await append.catch(() => undefined);
        await finalize;
      }
    },
  );

  it("rejects appends admitted after finalization starts", async () => {
    const store = createLocalFileRunLogStore(baseDir);
    const handle = await store.begin(beginInput);
    await store.append(handle, { stream: "stdout", chunk: "kept", ts: "t1" });

    const finalize = store.finalize(handle);
    await expect(store.append(handle, { stream: "stderr", chunk: "too late", ts: "t2" })).resolves.toBe(0);
    const summary = await finalize;
    const read = await store.read(handle);

    expect(summary.bytes).toBe(Buffer.byteLength(read.content, "utf8"));
    expect(read.content).toContain("kept");
    expect(read.content).not.toContain("too late");
  });
});
