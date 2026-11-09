import { createReadStream, createWriteStream, promises as fs } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { createGzip, createGunzip } from "node:zlib";
import { pipeline } from "node:stream/promises";
import { addAbortSignal } from "node:stream";
import type { Readable } from "node:stream";
import { createS3StorageProvider } from "../storage/s3-provider.js";
import type { StorageProvider } from "../storage/types.js";
import { notFound } from "../errors.js";
import { resolvePaperclipInstanceRoot } from "../home-paths.js";
import { logger } from "../middleware/logger.js";
import {
  getRunLogArchiveObjectStore,
  getRunLogArchiveStorageProvider,
} from "../storage/index.js";

/**
 * Storage tier a run-log currently lives in.
 *  - `local_file`: hot copy on the shared volume (may be raw `.ndjson` or `.ndjson.gz`).
 *  - `s3`: cold copy archived to object storage; `logRef` is the full S3 object key.
 *  - `missing`: hot copy is gone and was never archived (e.g. purged during an
 *    ENOSPC incident). Reads 404; the archiver sets this so dead rows leave the
 *    sweep candidate pool instead of burning the failure budget every sweep.
 * The DB column is the source of truth; the archiver flips it local_file → s3
 * (or local_file → missing when the hot file has vanished).
 */
export type RunLogStoreType = "local_file" | "s3" | "missing";

/** Object-key namespace for archived run-logs: `<companyId>/run-logs/<agentId>/<runId>.ndjson.gz`. */
export const RUN_LOG_OBJECT_NAMESPACE = "run-logs";

/** Minimal storage surface the store needs to stream an archived (s3) run-log back. */
export interface RunLogS3Reader {
  getObject(input: { objectKey: string; signal?: AbortSignal }): Promise<{ stream: Readable }>;
}

export interface CreateRunLogStoreOptions {
  /** When set, `read()` can resolve archived `store: "s3"` handles. */
  s3Reader?: RunLogS3Reader | null;
}

export interface DurableRunLogStoreOptions extends CreateRunLogStoreOptions {
  basePath: string;
  // Mirrors active and finalized logs to object storage. This is separate from
  // the fork's cold-archive reader, which serves compressed archived handles.
  s3?: {
    provider: StorageProvider;
    keyPrefix?: string;
    inflightMirrorMs?: number;
  };
}

export interface RunLogHandle {
  store: RunLogStoreType;
  logRef: string;
  attemptId?: string;
}

export interface RunLogReadOptions {
  offset?: number;
  limitBytes?: number;
  signal?: AbortSignal;
}

export interface RunLogReadResult {
  content: string;
  nextOffset?: number;
}

export interface RunLogFinalizeSummary {
  /** Byte length of the ORIGINAL, uncompressed log content; null if writes did not settle. */
  bytes: number | null;
  /** sha256 of the ORIGINAL, uncompressed log content; null if writes did not settle. */
  sha256?: string | null;
  compressed: boolean;
  /** Final relative logRef to persist — `.ndjson.gz` when compressed, else `.ndjson`. */
  logRef: string;
}

export interface RunLogStore {
  begin(input: { companyId: string; agentId: string; runId: string }): Promise<RunLogHandle>;
  append(
    handle: RunLogHandle,
    event: { stream: "stdout" | "stderr" | "system"; chunk: string; ts: string; seq?: number },
  ): Promise<number>;
  finalize(handle: RunLogHandle): Promise<RunLogFinalizeSummary>;
  read(handle: RunLogHandle, opts?: RunLogReadOptions): Promise<RunLogReadResult>;
  flushInflightMirrors?(): Promise<void>;
}

function safeSegments(...segments: string[]) {
  return segments.map((segment) => segment.replace(/[^a-zA-Z0-9._-]/g, "_"));
}

function normalizeKeyPrefix(prefix: string | undefined): string {
  if (!prefix) return "";
  return prefix.trim().split("/").filter(Boolean).join("/");
}

function attachAbortSignal(stream: Readable, signal?: AbortSignal): Readable {
  return signal ? addAbortSignal(signal, stream) : stream;
}

function resolveWithin(basePath: string, relativePath: string) {
  const resolved = path.resolve(basePath, relativePath);
  const base = path.resolve(basePath) + path.sep;
  if (!resolved.startsWith(base) && resolved !== path.resolve(basePath)) {
    throw new Error("Invalid log path");
  }
  return resolved;
}

/**
 * Compression is on by default. Set `PAPERCLIP_RUN_LOG_COMPRESS=0` (or `false`)
 * to keep completed run-logs as raw `.ndjson`. Read at finalize time so tests
 * and operators can toggle it without a restart.
 */
function compressionEnabled(): boolean {
  const raw = process.env.PAPERCLIP_RUN_LOG_COMPRESS;
  if (raw == null) return true;
  const normalized = raw.trim().toLowerCase();
  return normalized !== "0" && normalized !== "false" && normalized !== "no" && normalized !== "off";
}

const DEFAULT_RUN_LOG_MAX_BYTES = 512 * 1024 * 1024; // 512 MiB

/**
 * Per-run cap on persisted (uncompressed) log bytes, so a single runaway run
 * cannot fill the shared volume between daily sweeps. Set
 * `PAPERCLIP_RUN_LOG_MAX_BYTES` to override; unset/invalid/<=0 falls back to
 * the 512 MiB default. Read per call so tests/operators can retune without a
 * restart.
 */
function runLogMaxBytes(): number {
  const raw = process.env.PAPERCLIP_RUN_LOG_MAX_BYTES;
  if (raw == null) return DEFAULT_RUN_LOG_MAX_BYTES;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_RUN_LOG_MAX_BYTES;
  return parsed;
}

const RUN_LOG_TRUNCATION_STREAM = "system" as const;

interface RunLogCapState {
  writtenBytes: number;
  truncated: boolean;
}

/**
 * Range-read a gzip stream. offset/limitBytes semantics are over the
 * UNCOMPRESSED byte stream. The whole payload is never buffered: the first
 * `offset` decompressed bytes are discarded, up to `limitBytes` bytes are
 * collected, then the pipeline is destroyed early. `nextOffset` is set only
 * when the underlying stream still had data past what we returned.
 *
 * `source` is any Readable of gzip bytes — a local `createReadStream` for the
 * hot tier, or an S3 object stream for the cold tier — so both tiers share one
 * decompression + slicing implementation.
 *
 * nextOffset divergence from the raw path: when the decompressed stream ends at
 * (or before) what we returned, `nextOffset` is left undefined — i.e. "done".
 * readFileRange instead clamps and can return nextOffset === size so a live tail
 * keeps polling. That is intentional: a finalized `.gz` is immutable, so "no
 * more bytes" is terminal and there is nothing to poll for. An offset at/beyond
 * uncompressed EOF therefore yields `{ content: "", nextOffset: undefined }`.
 */
export async function readGunzipRange(
  source: Readable,
  offset: number,
  limitBytes: number,
): Promise<RunLogReadResult> {
  if (limitBytes <= 0) {
    source.destroy();
    return { content: "", nextOffset: offset };
  }

  let skipped = 0; // uncompressed bytes discarded so far (up to offset)
  let collected = 0; // uncompressed bytes retained (up to limitBytes)
  let hasMore = false; // stream produced data beyond what we returned
  const chunks: Buffer[] = [];

  const gunzip = createGunzip();

  await new Promise<void>((resolve, reject) => {
    let finished = false;
    const finish = (err?: Error) => {
      if (finished) return;
      finished = true;
      // Stop the pipeline early once we have enough; ignore late errors.
      source.destroy();
      gunzip.destroy();
      if (err) reject(err);
      else resolve();
    };

    source.on("error", finish);
    gunzip.on("error", finish);
    gunzip.on("end", () => finish());

    gunzip.on("data", (raw: Buffer) => {
      let buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);

      // Discard bytes before `offset`.
      if (skipped < offset) {
        const toSkip = Math.min(offset - skipped, buf.length);
        skipped += toSkip;
        buf = buf.subarray(toSkip);
        if (buf.length === 0) return;
      }

      if (collected >= limitBytes) {
        // Already full; any further byte means there is more data.
        hasMore = true;
        finish();
        return;
      }

      const remaining = limitBytes - collected;
      if (buf.length <= remaining) {
        chunks.push(buf);
        collected += buf.length;
      } else {
        chunks.push(buf.subarray(0, remaining));
        collected += remaining;
        hasMore = true;
        finish();
      }
    });

    source.pipe(gunzip);
  });

  const content = Buffer.concat(chunks).toString("utf8");
  const nextOffset = hasMore ? offset + collected : undefined;
  return { content, nextOffset };
}

/**
 * Stream-gzip `srcAbs` → `destAbs` crash-safely (tmp → rename). Does NOT unlink
 * the source; callers decide when the raw file is safe to remove. Shared by
 * finalize (compress-on-complete) and the archiver (gzip legacy raw logs before
 * upload).
 */
export async function gzipFileToGz(srcAbs: string, destAbs: string): Promise<void> {
  const tmpAbs = `${destAbs}.tmp`;
  try {
    await pipeline(createReadStream(srcAbs), createGzip(), createWriteStream(tmpAbs));
    await fs.rename(tmpAbs, destAbs);
  } catch (err) {
    await fs.unlink(tmpAbs).catch(() => undefined);
    throw err;
  }
}

async function gunzipFileToRaw(srcAbs: string, destAbs: string): Promise<void> {
  const tmpAbs = `${destAbs}.tmp`;
  try {
    await pipeline(createReadStream(srcAbs), createGunzip(), createWriteStream(tmpAbs, { flags: "wx", mode: 0o600 }));
    await fs.rename(tmpAbs, destAbs);
    await fs.unlink(srcAbs);
  } catch (err) {
    await fs.unlink(tmpAbs).catch(() => undefined);
    throw err;
  }
}

/** Absolute base dir the default run-log store reads/writes (env-overridable). */
export function resolveRunLogBasePath(): string {
  return process.env.RUN_LOG_BASE_PATH ?? path.resolve(resolvePaperclipInstanceRoot(), "data", "run-logs");
}

function toPosixKey(relPath: string): string {
  return relPath.split(path.sep).join("/");
}

async function sha256File(filePath: string): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(filePath);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(hash.digest("hex")));
  });
}

export interface RunLogArchiveSource {
  /** Absolute path to the gzip file to upload. */
  absPath: string;
  /** Relative path (posix) under the base dir, e.g. `<companyId>/<agentId>/<runId>.ndjson.gz`. */
  relPath: string;
  /** Company-scoped durable key: `<companyId>/run-logs/<agentId>/<runId>.ndjson.gz`. */
  objectKey: string;
  /** On-disk (compressed) size in bytes. */
  bytes: number;
  /** SHA-256 of the exact compressed bytes uploaded. */
  sha256: string;
}

/**
 * Resolve the local file backing a run-log `logRef` and guarantee it is gzipped,
 * ready for cold-archive upload. Reuses the same drift tolerance as the read
 * path: a raw legacy `.ndjson` is compressed in place (streaming gzip, then the
 * raw file is unlinked) so only a `.ndjson.gz` is ever uploaded. Throws
 * `notFound` when nothing backs the ref on disk.
 */
export async function prepareRunLogArchiveSource(
  basePath: string,
  logRef: string,
): Promise<RunLogArchiveSource> {
  const direct = resolveWithin(basePath, logRef);
  const directStat = await fs.stat(direct).catch(() => null);

  let gzAbs: string;
  if (directStat && logRef.endsWith(".gz")) {
    gzAbs = direct;
  } else if (directStat) {
    // Raw `.ndjson` on disk under the exact ref: compress it, then drop the raw.
    gzAbs = `${direct}.gz`;
    await gzipFileToGz(direct, gzAbs);
    await fs.unlink(direct).catch(() => undefined);
  } else if (logRef.endsWith(".ndjson")) {
    // Ref points at raw, but only the `.gz` exists (compress crash-window / legacy row).
    const gzCandidate = resolveWithin(basePath, `${logRef}.gz`);
    if (!(await fs.stat(gzCandidate).catch(() => null))) throw notFound("Run log not found");
    gzAbs = gzCandidate;
  } else if (logRef.endsWith(".ndjson.gz")) {
    // Ref points at gz, but only the raw exists (gzip-failure fallback): compress it now.
    const rawCandidate = resolveWithin(basePath, logRef.slice(0, -".gz".length));
    if (!(await fs.stat(rawCandidate).catch(() => null))) throw notFound("Run log not found");
    gzAbs = direct;
    await gzipFileToGz(rawCandidate, gzAbs);
    await fs.unlink(rawCandidate).catch(() => undefined);
  } else {
    throw notFound("Run log not found");
  }

  const stat = await fs.stat(gzAbs).catch(() => null);
  if (!stat) throw notFound("Run log not found");
  const relPath = toPosixKey(path.relative(basePath, gzAbs));
  const [companyId, ...companyRelativePath] = relPath.split("/");
  if (!companyId || companyRelativePath.length === 0) {
    throw new Error("Invalid company-scoped run-log path");
  }
  return {
    absPath: gzAbs,
    relPath,
    objectKey: `${companyId}/${RUN_LOG_OBJECT_NAMESPACE}/${companyRelativePath.join("/")}`,
    bytes: stat.size,
    sha256: await sha256File(gzAbs),
  };
}

export function createDurableRunLogStore(options: DurableRunLogStoreOptions): RunLogStore {
  const { basePath, s3 } = options;
  const s3Reader = options.s3Reader;
  const s3Prefix = normalizeKeyPrefix(s3?.keyPrefix);
  const inflightMirrorMs = s3?.inflightMirrorMs && s3.inflightMirrorMs > 0 ? s3.inflightMirrorMs : 0;

  function s3Key(logRef: string): string {
    return s3Prefix ? `${s3Prefix}/${logRef}` : logRef;
  }

  interface InflightMirrorEntry {
    dirty: boolean;
    lastMirrorAt: number;
    timer: NodeJS.Timeout | null;
    upload: Promise<boolean> | null;
  }
  const inflightMirrors = new Map<string, InflightMirrorEntry>();

  function mirrorInflightNow(logRef: string, entry: InflightMirrorEntry): Promise<boolean> {
    entry.dirty = false;
    const upload = (async () => {
      const absPath = resolveWithin(basePath, logRef);
      const stat = await fs.stat(absPath);
      if (stat.size === 0) return true;
      await s3!.provider.putObject({
        objectKey: s3Key(logRef),
        body: createReadStream(absPath, { start: 0, end: stat.size - 1 }),
        contentType: "application/x-ndjson",
        contentLength: stat.size,
      });
      return true;
    })().catch((err) => {
      logger.warn({ err, objectKey: s3Key(logRef) }, "run-log: failed to mirror in-flight log");
      entry.dirty = true;
      return false;
    }).finally(() => {
      entry.lastMirrorAt = Date.now();
      entry.upload = null;
      if (entry.dirty) scheduleInflightMirror(logRef, entry);
    });
    entry.upload = upload;
    return upload;
  }

  function scheduleInflightMirror(logRef: string, entry: InflightMirrorEntry): void {
    if (inflightMirrors.get(logRef) !== entry || entry.timer || entry.upload) return;
    const delay = Math.max(0, inflightMirrorMs - (Date.now() - entry.lastMirrorAt));
    entry.timer = setTimeout(() => {
      entry.timer = null;
      void mirrorInflightNow(logRef, entry);
    }, delay);
    entry.timer.unref?.();
  }

  function noteInflightAppend(logRef: string): void {
    if (!s3 || inflightMirrorMs <= 0) return;
    let entry = inflightMirrors.get(logRef);
    if (!entry) {
      entry = { dirty: false, lastMirrorAt: Date.now(), timer: null, upload: null };
      inflightMirrors.set(logRef, entry);
    }
    entry.dirty = true;
    scheduleInflightMirror(logRef, entry);
  }

  async function retireInflightMirror(logRef: string): Promise<void> {
    const entry = inflightMirrors.get(logRef);
    if (!entry) return;
    inflightMirrors.delete(logRef);
    if (entry.timer) {
      clearTimeout(entry.timer);
      entry.timer = null;
    }
    if (entry.upload) await entry.upload;
  }

  // logRef -> cap-tracking state. Cleared in finalize(); reseeded from disk
  // (fs.stat) the first time an unknown logRef is appended to, so a server
  // restart mid-run doesn't lose the running total.
  const capState = new Map<string, RunLogCapState>();
  // Close append admission per handle at finalize, then drain accepted disk
  // writes before hashing/compressing. A later retry can reuse the same logRef.
  const closingHandles = new WeakSet<RunLogHandle>();
  const pendingAppends = new WeakMap<RunLogHandle, Set<Promise<void>>>();
  const appendAfterFinalizeWarned = new WeakSet<RunLogHandle>();

  async function ensureDir(relativeDir: string) {
    const dir = resolveWithin(basePath, relativeDir);
    await fs.mkdir(dir, { recursive: true });
  }

  async function getCapState(logRef: string, absPath: string): Promise<RunLogCapState> {
    const existing = capState.get(logRef);
    if (existing) return existing;
    const stat = await fs.stat(absPath).catch(() => null);
    const writtenBytes = stat?.size ?? 0;
    // Restart seeding: a run already at/over the cap had its truncation marker
    // written by the previous instance (or the file is already over cap). Seed
    // `truncated: true` so we silently drop further appends instead of writing a
    // SECOND marker. Only a live crossing of the cap (below) writes the marker.
    const seeded: RunLogCapState = {
      writtenBytes,
      truncated: writtenBytes >= runLogMaxBytes(),
    };
    capState.set(logRef, seeded);
    return seeded;
  }

  async function readFileRange(filePath: string, offset: number, limitBytes: number, signal?: AbortSignal): Promise<RunLogReadResult> {
    signal?.throwIfAborted();
    const stat = await fs.stat(filePath).catch(() => null);
    if (!stat) throw notFound("Run log not found");

    const start = Math.max(0, Math.min(offset, stat.size));
    const end = Math.max(start, Math.min(start + limitBytes - 1, stat.size - 1));

    if (start > end) {
      return { content: "", nextOffset: start };
    }

    const chunks: Buffer[] = [];
    await new Promise<void>((resolve, reject) => {
      const stream = attachAbortSignal(createReadStream(filePath, { start, end }), signal);
      stream.on("data", (chunk) => {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      });
      stream.on("error", reject);
      stream.on("end", () => resolve());
    });

    const content = Buffer.concat(chunks).toString("utf8");
    const nextOffset = end + 1 < stat.size ? end + 1 : undefined;
    return { content, nextOffset };
  }

  async function readS3Range(
    logRef: string,
    offset: number,
    limitBytes: number,
    signal?: AbortSignal,
  ): Promise<RunLogReadResult> {
    signal?.throwIfAborted();
    if (!s3) throw notFound("Run log not found");
    const objectKey = s3Key(logRef);
    const head = await s3.provider.headObject({ objectKey, signal });
    signal?.throwIfAborted();
    if (!head.exists) throw notFound("Run log not found");

    const total = head.contentLength ?? 0;
    const start = Math.max(0, Math.min(offset, total));
    const end = Math.min(start + limitBytes - 1, total - 1);
    if (total === 0 || start > end) return { content: "", nextOffset: start < total ? start : undefined };

    const result = await s3.provider.getObject({ objectKey, range: { start, end }, signal });
    signal?.throwIfAborted();
    const stream = attachAbortSignal(result.stream, signal);
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    signal?.throwIfAborted();
    const content = Buffer.concat(chunks).toString("utf8");
    return { content, nextOffset: end + 1 < total ? end + 1 : undefined };
  }

  /**
   * Range-read a gzip-compressed log file. offset/limitBytes semantics are over
   * the UNCOMPRESSED byte stream (identical to readFileRange). Delegates to the
   * shared `readGunzipRange`, which the S3 read path also reuses.
   */
  async function readGzipRange(filePath: string, offset: number, limitBytes: number, signal?: AbortSignal): Promise<RunLogReadResult> {
    signal?.throwIfAborted();
    const exists = await fs.stat(filePath).catch(() => null);
    if (!exists) throw notFound("Run log not found");
    return readGunzipRange(attachAbortSignal(createReadStream(filePath), signal), offset, limitBytes);
  }

  /**
   * Resolve which file backs a logRef, tolerating drift between the DB row's
   * ref and what is on disk:
   *  - exact path if present
   *  - `.ndjson` ref → try `.ndjson.gz` (legacy rows + the compress crash window)
   *  - `.ndjson.gz` ref → try raw `.ndjson` (gzip-failure fallback)
   * Returns null when no local copy exists so the durable reader can try S3.
   */
  async function resolveReadTarget(logRef: string): Promise<{ absPath: string; compressed: boolean } | null> {
    const absPath = resolveWithin(basePath, logRef);
    if (await fs.stat(absPath).catch(() => null)) {
      return { absPath, compressed: logRef.endsWith(".gz") };
    }

    if (logRef.endsWith(".ndjson")) {
      const gzRef = `${logRef}.gz`;
      const gzAbs = resolveWithin(basePath, gzRef);
      if (await fs.stat(gzAbs).catch(() => null)) {
        return { absPath: gzAbs, compressed: true };
      }
    } else if (logRef.endsWith(".ndjson.gz")) {
      const rawRef = logRef.slice(0, -".gz".length);
      const rawAbs = resolveWithin(basePath, rawRef);
      if (await fs.stat(rawAbs).catch(() => null)) {
        return { absPath: rawAbs, compressed: false };
      }
    }

    return null;
  }

  return {
    async begin(input) {
      const [companyId, agentId] = safeSegments(input.companyId, input.agentId);
      const runId = safeSegments(input.runId)[0]!;
      const relDir = path.join(companyId, agentId);
      const relPath = path.join(relDir, `${runId}.ndjson`);
      await ensureDir(relDir);

      const absPath = resolveWithin(basePath, relPath);
      await retireInflightMirror(relPath);
      const rawExists = await fs.stat(absPath).then(() => true, () => false);
      const gzPath = `${absPath}.gz`;
      if (!rawExists && await fs.stat(gzPath).catch(() => null)) {
        // A retry may reopen a previously finalized run. Restore the compressed
        // prefix before appending so every attempt remains in chronological order.
        await gunzipFileToRaw(gzPath, absPath);
      } else if (!rawExists && s3) {
        const head = await s3.provider.headObject({ objectKey: s3Key(relPath) });
        if (head.exists) {
          const temporary = `${absPath}.${randomUUID()}.restore`;
          try {
            const object = await s3.provider.getObject({ objectKey: s3Key(relPath) });
            await pipeline(object.stream, createWriteStream(temporary, { flags: "wx", mode: 0o600 }));
            if (typeof head.contentLength === "number" && (await fs.stat(temporary)).size !== head.contentLength) {
              throw new Error("Durable run log restore returned an incomplete prefix");
            }
            await fs.link(temporary, absPath).catch((err: NodeJS.ErrnoException) => {
              if (err.code !== "EEXIST") throw err;
            });
          } finally {
            await fs.rm(temporary, { force: true });
          }
        }
      }
      const file = await fs.open(absPath, "a", 0o600);
      await file.close();
      return { store: "local_file", logRef: relPath, attemptId: randomUUID() };
    },

    async append(handle, event) {
      if (handle.store !== "local_file") return 0;
      if (closingHandles.has(handle)) {
        if (!appendAfterFinalizeWarned.has(handle)) {
          appendAfterFinalizeWarned.add(handle);
          logger.warn({ logRef: handle.logRef }, "run-log: append after finalize ignored");
        }
        return 0;
      }

      const operation = (async () => {
        const absPath = resolveWithin(basePath, handle.logRef);
        const state = await getCapState(handle.logRef, absPath);
        if (state.truncated) return 0;

        const line = JSON.stringify({
          ts: event.ts,
          ...(handle.attemptId ? { attemptId: handle.attemptId } : {}),
          stream: event.stream,
          chunk: event.chunk,
          ...(typeof event.seq === "number" && Number.isFinite(event.seq) ? { seq: event.seq } : {}),
        });
        const persisted = `${line}\n`;
        const persistedBytes = Buffer.byteLength(persisted, "utf8");
        const capBytes = runLogMaxBytes();

        if (state.writtenBytes + persistedBytes > capBytes) {
          state.truncated = true;
          const marker = `${JSON.stringify({
            ts: event.ts,
            stream: RUN_LOG_TRUNCATION_STREAM,
            chunk: `[run-log truncated: size cap ${capBytes} bytes reached; further output dropped]`,
          })}\n`;
          await fs.appendFile(absPath, marker, "utf8");
          state.writtenBytes += Buffer.byteLength(marker, "utf8");
          if (!closingHandles.has(handle)) noteInflightAppend(handle.logRef);
          return 0;
        }

        await fs.appendFile(absPath, persisted, "utf8");
        state.writtenBytes += persistedBytes;
        if (!closingHandles.has(handle)) noteInflightAppend(handle.logRef);
        return persistedBytes;
      })();
      let pending = pendingAppends.get(handle);
      if (!pending) {
        pending = new Set();
        pendingAppends.set(handle, pending);
      }
      const settled = operation.then(() => undefined, () => undefined);
      pending.add(settled);
      try {
        return await operation;
      } finally {
        pending.delete(settled);
      }
    },

    async finalize(handle) {
      closingHandles.add(handle);
      if (handle.store !== "local_file") {
        return { bytes: 0, compressed: false, logRef: handle.logRef };
      }
      const pending = pendingAppends.get(handle);
      if (pending?.size) {
        let timer: NodeJS.Timeout | undefined;
        const drained = await Promise.race([
          Promise.all([...pending]).then(() => true),
          new Promise<boolean>((resolve) => {
            timer = setTimeout(() => resolve(false), 5_000);
          }),
        ]);
        if (timer) clearTimeout(timer);
        if (!drained) {
          void retireInflightMirror(handle.logRef).catch(() => undefined);
          return { bytes: null, sha256: null, compressed: false, logRef: handle.logRef };
        }
      }
      await retireInflightMirror(handle.logRef);
      capState.delete(handle.logRef);
      const absPath = resolveWithin(basePath, handle.logRef);
      const stat = await fs.stat(absPath).catch(() => null);
      if (!stat) throw notFound("Run log not found");

      // bytes/sha256 always describe the ORIGINAL uncompressed content.
      const rawBytes = stat.size;
      const rawSha256 = await sha256File(absPath);

      if (s3) {
        try {
          await s3.provider.putObject({
            objectKey: s3Key(handle.logRef),
            body: createReadStream(absPath),
            contentType: "application/x-ndjson",
            contentLength: stat.size,
          });
        } catch (err) {
          logger.warn({ err, objectKey: s3Key(handle.logRef) }, "run-log: failed to mirror finalized log");
        }
      }

      const uncompressedSummary: RunLogFinalizeSummary = {
        bytes: rawBytes,
        sha256: rawSha256,
        compressed: false,
        logRef: handle.logRef,
      };

      if (!compressionEnabled()) {
        return uncompressedSummary;
      }

      const gzRef = `${handle.logRef}.gz`;
      const gzAbs = resolveWithin(basePath, gzRef);

      // Crash-safe ordering: write tmp → rename → unlink raw. On any failure,
      // clean up the tmp and keep the raw file. finalize must never lose the
      // log or throw just because gzip failed.
      try {
        await gzipFileToGz(absPath, gzAbs);
        await fs.unlink(absPath).catch((unlinkErr) => {
          // Compressed copy is durable; a leftover raw file is harmless (read()
          // prefers the exact ref, which is now the .gz).
          logger.warn(
            { err: unlinkErr, logRef: handle.logRef },
            "run-log: failed to unlink raw file after compression",
          );
        });
        return {
          bytes: rawBytes,
          sha256: rawSha256,
          compressed: true,
          logRef: gzRef,
        };
      } catch (err) {
        logger.warn(
          { err, logRef: handle.logRef },
          "run-log: compression failed; leaving raw log uncompressed",
        );
        return uncompressedSummary;
      }
    },

    async read(handle, opts) {
      opts?.signal?.throwIfAborted();
      const offset = opts?.offset ?? 0;
      const limitBytes = opts?.limitBytes ?? 256_000;

      // Cold archive handles contain gzip data and retain the fork's full-key
      // lookup. The durable mirror below stores plain ndjson under the run ref.
      if (handle.store === "s3") {
        if (!s3Reader) throw notFound("Run log not found");
        const { stream } = await s3Reader.getObject({ objectKey: handle.logRef, signal: opts?.signal });
        opts?.signal?.throwIfAborted();
        return readGunzipRange(attachAbortSignal(stream, opts?.signal), offset, limitBytes);
      }

      if (handle.store === "missing") throw notFound("Run log not found");
      if (handle.store !== "local_file") throw notFound("Run log not found");

      const target = await resolveReadTarget(handle.logRef);
      if (target) {
        return target.compressed
          ? readGzipRange(target.absPath, offset, limitBytes, opts?.signal)
          : readFileRange(target.absPath, offset, limitBytes, opts?.signal);
      }
      if (!s3) throw notFound("Run log not found");
      // Finalized local logs may point at .ndjson.gz, while the transparent
      // durable mirror remains the original uncompressed .ndjson object.
      const rawRef = handle.logRef.endsWith(".ndjson.gz")
        ? handle.logRef.slice(0, -".gz".length)
        : handle.logRef;
      return readS3Range(rawRef, offset, limitBytes, opts?.signal);
    },

    async flushInflightMirrors() {
      if (!s3 || inflightMirrorMs <= 0) return;
      const flushEntry = async (logRef: string, entry: InflightMirrorEntry) => {
        for (;;) {
          if (entry.timer) {
            clearTimeout(entry.timer);
            entry.timer = null;
          }
          if (entry.upload) {
            await entry.upload;
            continue;
          }
          if (!entry.dirty) return;
          const uploaded = await mirrorInflightNow(logRef, entry);
          if (!uploaded) {
            if (entry.timer) {
              clearTimeout(entry.timer);
              entry.timer = null;
            }
            return;
          }
        }
      };
      await Promise.all([...inflightMirrors].map(([logRef, entry]) => flushEntry(logRef, entry)));
    },
  };
}

/** Backward-compatible constructor name for fork callers and existing fixtures. */
export function createLocalFileRunLogStore(
  basePath: string,
  options?: CreateRunLogStoreOptions,
): RunLogStore {
  return createDurableRunLogStore({ basePath, ...options });
}

function resolveRunLogS3(): DurableRunLogStoreOptions["s3"] {
  const bucket = process.env.RUN_LOG_S3_BUCKET?.trim();
  if (!bucket) return undefined;
  const provider = createS3StorageProvider({
    bucket,
    region: process.env.RUN_LOG_S3_REGION?.trim() || "us-east-1",
    endpoint: process.env.RUN_LOG_S3_ENDPOINT?.trim() || undefined,
    forcePathStyle: process.env.RUN_LOG_S3_FORCE_PATH_STYLE
      ? process.env.RUN_LOG_S3_FORCE_PATH_STYLE === "true"
      : true,
  });
  const inflightSeconds = Number.parseFloat(process.env.RUN_LOG_S3_INFLIGHT_MIRROR_SECONDS ?? "");
  return {
    provider,
    keyPrefix: process.env.RUN_LOG_S3_PREFIX?.trim() || "run-logs",
    inflightMirrorMs:
      Number.isFinite(inflightSeconds) && inflightSeconds > 0 ? Math.round(inflightSeconds * 1000) : undefined,
  };
}

let cachedStore: RunLogStore | null = null;

export function getRunLogStore() {
  if (cachedStore) return cachedStore;
  const basePath = resolveRunLogBasePath();
  // s3Reader defers provider resolution to read time, so a local_disk-only
  // instance never eagerly builds an S3 client, and archived (s3-tier) runs are
  // still transparently readable when object storage is configured.
  cachedStore = createDurableRunLogStore({
    basePath,
    s3: resolveRunLogS3(),
    s3Reader: {
      async getObject(input) {
        const objectStore = getRunLogArchiveObjectStore();
        const metadata = await objectStore?.find(input.objectKey);
        if (objectStore?.shared) {
          if (!metadata) throw notFound("Run log not found");
          const stream = await objectStore.get(metadata);
          return { stream: attachAbortSignal(stream, input.signal) };
        }
        return getRunLogArchiveStorageProvider().getObject({ objectKey: input.objectKey, signal: input.signal });
      },
    },
  });
  return cachedStore;
}

export async function flushInFlightRunLogMirrors(): Promise<void> {
  await cachedStore?.flushInflightMirrors?.();
}
