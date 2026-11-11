import { createReadStream, createWriteStream, promises as fs } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { createGzip, createGunzip } from "node:zlib";
import { pipeline } from "node:stream/promises";
import { addAbortSignal, type Readable } from "node:stream";
import { notFound } from "../errors.js";
import { createS3StorageProvider } from "../storage/s3-provider.js";
import type { StorageProvider } from "../storage/types.js";
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
  /** When set, `read()` can resolve `store: "s3"` handles by streaming from object storage. */
  s3Reader?: RunLogS3Reader | null;
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
  /** Original, uncompressed bytes; null when accepted writes fail to settle. */
  bytes: number | null;
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
  signal?: AbortSignal,
): Promise<RunLogReadResult> {
  if (signal?.aborted) {
    source.destroy();
    signal.throwIfAborted();
  }
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

    if (signal) addAbortSignal(signal, source);
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

// Run-log store with TRANSPARENT durability. The store id stays "local_file" so
// nothing downstream (feedback.ts, the heartbeat read cast, fixtures) changes;
// the S3 mirror is keyed by the same logRef and is purely an implementation
// detail. Live append/tail stays on the pod-local file (fast, no per-chunk PUT);
// on finalize the complete .ndjson is uploaded to object storage; on read we try
// local first and fall back to S3 when the local file is gone. This is the fix
// for "Run log not found" after a deploy/restart (the /paperclip data dir is an
// emptyDir in cloud_tenant mode -- persistence is disabled to avoid the
// operator's privileged selinux-relabel init container in our hardened ns).
//
// Optionally (inflightMirrorMs > 0) the still-running log is ALSO mirrored to
// the same key at a throttled cadence and flushed on graceful shutdown, so a
// restart mid-run preserves the tail up to the last mirror instead of losing
// the whole in-flight log. Finalize retires the in-flight bookkeeping (waiting
// out any upload already on the wire) before writing the complete file, so a
// stale partial can never overwrite a finalized log.
function normalizeKeyPrefix(prefix: string | undefined): string {
  if (!prefix) return "";
  return prefix.trim().replace(/^\/+/, "").replace(/\/+$/, "");
}

export interface DurableRunLogStoreOptions {
  basePath: string;
  s3Reader?: RunLogS3Reader | null;
  // When provided, completed logs are mirrored to object storage on finalize and
  // served from there on read whenever the local file is missing (e.g. the pod
  // rolled and wiped the emptyDir). When omitted, the store is local-only (the
  // historical behaviour: a restart loses the log).
  s3?: {
    provider: StorageProvider;
    keyPrefix?: string;
    // When > 0, ALSO mirror the still-running log to the same object key at
    // most once per this interval (plus a flush hook for graceful shutdown),
    // so a crash mid-run loses at most one interval's tail instead of the
    // whole log. Off (undefined/0) preserves the historical finalize-only
    // mirroring: no extra PUT traffic unless explicitly opted in.
    inflightMirrorMs?: number;
  };
}

export function createDurableRunLogStore(options: DurableRunLogStoreOptions): RunLogStore {
  const { basePath } = options;
  const s3 = options.s3;
  const s3Prefix = normalizeKeyPrefix(s3?.keyPrefix);
  const inflightMirrorMs = s3?.inflightMirrorMs && s3.inflightMirrorMs > 0 ? s3.inflightMirrorMs : 0;
  // A run owns the write handle returned by begin(). Diagnostics can be
  // dispatched without awaiting the rest of onLog (DB progress/live events),
  // but finalize must include every file append already accepted on that handle.
  // Weak collections let completed handles disappear with their owning runs.
  const pendingAppends = new WeakMap<RunLogHandle, Set<Promise<void>>>();
  const closingHandles = new WeakSet<RunLogHandle>();
  const abandonedHandles = new WeakSet<RunLogHandle>();

  function s3Key(logRef: string): string {
    const rawRef = logRef.endsWith(".ndjson.gz") ? logRef.slice(0, -3) : logRef;
    return s3Prefix ? `${s3Prefix}/${rawRef}` : rawRef;
  }

  // In-flight mirror bookkeeping, keyed by logRef. The mirror uploads the
  // CURRENT (partial) file to the SAME key finalize uses: readers already
  // range-read that key, so a partial object is served exactly like a live
  // tail, and finalize simply overwrites it with the complete file. One
  // entry exists only between the first post-interval-eligible append and
  // finalize.
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
        // Bound the stream to the stat'ed size: the run is still appending,
        // and an unbounded stream that grows past stat.size would violate
        // the declared contentLength and fail (or truncate) the upload.
        // Bytes appended after the stat stay dirty and ride the next mirror.
        body: createReadStream(absPath, { start: 0, end: stat.size - 1 }),
        contentType: "application/x-ndjson",
        contentLength: stat.size,
      });
      return true;
    })().catch((err) => {
      // Best-effort like the finalize mirror: a failing upload must never
      // break the run, but a persistently broken mirror should be visible.
      console.warn(
        `[run-log-store] Failed to mirror in-flight run log to object storage (key: ${s3Key(logRef)}):`,
        err,
      );
      // Re-dirty so the tail retries next interval even without new appends;
      // the lastMirrorAt stamp below bounds retries to one per interval.
      entry.dirty = true;
      return false;
    }).finally(() => {
      // Stamp AFTER the attempt so a slow or failing endpoint self-throttles
      // to one attempt per interval instead of hot-looping.
      entry.lastMirrorAt = Date.now();
      entry.upload = null;
      if (entry.dirty) scheduleInflightMirror(logRef, entry);
    });
    entry.upload = upload;
    return upload;
  }

  function scheduleInflightMirror(logRef: string, entry: InflightMirrorEntry): void {
    if (inflightMirrors.get(logRef) !== entry) return;
    if (entry.timer || entry.upload) return;
    const delay = Math.max(0, inflightMirrorMs - (Date.now() - entry.lastMirrorAt));
    entry.timer = setTimeout(() => {
      entry.timer = null;
      void mirrorInflightNow(logRef, entry);
    }, delay);
    // Never keep the process alive just to mirror a tail.
    entry.timer.unref?.();
  }

  function noteInflightAppend(logRef: string): void {
    if (!s3 || inflightMirrorMs <= 0) return;
    let entry = inflightMirrors.get(logRef);
    if (!entry) {
      // First mirror lands one full interval after the first append: a run
      // that finalizes sooner is covered by the finalize upload, and this
      // keeps the steady-state cost at one PUT per interval per active run.
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
    // An upload still in flight could otherwise finish AFTER finalize's
    // complete-file upload and overwrite it with a stale partial.
    if (entry.upload) await entry.upload;
  }

  const capState = new Map<string, RunLogCapState>();
  const appendQueues = new Map<string, Promise<unknown>>();
  const sealedRefs = new Set<string>();
  const abandonedRefs = new Set<string>();
  const finalizations = new WeakMap<RunLogHandle, Promise<RunLogFinalizeSummary>>();

  async function ensureDir(relativeDir: string) {
    const dir = resolveWithin(basePath, relativeDir);
    await fs.mkdir(dir, { recursive: true });
  }

  async function getCapState(logRef: string, absPath: string): Promise<RunLogCapState> {
    const existing = capState.get(logRef);
    if (existing) return existing;
    const stat = await fs.stat(absPath).catch(() => null);
    const writtenBytes = stat?.size ?? 0;
    // A rejected large chunk can leave its marker below the cap. Recognize
    // that terminal marker after restart or a compressed retry as well.
    let truncated = writtenBytes >= runLogMaxBytes();
    if (!truncated && writtenBytes > 0) {
      const file = await fs.open(absPath, "r");
      try {
        const tail = Buffer.alloc(Math.min(1024, writtenBytes));
        const { bytesRead } = await file.read(tail, 0, tail.length, writtenBytes - tail.length);
        const lastLine = tail.subarray(0, bytesRead).toString("utf8").trimEnd().split("\n").at(-1);
        try {
          const last = JSON.parse(lastLine ?? "");
          truncated = last.stream === RUN_LOG_TRUNCATION_STREAM && typeof last.chunk === "string"
            && /^\[run-log truncated: size cap \d+ bytes reached; further output dropped\]$/.test(last.chunk);
        } catch { /* A partial trailing record is not a truncation marker. */ }
      } finally {
        await file.close();
      }
    }
    const seeded: RunLogCapState = { writtenBytes, truncated };
    capState.set(logRef, seeded);
    return seeded;
  }

  async function readLocalRange(
    filePath: string,
    offset: number,
    limitBytes: number,
    signal?: AbortSignal,
  ): Promise<RunLogReadResult | null> {
    signal?.throwIfAborted();
    const start = Math.max(0, offset);
    // Read one extra byte to discover whether another page exists. A single
    // abortable stream avoids an uncancellable stat before opening the file.
    const chunks: Buffer[] = [];
    try {
      const stream = createReadStream(filePath, { start, end: start + limitBytes, signal });
      for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
    } catch (err) {
      signal?.throwIfAborted();
      // A missing file, including deletion before open, falls back to S3.
      if ((err as NodeJS.ErrnoException | null)?.code === "ENOENT") return null;
      throw err;
    }
    signal?.throwIfAborted();
    const bytes = Buffer.concat(chunks);
    const content = bytes.subarray(0, limitBytes).toString("utf8");
    const nextOffset = bytes.length > limitBytes ? start + limitBytes : undefined;
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
    const key = s3Key(logRef);
    const head = await s3.provider.headObject({ objectKey: key, signal });
    signal?.throwIfAborted();
    if (!head.exists) throw notFound("Run log not found");
    const total = head.contentLength ?? 0;
    const start = Math.max(0, Math.min(offset, total));
    // Unlike local file streams, S3 rejects a range that starts at or past
    // EOF with 416 InvalidRange, so a caught-up reader (offset === total)
    // must short-circuit to an empty read instead of clamping end up to
    // start and requesting `bytes=total-total`.
    const end = Math.min(start + limitBytes - 1, total - 1);
    if (total === 0 || start > end) return { content: "", nextOffset: start < total ? start : undefined };

    const result = await s3.provider.getObject({ objectKey: key, range: { start, end }, signal });
    // Destroy a body that stalls after headers arrive, including a body
    // returned just after the caller cancelled the request.
    if (signal) addAbortSignal(signal, result.stream);
    const chunks: Buffer[] = [];
    for await (const chunk of result.stream) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    signal?.throwIfAborted();
    const content = Buffer.concat(chunks).toString("utf8");
    const nextOffset = end + 1 < total ? end + 1 : undefined;
    return { content, nextOffset };
  }

  return {
    async begin(input) {
      const [companyId, agentId] = safeSegments(input.companyId, input.agentId);
      const runId = safeSegments(input.runId)[0]!;
      const relDir = path.join(companyId, agentId);
      const relPath = path.join(relDir, `${runId}.ndjson`);
      await ensureDir(relDir);

      const absPath = resolveWithin(basePath, relPath);
      if (abandonedRefs.has(relPath)) throw new Error("Run log still has unsettled writes");
      await retireInflightMirror(relPath);
      await appendQueues.get(relPath);
      // A retry resumes the uncompressed history, including a prior compressed
      // attempt. Publish without replacing a concurrently restored raw prefix.
      const gzPath = `${absPath}.gz`;
      if (await fs.stat(gzPath).catch(() => null)) {
        if (!(await fs.stat(absPath).catch(() => null))) {
          const temporary = `${absPath}.${randomUUID()}.restore`;
          try {
            await pipeline(createReadStream(gzPath), createGunzip(), createWriteStream(temporary, { flags: "wx", mode: 0o600 }));
            await fs.link(temporary, absPath).catch((error: NodeJS.ErrnoException) => {
              if (error.code !== "EEXIST") throw error;
            });
          } catch (error) {
            // Another begin may publish the raw prefix and remove gzip before
            // this reader opens it. Accept only a proven replacement raw file.
            if ((error as NodeJS.ErrnoException).code !== "ENOENT" || !(await fs.stat(absPath).catch(() => null))) throw error;
          } finally {
            await fs.rm(temporary, { force: true });
          }
        }
        await fs.rm(gzPath, { force: true });
      }
      sealedRefs.delete(relPath);
      capState.delete(relPath);
      // Retries share a run ID. Never truncate their earlier evidence. On a
      // new pod, restore the durable prefix before opening another attempt.
      const exists = await fs.stat(absPath).then(() => true, (error) => {
        if (error.code === "ENOENT") return false;
        throw error;
      });
      if (!exists && s3) {
        const key = s3Key(relPath);
        const head = await s3.provider.headObject({ objectKey: key });
        if (head.exists) {
          const temporary = `${absPath}.${randomUUID()}.restore`;
          try {
            const object = await s3.provider.getObject({ objectKey: key });
            await pipeline(object.stream, createWriteStream(temporary, { flags: "wx", mode: 0o600 }));
            if (typeof head.contentLength === "number" && (await fs.stat(temporary)).size !== head.contentLength) {
              throw new Error("Durable run log restore returned an incomplete prefix");
            }
            // Publish the complete prefix without replacing a file another
            // attempt has already restored and started appending to.
            await fs.link(temporary, absPath).catch((error: NodeJS.ErrnoException) => {
              if (error.code !== "EEXIST") throw error;
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
      if (handle.store !== "local_file" || closingHandles.has(handle) || sealedRefs.has(handle.logRef)) return 0;
      const absPath = resolveWithin(basePath, handle.logRef);

      const line = JSON.stringify({
        ts: event.ts,
        ...(handle.attemptId ? { attemptId: handle.attemptId } : {}),
        stream: event.stream,
        chunk: event.chunk,
        // Monotonic per-run sequence so readers can dedupe and order records
        // even when several identical chunks share the same millisecond ts
        // (common for ACP-style token deltas).
        ...(typeof event.seq === "number" && Number.isFinite(event.seq) ? { seq: event.seq } : {}),
      });
      const persisted = `${line}\n`;
      let pending = pendingAppends.get(handle);
      if (!pending) {
        pending = new Set();
        pendingAppends.set(handle, pending);
      }
      // Register before any await, including cap-state stat. Serialize by ref
      // so concurrent accepted appends cannot exceed the cap or reorder lines.
      const write = (appendQueues.get(handle.logRef) ?? Promise.resolve()).then(async () => {
        const state = await getCapState(handle.logRef, absPath);
        if (state.truncated) return 0;
        const persistedBytes = Buffer.byteLength(persisted, "utf8");
        const capBytes = runLogMaxBytes();
        if (state.writtenBytes + persistedBytes > capBytes) {
          const marker = `${JSON.stringify({ ts: event.ts, stream: RUN_LOG_TRUNCATION_STREAM,
            chunk: `[run-log truncated: size cap ${capBytes} bytes reached; further output dropped]` })}\n`;
          await fs.appendFile(absPath, marker, "utf8");
          state.truncated = true;
          state.writtenBytes += Buffer.byteLength(marker, "utf8");
          if (!closingHandles.has(handle)) noteInflightAppend(handle.logRef);
          return 0;
        }
        await fs.appendFile(absPath, persisted, "utf8");
        state.writtenBytes += persistedBytes;
        if (!closingHandles.has(handle)) noteInflightAppend(handle.logRef);
        return persistedBytes;
      });
      const tracked = write.then(() => undefined);
      pending.add(tracked);
      const queue = tracked.catch(() => undefined);
      appendQueues.set(handle.logRef, queue);
      try {
        await tracked;
        return await write;
      } finally {
        pending.delete(tracked);
        if (appendQueues.get(handle.logRef) === queue) appendQueues.delete(handle.logRef);
      }
    },

    finalize(handle) {
      const existing = finalizations.get(handle);
      if (existing) return existing;
      const operation = (async (): Promise<RunLogFinalizeSummary> => {
        if (handle.store !== "local_file") return { bytes: 0, compressed: false, logRef: handle.logRef };
        if (abandonedHandles.has(handle)) return { bytes: null, sha256: null, compressed: false, logRef: handle.logRef };
        // Close admission before the first await. Drain file writes only, not
        // heartbeat's later DB/live-event persistence, then freeze one consistent
        // byte count, hash, and durable copy. Late diagnostics cannot mutate it.
        closingHandles.add(handle);
        sealedRefs.add(handle.logRef);
        let drainTimer: NodeJS.Timeout | undefined;
        const drained = await Promise.race([
          Promise.allSettled(pendingAppends.get(handle) ?? []).then(() => true),
          new Promise<boolean>((resolve) => {
            drainTimer = setTimeout(() => resolve(false), 3_000);
            drainTimer.unref?.();
          }),
        ]).finally(() => clearTimeout(drainTimer));
        if (!drained) {
          // An in-flight fs append cannot be cancelled safely. Do not hash or
          // mirror a file it may still change, and never re-arm mirroring when
          // that write eventually finishes. Terminal run status can still settle.
          abandonedHandles.add(handle);
          abandonedRefs.add(handle.logRef);
          void retireInflightMirror(handle.logRef).catch(() => undefined);
          console.warn("[run-log-store] Pending log writes did not settle within 3000ms; final log size and hash are unknown.");
          return { bytes: null, sha256: null, compressed: false, logRef: handle.logRef };
        }
        await retireInflightMirror(handle.logRef);
        const absPath = resolveWithin(basePath, handle.logRef);
        const stat = await fs.stat(absPath).catch(() => null);
        if (!stat) throw notFound("Run log not found");

        // bytes/sha256 always describe the ORIGINAL uncompressed content.
        const rawBytes = stat.size;
        const rawSha256 = await sha256File(absPath);
        capState.delete(handle.logRef);

        // Mutable mirrors always contain raw NDJSON. Cold archives use separate
        // immutable company-scoped gzip keys and PostgreSQL integrity metadata.
        if (s3) {
          const body = createReadStream(absPath);
          try {
            await s3.provider.putObject({ objectKey: s3Key(handle.logRef), body,
              contentType: "application/x-ndjson", contentLength: rawBytes });
          } catch (err) {
            logger.warn({ err, logRef: handle.logRef }, "run-log: final mirror failed; retaining local log");
          } finally {
            body.destroy();
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
      })();
      finalizations.set(handle, operation);
      return operation;
    },

    async read(handle, opts) {
      const signal = opts?.signal;
      signal?.throwIfAborted();
      const offset = Math.max(0, opts?.offset ?? 0);
      const limitBytes = Math.max(0, opts?.limitBytes ?? 256_000);

      // Cold tier: stream the archived object, gunzip, and apply the SAME
      // uncompressed offset/limitBytes semantics as the hot gz path. logRef is
      // the full S3 object key, so no company scoping is needed here.
      if (handle.store === "s3") {
        const reader = options?.s3Reader;
        if (!reader) throw notFound("Run log not found");
        const { stream } = await reader.getObject({ objectKey: handle.logRef, signal });
        return readGunzipRange(stream, offset, limitBytes, signal);
      }

      if (handle.store === "missing") {
        // Hot copy is gone and there is no cold archive to fall back to.
        throw notFound("Run log not found");
      }

      if (handle.store !== "local_file") {
        throw notFound("Run log not found");
      }
      const refs = handle.logRef.endsWith(".ndjson.gz")
        ? [handle.logRef, handle.logRef.slice(0, -3)]
        : [handle.logRef, `${handle.logRef}.gz`];
      for (const ref of refs) {
        const absPath = resolveWithin(basePath, ref);
        if (!ref.endsWith(".gz")) {
          const local = await readLocalRange(absPath, offset, limitBytes, signal);
          if (local) return local;
        } else {
          try {
            return await readGunzipRange(createReadStream(absPath), offset, limitBytes, signal);
          } catch (err) {
            signal?.throwIfAborted();
            if ((err as NodeJS.ErrnoException)?.code !== "ENOENT") throw err;
          }
        }
      }
      return readS3Range(handle.logRef, offset, limitBytes, signal);
    },
    async flushInflightMirrors() {
      if (!s3 || inflightMirrorMs <= 0) return;
      const flushEntry = async (logRef: string, entry: InflightMirrorEntry) => {
        // Loop until the entry is clean: an append that lands while an
        // upload is on the wire re-dirties the entry, and its follow-up
        // mirror sits on an unref'ed timer that would never fire once the
        // process exits — so re-check after every await instead of trusting
        // a single pass. A FAILED attempt ends the loop instead of retrying:
        // hot-looping a down endpoint at shutdown would spin forever, and
        // the flush is best-effort by design.
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

export function createLocalFileRunLogStore(basePath: string, options?: CreateRunLogStoreOptions): RunLogStore {
  return createDurableRunLogStore({ basePath, ...options });
}

function resolveRunLogS3(): DurableRunLogStoreOptions["s3"] {
  const bucket = process.env.RUN_LOG_S3_BUCKET?.trim();
  if (!bucket) return undefined;
  const provider = createS3StorageProvider({
    bucket,
    region: process.env.RUN_LOG_S3_REGION?.trim() || "us-east-1",
    endpoint: process.env.RUN_LOG_S3_ENDPOINT?.trim() || undefined,
    prefix: undefined, // prefixing is handled by keyPrefix below (kept off the provider)
    forcePathStyle: process.env.RUN_LOG_S3_FORCE_PATH_STYLE
      ? process.env.RUN_LOG_S3_FORCE_PATH_STYLE === "true"
      : true, // Cubbit (and most S3-compatible endpoints) need path-style
  });
  // Opt-in in-flight tail mirroring: at most one partial upload per interval
  // per active run, so a crash loses at most one interval's tail. Unset/0
  // keeps the historical finalize-only mirroring.
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
          return { stream: await objectStore.get(metadata, { signal: input.signal }) };
        }
        return getRunLogArchiveStorageProvider().getObject(input);
      },
    },
  });
  return cachedStore;
}

export async function flushInFlightRunLogMirrors(): Promise<void> {
  await cachedStore?.flushInflightMirrors?.();
}
