import { createHash } from "node:crypto";
import { addAbortSignal, Readable, Transform } from "node:stream";
import { and, eq } from "drizzle-orm";
import {
  durableObjects,
  type Db,
  type DurableObjectKind,
  type DurableObjectStatus,
} from "@paperclipai/db";
import type { StorageProvider } from "./types.js";

export type { DurableObjectKind, DurableObjectStatus };

export interface DurableObjectMetadata {
  id: string;
  companyId: string | null;
  kind: DurableObjectKind;
  provider: string;
  backendId: string;
  objectKey: string;
  contentType: string;
  byteSize: number;
  sha256: string;
  version: string | null;
  etag: string | null;
  status: DurableObjectStatus;
  corruptionReason: string | null;
  verifiedAt: Date;
}

export interface DurableObjectMetadataStore {
  commit(input: Omit<DurableObjectMetadata, "id" | "status" | "corruptionReason">): Promise<DurableObjectMetadata>;
  find(backendId: string, objectKey: string): Promise<DurableObjectMetadata | null>;
  markCorrupt(id: string, reason: string): Promise<void>;
  markDeleted(id: string): Promise<void>;
}

export interface ObjectStorePutInput {
  companyId: string | null;
  kind: DurableObjectKind;
  objectKey: string;
  contentType: string;
  body: Buffer | Readable;
  contentLength: number;
  sha256: string;
}

function assertKeyScope(input: Pick<ObjectStorePutInput, "companyId" | "objectKey">): void {
  const expected = input.companyId ? `${input.companyId}/` : "system/";
  if (!input.objectKey.startsWith(expected)) {
    throw new Error(`Durable object key must start with ${expected}`);
  }
}

function sha256Base64(hex: string): string {
  return Buffer.from(hex, "hex").toString("base64");
}

function createIntegrityStream(
  source: Readable,
  metadata: DurableObjectMetadata,
  markCorrupt: (reason: string) => Promise<void>,
  signal?: AbortSignal,
): Readable {
  const hash = createHash("sha256");
  let bytes = 0;
  const verifier = new Transform({
    transform(chunk, _encoding, callback) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      bytes += buffer.length;
      hash.update(buffer);
      callback(null, buffer);
    },
    flush(callback) {
      const sha256 = hash.digest("hex");
      if (bytes === metadata.byteSize && sha256 === metadata.sha256) {
        callback();
        return;
      }
      void markCorrupt("read_integrity_mismatch")
        .catch(() => undefined)
        .finally(() => callback(new Error("Durable object integrity check failed")));
    },
  });
  // pipe alone does not forward source errors or stop the source when a
  // paginated reader destroys its verifier early. Preserve full-read integrity
  // checks, but do not misclassify cancellation/network failures as corruption.
  const forwardError = (error: Error) => verifier.destroy(error);
  source.once("error", forwardError);
  source.once("close", () => {
    source.off("error", forwardError);
    if (!source.readableEnded && !verifier.destroyed) {
      verifier.destroy(new Error("Durable object source closed before end"));
    }
  });
  verifier.once("close", () => {
    source.unpipe(verifier);
    source.destroy();
  });
  if (signal) addAbortSignal(signal, verifier);
  return source.pipe(verifier);
}

export interface ObjectStore {
  shared: boolean;
  put(input: ObjectStorePutInput): Promise<DurableObjectMetadata>;
  find(objectKey: string): Promise<DurableObjectMetadata | null>;
  get(metadata: DurableObjectMetadata, options?: { signal?: AbortSignal }): Promise<Readable>;
  delete(metadata: DurableObjectMetadata): Promise<void>;
}

export function createObjectStore(deps: {
  provider: StorageProvider;
  metadata: DurableObjectMetadataStore;
}): ObjectStore {
  return {
    shared: deps.provider.shared,

    async put(input: ObjectStorePutInput): Promise<DurableObjectMetadata> {
      assertKeyScope(input);
      if (!deps.provider.shared) {
        throw new Error("Durable objects require a shared storage provider");
      }

      const checksumSha256 = sha256Base64(input.sha256);
      let upload;
      try {
        upload = await deps.provider.putObject({
          objectKey: input.objectKey,
          body: input.body,
          contentType: input.contentType,
          contentLength: input.contentLength,
          checksumSha256,
          ifNoneMatch: true,
        });
      } catch (error) {
        const status = typeof error === "object" && error !== null && "status" in error
          ? (error as { status?: unknown }).status
          : undefined;
        if (status !== 409) throw error;

        // Conditional create lost the race, or a prior attempt uploaded bytes
        // then failed before metadata commit. Adopt the backend object only
        // when HEAD length/checksum match this put; otherwise keep failing.
        // Without orphan adoption, run-log archive sweeps hot-loop forever on
        // "Object already exists" after a PG blip mid-commit (prod: 25x/30m).
        const existing = await deps.metadata.find(deps.provider.backendId, input.objectKey);
        const head = await deps.provider.headObject({
          objectKey: input.objectKey,
          version: existing?.version ?? undefined,
        });
        if (
          !head.exists ||
          head.contentLength !== input.contentLength ||
          (head.checksumSha256 && head.checksumSha256 !== checksumSha256)
        ) {
          if (existing) {
            throw new Error("Durable object retry verification failed");
          }
          throw error;
        }

        if (
          existing?.status === "committed" &&
          existing.companyId === input.companyId &&
          existing.kind === input.kind &&
          existing.contentType === input.contentType &&
          existing.byteSize === input.contentLength &&
          existing.sha256 === input.sha256
        ) {
          return existing;
        }

        if (existing != null) {
          // Row exists but does not match this put (corrupt/deleted/different
          // content). Do not invent a second metadata row under the unique key.
          throw error;
        }

        return await deps.metadata.commit({
          companyId: input.companyId,
          kind: input.kind,
          provider: deps.provider.id,
          backendId: deps.provider.backendId,
          objectKey: input.objectKey,
          contentType: input.contentType,
          byteSize: input.contentLength,
          sha256: input.sha256,
          version: head.version ?? null,
          etag: head.etag ?? null,
          verifiedAt: new Date(),
        });
      }

      // Conditional PUT publishes bytes that another retry may already adopt.
      // A failed/uncertain metadata commit is not deletion authority: deleting
      // here can destroy a concurrently committed object's exact version.
      // Retain potential orphans for verified retry adoption instead.
      const head = await deps.provider.headObject({
        objectKey: input.objectKey,
        version: upload.version,
      });
      if (!head.exists || head.contentLength !== input.contentLength) {
        throw new Error("Durable object verification failed: length mismatch");
      }
      if (head.checksumSha256 && head.checksumSha256 !== checksumSha256) {
        throw new Error("Durable object verification failed: checksum mismatch");
      }

      return await deps.metadata.commit({
        companyId: input.companyId,
        kind: input.kind,
        provider: deps.provider.id,
        backendId: deps.provider.backendId,
        objectKey: input.objectKey,
        contentType: input.contentType,
        byteSize: input.contentLength,
        sha256: input.sha256,
        version: head.version ?? upload.version ?? null,
        etag: head.etag ?? upload.etag ?? null,
        verifiedAt: new Date(),
      });
    },

    async find(objectKey: string): Promise<DurableObjectMetadata | null> {
      return deps.metadata.find(deps.provider.backendId, objectKey);
    },

    async get(metadata: DurableObjectMetadata, options?: { signal?: AbortSignal }): Promise<Readable> {
      options?.signal?.throwIfAborted();
      if (metadata.backendId !== deps.provider.backendId) {
        throw new Error("Durable object belongs to a different storage backend");
      }
      if (metadata.status !== "committed") {
        throw new Error(`Durable object is ${metadata.status}`);
      }
      const result = await deps.provider.getObject({
        objectKey: metadata.objectKey,
        version: metadata.version ?? undefined,
        ...(options?.signal ? { signal: options.signal } : {}),
      });
      if (options?.signal?.aborted) {
        result.stream.destroy();
        options.signal.throwIfAborted();
      }
      return createIntegrityStream(
        result.stream,
        metadata,
        (reason) => deps.metadata.markCorrupt(metadata.id, reason),
        options?.signal,
      );
    },

    async delete(metadata: DurableObjectMetadata): Promise<void> {
      if (metadata.backendId !== deps.provider.backendId) {
        throw new Error("Durable object belongs to a different storage backend");
      }
      if (metadata.status === "deleted") return;
      await deps.provider.deleteObject({
        objectKey: metadata.objectKey,
        version: metadata.version ?? undefined,
      });
      await deps.metadata.markDeleted(metadata.id);
    },
  };
}

export function createDrizzleDurableObjectMetadataStore(db: Db): DurableObjectMetadataStore {
  return {
    async commit(input) {
      const [row] = await db.insert(durableObjects).values(input).returning();
      if (!row) throw new Error("Failed to commit durable object metadata");
      return row;
    },

    async find(backendId, objectKey) {
      const [row] = await db
        .select()
        .from(durableObjects)
        .where(and(eq(durableObjects.backendId, backendId), eq(durableObjects.objectKey, objectKey)))
        .limit(1);
      return row ?? null;
    },

    async markCorrupt(id, reason) {
      await db
        .update(durableObjects)
        .set({
          status: "corrupt",
          corruptionReason: reason,
          updatedAt: new Date(),
        })
        .where(eq(durableObjects.id, id));
    },

    async markDeleted(id) {
      await db
        .update(durableObjects)
        .set({
          status: "deleted",
          corruptionReason: null,
          updatedAt: new Date(),
        })
        .where(eq(durableObjects.id, id));
    },
  };
}
