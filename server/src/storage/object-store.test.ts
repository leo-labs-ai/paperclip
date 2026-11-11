import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { buffer } from "node:stream/consumers";
import { describe, expect, it, vi } from "vitest";
import { createObjectStore, type DurableObjectMetadataStore } from "./object-store.js";
import { createStorageService } from "./service.js";
import type { StorageProvider } from "./types.js";

function sha256(input: Buffer): string {
  return createHash("sha256").update(input).digest("hex");
}

async function readerDrain(result: ReturnType<ReturnType<typeof createStorageService>["getObject"]>): Promise<void> {
  const object = await result;
  await buffer(object.stream);
}

function fixture(overrides: Partial<StorageProvider> = {}) {
  const objects = new Map<string, Buffer>();
  const provider: StorageProvider = {
    id: "s3",
    backendId: "s3:test:paperclip:",
    shared: true,
    async putObject(input) {
      if (input.ifNoneMatch && objects.has(input.objectKey)) {
        throw Object.assign(new Error("exists"), { status: 409 });
      }
      const body = input.body instanceof Readable ? await buffer(input.body) : input.body;
      objects.set(input.objectKey, body);
      return { etag: "etag-1", version: "v1", checksumSha256: input.checksumSha256 };
    },
    async getObject(input) {
      const body = objects.get(input.objectKey);
      if (!body) throw new Error("missing");
      return { stream: Readable.from(body), contentLength: body.length, version: "v1" };
    },
    async headObject(input) {
      const body = objects.get(input.objectKey);
      return body
        ? { exists: true, contentLength: body.length, etag: "etag-1", version: "v1" }
        : { exists: false };
    },
    async deleteObject(input) {
      objects.delete(input.objectKey);
    },
    ...overrides,
  };

  const rows: Array<Awaited<ReturnType<DurableObjectMetadataStore["commit"]>>> = [];
  const metadata: DurableObjectMetadataStore = {
    async commit(input) {
      const row = {
        ...input,
        id: `object-${rows.length + 1}`,
        status: "committed" as const,
        corruptionReason: null,
      };
      rows.push(row);
      return row;
    },
    async find(backendId, objectKey) {
      return rows.find((row) => row.backendId === backendId && row.objectKey === objectKey) ?? null;
    },
    markCorrupt: vi.fn(async (id, reason) => {
      const row = rows.find((candidate) => candidate.id === id);
      if (row) {
        row.status = "corrupt";
        row.corruptionReason = reason;
      }
    }),
    markDeleted: vi.fn(async (id) => {
      const row = rows.find((candidate) => candidate.id === id);
      if (row) {
        row.status = "deleted";
        row.corruptionReason = null;
      }
    }),
  };
  return { store: createObjectStore({ provider, metadata }), provider, metadata, objects, rows };
}

describe("durable object store", () => {
  it("uploads, verifies, then commits integrity metadata", async () => {
    const { store, rows } = fixture();
    const body = Buffer.from("shared bytes");
    const committed = await store.put({
      companyId: "company-1",
      kind: "run_log",
      objectKey: "company-1/run-logs/run-1.ndjson.gz",
      contentType: "application/gzip",
      body: Readable.from(body),
      contentLength: body.length,
      sha256: sha256(body),
    });

    expect(committed.status).toBe("committed");
    expect(committed.byteSize).toBe(body.length);
    expect(committed.sha256).toBe(sha256(body));
    expect(committed.version).toBe("v1");
    expect(committed.backendId).toBe("s3:test:paperclip:");
    expect(rows).toHaveLength(1);
  });

  it("does not publish metadata and removes bytes when verification fails", async () => {
    const deleteObject = vi.fn(async () => {});
    const { store, rows } = fixture({
      async headObject() {
        return { exists: true, contentLength: 999, version: "v1" };
      },
      deleteObject,
    });
    const body = Buffer.from("shared bytes");

    await expect(store.put({
      companyId: "company-1",
      kind: "asset",
      objectKey: "company-1/assets/asset-1",
      contentType: "application/octet-stream",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    })).rejects.toThrow("length mismatch");
    expect(rows).toHaveLength(0);
    expect(deleteObject).toHaveBeenCalledWith({ objectKey: "company-1/assets/asset-1", version: "v1" });
  });

  it("rejects a checksum mismatch before publishing metadata", async () => {
    const { store, rows } = fixture({
      async headObject() {
        return {
          exists: true,
          contentLength: Buffer.byteLength("shared bytes"),
          checksumSha256: Buffer.alloc(32, 1).toString("base64"),
          version: "v1",
        };
      },
    });
    const body = Buffer.from("shared bytes");

    await expect(store.put({
      companyId: "company-1",
      kind: "asset",
      objectKey: "company-1/assets/asset-1",
      contentType: "application/octet-stream",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    })).rejects.toThrow("checksum mismatch");
    expect(rows).toHaveLength(0);
  });

  it("lets an independent service instance read committed bytes", async () => {
    const first = fixture();
    const writer = createStorageService(first.provider, first.store);
    const stored = await writer.putFile({
      companyId: "company-1",
      durableKind: "asset",
      namespace: "assets",
      originalFilename: "artifact.bin",
      contentType: "application/octet-stream",
      body: Buffer.from("shared bytes"),
    });
    const secondStore = createObjectStore({ provider: first.provider, metadata: first.metadata });
    const reader = createStorageService(first.provider, secondStore);

    expect(stored.durableObject).toBeDefined();
    const object = await reader.getObject("company-1", stored.objectKey);
    expect(await buffer(object.stream)).toEqual(Buffer.from("shared bytes"));
  });

  it("pins range reads to the committed backend version", async () => {
    const first = fixture();
    const getObject = vi.fn(first.provider.getObject);
    const provider = { ...first.provider, getObject };
    const store = createObjectStore({ provider, metadata: first.metadata });
    const writer = createStorageService(provider, store);
    const stored = await writer.putFile({
      companyId: "company-1",
      durableKind: "asset",
      namespace: "assets",
      originalFilename: "artifact.bin",
      contentType: "application/octet-stream",
      body: Buffer.from("shared bytes"),
    });

    await readerDrain(writer.getObject("company-1", stored.objectKey, { range: { start: 0, end: 5 } }));
    expect(getObject).toHaveBeenCalledWith({
      objectKey: stored.objectKey,
      version: "v1",
      range: { start: 0, end: 5 },
    });
  });

  it("returns matching committed metadata when a conditional retry finds the object", async () => {
    const { store } = fixture();
    const body = Buffer.from("shared bytes");
    const input = {
      companyId: "company-1",
      kind: "run_log" as const,
      objectKey: "company-1/run-logs/run-1.ndjson.gz",
      contentType: "application/gzip",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    };
    const committed = await store.put(input);

    await expect(store.put(input)).resolves.toEqual(committed);
  });

  it("adopts an orphan backend object on 409 when metadata was never committed", async () => {
    const body = Buffer.from("shared bytes");
    const checksumSha256 = Buffer.from(sha256(body), "hex").toString("base64");
    const { store, rows, objects } = fixture({
      async putObject() {
        throw Object.assign(new Error("exists"), { status: 409 });
      },
      async headObject() {
        return {
          exists: true,
          contentLength: body.length,
          checksumSha256,
          etag: "etag-orphan",
          version: "v-orphan",
        };
      },
    });
    objects.set("company-1/run-logs/run-orphan.ndjson.gz", body);

    const committed = await store.put({
      companyId: "company-1",
      kind: "run_log",
      objectKey: "company-1/run-logs/run-orphan.ndjson.gz",
      contentType: "application/gzip",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    });

    expect(committed.status).toBe("committed");
    expect(committed.version).toBe("v-orphan");
    expect(committed.etag).toBe("etag-orphan");
    expect(committed.sha256).toBe(sha256(body));
    expect(rows).toHaveLength(1);
  });

  it("still rejects a 409 orphan when HEAD length does not match the put", async () => {
    const body = Buffer.from("shared bytes");
    const { store, rows } = fixture({
      async putObject() {
        throw Object.assign(new Error("exists"), { status: 409 });
      },
      async headObject() {
        return { exists: true, contentLength: 999, version: "v1" };
      },
    });

    await expect(store.put({
      companyId: "company-1",
      kind: "run_log",
      objectKey: "company-1/run-logs/run-orphan.ndjson.gz",
      contentType: "application/gzip",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    })).rejects.toMatchObject({ status: 409 });
    expect(rows).toHaveLength(0);
  });

  it("does not treat a non-conflict upload error as an idempotent retry", async () => {
    const { store } = fixture({
      async putObject() {
        throw new Error("network unavailable");
      },
    });
    const body = Buffer.from("shared bytes");

    await expect(store.put({
      companyId: "company-1",
      kind: "run_log",
      objectKey: "company-1/run-logs/run-1.ndjson.gz",
      contentType: "application/gzip",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    })).rejects.toThrow("network unavailable");
  });

  it("rejects a conditional retry when the committed backend bytes no longer verify", async () => {
    const first = fixture();
    const body = Buffer.from("shared bytes");
    const input = {
      companyId: "company-1",
      kind: "run_log" as const,
      objectKey: "company-1/run-logs/run-1.ndjson.gz",
      contentType: "application/gzip",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    };
    await first.store.put(input);
    const corruptRetry = createObjectStore({
      provider: {
        ...first.provider,
        async headObject() {
          return { exists: true, contentLength: 999, version: "v1" };
        },
      },
      metadata: first.metadata,
    });

    await expect(corruptRetry.put(input)).rejects.toThrow("retry verification failed");
  });

  it("retains uploaded bytes when metadata commit outcome is uncertain", async () => {
    const deleteObject = vi.fn(async () => {});
    const { provider } = fixture({ deleteObject });
    const body = Buffer.from("shared bytes");
    const failing = createObjectStore({
      provider,
      metadata: {
        commit: async () => { throw new Error("database unavailable"); },
        find: async () => null,
        markCorrupt: async () => {},
        markDeleted: async () => {},
      },
    });

    await expect(failing.put({
      companyId: null,
      kind: "database_backup",
      objectKey: "system/backups/backup-1.sql.gz",
      contentType: "application/gzip",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    })).rejects.toThrow("database unavailable");
    expect(deleteObject).not.toHaveBeenCalled();
  });

  it("deletes the committed version before tombstoning its metadata", async () => {
    const first = fixture();
    const deleteObject = vi.fn(first.provider.deleteObject);
    const provider = { ...first.provider, deleteObject };
    const store = createObjectStore({ provider, metadata: first.metadata });
    const service = createStorageService(provider, store);
    const stored = await service.putFile({
      companyId: "company-1",
      durableKind: "asset",
      namespace: "assets",
      originalFilename: "artifact.bin",
      contentType: "application/octet-stream",
      body: Buffer.from("shared bytes"),
    });

    await service.deleteObject("company-1", stored.objectKey);

    expect(deleteObject).toHaveBeenCalledWith({
      objectKey: stored.objectKey,
      version: "v1",
    });
    expect(stored.durableObject?.status).toBe("deleted");
    await expect(service.getObject("company-1", stored.objectKey)).rejects.toThrow("Durable object is deleted");
  });

  it("does not tombstone metadata when backend deletion fails", async () => {
    const first = fixture();
    const provider = {
      ...first.provider,
      async deleteObject() {
        throw new Error("storage unavailable");
      },
    };
    const store = createObjectStore({ provider, metadata: first.metadata });
    const body = Buffer.from("shared bytes");
    const committed = await store.put({
      companyId: "company-1",
      kind: "asset",
      objectKey: "company-1/assets/asset-1",
      contentType: "application/octet-stream",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    });

    await expect(store.delete(committed)).rejects.toThrow("storage unavailable");
    expect(committed.status).toBe("committed");
  });

  it("marks an object corrupt when streamed bytes fail integrity verification", async () => {
    const { store, rows, objects, metadata } = fixture();
    const original = Buffer.from("good bytes");
    const committed = await store.put({
      companyId: "company-1",
      kind: "asset",
      objectKey: "company-1/assets/asset-1",
      contentType: "application/octet-stream",
      body: original,
      contentLength: original.length,
      sha256: sha256(original),
    });
    objects.set(committed.objectKey, Buffer.from("bad bytes!"));

    const stream = await store.get(committed);
    await expect(buffer(stream)).rejects.toThrow("integrity check failed");
    expect(metadata.markCorrupt).toHaveBeenCalledWith(committed.id, "read_integrity_mismatch");
    expect(rows[0].status).toBe("corrupt");
  });

  it("rejects replica-local providers and keys outside their tenant scope", async () => {
    const local = fixture({ id: "local_disk", shared: false });
    const body = Buffer.from("bytes");
    await expect(local.store.put({
      companyId: "company-1",
      kind: "asset",
      objectKey: "company-1/assets/asset-1",
      contentType: "application/octet-stream",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    })).rejects.toThrow("shared storage provider");

    const shared = fixture();
    await expect(shared.store.put({
      companyId: "company-1",
      kind: "asset",
      objectKey: "company-2/assets/asset-1",
      contentType: "application/octet-stream",
      body,
      contentLength: body.length,
      sha256: sha256(body),
    })).rejects.toThrow("company-1/");
  });
  it("passes cancellation to a pending immutable-version GET", async () => {
    let started!: () => void;
    const pending = new Promise<void>(resolve => { started = resolve; });
    const getObject = vi.fn<StorageProvider["getObject"]>(async input => {
      expect(input.version).toBe("v1");
      started();
      return new Promise((_resolve, reject) => {
        input.signal!.addEventListener("abort", () => reject(input.signal!.reason), { once: true });
      });
    });
    const { store, metadata } = fixture({ getObject });
    const body = Buffer.from("bytes");
    const committed = await store.put({ companyId: "company-1", kind: "run_log",
      objectKey: "company-1/run-logs/cancel", contentType: "application/octet-stream",
      body, contentLength: body.length, sha256: sha256(body) });
    const controller = new AbortController();
    const read = store.get(committed, { signal: controller.signal });
    const rejected = expect(read).rejects.toMatchObject({ name: "AbortError" });
    await pending;
    controller.abort();
    await rejected;
    expect(getObject).toHaveBeenCalledWith({ objectKey: committed.objectKey, version: "v1", signal: controller.signal });
    expect(metadata.markCorrupt).not.toHaveBeenCalled();
  });

  it("destroys a stalled integrity source on cancellation without marking it corrupt", async () => {
    const source = new Readable({ read() {} });
    const { store, metadata } = fixture({ async getObject() { return { stream: source }; } });
    const body = Buffer.from("bytes");
    const committed = await store.put({ companyId: "company-1", kind: "run_log",
      objectKey: "company-1/run-logs/cancel-body", contentType: "application/octet-stream",
      body, contentLength: body.length, sha256: sha256(body) });
    const controller = new AbortController();
    const stream = await store.get(committed, { signal: controller.signal });
    const rejected = expect(buffer(stream)).rejects.toMatchObject({ name: "AbortError" });
    controller.abort();
    await rejected;
    expect(source.destroyed).toBe(true);
    expect(metadata.markCorrupt).not.toHaveBeenCalled();
  });

  it("forwards source errors through the integrity stream", async () => {
    const failure = new Error("fixture transport closed");
    const source = new Readable({ read() { this.destroy(failure); } });
    const { store, metadata } = fixture({ async getObject() { return { stream: source }; } });
    const body = Buffer.from("bytes");
    const committed = await store.put({ companyId: "company-1", kind: "run_log",
      objectKey: "company-1/run-logs/source-error", contentType: "application/octet-stream",
      body, contentLength: body.length, sha256: sha256(body) });
    await expect(buffer(await store.get(committed))).rejects.toBe(failure);
    expect(source.destroyed).toBe(true);
    expect(metadata.markCorrupt).not.toHaveBeenCalled();
  });

  it("stops the underlying source when a bounded reader closes early", async () => {
    let emitted = false;
    const source = new Readable({ read() {
      if (!emitted) { emitted = true; this.push(Buffer.from("prefix")); }
    } });
    const { store, metadata } = fixture({ async getObject() { return { stream: source }; } });
    const body = Buffer.from("prefix-tail");
    const committed = await store.put({ companyId: "company-1", kind: "run_log",
      objectKey: "company-1/run-logs/early-close", contentType: "application/octet-stream",
      body, contentLength: body.length, sha256: sha256(body) });
    const closed = new Promise<void>(resolve => source.once("close", resolve));
    for await (const chunk of await store.get(committed)) {
      expect(Buffer.from(chunk).toString()).toBe("prefix");
      break;
    }
    await closed;
    expect(source.destroyed).toBe(true);
    expect(metadata.markCorrupt).not.toHaveBeenCalled();
  });

  it("never deletes bytes committed by a concurrent orphan adopter", async () => {
    const { store, provider, metadata } = fixture();
    const deleted = vi.spyOn(provider, "deleteObject");
    let reached!: () => void, release!: () => void;
    const atCommit = new Promise<void>(resolve => { reached = resolve; });
    const resume = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(metadata, "commit").mockImplementationOnce(async () => {
      reached();
      await resume;
      throw Object.assign(new Error("metadata key already committed"), { code: "23505" });
    });
    const body = Buffer.from("shared archive");
    const input = { companyId: "company-1", kind: "run_log" as const,
      objectKey: "company-1/run-logs/adoption-race", contentType: "application/octet-stream",
      body, contentLength: body.length, sha256: sha256(body) };
    const first = store.put(input);
    const rejected = expect(first).rejects.toThrow("metadata key already committed");
    try {
      await atCommit;
      const adopted = await store.put(input);
      release();
      await rejected;
      expect(await buffer(await store.get(adopted))).toEqual(body);
      expect(deleted).not.toHaveBeenCalled();
    } finally {
      release();
      await first.catch(() => undefined);
    }
  });

  it("rejects a silently destroyed source instead of hanging its verifier", async () => {
    const source = new Readable({ read() { this.destroy(); } });
    const { store, metadata } = fixture({ async getObject() { return { stream: source }; } });
    const body = Buffer.from("bytes");
    const committed = await store.put({ companyId: "company-1", kind: "run_log",
      objectKey: "company-1/run-logs/premature-close", contentType: "application/octet-stream",
      body, contentLength: body.length, sha256: sha256(body) });
    await expect(buffer(await store.get(committed))).rejects.toThrow("source closed before end");
    expect(metadata.markCorrupt).not.toHaveBeenCalled();
  });

});
