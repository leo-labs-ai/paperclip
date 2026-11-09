import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import type { ObjectStore } from "./object-store.js";
import { Readable } from "node:stream";
import type { StorageService, StorageProvider, PutFileInput, PutFileResult } from "./types.js";
import { badRequest, forbidden, unprocessable } from "../errors.js";

const MAX_SEGMENT_LENGTH = 120;

function sanitizeSegment(value: string): string {
  const cleaned = value
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/_{2,}/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!cleaned) return "file";
  return cleaned.slice(0, MAX_SEGMENT_LENGTH);
}

function normalizeNamespace(namespace: string): string {
  const normalized = namespace
    .split("/")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
    .map((entry) => sanitizeSegment(entry));
  if (normalized.length === 0) return "misc";
  return normalized.join("/");
}

function splitFilename(filename: string | null): { stem: string; ext: string } {
  if (!filename) return { stem: "file", ext: "" };
  const base = path.basename(filename).trim();
  if (!base) return { stem: "file", ext: "" };

  const extRaw = path.extname(base);
  const stemRaw = extRaw ? base.slice(0, base.length - extRaw.length) : base;
  const stem = sanitizeSegment(stemRaw);
  const ext = extRaw
    .toLowerCase()
    .replace(/[^a-z0-9.]/g, "")
    .slice(0, 16);
  return {
    stem,
    ext,
  };
}

function ensureCompanyPrefix(companyId: string, objectKey: string): void {
  const expectedPrefix = `${companyId}/`;
  if (!objectKey.startsWith(expectedPrefix)) {
    throw forbidden("Object does not belong to company");
  }
  if (objectKey.includes("..")) {
    throw badRequest("Invalid object key");
  }
}

function hashBuffer(input: Buffer): string {
  return createHash("sha256").update(input).digest("hex");
}

function buildObjectKey(companyId: string, namespace: string, originalFilename: string | null): string {
  const ns = normalizeNamespace(namespace);
  const now = new Date();
  const year = String(now.getUTCFullYear());
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");
  const { stem, ext } = splitFilename(originalFilename);
  const suffix = randomUUID();
  const filename = `${suffix}-${stem}${ext}`;
  return `${companyId}/${ns}/${year}/${month}/${day}/${filename}`;
}

function assertPutFileInput(input: PutFileInput): void {
  if (!input.companyId || input.companyId.trim().length === 0) {
    throw unprocessable("companyId is required");
  }
  if (!input.namespace || input.namespace.trim().length === 0) {
    throw unprocessable("namespace is required");
  }
  if (!input.contentType || input.contentType.trim().length === 0) {
    throw unprocessable("contentType is required");
  }
  if (!(input.body instanceof Buffer) && !(input.body instanceof Readable)) {
    throw unprocessable("body must be a Buffer or Readable");
  }
  if (!Buffer.isBuffer(input.body) && (!("byteSize" in input) || !Number.isSafeInteger(input.byteSize) || input.byteSize < 0 || !/^[0-9a-f]{64}$/.test(input.sha256))) {
    throw unprocessable("Streamed files require an exact byte size and SHA-256");
  }
  if (("byteSize" in input ? input.byteSize : input.body.length) <= 0) {
    throw unprocessable("File is empty");
  }
}

export function createStorageService(
  provider: StorageProvider,
  durableStore?: ObjectStore,
): StorageService {
  return {
    provider: provider.id,
    shared: provider.shared,

    async putFile(input: PutFileInput): Promise<PutFileResult> {
      assertPutFileInput(input);
      const objectKey = input.objectKey ?? buildObjectKey(input.companyId, input.namespace, input.originalFilename);
      ensureCompanyPrefix(input.companyId, objectKey);
      const byteSize = "byteSize" in input ? input.byteSize : input.body.length;
      const contentType = input.contentType.trim().toLowerCase();
      const streaming = "byteSize" in input;
      const sha256 = streaming ? input.sha256 : hashBuffer(input.body);
      if (input.durableKind && provider.shared && !durableStore) {
        throw new Error("Durable object metadata store is not configured");
      }
      if (input.durableKind && provider.shared && streaming) {
        throw unprocessable("Durable streamed uploads are not supported");
      }
      const durableObject = input.durableKind && provider.shared
        ? await durableStore!.put({
            companyId: input.companyId,
            kind: input.durableKind,
            objectKey,
            contentType,
            body: input.body as Buffer,
            contentLength: byteSize,
            sha256,
          })
        : undefined;
      if (!durableObject) {
        try {
          await provider.putObject({ objectKey, body: input.body, contentType, contentLength: byteSize });
        } catch (error) {
          await provider.deleteObject({ objectKey }).catch(() => {});
          throw error;
        } finally {
          if (streaming) input.body.destroy();
        }
      }

      return {
        provider: provider.id,
        objectKey,
        contentType,
        byteSize,
        sha256,
        originalFilename: input.originalFilename,
        durableObject,
      };
    },

    async getObject(companyId: string, objectKey: string, options) {
      ensureCompanyPrefix(companyId, objectKey);
      const metadata = durableStore && provider.shared
        ? await durableStore.find(objectKey)
        : null;
      if (metadata) {
        if (metadata.status !== "committed") {
          throw new Error(`Durable object is ${metadata.status}`);
        }
        if (!options?.range) {
          return {
            stream: await durableStore!.get(metadata),
            contentType: metadata.contentType,
            contentLength: metadata.byteSize,
            etag: metadata.etag ?? undefined,
            version: metadata.version ?? undefined,
          };
        }
      }
      return provider.getObject({
        objectKey,
        version: metadata?.version ?? undefined,
        range: options?.range,
      });
    },

    async headObject(companyId: string, objectKey: string) {
      ensureCompanyPrefix(companyId, objectKey);
      const metadata = durableStore && provider.shared
        ? await durableStore.find(objectKey)
        : null;
      if (metadata && metadata.status !== "committed") {
        return { exists: false };
      }
      return provider.headObject({
        objectKey,
        version: metadata?.version ?? undefined,
      });
    },

    async deleteObject(companyId: string, objectKey: string) {
      ensureCompanyPrefix(companyId, objectKey);
      const metadata = durableStore && provider.shared
        ? await durableStore.find(objectKey)
        : null;
      if (metadata) {
        await durableStore!.delete(metadata);
        return;
      }
      await provider.deleteObject({ objectKey });
    },
  };
}
