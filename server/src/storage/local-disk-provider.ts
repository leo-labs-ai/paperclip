import { createReadStream, promises as fs } from "node:fs";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import type { StorageProvider, GetObjectResult, HeadObjectResult } from "./types.js";
import { notFound, badRequest, conflict } from "../errors.js";

function normalizeObjectKey(objectKey: string): string {
  const normalized = objectKey.replace(/\\/g, "/").trim();
  if (!normalized || normalized.startsWith("/")) {
    throw badRequest("Invalid object key");
  }

  const parts = normalized.split("/").filter((part) => part.length > 0);
  if (parts.length === 0 || parts.some((part) => part === "." || part === "..")) {
    throw badRequest("Invalid object key");
  }

  return parts.join("/");
}

function resolveWithin(baseDir: string, objectKey: string): string {
  const normalizedKey = normalizeObjectKey(objectKey);
  const resolved = path.resolve(baseDir, normalizedKey);
  const base = path.resolve(baseDir);
  if (resolved !== base && !resolved.startsWith(base + path.sep)) {
    throw badRequest("Invalid object key path");
  }
  return resolved;
}

async function statOrNull(filePath: string) {
  try {
    return await fs.stat(filePath);
  } catch {
    return null;
  }
}

export function createLocalDiskStorageProvider(baseDir: string): StorageProvider {
  const root = path.resolve(baseDir);

  return {
    id: "local_disk",
    backendId: `local_disk:${root}`,
    shared: false,

    async putObject(input) {
      const targetPath = resolveWithin(root, input.objectKey);
      const dir = path.dirname(targetPath);
      await fs.mkdir(dir, { recursive: true });

      if (input.ifNoneMatch && await statOrNull(targetPath)) {
        throw conflict("Object already exists");
      }

      const tempPath = `${targetPath}.tmp-${randomUUID()}`;
      try {
        if (Buffer.isBuffer(input.body)) {
          await fs.writeFile(tempPath, input.body);
        } else {
          await pipeline(input.body, (await fs.open(tempPath, "wx")).createWriteStream());
        }
        if (input.ifNoneMatch) {
          try {
            await fs.link(tempPath, targetPath);
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code === "EEXIST") throw conflict("Object already exists");
            throw error;
          }
          await fs.unlink(tempPath);
        } else {
          await fs.rename(tempPath, targetPath);
        }
      } catch (error) {
        await fs.unlink(tempPath).catch(() => undefined);
        throw error;
      }
      return { checksumSha256: input.checksumSha256 };
    },

    async getObject(input): Promise<GetObjectResult> {
      const filePath = resolveWithin(root, input.objectKey);
      const stat = await statOrNull(filePath);
      if (!stat || !stat.isFile()) {
        throw notFound("Object not found");
      }
      const streamOptions = input.range
        ? { start: input.range.start, end: input.range.end }
        : undefined;
      const contentLength = input.range
        ? input.range.end - input.range.start + 1
        : stat.size;
      return {
        stream: createReadStream(filePath, streamOptions),
        contentLength,
        lastModified: stat.mtime,
      };
    },

    async headObject(input): Promise<HeadObjectResult> {
      const filePath = resolveWithin(root, input.objectKey);
      const stat = await statOrNull(filePath);
      if (!stat || !stat.isFile()) {
        return { exists: false };
      }
      return {
        exists: true,
        contentLength: stat.size,
        lastModified: stat.mtime,
      };
    },

    async deleteObject(input): Promise<void> {
      const filePath = resolveWithin(root, input.objectKey);
      try {
        await fs.unlink(filePath);
      } catch {
        // idempotent delete
      }
    },
  };
}
