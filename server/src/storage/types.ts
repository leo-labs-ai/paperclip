import type { StorageProvider as StorageProviderId } from "@paperclipai/shared";
import type { Readable } from "node:stream";
import type { DurableObjectKind, DurableObjectMetadata } from "./object-store.js";

export interface PutObjectInput {
  objectKey: string;
  // Readable bodies stream straight to the backend (contentLength must be the
  // exact byte size); Buffer stays supported for small payloads.
  body: Buffer | Readable;
  contentType: string;
  contentLength: number;
  /** Base64-encoded SHA-256 of the exact object bytes. */
  checksumSha256?: string;
  /** Fail instead of replacing an object that already exists. */
  ifNoneMatch?: boolean;
}

export interface PutObjectResult {
  etag?: string;
  version?: string;
  checksumSha256?: string;
}

export interface DeleteObjectInput {
  objectKey: string;
  /** Delete only this immutable backend version when supported. */
  version?: string;
}

export interface GetObjectInput {
  objectKey: string;
  /** Read an immutable backend version when supported. */
  version?: string;
  // S3 reads cancel pending requests and their response streams.
  signal?: AbortSignal;
  range?: {
    start: number;
    end: number;
  };
}

export interface GetObjectResult {
  stream: Readable;
  contentType?: string;
  contentLength?: number;
  etag?: string;
  version?: string;
  checksumSha256?: string;
  lastModified?: Date;
}

export interface HeadObjectResult {
  exists: boolean;
  contentType?: string;
  contentLength?: number;
  etag?: string;
  version?: string;
  checksumSha256?: string;
  lastModified?: Date;
}

export interface StorageProvider {
  id: StorageProviderId;
  /** Stable identity for the configured backend, including bucket/root and prefix. */
  backendId: string;
  shared: boolean;
  putObject(input: PutObjectInput): Promise<PutObjectResult>;
  getObject(input: GetObjectInput): Promise<GetObjectResult>;
  headObject(input: GetObjectInput): Promise<HeadObjectResult>;
  deleteObject(input: DeleteObjectInput): Promise<void>;
}

export type PutFileInput = {
  /** Server-allocated, company-prefixed key for durable idempotent uploads. Never accept from client input. */
  objectKey?: string;
  companyId: string;
  /** Commit integrity metadata when this upload is a durable HA artifact. */
  durableKind?: DurableObjectKind;
  namespace: string;
  originalFilename: string | null;
  contentType: string;
} & ({ body: Buffer } | { body: Readable; byteSize: number; sha256: string });

export interface PutFileResult {
  provider: StorageProviderId;
  objectKey: string;
  contentType: string;
  byteSize: number;
  sha256: string;
  originalFilename: string | null;
  durableObject?: DurableObjectMetadata;
}

export interface StorageService {
  provider: StorageProviderId;
  shared: boolean;
  putFile(input: PutFileInput): Promise<PutFileResult>;
  getObject(companyId: string, objectKey: string, options?: Pick<GetObjectInput, "range">): Promise<GetObjectResult>;
  headObject(companyId: string, objectKey: string): Promise<HeadObjectResult>;
  deleteObject(companyId: string, objectKey: string): Promise<void>;
}
