import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import express from "express";
import request from "supertest";
import type { Db } from "@paperclipai/db";
import { healthRoutes } from "../routes/health.js";
import * as devServerStatus from "../dev-server-status.js";
import { serverVersion } from "../version.js";

const mockReadPersistedDevServerStatus = vi.hoisted(() => vi.fn());
const testServerInfo = {
  processStartedAt: "2026-06-26T00:00:00.000Z",
  git: {
    available: true,
    fullSha: "0123456789abcdef0123456789abcdef01234567",
    shortSha: "0123456",
    branchName: "master",
    subject: "Add server info debug view",
    committedAt: "2026-06-25T23:00:00.000Z",
    localChanges: {
      available: true,
      hasLocalChanges: false,
      stagedFileCount: 0,
      unstagedFileCount: 0,
      untrackedFileCount: 0,
    },
  },
} as const;

function createHealthyDb(): Db {
  return {
    execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
  } as unknown as Db;
}

vi.mock("../dev-server-status.js", () => ({
  readPersistedDevServerStatus: mockReadPersistedDevServerStatus,
  toDevServerHealthStatus: vi.fn(),
}));

function createApp(
  db?: Db,
  serverInfo = testServerInfo,
  databaseBackupHealth?: Parameters<typeof healthRoutes>[1]["databaseBackupHealth"],
  runtimeEnv?: Parameters<typeof healthRoutes>[1]["runtimeEnv"],
) {
  const app = express();
  app.use(
    "/health",
    healthRoutes(db, {
      deploymentMode: "local_trusted",
      deploymentExposure: "private",
      authReady: true,
      companyDeletionEnabled: true,
      serverInfo,
      databaseBackupHealth,
      runtimeEnv,
    }),
  );
  return app;
}

describe("GET /health", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReadPersistedDevServerStatus.mockReturnValue(undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  });
  it("returns 200 with status ok", async () => {
    const app = createApp();
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: "ok", version: serverVersion, serverVersion: serverVersion, commit: testServerInfo.git.fullSha, serverInfo: testServerInfo });
  }, 15_000);

  it("keeps the self-hosted health response byte-identical and omits cloud", async () => {
    const app = createApp(undefined, testServerInfo, undefined, {});

    const res = await request(app).get("/health");

    const baseline = {
      status: "ok",
      version: serverVersion,
      serverVersion,
      commit: testServerInfo.git.fullSha,
      serverInfo: testServerInfo,
    };
    expect(res.text).toBe(JSON.stringify(baseline));
    expect(Object.prototype.hasOwnProperty.call(res.body, "cloud")).toBe(false);
  });

  it("exposes public stack metadata on cloud-simulated health", async () => {
    const app = createApp(undefined, testServerInfo, undefined, {
      PAPERCLIP_CLOUD_TENANT_SERVER_TOKEN: "tenant-token",
      PAPERCLIP_CLOUD_STACK_ID: "stack-1",
      PAPERCLIP_STACK_SLUG: "acme",
      PAPERCLIP_CLOUD_ACCOUNT_GROUP_ID: "account-group-1",
      PAPERCLIP_PRIMARY_HOST: "acme.paperclip.app",
      PAPERCLIP_CLOUD_API_ORIGIN: "https://app.paperclip.app",
    });

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body.cloud).toEqual({
      managed: true,
      managedBy: "paperclip-cloud",
      stackSlug: "acme",
      cloudBaseUrl: "https://app.paperclip.app",
    });
  });

  it("lists operator-hidden settings and drops unknown keys", async () => {
    const app = createApp(undefined, testServerInfo, undefined, {
      PAPERCLIP_HIDDEN_SETTINGS: "instance.plugins,instance.adapters,instance.bogus",
    });

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body.hiddenSettings).toEqual(["instance.plugins", "instance.adapters"]);
  });

  it("omits hiddenSettings entirely when nothing is hidden", async () => {
    const app = createApp(undefined, testServerInfo, undefined, {});

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(Object.prototype.hasOwnProperty.call(res.body, "hiddenSettings")).toBe(false);
  });

  it("returns 200 when the database probe succeeds", async () => {
    const db = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
    } as unknown as Db;
    const app = createApp(db);

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(db.execute).toHaveBeenCalledTimes(1);
    expect(res.body).toMatchObject({
      status: "ok",
      version: serverVersion,
      serverInfo: testServerInfo,
    });
  });

  it("returns 503 when the database probe fails", async () => {
    const db = {
      execute: vi.fn().mockRejectedValue(new Error("connect ECONNREFUSED")),
    } as unknown as Db;
    const app = createApp(db);

    const res = await request(app).get("/health");

    expect(res.status).toBe(503);
    expect(res.body).toEqual({
      status: "unhealthy",
      version: serverVersion,
      serverVersion,
      commit: testServerInfo.git.fullSha,
      error: "database_unreachable",
      serverInfo: testServerInfo,
      eventLoopLag: res.body.eventLoopLag,
    });
    expect(res.body.eventLoopLag).toMatchObject({ worstMaxMs: expect.any(Number) });
  });

  it("stays ready under main-pool starvation when the dedicated probe connection answers", async () => {
    // Simulated pool pressure: every main-pool query hangs (all connections
    // held by a long maintenance statement). The dedicated probe client still
    // answers, so health must stay 200 instead of timing out into a 503.
    const starvedDb = {
      execute: vi.fn(() => new Promise(() => {})),
    } as unknown as Db;
    const probeDb = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
    } as unknown as Db;
    const app = express();
    app.use(
      "/health",
      healthRoutes(starvedDb, {
        deploymentMode: "local_trusted",
        deploymentExposure: "private",
        authReady: true,
        companyDeletionEnabled: true,
        serverInfo: testServerInfo,
        probeDb,
      }),
    );

    const started = Date.now();
    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body.status).toBe("ok");
    expect(probeDb.execute).toHaveBeenCalledTimes(1);
    expect(starvedDb.execute).not.toHaveBeenCalled();
    // Responsive: no waiting out the probe timeout.
    expect(Date.now() - started).toBeLessThan(2500);
  });

  it("degrades instead of hard-failing when the probe was recently healthy", async () => {
    let healthy = true;
    const db = {
      execute: vi.fn(() => (healthy ? Promise.resolve([{ "?column?": 1 }]) : new Promise(() => {}))),
    } as unknown as Db;
    const app = createApp(db);

    const ok = await request(app).get("/health");
    expect(ok.status).toBe(200);

    healthy = false;
    const degraded = await request(app).get("/health");
    expect(degraded.status).toBe(200);
    expect(degraded.body).toMatchObject({
      status: "degraded",
      warning: "database_slow",
    });
  }, 15_000);

  it("returns safe server info fallbacks when git metadata is unavailable", async () => {
    const app = createApp(undefined, {
      processStartedAt: "2026-06-26T00:00:00.000Z",
      git: {
        available: false,
        unavailableReason: "git_unavailable",
      },
    });

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body.serverInfo).toEqual({
      processStartedAt: "2026-06-26T00:00:00.000Z",
      git: {
        available: false,
        unavailableReason: "git_unavailable",
      },
    });
    // With no git metadata baked in, the exposed commit is null (not omitted).
    expect(res.body.commit).toBeNull();
  });

  it("surfaces a stale database backup warning in full health details", async () => {
    const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "paperclip-health-backups-"));
    const backupFile = path.join(backupDir, "paperclip-20260705-031702.sql.gz");
    fs.writeFileSync(backupFile, "backup");
    fs.utimesSync(
      backupFile,
      new Date("2026-07-05T03:17:02.000Z"),
      new Date("2026-07-05T03:17:02.000Z"),
    );
    const app = createApp(createHealthyDb(), testServerInfo, {
      enabled: true,
      backupDir,
      maxAgeHours: 26,
      now: new Date("2026-07-06T13:00:00.000Z"),
    });

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body.databaseBackup).toMatchObject({
      status: "warning",
      backupDir,
      maxAgeHours: 26,
      latestBackup: {
        name: "paperclip-20260705-031702.sql.gz",
        ageHours: 33.7,
      },
      warnings: [
        {
          code: "database_backup_stale",
        },
      ],
    });
    expect(res.body.warnings).toEqual(res.body.databaseBackup.warnings);
  });

  it("surfaces database backup failure markers in full health details", async () => {
    const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "paperclip-health-backups-"));
    const backupFile = path.join(backupDir, "paperclip-20260706-031702.sql.gz");
    const alertFile = path.join(backupDir, "db-backup-to-s3.failure");
    fs.writeFileSync(backupFile, "backup");
    fs.writeFileSync(alertFile, "db-backup-to-s3 failed at 2026-07-06T03:17:00.000Z exit=1\n");
    const app = createApp(createHealthyDb(), testServerInfo, {
      enabled: true,
      backupDir,
      maxAgeHours: 26,
      alertFile,
      now: new Date("2026-07-06T04:00:00.000Z"),
    });

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body.databaseBackup).toMatchObject({
      status: "warning",
      lastFailure: {
        path: alertFile,
        message: "db-backup-to-s3 failed at 2026-07-06T03:17:00.000Z exit=1",
      },
      warnings: [
        {
          code: "database_backup_last_failure",
          message: "db-backup-to-s3 failed at 2026-07-06T03:17:00.000Z exit=1",
        },
      ],
    });
  });

  it("finds conventional database backup failure markers without an explicit alert file", async () => {
    const backupRoot = fs.mkdtempSync(path.join(os.tmpdir(), "paperclip-health-backups-root-"));
    const backupDir = path.join(backupRoot, "backups");
    fs.mkdirSync(backupDir);
    const backupFile = path.join(backupDir, "paperclip-20260706-031702.sql.gz");
    const alertFile = path.join(backupRoot, "db-backup-to-s3.failure");
    fs.writeFileSync(backupFile, "backup");
    fs.writeFileSync(alertFile, "db-backup-to-s3 failed beside backups\n");
    const app = createApp(createHealthyDb(), testServerInfo, {
      enabled: true,
      backupDir,
      maxAgeHours: 26,
      now: new Date("2026-07-06T04:00:00.000Z"),
    });

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body.databaseBackup).toMatchObject({
      status: "warning",
      lastFailure: {
        path: alertFile,
        message: "db-backup-to-s3 failed beside backups",
      },
      warnings: [
        {
          code: "database_backup_last_failure",
          message: "db-backup-to-s3 failed beside backups",
        },
      ],
    });
  });

  it("surfaces redacted database backup warnings for anonymous authenticated probes", async () => {
    const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "paperclip-health-redacted-backups-"));
    const backupFile = path.join(backupDir, "paperclip-20260705-031702.sql.gz");
    fs.writeFileSync(backupFile, "backup");
    fs.utimesSync(
      backupFile,
      new Date("2026-07-05T03:17:02.000Z"),
      new Date("2026-07-05T03:17:02.000Z"),
    );
    const { healthRoutes } = await import("../routes/health.js");
    const db = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
      select: vi.fn(() => ({
        from: vi.fn(() => ({
          where: vi.fn().mockResolvedValue([{ count: 1 }]),
        })),
      })),
    } as unknown as Db;
    const app = express();
    app.use((req, _res, next) => {
      (req as any).actor = { type: "none", source: "none" };
      next();
    });
    app.use(
      "/health",
      healthRoutes(db, {
        deploymentMode: "authenticated",
        deploymentExposure: "public",
        authReady: true,
        companyDeletionEnabled: false,
        serverInfo: testServerInfo,
        databaseBackupHealth: {
          enabled: true,
          backupDir,
          maxAgeHours: 26,
          now: new Date("2026-07-06T13:00:00.000Z"),
        },
      }),
    );

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      status: "ok",
      deploymentMode: "authenticated",
      deploymentExposure: "public",
      localAiLoginSupported: false,
      commit: testServerInfo.git.fullSha,
      bootstrapStatus: "ready",
      bootstrapInviteActive: false,
      databaseBackup: {
        enabled: true,
        status: "warning",
        warnings: [
          {
            code: "database_backup_stale",
            message: "Latest database backup is stale.",
          },
        ],
      },
      warnings: [
        {
          code: "database_backup_stale",
          message: "Latest database backup is stale.",
        },
      ],
    });
  });

  it("redacts detailed metadata for anonymous requests in authenticated mode", async () => {
    const devServerStatus = await import("../dev-server-status.js");
    vi.spyOn(devServerStatus, "readPersistedDevServerStatus").mockReturnValue(undefined);
    const { healthRoutes } = await import("../routes/health.js");
    const db = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
      select: vi.fn(() => ({
        from: vi.fn(() => ({
          where: vi.fn().mockResolvedValue([{ count: 1 }]),
        })),
      })),
    } as unknown as Db;
    const app = express();
    app.use((req, _res, next) => {
      (req as any).actor = { type: "none", source: "none" };
      next();
    });
    app.use(
      "/health",
      healthRoutes(db, {
        deploymentMode: "authenticated",
        deploymentExposure: "public",
        authReady: true,
        companyDeletionEnabled: false,
        serverInfo: testServerInfo,
      }),
    );

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      status: "ok",
      deploymentMode: "authenticated",
      deploymentExposure: "public",
      localAiLoginSupported: false,
      commit: testServerInfo.git.fullSha,
      bootstrapStatus: "ready",
      bootstrapInviteActive: false,
    });
    expect(res.body.serverInfo).toBeUndefined();
  });

  it("redacts detailed metadata when authenticated mode is reached without auth middleware", async () => {
    const devServerStatus = await import("../dev-server-status.js");
    vi.spyOn(devServerStatus, "readPersistedDevServerStatus").mockReturnValue(undefined);
    const { healthRoutes } = await import("../routes/health.js");
    const db = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
      select: vi.fn(() => ({
        from: vi.fn(() => ({
          where: vi.fn().mockResolvedValue([{ count: 1 }]),
        })),
      })),
    } as unknown as Db;
    const app = express();
    app.use(
      "/health",
      healthRoutes(db, {
        deploymentMode: "authenticated",
        deploymentExposure: "public",
        authReady: true,
        companyDeletionEnabled: false,
        serverInfo: testServerInfo,
      }),
    );

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      status: "ok",
      deploymentMode: "authenticated",
      deploymentExposure: "public",
      localAiLoginSupported: false,
      commit: testServerInfo.git.fullSha,
      bootstrapStatus: "ready",
      bootstrapInviteActive: false,
    });
    expect(res.body.serverInfo).toBeUndefined();
  });

  it("keeps detailed metadata for authenticated requests in authenticated mode", async () => {
    const devServerStatus = await import("../dev-server-status.js");
    vi.spyOn(devServerStatus, "readPersistedDevServerStatus").mockReturnValue(undefined);
    const { healthRoutes } = await import("../routes/health.js");
    const db = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
      select: vi.fn(() => ({
        from: vi.fn(() => ({
          where: vi.fn().mockResolvedValue([{ count: 1 }]),
        })),
      })),
    } as unknown as Db;
    const app = express();
    app.use((req, _res, next) => {
      (req as any).actor = { type: "board", userId: "user-1", source: "session" };
      next();
    });
    app.use(
      "/health",
      healthRoutes(db, {
        deploymentMode: "authenticated",
        deploymentExposure: "public",
        authReady: true,
        companyDeletionEnabled: false,
        serverInfo: testServerInfo,
      }),
    );

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      status: "ok",
      version: serverVersion,
      serverVersion,
      deploymentMode: "authenticated",
      deploymentExposure: "public",
      authReady: true,
      bootstrapStatus: "ready",
      bootstrapInviteActive: false,
      features: {
        companyDeletionEnabled: false,
      },
      serverInfo: testServerInfo,
    });
  });

  it("reports bootstrap_pending in authenticated mode when no instance admin exists", async () => {
    const { healthRoutes } = await import("../routes/health.js");
    const db = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
      select: vi.fn(() => ({
        from: vi.fn(() => ({
          where: vi.fn().mockResolvedValue([{ count: 0 }]),
        })),
      })),
    } as unknown as Db;
    const app = express();
    app.use((req, _res, next) => {
      (req as any).actor = { type: "none", source: "none" };
      next();
    });
    app.use(
      "/health",
      healthRoutes(db, {
        deploymentMode: "authenticated",
        deploymentExposure: "public",
        authReady: true,
        companyDeletionEnabled: false,
        serverInfo: testServerInfo,
      }),
    );

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      status: "ok",
      bootstrapStatus: "bootstrap_pending",
      bootstrapInviteActive: false,
    });
  });

  it("reports bootstrapStatus ready for cloud-managed instances regardless of instance admin count", async () => {
    vi.stubEnv("PAPERCLIP_CLOUD_TENANT_SERVER_TOKEN", "test-tenant-server-token");
    const { healthRoutes } = await import("../routes/health.js");
    const db = {
      execute: vi.fn().mockResolvedValue([{ "?column?": 1 }]),
      select: vi.fn(() => ({
        from: vi.fn(() => ({
          where: vi.fn().mockResolvedValue([{ count: 0 }]),
        })),
      })),
    } as unknown as Db;
    const app = express();
    app.use((req, _res, next) => {
      (req as any).actor = { type: "none", source: "none" };
      next();
    });
    app.use(
      "/health",
      healthRoutes(db, {
        deploymentMode: "authenticated",
        deploymentExposure: "public",
        authReady: true,
        companyDeletionEnabled: false,
        serverInfo: testServerInfo,
      }),
    );

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      status: "ok",
      bootstrapStatus: "ready",
      bootstrapInviteActive: false,
    });
  });
});

describe("GET /health/auth-db", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("reports ready without requiring an actor when the probe query succeeds", async () => {
    const db = createHealthyDb();
    const app = createApp(db);

    const res = await request(app).get("/health/auth-db");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: "ready" });
    expect(db.execute).toHaveBeenCalledTimes(1);
  });

  it("reports not_ready without a database configured", async () => {
    const app = createApp(undefined);

    const res = await request(app).get("/health/auth-db");

    expect(res.status).toBe(503);
    expect(res.body).toEqual({ status: "not_ready" });
  });

  it("reports not_ready and does not leak error detail when the probe query fails", async () => {
    const db = {
      execute: vi.fn().mockRejectedValue(new Error("write CONNECTION_CLOSED db.internal:5432")),
    } as unknown as Db;
    const app = createApp(db);

    const res = await request(app).get("/health/auth-db");

    expect(res.status).toBe(503);
    expect(res.body).toEqual({ status: "not_ready" });
    expect(JSON.stringify(res.body)).not.toMatch(/CONNECTION_CLOSED|db\.internal/);
  });

  it("reports not_ready within a bounded time when the probe query never settles", async () => {
    const db = {
      execute: vi.fn(() => new Promise(() => {})),
    } as unknown as Db;
    const app = createApp(db);

    const startedAt = Date.now();
    const res = await request(app).get("/health/auth-db");
    const elapsedMs = Date.now() - startedAt;

    expect(res.status).toBe(503);
    expect(res.body).toEqual({ status: "not_ready" });
    // Bounded by the probe's own tighter timeout (HEALTH_DB_PROBE_TIMEOUT_MS,
    // 3s), not authDbLookup's general-purpose default (5s) — and nowhere near
    // "forever".
    expect(elapsedMs).toBeLessThan(4_000);
  }, 6_000);
});
