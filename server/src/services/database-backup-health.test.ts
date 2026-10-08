import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { gzipSync } from "node:zlib";
import { afterEach, expect, it } from "vitest";
import { inspectDatabaseBackupHealth } from "./database-backup-health.js";

const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
it("does not let an unfinished, empty, or directory-shaped backup hide a stale completed backup", () => {
  const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "backup-health-"));
  directories.push(backupDir);
  const old = path.join(backupDir, "old.sql.gz");
  fs.writeFileSync(old, gzipSync("SELECT 1;\n"));
  fs.utimesSync(old, new Date("2026-01-01"), new Date("2026-01-01"));
  fs.writeFileSync(path.join(backupDir, "in-progress.sql.gz.partial"), gzipSync("SELECT 2;\n"));
  fs.writeFileSync(path.join(backupDir, "empty.sql.gz"), gzipSync(""));
  fs.mkdirSync(path.join(backupDir, "directory.sql.gz"));
  const status = inspectDatabaseBackupHealth({ enabled: true, backupDir, maxAgeHours: 24, now: new Date("2026-01-03") });
  expect(status.latestBackup?.name).toBe("old.sql.gz");
  expect(status.warnings.map(({ code }) => code)).toContain("database_backup_stale");
  fs.renameSync(path.join(backupDir, "in-progress.sql.gz.partial"), path.join(backupDir, "completed.sql.gz"));
  expect(inspectDatabaseBackupHealth({ enabled: true, backupDir, maxAgeHours: 24 }).status).toBe("ok");
});
