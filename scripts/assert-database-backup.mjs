// Run with the image's installed tsx loader in the disposable verification network.
// The image ships database source modules, not packages/db/dist. No production config.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, basename } from "node:path";
import { gunzipSync } from "node:zlib";
import { runDatabaseBackup, runDatabaseRestore } from "/app/packages/db/src/backup-lib.ts";

const host = process.env.PAPERCLIP_BACKUP_PROOF_HOST || "backup-db";
assert.ok(host === "backup-db" || host === "127.0.0.1", "Use only the disposable proof database");
const sourceUrl = new URL(`postgresql://postgres@${host}/postgres?application_name=backup-image-proof`);
sourceUrl.password = "synthetic-proof-password";
const source = sourceUrl.href;
const targetUrl = new URL(source);
targetUrl.pathname = "/backup_restore";
const target = targetUrl.href;
const query = (connectionString, sql) => execFileSync("psql", ["--dbname", connectionString, "--no-psqlrc", "--set=ON_ERROR_STOP=1", "-tAc", sql], {
  env: { ...process.env, PGCONNECT_TIMEOUT: "10" }, encoding: "utf8",
}).trim();
assert.match(execFileSync("pg_dump", ["--version"], { encoding: "utf8" }), /PostgreSQL\) 17\./);
query(source, "CREATE TABLE backup_fixture (id integer PRIMARY KEY); INSERT INTO backup_fixture VALUES (42);");
query(source, "CREATE DATABASE backup_restore");
const backupDir = mkdtempSync(join(tmpdir(), "backup-image-proof-"));
try {
  const result = await runDatabaseBackup({ connectionString: source, backupDir, backupEngine: "pg_dump", retention: { dailyDays: 7, weeklyWeeks: 4, monthlyMonths: 1 } });
  assert.deepEqual(readdirSync(backupDir), [basename(result.backupFile)]);
  assert.equal(statSync(result.backupFile).mode & 0o777, 0o600);
  assert.match(gunzipSync(readFileSync(result.backupFile)).toString(), /PostgreSQL database dump/);
  await runDatabaseRestore({ connectionString: target, backupFile: result.backupFile });
  assert.equal(query(target, "SELECT id FROM backup_fixture"), "42");
  console.log("PostgreSQL 17 image backup/restore, gzip integrity, and private publication passed.");
} finally {
  rmSync(backupDir, { recursive: true, force: true });
}
