import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { gunzipSync } from "node:zlib";
import { afterEach, expect, it, vi } from "vitest";
import { runDatabaseBackup } from "./backup-lib.js";

// This fixture exercises the actual child process, filesystem, and gzip stream.
// Only the database readiness query is replaced; dump content comes from the child.
vi.mock("postgres", () => ({
  default: () => Object.assign(async () => [], { end: async () => {} }),
}));
const directories: string[] = [];
const original = process.env.PAPERCLIP_PG_DUMP_PATH;
afterEach(() => {
  if (original === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
  else process.env.PAPERCLIP_PG_DUMP_PATH = original;
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
it("publishes exactly one private completed backup only after the producer exits", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "backup-publication-"));
  directories.push(directory);
  const backupDir = path.join(directory, "backups");
  const ready = path.join(directory, "ready");
  const release = path.join(directory, "release");
  const executable = path.join(directory, "pg_dump");
  fs.writeFileSync(executable, `#!${process.execPath}
const fs = require('node:fs');
process.stdout.write('-- fixture dump\\n');
fs.writeFileSync(${JSON.stringify(ready)}, 'ready');
const timer = setInterval(() => {
  if (fs.existsSync(${JSON.stringify(release)})) {
    clearInterval(timer);
    process.stdout.end('SELECT 1;\\n');
  }
}, 5);
`, { mode: 0o700 });
  process.env.PAPERCLIP_PG_DUMP_PATH = executable;
  const pending = runDatabaseBackup({
    connectionString: "postgresql://fixture@localhost/example",
    backupDir,
    backupEngine: "pg_dump",
    retention: { dailyDays: 7, weeklyWeeks: 4, monthlyMonths: 1 },
  });
  try {
    await vi.waitFor(() => expect(fs.existsSync(ready)).toBe(true));
    const inProgress = fs.readdirSync(backupDir);
    expect(inProgress.length).toBeGreaterThan(0);
    expect(inProgress.every((name) => name.endsWith(".partial"))).toBe(true);
    for (const name of inProgress) expect(fs.statSync(path.join(backupDir, name)).mode & 0o777).toBe(0o600);
  } finally {
    fs.writeFileSync(release, "release");
  }
  const result = await pending;
  expect(fs.readdirSync(backupDir)).toEqual([path.basename(result.backupFile)]);
  expect(result.backupFile.endsWith(".sql.gz")).toBe(true);
  expect(gunzipSync(fs.readFileSync(result.backupFile)).toString()).toBe("-- fixture dump\nSELECT 1;\n");
  expect(fs.statSync(result.backupFile).mode & 0o777).toBe(0o600);
});
