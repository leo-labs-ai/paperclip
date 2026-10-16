import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { gunzipSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Transform } from "node:stream";
import { runPgDumpBackup } from "./backup-lib.js";

const compression = vi.hoisted(() => ({ fail: false }));
vi.mock("node:zlib", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:zlib")>();
  return {
    ...original,
    createGzip: (...args: Parameters<typeof original.createGzip>) => compression.fail
      ? new Transform({ transform(_chunk, _encoding, callback) { callback(new Error("synthetic gzip failure")); } })
      : original.createGzip(...args),
  };
});
const directories: string[] = [];
const original = process.env.PAPERCLIP_PG_DUMP_PATH;
afterEach(() => {
  compression.fail = false;
  if (original === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
  else process.env.PAPERCLIP_PG_DUMP_PATH = original;
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});
function fixture(script?: string) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "paperclip-pipeline-"));
  directories.push(directory);
  const executable = path.join(directory, "pg_dump");
  if (script) fs.writeFileSync(executable, `#!${process.execPath}\n${script}`, { mode: 0o700 });
  process.env.PAPERCLIP_PG_DUMP_PATH = executable;
  return { directory, backupFile: path.join(directory, "backup.sql.gz.partial") };
}
const connectionString = "postgresql://fixture:synthetic-password@localhost/example?sslmode=require";

describe("pg_dump pipeline", () => {
  it("drains a failed spawn before cleanup can race a late output open", async () => {
    const options = fixture();
    await expect(runPgDumpBackup({ ...options, connectionString, connectTimeout: 1 })).rejects.toThrow();
    fs.rmSync(options.backupFile, { force: true });
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(fs.existsSync(options.backupFile)).toBe(false);
  });

  it("preserves the established native connection argument and creates a private completed gzip", async () => {
    const options = fixture(`
if (!process.argv.includes(${JSON.stringify("--dbname=" + connectionString)})) process.exit(3);
process.stdout.write('-- fixture dump\\nSELECT 1;\\n');
`);
    await runPgDumpBackup({ ...options, connectionString, connectTimeout: 1 });
    expect(gunzipSync(fs.readFileSync(options.backupFile)).toString()).toBe("-- fixture dump\nSELECT 1;\n");
    expect(fs.statSync(options.backupFile).mode & 0o777).toBe(0o600);
  });

  it("drains and terminates the producer on compression failure", async () => {
    const options = fixture("setInterval(() => process.stdout.write('SQL'), 5);");
    compression.fail = true;
    await expect(runPgDumpBackup({ ...options, connectionString, connectTimeout: 1 })).rejects.toThrow("synthetic gzip failure");
    fs.rmSync(options.backupFile, { force: true });
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(fs.existsSync(options.backupFile)).toBe(false);
  });

  it("rejects an empty successful dump", async () => {
    const options = fixture("process.exitCode = 0;");
    await expect(runPgDumpBackup({ ...options, connectionString, connectTimeout: 1 })).rejects.toThrow("empty backup");
  });

  it("rejects a nonzero process even when its gzip stream completed", async () => {
    const options = fixture("process.stdout.write('partial SQL'); process.exitCode = 7;");
    await expect(runPgDumpBackup({ ...options, connectionString, connectTimeout: 1 })).rejects.toThrow("exit code 7");
  });

  it("terminates and drains the producer when the output cannot be opened", async () => {
    const options = fixture("setInterval(() => process.stdout.write('SQL'), 5);");
    fs.mkdirSync(options.backupFile);
    await expect(runPgDumpBackup({ ...options, connectionString, connectTimeout: 1 })).rejects.toThrow();
  });
});
