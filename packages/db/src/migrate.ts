import { applyPendingMigrations, inspectMigrations, type MigrationProgress } from "./client.js";
import { MigrationCoordinator } from "./migration-coordinator.js";
import { resolveMigrationConnection } from "./migration-runtime.js";

async function main(progress?: MigrationProgress): Promise<void> {
  const resolved = await resolveMigrationConnection();

  console.log(`Migrating database via ${resolved.source}`);

  try {
    const runMigrations = async () => {
      progress?.("inspect");
      const before = await inspectMigrations(resolved.connectionString);
      if (before.status === "upToDate") {
        console.log("No pending migrations");
        return;
      }

      console.log(`Applying ${before.pendingMigrations.length} pending migration(s)...`);
      await applyPendingMigrations(resolved.connectionString, progress);

      progress?.("verify");
      const after = await inspectMigrations(resolved.connectionString);
      if (after.status !== "upToDate") {
        throw new Error(`Migrations incomplete: ${after.pendingMigrations.join(", ")}`);
      }
      console.log("Migrations complete");
    };

    if (resolved.mode === "postgres") {
      const coordinator = new MigrationCoordinator(resolved.connectionString);
      await coordinator.withExclusiveMigrationLock(runMigrations, {
        timeoutMs: resolved.lockTimeoutMs,
        onStateChange: (state) => {
          console.log(`Migration coordination: ${state} (lock ${coordinator.lockId})`);
        },
      });
    } else {
      await runMigrations();
    }
  } finally {
    await resolved.stop();
  }
}

if (process.env.PAPERCLIP_MIGRATION_DIAGNOSTICS !== "1") {
  await main();
} else {
  let last: { phase: string; file?: string; statement?: number } = { phase: "connect" };
  try {
    await main((phase, file, statement) => {
      last = { phase, file, statement };
      console.error(JSON.stringify({ event: "migration-progress", ...last }));
    });
  } catch (error) {
    // Coordinator cleanup can wrap the primary error. Never serialize driver
    // errors: their message, detail, query or parameters can contain row data.
    const sqlstate = primarySqlstate(error);
    console.error(JSON.stringify({ event: "migration-failed", last, sqlstate,
      reason: sqlstate === "08P01" ? "protocol violation" : "migration failed" }));
    process.exitCode = 1;
  }
}

function primarySqlstate(error: unknown): string | null {
  const seen = new Set<unknown>();
  while (error instanceof Error && !seen.has(error)) {
    seen.add(error);
    if ("code" in error && typeof error.code === "string" && /^[A-Z0-9]{5}$/.test(error.code)) return error.code;
    error = error instanceof AggregateError ? error.errors[0] : error.cause;
  }
  return null;
}
