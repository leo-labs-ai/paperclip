import { readConfigFile, resolveDeploymentMode } from "./config-file.js";
import { parseChatWebhookPublicBaseUrl } from "./chat-webhook-public-url.js";
import { execFileSync } from "node:child_process";
import {
  loadDatabaseEnvironment,
  resolveDatabaseEnvironment,
  resolveDefinedDatabaseUrl,
} from "@paperclipai/db/runtime-config";
import { maybeRepairLegacyWorktreeConfigAndEnvFiles } from "./worktree-config.js";
import { basename, dirname } from "node:path";
import { resolvePaperclipEnvPath } from "./paths.js";
import { applyEmptyWorktreeSigningSecrets } from "./dev-runner-worktree.js";
import {
  AUTH_BASE_URL_MODES,
  BIND_MODES,
  DEPLOYMENT_EXPOSURES,
  SECRET_PROVIDERS,
  STORAGE_PROVIDERS,
  type BindMode,
  type AuthBaseUrlMode,
  type DeploymentExposure,
  type DeploymentMode,
  type SecretProvider,
  type StorageProvider,
  inferBindModeFromHost,
  resolveRuntimeBind,
  validateConfiguredBindMode,
} from "@paperclipai/shared";
import { resolveMigrationConfig, type DeploymentProfile } from "@paperclipai/db/migration-config";
import {
  resolveDefaultBackupDir,
  resolveDefaultEmbeddedPostgresDir,
  resolveDefaultSecretsKeyFilePath,
  resolveDefaultStorageDir,
  resolveHomeAwarePath,
} from "./home-paths.js";

loadDatabaseEnvironment();
const PAPERCLIP_ENV_FILE_PATH = resolvePaperclipEnvPath();

if (basename(dirname(PAPERCLIP_ENV_FILE_PATH)) === ".paperclip") {
  applyEmptyWorktreeSigningSecrets(dirname(dirname(PAPERCLIP_ENV_FILE_PATH)));
}
maybeRepairLegacyWorktreeConfigAndEnvFiles();

const TAILSCALE_DETECT_TIMEOUT_MS = 3000;

/**
 * Parse an integer config knob, clamped to `[min, max]` (`max` optional,
 * defaulting to unbounded). Rejects anything that is not a finite integer —
 * `Infinity`, `NaN`, and non-integers like `1.5` all fall back to `def` — so a
 * fat-fingered env value can't produce a nonsensical sweep interval / budget.
 */
function clampIntEnv(
  raw: string | undefined,
  def: number,
  min: number,
  max = Number.POSITIVE_INFINITY,
): number {
  if (raw == null || raw.trim() === "") return def;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed)) return def;
  return Math.min(max, Math.max(min, parsed));
}

/**
 * Ceiling on `PAPERCLIP_RUN_RESULT_RETENTION_BATCH_SIZE`.
 *
 * The retention sweeper binds one query parameter per candidate id, and
 * PostgreSQL's wire protocol caps a statement at 65,535 of them. A batch size
 * above that makes every `trimResultJson` call throw — and because the paging
 * cursor is function-local, the throw kills it, so the next tick restarts from
 * the beginning and dies at exactly the same point. The result is not a slow
 * sweeper but a permanently wedged one: it logs an error every 24h and trims
 * nothing, while the table it exists to bound keeps growing.
 *
 * The realistic route there is an operator impatient with the backlog setting
 * this to something like 100000 to "just do it all at once". Clamping turns
 * that into a large-but-working batch instead of a silent no-op. 5,000 is 25x
 * the default with an order of magnitude of headroom under the protocol limit,
 * and keeps a single batch's transaction — and its WAL burst — bounded.
 */
const RUN_RESULT_RETENTION_MAX_BATCH_SIZE = 5_000;

type DatabaseMode = "embedded-postgres" | "postgres";
export type { DeploymentProfile } from "@paperclipai/db/migration-config";

export interface Config {
  deploymentProfile: DeploymentProfile;
  migrationLockTimeoutMs: number;
  deploymentMode: DeploymentMode;
  deploymentExposure: DeploymentExposure;
  bind: BindMode;
  customBindHost: string | undefined;
  host: string;
  port: number;
  allowedHostnames: string[];
  authBaseUrlMode: AuthBaseUrlMode;
  authPublicBaseUrl: string | undefined;
  chatWebhookPublicBaseUrl: string | undefined;
  authDisableSignUp: boolean;
  databaseMode: DatabaseMode;
  databaseUrl: string | undefined;
  databaseMigrationUrl: string | undefined;
  embeddedPostgresDataDir: string;
  embeddedPostgresPort: number;
  databaseBackupEnabled: boolean;
  databaseBackupIntervalMinutes: number;
  databaseBackupRetentionDays: number;
  databaseBackupDir: string;
  workspaceReaperCooldownDays: number;
  serveUi: boolean;
  uiDevMiddleware: boolean;
  secretsProvider: SecretProvider;
  secretsStrictMode: boolean;
  secretsMasterKeyFilePath: string;
  storageProvider: StorageProvider;
  storageLocalDiskBaseDir: string;
  storageS3Bucket: string;
  storageS3Region: string;
  storageS3Endpoint: string | undefined;
  storageS3Prefix: string;
  storageS3ForcePathStyle: boolean;
  runLogArchiveMode: "auto" | "off" | "s3";
  runLogHotRetentionDays: number;
  runLogCompanyBudgetBytes: number;
  runLogSweepIntervalMs: number;
  runLogSweepItemLimit: number;
  runResultRetentionEnabled: boolean;
  runResultRetentionDays: number;
  runResultRetentionIntervalMs: number;
  runResultRetentionBatchSize: number;
  runResultRetentionItemLimit: number;
  feedbackExportBackendUrl: string | undefined;
  feedbackExportBackendToken: string | undefined;
  heartbeatSchedulerEnabled: boolean;
  heartbeatSchedulerIntervalMs: number;
  companyDeletionEnabled: boolean;
  telemetryEnabled: boolean;
  announcementsEnabled: boolean;
  announcementsFeedUrl: string;
}

function detectTailnetBindHost(): string | undefined {
  const explicit = process.env.PAPERCLIP_TAILNET_BIND_HOST?.trim();
  if (explicit) return explicit;

  try {
    const stdout = execFileSync("tailscale", ["ip", "-4"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      timeout: TAILSCALE_DETECT_TIMEOUT_MS,
    });
    return stdout
      .split(/\r?\n/)
      .map((line) => line.trim())
      .find(Boolean);
  } catch {
    return undefined;
  }
}

export function loadConfig(): Config {
  const fileConfig = readConfigFile();
  const fileDatabaseMode =
    (fileConfig?.database.mode === "postgres" ? "postgres" : "embedded-postgres") as DatabaseMode;

  const fileDbUrl =
    fileDatabaseMode === "postgres"
      ? fileConfig?.database.connectionString
      : undefined;
  const fileDatabaseBackup = fileConfig?.database.backup;
  const fileSecrets = fileConfig?.secrets;
  const fileStorage = fileConfig?.storage;

  const providerFromEnvRaw = process.env.PAPERCLIP_SECRETS_PROVIDER;
  const providerFromEnv =
    providerFromEnvRaw && SECRET_PROVIDERS.includes(providerFromEnvRaw as SecretProvider)
      ? (providerFromEnvRaw as SecretProvider)
      : null;
  const providerFromFile = fileSecrets?.provider;
  const secretsProvider: SecretProvider = providerFromEnv ?? providerFromFile ?? "local_encrypted";

  const storageProviderFromEnvRaw = process.env.PAPERCLIP_STORAGE_PROVIDER;
  const storageProviderFromEnv =
    storageProviderFromEnvRaw && STORAGE_PROVIDERS.includes(storageProviderFromEnvRaw as StorageProvider)
      ? (storageProviderFromEnvRaw as StorageProvider)
      : null;
  const storageProvider: StorageProvider = storageProviderFromEnv ?? fileStorage?.provider ?? "local_disk";
  const storageLocalDiskBaseDir = resolveHomeAwarePath(
    process.env.PAPERCLIP_STORAGE_LOCAL_DIR ??
      fileStorage?.localDisk?.baseDir ??
      resolveDefaultStorageDir(),
  );
  const storageS3Bucket = process.env.PAPERCLIP_STORAGE_S3_BUCKET ?? fileStorage?.s3?.bucket ?? "paperclip";
  const storageS3Region = process.env.PAPERCLIP_STORAGE_S3_REGION ?? fileStorage?.s3?.region ?? "us-east-1";
  const storageS3Endpoint = process.env.PAPERCLIP_STORAGE_S3_ENDPOINT ?? fileStorage?.s3?.endpoint ?? undefined;
  const storageS3Prefix = process.env.PAPERCLIP_STORAGE_S3_PREFIX ?? fileStorage?.s3?.prefix ?? "";
  const storageS3ForcePathStyle =
    process.env.PAPERCLIP_STORAGE_S3_FORCE_PATH_STYLE !== undefined
      ? process.env.PAPERCLIP_STORAGE_S3_FORCE_PATH_STYLE === "true"
      : (fileStorage?.s3?.forcePathStyle ?? false);
  // Run-log cold-archive knobs. `auto` archives only when the app-wide storage
  // provider is object storage (storageProvider === "s3"); `s3` forces the
  // archive leg onto object storage (using the storageS3* settings) even while
  // the app's primary storage stays local_disk; `off` disables the sweeper. Hot
  // files then only age out via the infra janitor backstop. Read here so the
  // scheduler and archiver share one resolved source of truth.
  const runLogArchiveModeRaw = process.env.PAPERCLIP_RUN_LOG_ARCHIVE?.trim().toLowerCase();
  const runLogArchiveMode: "auto" | "off" | "s3" =
    runLogArchiveModeRaw === "off" ? "off" : runLogArchiveModeRaw === "s3" ? "s3" : "auto";
  const runLogHotRetentionDays = clampIntEnv(
    process.env.PAPERCLIP_RUN_LOG_HOT_RETENTION_DAYS,
    30,
    1,
  );
  const runLogCompanyBudgetBytes = clampIntEnv(
    process.env.PAPERCLIP_RUN_LOG_COMPANY_BUDGET_BYTES,
    5 * 1024 * 1024 * 1024,
    1,
  );
  const runLogSweepIntervalMs = clampIntEnv(
    process.env.PAPERCLIP_RUN_LOG_SWEEP_INTERVAL_MS,
    60 * 60 * 1000,
    60_000,
  );
  const runLogSweepItemLimit = clampIntEnv(
    process.env.PAPERCLIP_RUN_LOG_SWEEP_ITEM_LIMIT,
    200,
    1,
  );
  // `heartbeat_runs.result_json` retention. Trims oversized stdout/stderr in
  // place on runs past the window; see services/heartbeat-result-retention.ts
  // for why the column is trimmed rather than nulled. Daily is deliberate — the
  // work is proportional to what aged past the cutoff since the last sweep, and
  // each batch detoasts multi-megabyte values.
  // Opt-out, not opt-in: the bloat it prevents is unbounded, and it only trims
  // output the API has never served. `=== "false"` rather than `!== "true"` so a
  // typo'd value leaves the sweeper running rather than silently off.
  const runResultRetentionEnabled = process.env.PAPERCLIP_RUN_RESULT_RETENTION_ENABLED !== "false";
  const runResultRetentionDays = clampIntEnv(
    process.env.PAPERCLIP_RUN_RESULT_RETENTION_DAYS,
    30,
    1,
  );
  const runResultRetentionIntervalMs = clampIntEnv(
    process.env.PAPERCLIP_RUN_RESULT_RETENTION_INTERVAL_MS,
    24 * 60 * 60 * 1000,
    60_000,
  );
  const runResultRetentionBatchSize = clampIntEnv(
    process.env.PAPERCLIP_RUN_RESULT_RETENTION_BATCH_SIZE,
    200,
    1,
    RUN_RESULT_RETENTION_MAX_BATCH_SIZE,
  );
  const runResultRetentionItemLimit = clampIntEnv(
    process.env.PAPERCLIP_RUN_RESULT_RETENTION_ITEM_LIMIT,
    2000,
    1,
  );
  const feedbackExportBackendUrl =
    process.env.PAPERCLIP_FEEDBACK_EXPORT_BACKEND_URL?.trim() ||
    process.env.PAPERCLIP_TELEMETRY_BACKEND_URL?.trim() ||
    undefined;
  const feedbackExportBackendToken =
    process.env.PAPERCLIP_FEEDBACK_EXPORT_BACKEND_TOKEN?.trim() ||
    process.env.PAPERCLIP_TELEMETRY_BACKEND_TOKEN?.trim() ||
    undefined;

  const deploymentMode = resolveDeploymentMode(fileConfig);
  const strictModeFromEnv = process.env.PAPERCLIP_SECRETS_STRICT_MODE;
  const secretsStrictMode =
    strictModeFromEnv !== undefined
      ? strictModeFromEnv === "true"
      : (fileSecrets?.strictMode ?? deploymentMode === "authenticated");
  const deploymentExposureFromEnvRaw = process.env.PAPERCLIP_DEPLOYMENT_EXPOSURE;
  const deploymentExposureFromEnv =
    deploymentExposureFromEnvRaw &&
    DEPLOYMENT_EXPOSURES.includes(deploymentExposureFromEnvRaw as DeploymentExposure)
      ? (deploymentExposureFromEnvRaw as DeploymentExposure)
      : null;
  const deploymentExposure: DeploymentExposure =
    deploymentMode === "local_trusted"
      ? "private"
      : (deploymentExposureFromEnv ?? fileConfig?.server.exposure ?? "private");
  const bindFromEnvRaw = process.env.PAPERCLIP_BIND;
  const bindFromEnv =
    bindFromEnvRaw && BIND_MODES.includes(bindFromEnvRaw as BindMode)
      ? (bindFromEnvRaw as BindMode)
      : null;
  const configuredHost = process.env.HOST ?? fileConfig?.server.host ?? "127.0.0.1";
  const tailnetBindHost = detectTailnetBindHost();
  const bind =
    bindFromEnv ??
    fileConfig?.server.bind ??
    inferBindModeFromHost(configuredHost, { tailnetBindHost });
  const customBindHost = process.env.PAPERCLIP_BIND_HOST ?? fileConfig?.server.customBindHost;
  const authBaseUrlModeFromEnvRaw = process.env.PAPERCLIP_AUTH_BASE_URL_MODE;
  const authBaseUrlModeFromEnv =
    authBaseUrlModeFromEnvRaw &&
    AUTH_BASE_URL_MODES.includes(authBaseUrlModeFromEnvRaw as AuthBaseUrlMode)
      ? (authBaseUrlModeFromEnvRaw as AuthBaseUrlMode)
      : null;
  const publicUrlFromEnv = process.env.PAPERCLIP_PUBLIC_URL;
  const configuredAuthPublicBaseUrlRaw = [
    process.env.PAPERCLIP_AUTH_PUBLIC_BASE_URL,
    process.env.BETTER_AUTH_URL,
    process.env.BETTER_AUTH_BASE_URL,
    publicUrlFromEnv,
    fileConfig?.auth?.publicBaseUrl,
  ].find((value): value is string => typeof value === "string" && value.trim().length > 0);
  const managedRuntimePublicUrl = process.env.PAPERCLIP_MANAGED_RUNTIME_PUBLIC_URL?.trim() || undefined;
  const authPublicBaseUrlRaw = configuredAuthPublicBaseUrlRaw ?? managedRuntimePublicUrl;
  const authPublicBaseUrl = authPublicBaseUrlRaw?.trim() || undefined;
  const authBaseUrlMode: AuthBaseUrlMode =
    authBaseUrlModeFromEnv ??
    (configuredAuthPublicBaseUrlRaw === undefined && managedRuntimePublicUrl
      ? "explicit"
      : fileConfig?.auth?.baseUrlMode ?? (authPublicBaseUrl ? "explicit" : "auto"));
  const disableSignUpFromEnv = process.env.PAPERCLIP_AUTH_DISABLE_SIGN_UP;
  const authDisableSignUp: boolean =
    disableSignUpFromEnv !== undefined
      ? disableSignUpFromEnv === "true"
      : (fileConfig?.auth?.disableSignUp ?? false);
  const allowedHostnamesFromEnvRaw = process.env.PAPERCLIP_ALLOWED_HOSTNAMES;
  const allowedHostnamesFromEnv = allowedHostnamesFromEnvRaw
    ? allowedHostnamesFromEnvRaw
      .split(",")
      .map((value) => value.trim().toLowerCase())
      .filter((value) => value.length > 0)
    : null;
  const publicUrlHostname = authPublicBaseUrl
    ? (() => {
      try {
        return new URL(authPublicBaseUrl).hostname.trim().toLowerCase();
      } catch {
        return null;
      }
    })()
    : null;
  const allowedHostnames = Array.from(
    new Set(
      [
        ...(allowedHostnamesFromEnv ?? fileConfig?.server.allowedHostnames ?? []),
        ...(publicUrlHostname ? [publicUrlHostname] : []),
      ]
        .map((value) => value.trim().toLowerCase())
        .filter(Boolean),
    ),
  );
  const companyDeletionEnvRaw = process.env.PAPERCLIP_ENABLE_COMPANY_DELETION;
  const companyDeletionEnabled =
    companyDeletionEnvRaw !== undefined
      ? companyDeletionEnvRaw === "true"
      : deploymentMode === "local_trusted";
  const databaseBackupEnabled =
    process.env.PAPERCLIP_DB_BACKUP_ENABLED !== undefined
      ? process.env.PAPERCLIP_DB_BACKUP_ENABLED === "true"
      : (fileDatabaseBackup?.enabled ?? true);
  const databaseBackupIntervalMinutes = Math.max(
    1,
    Number(process.env.PAPERCLIP_DB_BACKUP_INTERVAL_MINUTES) ||
      fileDatabaseBackup?.intervalMinutes ||
      60,
  );
  const databaseBackupRetentionDays = Math.max(
    1,
    Number(process.env.PAPERCLIP_DB_BACKUP_RETENTION_DAYS) ||
      fileDatabaseBackup?.retentionDays ||
      7,
  );
  const databaseBackupDir = resolveHomeAwarePath(
    process.env.PAPERCLIP_DB_BACKUP_DIR ??
      fileDatabaseBackup?.dir ??
      resolveDefaultBackupDir(),
  );
  // The terminal-workspace reaper waits this many days after an issue tree
  // becomes terminal before it archives the workspace. A person can reopen
  // the work inside this window. A value of 0 disables the cooldown and
  // restores immediate reaping.
  const workspaceReaperCooldownDaysEnv =
    process.env.PAPERCLIP_WORKSPACE_REAPER_COOLDOWN_DAYS?.trim();
  const workspaceReaperCooldownDaysRaw = Number(workspaceReaperCooldownDaysEnv);
  const workspaceReaperCooldownDays =
    workspaceReaperCooldownDaysEnv
      && Number.isFinite(workspaceReaperCooldownDaysRaw)
      && workspaceReaperCooldownDaysRaw >= 0
      ? workspaceReaperCooldownDaysRaw
      : 7;

  const databaseEnvironment = resolveDatabaseEnvironment();
  const databaseUrl = resolveDefinedDatabaseUrl(databaseEnvironment, "DATABASE_URL") ?? fileDbUrl;
  const databaseMigrationUrl = resolveDefinedDatabaseUrl(
    databaseEnvironment,
    "DATABASE_MIGRATION_URL",
  );
  const migrationConfig = resolveMigrationConfig(
    databaseEnvironment,
    databaseUrl,
    databaseMigrationUrl,
  );
  const { deploymentProfile, lockTimeoutMs: migrationLockTimeoutMs } = migrationConfig;
  const bindValidationErrors = validateConfiguredBindMode({
    deploymentMode,
    deploymentExposure,
    bind,
    host: configuredHost,
    customBindHost,
  });
  if (bindValidationErrors.length > 0) {
    throw new Error(bindValidationErrors[0]);
  }
  const resolvedBind = resolveRuntimeBind({
    bind,
    host: configuredHost,
    customBindHost,
    tailnetBindHost,
  });
  if (resolvedBind.errors.length > 0) {
    throw new Error(resolvedBind.errors[0]);
  }

  return {
    deploymentProfile,
    migrationLockTimeoutMs,
    deploymentMode,
    deploymentExposure,
    bind: resolvedBind.bind,
    customBindHost: resolvedBind.customBindHost,
    host: resolvedBind.host,
    port: Number(process.env.PORT) || fileConfig?.server.port || 3100,
    allowedHostnames,
    authBaseUrlMode,
    authPublicBaseUrl,
    chatWebhookPublicBaseUrl: parseChatWebhookPublicBaseUrl(
      process.env.PAPERCLIP_CHAT_WEBHOOK_PUBLIC_URL,
    ),
    authDisableSignUp,
    databaseMode: fileDatabaseMode,
    databaseUrl,
    databaseMigrationUrl,
    embeddedPostgresDataDir: resolveHomeAwarePath(
      fileConfig?.database.embeddedPostgresDataDir ?? resolveDefaultEmbeddedPostgresDir(),
    ),
    embeddedPostgresPort: fileConfig?.database.embeddedPostgresPort ?? 54329,
    databaseBackupEnabled,
    databaseBackupIntervalMinutes,
    databaseBackupRetentionDays,
    databaseBackupDir,
    workspaceReaperCooldownDays,
    serveUi:
      process.env.SERVE_UI !== undefined
        ? process.env.SERVE_UI === "true"
        : fileConfig?.server.serveUi ?? true,
    uiDevMiddleware: process.env.PAPERCLIP_UI_DEV_MIDDLEWARE === "true",
    secretsProvider,
    secretsStrictMode,
    secretsMasterKeyFilePath:
      resolveHomeAwarePath(
        process.env.PAPERCLIP_SECRETS_MASTER_KEY_FILE ??
          fileSecrets?.localEncrypted.keyFilePath ??
          resolveDefaultSecretsKeyFilePath(),
      ),
    storageProvider,
    storageLocalDiskBaseDir,
    storageS3Bucket,
    storageS3Region,
    storageS3Endpoint,
    storageS3Prefix,
    storageS3ForcePathStyle,
    runLogArchiveMode,
    runLogHotRetentionDays,
    runLogCompanyBudgetBytes,
    runLogSweepIntervalMs,
    runLogSweepItemLimit,
    runResultRetentionEnabled,
    runResultRetentionDays,
    runResultRetentionIntervalMs,
    runResultRetentionBatchSize,
    runResultRetentionItemLimit,
    feedbackExportBackendUrl,
    feedbackExportBackendToken,
    heartbeatSchedulerEnabled: process.env.HEARTBEAT_SCHEDULER_ENABLED !== "false",
    heartbeatSchedulerIntervalMs: Math.max(10000, Number(process.env.HEARTBEAT_SCHEDULER_INTERVAL_MS) || 30000),
    companyDeletionEnabled,
    telemetryEnabled: fileConfig?.telemetry?.enabled ?? true,
    announcementsEnabled: process.env.PAPERCLIP_ANNOUNCEMENTS_ENABLED !== "false",
    announcementsFeedUrl: process.env.PAPERCLIP_ANNOUNCEMENTS_FEED_URL?.trim() || "https://pages.paperclip.ing/announcements/v1/current.json",
  };
}
