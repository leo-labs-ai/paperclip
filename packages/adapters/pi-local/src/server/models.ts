import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AdapterModel } from "@paperclipai/adapter-utils";
import { asString, runChildProcess } from "@paperclipai/adapter-utils/server-utils";

const MODELS_CACHE_TTL_MS = 5 * 60_000;
const MODELS_CACHE_STALE_TTL_MS = 24 * 60 * 60_000;
const MODELS_CACHE_MAX_ENTRIES = 64;
const MODEL_DISCOVERY_TIMEOUT_SEC = 60;
const MODELS_DISCOVERY_TIMEOUT_COOLDOWN_MS = 5 * 60_000;

/**
 * Raised when `pi --list-models` could not be run to completion (timeout, spawn
 * failure, non-zero exit). It means model availability is *unknown*, which is
 * different from a completed discovery that did not list the configured model.
 */
export class PiModelDiscoveryUnavailableError extends Error {
  readonly timedOut: boolean;

  constructor(message: string, options?: { timedOut?: boolean }) {
    super(message);
    this.name = "PiModelDiscoveryUnavailableError";
    this.timedOut = options?.timedOut ?? false;
  }
}

function firstNonEmptyLine(text: string): string {
  return (
    text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .find(Boolean) ?? ""
  );
}

function parseModelsOutput(stdout: string): AdapterModel[] {
  const parsed: AdapterModel[] = [];
  const lines = stdout.split(/\r?\n/);
  
  // Skip header line if present
  let startIndex = 0;
  if (lines.length > 0 && (lines[0].includes("provider") || lines[0].includes("model"))) {
    startIndex = 1;
  }
  
  for (let i = startIndex; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    // Parse format: "provider   model   context  max-out  thinking  images"
    // Split by 2+ spaces to handle the columnar format
    const parts = line.split(/\s{2,}/);
    if (parts.length < 2) continue;
    
    const provider = parts[0].trim();
    const model = parts[1].trim();
    
    if (!provider || !model) continue;
    if (provider === "provider" && model === "model") continue; // Skip header
    
    const id = `${provider}/${model}`;
    parsed.push({ id, label: id });
  }
  
  return parsed;
}

function dedupeModels(models: AdapterModel[]): AdapterModel[] {
  const seen = new Set<string>();
  const deduped: AdapterModel[] = [];
  for (const model of models) {
    const id = model.id.trim();
    if (!id || seen.has(id)) continue;
    seen.add(id);
    deduped.push({ id, label: model.label.trim() || id });
  }
  return deduped;
}

function sortModels(models: AdapterModel[]): AdapterModel[] {
  return [...models].sort((a, b) =>
    a.id.localeCompare(b.id, "en", { numeric: true, sensitivity: "base" }),
  );
}

function resolvePiCommand(input: unknown): string {
  const envOverride =
    typeof process.env.PAPERCLIP_PI_COMMAND === "string" &&
    process.env.PAPERCLIP_PI_COMMAND.trim().length > 0
      ? process.env.PAPERCLIP_PI_COMMAND.trim()
      : "pi";
  return asString(input, envOverride);
}

type DiscoveryCacheEntry = {
  expiresAt: number;
  staleUntil: number;
  models: AdapterModel[];
};

const discoveryCache = new Map<string, DiscoveryCacheEntry>();
const discoveryRequests = new Map<string, Promise<AdapterModel[]>>();
// A discovery that timed out costs MODEL_DISCOVERY_TIMEOUT_SEC of run wall clock.
// Remember it briefly so back-to-back runs on a contended host fail fast instead
// of each paying the full timeout again.
const discoveryTimeoutUntil = new Map<string, number>();
const VOLATILE_ENV_KEY_EXACT = new Set([
  "PAPERCLIP_AGENT_ID",
  "PAPERCLIP_COMPANY_ID",
  "PAPERCLIP_RUN_ID",
  "PAPERCLIP_TASK_ID",
  "PAPERCLIP_ISSUE_WORK_MODE",
  "PAPERCLIP_WAKE_REASON",
  "PAPERCLIP_WAKE_COMMENT_ID",
  "PAPERCLIP_APPROVAL_ID",
  "PAPERCLIP_APPROVAL_STATUS",
  "PAPERCLIP_LINKED_ISSUE_IDS",
  "PAPERCLIP_WAKE_PAYLOAD_JSON",
  "PAPERCLIP_WORKSPACE_CWD",
  "PAPERCLIP_WORKSPACE_SOURCE",
  "PAPERCLIP_WORKSPACE_STRATEGY",
  "PAPERCLIP_WORKSPACE_ID",
  "PAPERCLIP_WORKSPACE_REPO_URL",
  "PAPERCLIP_WORKSPACE_REPO_REF",
  "PAPERCLIP_WORKSPACE_BRANCH",
  "PAPERCLIP_WORKSPACE_WORKTREE_PATH",
  "PAPERCLIP_WORKSPACES_JSON",
]);

function isVolatileEnvKey(key: string): boolean {
  return VOLATILE_ENV_KEY_EXACT.has(key);
}

function hashValue(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function discoveryEnvironment(env: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(env).filter(([key]) => !isVolatileEnvKey(key)));
}

function discoveryCacheKey(command: string, cwd: string, env: Record<string, string>) {
  const envKey = Object.entries(env)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${hashValue(value)}`)
    .join("\n");
  return `${command}\n${cwd}\n${envKey}`;
}

function cacheDiscovery(key: string, entry: DiscoveryCacheEntry) {
  discoveryCache.delete(key);
  discoveryCache.set(key, entry);
  while (discoveryCache.size > MODELS_CACHE_MAX_ENTRIES) {
    const oldestKey = discoveryCache.keys().next().value;
    if (oldestKey === undefined) break;
    discoveryCache.delete(oldestKey);
  }
}

function pruneExpiredDiscoveryCache(now: number) {
  for (const [key, value] of discoveryCache.entries()) {
    if (value.staleUntil <= now) discoveryCache.delete(key);
  }
}

async function runPiModelDiscovery(
  command: string,
  cwd: string,
  runtimeEnv: Record<string, string>,
): Promise<AdapterModel[]> {
  const result = await runChildProcess(
    `pi-models-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    command,
    ["--list-models"],
    {
      cwd,
      env: runtimeEnv,
      timeoutSec: MODEL_DISCOVERY_TIMEOUT_SEC,
      graceSec: 3,
      onLog: async () => {},
    },
  );

  if (result.timedOut) {
    throw new PiModelDiscoveryUnavailableError("`pi --list-models` timed out.", {
      timedOut: true,
    });
  }
  if ((result.exitCode ?? 1) !== 0) {
    const detail = firstNonEmptyLine(result.stderr) || firstNonEmptyLine(result.stdout);
    throw new PiModelDiscoveryUnavailableError(
      detail ? `\`pi --list-models\` failed: ${detail}` : "`pi --list-models` failed.",
    );
  }

  // Current Pi writes model rows to stdout; older releases wrote them to stderr.
  const output = result.stdout || result.stderr;
  return sortModels(dedupeModels(parseModelsOutput(output)));
}

export async function discoverPiModels(input: {
  command?: unknown;
  cwd?: unknown;
  env?: unknown;
} = {}): Promise<AdapterModel[]> {
  const command = resolvePiCommand(input.command);
  const cwd = asString(input.cwd, process.cwd());
  const env = normalizeEnv(input.env);
  const runtimeEnv = normalizeEnv({ ...process.env, ...env });
  return runPiModelDiscovery(command, cwd, runtimeEnv);
}

function normalizeEnv(input: unknown): Record<string, string> {
  const envInput = typeof input === "object" && input !== null && !Array.isArray(input)
    ? (input as Record<string, unknown>)
    : {};
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(envInput)) {
    if (typeof value === "string") env[key] = value;
  }
  return env;
}

export async function discoverPiModelsCached(input: {
  command?: unknown;
  cwd?: unknown;
  env?: unknown;
} = {}): Promise<AdapterModel[]> {
  const command = resolvePiCommand(input.command);
  const cwd = asString(input.cwd, process.cwd());
  const env = normalizeEnv(input.env);
  const discoveryEnv = discoveryEnvironment(env);
  const runtimeEnv = normalizeEnv({ ...process.env, ...discoveryEnv });
  const key = discoveryCacheKey(command, cwd, runtimeEnv);
  const now = Date.now();
  pruneExpiredDiscoveryCache(now);
  const cached = discoveryCache.get(key);
  if (cached && cached.expiresAt > now) return cached.models;

  const cooldownUntil = discoveryTimeoutUntil.get(key);
  if (cooldownUntil !== undefined) {
    if (cooldownUntil > now) {
      if (cached && cached.staleUntil > now) return cached.models;
      throw new PiModelDiscoveryUnavailableError(
        "`pi --list-models` timed out recently; skipping discovery.",
        { timedOut: true },
      );
    }
    discoveryTimeoutUntil.delete(key);
  }

  let request = discoveryRequests.get(key);
  if (!request) {
    request = runPiModelDiscovery(command, cwd, runtimeEnv)
      .then((models) => {
        const refreshedAt = Date.now();
        discoveryTimeoutUntil.delete(key);
        cacheDiscovery(key, {
          expiresAt: refreshedAt + MODELS_CACHE_TTL_MS,
          staleUntil: refreshedAt + MODELS_CACHE_STALE_TTL_MS,
          models,
        });
        return models;
      })
      .catch((error: unknown) => {
        if (error instanceof PiModelDiscoveryUnavailableError && error.timedOut) {
          discoveryTimeoutUntil.set(key, Date.now() + MODELS_DISCOVERY_TIMEOUT_COOLDOWN_MS);
        }
        const stale = discoveryCache.get(key);
        if (!stale || stale.staleUntil <= Date.now()) throw error;
        console.warn("[paperclip] Pi model refresh failed; using cached models.");
        return stale.models;
      })
      .finally(() => {
        discoveryRequests.delete(key);
      });
    discoveryRequests.set(key, request);
  }

  return request;
}

/**
 * Fast path for the model preflight: Pi resolves custom providers/models from
 * `$PI_CODING_AGENT_DIR/models.json` (falling back to `$HOME/.pi/agent`), and
 * paperclip deployments declare their providers there. When the configured
 * model is listed in that file we can answer the preflight without spawning
 * `pi --list-models` at all — a full Pi CLI boot that costs ~15s on a cold
 * volume and is the top agent-error source when it overruns its 60s cap.
 * Returns null when the file is absent/unreadable/unparseable or does not
 * mention the model (e.g. built-in providers), so callers fall back to spawn
 * discovery.
 */
export async function readPiModelsFromAgentConfig(
  runtimeEnv: Record<string, string>,
): Promise<AdapterModel[] | null> {
  const agentDir =
    runtimeEnv.PI_CODING_AGENT_DIR?.trim() ||
    path.join(runtimeEnv.HOME?.trim() || os.homedir(), ".pi", "agent");
  let raw: string;
  try {
    raw = await fs.readFile(path.join(agentDir, "models.json"), "utf8");
  } catch {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as { providers?: Record<string, { models?: unknown }> };
    if (!parsed || typeof parsed !== "object" || !parsed.providers) return null;
    const models: AdapterModel[] = [];
    for (const [provider, config] of Object.entries(parsed.providers)) {
      if (!config || typeof config !== "object" || !Array.isArray(config.models)) continue;
      for (const entry of config.models) {
        const id =
          typeof entry === "string"
            ? entry
            : typeof (entry as { id?: unknown })?.id === "string"
              ? (entry as { id: string }).id
              : "";
        if (!id.trim()) continue;
        const full = `${provider}/${id.trim()}`;
        models.push({ id: full, label: full });
      }
    }
    if (models.length === 0) return null;
    return sortModels(dedupeModels(models));
  } catch {
    return null;
  }
}

export async function ensurePiModelConfiguredAndAvailable(input: {
  model?: unknown;
  command?: unknown;
  cwd?: unknown;
  env?: unknown;
}): Promise<AdapterModel[]> {
  const model = asString(input.model, "").trim();
  if (!model) {
    throw new Error("Pi requires `adapterConfig.model` in provider/model format.");
  }

  // Answer from the static Pi agent config when it already lists the model;
  // no child process needed.
  const fileEnv = normalizeEnv({ ...process.env, ...normalizeEnv(input.env) });
  const fileModels = await readPiModelsFromAgentConfig(fileEnv);
  if (fileModels?.some((entry) => entry.id === model)) return fileModels;

  let models: AdapterModel[];
  try {
    models = await discoverPiModelsCached({
      command: input.command,
      cwd: input.cwd,
      env: input.env,
    });
  } catch (error) {
    if (!(error instanceof PiModelDiscoveryUnavailableError) || !error.timedOut) throw error;
    // Model discovery is a preflight check, not the run itself. When the check
    // cannot be completed (the child process timed out) we do
    // not know that the model is bad, so failing the run here turns a slow node
    // into a failed agent run. Warn and let the run proceed; a genuinely bad
    // model still fails with the adapter's own error.
    console.warn(
      `[paperclip] Pi model discovery unavailable (${error.message}); skipping model preflight for ${model}.`,
    );
    return [];
  }

  if (models.length === 0) {
    throw new Error("Pi returned no models. Run `pi --list-models` and verify provider auth.");
  }

  if (!models.some((entry) => entry.id === model)) {
    throw new Error(formatPiModelUnavailableMessage(model, models));
  }

  return models;
}

const CLOSE_MATCH_LIMIT = 5;

function levenshtein(a: string, b: string): number {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;
  let prev = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 1; i <= a.length; i += 1) {
    const curr = [i];
    for (let j = 1; j <= b.length; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(curr[j - 1]! + 1, prev[j]! + 1, prev[j - 1]! + cost);
    }
    prev = curr;
  }
  return prev[b.length]!;
}

function bareModelName(id: string): string {
  const slash = id.indexOf("/");
  return (slash >= 0 ? id.slice(slash + 1) : id).toLowerCase();
}

/**
 * Rank available model ids by similarity to `model`. Matches on the bare model
 * name (provider prefix stripped) so `clawrouter/gpt-5.6-luna` suggests
 * `clawrouter/gpt-5.6-luna-200k`, and a wrong provider prefix still finds the
 * right model under another provider.
 */
export function findClosePiModelMatches(
  model: string,
  models: AdapterModel[],
  limit = CLOSE_MATCH_LIMIT,
): string[] {
  const target = bareModelName(model);
  if (!target) return [];
  const threshold = Math.max(2, Math.floor(target.length / 3));
  const scored: Array<{ id: string; score: number }> = [];
  for (const entry of models) {
    const candidate = bareModelName(entry.id);
    let score: number;
    if (candidate === target) score = 0;
    else if (candidate.includes(target) || target.includes(candidate)) score = 1;
    else {
      const distance = levenshtein(target, candidate);
      if (distance > threshold) continue;
      score = 1 + distance;
    }
    scored.push({ id: entry.id, score });
  }
  scored.sort((a, b) => a.score - b.score || a.id.localeCompare(b.id));
  return scored.slice(0, limit).map((entry) => entry.id);
}

/**
 * Error text for a model that Pi (and therefore ClawRouter) cannot resolve.
 * Lists close matches first, then every available model; the old message
 * showed only the first 12 ids alphabetically, which usually hid the model the
 * operator meant.
 */
export function formatPiModelUnavailableMessage(model: string, models: AdapterModel[]): string {
  const ids = models.map((entry) => entry.id);
  const close = findClosePiModelMatches(model, models);
  const parts = [`Configured Pi model is unavailable: ${model}.`];
  if (close.length > 0) parts.push(`Did you mean: ${close.join(", ")}?`);
  parts.push(`Available models (${ids.length}): ${ids.join(", ")}`);
  return parts.join(" ");
}

export type PiModelPersistenceValidation =
  | { status: "valid" }
  | { status: "invalid"; message: string }
  | { status: "unverified"; reason: string };

/**
 * Save-time check used when an agent is created or its model changes. Uses the
 * same sources as the run preflight (agent-config models.json, then
 * `pi --list-models`, which reflects ClawRouter's catalog).
 *
 * Fail-open: when the catalog cannot be read (discovery errors, times out, or
 * returns nothing) the result is `unverified` and callers should save with a
 * warning instead of blocking agent edits on a ClawRouter/Pi outage. The run
 * preflight still rejects a bad model with the full error later.
 */
export async function validatePiModelForPersistence(input: {
  model?: unknown;
  command?: unknown;
  cwd?: unknown;
  env?: unknown;
}): Promise<PiModelPersistenceValidation> {
  const model = asString(input.model, "").trim();
  if (!model) return { status: "unverified", reason: "no model configured" };

  const fileEnv = normalizeEnv({ ...process.env, ...normalizeEnv(input.env) });
  const fileModels = await readPiModelsFromAgentConfig(fileEnv);
  if (fileModels?.some((entry) => entry.id === model)) return { status: "valid" };

  let models: AdapterModel[];
  try {
    models = await discoverPiModelsCached({ command: input.command, cwd: input.cwd, env: input.env });
  } catch (error) {
    return { status: "unverified", reason: error instanceof Error ? error.message : String(error) };
  }
  const merged = sortModels(dedupeModels([...(fileModels ?? []), ...models]));
  if (merged.length === 0) return { status: "unverified", reason: "Pi returned no models" };
  if (merged.some((entry) => entry.id === model)) return { status: "valid" };
  return { status: "invalid", message: formatPiModelUnavailableMessage(model, merged) };
}

export async function listPiModels(): Promise<AdapterModel[]> {
  try {
    return await discoverPiModelsCached();
  } catch {
    return [];
  }
}

export function resetPiModelsCacheForTests() {
  discoveryCache.clear();
  discoveryRequests.clear();
  discoveryTimeoutUntil.clear();
}

export function piModelsCacheSizeForTests() {
  return discoveryCache.size;
}
