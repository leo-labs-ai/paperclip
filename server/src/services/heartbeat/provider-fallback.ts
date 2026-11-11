import { parseObject } from "../../adapters/utils.js";

export interface TransientProviderFallbackDefinition {
  key: string;
  adapterConfig: Record<string, unknown>;
  forceFreshSession: boolean;
  delayMs: number | null;
}

export interface ProviderFallbackApplication {
  requestedKey: string | null;
  appliedKey: string | null;
  fallbackReason: string | null;
  adapterConfig: Record<string, unknown> | null;
  forceFreshSession: boolean;
}

function readNonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function readNonNegativeInteger(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return Math.max(0, Math.floor(parsed));
}

export function readTransientProviderFallbackDefinitions(runtimeConfig: unknown): TransientProviderFallbackDefinition[] {
  const runtimeRecord = parseObject(runtimeConfig);
  const rawFallbacks = Array.isArray(runtimeRecord.providerFallbacks)
    ? runtimeRecord.providerFallbacks
    : Array.isArray(runtimeRecord.modelFallbacks)
      ? runtimeRecord.modelFallbacks
      : [];

  const definitions: TransientProviderFallbackDefinition[] = [];
  for (const [index, rawFallback] of rawFallbacks.entries()) {
    const fallback = parseObject(rawFallback);
    if (fallback.enabled === false) continue;
    const adapterConfig = parseObject(fallback.adapterConfig);
    if (Object.keys(adapterConfig).length === 0) continue;
    definitions.push({
      key: readNonEmptyString(fallback.key) ?? `fallback-${index + 1}`,
      adapterConfig,
      forceFreshSession: fallback.forceFreshSession !== false,
      delayMs: readNonNegativeInteger(fallback.delayMs),
    });
  }
  return definitions;
}

export function resolveTransientProviderFallbackForAttempt(
  runtimeConfig: unknown,
  attempt: number,
): (TransientProviderFallbackDefinition & { attempt: number }) | null {
  if (!Number.isInteger(attempt) || attempt <= 0) return null;
  const fallback = readTransientProviderFallbackDefinitions(runtimeConfig)[attempt - 1] ?? null;
  return fallback ? { ...fallback, attempt } : null;
}

export function readContextProviderFallbackKey(contextSnapshot: Record<string, unknown> | null | undefined): string | null {
  const nested = parseObject(contextSnapshot?.paperclipProviderFallback);
  return readNonEmptyString(contextSnapshot?.providerFallbackKey) ?? readNonEmptyString(nested.key);
}

export function resolveProviderFallbackApplication(input: {
  agentRuntimeConfig: unknown;
  contextSnapshot: Record<string, unknown> | null | undefined;
}): ProviderFallbackApplication {
  const requestedKey = readContextProviderFallbackKey(input.contextSnapshot);
  if (!requestedKey) {
    return {
      requestedKey: null,
      appliedKey: null,
      fallbackReason: null,
      adapterConfig: null,
      forceFreshSession: false,
    };
  }

  const fallback = readTransientProviderFallbackDefinitions(input.agentRuntimeConfig)
    .find((candidate) => candidate.key === requestedKey) ?? null;
  if (!fallback) {
    return {
      requestedKey,
      appliedKey: null,
      fallbackReason: "agent_runtime_provider_fallback_not_configured",
      adapterConfig: null,
      forceFreshSession: false,
    };
  }

  return {
    requestedKey,
    appliedKey: fallback.key,
    fallbackReason: null,
    adapterConfig: fallback.adapterConfig,
    forceFreshSession: fallback.forceFreshSession,
  };
}

export function providerFallbackRunMetadata(
  providerFallback: ProviderFallbackApplication,
): Record<string, unknown> | null {
  if (!providerFallback.requestedKey) return null;
  const model = readNonEmptyString(providerFallback.adapterConfig?.model);
  return {
    requestedKey: providerFallback.requestedKey,
    appliedKey: providerFallback.appliedKey,
    fallbackReason: providerFallback.fallbackReason,
    forceFreshSession: providerFallback.forceFreshSession,
    ...(model ? { model } : {}),
  };
}

export function mergeProviderFallbackRunMetadata(
  resultJson: Record<string, unknown> | null,
  providerFallback: ProviderFallbackApplication,
): Record<string, unknown> | null {
  const metadata = providerFallbackRunMetadata(providerFallback);
  if (!metadata) return resultJson;
  return {
    ...(resultJson ?? {}),
    providerFallback: metadata,
  };
}
