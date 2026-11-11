import { describe, expect, it } from "vitest";
import {
  mergeProviderFallbackRunMetadata,
  readContextProviderFallbackKey,
  readTransientProviderFallbackDefinitions,
  resolveProviderFallbackApplication,
  resolveTransientProviderFallbackForAttempt,
} from "./provider-fallback.js";

describe("provider fallback contract", () => {
  it("filters disabled and empty rungs without renumbering generated keys", () => {
    const config = { providerFallbacks: [
      { enabled: false, adapterConfig: { model: "disabled" } },
      { adapterConfig: {} },
      { adapterConfig: { model: "backup" }, delayMs: "12.9" },
      { key: " custom ", adapterConfig: { model: "other", env: { FLAG: "1" } }, forceFreshSession: false, delayMs: -2 },
    ] };
    expect(readTransientProviderFallbackDefinitions(config)).toEqual([
      { key: "fallback-3", adapterConfig: { model: "backup" }, forceFreshSession: true, delayMs: 12 },
      { key: " custom ", adapterConfig: { model: "other", env: { FLAG: "1" } }, forceFreshSession: false, delayMs: 0 },
    ]);
    expect(resolveTransientProviderFallbackForAttempt(config, 1)?.key).toBe("fallback-3");
    expect(resolveTransientProviderFallbackForAttempt(config, 2)?.key).toBe(" custom ");
    for (const attempt of [0, -1, 1.5, 3, NaN]) {
      expect(resolveTransientProviderFallbackForAttempt(config, attempt)).toBeNull();
    }
  });

  it("uses the model alias only when the provider list is absent", () => {
    const modelFallbacks = [{ adapterConfig: { model: "alias" } }];
    expect(readTransientProviderFallbackDefinitions({ modelFallbacks })).toEqual([
      { key: "fallback-1", adapterConfig: { model: "alias" }, forceFreshSession: true, delayMs: null },
    ]);
    expect(readTransientProviderFallbackDefinitions({ providerFallbacks: [], modelFallbacks })).toEqual([]);
  });

  it("prefers a nonblank top-level key and retains a missing-rung reason", () => {
    expect(readContextProviderFallbackKey({ providerFallbackKey: " top ", paperclipProviderFallback: { key: "nested" } })).toBe(" top ");
    expect(readContextProviderFallbackKey({ providerFallbackKey: " ", paperclipProviderFallback: { key: "nested" } })).toBe("nested");
    const application = resolveProviderFallbackApplication({ agentRuntimeConfig: {}, contextSnapshot: { providerFallbackKey: "removed" } });
    expect(mergeProviderFallbackRunMetadata({ errorFamily: "provider_quota" }, application)).toEqual({
      errorFamily: "provider_quota",
      providerFallback: { requestedKey: "removed", appliedKey: null, fallbackReason: "agent_runtime_provider_fallback_not_configured", forceFreshSession: false },
    });
  });

  it("preserves config overrides but records only fallback metadata", () => {
    const adapterConfig = { model: " backup ", env: { TOKEN: "not-for-result" }, extraArgs: ["--safe"] };
    const application = resolveProviderFallbackApplication({
      agentRuntimeConfig: { providerFallbacks: [{ key: "backup", adapterConfig }] },
      contextSnapshot: { paperclipProviderFallback: { key: "backup" } },
    });
    expect(application.adapterConfig).toEqual(adapterConfig);
    expect(mergeProviderFallbackRunMetadata({ existing: true }, application)).toEqual({
      existing: true,
      providerFallback: { requestedKey: "backup", appliedKey: "backup", fallbackReason: null, forceFreshSession: true, model: " backup " },
    });
    const absent = resolveProviderFallbackApplication({ agentRuntimeConfig: {}, contextSnapshot: null });
    const original = { existing: true };
    expect(mergeProviderFallbackRunMetadata(original, absent)).toBe(original);
    expect(mergeProviderFallbackRunMetadata(null, absent)).toBeNull();
  });
});
