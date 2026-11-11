import { describe, expect, it } from "vitest";
import type { AdapterUsageCheckpoint } from "@paperclipai/adapter-utils";
import { priceAnthropicReceipt } from "./anthropic-pricing.js";
import { priceCodexReceipt } from "./codex-pricing.js";

for (const [provider, model, price] of [
  ["anthropic", "claude-sonnet-5", priceAnthropicReceipt],
  ["openai", "gpt-6-astra", priceCodexReceipt],
] as const) {
  describe(provider + " receipt input conventions", () => {
    const canonical: AdapterUsageCheckpoint = {
      complete: true, provider, biller: provider, model, billingType: "metered_api",
      usageBasis: "per_run", costStatus: "unpriced",
      usage: { inputTokens: 100, cachedInputTokens: 20, cacheWriteTokens: 10, outputTokens: 5 },
    };
    it("retains canonical rate-card estimation", () => {
      expect(price(canonical).costStatus).toBe("estimated");
    });
    it("does not estimate inclusive cached reads as uncached input", () => {
      const legacy = { ...canonical, cachedTokensIncludedInInput: true };
      expect(price(legacy)).toBe(legacy);
    });
    it.each([0, 10])("does not alias the legacy creation bucket (%i) to canonical writes", creation => {
      const legacy = { ...canonical, usage: { ...canonical.usage!, cacheCreationInputTokens: creation } };
      expect(price(legacy)).toBe(legacy);
    });
  });
}
