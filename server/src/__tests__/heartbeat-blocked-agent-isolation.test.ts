import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  agents,
  agentWakeupRequests,
  approvals,
  budgetIncidents,
  budgetPolicies,
  companies,
  costEvents,
  createDb,
  heartbeatRunEvents,
  heartbeatRuns,
  issues,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { heartbeatService } from "../services/heartbeat.ts";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres blocked-agent isolation tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
  );
}

/**
 * Regression test for a production incident (2026-08-18).
 *
 * enqueueWakeup throws for per-agent gate failures. The most common one in practice is the
 * budget hard-stop, which surfaced as this line every 30s in server.log:
 *
 *   ERROR: heartbeat timer tick failed
 *     err: { "type": "HttpError",
 *            "message": "Agent cannot start because its budget hard-stop is still exceeded.",
 *            stack: ... at enqueueWakeup ... at async Object.tickTimers }
 *
 * tickTimers had no per-agent guard, so a single over-budget agent aborted the entire sweep:
 * every agent ordered after it silently stopped receiving heartbeats, and tickDueIssueMonitors
 * never ran at all. The failure was permanent, because a hard-stopped budget does not clear on
 * its own.
 */
describeEmbeddedPostgres("heartbeat blocked-agent isolation", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("heartbeat-blocked-agent-isolation-");
    db = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    await db.delete(costEvents);
    await db.delete(budgetIncidents);
    await db.delete(budgetPolicies);
    await db.delete(heartbeatRunEvents);
    await db.delete(heartbeatRuns);
    await db.delete(agentWakeupRequests);
    await db.delete(approvals);
    await db.delete(issues);
    await db.delete(agents);
    await db.delete(companies);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function insertAgent(companyId: string, name: string) {
    const agentId = randomUUID();
    await db.insert(agents).values([
      {
        id: agentId,
        companyId,
        name,
        role: "worker",
        status: "active",
        adapterType: "codex_local",
        adapterConfig: {},
        runtimeConfig: {
          heartbeat: {
            enabled: true,
            intervalSec: 60,
            wakeOnDemand: true,
          },
        },
        permissions: {},
        // Force the interval to be elapsed at tick time.
        createdAt: new Date("2026-06-04T00:00:00Z"),
        lastHeartbeatAt: new Date("2026-06-04T00:00:00Z"),
      },
    ]);
    return agentId;
  }

  /** Put `agentId` over an active hard-stop budget so enqueueWakeup throws for it. */
  async function hardStopAgent(companyId: string, agentId: string) {
    await db.insert(budgetPolicies).values([
      {
        companyId,
        scopeType: "agent",
        scopeId: agentId,
        metric: "billed_cents",
        windowKind: "lifetime",
        amount: 100,
        hardStopEnabled: true,
        isActive: true,
      },
    ]);
    await db.insert(costEvents).values([
      {
        companyId,
        agentId,
        provider: "test",
        model: "test-model",
        costCents: 5_000,
        occurredAt: new Date("2026-06-04T00:05:00Z"),
      },
    ]);
  }

  it("keeps sweeping other agents when one agent is budget hard-stopped", async () => {
    const companyId = randomUUID();
    await db.insert(companies).values([{ id: companyId, name: "Blocked Co", status: "active" }]);

    // Two agents in the same company. `blocked` is over budget; `healthy` is not.
    const blockedId = await insertAgent(companyId, "blocked-agent");
    const healthyId = await insertAgent(companyId, "healthy-agent");
    await hardStopAgent(companyId, blockedId);

    const heartbeat = heartbeatService(db);
    const result = await heartbeat.tickTimers(new Date("2026-06-04T00:10:00Z"));

    // Before the fix this rejected with HttpError and never reached the healthy agent.
    // Both timer attempts now produce durable skips, and the sweep completes.
    expect(result.checked).toBe(2);
    expect(result.skipped).toBe(2);

    // The blocked agent's refusal is still durably recorded, not swallowed.
    const wakeups = await db.select().from(agentWakeupRequests);
    const blockedSkips = wakeups.filter(
      (w) => w.agentId === blockedId && w.status === "skipped",
    );
    expect(blockedSkips.length).toBe(1);

    // Resolving at all also means tickDueIssueMonitors ran. It sits after the agent loop,
    // so the pre-fix throw skipped every scheduled issue monitor on every tick as well.
    expect(result).toHaveProperty("enqueued");
  });
});
