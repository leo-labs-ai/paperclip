import { randomUUID } from "node:crypto";
import { agents, companies, createDb, heartbeatRuns, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS, type Db } from "@paperclipai/db";
import { eq } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "../../__tests__/helpers/embedded-postgres.js";
import { createHeartbeatRunOutput, type HeartbeatRunOutputInput } from "./run-output.js";

vi.mock("../live-events.js", () => ({ publishLiveEvent: vi.fn() }));
const support = await getEmbeddedPostgresTestSupport();
const describePostgres = support.supported ? describe : describe.skip;
if (!support.supported) console.warn(`Skipping run output database tests: ${support.reason}`);

describePostgres("run output admission ownership", () => {
  let db: Db;
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("paperclip-run-output-");
    db = createDb(database.connectionString);
  }, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS);
  afterAll(async () => {
    await db?.$client.end();
    await database?.cleanup();
  });

  async function fixture() {
    const [company] = await db.insert(companies).values({ name: "Output ownership", issuePrefix: `O${randomUUID().slice(0, 6)}` }).returning();
    const [agent] = await db.insert(agents).values({ companyId: company.id, name: "Output owner" }).returning();
    const [run] = await db.insert(heartbeatRuns).values({ companyId: company.id, agentId: agent.id,
      status: "running", runtimeMode: "legacy", ownerToken: randomUUID(), fence: 1, controllerBootId: randomUUID() }).returning();
    if (!run.ownerToken || !run.controllerBootId) throw new Error("Missing fixture ownership");
    const input = {
      run, runId: run.id, issueId: null,
      authority: { kind: "legacy", executionClaim: { ownerToken: run.ownerToken, fence: run.fence }, executionControllerBootId: run.controllerBootId },
      runLogStore: {
        begin: vi.fn<HeartbeatRunOutputInput["runLogStore"]["begin"]>(async () => ({ store: "local_file", logRef: "fixture.log" })),
        append: vi.fn<HeartbeatRunOutputInput["runLogStore"]["append"]>(async () => 7),
      },
      identityRedactor: { chunk: (_stream: "stdout" | "stderr", text: string) => text, redact: <T>(value: T) => value },
      secretKeys: new Set<string>(),
      appendRunEvent: vi.fn<HeartbeatRunOutputInput["appendRunEvent"]>(async () => undefined),
      getCurrentUserRedactionOptions: async () => ({}),
    } satisfies HeartbeatRunOutputInput;
    return { run, input, output: createHeartbeatRunOutput(db, input) };
  }

  it("persists output and renews only the admitted legacy lease", async () => {
    const { run, input, output } = await fixture();
    const handlers = await output.begin(run);
    await handlers.onLog("stdout", "accepted");
    const [saved] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(input.runLogStore.append).toHaveBeenCalledOnce();
    expect(saved).toMatchObject({ logRef: "fixture.log", lastOutputSeq: 1, lastOutputBytes: 7, ownerToken: run.ownerToken });
    expect(saved.leaseExpiresAt).toBeInstanceOf(Date);
  });

  it.each([
    { ownerToken: "replacement" }, { fence: 2 }, { controllerBootId: "replacement" }, { status: "cancelled" },
  ])("rejects output after admission changes: %j", async patch => {
    const { run, input, output } = await fixture();
    const handlers = await output.begin(run);
    await db.update(heartbeatRuns).set(patch).where(eq(heartbeatRuns.id, run.id));
    await handlers.onLog("stdout", "stale");
    expect(input.runLogStore.append).not.toHaveBeenCalled();
    expect(output.stdoutExcerpt).toBe("");
    const [saved] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run.id));
    expect(saved.lastOutputSeq).toBe(run.lastOutputSeq);
  });

  it("rejects stale admission before opening or restoring a log", async () => {
    const { run, input, output } = await fixture();
    await db.update(heartbeatRuns).set({ ownerToken: "replacement" }).where(eq(heartbeatRuns.id, run.id));
    await expect(output.begin(run)).rejects.toThrow("ownership changed before log binding");
    expect(input.runLogStore.begin).not.toHaveBeenCalled();
  });

  it("forwards captured authority and fallback metadata to adapter events", async () => {
    const { run, input } = await fixture();
    const metadata = { key: "fallback-provider" };
    const output = createHeartbeatRunOutput(db, { ...input, providerFallbackMetadata: metadata });
    const handlers = await output.begin(run);
    await handlers.onAdapterMeta({ adapterType: "process", command: "fixture" });
    expect(input.appendRunEvent).toHaveBeenCalledWith(run,
      expect.objectContaining({ payload: expect.objectContaining({ providerFallback: metadata }) }),
      input.authority.executionClaim, false, input.authority.executionControllerBootId);
    await handlers.onAdapterEvent({ eventType: "fixture" });
    expect(input.appendRunEvent).toHaveBeenCalledWith(run, expect.objectContaining({ eventType: "fixture" }),
      input.authority.executionClaim, false, input.authority.executionControllerBootId);
  });
});
