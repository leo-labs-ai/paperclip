import type { Db } from "@paperclipai/db";
import { describe, expect, it, vi } from "vitest";
import { recordHeartbeatAdapterRuntimeServices, startHeartbeatRuntimeServices } from "./runtime-services.js";

function deniedInput() {
  const access = vi.fn(() => { throw new Error("Unexpected database access"); });
  const db = new Proxy({}, { get: access }) as Db;
  const addComment = vi.fn();
  const persistRunContext = vi.fn(async () => false);
  const input: Parameters<typeof startHeartbeatRuntimeServices>[1] &
    Parameters<typeof recordHeartbeatAdapterRuntimeServices>[1] = {
      run: { id: "run" },
      agent: { id: "agent", name: "Runtime agent", companyId: "company", adapterType: "process" },
      issueId: null, issueRef: null,
      executionWorkspace: {
        baseCwd: "/workspace", cwd: "/workspace", source: "agent_home",
        projectId: null, workspaceId: null, repoUrl: null, repoRef: null,
        strategy: "project_primary", branchName: null, worktreePath: null,
        warnings: [], created: false, branchCreatedByRuntime: false,
      },
      context: {}, persistRunContext, issuesSvc: { addComment }, onLog: vi.fn(async () => {}),
      persistedExecutionWorkspace: null, hostExecutionWorkspaceConfig: {},
      adapterEnv: {}, workspaceOperationRecorder: undefined,
      adapterResult: {}, runtimeServices: [],
    };
  return { db, access, addComment, persistRunContext, input };
}

describe("runtime service execution authority", () => {
  it("rejects stale startup before database or publication effects", async () => {
    const f = deniedInput();
    await expect(startHeartbeatRuntimeServices(f.db, f.input)).rejects.toThrow("Execution ownership lost");
    expect(f.access).not.toHaveBeenCalled();
    expect(f.addComment).not.toHaveBeenCalled();
  });

  it("does not demand execution authority for an adapter result without services", async () => {
    const f = deniedInput();
    expect(await recordHeartbeatAdapterRuntimeServices(f.db, f.input)).toBe("not_reported");
    expect(f.persistRunContext).not.toHaveBeenCalled();
    expect(f.access).not.toHaveBeenCalled();
    expect(f.addComment).not.toHaveBeenCalled();
  });

  it("denies optional service effects without throwing away the final receipt", async () => {
    const f = deniedInput();
    f.input.adapterResult.runtimeServices = [{ serviceName: "preview" }];
    expect(await recordHeartbeatAdapterRuntimeServices(f.db, f.input)).toBe("denied");
    expect(f.persistRunContext).toHaveBeenCalledOnce();
    expect(f.access).not.toHaveBeenCalled();
    expect(f.addComment).not.toHaveBeenCalled();
  });
});
