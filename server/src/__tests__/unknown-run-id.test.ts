import express from "express";
import request from "supertest";
import { describe, expect, it } from "vitest";
import { HttpError } from "../errors.js";
import { errorHandler } from "../middleware/error-handler.js";
import {
  databaseFaultForClient,
  serializeErrorWithoutDatabaseQuery,
} from "../middleware/redact-db-fault.js";
import {
  isHeartbeatRunReferenceViolation,
  unknownRunIdError,
} from "../services/heartbeat-run-scope.js";

const SQL_CANARY = "select secret_column from tenants where token = $1";
const PARAM_CANARY = "sql-leak-param-canary";

function drizzleFault() {
  const cause = Object.assign(new Error(`insert or update violates foreign key constraint "issues_checkout_run_id_heartbeat_runs_id_fk"`), {
    code: "23503",
    constraint_name: "issues_checkout_run_id_heartbeat_runs_id_fk",
  });
  return Object.assign(new Error(`Failed query: ${SQL_CANARY}\nparams: ${PARAM_CANARY}`), {
    query: SQL_CANARY,
    params: [PARAM_CANARY],
    cause,
  });
}

describe("unknown run id contract", () => {
  it("recognizes only heartbeat-run foreign keys, including a Drizzle wrapper", () => {
    expect(isHeartbeatRunReferenceViolation(drizzleFault())).toBe(true);
    expect(isHeartbeatRunReferenceViolation({
      cause: { code: "23503", constraint_name: "activity_log_company_id_companies_id_fk" },
    })).toBe(false);
    expect(isHeartbeatRunReferenceViolation({
      code: "23503",
      message: 'violates foreign key constraint "activity_log_run_id_heartbeat_runs_id_fk"',
    })).toBe(true);
  });

  it("documents unknown_run_id as 422 without query or params", () => {
    const error = unknownRunIdError();
    expect(error).toBeInstanceOf(HttpError);
    expect(error.status).toBe(422);
    expect(error.message).toBe("unknown_run_id");
    expect(JSON.stringify(error.details)).not.toContain(SQL_CANARY);
    expect(JSON.stringify(error.details)).not.toContain("params");
  });

  it("strips query text and bind parameters from logged errors", () => {
    const asText = serializeErrorWithoutDatabaseQuery(
      `Failed query: ${SQL_CANARY}\nparams: ${PARAM_CANARY}`,
    );
    expect(asText).toBe("Failed query: [redacted]");
    expect(asText).not.toContain(SQL_CANARY);
    expect(asText).not.toContain(PARAM_CANARY);

    const serialized = JSON.stringify(serializeErrorWithoutDatabaseQuery(drizzleFault()));
    expect(serialized).not.toContain(SQL_CANARY);
    expect(serialized).not.toContain(PARAM_CANARY);
    expect(serialized).not.toContain('"query"');
    expect(serialized).not.toContain('"params"');
    expect(serialized).toContain("23503");
    expect(serialized).toContain("issues_checkout_run_id_heartbeat_runs_id_fk");
  });

  it("redacts nested query text without dropping non-secret diagnostics or looping on cycles", () => {
    const cause: Record<string, unknown> = {
      code: "23503",
      constraint_name: "issues_checkout_run_id_heartbeat_runs_id_fk",
      message: "insert or update violates foreign key constraint",
    };
    const details: Record<string, unknown> = {
      note: "pool exhausted",
      cause,
    };
    details.self = details;
    cause.details = details;
    let buried: unknown = { query: SQL_CANARY, params: [PARAM_CANARY] };
    for (let depth = 0; depth < 20; depth += 1) buried = { details: buried };
    const fault = Object.assign(new Error(`Failed query: ${SQL_CANARY}\nparams: ${PARAM_CANARY}`), {
      query: SQL_CANARY,
      params: [PARAM_CANARY],
      code: "23503",
      summary: "still-operational",
      cause,
      details: buried,
    });

    const serialized = JSON.stringify(serializeErrorWithoutDatabaseQuery(fault));
    expect(serialized).toContain("[circular]");
    expect(serialized).toContain("[max-depth]");
    expect(serialized).toContain("pool exhausted");
    expect(serialized).toContain("23503");
    expect(serialized).toContain("issues_checkout_run_id_heartbeat_runs_id_fk");
    expect(serialized).toContain("still-operational");
    expect(serialized).not.toContain(SQL_CANARY);
    expect(serialized).not.toContain(PARAM_CANARY);
    expect(serialized).not.toContain('"query"');
    expect(serialized).not.toContain('"params"');

    const safe = databaseFaultForClient(fault);
    expect(safe).not.toBe(fault);
    expect(safe.message).toBe("Failed query: [redacted]");
    expect(JSON.stringify(safe.cause)).toContain("issues_checkout_run_id_heartbeat_runs_id_fk");
    expect(JSON.stringify(safe)).not.toContain(SQL_CANARY);
    expect(JSON.stringify(safe)).not.toContain(PARAM_CANARY);

    const operational = Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" });
    expect(databaseFaultForClient(operational)).toBe(operational);
    expect(JSON.stringify(serializeErrorWithoutDatabaseQuery(operational))).toContain("connection refused");

    let onlyDeep: unknown = {
      query: SQL_CANARY,
      params: [PARAM_CANARY],
      message: `Failed query: ${SQL_CANARY}`,
    };
    for (let depth = 0; depth < 20; depth += 1) onlyDeep = { details: onlyDeep };
    const deepOnly = Object.assign(new Error("pool exhausted"), {
      code: "53300",
      summary: "still-operational",
      details: onlyDeep,
    });
    const deepSafe = databaseFaultForClient(deepOnly);
    expect(deepSafe).not.toBe(deepOnly);
    expect(deepSafe.message).toBe("pool exhausted");
    const deepSerialized = JSON.stringify(deepSafe);
    expect(deepSerialized).toContain("[max-depth]");
    expect(deepSerialized).toContain("still-operational");
    expect(deepSerialized).toContain("53300");
    expect(deepSerialized).not.toContain(SQL_CANARY);
    expect(deepSerialized).not.toContain(PARAM_CANARY);
    expect(deepSerialized).not.toContain('"query"');
    expect(deepSerialized).not.toContain('"params"');
  });

  it("does not put query or params on an unexpected database 500", async () => {
    const app = express();
    app.get("/boom", () => {
      throw drizzleFault();
    });
    app.use(errorHandler);
    const res = await request(app).get("/boom");
    const body = JSON.stringify(res.body);
    expect(res.status).toBe(500);
    expect(res.body).toEqual({ error: "Internal server error" });
    expect(body).not.toContain(SQL_CANARY);
    expect(body).not.toContain(PARAM_CANARY);
    expect(body).not.toContain("Failed query");
    expect(databaseFaultForClient(drizzleFault()).message).not.toContain(PARAM_CANARY);
  });
});
