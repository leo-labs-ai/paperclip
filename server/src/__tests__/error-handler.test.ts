import type { NextFunction, Request, Response } from "express";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { HttpError } from "../errors.js";
import { AuthDbTimeoutError } from "../middleware/auth.js";
import { errorHandler } from "../middleware/error-handler.js";
import { logger } from "../middleware/logger.js";

const recordResponsibleUserDenialOnActiveRunMock = vi.hoisted(() => vi.fn());
const captureExceptionMock = vi.hoisted(() => vi.fn());
const telemetryMocks = vi.hoisted(() => ({
  client: {},
  trackErrorHandlerCrash: vi.fn(),
}));

vi.mock("../services/responsible-user-denial-run-outcomes.js", () => ({
  recordResponsibleUserDenialOnActiveRun:
    recordResponsibleUserDenialOnActiveRunMock,
}));

vi.mock("../sentry.js", () => ({ captureException: captureExceptionMock }));
vi.mock("../telemetry.js", () => ({
  getTelemetryClient: () => telemetryMocks.client,
}));
vi.mock("@paperclipai/shared/telemetry", () => ({
  trackErrorHandlerCrash: telemetryMocks.trackErrorHandlerCrash,
}));

function makeReq(): Request {
  return {
    method: "GET",
    originalUrl: "/api/test",
    body: { a: 1 },
    params: { id: "123" },
    query: { q: "x" },
  } as unknown as Request;
}

function makeRes(): Response {
  const res = {
    status: vi.fn(),
    json: vi.fn(),
  } as unknown as Response;
  (res.status as unknown as ReturnType<typeof vi.fn>).mockReturnValue(res);
  return res;
}

describe("errorHandler", () => {
  beforeEach(() => {
    recordResponsibleUserDenialOnActiveRunMock.mockReset();
    recordResponsibleUserDenialOnActiveRunMock.mockResolvedValue(null);
    captureExceptionMock.mockReset();
    telemetryMocks.trackErrorHandlerCrash.mockReset();
  });

  it("attaches the original Error to res.err for 500s", () => {
    const req = makeReq();
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;
    const err = new Error("boom");

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith({ error: "Internal server error" });
    expect(res.err).toBe(err);
    expect(res.__errorContext?.error?.message).toBe("boom");
  });

  it("ends aborted client requests without reporting a crash", () => {
    // A closed tab or dropped network surfaces as `Error: aborted` with
    // ECONNRESET; there is no server fault and nobody left to answer.
    const req = makeReq();
    const res = { ...makeRes(), end: vi.fn(), headersSent: false } as any;
    (res.status as ReturnType<typeof vi.fn>).mockReturnValue(res);
    const next = vi.fn() as unknown as NextFunction;
    const err = Object.assign(new Error("aborted"), { code: "ECONNRESET" });

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(499);
    expect(res.end).toHaveBeenCalled();
    expect(res.json).not.toHaveBeenCalled();
    expect(captureExceptionMock).not.toHaveBeenCalled();
    expect(telemetryMocks.trackErrorHandlerCrash).not.toHaveBeenCalled();
  });

  it("exposes raw 500 messages for trusted Cloud tenant imports", () => {
    const req = {
      ...makeReq(),
      method: "POST",
      originalUrl: "/api/companies/import",
      actor: {
        type: "board",
        userId: "cloud-user",
        source: "cloud_tenant",
      },
    } as unknown as Request;
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;
    const err = new Error("portable file references missing upload id");

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith({
      error: "Internal server error",
      message: "portable file references missing upload id",
    });
    expect(res.err).toBe(err);
  });

  it("attaches HttpError instances for 500 responses", () => {
    const req = makeReq();
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;
    const err = new HttpError(500, "db exploded");

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith({ error: "db exploded" });
    expect(res.err).toBe(err);
    expect(res.__errorContext?.error?.message).toBe("db exploded");
  });

  it("sanitizes chat setup errors before logs and crash reporting", () => {
    const req = {
      ...makeReq(),
      method: "POST",
      originalUrl: "/api/chat-endpoints/endpoint-1/setup",
      body: { credentials: { botToken: "setup-error-token-canary" } },
    } as unknown as Request;
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;
    const err = new Error("provider echoed setup-error-token-canary");
    err.name = "SecretName-setup-error-token-canary";

    errorHandler(err, req, res, next);

    expect(res.err).not.toBe(err);
    expect(res.err).toMatchObject({
      name: "Error",
      message: "Secret-sensitive request failed",
    });
    expect(res.__errorContext.error).toEqual({
      name: "Error",
      message: "Secret-sensitive request failed",
    });
    expect(captureExceptionMock).toHaveBeenCalledWith(res.err);
    expect(JSON.stringify(captureExceptionMock.mock.calls)).not.toContain(
      "setup-error-token-canary",
    );
    expect(telemetryMocks.trackErrorHandlerCrash).toHaveBeenCalledWith(
      telemetryMocks.client,
      { errorCode: "Error" },
    );
    expect(
      JSON.stringify(telemetryMocks.trackErrorHandlerCrash.mock.calls),
    ).not.toContain("setup-error-token-canary");
  });

  it("keeps actionable setup validation details while removing submitted credentials", () => {
    const req = {
      ...makeReq(),
      method: "POST",
      originalUrl: "/api/chat-endpoints/endpoint-1/setup",
      body: { credentials: { botToken: "invalid-token-canary" } },
    } as unknown as Request;
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;
    const err = new HttpError(
      422,
      "Missing required Slack scopes for invalid-token-canary",
      {
        code: "chat_provider_permissions_missing",
        credentials: { botToken: "invalid-token-canary" },
        explanation: "Provider rejected invalid-token-canary",
        requiredScopes: ["chat:write", "reactions:write"],
      },
    );

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(422);
    expect(res.json).toHaveBeenCalledWith({
      error: "Missing required Slack scopes for [REDACTED]",
      code: "chat_provider_permissions_missing",
      details: {
        code: "chat_provider_permissions_missing",
        credentials: "[REDACTED]",
        explanation: "Provider rejected [REDACTED]",
        requiredScopes: ["chat:write", "reactions:write"],
      },
    });
  });

  it("returns 400 for Zod validation errors from another module instance", () => {
    const req = makeReq();
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;
    const issue = {
      code: "invalid_type",
      expected: "string",
      received: "undefined",
      path: ["provider"],
      message: "Required",
    };
    const err = Object.assign(new Error("Validation failed"), {
      name: "ZodError",
      issues: [issue],
      errors: [issue],
    });

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.json).toHaveBeenCalledWith({
      error: "Validation error",
      details: [issue],
    });
    expect(res.err).toBeUndefined();
    expect(res.__errorContext).toBeUndefined();
  });

  it("removes submitted credentials from setup Zod issue prose", () => {
    const req = {
      ...makeReq(),
      method: "POST",
      originalUrl: "/api/chat-endpoints/endpoint-1/setup",
      body: { credentials: { botToken: "zod-token-canary" } },
    } as unknown as Request;
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;
    const issue = {
      code: "custom",
      path: ["credentials", "botToken"],
      message: "Rejected zod-token-canary",
    };
    const err = Object.assign(new Error("Validation failed"), {
      name: "ZodError",
      issues: [issue],
      errors: [issue],
    });

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.json).toHaveBeenCalledWith({
      error: "Validation error",
      details: [
        {
          code: "custom",
          path: ["credentials", "botToken"],
          message: "Rejected [REDACTED]",
        },
      ],
    });
  });

  it("records responsible-user denial codes on the active agent run", () => {
    const db = { marker: "db" };
    const req = {
      ...makeReq(),
      app: { locals: { paperclipDb: db } },
      actor: {
        type: "agent",
        agentId: "agent-1",
        companyId: "company-1",
        runId: "run-1",
        source: "agent_jwt",
      },
    } as unknown as Request;
    const res = makeRes();
    const next = vi.fn() as unknown as NextFunction;
    const err = new HttpError(403, "Responsible user is not authorized", {
      code: "RESPONSIBLE_USER_UNAUTHORIZED",
    });

    errorHandler(err, req, res, next);

    expect(res.status).toHaveBeenCalledWith(403);
    expect(res.json).toHaveBeenCalledWith({
      error: "Responsible user is not authorized",
      code: "RESPONSIBLE_USER_UNAUTHORIZED",
      details: { code: "RESPONSIBLE_USER_UNAUTHORIZED" },
    });
    expect(recordResponsibleUserDenialOnActiveRunMock).toHaveBeenCalledWith(
      db,
      {
        runId: "run-1",
        agentId: "agent-1",
        companyId: "company-1",
        code: "RESPONSIBLE_USER_UNAUTHORIZED",
      },
    );
  });
});

describe("errorHandler on a timed-out auth database lookup (HOM-441)", () => {
  beforeEach(() => {
    captureExceptionMock.mockReset();
    telemetryMocks.trackErrorHandlerCrash.mockReset();
  });

  it("answers 503 with the stable code instead of a generic 500", () => {
    const req = makeReq();
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;

    errorHandler(new AuthDbTimeoutError(5_000), req, res, next);

    expect(res.status).toHaveBeenCalledWith(503);
    expect(res.json).toHaveBeenCalledWith({
      error: "Auth database lookup did not complete within 5000ms",
      code: "AUTH_DB_TIMEOUT",
      details: { code: "AUTH_DB_TIMEOUT" },
    });
  });

  it("does not report a crash, and rate-limits its warn log to once per window", () => {
    // A far-future system time keeps this test's own rate-limit window
    // (a module-level timestamp shared across every call in the process)
    // independent of whatever real wall-clock time other tests in this file
    // already advanced it to.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2030-01-01T00:00:00.000Z"));
    const warnSpy = vi.spyOn(logger, "warn").mockImplementation((() => {}) as any);
    try {
      const req = makeReq();
      const res = makeRes() as any;
      const next = vi.fn() as unknown as NextFunction;

      errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      expect(captureExceptionMock).not.toHaveBeenCalled();
      expect(telemetryMocks.trackErrorHandlerCrash).not.toHaveBeenCalled();
      expect(warnSpy).toHaveBeenCalledTimes(1);
      expect(warnSpy).toHaveBeenCalledWith(
        expect.objectContaining({ err: expect.any(Error) }),
        expect.stringContaining("not a crash"),
      );

      // Still inside the rate-limit window: no second warn log.
      vi.setSystemTime(new Date("2030-01-01T00:00:10.000Z"));
      errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      expect(warnSpy).toHaveBeenCalledTimes(1);

      // Past the window: logs again.
      vi.setSystemTime(new Date("2030-01-01T00:00:31.000Z"));
      errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      expect(warnSpy).toHaveBeenCalledTimes(2);
      expect(captureExceptionMock).not.toHaveBeenCalled();
      expect(telemetryMocks.trackErrorHandlerCrash).not.toHaveBeenCalled();
    } finally {
      warnSpy.mockRestore();
      vi.useRealTimers();
    }
  });

  it("counts the occurrences a suppressed window stood in for", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2031-01-01T00:00:00.000Z"));
    const warnSpy = vi.spyOn(logger, "warn").mockImplementation((() => {}) as any);
    try {
      const req = makeReq();
      const res = makeRes() as any;
      const next = vi.fn() as unknown as NextFunction;

      errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      expect(warnSpy).toHaveBeenLastCalledWith(
        expect.objectContaining({ suppressedSinceLastLog: 0 }),
        expect.any(String),
      );

      for (let i = 0; i < 4; i++) {
        vi.setSystemTime(new Date(`2031-01-01T00:00:0${i + 1}.000Z`));
        errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      }
      expect(warnSpy).toHaveBeenCalledTimes(1);

      vi.setSystemTime(new Date("2031-01-01T00:00:31.000Z"));
      errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      expect(warnSpy).toHaveBeenCalledTimes(2);
      expect(warnSpy).toHaveBeenLastCalledWith(
        expect.objectContaining({ suppressedSinceLastLog: 4 }),
        expect.any(String),
      );
    } finally {
      warnSpy.mockRestore();
      vi.useRealTimers();
    }
  });

  it("flushes the suppressed count when the burst stops instead of losing it", async () => {
    // A switchover produces its whole burst in a few seconds and then stops,
    // so the request that would have carried the count out on the next window
    // never arrives.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2032-01-01T00:00:00.000Z"));
    const warnSpy = vi.spyOn(logger, "warn").mockImplementation((() => {}) as any);
    try {
      const req = makeReq();
      const res = makeRes() as any;
      const next = vi.fn() as unknown as NextFunction;

      errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      for (let i = 0; i < 3; i++) {
        await vi.advanceTimersByTimeAsync(1_000);
        errorHandler(new AuthDbTimeoutError(5_000), req, res, next);
      }
      expect(warnSpy).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(30_000);

      expect(warnSpy).toHaveBeenCalledTimes(2);
      expect(warnSpy).toHaveBeenLastCalledWith(
        expect.objectContaining({ suppressedSinceLastLog: 3 }),
        expect.any(String),
      );

      // Nothing further is emitted once the count has been flushed.
      await vi.advanceTimersByTimeAsync(60_000);
      expect(warnSpy).toHaveBeenCalledTimes(2);
    } finally {
      warnSpy.mockRestore();
      vi.useRealTimers();
    }
  });

  it("attaches no error context, and marks the response so the access log stays at warn level", () => {
    const warnSpy = vi.spyOn(logger, "warn").mockImplementation((() => {}) as any);
    try {
      const req = makeReq();
      const res = makeRes() as any;
      const next = vi.fn() as unknown as NextFunction;

      errorHandler(new AuthDbTimeoutError(5_000), req, res, next);

      expect(res.__errorContext).toBeUndefined();
      expect(res.err).toBeUndefined();
      expect(res.__transientServiceUnavailable).toBe(true);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("keeps every other 500-class error on the crash-reporting path", () => {
    const req = makeReq();
    const res = makeRes() as any;
    const next = vi.fn() as unknown as NextFunction;

    errorHandler(new HttpError(503, "Database is unreachable"), req, res, next);

    expect(res.__transientServiceUnavailable).toBeUndefined();
    expect(res.__errorContext).toBeDefined();
    expect(captureExceptionMock).toHaveBeenCalledTimes(1);
  });
});
