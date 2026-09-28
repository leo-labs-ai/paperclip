import type { Request, Response, NextFunction } from "express";
import type { Db } from "@paperclipai/db";
import { ZodError } from "zod";
import { HttpError, serviceUnavailable } from "../errors.js";
import { trackErrorHandlerCrash } from "@paperclipai/shared/telemetry";
import { getTelemetryClient } from "../telemetry.js";
import { captureException } from "../sentry.js";
import { COMPANY_IMPORT_API_PATH } from "../routes/company-import-paths.js";
import { logger } from "./logger.js";
import { isSecretSensitiveHttpRequest } from "./http-log-policy.js";
import {
  collectSensitiveStringValues,
  redactSensitiveValueOccurrences,
} from "./redact-sensitive.js";
import { recordResponsibleUserDenialOnActiveRun } from "../services/responsible-user-denial-run-outcomes.js";

export interface ErrorContext {
  error: {
    message: string;
    stack?: string;
    name?: string;
    details?: unknown;
    raw?: unknown;
  };
  method: string;
  url: string;
  reqBody?: unknown;
  reqParams?: unknown;
  reqQuery?: unknown;
}

function isRedactedSkillPolicyDenial(details: Record<string, unknown> | null) {
  return details?.code === "skill_policy_denied";
}

function readZodIssues(err: unknown): unknown[] | null {
  if (err instanceof ZodError) return err.issues;
  if (
    !err ||
    typeof err !== "object" ||
    (err as { name?: unknown }).name !== "ZodError"
  )
    return null;
  const issues = (err as { issues?: unknown }).issues;
  return Array.isArray(issues) ? issues : null;
}

function attachErrorContext(
  req: Request,
  res: Response,
  payload: ErrorContext["error"],
  rawError?: Error,
) {
  (res as any).__errorContext = {
    error: payload,
    method: req.method,
    url: req.originalUrl,
    reqBody: req.body,
    reqParams: req.params,
    reqQuery: req.query,
  } satisfies ErrorContext;
  if (rawError) {
    (res as any).err = rawError;
  }
}

function sanitizeSecretSensitiveError(req: Request, error: Error): Error {
  if (!isSecretSensitiveHttpRequest(req.method, req.originalUrl)) return error;
  const sanitized = new Error("Secret-sensitive request failed");
  // Both `name` and `message` are attacker/provider-controlled properties on
  // JavaScript errors. Do not preserve either on a credential-bearing route.
  sanitized.name = "Error";
  return sanitized;
}

function sanitizeSecretSensitiveResponse(
  req: Request,
  value: unknown,
): unknown {
  if (!isSecretSensitiveHttpRequest(req.method, req.originalUrl)) return value;
  return redactSensitiveValueOccurrences(
    value,
    collectSensitiveStringValues(req.body),
  );
}

/** Report a server-side crash to every error sink. */
function reportCrash(error: Error): void {
  const tc = getTelemetryClient();
  if (tc) trackErrorHandlerCrash(tc, { errorCode: error.name });
  captureException(error);
}

const AUTH_DB_TIMEOUT_WARN_INTERVAL_MS = 30_000;
let lastAuthDbTimeoutWarnAt = 0;

/**
 * `AUTH_DB_TIMEOUT` (see `AuthDbTimeoutError` in middleware/auth.ts) means a
 * Bearer/agent-key auth DB lookup hit its bound -- the expected, transient
 * shape of a database failover in progress, not a server bug. A CloudNativePG
 * primary switchover can make every in-flight auth request hit this at once,
 * and `reportCrash()` (Sentry + telemetry) firing once per request would
 * flood those sinks with a burst of identical, non-actionable crash events at
 * exactly the moment an operator needs real signal, instead of the ordinary
 * traffic-shaped signal a transient 503 should produce. A rate-limited warn
 * log still leaves a paper trail without the flood.
 */
function logAuthDbTimeoutWarning(error: Error): void {
  const now = Date.now();
  if (now - lastAuthDbTimeoutWarnAt < AUTH_DB_TIMEOUT_WARN_INTERVAL_MS) return;
  lastAuthDbTimeoutWarnAt = now;
  logger.warn(
    { err: error },
    "auth database lookup timed out (503) -- reported as a warning, not a crash, because this is the expected shape of an in-progress failover",
  );
}

function getPaperclipDb(req: Request): Db | null {
  const locals = req.app?.locals as { paperclipDb?: Db; db?: Db } | undefined;
  return locals?.paperclipDb ?? locals?.db ?? null;
}

function recordResponsibleUserDenialFromHttpError(
  req: Request,
  details: Record<string, unknown> | null,
) {
  if (req.actor?.type !== "agent") return;
  const db = getPaperclipDb(req);
  if (!db) return;

  void recordResponsibleUserDenialOnActiveRun(db, {
    runId: req.actor.runId ?? null,
    agentId: req.actor.agentId ?? null,
    companyId: req.actor.companyId ?? null,
    code: details?.code,
  }).catch((recordErr) => {
    logger.warn(
      {
        err: recordErr,
        runId: req.actor?.runId ?? null,
        agentId:
          req.actor?.type === "agent" ? (req.actor.agentId ?? null) : null,
      },
      "failed to record responsible-user denial on heartbeat run",
    );
  });
}

export function errorHandler(
  err: unknown,
  req: Request,
  res: Response,
  _next: NextFunction,
) {
  // An auth DB lookup that hit its ceiling (`AuthDbTimeoutError` in
  // middleware/auth.ts) means the database is transiently unreachable, not
  // that the server is broken: answer 503 with the stable code so clients and
  // the k8s stack can tell the two apart and retry. Matched on the code rather
  // than the class to keep this module free of an import cycle through auth.
  if (
    err instanceof Error &&
    (err as { code?: unknown }).code === "AUTH_DB_TIMEOUT"
  ) {
    err = serviceUnavailable(err.message, { code: "AUTH_DB_TIMEOUT" });
  }

  if (err instanceof HttpError) {
    const details =
      err.details &&
      typeof err.details === "object" &&
      !Array.isArray(err.details)
        ? (err.details as Record<string, unknown>)
        : null;
    const redactedSkillPolicyDenial = isRedactedSkillPolicyDenial(details);
    const workspaceRepairPreconditionFailure =
      details?.code === "workspace_repair_precondition_failed";
    const structuredConnectionError = new Set([
      "user_authorization_required",
      "organization_authorization_required",
      "grant_audience_denied",
      "grant_revoked",
      "needs_reauthorization",
      "installation_required",
      "connection_not_installed",
      "subject_not_permitted",
      "standing_delegation_required",
      "grant_owner_membership_inactive",
    ]).has(typeof details?.code === "string" ? details.code : "");
    const responseDetailsValue = sanitizeSecretSensitiveResponse(
      req,
      err.details,
    );
    const responseDetails =
      responseDetailsValue &&
      typeof responseDetailsValue === "object" &&
      !Array.isArray(responseDetailsValue)
        ? (responseDetailsValue as Record<string, unknown>)
        : null;
    recordResponsibleUserDenialFromHttpError(req, details);
    if (err.status >= 500) {
      const reportableError = sanitizeSecretSensitiveError(req, err);
      attachErrorContext(
        req,
        res,
        isSecretSensitiveHttpRequest(req.method, req.originalUrl)
          ? { message: reportableError.message, name: reportableError.name }
          : {
              message: err.message,
              stack: err.stack,
              name: err.name,
              details: err.details,
            },
        reportableError,
      );
      if (details?.code === "AUTH_DB_TIMEOUT") {
        logAuthDbTimeoutWarning(reportableError);
      } else {
        reportCrash(reportableError);
      }
    }
    const secretSensitiveServerError =
      err.status >= 500 &&
      isSecretSensitiveHttpRequest(req.method, req.originalUrl);
    res.status(err.status).json(
      secretSensitiveServerError
        ? { error: "Internal server error" }
        : {
            error: sanitizeSecretSensitiveResponse(req, err.message),
            ...(typeof responseDetails?.code === "string"
              ? { code: responseDetails.code }
              : {}),
            ...(redactedSkillPolicyDenial &&
            typeof responseDetails?.reason === "string"
              ? { reason: responseDetails.reason }
              : {}),
            ...(workspaceRepairPreconditionFailure &&
            typeof responseDetails?.reason === "string"
              ? { reason: responseDetails.reason }
              : {}),
            ...(workspaceRepairPreconditionFailure &&
            typeof responseDetails?.repairPhase === "string"
              ? { repairPhase: responseDetails.repairPhase }
              : {}),
            ...(typeof responseDetails?.remediation === "string" ||
            (structuredConnectionError &&
              responseDetails?.remediation &&
              typeof responseDetails.remediation === "object")
              ? { remediation: responseDetails.remediation }
              : {}),
            ...(structuredConnectionError && responseDetails?.connection
              ? { connection: responseDetails.connection }
              : {}),
            ...(structuredConnectionError && responseDetails?.subject
              ? { subject: responseDetails.subject }
              : {}),
            ...(structuredConnectionError &&
            typeof responseDetails?.grantId === "string"
              ? { grantId: responseDetails.grantId }
              : {}),
            ...(!redactedSkillPolicyDenial &&
            !workspaceRepairPreconditionFailure &&
            responseDetailsValue
              ? { details: responseDetailsValue }
              : {}),
          },
    );
    return;
  }

  const zodIssues = readZodIssues(err);
  if (zodIssues) {
    res.status(400).json({
      error: "Validation error",
      details: sanitizeSecretSensitiveResponse(req, zodIssues),
    });
    return;
  }

  const rootError = err instanceof Error ? err : new Error(String(err));

  // The client tore down the connection mid-request (closed tab, dropped
  // mobile network, cancelled upload): Node surfaces it as `Error: aborted`
  // with ECONNRESET. There is no server fault to report and nobody left to
  // answer, so skip the error sinks and just close out the response.
  if (
    rootError.message === "aborted" &&
    (rootError as NodeJS.ErrnoException).code === "ECONNRESET"
  ) {
    if (!res.headersSent) res.status(499);
    res.end();
    return;
  }

  const reportableError = sanitizeSecretSensitiveError(req, rootError);
  attachErrorContext(
    req,
    res,
    isSecretSensitiveHttpRequest(req.method, req.originalUrl)
      ? { message: reportableError.message, name: reportableError.name }
      : err instanceof Error
        ? { message: err.message, stack: err.stack, name: err.name }
        : {
            message: String(err),
            raw: err,
            stack: rootError.stack,
            name: rootError.name,
          },
    reportableError,
  );

  reportCrash(reportableError);

  res.status(500).json({
    error: "Internal server error",
    ...(shouldExposeTrustedCloudTenantImportError(req)
      ? { message: rootError.message }
      : {}),
  });
}

function shouldExposeTrustedCloudTenantImportError(req: Request) {
  return (
    req.actor?.source === "cloud_tenant" &&
    req.method === "POST" &&
    req.originalUrl.split("?")[0] === COMPANY_IMPORT_API_PATH
  );
}
