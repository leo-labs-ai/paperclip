import pino from "pino";

const FAILED_QUERY = /Failed query:[\s\S]*/g;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/**
 * Drizzle's query error message and enumerable `query` / `params` fields
 * include the SQL text and bind values. Those must not reach clients or logs:
 * bind values can be tenant data, and issue #84 observed them on the 500 path.
 * The postgres code and constraint name stay available on the cause chain.
 */
export function redactDatabaseFaultText(value: string): string {
  if (!value.includes("Failed query:")) return value;
  return value.replace(FAILED_QUERY, "Failed query: [redacted]");
}

function stripQueryParams(value: unknown, depth: number): unknown {
  if (depth > 8 || value == null) return value;
  if (typeof value === "string") return redactDatabaseFaultText(value);
  if (Array.isArray(value)) return value.map((entry) => stripQueryParams(entry, depth + 1));
  if (typeof value !== "object") return value;
  const output: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (key === "query" || key === "params") continue;
    output[key] = stripQueryParams(entry, depth + 1);
  }
  return output;
}

export function serializeErrorWithoutDatabaseQuery(error: unknown) {
  if (!(error instanceof Error)) {
    return isPlainObject(error) ? stripQueryParams(error, 0) : error;
  }
  return stripQueryParams(pino.stdSerializers.errWithCause(error), 0);
}

function errorChainHasQueryLeak(error: unknown, depth = 0): boolean {
  if (depth > 6 || !error || typeof error !== "object") return false;
  if ("query" in error || "params" in error) return true;
  if (error instanceof Error && error.message.includes("Failed query:")) return true;
  const message = (error as { message?: unknown }).message;
  if (typeof message === "string" && message.includes("Failed query:")) return true;
  return errorChainHasQueryLeak((error as { cause?: unknown }).cause, depth + 1);
}

/**
 * Client- and sink-safe copy of an unexpected database fault. Drops query text
 * and bind parameters. Non-database errors are returned unchanged.
 */
export function databaseFaultForClient(error: unknown): Error {
  const root = error instanceof Error ? error : new Error(String(error));
  if (!errorChainHasQueryLeak(root)) return root;
  const safe = new Error(redactDatabaseFaultText(root.message));
  safe.name = root.name === "DrizzleQueryError" ? "DatabaseError" : root.name;
  if (root.stack) safe.stack = redactDatabaseFaultText(root.stack);
  const code = (root as { code?: unknown }).code;
  if (typeof code === "string") {
    Object.assign(safe, { code });
  }
  return safe;
}
