const FAILED_QUERY = /Failed query:[\s\S]*/g;
const MAX_REDACTION_DEPTH = 8;
const DROPPED_DATABASE_KEYS = new Set(["query", "params"]);

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/**
 * Drizzle's query error message and enumerable `query` / `params` fields
 * include the SQL text and bind values. Those must not reach clients or logs:
 * bind values can be tenant data, and issue #84 observed them on the 500 path.
 * Postgres code, constraint name, and other non-secret fields stay.
 */
export function redactDatabaseFaultText(value: string): string {
  if (!value.includes("Failed query:")) return value;
  return value.replace(FAILED_QUERY, "Failed query: [redacted]");
}

function redactDatabaseFaultValue(
  value: unknown,
  depth: number,
  seen: WeakSet<object>,
): unknown {
  if (typeof value === "string") return redactDatabaseFaultText(value);
  if (value == null || typeof value !== "object") return value;
  if (depth > MAX_REDACTION_DEPTH) return "[max-depth]";
  if (seen.has(value)) return "[circular]";
  seen.add(value);
  try {
    if (value instanceof Error) {
      const output: Record<string, unknown> = {
        type: value.name,
        message: redactDatabaseFaultText(value.message),
      };
      if (value.stack) output.stack = redactDatabaseFaultText(value.stack);
      for (const [key, entry] of Object.entries(value)) {
        if (DROPPED_DATABASE_KEYS.has(key) || key === "message" || key === "stack") continue;
        output[key] = redactDatabaseFaultValue(entry, depth + 1, seen);
      }
      const cause = (value as { cause?: unknown }).cause;
      if (cause !== undefined && !("cause" in output)) {
        output.cause = redactDatabaseFaultValue(cause, depth + 1, seen);
      }
      return output;
    }
    if (Array.isArray(value)) {
      return value.map((entry) => redactDatabaseFaultValue(entry, depth + 1, seen));
    }
    const output: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      if (DROPPED_DATABASE_KEYS.has(key)) continue;
      output[key] = redactDatabaseFaultValue(entry, depth + 1, seen);
    }
    return output;
  } finally {
    seen.delete(value);
  }
}

function objectChildren(value: object): unknown[] {
  const children = Array.isArray(value) ? [...value] : Object.values(value);
  if (value instanceof Error || "cause" in value) {
    children.push((value as { cause?: unknown }).cause);
  }
  return children;
}

function graphHasQueryLeak(value: unknown, depth: number, seen: WeakSet<object>): boolean {
  if (typeof value === "string") return value.includes("Failed query:");
  if (value == null || typeof value !== "object") return false;
  if (seen.has(value)) return false;
  // An object past the cap is not inspected. Returning the original would
  // keep a query or params field that the redacted copy replaces with
  // "[max-depth]". Treat that unseen tail as a leak so the copy is used.
  if (depth > MAX_REDACTION_DEPTH) return true;
  seen.add(value);
  try {
    if (value instanceof Error && value.message.includes("Failed query:")) return true;
    if (Array.isArray(value)) {
      return value.some((entry) => graphHasQueryLeak(entry, depth + 1, seen));
    }
    for (const [key, entry] of Object.entries(value)) {
      if (DROPPED_DATABASE_KEYS.has(key)) return true;
      if (graphHasQueryLeak(entry, depth + 1, seen)) return true;
    }
    const cause = (value as { cause?: unknown }).cause;
    if (cause !== undefined && !Object.prototype.hasOwnProperty.call(value, "cause")) {
      return graphHasQueryLeak(cause, depth + 1, seen);
    }
    return false;
  } finally {
    seen.delete(value);
  }
}

function graphHasCycle(value: unknown, depth: number, seen: WeakSet<object>): boolean {
  if (value == null || typeof value !== "object") return false;
  if (seen.has(value)) return true;
  if (depth > MAX_REDACTION_DEPTH) return false;
  seen.add(value);
  try {
    return objectChildren(value).some((entry) => graphHasCycle(entry, depth + 1, seen));
  } finally {
    seen.delete(value);
  }
}

export function serializeErrorWithoutDatabaseQuery(error: unknown) {
  if (typeof error === "string") return redactDatabaseFaultText(error);
  if (error == null || typeof error !== "object") return error;
  if (!(error instanceof Error) && !isPlainObject(error) && !Array.isArray(error)) return error;
  return redactDatabaseFaultValue(error, 0, new WeakSet());
}

/**
 * Client- and sink-safe copy of an unexpected database fault. Drops query text
 * and bind parameters. Keeps postgres code, constraint name, and other
 * non-secret diagnostics, including a bounded cause chain. Non-database errors
 * without a cycle are returned unchanged.
 */
export function databaseFaultForClient(error: unknown): Error {
  const root = error instanceof Error ? error : new Error(String(error));
  const leak = graphHasQueryLeak(root, 0, new WeakSet());
  const cycle = graphHasCycle(root, 0, new WeakSet());
  if (!leak && !cycle) return root;
  const redacted = redactDatabaseFaultValue(root, 0, new WeakSet());
  const safe = new Error(
    root.message.includes("Failed query:")
      ? redactDatabaseFaultText(root.message)
      : root.message,
  );
  const redactedRecord = redacted as Record<string, unknown>;
  safe.name = root.name === "DrizzleQueryError" ? "DatabaseError" : root.name;
  if (typeof redactedRecord.stack === "string") safe.stack = redactedRecord.stack;
  for (const [key, entry] of Object.entries(redactedRecord)) {
    if (key === "type" || key === "message" || key === "stack") continue;
    Object.assign(safe, { [key]: entry });
  }
  return safe;
}
