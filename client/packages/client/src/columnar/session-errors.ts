import { SyncError } from "../error.js";
import { InvalidSyncAuthError } from "../transport.js";

/** The string `code` a refusal carries, if it carries one. */
function errorCode(value: Error | undefined): string | undefined {
  return value && "code" in value && typeof value.code === "string"
    ? value.code
    : undefined;
}

/** A refusal from the overlay before anything was applied. */
export function optimismRefused(error: unknown): SyncError {
  const value = error instanceof Error ? error : undefined;
  const code = errorCode(value);
  if (code === "CAPACITY_EXCEEDED")
    return new SyncError(
      "CAPACITY_EXCEEDED",
      "mutation",
      "none",
      value?.message ?? "the overlay cannot hold another mutation",
      { cause: error },
    );
  return new SyncError(
    "MUTATION_REJECTED",
    "mutation",
    "none",
    value?.message ?? String(error),
    { cause: error },
  );
}

export function queryError(error: unknown): SyncError {
  if (error instanceof SyncError) return error;
  const value = error instanceof Error ? error : undefined;
  const code = errorCode(value);
  const message = value?.message ?? String(error);
  if (value?.name === "AbortError")
    return new SyncError("CANCELLED", "query", "none", "query cancelled", {
      cause: error,
    });
  switch (code) {
    case "RESULT_TOO_LARGE":
      return new SyncError("RESULT_TOO_LARGE", "query", "none", message, {
        cause: error,
      });
    case "INVALID_QUERY":
      return new SyncError("INVALID_QUERY", "query", "none", message, {
        cause: error,
      });
    case "CAPACITY_EXCEEDED":
      return new SyncError("CAPACITY_EXCEEDED", "query", "none", message, {
        cause: error,
      });
    default:
      return new SyncError("QUERY_FAILED", "query", "none", message, {
        cause: error,
      });
  }
}

export function activationError(error: unknown): SyncError {
  if (error instanceof SyncError) return error;
  if (error instanceof InvalidSyncAuthError)
    return new SyncError("AUTH_FAILED", "auth", "none", error.message, {
      cause: error,
    });
  const value = error instanceof Error ? error : undefined;
  const code = errorCode(value);
  // `#rebuildOrReport` picks its recovery from these codes, so each keeps its
  // own rather than folding into `INTERNAL_ERROR`.
  if (code === "RESNAPSHOT_REQUIRED")
    return new SyncError(
      "RESNAPSHOT_REQUIRED",
      "sync",
      "reset",
      value?.message ?? "this mount fell behind the source's durable range",
      { cause: error },
    );
  if (code === "AUTHORIZATION_REVOKED")
    return new SyncError(
      "AUTHORIZATION_REVOKED",
      "auth",
      "reset",
      value?.message ?? "this scope's authorization was revoked",
      { cause: error },
    );
  if (code === "AUTHORIZATION_EXPIRED")
    return new SyncError(
      "AUTHORIZATION_EXPIRED",
      "auth",
      "reset",
      value?.message ?? "this plan's authorization expired",
      { cause: error },
    );
  if (code === "ARTIFACT_INVALID" || code === "CHANGE_STREAM_INVALID")
    return new SyncError(
      "INTEGRITY_FAILED",
      "sync",
      "reset",
      "artifact integrity verification failed",
      { cause: error },
    );
  if (code === "CAPACITY_EXCEEDED")
    return new SyncError(
      "CAPACITY_EXCEEDED",
      "sync",
      "none",
      value?.message ?? "the artifact exceeds the memory budget",
      { cause: error },
    );
  return new SyncError(
    "INTERNAL_ERROR",
    "sync",
    "reset",
    value?.message ?? String(error),
    { cause: error },
  );
}

export function cancelled(
  scope: "sync" | "query" | "mutation",
  recovery: "none" | "retry" = "retry",
): SyncError {
  return new SyncError(
    "CANCELLED",
    scope,
    recovery,
    "the session reset before this operation completed",
  );
}
