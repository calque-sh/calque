/** Why a `SyncError` was raised. */
export type SyncErrorCode =
  /**
   * The server has not published the artifact for this grant's authorization
   * generation yet, as after a revocation. The client waits it out on the
   * server's schedule and reports it only when the wait would pass 120 s.
   */
  | "ARTIFACT_PREPARING"
  /**
   * The server refused the bearer token from `auth()` (HTTP 401 or 403) for
   * the bootstrap, the artifact download or a mutation. Recovery `reset`, once
   * `auth()` returns a grant the server accepts; `none` when `auth()` returned
   * a malformed value.
   */
  | "AUTH_FAILED"
  /**
   * The grant this session was built from expired: the change stream refused
   * it with `AUTHORIZATION_CHANGED`. The session rebuilds from `auth()`
   * without blanking first, and reports this only as the error of a
   * `locked` status with reason `expired`, when that rebuild fails.
   */
  | "AUTHORIZATION_EXPIRED"
  /**
   * An authenticated revocation barrier stood in the change stream. The
   * client suspended its affected queries, cancelled its pending mutations,
   * discarded everything it held for the revoked scope, and is rebuilding
   * from a fresh grant. Reported only as the error of a `locked` status with
   * reason `revoked`, when that rebuild fails.
   */
  | "AUTHORIZATION_REVOKED"
  /**
   * The operation stopped before it finished: its `AbortSignal` fired
   * (recovery `none`), or the mount it ran against was replaced or torn down.
   * After a teardown the call can run again on the next mount (recovery
   * `retry`); a mutation cancelled this way was never submitted. The
   * exception is `retryMutation`: the teardown dropped the request it would
   * resend, so the rebuilt rows show whether the mutation landed (recovery
   * `none`).
   */
  | "CANCELLED"
  /**
   * A memory or size bound was reached: the server refused the bootstrap
   * because the working set does not fit `memoryBudgetBytes`, the plan or the
   * artifact does not fit it, a query does not fit the query memory pool or too
   * many run at once, too many mutations are pending, or a change response
   * exceeded the size the client asked for. Recovery `none`.
   */
  | "CAPACITY_EXCEEDED"
  /** `close()` was called; every later call rejects with this. */
  | "CLOSED"
  /**
   * The artifact or a change-stream response failed validation, or the change
   * endpoint refused the stream for a reason other than authorization or the
   * client falling behind. Recovery `reset`.
   */
  | "INTEGRITY_FAILED"
  /**
   * A failure the client does not classify, such as `auth()` throwing; the
   * message carries the cause. Recovery `reset`.
   */
  | "INTERNAL_ERROR"
  /**
   * The query engine refused the query as written, such as a `sum` over a
   * non-numeric column. Recovery `none`: change the query.
   */
  | "INVALID_QUERY"
  /**
   * A server response broke the protocol: a malformed or oversized body, or a
   * bootstrap plan that does not match the generated schema. Recovery `reset`;
   * `retry` for a plan that had already expired. A mutation response that
   * breaks the protocol is `MUTATION_AMBIGUOUS` instead, since the mutation
   * may have committed.
   */
  | "INVALID_RESPONSE"
  /**
   * A `fetch` did not complete. The bootstrap retries it on its own schedule
   * before reporting it. Recovery `retry`.
   */
  | "NETWORK_ERROR"
  /**
   * The query engine failed for a reason it does not classify; the message
   * carries the cause. Recovery `none`.
   */
  | "QUERY_FAILED"
  /**
   * The result has more rows than `maxRows` or more bytes than `maxBytes`, and
   * no partial result is returned. Recovery `none`: narrow it with `limit`,
   * raise `maxRows` (up to 10,000) or `maxBytes` (up to 16 MiB), or read it in
   * pages with `window`.
   */
  | "RESULT_TOO_LARGE"
  /**
   * The server said the client must rebuild from a new snapshot, as when its
   * cursor is older than the server still retains. The client rebuilds by
   * itself; the application sees this code only when the server still refuses
   * after three rebuilds within a minute, with recovery `reset`.
   */
  | "RESNAPSHOT_REQUIRED"
  /**
   * The bootstrap request or the artifact download failed with an HTTP status
   * other than 401, 403, the bootstrap's preparing 503 or its 422
   * `CAPACITY_EXCEEDED`. Recovery `retry`; `none` when the server refused the
   * bootstrap request itself with a 400, 413 or 422, since the same request
   * meets the same refusal.
   */
  | "SERVER_ERROR"
  /**
   * The browser does not report the Chromium brand, or lacks the byte-stream
   * reader the artifact download uses. The client refuses before it sends any
   * request. Recovery `none`.
   */
  | "UNSUPPORTED_BROWSER"
  /**
   * A mutation ended without a decided outcome and may have committed: no
   * response, an unreadable or malformed one, a status other than 400, 401,
   * 403, 404 or 422, or a reset, rebuild, lock or failure that tore down the
   * mount holding it. Recovery `retry` while that mount is current:
   * `retryMutation(mutationId)` resends it under the same id. Recovery `none`
   * once the mount is gone, since nothing is left to resend; the rebuilt rows
   * show whether it landed.
   */
  | "MUTATION_AMBIGUOUS"
  /**
   * The mutation was refused before anything committed: the server answered
   * 400, 404 or 422, the client could not apply it locally, or
   * `retryMutation` found nothing to resend. Recovery `none`. A server that
   * decides against a mutation resolves the call with a `rejected` or
   * `conflict` result instead.
   */
  | "MUTATION_REJECTED";

/** Which surface a `SyncError` came from. */
export type SyncErrorScope = "query" | "sync" | "mutation" | "auth" | "client";

/**
 * What the application does about a `SyncError`.
 *
 * - `none`: the client has nothing to repeat. The call failed on its input or
 *   on a bound, so change the query, the mutation, what `auth()` returns or
 *   `memoryBudgetBytes`; it failed because the application cancelled it or
 *   closed the client; or it is an ambiguous mutation whose mount is gone,
 *   and the rebuilt rows show whether it landed.
 * - `retry`: the same call may succeed later. Run the query again, call
 *   `retryMutation(mutationId)` for an ambiguous mutation, or `refresh()` when
 *   the client's status is `error`.
 * - `reset`: call `reset()`, which rebuilds the client from a fresh `auth()`
 *   grant. After an authorization error, make `auth()` return a new grant
 *   first.
 */
export type SyncRecovery = "none" | "retry" | "reset";

/**
 * A failure the client, a query or a mutation reports: `code` names it,
 * `scope` says where it arose, and `recovery` says what the application does
 * next.
 */
export class SyncError extends Error {
  readonly code: SyncErrorCode;
  readonly scope: SyncErrorScope;
  readonly recovery: SyncRecovery;

  constructor(
    code: SyncErrorCode,
    scope: SyncErrorScope,
    recovery: SyncRecovery,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "SyncError";
    this.code = code;
    this.scope = scope;
    this.recovery = recovery;
  }
}

export function responseError(
  status: number,
  scope: SyncErrorScope,
  message: string,
): SyncError {
  if (status === 401 || status === 403) {
    return new SyncError("AUTH_FAILED", "auth", "reset", message);
  }
  return new SyncError("SERVER_ERROR", scope, "retry", message);
}

export function networkError(
  error: unknown,
  scope: SyncErrorScope,
  message: string,
): SyncError {
  if (error instanceof SyncError) return error;
  if (isAbortError(error)) {
    return new SyncError("CANCELLED", scope, "none", "request cancelled", {
      cause: error,
    });
  }
  return new SyncError("NETWORK_ERROR", scope, "retry", message, {
    cause: error,
  });
}

function isAbortError(error: unknown): boolean {
  return (
    error instanceof DOMException &&
    (error.name === "AbortError" || error.name === "TimeoutError")
  );
}
