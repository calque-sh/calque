/**
 * The refusals the direct-columnar engine raises, and the codes the client
 * classifies them by.
 */
type RuntimeErrorCode =
  /** Bytes that are not the artifact the plan bound, or are not intact. */
  | "ARTIFACT_INVALID"
  /** A change frame that does not continue this mount's canonical stream. */
  | "CHANGE_STREAM_INVALID"
  /** The reservation would exceed the mount's admitted memory. */
  | "CAPACITY_EXCEEDED"
  /** A request that this mount's current lifecycle state does not allow. */
  | "INVALID_LIFECYCLE"
  /** A malformed request: a caller error, not a state one. */
  | "INVALID_REQUEST"
  /**
   * A transport hiccup the live session retries, including a server that asked
   * to be tried again.
   */
  | "CANCELLED"
  /** The mount fell behind the source's durable range and must rebuild. */
  | "RESNAPSHOT_REQUIRED"
  /**
   * The change stream refused the plan's authorization: its grant expired,
   * or its generation was fenced and this mount asked past the barrier. The
   * session rebuilds from a fresh grant, or locks.
   */
  | "AUTHORIZATION_EXPIRED"
  /**
   * An authenticated revocation barrier stood in the batch. Nothing from that
   * batch is applied: the mount holds state the barrier retires, so it must
   * suspend and rebuild rather than advance.
   */
  | "AUTHORIZATION_REVOKED";

export class RuntimeError extends Error {
  readonly code: RuntimeErrorCode;

  constructor(code: RuntimeErrorCode, message: string) {
    super(message);
    this.name = "RuntimeError";
    this.code = code;
  }
}
