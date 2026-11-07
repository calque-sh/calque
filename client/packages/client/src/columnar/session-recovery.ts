/**
 * Snapshot rebuilds a session may start per window. A rate rather than a
 * lifetime allowance: a mount the stream refuses on its first poll loops
 * within milliseconds, while a healthy tab whose grant expires rebuilds once
 * an hour. Only an adopted publication clears the count, since a looping
 * mount reaches `ready` on every pass.
 */
export const MAX_RESNAPSHOT_REBUILDS = 3;
export const RESNAPSHOT_REBUILD_WINDOW_MS = 60_000;
export const REBUILD_ABANDON_MESSAGE =
  "the session rebuilt from a fresh snapshot before the canonical echo; the mutation may have committed";
export const RESET_ABANDON_MESSAGE =
  "the session reset before the canonical echo; the mutation may have committed";
/**
 * A revocation cancels pending mutations rather than resubmitting them, since
 * their envelopes carry the scope token the barrier retired. One already
 * executing may still commit and show only in the rebuilt snapshot, so
 * reissuing it blindly can apply it twice.
 */
export const REVOKED_ABANDON_MESSAGE =
  "an authenticated revocation barrier retired this mutation's grant before the canonical echo; the mutation may have committed, so read the rebuilt snapshot before reissuing it";
export const LOCKED_ABANDON_MESSAGE =
  "the session locked before the canonical echo; the mutation may have committed";

/** Why a session that lost its authorization is being rebuilt. */
export type AuthorizationRecovery = "revoked" | "expired";
