import { delay } from "@calque/worker/columnar";

import { SyncError } from "../error.js";
import { BootstrapConflictError } from "../transport.js";

/**
 * How far into its plan's remaining life a mount renews it. A plan issued
 * under a 15-minute grant renews after 10 minutes, which leaves 5 for retries.
 */
export const RENEW_AFTER_SHARE = 2 / 3;

/** Delays between renewals after a failure another attempt may not repeat. */
export const RENEWAL_RETRY_DELAYS_MS: readonly number[] = Object.freeze([
  1_000, 5_000, 15_000, 30_000,
]);

/**
 * No renewal goes out this close to the plan's expiry; the change stream's
 * expiry, and the rebuild that answers it, are nearer than the answer.
 */
export const RENEWAL_CUTOFF_MS = 5_000;

/**
 * When the next renewal of a plan expiring at `expiresAt` goes out after
 * `failures` failed ones, or `undefined` when it would go out too late.
 */
export function nextRenewalAt(
  now: number,
  expiresAt: number,
  failures: number,
): number | undefined {
  const at =
    failures === 0
      ? now + (expiresAt - now) * RENEW_AFTER_SHARE
      : now +
        (RENEWAL_RETRY_DELAYS_MS[failures - 1] ??
          RENEWAL_RETRY_DELAYS_MS[RENEWAL_RETRY_DELAYS_MS.length - 1] ??
          0);
  return at + RENEWAL_CUTOFF_MS < expiresAt ? at : undefined;
}

/** The longest delay `setTimeout` keeps; a longer one fires at once. */
const MAX_TIMER_MS = 2 ** 31 - 1;

/** Waits until `at`, or until `signal` aborts. */
export async function waitUntil(
  at: number,
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted && Date.now() < at)
    await delay(Math.min(at - Date.now(), MAX_TIMER_MS), signal);
}

/**
 * Whether a renewal that failed with `error` may succeed if sent again: the
 * network, a server that asked to be tried again, or a grant `auth()` may
 * replace by then. A renewal the server refused for its content, or whose
 * resident state no longer holds there, is not sent again.
 */
export function renewalRetryable(error: unknown): boolean {
  return (
    error instanceof SyncError &&
    !(error instanceof BootstrapConflictError) &&
    (error.recovery === "retry" || error.code === "AUTH_FAILED")
  );
}
