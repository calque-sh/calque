import type { SyncError } from "../error.js";

/**
 * Delays between bootstrap requests whose `fetch` rejected, as while the
 * service starts or a proxy is between upstreams. A response with any status,
 * 5xx included, is reported without a second request. Every retry resends the
 * grant the first attempt took, so the 9.75 s total stays well inside a
 * short-lived grant's lifetime. The artifact download uses the same schedule
 * for a request or body the network cut.
 */
export const BOOTSTRAP_RETRY_DELAYS_MS: readonly number[] = Object.freeze([
  250, 500, 1_000, 2_000, 3_000, 3_000,
]);

/**
 * An artifact download its plan can no longer finish, carrying the error that
 * said so: the server refused the plan's capability with a 401 or a 404, or
 * the network cut the download and its retry would have gone out at or after
 * the plan expired. `#start` answers it by activating again from `auth()` and
 * a new plan, once per epoch, and reports the error if it recurs.
 */
export class PlanOutlived {
  readonly error: SyncError;

  constructor(error: SyncError) {
    this.error = error;
  }
}
