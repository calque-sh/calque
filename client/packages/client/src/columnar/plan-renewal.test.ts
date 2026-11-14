import { describe, expect, it } from "vitest";

import { SyncError } from "../error.js";
import {
  ArtifactPreparingError,
  BootstrapConflictError,
} from "../transport.js";
import {
  nextRenewalAt,
  RENEWAL_CUTOFF_MS,
  renewalRetryable,
} from "./plan-renewal.js";

const MINUTE = 60_000;

describe("the renewal schedule", () => {
  it("renews after two thirds of the plan's remaining life", () => {
    expect(nextRenewalAt(0, 15 * MINUTE, 0)).toBe(10 * MINUTE);
    expect(nextRenewalAt(MINUTE, 61 * MINUTE, 0)).toBe(41 * MINUTE);
  });

  it("retries after one, five, fifteen and then every thirty seconds", () => {
    expect(
      [1, 2, 3, 4, 5, 9].map((failures) =>
        nextRenewalAt(0, 15 * MINUTE, failures),
      ),
    ).toEqual([1_000, 5_000, 15_000, 30_000, 30_000, 30_000]);
  });

  it("sends nothing whose answer could not arrive before the plan's last seconds", () => {
    expect(nextRenewalAt(0, 15_000, 0)).toBeUndefined();
    expect(nextRenewalAt(0, 15_001, 0)).toBeCloseTo(10_000.67, 1);
    expect(nextRenewalAt(0, 30_000 + RENEWAL_CUTOFF_MS, 4)).toBeUndefined();
    expect(nextRenewalAt(0, 30_001 + RENEWAL_CUTOFF_MS, 4)).toBe(30_000);
    expect(nextRenewalAt(10, 5, 0)).toBeUndefined();
  });

  it("asks again after the network, the server's own retry, or a grant auth() may replace", () => {
    const retryable = [
      new SyncError("NETWORK_ERROR", "sync", "retry", "cut"),
      new SyncError("SERVER_ERROR", "sync", "retry", "HTTP 503"),
      new ArtifactPreparingError("preparing", 1_000),
      new SyncError("AUTH_FAILED", "auth", "reset", "HTTP 401"),
    ];
    const final = [
      new SyncError("SERVER_ERROR", "sync", "none", "HTTP 400"),
      new BootstrapConflictError("HTTP 409 RESNAPSHOT_REQUIRED"),
      new SyncError("INVALID_RESPONSE", "sync", "reset", "not a renewal"),
      new SyncError("CANCELLED", "sync", "none", "request cancelled"),
      new TypeError("auth() threw"),
    ];
    expect(retryable.map(renewalRetryable)).toEqual([true, true, true, true]);
    expect(final.map(renewalRetryable)).toEqual([
      false,
      false,
      false,
      false,
      false,
    ]);
  });
});
