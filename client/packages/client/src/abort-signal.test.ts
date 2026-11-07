import { describe, expect, it } from "vitest";

import {
  isAbortSignalAborted,
  sealOptionalAbortSignal,
} from "./abort-signal.js";

const INVALID_SIGNAL = new TypeError("signal must be a genuine AbortSignal");

describe("AbortSignal admission", () => {
  it("accepts genuine signals and rejects forged or wrong-receiver values", () => {
    const signal = new AbortController().signal;
    const revoked = Proxy.revocable(signal, {});
    revoked.revoke();
    expect(sealOptionalAbortSignal(undefined)).toBeUndefined();
    expect(sealOptionalAbortSignal(signal)).toBe(signal);

    for (const invalid of [
      null,
      false,
      "signal",
      {},
      {
        aborted: false,
        addEventListener: () => undefined,
        removeEventListener: () => undefined,
      },
      Object.create(AbortSignal.prototype),
      revoked.proxy,
    ]) {
      expect(() => sealOptionalAbortSignal(invalid)).toThrowError(
        INVALID_SIGNAL,
      );
    }
  });

  it("uses intrinsic accessors and listener methods", () => {
    const controller = new AbortController();
    Object.defineProperties(controller.signal, {
      addEventListener: {
        value: () => {
          throw new Error("own addEventListener must not run");
        },
      },
      removeEventListener: {
        value: () => {
          throw new Error("own removeEventListener must not run");
        },
      },
      aborted: { value: false },
      reason: {
        get: () => {
          throw new Error("own reason must not run");
        },
      },
    });
    const signal = sealOptionalAbortSignal(controller.signal);
    if (!signal) throw new Error("unreachable");

    controller.abort(new Error("caller cancellation"));
    expect(isAbortSignalAborted(signal)).toBe(true);
  });
});
