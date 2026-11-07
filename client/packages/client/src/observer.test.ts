import { afterEach, describe, expect, it, vi } from "vitest";

import { assertObserver, ObserverRegistry } from "./observer.js";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("observer isolation", () => {
  it("rejects a non-function with one fixed error", () => {
    expect(() => assertObserver(undefined)).toThrowError(
      new TypeError("listener must be a function"),
    );
    expect(() => assertObserver({})).toThrowError(
      new TypeError("listener must be a function"),
    );
  });

  it("reports a throwing listener and continues the publication", () => {
    const reported: unknown[] = [];
    vi.stubGlobal("reportError", (error: unknown) => reported.push(error));
    const registry = new ObserverRegistry<true>();
    const failure = new Error("observer failed");
    const later = vi.fn();
    registry.set(() => {
      throw failure;
    }, true);
    registry.set(later, true);

    expect(() => registry.notify()).not.toThrow();
    expect(reported).toEqual([failure]);
    expect(later).toHaveBeenCalledOnce();
  });

  it("contains a failing reporter and still runs later observers", () => {
    vi.stubGlobal("reportError", () => {
      throw new Error("reporter failed");
    });
    const registry = new ObserverRegistry<true>();
    const later = vi.fn();
    registry.set(() => {
      throw new Error("observer failed");
    }, true);
    registry.set(later, true);

    expect(() => registry.notify()).not.toThrow();
    expect(later).toHaveBeenCalledOnce();
  });

  it("uses serialized start-of-publication snapshots under reentrancy", () => {
    vi.stubGlobal("reportError", vi.fn());
    const registry = new ObserverRegistry<string>();
    const calls: string[] = [];
    let reentered = false;
    const second = () => calls.push("second");
    const third = () => calls.push("third");
    const added = () => calls.push("added");
    const first = () => {
      calls.push("first");
      if (reentered) return;
      reentered = true;
      registry.delete(second);
      registry.delete(third);
      registry.set(added, "added");
      registry.set(third, "third-readded");
      registry.notify();
    };
    registry.set(first, "first");
    registry.set(second, "second");
    registry.set(third, "third");

    registry.notify();

    expect(calls).toEqual(["first", "first", "added", "third"]);
  });

  it("bounds recursive publication without leaking the exception", () => {
    const reported: unknown[] = [];
    vi.stubGlobal("reportError", (error: unknown) => reported.push(error));
    const registry = new ObserverRegistry<true>();
    let calls = 0;
    registry.set(() => {
      calls += 1;
      registry.notify();
    }, true);

    expect(() => registry.notify()).not.toThrow();
    expect(calls).toBe(64);
    expect(reported).toHaveLength(1);
    expect(reported[0]).toEqual(
      new Error("observer publication exceeded the reentrancy bound"),
    );
  });
});
