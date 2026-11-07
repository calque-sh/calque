/**
 * Two mutations on one row. The overlay orders pending mutations by submission
 * and the server orders them by commit; the echo of each drops only its own
 * delta, so the later mutation stays on top until its own echo, and a
 * rejection in the middle refolds the survivors.
 */
import { describe, expect, test } from "vitest";

import {
  OverlayEngine,
  inspectOverlay,
  type CanonicalBatch,
  type KeyPart,
  type OverlayBase,
} from "./overlay.js";

const COLUMNS = ["id", "status"];
const ID = 0;
const STATUS = 1;
const ROW = 3;

const key = (index: number): readonly KeyPart[] => [`row-${index}`];
const keyBytes = (index: number) => new TextEncoder().encode(`row-${index}`);

function base(rows: number): OverlayBase {
  return {
    rows,
    lookupKey: (parts) => {
      const first = parts[0];
      if (typeof first !== "string" || !first.startsWith("row-")) return -1;
      const index = Number(first.slice(4));
      return Number.isInteger(index) && index >= 0 && index < rows ? index : -1;
    },
    valueAt: (column, row) => (column === ID ? `row-${row}` : `status-${row}`),
  };
}

const engineOf = () =>
  new OverlayEngine([{ name: "c", columns: COLUMNS, base: base(10) }]);

const patch = (mutationId: string, status: string) => ({
  mutationId,
  actions: [
    {
      kind: "patch" as const,
      collection: "c",
      primaryKey: keyBytes(ROW),
      key: key(ROW),
      changes: [{ column: STATUS, value: status, mode: "set" as const }],
    },
  ],
});

/** The server's echo: its committed value for the row plus the id it names. */
const echo = (mutationId: string, status: string): CanonicalBatch => ({
  transactions: [
    {
      operations: [
        {
          kind: "update",
          collection: "c",
          primaryKey: keyBytes(ROW),
          key: key(ROW),
          changed: [{ column: STATUS, value: status }],
        },
      ],
      mutationIds: [mutationId],
    },
  ],
});

const visible = (engine: OverlayEngine) =>
  engine.rows("c", ROW, 1)[0]?.[STATUS];
const canonical = (engine: OverlayEngine) =>
  inspectOverlay(engine, "c").canonical.row(ROW)?.values[STATUS];

describe("two mutations on one row", () => {
  test("each echo drops its own delta and the later mutation stays on top", () => {
    const engine = engineOf();
    engine.applyOptimistic(patch("m1", "a"));
    engine.applyOptimistic(patch("m2", "b"));
    expect(visible(engine)).toBe("b");
    expect(engine.pendingMutationIds()).toEqual(["m1", "m2"]);

    // m1's echo lands its committed value beneath m2, which still wins.
    expect(
      engine.applyCanonical(echo("m1", "a")).acknowledgedMutationIds,
    ).toEqual(["m1"]);
    expect(visible(engine)).toBe("b");
    expect(canonical(engine)).toBe("a");
    expect(engine.pendingMutationIds()).toEqual(["m2"]);

    expect(
      engine.applyCanonical(echo("m2", "b")).acknowledgedMutationIds,
    ).toEqual(["m2"]);
    expect(visible(engine)).toBe("b");
    expect(canonical(engine)).toBe("b");
    expect(engine.pendingMutationIds()).toEqual([]);
    expect(inspectOverlay(engine, "c").pending.patchedRows).toBe(0);
  });

  test("rejecting the later mutation exposes the earlier one until its echo", () => {
    const engine = engineOf();
    engine.applyOptimistic(patch("m1", "a"));
    engine.applyOptimistic(patch("m2", "b"));
    engine.dropMutation("m2");
    // m1 is still optimistic: its value shows, its delta is still pending, and
    // the canonical plane beneath it is untouched.
    expect(visible(engine)).toBe("a");
    expect(engine.pendingMutationIds()).toEqual(["m1"]);
    expect(inspectOverlay(engine, "c").canonical.row(ROW)).toBeUndefined();

    engine.applyCanonical(echo("m1", "a"));
    expect(visible(engine)).toBe("a");
    expect(canonical(engine)).toBe("a");
    expect(engine.pendingMutationIds()).toEqual([]);
  });

  test("an echo this mount never submitted applies as ordinary canonical change", () => {
    const engine = engineOf();
    engine.applyOptimistic(patch("m1", "a"));
    const result = engine.applyCanonical(echo("another-device", "z"));
    expect(result.acknowledgedMutationIds).toEqual(["another-device"]);
    // The foreign echo moved the canonical plane; m1 is refolded over it and
    // is still pending, because no echo named it.
    expect(engine.pendingMutationIds()).toEqual(["m1"]);
    expect(visible(engine)).toBe("a");
    expect(canonical(engine)).toBe("z");
  });
});
