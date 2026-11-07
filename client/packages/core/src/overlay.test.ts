/**
 * The overlay's own guarantees, asserted without the SQLite oracle: the
 * canonical plane holds state rather than a log, the order vector, view
 * invalidation, the truncate flag, the structural integrity check, and the
 * budget.
 */
import { describe, expect, test } from "vitest";

import {
  OverlayEngine,
  OverlayError,
  UNSET,
  inspectOverlay,
  type CanonicalBatch,
  type CanonicalOperation,
  type KeyPart,
  type OverlayBase,
  type SqlValue,
} from "./overlay.js";

const COLUMNS = ["id", "status", "count", "title"];
const ID = 0;
const STATUS = 1;
const COUNT = 2;
const TITLE = 3;

const key = (index: number): readonly KeyPart[] => [`row-${index}`];
const keyBytes = (index: number) => new TextEncoder().encode(`row-${index}`);

/** A synthetic base: `row-<n>` maps to physical row `n`, values are derived. */
function base(rows: number): OverlayBase {
  return {
    rows,
    lookupKey: (parts) => {
      const first = parts[0];
      if (typeof first !== "string" || !first.startsWith("row-")) return -1;
      const index = Number(first.slice(4));
      return Number.isInteger(index) && index >= 0 && index < rows ? index : -1;
    },
    valueAt: (column, row) =>
      column === ID
        ? `row-${row}`
        : column === COUNT
          ? row
          : `${COLUMNS[column] ?? "?"}-${row}`,
  };
}

const engineOf = (rows: number, budgetBytes?: number) =>
  new OverlayEngine(
    [{ name: "c", columns: COLUMNS, base: base(rows) }],
    budgetBytes === undefined ? {} : { budgetBytes },
  );

const batch = (...operations: CanonicalOperation[]): CanonicalBatch => ({
  transactions: [{ operations, mutationIds: [] }],
});

const update = (
  index: number,
  changed: readonly { column: number; value: SqlValue }[],
): CanonicalOperation => ({
  kind: "update",
  collection: "c",
  primaryKey: keyBytes(index),
  key: key(index),
  changed,
});

const remove = (index: number): CanonicalOperation => ({
  kind: "delete",
  collection: "c",
  primaryKey: keyBytes(index),
  key: key(index),
});

const insert = (index: number, values: SqlValue[]): CanonicalOperation => ({
  kind: "insert",
  collection: "c",
  primaryKey: keyBytes(index),
  key: key(index),
  values,
});

describe("the canonical plane is a state, not a log", () => {
  test("a row updated fifty times holds one entry", () => {
    const engine = engineOf(100);
    for (let round = 0; round < 50; round++)
      engine.applyCanonical(
        batch(update(7, [{ column: COUNT, value: round }])),
      );
    const { canonical } = inspectOverlay(engine, "c");
    expect(canonical.patchedRows).toBe(1);
    expect(engine.overlayRowCount()).toBe(1);
    expect(engine.rows("c", 7, 1)).toEqual([
      ["row-7", "status-7", 49, "title-7"],
    ]);
  });

  test("a patch writes only its changed slots and reads through for the rest", () => {
    const engine = engineOf(100);
    engine.applyCanonical(
      batch(update(3, [{ column: STATUS, value: "closed" }])),
    );
    const row = inspectOverlay(engine, "c").canonical.row(3);
    expect(row?.values).toEqual([UNSET, "closed", UNSET, UNSET]);
    expect(row?.tombstone).toBe(false);
    // A canonical delta touches only the canonical plane.
    expect(inspectOverlay(engine, "c").pending.row(3)).toBeUndefined();
    expect(engine.rows("c", 3, 1)).toEqual([["row-3", "closed", 3, "title-3"]]);
  });

  test("a full tuple writes only the cells that differ from the base", () => {
    const engine = engineOf(100);
    engine.applyCanonical(
      batch(
        update(3, [
          { column: ID, value: "row-3" },
          { column: STATUS, value: "status-3" },
          { column: COUNT, value: 3 },
          { column: TITLE, value: "renamed" },
        ]),
      ),
    );
    expect(inspectOverlay(engine, "c").canonical.row(3)?.values).toEqual([
      UNSET,
      UNSET,
      UNSET,
      "renamed",
    ]);
    expect(engine.rows("c", 3, 1)).toEqual([
      ["row-3", "status-3", 3, "renamed"],
    ]);
  });

  test("an update that changes nothing allocates no row", () => {
    const engine = engineOf(100);
    const result = engine.applyCanonical(
      batch(update(3, [{ column: COUNT, value: 3 }])),
    );
    expect(result.changedCollections).toEqual(["c"]);
    expect(engine.estimatedBytes()).toBe(0);
    expect(engine.overlayRowCount()).toBe(0);
    expect(inspectOverlay(engine, "c").canonical.row(3)).toBeUndefined();
  });

  test("a written cell set back to its base value keeps the write", () => {
    const engine = engineOf(100);
    engine.applyCanonical(batch(update(3, [{ column: COUNT, value: 9 }])));
    engine.applyCanonical(batch(update(3, [{ column: COUNT, value: 3 }])));
    expect(inspectOverlay(engine, "c").canonical.row(3)?.values).toEqual([
      UNSET,
      UNSET,
      3,
      UNSET,
    ]);
    expect(engine.rows("c", 3, 1)).toEqual([
      ["row-3", "status-3", 3, "title-3"],
    ]);
  });

  test("a base value is held only by an identical value", () => {
    const engine = engineOf(100);
    // Row 0's count is 0 in the base: -0 is SQL-equal to it and still written.
    engine.applyCanonical(batch(update(0, [{ column: COUNT, value: -0 }])));
    expect(Object.is(engine.rows("c", 0, 1)[0]?.[COUNT], -0)).toBe(true);
    // Row 5's count is the number 5; the string "5" is another value.
    engine.applyCanonical(batch(update(5, [{ column: COUNT, value: "5" }])));
    expect(engine.rows("c", 5, 1)).toEqual([
      ["row-5", "status-5", "5", "title-5"],
    ]);
  });

  test("a byte string is held by the same bytes, not the same object", () => {
    const blob = (row: number) => Uint8Array.of(row, 0xff);
    const engine = new OverlayEngine([
      {
        name: "b",
        columns: ["id", "blob"],
        base: {
          rows: 4,
          lookupKey: (parts) =>
            ["b0", "b1", "b2", "b3"].indexOf(String(parts[0])),
          valueAt: (column, row) => (column === 0 ? `b${row}` : blob(row)),
        },
      },
    ]);
    const write = (row: number, value: Uint8Array) =>
      engine.applyCanonical(
        batch({
          kind: "update",
          collection: "b",
          primaryKey: new TextEncoder().encode(`b${row}`),
          key: [`b${row}`],
          changed: [{ column: 1, value }],
        }),
      );
    write(1, blob(1));
    write(2, Uint8Array.of(2, 0xfe));
    expect(inspectOverlay(engine, "b").canonical.row(1)).toBeUndefined();
    expect(inspectOverlay(engine, "b").canonical.row(2)?.values).toEqual([
      UNSET,
      Uint8Array.of(2, 0xfe),
    ]);
  });

  test("a truncated base holds no value for a later update", () => {
    const engine = engineOf(10);
    engine.applyCanonical(batch({ kind: "truncate", collection: "c" }));
    engine.applyCanonical(batch(insert(3, ["row-3", "open", 7, "fresh"])));
    engine.applyCanonical(batch(update(3, [{ column: COUNT, value: 3 }])));
    expect(engine.rows("c", 0, 1)).toEqual([["row-3", "open", 3, "fresh"]]);
  });

  test("overlay memory is bounded by distinct touched rows, not change volume", () => {
    const engine = engineOf(10_000);
    for (let round = 0; round < 200; round++)
      engine.applyCanonical(
        batch(
          ...Array.from({ length: 10 }, (_unused, index) =>
            update(index, [{ column: COUNT, value: round }]),
          ),
        ),
      );
    expect(engine.overlayRowCount()).toBe(10);
  });
});

describe("the pending plane", () => {
  test("attributes a delta through the mutation list, and unwind is a removal", () => {
    const engine = engineOf(50);
    engine.applyOptimistic({
      mutationId: "m1",
      actions: [
        {
          kind: "patch",
          collection: "c",
          primaryKey: keyBytes(4),
          key: key(4),
          changes: [{ column: STATUS, value: "guess", mode: "set" }],
        },
      ],
    });
    // Attribution lives on the pending-mutation list, never on a data row:
    // unwinding clears the plane and replays the survivors.
    expect(inspectOverlay(engine, "c").pending.row(4)).toBeDefined();
    expect(engine.pendingMutationIds()).toEqual(["m1"]);
    // An optimistic action never writes the canonical plane beneath it.
    expect(inspectOverlay(engine, "c").canonical.row(4)).toBeUndefined();
    expect(engine.rows("c", 4, 1)[0]?.[STATUS]).toBe("guess");

    engine.dropMutation("m1");
    expect(inspectOverlay(engine, "c").pending.patchedRows).toBe(0);
    // The base was never destroyed, so the removal is the whole unwind.
    expect(engine.rows("c", 4, 1)[0]?.[STATUS]).toBe("status-4");
  });

  test("an acknowledgement drops the deltas in the same step as the canonical apply", () => {
    const engine = engineOf(50);
    engine.applyOptimistic({
      mutationId: "m1",
      actions: [
        {
          kind: "patch",
          collection: "c",
          primaryKey: keyBytes(2),
          key: key(2),
          changes: [{ column: TITLE, value: "guess", mode: "set" }],
        },
      ],
    });
    const result = engine.applyCanonical({
      transactions: [
        {
          operations: [update(2, [{ column: TITLE, value: "truth" }])],
          mutationIds: ["m1"],
        },
      ],
    });
    expect(result.acknowledgedMutationIds).toEqual(["m1"]);
    expect(engine.pendingMutationIds()).toEqual([]);
    expect(engine.rows("c", 2, 1)[0]?.[TITLE]).toBe("truth");
  });

  test("a pending patch over a canonical tombstone is dropped from the fold", () => {
    const engine = engineOf(20);
    engine.applyOptimistic({
      mutationId: "m1",
      actions: [
        {
          kind: "patch",
          collection: "c",
          primaryKey: keyBytes(5),
          key: key(5),
          changes: [{ column: STATUS, value: "guess", mode: "set" }],
        },
      ],
    });
    engine.applyCanonical(batch(remove(5)));
    expect(engine.rows("c").some((row) => row[ID] === "row-5")).toBe(false);
    expect(engine.droppedActions()).toEqual([
      {
        mutationId: "m1",
        collection: "c",
        actionIndex: 0,
        reason: "absent-row",
      },
    ]);
    expect(engine.pendingMutationIds()).toEqual(["m1"]);
  });

  test("only a pending insert may clear a tombstone", () => {
    const engine = engineOf(20);
    engine.applyCanonical(batch(remove(5)));
    engine.applyOptimistic({
      mutationId: "m1",
      actions: [
        {
          kind: "insert",
          collection: "c",
          primaryKey: keyBytes(5),
          key: key(5),
          values: ["row-5", "revived", 99, "t"],
        },
      ],
    });
    expect(engine.rows("c").filter((row) => row[ID] === "row-5")).toEqual([
      ["row-5", "revived", 99, "t"],
    ]);
    // Reviving at the base address keeps the row in base order.
    expect(inspectOverlay(engine, "c").pending.insertedRows).toBe(0);
    expect(engine.rows("c")[5]?.[ID]).toBe("row-5");
  });

  test("refuses more than the pending-mutation bound", () => {
    const engine = engineOf(1_000);
    for (let index = 0; index < 256; index++)
      engine.applyOptimistic({
        mutationId: `m${index}`,
        actions: [
          {
            kind: "patch",
            collection: "c",
            primaryKey: keyBytes(index),
            key: key(index),
            changes: [{ column: COUNT, value: 1, mode: "increment" }],
          },
        ],
      });
    expect(() =>
      engine.applyOptimistic({
        mutationId: "overflow",
        actions: [
          {
            kind: "patch",
            collection: "c",
            primaryKey: keyBytes(900),
            key: key(900),
            changes: [{ column: COUNT, value: 1, mode: "increment" }],
          },
        ],
      }),
    ).toThrow(/CAPACITY_EXCEEDED/u);
  });

  test("refuses a mutation of more than 256 actions, and takes one of 256", () => {
    const engine = engineOf(1_000);
    const increments = (from: number, count: number) =>
      Array.from({ length: count }, (_unused, offset) => ({
        kind: "patch" as const,
        collection: "c",
        primaryKey: keyBytes(from + offset),
        key: key(from + offset),
        changes: [{ column: COUNT, value: 1, mode: "increment" as const }],
      }));
    expect(() =>
      engine.applyOptimistic({
        mutationId: "overflow",
        actions: increments(0, 257),
      }),
    ).toThrow("CAPACITY_EXCEEDED: at most 256 actions per mutation");
    expect(engine.pendingMutationIds()).toEqual([]);
    expect(inspectOverlay(engine, "c").pending.patchedRows).toBe(0);
    engine.applyOptimistic({ mutationId: "full", actions: increments(0, 256) });
    expect(engine.pendingMutationIds()).toEqual(["full"]);
    expect(inspectOverlay(engine, "c").pending.patchedRows).toBe(256);
  });
});

describe("merged reads through the order vector", () => {
  test("membership changes shift offsets and inserts land after the base", () => {
    const engine = engineOf(10);
    expect(engine.rowCount("c")).toBe(10);
    engine.applyCanonical(
      batch(remove(3), insert(99, ["row-99", "s", 1, "t"])),
    );
    expect(engine.rowCount("c")).toBe(10);
    const ids = engine.rows("c", 0, 20, [ID]).map((row) => row[0]);
    expect(ids).toEqual([
      "row-0",
      "row-1",
      "row-2",
      "row-4",
      "row-5",
      "row-6",
      "row-7",
      "row-8",
      "row-9",
      "row-99",
    ]);
    // The address at logical position 9 is an inserted one, above `base.rows`.
    expect(inspectOverlay(engine, "c").order[9]).toBeGreaterThanOrEqual(10);
  });

  test("a window is offset-addressed and projects the requested fields", () => {
    const engine = engineOf(10);
    expect(engine.rows("c", 2, 3, [ID, COUNT])).toEqual([
      ["row-2", 2],
      ["row-3", 3],
      ["row-4", 4],
    ]);
  });

  test("a stale view is refused rather than served as a torn read", () => {
    const engine = engineOf(10);
    const view = engine.createView("c");
    expect(engine.rowsFromView(view, 0, 2, [ID])).toEqual([
      ["row-0"],
      ["row-1"],
    ]);
    engine.applyCanonical(batch(remove(0)));
    expect(() => engine.rowsFromView(view)).toThrow(/VIEW_STALE/u);
    expect(engine.generation("c")).toBeGreaterThan(view.generation);
  });

  test("an idle interaction does not invalidate a view", () => {
    const engine = engineOf(10);
    const view = engine.createView("c");
    expect(engine.rows("c", 0, 1)).toHaveLength(1);
    expect(engine.rowCount("c")).toBe(10);
    expect(() => engine.rowsFromView(view, 0, 1)).not.toThrow();
  });
});

describe("truncate", () => {
  test("suppresses the base wholesale and empties both maps", () => {
    const engine = engineOf(10);
    engine.applyCanonical(batch(update(1, [{ column: STATUS, value: "x" }])));
    expect(engine.overlayRowCount()).toBe(1);
    engine.applyCanonical(batch({ kind: "truncate", collection: "c" }));
    expect(engine.rows("c")).toEqual([]);
    expect(engine.overlayRowCount()).toBe(0);
    expect(inspectOverlay(engine, "c").truncated).toBe(true);
  });

  test("a base key re-enters only through a complete row image", () => {
    const engine = engineOf(10);
    engine.applyCanonical(batch({ kind: "truncate", collection: "c" }));
    engine.applyCanonical(batch(insert(4, ["row-4", "back", 7, "t"])));
    expect(engine.rows("c")).toEqual([["row-4", "back", 7, "t"]]);
    expect(() =>
      engine.applyCanonical(batch(update(5, [{ column: STATUS, value: "x" }]))),
    ).toThrow(/RESNAPSHOT_REQUIRED/u);
  });
});

describe("the structural integrity check", () => {
  test("an update whose key resolves to no live row escalates", () => {
    const engine = engineOf(10);
    expect(() =>
      engine.applyCanonical(batch(update(50, [{ column: COUNT, value: 1 }]))),
    ).toThrow(/RESNAPSHOT_REQUIRED/u);
  });

  test("a delete whose key resolves to no live row escalates", () => {
    const engine = engineOf(10);
    engine.applyCanonical(batch(remove(2)));
    expect(() => engine.applyCanonical(batch(remove(2)))).toThrow(
      /RESNAPSHOT_REQUIRED/u,
    );
  });

  test("an insert whose key resolves to a live row escalates", () => {
    const engine = engineOf(10);
    expect(() =>
      engine.applyCanonical(batch(insert(2, ["row-2", "s", 1, "t"]))),
    ).toThrow(/RESNAPSHOT_REQUIRED/u);
  });

  test("a duplicate acknowledgement in one transaction is refused", () => {
    const engine = engineOf(10);
    expect(() =>
      engine.applyCanonical({
        transactions: [{ operations: [], mutationIds: ["m1", "m1"] }],
      }),
    ).toThrow(/CHANGE_STREAM_INVALID/u);
  });

  test("an operation naming an unknown collection is refused", () => {
    const engine = engineOf(10);
    expect(() =>
      engine.applyCanonical(batch({ kind: "truncate", collection: "nope" })),
    ).toThrow(OverlayError);
  });
});

describe("the overlay budget", () => {
  test("exhaustion asks for a fresh base rather than compacting in place", () => {
    const engine = engineOf(10_000, 4_000);
    expect(() => {
      for (let index = 0; index < 200; index++)
        engine.applyCanonical(
          batch(update(index, [{ column: COUNT, value: index + 1 }])),
        );
    }).toThrow(/OVERLAY_BUDGET_EXHAUSTED/u);
  });
});

describe("the cell store", () => {
  const WIDE = Array.from({ length: 20 }, (_unused, index) =>
    index === 0 ? "id" : `c${index}`,
  );

  /** A twenty-column base, wide enough for every sparse capacity class. */
  const wideEngine = () =>
    new OverlayEngine([
      {
        name: "c",
        columns: WIDE,
        base: {
          rows: 100,
          lookupKey: (parts) => {
            const first = parts[0];
            if (typeof first !== "string" || !first.startsWith("row-"))
              return -1;
            const index = Number(first.slice(4));
            return Number.isInteger(index) && index >= 0 && index < 100
              ? index
              : -1;
          },
          valueAt: (column, row) =>
            column === ID ? `row-${row}` : `base-${column}-${row}`,
        },
      },
    ]);

  const wideUpdate = (
    index: number,
    changed: readonly { column: number; value: SqlValue }[],
  ): CanonicalBatch => ({
    transactions: [
      {
        operations: [
          {
            kind: "update",
            collection: "c",
            primaryKey: keyBytes(index),
            key: key(index),
            changed,
          },
        ],
        mutationIds: [],
      },
    ],
  });

  test("a column written to NULL still reads NULL, not the base value", () => {
    const engine = engineOf(10);
    engine.applyCanonical(batch(update(2, [{ column: STATUS, value: null }])));
    expect(engine.rows("c", 2, 1)).toEqual([["row-2", null, 2, "title-2"]]);
    // The stored NULL and the three columns no delta wrote are different facts.
    expect(inspectOverlay(engine, "c").canonical.row(2)?.values).toEqual([
      UNSET,
      null,
      UNSET,
      UNSET,
    ]);
  });

  test("a row grown one column at a time keeps every cell across every promotion", () => {
    const engine = wideEngine();
    for (let column = 1; column < WIDE.length; column++) {
      engine.applyCanonical(
        wideUpdate(7, [{ column, value: `set-${column}` }]),
      );
      // Every column written so far still reads its own value, and every column
      // not yet written still falls through to the base.
      const row = engine.rows("c", 7, 1)[0] ?? [];
      for (let probe = 1; probe < WIDE.length; probe++)
        expect(row[probe]).toBe(
          probe <= column ? `set-${probe}` : `base-${probe}-7`,
        );
    }
  });

  test("a NULL written before a promotion survives the promotion", () => {
    const engine = wideEngine();
    engine.applyCanonical(wideUpdate(3, [{ column: 1, value: null }]));
    for (let column = 2; column < WIDE.length; column++)
      engine.applyCanonical(wideUpdate(3, [{ column, value: column }]));
    expect(engine.rows("c", 3, 1)[0]?.[1]).toBeNull();
    expect(inspectOverlay(engine, "c").canonical.row(3)?.values[1]).toBeNull();
  });

  test("a delete returns the row's cells and a revival reuses them", () => {
    const engine = wideEngine();
    for (let column = 1; column < WIDE.length; column++)
      engine.applyCanonical(wideUpdate(4, [{ column, value: column }]));
    const dense = engine.estimatedBytes();
    engine.applyCanonical(
      batch({
        kind: "delete",
        collection: "c",
        primaryKey: keyBytes(4),
        key: key(4),
      }),
    );
    engine.applyCanonical(
      batch({
        kind: "insert",
        collection: "c",
        primaryKey: keyBytes(4),
        key: key(4),
        values: WIDE.map((_unused, column) => `back-${column}`),
      }),
    );
    // The revived row's dense run came off the free list rather than extending
    // the slab, so a delete/insert cycle does not leak cells into the budget.
    expect(engine.estimatedBytes()).toBe(dense);
    expect(engine.rows("c", 4, 1)[0]?.[5]).toBe("back-5");
  });

  test("the budget charges cells a delta wrote, not columns the collection has", () => {
    const sparse = wideEngine();
    sparse.applyCanonical(wideUpdate(1, [{ column: 3, value: "x" }]));
    const image = wideEngine();
    image.applyCanonical(
      batch({
        kind: "delete",
        collection: "c",
        primaryKey: keyBytes(1),
        key: key(1),
      }),
    );
    image.applyCanonical(
      batch({
        kind: "insert",
        collection: "c",
        primaryKey: keyBytes(1),
        key: key(1),
        values: WIDE.map((_unused, column) => column),
      }),
    );
    expect(sparse.overlayRowCount()).toBe(image.overlayRowCount());
    expect(sparse.estimatedBytes()).toBeLessThan(image.estimatedBytes());
  });

  test("a fold releases the pending plane's storage entirely", () => {
    const engine = wideEngine();
    const empty = engine.estimatedBytes();
    engine.applyOptimistic({
      mutationId: "m1",
      actions: [
        {
          kind: "patch",
          collection: "c",
          primaryKey: keyBytes(9),
          key: key(9),
          changes: [{ column: 2, value: "guess", mode: "set" }],
        },
      ],
    });
    expect(engine.estimatedBytes()).toBeGreaterThan(empty);
    engine.dropMutation("m1");
    expect(engine.estimatedBytes()).toBe(empty);
  });
});

describe("the budget is an upper bound, not an estimate", () => {
  test("the order vector is charged, because a read allocates it", () => {
    const engine = engineOf(10_000);
    engine.applyCanonical(batch(update(1, [{ column: STATUS, value: "x" }])));
    const beforeRead = engine.estimatedBytes();
    // The first read builds the order vector, one Uint32Array of base.rows; a
    // budget that ignored it would read below the engine's footprint.
    engine.rowCount("c");
    expect(engine.estimatedBytes()).toBe(beforeRead + 10_000 * 4);
  });

  test("exhaustion can be reached by the order vector alone", () => {
    // A base too large for the budget is a resnapshot condition before any
    // delta lands, reported as one rather than as an allocation failure.
    const engine = engineOf(100_000, 4_000);
    engine.rowCount("c");
    expect(() =>
      engine.applyCanonical(batch(update(1, [{ column: STATUS, value: "x" }]))),
    ).toThrow(/OVERLAY_BUDGET_EXHAUSTED/u);
  });
});

describe("a partial delta application is sealed, never published", () => {
  /**
   * Two transactions arrive in one batch. The first is legal; the second
   * deletes a row the base never had, which raises `RESNAPSHOT_REQUIRED`. The
   * planes are mutated in place, so at the throw transaction 1 is applied: a
   * state PostgreSQL never had.
   */
  const tornBatch = (): CanonicalBatch => ({
    transactions: [
      {
        operations: [update(1, [{ column: STATUS, value: "torn" }])],
        mutationIds: [],
      },
      { operations: [remove(9_999)], mutationIds: [] },
    ],
  });

  test("the batch is refused", () => {
    const engine = engineOf(10);
    expect(() => engine.applyCanonical(tornBatch())).toThrow(OverlayError);
    expect(engine.sealed).toBe(true);
  });

  test("no reader can observe the half-applied transaction", () => {
    const engine = engineOf(10);
    expect(engine.rows("c", 1, 1)[0]?.[STATUS]).toBe("status-1");
    expect(() => engine.applyCanonical(tornBatch())).toThrow(OverlayError);
    for (const read of [
      () => engine.rows("c", 1, 1),
      () => engine.rowCount("c"),
      () => engine.generation("c"),
      () => engine.createView("c"),
      () => inspectOverlay(engine, "c"),
    ])
      expect(read).toThrow(/RESNAPSHOT_REQUIRED.*sealed/su);
  });

  test("a sealed engine refuses further deltas rather than compounding one", () => {
    const engine = engineOf(10);
    expect(() => engine.applyCanonical(tornBatch())).toThrow(OverlayError);
    expect(() =>
      engine.applyCanonical(batch(update(2, [{ column: STATUS, value: "x" }]))),
    ).toThrow(/RESNAPSHOT_REQUIRED.*sealed/su);
  });
});
