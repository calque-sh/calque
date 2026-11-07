/**
 * Three numbers the overlay's design rests on, each printed with the rule that
 * would reject the design. Run with `pnpm --filter @calque/core run measure`;
 * it stays outside the `test` gate, which must not depend on timing.
 *
 * 1. Coalesced order-vector rebuild against the 250 ms commit-to-paint budget.
 *    A single-change frame above about 20 ms means a shift index should
 *    replace the order vector.
 * 2. Overlay row memory, on which the budget's row headroom rests. A
 *    twenty-column row above about 500 bytes fails. A row's cost depends on how
 *    many columns its delta touched, so 2a reports a sparse patch and a full
 *    row image separately.
 * 3. The fold against SQL replay, over the same action stream.
 */
import { test } from "vitest";

import {
  OverlayEngine,
  type CanonicalBatch,
  type CanonicalOperation,
  type KeyPart,
  type OptimisticMutation,
  type OverlayBase,
  type SqlValue,
} from "./overlay.js";
import { SqliteReference } from "./sqlite-reference.fixture.js";

const MILLION = 1_000_000;

const columnNames = (width: number) =>
  Array.from({ length: width }, (_unused, index) =>
    index === 0 ? "id" : `c${index}`,
  );

/**
 * A synthetic base: `row-<n>` resolves to physical row `n` in constant time.
 */
function syntheticBase(rows: number): OverlayBase {
  return {
    rows,
    lookupKey: (parts) => {
      const first = parts[0];
      if (typeof first !== "string" || !first.startsWith("row-")) return -1;
      const index = Number(first.slice(4));
      return Number.isInteger(index) && index >= 0 && index < rows ? index : -1;
    },
    valueAt: (column, row) => (column === 0 ? `row-${row}` : row + column),
  };
}

const key = (index: number): readonly KeyPart[] => [`row-${index}`];
const encoder = new TextEncoder();
const keyBytes = (index: number) => encoder.encode(`row-${index}`);

const median = (values: number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? Number.NaN;
};

const ms = (value: number) => `${value.toFixed(3)} ms`;

function collect(): void {
  const gc = (globalThis as { gc?: () => void }).gc;
  if (!gc) return;
  gc();
  gc();
  gc();
}

// ------------------------------------------------ 1: order-vector rebuild

test("measurement 1: coalesced order-vector rebuild at one million rows", () => {
  const width = 20;
  const names = columnNames(width);
  const rows = MILLION;
  const repeats = 7;
  const lines: string[] = [];

  for (const changes of [0, 1, 10, 100, 10_000]) {
    const applied: number[] = [];
    const rebuilt: number[] = [];
    const windowed: number[] = [];
    for (let round = 0; round < repeats; round++) {
      const engine = new OverlayEngine(
        [{ name: "c", columns: names, base: syntheticBase(rows) }],
        { budgetBytes: Number.MAX_SAFE_INTEGER },
      );
      // Open the collection: the unfiltered order vector is built as identity.
      engine.rowCount("c");

      const operations: CanonicalOperation[] =
        changes === 0
          ? [
              {
                kind: "update",
                collection: "c",
                primaryKey: keyBytes(500_000),
                key: key(500_000),
                changed: [{ column: 3, value: "patched" }],
              },
            ]
          : Array.from({ length: changes }, (_unused, index) => {
              const row = (index * 7919 + 13) % rows;
              return {
                kind: "delete" as const,
                collection: "c",
                primaryKey: keyBytes(row),
                key: key(row),
              };
            });
      const batch: CanonicalBatch = {
        transactions: [{ operations, mutationIds: [] }],
      };

      const t0 = performance.now();
      engine.applyCanonical(batch);
      const t1 = performance.now();
      engine.rowCount("c");
      const t2 = performance.now();
      engine.rows("c", 400_000, 200);
      const t3 = performance.now();
      applied.push(t1 - t0);
      rebuilt.push(t2 - t1);
      windowed.push(t3 - t2);
    }
    lines.push(
      `  k=${String(changes).padStart(5)} membership changes  apply ${ms(median(applied))}  rebuild ${ms(median(rebuilt))}  rows(400k,200) ${ms(median(windowed))}`,
    );
  }

  const singleChange = lines[1] ?? "";
  console.log(
    [
      "",
      "MEASUREMENT 1 -- coalesced order-vector rebuild, 1,000,000 rows x 20 columns",
      "  kill rule: a one-change frame above ~20 ms means build a shift index",
      ...lines,
      `  k=0 is a pure column patch: it must not dirty the order vector at all.`,
      `  one-change frame: ${singleChange.trim()}`,
      "",
    ].join("\n"),
  );
}, 300_000);

// ---------------------------------------------------- 2: overlay row memory

interface Cell {
  readonly bytesPerRow: number;
  readonly externalPerRow: number;
  readonly totalPerRow: number;
  readonly totalMiB: number;
  /** What `estimatedBytes()` claims, to check the budget's own model. */
  readonly accounted: number;
  readonly rows: number;
}

/**
 * Total bytes this process holds, JS heap plus off-heap. `arrayBuffers` is not
 * added: Node documents it as a subset of `external`, so summing the two would
 * count every typed-array backing store twice.
 */
function residentBytes(): number {
  const usage = process.memoryUsage();
  return usage.heapUsed + usage.external;
}

/**
 * One grid cell, in its own scope so the engine it builds is unreachable once
 * it returns and can be collected before the next cell takes its baseline.
 * `changed` is how many columns each patch writes; `changed === width` is a
 * full row image. The typed arrays land off-heap and the cell slabs on it, so
 * `totalPerRow` is the figure to read, with the split printed beside it.
 */
function measureCell(width: number, entries: number, changed: number): Cell {
  const names = columnNames(width);
  const columns = Array.from({ length: changed }, (_unused, index) => index);
  collect();
  const beforeHeap = process.memoryUsage().heapUsed;
  const before = residentBytes();
  const engine = new OverlayEngine(
    [{ name: "c", columns: names, base: syntheticBase(entries) }],
    { budgetBytes: Number.MAX_SAFE_INTEGER },
  );
  const chunk = 5_000;
  for (let start = 0; start < entries; start += chunk) {
    const operations: CanonicalOperation[] = [];
    for (let index = start; index < Math.min(start + chunk, entries); index++)
      operations.push({
        kind: "update",
        collection: "c",
        primaryKey: keyBytes(index),
        key: key(index),
        changed: columns.map((column) => ({ column, value: index + column })),
      });
    engine.applyCanonical({ transactions: [{ operations, mutationIds: [] }] });
  }
  collect();
  const total = residentBytes() - before;
  const heap = process.memoryUsage().heapUsed - beforeHeap;
  const rows = engine.overlayRowCount();
  return {
    bytesPerRow: heap / rows,
    externalPerRow: (total - heap) / rows,
    totalPerRow: total / rows,
    totalMiB: total / 1_048_576,
    accounted: engine.estimatedBytes() / rows,
    rows,
  };
}

const cellLine = (label: string, cell: Cell) =>
  `  ${label}: ${cell.totalPerRow.toFixed(1)} B/row measured ` +
  `(${cell.bytesPerRow.toFixed(1)} heap + ${cell.externalPerRow.toFixed(1)} off-heap), ` +
  `estimatedBytes() ${cell.accounted.toFixed(1)}, ` +
  `${cell.totalMiB.toFixed(1)} MiB for ${cell.rows} rows`;

test("measurement 2: overlay row memory, one changed column", () => {
  const lines: string[] = [];
  for (const width of [10, 20, 50]) {
    for (const entries of [10_000, 100_000, 1_000_000]) {
      const cell = measureCell(width, entries, 1);
      collect();
      lines.push(
        cellLine(
          `${String(width).padStart(2)} columns x ${String(entries).padStart(9)} rows`,
          cell,
        ),
      );
    }
  }
  console.log(
    [
      "",
      "MEASUREMENT 2 -- overlay memory, a ONE-COLUMN patch per row",
      "  kill rule: a 20-column row above ~500 B means a columnar overlay",
      "  this is the CDC and bulk-write shape: one column of `width` changed",
      "  (forced GC via NODE_OPTIONS=--expose-gc; the 10^4 cells are GC noise)",
      ...lines,
      "",
    ].join("\n"),
  );
}, 900_000);

test("measurement 2a: sparse patch versus full row image", () => {
  const width = 20;
  const entries = 1_000_000;
  const lines: string[] = [];
  for (const changed of [1, 3, width]) {
    const cell = measureCell(width, entries, changed);
    collect();
    lines.push(
      cellLine(
        `${String(changed).padStart(2)} of ${width} columns changed`,
        cell,
      ),
    );
  }
  console.log(
    [
      "",
      "MEASUREMENT 2a -- what a row costs as a function of what it changed",
      "  20 columns x 1,000,000 rows",
      "  1 and 3 changed columns are the realistic CDC / optimistic patch;",
      "  20 of 20 is a full row image, which the store holds as a dense run.",
      ...lines,
      "",
    ].join("\n"),
  );
}, 900_000);

/**
 * Where a twenty-column row's bytes go, varied one field at a time through the
 * real engine, so the grid's per-row cost can be split by part of the store.
 */
test("measurement 2b: the reshaped row, field by field", () => {
  const entries = 1_000_000;
  const width = 20;

  /** A patch touching `changed` columns. */
  const build = (changed: number) => {
    const names = columnNames(width);
    const columns = Array.from({ length: changed }, (_unused, i) => i);
    collect();
    const before = residentBytes();
    const engine = new OverlayEngine(
      [{ name: "c", columns: names, base: syntheticBase(entries) }],
      { budgetBytes: Number.MAX_SAFE_INTEGER },
    );
    const chunk = 5_000;
    for (let start = 0; start < entries; start += chunk) {
      const operations: CanonicalOperation[] = [];
      for (let index = start; index < Math.min(start + chunk, entries); index++)
        operations.push({
          kind: "update",
          collection: "c",
          primaryKey: keyBytes(index),
          key: key(index),
          changed: columns.map((column) => ({ column, value: index + column })),
        });
      engine.applyCanonical({
        transactions: [{ operations, mutationIds: [] }],
      });
    }
    collect();
    return (residentBytes() - before) / engine.overlayRowCount();
  };

  const bare = build(0);
  collect();
  const one = build(1);
  collect();
  const three = build(3);
  collect();
  const dense = build(width);
  collect();

  console.log(
    [
      "",
      "MEASUREMENT 2b -- where a reshaped 20-column row's bytes go (1,000,000 rows)",
      `  slot + Map<address,slot> entry, no cells           ${bare.toFixed(1)} B`,
      `  + one sparse cell                                  ${one.toFixed(1)} B  (cell costs ${(one - bare).toFixed(1)} B)`,
      `  + three sparse cells                               ${three.toFixed(1)} B  (cells cost ${(three - bare).toFixed(1)} B)`,
      `  + a dense twenty-cell run (full row image)         ${dense.toFixed(1)} B  (cells cost ${(dense - bare).toFixed(1)} B)`,
      "",
      "  For comparison, the dense-array shape this replaced: 501.4 B on the heap",
      "  plus 9.9 B off it, of which 200.0 B was the per-row key Uint8Array and",
      "  208.0 B the dense twenty-slot values array. Both are gone.",
      "",
    ].join("\n"),
  );
}, 900_000);

/**
 * Whether `estimatedBytes()` bounds what the engine holds after a read, at the
 * shape where the budget binds. The grid above never reads, so it never
 * allocates the order vector, which the budget must also charge.
 */
test("measurement 2c: does estimatedBytes() bound the engine's footprint?", () => {
  const width = 20;
  const entries = MILLION;
  const names = columnNames(width);
  collect();
  const before = residentBytes();
  const engine = new OverlayEngine(
    [{ name: "c", columns: names, base: syntheticBase(entries) }],
    { budgetBytes: Number.MAX_SAFE_INTEGER },
  );
  const chunk = 5_000;
  for (let start = 0; start < entries; start += chunk) {
    const operations: CanonicalOperation[] = [];
    for (let index = start; index < Math.min(start + chunk, entries); index++)
      operations.push({
        kind: "update",
        collection: "c",
        primaryKey: keyBytes(index),
        key: key(index),
        changed: [{ column: 3, value: index }],
      });
    engine.applyCanonical({ transactions: [{ operations, mutationIds: [] }] });
  }
  collect();
  const writesOnly = residentBytes() - before;
  const accountedBeforeRead = engine.estimatedBytes();
  // A read allocates the order vector the write path never touches.
  engine.rowCount("c");
  engine.rows("c", 400_000, 10);
  collect();
  const afterRead = residentBytes() - before;
  const accountedAfterRead = engine.estimatedBytes();
  const rows = engine.overlayRowCount();

  const mib = (value: number) => `${(value / 1_048_576).toFixed(1)} MiB`;
  console.log(
    [
      "",
      "MEASUREMENT 2c -- estimatedBytes() against the engine's real footprint",
      "  1,000,000 rows x 20 columns, one changed column per row",
      `  writes only, no read:  measured ${mib(writesOnly)}  estimatedBytes() ${mib(accountedBeforeRead)}  ` +
        `(${(writesOnly / rows).toFixed(1)} vs ${(accountedBeforeRead / rows).toFixed(1)} B/row)`,
      `  after one read:        measured ${mib(afterRead)}  estimatedBytes() ${mib(accountedAfterRead)}  ` +
        `(${(afterRead / rows).toFixed(1)} vs ${(accountedAfterRead / rows).toFixed(1)} B/row)`,
      `  the read costs ${mib(afterRead - writesOnly)}: the order vector is 4 B per base row.`,
      "  A budget that does not charge it is not an upper bound on the engine.",
      "",
    ].join("\n"),
  );
}, 900_000);

// ------------------------------------------------- 3: fold vs SQL replay

test("measurement 3: the fold against SQL replay", () => {
  const width = 8;
  const names = columnNames(width);
  const baseRows = 20_000;
  const repeats = 20;
  const lines: string[] = [];

  const referenceSchema = [
    {
      name: "c",
      columns: names.map((name, index) => ({
        name,
        affinity: index === 0 ? ("TEXT" as const) : ("REAL" as const),
        nullable: false,
      })),
      primaryKey: [0],
    },
  ];

  const seedRow = (index: number): SqlValue[] => [
    `row-${index}`,
    ...Array.from({ length: width - 1 }, (_unused, column) => index + column),
  ];

  for (const pendingCount of [1, 8, 64]) {
    const actionsPer = 4;
    const seed = new Map<string, SqlValue[][]>([
      [
        "c",
        Array.from({ length: baseRows }, (_unused, index) => seedRow(index)),
      ],
    ]);
    const engine = new OverlayEngine(
      [{ name: "c", columns: names, base: syntheticBase(baseRows) }],
      { budgetBytes: Number.MAX_SAFE_INTEGER },
    );
    const reference = new SqliteReference(referenceSchema, seed);
    try {
      const mutations: OptimisticMutation[] = Array.from(
        { length: pendingCount },
        (_unused, index) => ({
          mutationId: `m${index}`,
          actions: Array.from({ length: actionsPer }, (_unused2, action) => {
            const row = (index * actionsPer + action) * 3 + 1;
            return {
              kind: "patch" as const,
              collection: "c",
              primaryKey: keyBytes(row),
              key: key(row),
              changes: [
                { column: 1, value: 1, mode: "increment" as const },
                { column: 2, value: 2, mode: "set" as const },
              ],
            };
          }),
        }),
      );
      for (const mutation of mutations) {
        engine.applyOptimistic(mutation);
        reference.submitMutation(mutation);
      }

      const overlayTimes: number[] = [];
      const sqliteTimes: number[] = [];
      for (let round = 0; round < repeats; round++) {
        const operations: CanonicalOperation[] = Array.from(
          { length: 8 },
          (_unused, index) => {
            const row = 10_000 + round * 8 + index;
            return {
              kind: "update" as const,
              collection: "c",
              primaryKey: keyBytes(row),
              key: key(row),
              changed: [{ column: 3, value: round }],
            };
          },
        );
        const batch: CanonicalBatch = {
          transactions: [{ operations, mutationIds: [] }],
        };
        const t0 = performance.now();
        engine.applyCanonical(batch);
        const t1 = performance.now();
        reference.applyBatch(batch);
        const t2 = performance.now();
        overlayTimes.push(t1 - t0);
        sqliteTimes.push(t2 - t1);
      }
      const overlay = median(overlayTimes);
      const sqlite = median(sqliteTimes);
      lines.push(
        `  ${String(pendingCount).padStart(3)} pending x ${actionsPer} actions: ` +
          `fold ${ms(overlay)}  SQL replay ${ms(sqlite)}  ` +
          `ratio ${(sqlite / overlay).toFixed(1)}x`,
      );
    } finally {
      reference.close();
    }
  }
  console.log(
    [
      "",
      "MEASUREMENT 3 -- the fold against SQL replay, same action stream",
      "  8-operation canonical batch over a 20,000-row base, median of 20",
      ...lines,
      "",
    ].join("\n"),
  );
}, 300_000);
