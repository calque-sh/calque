/**
 * Ordered windows, the keyed sort under them, and queries run as tasks,
 * against the naive oracle: a full sort of projected rows. Every fixture is
 * drawn from a fixed seed, so a failure names a case that replays.
 */
import { describe, expect, test } from "vitest";

import {
  artifactQueryVector,
  openDirectColumnarArtifact,
  type DirectColumnarSchema,
} from "./artifact.js";
import { encodeArtifact } from "./artifact.fixture.js";
import { OverlayEngine, artifactBase, type SqlValue } from "./overlay.js";
import {
  ColumnarQueryEngine,
  MAX_QUERY_RESTARTS,
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  inspectQueryExecution,
  type QueryCompletion,
  type QueryOrder,
  type QueryPredicate,
  type QueryProjection,
  type QueryResult,
  type QuerySpecification,
  type QueryTask,
} from "./query.js";
import {
  referenceQuery,
  type QueryReferenceSchema,
} from "./query-reference.fixture.js";

type FixtureRow = Readonly<Record<string, unknown>>;

/**
 * Each ordering kind the sort reads: an int64 and a timestamp, keyed by their
 * encoded order; a dictionary whose codes order as its texts, keyed by code;
 * one holding characters outside the Basic Multilingual Plane, whose codes do
 * not; and a text column, read as values.
 */
const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "events",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "level", type: "dict", nullable: true },
        { name: "glyph", type: "dict", nullable: true },
        { name: "amount", type: "int64", nullable: true },
        { name: "occurred", type: "timestamp", nullable: true },
        { name: "note", type: "utf8", nullable: true },
      ],
    },
  ],
};

const COLUMNS = SCHEMA.tables[0]?.columns.map((column) => column.name) ?? [];
const SCHEMAS: readonly QueryReferenceSchema[] = [
  { name: "events", columns: COLUMNS },
];

/** mulberry32: a small generator whose sequence a seed fixes. */
function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 2 ** 32;
  };
}

function pick<T>(random: () => number, values: readonly T[]): T {
  const value = values[Math.floor(random() * values.length)];
  if (value === undefined) throw new Error("pick from an empty list");
  return value;
}

const LEVELS = ["debug", "info", "warn", "érreur", "", "�"];
/**
 * U+FF01 sorts before U+1F600 by code point, so by UTF-8 bytes and by code,
 * and after it by UTF-16 code unit, which is how JavaScript compares them.
 */
const GLYPHS = ["！", "\u{1F600}", "a", "", "\u{10000}"];

const MICROS_2026 = BigInt(Date.UTC(2026, 2, 1)) * 1_000n;
/** Outside the safe microsecond range. */
const YEAR_1500 = BigInt(Date.UTC(1500, 0, 1)) * 1_000n;

/**
 * Values from small domains, so every ordering column holds long runs of
 * equal keys and only the row tie-break separates them; an amount outside
 * the safe range and a timestamp before it, which no key stands for.
 */
function eventRow(random: () => number, id: number): FixtureRow {
  const amount = (): bigint | null => {
    const draw = random();
    if (draw < 0.08) return null;
    if (draw < 0.1) return pick(random, [2n ** 62n, -(2n ** 62n)]);
    return BigInt(Math.floor(random() * 9) - 4);
  };
  const occurred = (): bigint | null => {
    const draw = random();
    if (draw < 0.08) return null;
    if (draw < 0.1) return YEAR_1500;
    return (
      MICROS_2026 +
      BigInt(Math.floor(random() * 4)) * 1_000n +
      pick(random, [0n, 0n, 1n, 999n])
    );
  };
  return {
    id: BigInt(id),
    level: random() < 0.1 ? null : pick(random, LEVELS),
    glyph: random() < 0.1 ? null : pick(random, GLYPHS),
    amount: amount(),
    occurred: occurred(),
    note: random() < 0.1 ? null : `note-${String(Math.floor(random() * 6))}`,
  };
}

async function overlayOf(rows: readonly FixtureRow[]): Promise<OverlayEngine> {
  const encoded = encodeArtifact(SCHEMA, new Map([["events", rows]]));
  const workspace = await openDirectColumnarArtifact(
    encoded.bytes.slice().buffer,
    {
      byteLength: encoded.byteLength,
      digest: encoded.digest,
      identity: encoded.identity,
      schema: SCHEMA,
    },
  );
  return new OverlayEngine([
    {
      name: "events",
      columns: COLUMNS,
      base: artifactBase(workspace.table("events"), COLUMNS),
    },
  ]);
}

const fixtureRows = (count: number, seed: number): readonly FixtureRow[] => {
  const random = generator(seed);
  return Array.from({ length: count }, (_unused, id) => eventRow(random, id));
};

const eventKey = (id: number) => ({
  collection: "events",
  primaryKey: new TextEncoder().encode(String(id)),
  key: [BigInt(id)],
});

function eventUpdate(id: number, values: Record<string, SqlValue>) {
  return {
    kind: "update" as const,
    ...eventKey(id),
    changed: Object.entries(values).map(([name, value]) => ({
      column: COLUMNS.indexOf(name),
      value,
    })),
  };
}

function eventInsert(id: number, values: Record<string, SqlValue>) {
  const row: Record<string, SqlValue> = {
    id: BigInt(id),
    level: null,
    glyph: null,
    amount: null,
    occurred: null,
    note: null,
    ...values,
  };
  return {
    kind: "insert" as const,
    ...eventKey(id),
    values: COLUMNS.map((name) => row[name] ?? null),
  };
}

function applyCanonical(
  overlay: OverlayEngine,
  operations: readonly unknown[],
): void {
  overlay.applyCanonical({
    transactions: [{ mutationIds: [], operations }],
  } as Parameters<OverlayEngine["applyCanonical"]>[0]);
}

/**
 * Overlay values for each ordering column: ones a key stands for (a held
 * dictionary text, a canonical timestamp), ones none does (a text the
 * dictionary does not hold, a timestamp without milliseconds, an int64 past
 * the safe range), nulls, deletes, and inserts past the base.
 */
function applyDirtyState(overlay: OverlayEngine): void {
  applyCanonical(overlay, [
    eventUpdate(1, { level: "info", amount: 3n }),
    eventUpdate(2, { level: "never-in-dictionary", glyph: "\u{1F600}" }),
    eventUpdate(3, { level: null, occurred: "2026-03-01T00:00:00.001Z" }),
    eventUpdate(4, { occurred: "2026-03-01T00:00:00Z", amount: 2n ** 60n }),
    eventUpdate(5, { occurred: "2026-03-01T00:00:00.000999Z", note: null }),
    eventUpdate(6, { glyph: "zz", amount: null }),
    eventUpdate(7, { note: "note-0", level: "" }),
    { kind: "delete", ...eventKey(8) },
    { kind: "delete", ...eventKey(900) },
    eventInsert(100_000, {
      level: "brand-new",
      glyph: "！",
      amount: -4n,
      occurred: "1999-12-31T23:59:59.999Z",
      note: "inserted",
    }),
    eventInsert(100_001, { level: "debug", amount: 0n }),
  ]);
  overlay.applyOptimistic({
    mutationId: "query-window-pending",
    actions: [
      {
        kind: "patch",
        ...eventKey(10),
        changes: [
          { column: COLUMNS.indexOf("level"), value: "warn", mode: "set" },
          { column: COLUMNS.indexOf("amount"), value: -1n, mode: "set" },
        ],
      },
      { kind: "delete", ...eventKey(11) },
    ],
  });
}

/** A truncated collection has no encoded base cells to read. */
function applyTruncatedState(overlay: OverlayEngine): void {
  applyCanonical(overlay, [
    { kind: "truncate", collection: "events" },
    ...Array.from({ length: 40 }, (_unused, id) =>
      eventInsert(id, {
        level: pick(generator(id), [...LEVELS, null]),
        amount: BigInt(id % 3),
        occurred: id % 5 === 0 ? null : "2026-03-01T00:00:00.001Z",
      }),
    ),
  ]);
}

type OverlayState = "clean" | "dirty" | "truncated";
const STATES: readonly OverlayState[] = ["clean", "dirty", "truncated"];
const MIXED_ROWS = 3_000;

async function overlayIn(state: OverlayState): Promise<OverlayEngine> {
  const overlay = await overlayOf(fixtureRows(MIXED_ROWS, 0x0dde7));
  if (state === "dirty") applyDirtyState(overlay);
  if (state === "truncated") applyTruncatedState(overlay);
  return overlay;
}

const OPTIONS = {
  maxRows: MAX_QUERY_RESULT_ROWS,
  maxBytes: MAX_QUERY_RESULT_BYTES,
};

const engineOver = (overlay: OverlayEngine, capacityBytes = 1 << 30) =>
  new ColumnarQueryEngine(overlay, { capacityBytes });

const fields = (...names: string[]): QueryProjection[] =>
  names.map((name) => ({
    kind: "field",
    field: { source: "event", column: name },
    as: name,
  }));

const ordered = (
  orderBy: readonly QueryOrder[],
  extra: Partial<QuerySpecification> = {},
): QuerySpecification => ({
  version: 1,
  from: { collection: "events", as: "event" },
  select: fields(...COLUMNS),
  orderBy,
  ...extra,
});

/** One to three ordering columns, each direction and null placement. */
function randomOrder(random: () => number): QueryOrder[] {
  const columns = [...COLUMNS];
  return Array.from({ length: 1 + Math.floor(random() * 3) }, () => {
    const index = Math.floor(random() * columns.length);
    const [by] = columns.splice(index, 1);
    if (by === undefined) throw new Error("no column left to order by");
    return {
      by,
      direction: random() < 0.5 ? ("asc" as const) : ("desc" as const),
      ...(random() < 0.3
        ? {}
        : { nulls: random() < 0.5 ? ("first" as const) : ("last" as const) }),
    };
  });
}

const LEVEL_FILTER: QueryPredicate = {
  kind: "compare",
  operator: "neq",
  left: { kind: "field", field: { source: "event", column: "level" } },
  right: { kind: "literal", value: "info" },
};

/** Rows `offset` to `offset + count` of the oracle's whole result. */
function expectWindow(
  engine: ColumnarQueryEngine,
  expected: QueryResult,
  specification: QuerySpecification,
  offset: number,
  count: number,
): void {
  const window = engine.window(specification, { offset, count }, OPTIONS);
  expect(window.total).toBe(expected.rows.length);
  expect(window.offset).toBe(Math.min(offset, expected.rows.length));
  expect(window.columns).toEqual(expected.columns);
  expect(window.rows).toEqual(expected.rows.slice(offset, offset + count));
}

describe("ordered windows", () => {
  test("a window holds the rows a full sort of every row puts there", async () => {
    const random = generator(0x51de);
    const orders: QueryOrder[][] = [
      ...COLUMNS.flatMap((by) =>
        (["asc", "desc"] as const).flatMap((direction) =>
          (["first", "last"] as const).map((nulls) => [
            { by, direction, nulls },
          ]),
        ),
      ),
      ...Array.from({ length: 16 }, () => randomOrder(random)),
    ];
    for (const state of STATES) {
      const overlay = await overlayIn(state);
      const engine = engineOver(overlay);
      for (const orderBy of orders)
        for (const extra of [{}, { where: LEVEL_FILTER }]) {
          const specification = ordered(orderBy, extra);
          const expected = referenceQuery(overlay, SCHEMAS, specification);
          const total = expected.rows.length;
          const middle = Math.floor(total / 2);
          for (const [offset, count] of [
            [0, 25],
            [middle, 7],
            [Math.max(0, total - 5), 25],
            [total, 3],
            [total + 10, 1],
            [middle, 0],
          ] as const)
            expectWindow(engine, expected, specification, offset, count);
        }
    }
  }, 120_000);

  test("a query cut by a limit keeps the rows a full sort puts first", async () => {
    const random = generator(0x7091);
    for (const state of STATES) {
      const overlay = await overlayIn(state);
      const engine = engineOver(overlay);
      for (let draw = 0; draw < 12; draw++) {
        const orderBy = randomOrder(random);
        // Through the kept-best scan and the full sort on either side of
        // where one hands over to the other.
        for (const limit of [1, 5, 64, 65, 1_000, MIXED_ROWS + 5]) {
          const specification = ordered(orderBy, { limit });
          expect(engine.query(specification, OPTIONS)).toEqual(
            referenceQuery(overlay, SCHEMAS, specification),
          );
          const expected = referenceQuery(overlay, SCHEMAS, specification);
          expectWindow(engine, expected, specification, 3, 50);
        }
      }
    }
  }, 120_000);

  test("orders grouped rows and windows over them", async () => {
    const overlay = await overlayIn("dirty");
    const engine = engineOver(overlay);
    const specification: QuerySpecification = {
      version: 1,
      from: { collection: "events", as: "event" },
      select: [
        ...fields("level", "glyph"),
        { kind: "aggregate", operator: "count", as: "count" },
      ],
      groupBy: [
        { source: "event", column: "level" },
        { source: "event", column: "glyph" },
      ],
      orderBy: [
        { by: "count", direction: "desc" },
        { by: "glyph", direction: "asc", nulls: "first" },
      ],
    };
    const expected = referenceQuery(overlay, SCHEMAS, specification);
    expect(engine.query(specification, OPTIONS)).toEqual(expected);
    for (const offset of [0, 5, expected.rows.length - 2])
      expectWindow(engine, expected, specification, offset, 4);
  });

  test("a code order that differs from text order is read as values", async () => {
    const encoded = encodeArtifact(
      SCHEMA,
      new Map([["events", fixtureRows(64, 0x6c7)]]),
    );
    const workspace = await openDirectColumnarArtifact(
      encoded.bytes.slice().buffer,
      {
        byteLength: encoded.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema: SCHEMA,
      },
    );
    const table = workspace.table("events");
    const cells = (name: string) =>
      artifactQueryVector(table.column(name))?.encoded;
    const level = cells("level");
    const glyph = cells("glyph");
    expect(level?.kind === "dict" && level.codesOrderAsText()).toBe(true);
    expect(glyph?.kind === "dict" && glyph.codesAreText()).toBe(true);
    expect(glyph?.kind === "dict" && glyph.codesOrderAsText()).toBe(false);
  });

  test("a window's total is not capped at the rows one call may return", async () => {
    const rows = MAX_QUERY_RESULT_ROWS + 2_345;
    const overlay = await overlayOf(fixtureRows(rows, 0xb16));
    const engine = engineOver(overlay);
    const specification = ordered([
      { by: "occurred", direction: "desc" },
      { by: "id", direction: "desc" },
    ]);
    const expected = referenceQuery(overlay, SCHEMAS, specification);
    expect(expected.rows).toHaveLength(rows);
    for (const offset of [0, 9_990, rows - 17, rows - 1])
      expectWindow(engine, expected, specification, offset, 40);
    expectWindow(engine, expected, specification, 1, MAX_QUERY_RESULT_ROWS);
    // A whole result that large is refused, before it is sorted.
    expect(() => engine.query(specification, OPTIONS)).toThrow(
      /RESULT_TOO_LARGE|result has 12345 rows/u,
    );
  }, 60_000);

  test("reads a kept result while its collection stays where it was, and recomputes after it moves", async () => {
    const overlay = await overlayIn("clean");
    const engine = engineOver(overlay);
    const specification = ordered([{ by: "amount", direction: "asc" }], {
      where: LEVEL_FILTER,
    });
    engine.window(specification, { offset: 0, count: 10 });
    expect(inspectQueryExecution(engine).rowsExamined).toBeGreaterThan(0);
    const again = engine.window(specification, { offset: 100, count: 10 });
    expect(inspectQueryExecution(engine).rowsExamined).toBe(0);
    expect(again.rows).toEqual(
      referenceQuery(overlay, SCHEMAS, specification).rows.slice(100, 110),
    );
    applyCanonical(overlay, [
      eventUpdate(0, { level: "debug", amount: -(2n ** 63n) }),
    ]);
    const moved = engine.window(specification, { offset: 0, count: 10 });
    expect(inspectQueryExecution(engine).rowsExamined).toBeGreaterThan(0);
    expect(moved.rows[0]?.[0]).toBe(0n);
    expect(moved.rows).toEqual(
      referenceQuery(overlay, SCHEMAS, specification).rows.slice(0, 10),
    );
  });

  test("refuses a window outside its bounds", async () => {
    const engine = engineOver(await overlayIn("clean"));
    const specification = ordered([{ by: "id", direction: "asc" }]);
    for (const window of [
      { offset: -1, count: 1 },
      { offset: 0.5, count: 1 },
      { offset: 0, count: MAX_QUERY_RESULT_ROWS + 1 },
      { offset: 0, count: -1 },
    ])
      expect(() => engine.window(specification, window)).toThrow(
        /INVALID_QUERY|window/u,
      );
  });

  test("a window at or past the end of the result is empty and starts at the end", async () => {
    const engine = engineOver(await overlayIn("clean"));
    const specification = ordered([{ by: "id", direction: "asc" }]);
    const { total } = engine.window(specification, { offset: 0, count: 1 });
    expect(total).toBeGreaterThan(0);
    for (const offset of [total, total + 1, total + 10_000])
      expect(engine.window(specification, { offset, count: 5 })).toMatchObject({
        offset: total,
        total,
        rows: [],
      });
  });
});

describe("query tasks", () => {
  const LARGE_ROWS = 24_000;
  // NULL levels too, which `neq` alone leaves out, so that a level set to NULL
  // moves its row to the front.
  const specification = ordered(
    [
      { by: "level", direction: "asc", nulls: "first" },
      { by: "occurred", direction: "desc" },
    ],
    {
      where: {
        kind: "or",
        predicates: [
          LEVEL_FILTER,
          {
            kind: "is-null",
            expression: {
              kind: "field",
              field: { source: "event", column: "level" },
            },
          },
        ],
      },
    },
  );

  /** Steps until done, pausing at every slice; returns the steps it took. */
  function drive<Result>(
    task: QueryTask<Result>,
    between: (step: number) => void = () => undefined,
  ): { steps: number; completion: QueryCompletion<Result> } {
    for (let steps = 1; ; steps++) {
      const completion = task.step(() => true);
      if (completion) return { steps, completion };
      between(steps);
    }
  }

  test("a task paused at every slice returns what one uninterrupted run returns", async () => {
    const overlay = await overlayOf(fixtureRows(LARGE_ROWS, 0x5a1));
    const engine = engineOver(overlay);
    const window = { offset: 12_000, count: 30 };
    const whole = engine.window(specification, window);
    const sliced = engineOver(overlay);
    const { steps, completion } = drive(
      sliced.startWindow(specification, window),
    );
    expect(steps).toBeGreaterThan(20);
    expect(completion.result).toEqual(whole);
    expect(completion.generations).toEqual({
      events: overlay.generation("events"),
    });
  }, 60_000);

  test("a task restarts at the state a collection moved to between its steps", async () => {
    const overlay = await overlayOf(fixtureRows(LARGE_ROWS, 0x5a1));
    const engine = engineOver(overlay);
    const before = referenceQuery(overlay, SCHEMAS, specification);
    const window = { offset: 0, count: 5 };
    const task = engine.startWindow(specification, window);
    const { completion } = drive(task, (step) => {
      if (step === 3)
        applyCanonical(overlay, [
          // A null level, which sorts first, and a timestamp after every
          // other.
          eventUpdate(LARGE_ROWS - 1, {
            level: null,
            occurred: "2300-01-01T00:00:00.000Z",
          }),
        ]);
    });
    const expected = referenceQuery(overlay, SCHEMAS, specification);
    expect(expected.rows.slice(0, 5)).not.toEqual(before.rows.slice(0, 5));
    expect(expected.rows[0]?.[0]).toBe(BigInt(LARGE_ROWS - 1));
    expect(completion.result.rows).toEqual(expected.rows.slice(0, 5));
    expect(completion.generations).toEqual({
      events: overlay.generation("events"),
    });
  }, 60_000);

  test("after its last restart a task finishes in the step that begins the next attempt", async () => {
    const overlay = await overlayOf(fixtureRows(LARGE_ROWS, 0x5a1));
    const engine = engineOver(overlay);
    const task = engine.startWindow(specification, { offset: 0, count: 5 });
    let moves = 0;
    const { steps, completion } = drive(task, () => {
      moves++;
      applyCanonical(overlay, [eventUpdate(moves, { amount: BigInt(moves) })]);
    });
    expect(moves).toBe(MAX_QUERY_RESTARTS);
    expect(steps).toBe(MAX_QUERY_RESTARTS + 1);
    expect(completion.result.rows).toEqual(
      referenceQuery(overlay, SCHEMAS, specification).rows.slice(0, 5),
    );
  }, 60_000);

  test("cancelling a task releases its reservation", async () => {
    const overlay = await overlayOf(fixtureRows(LARGE_ROWS, 0x5a1));
    const required = engineOver(overlay).estimateReservationBytes(
      specification,
      { maxRows: 5 },
    );
    // One execution's reservation fits, two do not.
    const engine = engineOver(overlay, required + (required >> 1));
    const first = engine.startWindow(specification, { offset: 0, count: 5 });
    expect(first.step(() => true)).toBeUndefined();
    const second = engine.startWindow(specification, { offset: 0, count: 5 });
    expect(() => second.step(() => true)).toThrow(/pool is/u);
    first.cancel();
    expect(() => first.step(() => true)).toThrow(/ended/u);
    const third = engine.startWindow(specification, { offset: 0, count: 5 });
    expect(drive(third).completion.result.rows).toEqual(
      referenceQuery(overlay, SCHEMAS, specification).rows.slice(0, 5),
    );
  }, 60_000);

  test("a kept result gives way to an execution the pool cannot otherwise fit", async () => {
    const overlay = await overlayOf(fixtureRows(LARGE_ROWS, 0x5a1));
    const required = engineOver(overlay).estimateReservationBytes(
      specification,
      { maxRows: 5 },
    );
    const engine = new ColumnarQueryEngine(overlay, {
      capacityBytes: required,
      maxReservations: 1,
    });
    const reversed = ordered(
      [
        { by: "level", direction: "desc", nulls: "last" },
        { by: "occurred", direction: "asc" },
      ],
      { where: LEVEL_FILTER },
    );
    engine.window(specification, { offset: 0, count: 5 });
    engine.window(specification, { offset: 5, count: 5 });
    expect(inspectQueryExecution(engine).rowsExamined).toBe(0);
    expect(engine.window(reversed, { offset: 0, count: 5 }).rows).toEqual(
      referenceQuery(overlay, SCHEMAS, reversed).rows.slice(0, 5),
    );
    engine.window(specification, { offset: 0, count: 5 });
    expect(inspectQueryExecution(engine).rowsExamined).toBeGreaterThan(0);
  }, 60_000);

  test("a result computed while an older one is still being read replaces it", async () => {
    const overlay = await overlayOf(fixtureRows(LARGE_ROWS, 0x5a1));
    const engine = new ColumnarQueryEngine(overlay, {
      capacityBytes: 1 << 30,
      maxReservations: 2,
    });
    const wide = { offset: 0, count: MAX_QUERY_RESULT_ROWS };
    engine.window(specification, wide);
    // A task reading the kept result, paused partway through its rows.
    const reader = engine.startWindow(specification, wide);
    expect(reader.step(() => true)).toBeUndefined();
    applyCanonical(overlay, [eventUpdate(1, { amount: 1n })]);
    engine.window(specification, { offset: 0, count: 5 });
    reader.cancel();
    const next = engine.window(specification, { offset: 5, count: 5 });
    expect(inspectQueryExecution(engine).rowsExamined).toBe(0);
    expect(next.rows).toEqual(
      referenceQuery(overlay, SCHEMAS, specification).rows.slice(5, 10),
    );
    // The older result's reservation went with its reader: two executions
    // fit beside the kept result again.
    const orders: QueryOrder[][] = [
      [{ by: "occurred", direction: "asc" }],
      [{ by: "amount", direction: "desc" }],
    ];
    const running = orders.map((orderBy) =>
      engine.startWindow(ordered(orderBy), wide),
    );
    for (const task of running) expect(task.step(() => true)).toBeUndefined();
    for (const task of running) task.cancel();
  }, 60_000);

  test("a task that fails holds nothing and cannot step again", async () => {
    const overlay = await overlayOf(fixtureRows(LARGE_ROWS, 0x5a1));
    const engine = engineOver(overlay);
    const task = engine.start(ordered([{ by: "id", direction: "asc" }]), {
      maxRows: 10,
    });
    expect(() => task.step(() => false)).toThrow(
      "query result has 24000 rows, above maxRows (10); add .limit(n), pass { maxRows } up to 10000, or read it in pages with .window(offset, count)",
    );
    expect(() => task.step(() => false)).toThrow(/ended/u);
    expect(() =>
      engine.query(ordered([{ by: "id", direction: "asc" }], { limit: 3 })),
    ).not.toThrow();
  }, 60_000);
});
