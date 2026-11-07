/**
 * The artifact, overlay states and random predicates the query scan tests
 * share: one artifact that holds every encoded kind the block filter, the
 * encoded grouping and the UUID join read, each drawn from a fixed seed.
 */
import { expect } from "vitest";

import {
  openDirectColumnarArtifact,
  directColumnarTimestampText,
  type DirectColumnarSchema,
  type DirectColumnarWorkspace,
} from "./artifact.js";
import { encodeArtifact, fixtureSchemaHash } from "./artifact.fixture.js";
import { OverlayEngine, artifactBase, type SqlValue } from "./overlay.js";
import {
  ColumnarQueryEngine,
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  configureQueryMeasurement,
  inspectQueryExecution,
  validateQuerySpecification,
  type QueryExpression,
  type QueryLiteral,
  type QueryPredicate,
  type QueryProjection,
  type QueryResult,
  type QuerySpecification,
} from "./query.js";
import {
  referenceQuery,
  type QueryReferenceSchema,
} from "./query-reference.fixture.js";

export type FixtureRow = Readonly<Record<string, unknown>>;

export const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "events",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "project", type: "uuid", nullable: true },
        { name: "level", type: "dict", nullable: true },
        { name: "tag", type: "dict", nullable: false },
        { name: "amount", type: "int64", nullable: true },
        { name: "occurred", type: "timestamp", nullable: true },
        { name: "note", type: "utf8", nullable: true },
        { name: "label", type: "utf8", nullable: false },
      ],
    },
    {
      name: "projects",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "uuid", nullable: false },
        { name: "owner", type: "uuid", nullable: true },
        { name: "name", type: "utf8", nullable: false },
      ],
    },
  ],
};

export const SCHEMAS: readonly QueryReferenceSchema[] = SCHEMA.tables.map(
  (table) => ({
    name: table.name,
    columns: table.columns.map((column) => column.name),
  }),
);

export const columnOf = (table: string, name: string): number =>
  SCHEMAS.find((schema) => schema.name === table)?.columns.indexOf(name) ?? -1;

/**
 * Two 4,096-row statistics chunks, five 1,024-row filter blocks, and every
 * case of the per-block value patterns below.
 */
export const EVENT_ROWS = 5_000;

/** mulberry32: a small generator whose sequence a seed fixes. */
export function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 2 ** 32;
  };
}

export function pick<T>(random: () => number, values: readonly T[]): T {
  const value = values[Math.floor(random() * values.length)];
  if (value === undefined) throw new Error("pick from an empty list");
  return value;
}

/**
 * Forty project UUIDs: most random, a run that differs only in its last byte,
 * and all zeros. Projects 37 to 39 have no project row, so an inner join
 * drops the events that name them.
 */
export function uuidPool(): readonly Uint8Array[] {
  const random = generator(0x5eed);
  return Array.from({ length: 40 }, (_unused, index) => {
    if (index === 0) return new Uint8Array(16);
    if (index < 8) {
      const shared = new Uint8Array(16).fill(0xa5);
      shared[15] = index;
      return shared;
    }
    return Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256));
  });
}

export const POOL = uuidPool();
export const uuid = (index: number): Uint8Array => {
  const value = POOL[index];
  if (!value) throw new Error(`no pool UUID ${String(index)}`);
  return Uint8Array.from(value);
};
export const PROJECT_ROWS = 37;

/**
 * A NUL byte, an empty text, a non-ASCII one, and U+FFFD, whose UTF-8 bytes a
 * lone surrogate literal also encodes to, among the levels.
 */
export const LEVELS = [
  "debug",
  "info",
  "warn\u0000ing",
  "érreur",
  "fatal",
  "",
  "\uFFFD",
];

/**
 * `\uFEFFx` decodes to `x`, as `\uFEFF` does to the empty text: the fatal
 * decoder drops a leading byte-order mark. Two codes then read as one value,
 * which is the case the dictionary's encoded group ids must not split.
 */
export const TAGS = ["x", "\uFEFFx", "y", "\uFEFF", "z", ""];

/**
 * Texts whose UTF-8 bytes stand for them: an empty one, a NUL, non-ASCII,
 * U+FFFD, and a byte-order mark that does not lead. The labels are the tags
 * as plain UTF-8, where a leading mark makes two rows' bytes differ while
 * their texts are equal, so that column has no encoded form to read.
 */
export const NOTES = [
  ...Array.from({ length: 17 }, (_unused, index) => `note-${String(index)}`),
  "",
  "a\u0000b",
  "é",
  "\uFFFD",
  "x\uFEFF",
];

export const MICROS_2026 = BigInt(Date.UTC(2026, 2, 1)) * 1_000n;
/** Outside the safe microsecond range on either side. */
export const YEAR_1500 = BigInt(Date.UTC(1500, 0, 1)) * 1_000n;
export const YEAR_2300 = BigInt(Date.UTC(2300, 0, 1)) * 1_000n;

/**
 * Per 1,024-row block, one encoding case: small integers; values near the
 * safe limit whose sums leave it; a block spanning 2^63, stored raw; and
 * negatives near the limit.
 */
export function amountAt(random: () => number, id: number): bigint | null {
  if (random() < 0.06) return null;
  const small = BigInt(Math.floor(random() * 101) - 50);
  switch (Math.floor(id / 1_024) % 4) {
    case 0:
      return small;
    case 1:
      return BigInt(Number.MAX_SAFE_INTEGER) - BigInt(Math.floor(random() * 5));
    case 2:
      return random() < 0.05 ? pick(random, [2n ** 62n, -(2n ** 62n)]) : small;
    default:
      return -BigInt(Number.MAX_SAFE_INTEGER) + small;
  }
}

/**
 * Per 1,024-row block: whole milliseconds, which the encoder scales; one
 * millisecond at many precisions, where the text orders a whole millisecond
 * after its fractions; and years outside the safe range among 2026 values.
 */
export function occurredAt(random: () => number, id: number): bigint | null {
  if (random() < 0.05) return null;
  const millis = BigInt(Math.floor(random() * 50)) * 1_000n;
  switch (Math.floor(id / 1_024) % 3) {
    case 0:
      return MICROS_2026 + millis * 1_000n;
    case 1:
      return (
        MICROS_2026 +
        millis +
        pick(random, [0n, 1n, 500n, 998n, 999n, 1_000n, 1_001n])
      );
    default:
      return random() < 0.1
        ? pick(random, [YEAR_1500, YEAR_1500 + 1n, YEAR_2300, YEAR_2300 - 1n])
        : MICROS_2026 + millis + BigInt(Math.floor(random() * 3));
  }
}

export function eventRow(random: () => number, id: number): FixtureRow {
  const draw = random();
  return {
    id: BigInt(id),
    project: draw < 0.08 ? null : uuid(Math.floor(random() * POOL.length)),
    level: random() < 0.07 ? null : pick(random, LEVELS),
    tag: pick(random, TAGS),
    amount: amountAt(random, id),
    occurred: occurredAt(random, id),
    note: random() < 0.1 ? null : (NOTES[id % NOTES.length] ?? null),
    label: TAGS[id % TAGS.length] ?? "",
  };
}

export function projectRow(index: number): FixtureRow {
  return {
    id: uuid(index),
    owner: index % 4 === 0 ? null : uuid((index * 7) % POOL.length),
    // A name equal to a level, for a dictionary-to-text join.
    name: index === 3 ? "debug" : `project-${String(index)}`,
  };
}

export async function workspaceOf(
  schema: DirectColumnarSchema,
  tables: ReadonlyMap<string, readonly FixtureRow[]>,
): Promise<DirectColumnarWorkspace> {
  const encoded = encodeArtifact(schema, tables);
  expect(encoded.identity.schemaHash).toBe(fixtureSchemaHash(schema));
  return openDirectColumnarArtifact(encoded.bytes.slice().buffer, {
    byteLength: encoded.byteLength,
    digest: encoded.digest,
    identity: encoded.identity,
    schema,
  });
}

export async function overlayOf(
  schema: DirectColumnarSchema,
  tables: ReadonlyMap<string, readonly FixtureRow[]>,
): Promise<OverlayEngine> {
  const workspace = await workspaceOf(schema, tables);
  return new OverlayEngine(
    schema.tables.map(({ name, columns }) => {
      const names = columns.map((column) => column.name);
      return {
        name,
        columns: names,
        base: artifactBase(workspace.table(name), names),
      };
    }),
  );
}

export function baseTables(): ReadonlyMap<string, readonly FixtureRow[]> {
  const random = generator(0xc0ffee);
  return new Map<string, readonly FixtureRow[]>([
    [
      "events",
      Array.from({ length: EVENT_ROWS }, (_unused, id) => eventRow(random, id)),
    ],
    [
      "projects",
      Array.from({ length: PROJECT_ROWS }, (_unused, index) =>
        projectRow(index),
      ),
    ],
  ]);
}

export const baseOverlay = (): Promise<OverlayEngine> =>
  overlayOf(SCHEMA, baseTables());

export const eventKey = (id: number) => ({
  collection: "events",
  primaryKey: new TextEncoder().encode(String(id)),
  key: [BigInt(id)],
});
export const projectKey = (id: Uint8Array) => ({
  collection: "projects",
  primaryKey: Uint8Array.from(id),
  key: [Uint8Array.from(id)],
});

export function eventValues(
  id: number,
  values: Record<string, SqlValue>,
): SqlValue[] {
  const row: Record<string, SqlValue> = {
    id: BigInt(id),
    project: null,
    level: null,
    tag: "x",
    amount: null,
    occurred: null,
    note: null,
    label: "x",
    ...values,
  };
  return (SCHEMAS[0]?.columns ?? []).map((name) => row[name] ?? null);
}

export function eventUpdate(id: number, values: Record<string, SqlValue>) {
  return {
    kind: "update" as const,
    ...eventKey(id),
    changed: Object.entries(values).map(([name, value]) => ({
      column: columnOf("events", name),
      value,
    })),
  };
}

export function eventPatch(id: number, values: Record<string, SqlValue>) {
  return {
    kind: "patch" as const,
    ...eventKey(id),
    changes: Object.entries(values).map(([name, value]) => ({
      column: columnOf("events", name),
      value,
      mode: "set" as const,
    })),
  };
}

/**
 * Canonical and pending state whose values keep every column's kind, so sums
 * stay bigint: changed cells of each encoded column on base rows, including
 * values the dictionary does not hold and texts the base cannot produce;
 * deletes; inserts; and a project that appears for a dangling event UUID.
 */
export function applyDirtyState(overlay: OverlayEngine): void {
  overlay.applyCanonical({
    transactions: [
      {
        mutationIds: [],
        operations: [
          eventUpdate(1, { project: uuid(3) }),
          eventUpdate(2, { project: null }),
          eventUpdate(3, { project: uuid(38), tag: "\uFEFFx" }),
          eventUpdate(4, { amount: 5n }),
          eventUpdate(5, { amount: 2n ** 62n, occurred: null }),
          eventUpdate(6, { occurred: "2026-03-01T00:00:00.000001Z" }),
          eventUpdate(7, { occurred: "2026-03-01T00:00:49Z" }),
          eventUpdate(1_500, { tag: "x" }),
          eventUpdate(2_100, { occurred: "2400-01-01T00:00:00.000Z" }),
          eventUpdate(3_500, { amount: -(2n ** 53n), project: uuid(0) }),
          // A lone surrogate encodes as U+FFFD's bytes, and a leading mark's
          // bytes are no clean row's; neither text is one a clean row reads as.
          eventUpdate(20, { note: "\uD800", label: "\uFEFFx" }),
          eventUpdate(21, { note: "\uFFFD", label: "x" }),
          eventUpdate(22, { note: "\uFEFFnote-3" }),
          eventUpdate(23, { note: "note-3", amount: 2n ** 60n }),
          { kind: "delete", ...eventKey(9) },
          { kind: "delete", ...eventKey(4_100) },
          {
            kind: "insert",
            ...eventKey(100_000),
            values: eventValues(100_000, {
              project: uuid(39),
              level: "brand-new",
              amount: 12n,
              occurred: "1999-12-31T23:59:59.999Z",
              note: "inserted",
            }),
          },
          {
            kind: "insert",
            ...projectKey(uuid(38)),
            values: [uuid(38), uuid(1), "late-project"],
          },
          { kind: "delete", ...projectKey(uuid(5)) },
          {
            kind: "update",
            ...projectKey(uuid(6)),
            changed: [{ column: columnOf("projects", "owner"), value: null }],
          },
        ],
      },
    ],
  });
  // Level cells change only here, in pending patches, so a column whose base
  // cells only pending patches changed is asked about them; the odd state
  // adds a canonical level.
  overlay.applyOptimistic({
    mutationId: "query-scan-pending",
    actions: [
      eventPatch(1, { level: "info" }),
      eventPatch(2, { level: "never-in-dictionary" }),
      eventPatch(4, { level: null }),
      eventPatch(1_500, { level: "warn\u0000ing" }),
      eventPatch(10, { level: "fatal", project: uuid(2) }),
      eventPatch(11, { amount: BigInt(Number.MAX_SAFE_INTEGER) }),
      eventPatch(4_999, { occurred: "2026-03-01T00:00:00Z", tag: "y" }),
      { kind: "delete", ...eventKey(12) },
      {
        kind: "insert",
        ...eventKey(100_001),
        values: eventValues(100_001, {
          project: uuid(3),
          level: "debug",
          tag: "",
          amount: -7n,
          occurred: "2026-03-01T00:00:00.000999Z",
        }),
      },
    ],
  });
}

/**
 * Values no encoded cell can stand for: a 15-byte and a text project, a
 * numeric level, and a project owner that is not 16 bytes, which the UUID
 * join index cannot hold, with an event project equal to it, which only the
 * `valueKey` index then matches.
 */
export function applyOddState(overlay: OverlayEngine): void {
  applyDirtyState(overlay);
  overlay.applyCanonical({
    transactions: [
      {
        mutationIds: [],
        operations: [
          eventUpdate(13, { project: new Uint8Array(15) }),
          eventUpdate(14, { project: "not-a-uuid", level: 7 }),
          eventUpdate(15, { tag: "\uFEFF", note: 7, label: 7 }),
          eventUpdate(16, { project: Uint8Array.of(1, 2, 3) }),
          {
            kind: "update",
            ...projectKey(uuid(7)),
            changed: [
              {
                column: columnOf("projects", "owner"),
                value: Uint8Array.of(1, 2, 3),
              },
            ],
          },
        ],
      },
    ],
  });
}

/** A truncated collection has no encoded base cells to read. */
export function applyTruncatedState(overlay: OverlayEngine): void {
  overlay.applyCanonical({
    transactions: [
      {
        mutationIds: [],
        operations: [
          { kind: "truncate", collection: "events" },
          {
            kind: "insert",
            ...eventKey(1),
            values: eventValues(1, {
              project: uuid(1),
              level: "info",
              amount: 3n,
              occurred: "2026-03-01T00:00:00.001Z",
            }),
          },
          {
            kind: "insert",
            ...eventKey(2),
            values: eventValues(2, {
              project: uuid(1),
              level: "info",
              amount: 4n,
              occurred: "2026-03-01T00:00:00.001001Z",
            }),
          },
          {
            kind: "insert",
            ...eventKey(3),
            values: eventValues(3, { project: uuid(2), level: null }),
          },
        ],
      },
    ],
  });
}

export type OverlayState = "clean" | "dirty" | "odd" | "truncated";
export const STATES: readonly OverlayState[] = [
  "clean",
  "dirty",
  "odd",
  "truncated",
];

export async function overlayIn(state: OverlayState): Promise<OverlayEngine> {
  const overlay = await baseOverlay();
  if (state === "dirty") applyDirtyState(overlay);
  if (state === "odd") applyOddState(overlay);
  if (state === "truncated") applyTruncatedState(overlay);
  return overlay;
}

export const OPTIONS = {
  maxRows: MAX_QUERY_RESULT_ROWS,
  maxBytes: MAX_QUERY_RESULT_BYTES,
};

export function engineOver(
  overlay: OverlayEngine,
  features: { chunkPruning?: boolean; blockFilter?: boolean } = {},
): ColumnarQueryEngine {
  const engine = new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 30 });
  configureQueryMeasurement(engine, features);
  return engine;
}

export function expectReference(
  overlay: OverlayEngine,
  specification: QuerySpecification,
): QueryResult {
  const result = engineOver(overlay).query(specification, OPTIONS);
  expect(result).toEqual(referenceQuery(overlay, SCHEMAS, specification));
  return result;
}

export const field = (source: string, column: string): QueryExpression => ({
  kind: "field",
  field: { source, column },
});
export const literal = (value: QueryLiteral): QueryExpression => ({
  kind: "literal",
  value,
});
export const OPERATORS = ["eq", "neq", "lt", "lte", "gt", "gte"] as const;

/**
 * Literals per column: held values, absent ones, and other kinds. A null one
 * reaches a column's encoded comparison only, as the NULL test it stands for
 * there; a comparison refuses it.
 */
export const LITERALS: Readonly<Record<string, readonly QueryLiteral[]>> = {
  id: [0n, 17n, 4_096n, 4_999n, 100_000n, 17],
  project: [uuid(1), uuid(0), uuid(39), new Uint8Array(15), "x", null],
  level: [...LEVELS, "never-in-dictionary", "\uD800", null, 3],
  tag: [...TAGS, "\uFEFF\uFEFFx"],
  amount: [0n, 5n, -3n, BigInt(Number.MAX_SAFE_INTEGER), 2n ** 62n, 7, null],
  occurred: [
    "2026-03-01T00:00:00Z",
    "2026-03-01T00:00:00.000Z",
    "2026-03-01T00:00:01.000Z",
    "2026-03-01T00:00:00.000999Z",
    "2026-03-01T00:00:00.001Z",
    directColumnarTimestampText(YEAR_2300),
    null,
  ],
  note: [
    "note-3",
    "",
    "a\u0000b",
    "\uFFFD",
    "\uD800",
    "\uFEFFnote-3",
    "x\uFEFF",
    "never-a-note",
    null,
    3,
  ],
  label: [...TAGS, null],
};

export const comparable = (column: string): readonly QueryLiteral[] =>
  (LITERALS[column] ?? []).filter((value) => value !== null);

export function literalFor(random: () => number, column: string): QueryLiteral {
  if (column === "project" && random() < 0.5)
    return uuid(Math.floor(random() * POOL.length));
  return pick(random, comparable(column));
}

/**
 * Every literal of every column, under the operators each kind's encoded
 * comparison accepts: the edge cases, whatever the seed draws.
 */
export const LITERAL_PREDICATES: readonly QueryPredicate[] = Object.keys(
  LITERALS,
).flatMap((column) =>
  comparable(column).flatMap((value) =>
    (["id", "amount", "occurred"].includes(column)
      ? OPERATORS
      : (["eq", "neq"] as const)
    ).map((operator): QueryPredicate => ({
      kind: "compare",
      operator,
      left: field("event", column),
      right: literal(value),
    })),
  ),
);

export const EVENT_COLUMNS = [
  "id",
  "project",
  "level",
  "tag",
  "amount",
  "occurred",
  "note",
  "label",
];

export function randomLeaf(random: () => number): QueryPredicate {
  const column = pick(random, EVENT_COLUMNS);
  const draw = random();
  if (draw < 0.15)
    return {
      kind: "is-null",
      expression: field("event", column),
      negated: random() < 0.5,
    };
  if (draw < 0.22)
    return {
      kind: "compare",
      operator: pick(random, OPERATORS),
      left: field("event", column),
      right: field("event", pick(random, EVENT_COLUMNS)),
    };
  const operator =
    column === "project" || column === "tag" || random() < 0.5
      ? pick(random, ["eq", "neq"] as const)
      : pick(random, OPERATORS);
  const compared = literal(literalFor(random, column));
  return random() < 0.2
    ? {
        kind: "compare",
        operator,
        left: compared,
        right: field("event", column),
      }
    : {
        kind: "compare",
        operator,
        left: field("event", column),
        right: compared,
      };
}

export function randomPredicate(
  random: () => number,
  depth: number,
): QueryPredicate {
  const draw = random();
  if (depth === 0 || draw < 0.3) return randomLeaf(random);
  if (draw < 0.4)
    return { kind: "not", predicate: randomPredicate(random, depth - 1) };
  return {
    kind: draw < 0.7 ? "and" : "or",
    predicates: Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
      randomPredicate(random, depth - 1),
    ),
  };
}

/**
 * The columns whose encoded comparisons accept each operator here, so that a
 * predicate built only from them is decided in blocks unless a literal of
 * another kind makes a leaf fall back. The tag dictionary and the label
 * column are left out: their entries do not read as distinct texts, so they
 * have no encoded comparison.
 */
export const ENCODED_OPERATORS: Readonly<
  Record<string, readonly (typeof OPERATORS)[number][]>
> = {
  id: OPERATORS,
  project: ["eq", "neq"],
  level: ["eq", "neq"],
  amount: OPERATORS,
  occurred: ["eq", "neq"],
  note: ["eq", "neq"],
};

export function randomEncodedLeaf(random: () => number): QueryPredicate {
  const column = pick(random, Object.keys(ENCODED_OPERATORS));
  if (random() < 0.2)
    return {
      kind: "is-null",
      expression: field("event", column),
      negated: random() < 0.5,
    };
  return {
    kind: "compare",
    operator: pick(random, ENCODED_OPERATORS[column] ?? []),
    left: field("event", column),
    right: literal(literalFor(random, column)),
  };
}

/** A predicate of encoded leaves, each `and` and `or` with two to four. */
export function randomEncodedPredicate(
  random: () => number,
  depth: number,
): QueryPredicate {
  const draw = random();
  if (depth === 0 || draw < 0.25) return randomEncodedLeaf(random);
  if (draw < 0.35)
    return {
      kind: "not",
      predicate: randomEncodedPredicate(random, depth - 1),
    };
  return {
    kind: draw < 0.65 ? "and" : "or",
    predicates: Array.from({ length: 2 + Math.floor(random() * 3) }, () =>
      randomEncodedPredicate(random, depth - 1),
    ),
  };
}

export const eventFields = (...names: string[]): QueryProjection[] =>
  names.map((name) => ({
    kind: "field",
    field: { source: "event", column: name },
    as: name,
  }));
export const countAll: QueryProjection = {
  kind: "aggregate",
  operator: "count",
  as: "count",
};

/** Every aggregate kind over the events' encoded columns. */
export const AGGREGATES: readonly QueryProjection[] = [
  countAll,
  ...(
    [
      ["count", "project"],
      ["count", "level"],
      ["count", "amount"],
      ["count", "occurred"],
      ["sum", "amount"],
      ["min", "amount"],
      ["max", "amount"],
      ["min", "occurred"],
      ["max", "occurred"],
      ["max", "level"],
      ["min", "project"],
    ] as const
  ).map(([operator, column]): QueryProjection => ({
    kind: "aggregate",
    operator,
    field: { source: "event", column },
    as: `${operator}_${column}`,
  })),
];

/**
 * The block filter against the row path and the naive oracle, for every
 * predicate over each overlay state in `states`, counted as each path runs.
 * Each state takes its own few seconds, so the states are split across test
 * files that vitest runs side by side.
 */
export async function expectBlocksDecideAsRows(
  states: readonly OverlayState[],
): Promise<void> {
  const random = generator(0xb10c);
  const predicates = [
    ...LITERAL_PREDICATES,
    ...Array.from({ length: 24 }, () => randomPredicate(random, 3)),
    ...Array.from({ length: 32 }, () => randomEncodedPredicate(random, 3)),
  ];
  for (const state of states) {
    // Chunk statistics are built once per base column, by whichever query
    // asks first, and counted there: each path gets a base of its own.
    const blockOverlay = await overlayIn(state);
    const rowOverlay = await overlayIn(state);
    const engines = [true, false].map((chunkPruning) => ({
      block: engineOver(blockOverlay, { chunkPruning }),
      row: engineOver(rowOverlay, { chunkPruning, blockFilter: false }),
    }));
    for (const where of predicates) {
      const rows: QuerySpecification = {
        version: 1,
        from: { collection: "events", as: "event" },
        where,
        select: eventFields("id"),
      };
      const count: QuerySpecification = { ...rows, select: [countAll] };
      validateQuerySpecification(rows);
      // The ids as one text: a deep comparison of thousands of rows per
      // query would cost more than the queries.
      const ids = (result: QueryResult) =>
        result.rows.map((row) => String(row[0])).join(",");
      const expected = referenceQuery(blockOverlay, SCHEMAS, rows);
      for (const { block, row } of engines) {
        expect(ids(block.query(rows, OPTIONS))).toBe(ids(expected));
        expect(ids(row.query(rows, OPTIONS))).toBe(ids(expected));
        expect(inspectQueryExecution(block)).toEqual(
          inspectQueryExecution(row),
        );
        expect(block.query(count, OPTIONS).rows).toEqual([
          [expected.rows.length],
        ]);
        expect(row.query(count, OPTIONS).rows).toEqual([
          [expected.rows.length],
        ]);
        expect(inspectQueryExecution(block)).toEqual(
          inspectQueryExecution(row),
        );
      }
    }
  }
}
