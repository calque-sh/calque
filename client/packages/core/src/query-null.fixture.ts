/**
 * The rows, overlay changes, SQLite oracle and random predicates the NULL
 * predicate tests share, each drawn from a fixed seed.
 */
import { DatabaseSync } from "node:sqlite";

import { expect } from "vitest";

import {
  openDirectColumnarArtifact,
  type DirectColumnarSchema,
} from "./artifact.js";
import { encodeArtifact } from "./artifact.fixture.js";
import { OverlayEngine, artifactBase, type SqlValue } from "./overlay.js";
import {
  ColumnarQueryEngine,
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  configureQueryMeasurement,
  inspectQueryExecution,
  validateQuerySpecification,
  type QueryComparisonOperator,
  type QueryExpression,
  type QueryLiteral,
  type QueryPredicate,
  type QuerySpecification,
} from "./query.js";
import { referenceQuery } from "./query-reference.fixture.js";

export const COLUMNS = ["id", "a", "b", "s", "u"] as const;
export type Column = (typeof COLUMNS)[number];

export const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "t",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "a", type: "int64", nullable: true },
        { name: "b", type: "int64", nullable: true },
        { name: "s", type: "dict", nullable: true },
        { name: "u", type: "utf8", nullable: true },
      ],
    },
  ],
};

/**
 * Three 4,096-row statistics chunks and ten 1,024-row filter blocks. `a` rises
 * with `id`, so its chunks have disjoint ranges a range comparison can prune,
 * and is NULL in every row of the last chunk.
 */
export const ROWS = 10_000;
export const ALL_NULL_FROM = 8_192;

/**
 * ASCII only: SQLite orders text by UTF-8 bytes, JavaScript by UTF-16 units.
 */
export const TEXTS = ["", "a", "ab", "b", "B", "z"];
export const INTEGERS = [-3n, -1n, 0n, 1n, 2n, 3n, 5n, 8n, 9n, 12n];

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

export function baseRows(): SqlValue[][] {
  const random = generator(0x3e11);
  return Array.from({ length: ROWS }, (_unused, id) => [
    BigInt(id),
    id >= ALL_NULL_FROM || random() < 0.15
      ? null
      : BigInt(Math.floor(id / 1_000)),
    random() < 0.3 ? null : BigInt(Math.floor(random() * 5) - 2),
    random() < 0.25 ? null : pick(random, TEXTS),
    random() < 0.25 ? null : pick(random, TEXTS),
  ]);
}

export const OPTIONS = {
  maxRows: MAX_QUERY_RESULT_ROWS,
  maxBytes: MAX_QUERY_RESULT_BYTES,
};

export const primaryKey = (id: number) => ({
  collection: "t",
  primaryKey: new TextEncoder().encode(String(id)),
  key: [BigInt(id)],
});

/**
 * Overlay changes and the rows they leave: cells set to NULL and from NULL on
 * either side of a chunk boundary, one in the all-NULL chunk, a text the
 * dictionary does not hold, deletes, and inserted rows with NULLs.
 */
export const UPDATES: readonly (readonly [
  number,
  Partial<Record<Column, SqlValue>>,
])[] = [
  [5, { a: null, s: null }],
  [6, { a: 3n, b: null }],
  [7, { s: "not-in-the-dictionary", u: null }],
  [8, { b: 2n, u: "zz" }],
  [4_100, { a: null, u: "a" }],
  [9_000, { a: 4n, s: "b" }],
];
export const DELETES = [9, 5_000];
export const INSERTS: readonly SqlValue[][] = [
  [20_000n, null, 1n, "a", null],
  [20_001n, 2n, null, null, "b"],
];

export function changedRows(rows: readonly SqlValue[][]): SqlValue[][] {
  const changed = rows.map((row) => [...row]);
  for (const [id, values] of UPDATES)
    for (const [name, value] of Object.entries(values)) {
      const row = changed[id];
      if (!row) throw new Error(`no row ${String(id)}`);
      row[COLUMNS.indexOf(name as Column)] = value ?? null;
    }
  return [
    ...changed.filter((_row, id) => !DELETES.includes(id)),
    ...INSERTS.map((row) => [...row]),
  ];
}

export function applyChanges(overlay: OverlayEngine): void {
  overlay.applyCanonical({
    transactions: [
      {
        mutationIds: [],
        operations: [
          ...UPDATES.map(([id, values]) => ({
            kind: "update" as const,
            ...primaryKey(id),
            changed: Object.entries(values).map(([name, value]) => ({
              column: COLUMNS.indexOf(name as Column),
              value: value ?? null,
            })),
          })),
          ...DELETES.map((id) => ({
            kind: "delete" as const,
            ...primaryKey(id),
          })),
          ...INSERTS.map((values) => ({
            kind: "insert" as const,
            ...primaryKey(Number(values[0])),
            values,
          })),
        ],
      },
    ],
  });
}

export async function artifactOverlay(
  rows: readonly SqlValue[][],
): Promise<OverlayEngine> {
  const encoded = encodeArtifact(
    SCHEMA,
    new Map([
      [
        "t",
        rows.map((row) =>
          Object.fromEntries(COLUMNS.map((name, index) => [name, row[index]])),
        ),
      ],
    ]),
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
  return new OverlayEngine([
    {
      name: "t",
      columns: [...COLUMNS],
      base: artifactBase(workspace.table("t"), [...COLUMNS]),
      primaryKey: ["id"],
    },
  ]);
}

/** A base with no encoded vectors: every leaf is decided materialized. */
export function plainOverlay(rows: readonly SqlValue[][]): OverlayEngine {
  return new OverlayEngine([
    {
      name: "t",
      columns: [...COLUMNS],
      base: {
        rows: rows.length,
        lookupKey: () => -1,
        valueAt: (column, row) => rows[row]?.[column] ?? null,
      },
      primaryKey: ["id"],
    },
  ]);
}

export function engineOver(
  overlay: OverlayEngine,
  features: { chunkPruning?: boolean; blockFilter?: boolean } = {},
): ColumnarQueryEngine {
  const engine = new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 30 });
  configureQueryMeasurement(engine, features);
  return engine;
}

export function sqliteOf(rows: readonly SqlValue[][]): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec(
    "CREATE TABLE t (id INTEGER NOT NULL, a INTEGER, b INTEGER, s TEXT, u TEXT)",
  );
  const insert = database.prepare("INSERT INTO t VALUES (?, ?, ?, ?, ?)");
  database.exec("BEGIN");
  for (const row of rows) insert.run(...(row as (null | bigint | string)[]));
  database.exec("COMMIT");
  return database;
}

export const SQL_OPERATORS: Readonly<Record<QueryComparisonOperator, string>> =
  {
    eq: "=",
    neq: "<>",
    lt: "<",
    lte: "<=",
    gt: ">",
    gte: ">=",
  };

/** The predicate as SQLite text, its literals bound in order. */
export function sql(
  predicate: QueryPredicate,
  parameters: (bigint | string)[],
): string {
  const operand = (expression: QueryExpression) => {
    if (expression.kind === "field") return `"${expression.field.column}"`;
    if (
      typeof expression.value !== "bigint" &&
      typeof expression.value !== "string"
    )
      throw new Error("the differential binds integers and texts only");
    parameters.push(expression.value);
    return "?";
  };
  switch (predicate.kind) {
    case "compare":
      return `(${operand(predicate.left)} ${SQL_OPERATORS[predicate.operator]} ${operand(predicate.right)})`;
    case "is-null":
      return `(${operand(predicate.expression)} IS ${predicate.negated === true ? "NOT " : ""}NULL)`;
    case "not":
      return `(NOT ${sql(predicate.predicate, parameters)})`;
    default:
      return `(${predicate.predicates
        .map((child) => sql(child, parameters))
        .join(predicate.kind === "and" ? " AND " : " OR ")})`;
  }
}

export function sqliteIds(
  database: DatabaseSync,
  where: QueryPredicate,
): number[] {
  const parameters: (bigint | string)[] = [];
  const text = sql(where, parameters);
  return database
    .prepare(`SELECT id FROM t WHERE ${text} ORDER BY id`)
    .all(...parameters)
    .map((row) => Number(row.id));
}

export const field = (column: Column): QueryExpression => ({
  kind: "field",
  field: { source: "t", column },
});
export const literal = (value: QueryLiteral): QueryExpression => ({
  kind: "literal",
  value,
});
export const OPERATORS = ["eq", "neq", "lt", "lte", "gt", "gte"] as const;
export const INTEGER_COLUMNS: readonly Column[] = ["id", "a", "b"];
export const TEXT_COLUMNS: readonly Column[] = ["s", "u"];

export function randomLeaf(random: () => number): QueryPredicate {
  const integer = random() < 0.55;
  const columns = integer ? INTEGER_COLUMNS : TEXT_COLUMNS;
  const column = pick(random, columns);
  const draw = random();
  if (draw < 0.2)
    return {
      kind: "is-null",
      expression: field(column),
      negated: random() < 0.5,
    };
  const operator = pick(random, OPERATORS);
  if (draw < 0.4)
    return {
      kind: "compare",
      operator,
      left: field(column),
      right: field(pick(random, columns)),
    };
  const value = literal(
    integer ? pick(random, INTEGERS) : pick(random, [...TEXTS, "c", "zz"]),
  );
  return random() < 0.2
    ? { kind: "compare", operator, left: value, right: field(column) }
    : { kind: "compare", operator, left: field(column), right: value };
}

export function randomPredicate(
  random: () => number,
  depth: number,
): QueryPredicate {
  const draw = random();
  if (depth === 0 || draw < 0.3) return randomLeaf(random);
  if (draw < 0.5)
    return { kind: "not", predicate: randomPredicate(random, depth - 1) };
  return {
    kind: draw < 0.75 ? "and" : "or",
    predicates: Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
      randomPredicate(random, depth - 1),
    ),
  };
}

/** A predicate as text a failure message can show, bigints included. */
export function printable(predicate: QueryPredicate): string {
  return JSON.stringify(predicate, (_key, value: unknown) =>
    typeof value === "bigint" ? `${String(value)}n` : value,
  );
}

export function idsWhere(where: QueryPredicate): QuerySpecification {
  return {
    version: 1,
    from: { collection: "t", as: "t" },
    where,
    select: [{ kind: "field", field: { source: "t", column: "id" }, as: "id" }],
    orderBy: [{ by: "id", direction: "asc" }],
  };
}

export function engineIds(
  engine: ColumnarQueryEngine,
  where: QueryPredicate,
): number[] {
  return engine
    .query(idsWhere(where), OPTIONS)
    .rows.map((row) => Number(row[0]));
}

export const SCHEMAS = [{ name: "t", columns: [...COLUMNS] }];

/**
 * Every filter path and the reference over the clean and the changed
 * artifact select the rows SQLite's WHERE selects, for the random predicates
 * drawn from seeds `firstSeed` up to `endSeed`, and each path runs the way its
 * name says. The seeds are split across test files that vitest runs side by
 * side.
 */
export async function expectPathsSelectAsSqlite(
  firstSeed: number,
  endSeed: number,
): Promise<void> {
  const rows = baseRows();
  const clean = await artifactOverlay(rows);
  const dirty = await artifactOverlay(rows);
  applyChanges(dirty);
  const database = sqliteOf(rows);
  const changedDatabase = sqliteOf(changedRows(rows));
  const paths = [
    ["materialized", engineOver(plainOverlay(rows)), database],
    ["block, pruned", engineOver(clean), database],
    ["block", engineOver(clean, { chunkPruning: false }), database],
    ["row, pruned", engineOver(clean, { blockFilter: false }), database],
    [
      "row",
      engineOver(clean, { blockFilter: false, chunkPruning: false }),
      database,
    ],
    ["dirty block, pruned", engineOver(dirty), changedDatabase],
    [
      "dirty row, pruned",
      engineOver(dirty, { blockFilter: false }),
      changedDatabase,
    ],
  ] as const;
  const references = [
    ["reference", clean, database],
    ["dirty reference", dirty, changedDatabase],
  ] as const;
  // Per path, the counters of all its queries: each path must have run the
  // way its name says.
  const totals = new Map(
    paths.map(([name]) => [
      name,
      { encoded: 0, materialized: 0, chunksSkipped: 0 },
    ]),
  );
  const mismatches: unknown[] = [];
  for (let seed = firstSeed; seed < endSeed; seed++) {
    const where = randomPredicate(generator(seed), 4);
    const specification = idsWhere(where);
    validateQuerySpecification(specification);
    const expected = new Map(
      [database, changedDatabase].map((oracle) => [
        oracle,
        sqliteIds(oracle, where).join(","),
      ]),
    );
    const selected: (readonly [string, DatabaseSync, readonly unknown[]])[] = [
      ...paths.map(([name, engine, oracle]) => {
        const ids = engineIds(engine, where);
        const inspection = inspectQueryExecution(engine);
        const total = totals.get(name);
        if (total) {
          total.encoded += inspection.encodedComparisons;
          total.materialized += inspection.materializedComparisons;
          total.chunksSkipped += inspection.chunksSkipped;
        }
        return [name, oracle, ids] as const;
      }),
      ...references.map(
        ([name, overlay, oracle]) =>
          [
            name,
            oracle,
            referenceQuery(overlay, SCHEMAS, specification).rows.map((row) =>
              Number(row[0]),
            ),
          ] as const,
      ),
    ];
    for (const [name, oracle, ids] of selected)
      if (ids.join(",") !== expected.get(oracle))
        mismatches.push({ seed, name, where: printable(where) });
  }
  expect(mismatches).toEqual([]);
  for (const [name, total] of totals) {
    expect(total.encoded > 0, name).toBe(name !== "materialized");
    expect(total.materialized, name).toBeGreaterThan(0);
    expect(total.chunksSkipped > 0, name).toBe(name.endsWith("pruned"));
  }
}
