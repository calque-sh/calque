/**
 * Enum columns ordered as PostgreSQL orders them, in declaration order, on
 * every path a query takes: the materialized one over a plain base, the
 * encoded row and block paths over a dictionary and a UTF-8 column, with and
 * without chunk pruning, and the artifact under overlay changes whose cells
 * the encoded paths hand back. Declaration order here is the reverse of text
 * order for two of the three labels, so a text comparison anywhere fails.
 */
import { describe, expect, test } from "vitest";

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
  type ColumnarEnumLabels,
  type QueryComparisonOperator,
  type QueryExpression,
  type QueryLiteral,
  type QueryOrder,
  type QueryPredicate,
  type QuerySpecification,
} from "./query.js";

/** Text order is high, low, medium. */
const LABELS = ["low", "medium", "high"];
/** Texts outside the labels, which order after every label and by text. */
const UNLABELLED = ["alpha", "zeta"];
const COLUMNS = ["id", "level", "pace", "note"] as const;
type Column = (typeof COLUMNS)[number];

/**
 * `level` is a dictionary column and `pace` a UTF-8 one, both of the same
 * enum, as a domain over it would arrive; `note` is plain text holding the
 * same texts.
 */
const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "t",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "level", type: "dict", nullable: true },
        { name: "pace", type: "utf8", nullable: true },
        { name: "note", type: "dict", nullable: true },
      ],
    },
  ],
};

const ENUMS: readonly ColumnarEnumLabels[] = [
  { collection: "t", column: "level", labels: LABELS },
  { collection: "t", column: "pace", labels: LABELS },
];

/** Three statistics chunks of 4,096 rows and nine filter blocks. */
const ROWS = 9_000;
const VALUES: readonly (string | null)[] = [...LABELS, ...UNLABELLED, null];

function baseRows(): SqlValue[][] {
  return Array.from({ length: ROWS }, (_unused, id) => [
    BigInt(id),
    // The first chunk holds only "low" and nulls, so a text bound would
    // prune it from a range it belongs to.
    id < 4_096 ? (id % 3 === 0 ? null : "low") : (VALUES[id % 6] ?? null),
    VALUES[(id * 7 + 3) % 6] ?? null,
    VALUES[(id * 5 + 1) % 6] ?? null,
  ]);
}

const primaryKey = (id: number) => ({
  collection: "t",
  primaryKey: new TextEncoder().encode(String(id)),
  key: [BigInt(id)],
});

/**
 * Cells set to a text no dictionary entry holds, to labels and to NULL, on
 * either side of a chunk boundary, deletes, and inserted rows.
 */
const UPDATES: readonly (readonly [
  number,
  Partial<Record<Column, SqlValue>>,
])[] = [
  [3, { level: "omega", pace: "high" }],
  [4, { level: "high", pace: null }],
  [4_097, { level: null, pace: "omega" }],
  [8_000, { level: "medium", pace: "low" }],
];
const DELETES = [5, 6_001];
const INSERTS: readonly SqlValue[][] = [
  [20_000n, "beta", "medium", "beta"],
  [20_001n, "low", null, "low"],
];

function changedRows(rows: readonly SqlValue[][]): SqlValue[][] {
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

function applyChanges(overlay: OverlayEngine): void {
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

async function artifactOverlay(
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
function plainOverlay(rows: readonly SqlValue[][]): OverlayEngine {
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

function engineOver(
  overlay: OverlayEngine,
  features: { chunkPruning?: boolean; blockFilter?: boolean } = {},
): ColumnarQueryEngine {
  const engine = new ColumnarQueryEngine(
    overlay,
    { capacityBytes: 1 << 30 },
    ENUMS,
  );
  configureQueryMeasurement(engine, features);
  return engine;
}

const OPTIONS = {
  maxRows: MAX_QUERY_RESULT_ROWS,
  maxBytes: MAX_QUERY_RESULT_BYTES,
};

/** The oracle: PostgreSQL's order for an enum whose labels are `LABELS`. */
function enumOrder(left: string, right: string): number {
  const rank = (value: string) => {
    const index = LABELS.indexOf(value);
    return index < 0 ? LABELS.length : index;
  };
  const difference = rank(left) - rank(right);
  if (difference !== 0 || rank(left) < LABELS.length) return difference;
  return left < right ? -1 : left > right ? 1 : 0;
}

function holds(operator: QueryComparisonOperator, comparison: number): boolean {
  if (operator === "eq") return comparison === 0;
  if (operator === "neq") return comparison !== 0;
  if (operator === "lt") return comparison < 0;
  if (operator === "lte") return comparison <= 0;
  if (operator === "gt") return comparison > 0;
  return comparison >= 0;
}

const field = (column: Column): QueryExpression => ({
  kind: "field",
  field: { source: "t", column },
});
const literal = (value: QueryLiteral): QueryExpression => ({
  kind: "literal",
  value,
});
const compare = (
  operator: QueryComparisonOperator,
  left: QueryExpression,
  right: QueryExpression,
): QueryPredicate => ({ kind: "compare", operator, left, right });

/** Labels, texts outside them on either side, and the empty text. */
const LITERALS = [...LABELS, "alpha", "beta", "omega", "zz", ""];
const RANGES = ["lt", "lte", "gt", "gte"] as const;

type Row = readonly SqlValue[];
type Expected = (row: Row) => boolean;

/** Each range case: a predicate and the rows the oracle says it selects. */
function rangeCases(): (readonly [string, QueryPredicate, Expected])[] {
  const cell = (row: Row, column: Column) =>
    row[COLUMNS.indexOf(column)] ?? null;
  const ordered =
    (column: Column, operator: QueryComparisonOperator, value: string) =>
    (row: Row) => {
      const text = cell(row, column);
      return (
        typeof text === "string" && holds(operator, enumOrder(text, value))
      );
    };
  const cases: (readonly [string, QueryPredicate, Expected])[] = [];
  for (const column of ["level", "pace"] as const)
    for (const operator of RANGES)
      for (const value of LITERALS) {
        const expected = ordered(column, operator, value);
        cases.push([
          `${column} ${operator} ${JSON.stringify(value)}`,
          compare(operator, field(column), literal(value)),
          expected,
        ]);
        // The literal on the left, and the negation, which leaves NULL out.
        cases.push([
          `${JSON.stringify(value)} ${operator} ${column}`,
          compare(operator, literal(value), field(column)),
          ordered(
            column,
            ({ lt: "gt", lte: "gte", gt: "lt", gte: "lte" } as const)[operator],
            value,
          ),
        ]);
        cases.push([
          `not ${column} ${operator} ${JSON.stringify(value)}`,
          {
            kind: "not",
            predicate: compare(operator, field(column), literal(value)),
          },
          (row) => cell(row, column) !== null && !expected(row),
        ]);
      }
  cases.push([
    "level between low and medium",
    {
      kind: "and",
      predicates: [
        compare("gte", field("level"), literal("low")),
        compare("lte", field("level"), literal("medium")),
      ],
    },
    (row) => {
      const text = cell(row, "level");
      return text === "low" || text === "medium";
    },
  ]);
  cases.push([
    "level above medium or pace below medium, with a text leaf",
    {
      kind: "or",
      predicates: [
        compare("gt", field("level"), literal("medium")),
        {
          kind: "and",
          predicates: [
            compare("lt", field("pace"), literal("medium")),
            compare("eq", field("note"), literal("zeta")),
          ],
        },
      ],
    },
    (row) =>
      ordered("level", "gt", "medium")(row) ||
      (ordered("pace", "lt", "medium")(row) && cell(row, "note") === "zeta"),
  ]);
  cases.push([
    "level below pace, both of one enum",
    compare("lt", field("level"), field("pace")),
    (row) => {
      const left = cell(row, "level");
      const right = cell(row, "pace");
      return (
        typeof left === "string" &&
        typeof right === "string" &&
        enumOrder(left, right) < 0
      );
    },
  ]);
  cases.push([
    "a note, plain text, still ordered as text",
    compare("lt", field("note"), literal("low")),
    (row) => {
      const text = cell(row, "note");
      return typeof text === "string" && text < "low";
    },
  ]);
  cases.push([
    "level below an integer, which no text orders against",
    compare("lt", field("level"), literal(5n)),
    () => false,
  ]);
  return cases;
}

function idsWhere(where: QueryPredicate): QuerySpecification {
  return {
    version: 1,
    from: { collection: "t", as: "t" },
    where,
    select: [{ kind: "field", field: { source: "t", column: "id" }, as: "id" }],
    orderBy: [{ by: "id", direction: "asc" }],
  };
}

function engineIds(engine: ColumnarQueryEngine, where: QueryPredicate) {
  return engine
    .query(idsWhere(where), OPTIONS)
    .rows.map((row) => Number(row[0]));
}

function expectedIds(rows: readonly Row[], expected: Expected): number[] {
  return rows
    .filter(expected)
    .map((row) => Number(row[0]))
    .sort((left, right) => left - right);
}

async function paths() {
  const rows = baseRows();
  const changed = changedRows(rows);
  const clean = await artifactOverlay(rows);
  const dirty = await artifactOverlay(rows);
  applyChanges(dirty);
  return [
    ["materialized", engineOver(plainOverlay(rows)), rows],
    ["block, pruned", engineOver(clean), rows],
    ["block", engineOver(clean, { chunkPruning: false }), rows],
    ["row, pruned", engineOver(clean, { blockFilter: false }), rows],
    [
      "row",
      engineOver(clean, { blockFilter: false, chunkPruning: false }),
      rows,
    ],
    ["dirty block, pruned", engineOver(dirty), changed],
    ["dirty row", engineOver(dirty, { blockFilter: false }), changed],
  ] as const;
}

describe("an enum column", () => {
  test("selects by declaration order in every range, negated, between and against its own enum, on every filter path", async () => {
    for (const [name, engine, rows] of await paths()) {
      let encoded = 0;
      let materialized = 0;
      for (const [label, where, expected] of rangeCases()) {
        expect(engineIds(engine, where), `${name}: ${label}`).toEqual(
          expectedIds(rows, expected),
        );
        const inspection = inspectQueryExecution(engine);
        encoded += inspection.encodedComparisons;
        materialized += inspection.materializedComparisons;
      }
      if (name === "materialized") expect(encoded, name).toBe(0);
      else expect(encoded, name).toBeGreaterThan(materialized);
    }
  }, 60_000);

  test("sorts by declaration order, with nulls first or last and texts outside the labels after every label, on every path", async () => {
    for (const [name, engine, rows] of await paths())
      for (const column of ["level", "pace"] as const)
        for (const direction of ["asc", "desc"] as const)
          for (const nulls of ["first", "last"] as const)
            for (const limit of [undefined, 10]) {
              const index = COLUMNS.indexOf(column);
              const order: QueryOrder = { by: column, direction, nulls };
              const result = engine.query(
                {
                  version: 1,
                  from: { collection: "t", as: "t" },
                  select: [
                    {
                      kind: "field",
                      field: { source: "t", column },
                      as: column,
                    },
                    {
                      kind: "field",
                      field: { source: "t", column: "id" },
                      as: "id",
                    },
                  ],
                  orderBy: [order, { by: "id", direction: "asc" }],
                  ...(limit === undefined ? {} : { limit }),
                },
                OPTIONS,
              );
              const expected = [...rows]
                .sort((left, right) => {
                  const a = left[index] ?? null;
                  const b = right[index] ?? null;
                  if (a === null || b === null) {
                    if (a !== b)
                      return (a === null) === (nulls === "first") ? -1 : 1;
                  } else {
                    const comparison = enumOrder(String(a), String(b));
                    if (comparison !== 0)
                      return direction === "desc" ? -comparison : comparison;
                  }
                  return Number(left[0]) - Number(right[0]);
                })
                .slice(0, limit ?? rows.length)
                .map((row) => Number(row[0]));
              expect(
                result.rows.map((row) => Number(row[1])),
                `${name}: ${column} ${direction} nulls ${nulls} limit ${String(limit)}`,
              ).toEqual(expected);
            }
  }, 60_000);

  test("takes min and max by declaration order, and orders groups by a grouped enum and by an aggregate of one", async () => {
    for (const [name, engine, rows] of await paths()) {
      const extremes = engine.query(
        {
          version: 1,
          from: { collection: "t", as: "t" },
          select: (["level", "pace"] as const).flatMap((column) =>
            (["min", "max"] as const).map((operator) => ({
              kind: "aggregate" as const,
              operator,
              field: { source: "t", column },
              as: `${operator}_${column}`,
            })),
          ),
        },
        OPTIONS,
      );
      const sorted = (column: Column) =>
        rows
          .map((row) => row[COLUMNS.indexOf(column)] ?? null)
          .filter((value): value is string => typeof value === "string")
          .sort(enumOrder);
      expect(extremes.rows, name).toEqual([
        [
          sorted("level")[0],
          sorted("level").at(-1),
          sorted("pace")[0],
          sorted("pace").at(-1),
        ],
      ]);

      const grouped = engine.query(
        {
          version: 1,
          from: { collection: "t", as: "t" },
          select: [
            {
              kind: "field",
              field: { source: "t", column: "level" },
              as: "level",
            },
            { kind: "aggregate", operator: "count", as: "rows" },
          ],
          groupBy: [{ source: "t", column: "level" }],
          orderBy: [{ by: "level", direction: "asc", nulls: "last" }],
        },
        OPTIONS,
      );
      const levels = [
        ...new Set(sorted("level")),
        ...(rows.some((row) => row[1] === null) ? [null] : []),
      ];
      expect(
        grouped.rows.map((row) => row[0]),
        name,
      ).toEqual(levels);

      const byMax = engine.query(
        {
          version: 1,
          from: { collection: "t", as: "t" },
          select: [
            {
              kind: "field",
              field: { source: "t", column: "note" },
              as: "note",
            },
            {
              kind: "aggregate",
              operator: "max",
              field: { source: "t", column: "pace" },
              as: "pace",
            },
          ],
          groupBy: [{ source: "t", column: "note" }],
          orderBy: [
            { by: "pace", direction: "desc", nulls: "last" },
            { by: "note", direction: "asc", nulls: "last" },
          ],
        },
        OPTIONS,
      );
      const maxima = new Map<SqlValue, string | null>();
      for (const row of rows) {
        const note = row[3] ?? null;
        const pace = row[2] ?? null;
        const held = maxima.get(note) ?? null;
        if (!maxima.has(note)) maxima.set(note, null);
        if (
          typeof pace === "string" &&
          (held === null || enumOrder(pace, held) > 0)
        )
          maxima.set(note, pace);
      }
      const expected = [...maxima.entries()].sort(
        ([leftNote, left], [rightNote, right]) => {
          if (left === null || right === null) {
            if (left !== right) return left === null ? 1 : -1;
          } else {
            const comparison = enumOrder(right, left);
            if (comparison !== 0) return comparison;
          }
          if (leftNote === null || rightNote === null)
            return leftNote === rightNote ? 0 : leftNote === null ? 1 : -1;
          return String(leftNote) < String(rightNote) ? -1 : 1;
        },
      );
      expect(byMax.rows, name).toEqual(
        expected.map(([note, pace]) => [note, pace]),
      );
    }
  }, 60_000);

  test("refuses an order against a column of no enum, and labels that repeat", async () => {
    const engine = engineOver(plainOverlay(baseRows()));
    expect(() =>
      engine.query(
        idsWhere(compare("lt", field("level"), field("note"))),
        OPTIONS,
      ),
    ).toThrow(
      /orders only against a text or an enum column of the same labels/,
    );
    // Equality needs no order, so it still compares the texts.
    expect(
      engine.query(
        idsWhere(compare("eq", field("level"), field("note"))),
        OPTIONS,
      ).rows.length,
    ).toBeGreaterThan(0);
    const overlay = plainOverlay(baseRows());
    expect(
      () =>
        new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 20 }, [
          { collection: "t", column: "level", labels: ["low", "low"] },
        ]),
    ).toThrow(/must be distinct strings/);
    expect(
      () =>
        new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 20 }, [
          ...ENUMS,
          { collection: "t", column: "level", labels: LABELS },
        ]),
    ).toThrow(/are given twice/);
  });
});
