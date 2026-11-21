/**
 * `in-list` predicates, PostgreSQL's `column = ANY(values)`, on every path a
 * filter takes: the materialized one over a plain base, the encoded row and
 * block paths over each column kind the artifact encodes, with and without
 * chunk pruning, and the artifact under overlay changes whose cells the
 * encoded paths hand back.
 */
import { describe, expect, test, vi } from "vitest";

import {
  openDirectColumnarArtifact,
  type DirectColumnarSchema,
} from "./artifact.js";
import { encodeArtifact } from "./artifact.fixture.js";
import { OverlayEngine, artifactBase, type SqlValue } from "./overlay.js";
import {
  ColumnarQueryEngine,
  MAX_QUERY_LIST_BYTES,
  MAX_QUERY_LIST_VALUES,
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  MAX_QUERY_SPEC_BYTES,
  QueryError,
  canonicalQuerySpecification,
  configureQueryMeasurement,
  inspectQueryExecution,
  validateQuerySpecification,
  type ColumnarEnumLabels,
  type QueryLiteral,
  type QueryPredicate,
  type QuerySpecification,
} from "./query.js";
import { referenceQuery } from "./query-reference.fixture.js";

const COLUMNS = [
  "id",
  "tag",
  "note",
  "uid",
  "amount",
  "ratio",
  "flag",
  "level",
] as const;
type Column = (typeof COLUMNS)[number];

const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "t",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "tag", type: "dict", nullable: true },
        { name: "note", type: "utf8", nullable: true },
        { name: "uid", type: "uuid", nullable: true },
        { name: "amount", type: "int64", nullable: true },
        { name: "ratio", type: "f64", nullable: true },
        { name: "flag", type: "bool", nullable: true },
        { name: "level", type: "dict", nullable: true },
      ],
    },
  ],
};

const LABELS = ["low", "medium", "high"];
const ENUMS: readonly ColumnarEnumLabels[] = [
  { collection: "t", column: "level", labels: LABELS },
];

/** Three statistics chunks of 4,096 rows and nine filter blocks. */
const ROWS = 9_000;
const CHUNK_ROWS = 4_096;
const RATIOS = [0, -0, 1.5, 2.5, 1e10, -3];

function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state / 2 ** 32;
  };
}

function uuid(index: number): Uint8Array {
  const bytes = new Uint8Array(16);
  new DataView(bytes.buffer).setUint32(8, Math.imul(index + 1, 2_654_435_761));
  new DataView(bytes.buffer).setUint32(12, index);
  bytes[0] = 0xa0;
  return bytes;
}

const pad = (value: number) => String(value).padStart(3, "0");

function baseRows(): SqlValue[][] {
  const next = generator(7);
  return Array.from({ length: ROWS }, (_unused, id) => {
    const nullable = <T>(value: T): T | null => (next() < 0.1 ? null : value);
    return [
      BigInt(id),
      // The first chunk holds only ten tags, so a list of the others skips it.
      nullable(`tag-${pad(Math.floor(next() * (id < CHUNK_ROWS ? 10 : 40)))}`),
      nullable(`note-${pad(Math.floor(next() * 500))}`),
      nullable(uuid(Math.floor(next() * 300))),
      // Ascending, so each chunk holds its own range.
      id % 11 === 0 ? null : BigInt(id * 10),
      nullable(RATIOS[Math.floor(next() * RATIOS.length)] ?? 0),
      nullable(next() < 0.5),
      nullable(LABELS[Math.floor(next() * LABELS.length)] ?? "low"),
    ];
  });
}

const primaryKey = (id: number) => ({
  collection: "t",
  primaryKey: new TextEncoder().encode(String(id)),
  key: [BigInt(id)],
});

/**
 * Cells set to values no dictionary entry holds, to values a list names, to
 * NULL, on either side of a chunk boundary, with deletes and inserted rows.
 */
const UPDATES: readonly (readonly [
  number,
  Partial<Record<Column, SqlValue>>,
])[] = [
  [3, { tag: "tag-new", note: "note-new", amount: 77n, level: "high" }],
  [4, { tag: null, uid: uuid(999), ratio: -0, flag: null }],
  [4_097, { tag: "tag-005", note: null, amount: 40_970n, ratio: 0 }],
  [8_000, { uid: null, level: "omega", flag: true }],
];
const DELETES = [5, 6_001];
const INSERTS: readonly SqlValue[][] = [
  [20_000n, "tag-new", "note-001", uuid(999), 5n, 1.5, true, "low"],
  [20_001n, null, null, null, null, null, null, null],
];
const PENDING: readonly [number, Partial<Record<Column, SqlValue>>] = [
  9,
  { tag: "tag-pending", amount: 123n },
];

function changedRows(rows: readonly SqlValue[][]): SqlValue[][] {
  const changed = rows.map((row) => [...row]);
  for (const [id, values] of [...UPDATES, PENDING])
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
  overlay.applyOptimistic({
    mutationId: "in-list-pending",
    actions: [
      {
        kind: "patch",
        ...primaryKey(PENDING[0]),
        changes: Object.entries(PENDING[1]).map(([name, value]) => ({
          column: COLUMNS.indexOf(name as Column),
          value: value ?? null,
          mode: "set" as const,
        })),
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
        // Ids are row numbers.
        lookupKey: (key) =>
          typeof key[0] === "bigint" && key[0] < BigInt(rows.length)
            ? Number(key[0])
            : -1,
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

const inList = (
  column: Column,
  values: readonly QueryLiteral[],
): QueryPredicate => ({
  kind: "in-list",
  field: { source: "t", column },
  values,
});
const not = (predicate: QueryPredicate): QueryPredicate => ({
  kind: "not",
  predicate,
});
const and = (...predicates: QueryPredicate[]): QueryPredicate => ({
  kind: "and",
  predicates,
});
const or = (...predicates: QueryPredicate[]): QueryPredicate => ({
  kind: "or",
  predicates,
});
const eq = (column: Column, value: QueryLiteral): QueryPredicate => ({
  kind: "compare",
  operator: "eq",
  left: { kind: "field", field: { source: "t", column } },
  right: { kind: "literal", value },
});
const isNull = (column: Column): QueryPredicate => ({
  kind: "is-null",
  expression: { kind: "field", field: { source: "t", column } },
});
/** The form an application wrote before `in-list` existed. */
const anyOf = (column: Column, values: readonly QueryLiteral[]) =>
  or(...values.map((value) => eq(column, value)));

type Row = readonly SqlValue[];

function same(left: unknown, right: unknown): boolean {
  if (left instanceof Uint8Array || right instanceof Uint8Array)
    return (
      left instanceof Uint8Array &&
      right instanceof Uint8Array &&
      left.byteLength === right.byteLength &&
      left.every((byte, index) => byte === right[index])
    );
  return Object.is(left, right);
}

/** The oracle: the row's cell is not NULL and equals a value of the list. */
function member(row: Row, column: Column, values: readonly QueryLiteral[]) {
  const cell = row[COLUMNS.indexOf(column)] ?? null;
  return cell !== null && values.some((value) => same(cell, value));
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

function expectedIds(rows: readonly Row[], keep: (row: Row) => boolean) {
  return rows
    .filter(keep)
    .map((row) => Number(row[0]))
    .sort((left, right) => left - right);
}

async function paths() {
  const rows = baseRows();
  const changed = changedRows(rows);
  const clean = await artifactOverlay(rows);
  const dirty = await artifactOverlay(rows);
  applyChanges(dirty);
  const plain = plainOverlay(rows);
  const plainChanged = plainOverlay(rows);
  applyChanges(plainChanged);
  return [
    ["materialized", engineOver(plain), rows],
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
    ["dirty materialized", engineOver(plainChanged), changed],
  ] as const;
}

const FOREIGN = [1n, 1.5, true, new Uint8Array(3), "", "no-such-value"];

/** Each case: a name, a predicate, and the oracle's rows. */
function cases(): (readonly [string, QueryPredicate, (row: Row) => boolean])[] {
  const out: (readonly [string, QueryPredicate, (row: Row) => boolean])[] = [];
  const add = (
    column: Column,
    label: string,
    values: readonly QueryLiteral[],
  ) => {
    const keep = (row: Row) => member(row, column, values);
    out.push([`${column} in ${label}`, inList(column, values), keep]);
    // NULL is neither in the list nor out of it, unless the list is empty.
    out.push([
      `${column} not in ${label}`,
      not(inList(column, values)),
      (row) =>
        values.length === 0 ||
        ((row[COLUMNS.indexOf(column)] ?? null) !== null && !keep(row)),
    ]);
  };
  const uuids = [uuid(0), uuid(1), uuid(7), uuid(999), uuid(250)];
  add("tag", "a few", ["tag-003", "tag-017", "tag-new", "tag-pending"]);
  add("tag", "one", ["tag-020"]);
  add("tag", "duplicates", ["tag-003", "tag-003", "tag-004", "tag-003"]);
  add("tag", "none present", ["tag-099", "other"]);
  add("tag", "the dictionary's whole range", [
    ...Array.from({ length: 40 }, (_unused, index) => `tag-${pad(index)}`),
  ]);
  add("tag", "values of other types", [...FOREIGN, "tag-001"]);
  add("note", "a few", ["note-001", "note-250", "note-new", "note-499"]);
  add("note", "duplicates and the empty text", ["", "note-001", "note-001"]);
  add("note", "values of other types", [...FOREIGN, "note-002"]);
  add("note", "a text a column cannot hold", ["note-\ud800", "note-003"]);
  add("uid", "a few", uuids);
  add("uid", "a wrong length", [new Uint8Array(15), uuids[0] as Uint8Array]);
  add("uid", "values of other types", [...FOREIGN, uuid(3)]);
  add("amount", "ascending values", [0n, 10n, 40_970n, 77n, 123n, 5n, 89_990n]);
  add("amount", "values of other types", [
    10,
    "10",
    true,
    new Uint8Array(1),
    20n,
  ]);
  add("ratio", "zero, negative zero and others", [0, 1.5, 1e10]);
  add("ratio", "negative zero alone", [-0]);
  add("ratio", "positive zero alone", [0]);
  add("ratio", "NaN and infinities, which no cell holds", [
    Number.NaN,
    Number.POSITIVE_INFINITY,
    2.5,
  ]);
  add("ratio", "values of other types", [1n, "1.5", false]);
  add("flag", "true", [true]);
  add("flag", "both", [true, false]);
  add("flag", "values of other types", [1, "true"]);
  add("level", "labels, a text outside them, and a number", [
    "low",
    "high",
    "omega",
    3,
  ]);
  add("id", "a few ids", [0n, 1n, 4_095n, 4_096n, 8_999n, 20_000n, 5n]);
  for (const column of COLUMNS) add(column, "the empty list", []);
  out.push([
    "an in-list under and, or and not with plain leaves",
    and(
      or(inList("tag", ["tag-001", "tag-002", "tag-new"]), isNull("tag")),
      not(or(inList("amount", [20n, 30n, 77n]), eq("flag", false))),
    ),
    // `not` of an `or` is the `and` of their negations, each of which is
    // false for a NULL cell.
    (row) =>
      (member(row, "tag", ["tag-001", "tag-002", "tag-new"]) ||
        row[COLUMNS.indexOf("tag")] === null) &&
      row[COLUMNS.indexOf("amount")] !== null &&
      !member(row, "amount", [20n, 30n, 77n]) &&
      row[COLUMNS.indexOf("flag")] === true,
  ]);
  out.push([
    "two in-lists on different column kinds",
    and(inList("uid", uuids), inList("note", ["note-001", "note-002"])),
    (row) =>
      member(row, "uid", uuids) &&
      member(row, "note", ["note-001", "note-002"]),
  ]);
  out.push([
    "not of an or of in-lists, NULL out of both",
    not(or(inList("tag", ["tag-001"]), inList("note", ["note-001"]))),
    (row) =>
      row[COLUMNS.indexOf("tag")] !== null &&
      row[COLUMNS.indexOf("note")] !== null &&
      !member(row, "tag", ["tag-001"]) &&
      !member(row, "note", ["note-001"]),
  ]);
  return out;
}

describe("an in-list predicate", () => {
  test("selects exactly the rows whose cell is a listed value, negated, empty, with duplicates and values of other types, on every filter path", async () => {
    const all = cases();
    for (const [name, engine, rows] of await paths()) {
      let encoded = 0;
      let materialized = 0;
      for (const [label, where, keep] of all) {
        expect(engineIds(engine, where), `${name}: ${label}`).toEqual(
          expectedIds(rows, keep),
        );
        const inspection = inspectQueryExecution(engine);
        // An empty list is decided without reading a cell, on any base.
        if (label.includes("empty list")) continue;
        if (name === "materialized" || name === "dirty materialized")
          expect(inspection.encodedComparisons, `${name}: ${label}`).toBe(0);
        encoded += inspection.encodedComparisons;
        materialized += inspection.materializedComparisons;
      }
      if (name !== "materialized" && name !== "dirty materialized")
        expect(encoded, name).toBeGreaterThan(materialized);
    }
  }, 120_000);

  test("matches the reference query evaluator on every case", async () => {
    const all = cases();
    const reference = plainOverlay(baseRows());
    applyChanges(reference);
    const [, engine] = (await paths())[5] ?? [];
    for (const [label, where] of all) {
      const specification = idsWhere(where);
      expect(
        (engine as ColumnarQueryEngine).query(specification, OPTIONS),
        label,
      ).toEqual(
        referenceQuery(
          reference,
          [{ name: "t", columns: [...COLUMNS] }],
          specification,
        ),
      );
    }
  }, 120_000);

  test("gives the rows of the same list written as an or of eq, on random lists", async () => {
    const next = generator(99);
    const rows = baseRows();
    const lists: [Column, () => QueryLiteral][] = [
      ["tag", () => `tag-${pad(Math.floor(next() * 50))}`],
      ["note", () => `note-${pad(Math.floor(next() * 600))}`],
      ["uid", () => uuid(Math.floor(next() * 400))],
      ["amount", () => BigInt(Math.floor(next() * ROWS) * 10)],
      ["id", () => BigInt(Math.floor(next() * ROWS * 1.1))],
    ];
    for (const [name, engine] of await paths())
      for (let round = 0; round < 12; round++)
        for (const [column, draw] of lists) {
          const values = Array.from(
            { length: 1 + Math.floor(next() * 60) },
            draw,
          );
          expect(
            engineIds(engine, inList(column, values)),
            `${name}: ${column} round ${round}`,
          ).toEqual(engineIds(engine, anyOf(column, values)));
          expect(
            engineIds(engine, not(inList(column, values))),
            `${name}: not ${column} round ${round}`,
          ).toEqual(engineIds(engine, not(anyOf(column, values))));
        }
    expect(rows.length).toBe(ROWS);
  }, 120_000);

  test("skips the chunks whose bounds hold no listed value, and only those", async () => {
    const rows = baseRows();
    const engine = engineOver(await artifactOverlay(rows));
    // Ascending amounts: chunk 2 alone holds 81,920 and above.
    const high = [90_000n, 89_990n, 85_000n];
    expect(engineIds(engine, inList("amount", high))).toEqual(
      expectedIds(rows, (row) => member(row, "amount", high)),
    );
    expect(inspectQueryExecution(engine)).toMatchObject({
      chunksExamined: 3,
      chunksSkipped: 2,
    });
    // A value in each chunk skips none.
    const spread = [10n, 50_000n, 89_990n];
    engineIds(engine, inList("amount", spread));
    expect(inspectQueryExecution(engine)).toMatchObject({
      chunksSkipped: 0,
    });
    // Between two chunks' bounds nothing can match.
    const between = [40_950n + 5n];
    expect(engineIds(engine, inList("amount", between))).toEqual([]);
    // The first chunk's tags are the first ten only.
    const late = ["tag-020", "tag-030"];
    expect(engineIds(engine, inList("tag", late))).toEqual(
      expectedIds(rows, (row) => member(row, "tag", late)),
    );
    expect(inspectQueryExecution(engine).chunksSkipped).toBe(1);
    // The empty list matches no row, so every chunk is skipped.
    expect(engineIds(engine, inList("amount", []))).toEqual([]);
    expect(inspectQueryExecution(engine)).toMatchObject({
      chunksExamined: 3,
      chunksSkipped: 3,
    });
    // Its negation matches every row, NULL included.
    expect(engineIds(engine, not(inList("amount", []))).length).toBe(ROWS);
    expect(inspectQueryExecution(engine).chunksSkipped).toBe(0);
    // A negation skips a chunk only where its one value is the list's.
    engineIds(engine, not(inList("flag", [true, false])));
    expect(inspectQueryExecution(engine).chunksSkipped).toBe(0);
  });

  test("never skips a chunk a write dirtied after an earlier query found it clean", async () => {
    const rows = baseRows();
    const overlay = await artifactOverlay(rows);
    const engine = engineOver(overlay);
    const late = ["tag-020", "tag-030"];
    engineIds(engine, inList("tag", late));
    expect(inspectQueryExecution(engine).chunksSkipped).toBe(1);
    overlay.applyCanonical({
      transactions: [
        {
          mutationIds: [],
          operations: [
            {
              kind: "update",
              ...primaryKey(17),
              changed: [{ column: COLUMNS.indexOf("tag"), value: "tag-020" }],
            },
          ],
        },
      ],
    });
    expect(engineIds(engine, inList("tag", late))).toContain(17);
    expect(inspectQueryExecution(engine).chunksSkipped).toBe(0);
  });

  test("tests a dictionary larger than its code bitmap, and a bytes column of a plain base", async () => {
    const distinct = 70_000;
    const rows = Array.from({ length: distinct }, (_unused, id) => [
      BigInt(id),
      `code-${String(id).padStart(6, "0")}`,
    ]);
    const schema: DirectColumnarSchema = {
      format: "calque-direct-columnar",
      version: 4,
      tables: [
        {
          name: "t",
          primaryKey: ["id"],
          columns: [
            { name: "id", type: "int64", nullable: false },
            { name: "code", type: "dict", nullable: false },
          ],
        },
      ],
    };
    const encoded = encodeArtifact(
      schema,
      new Map([["t", rows.map(([id, code]) => ({ id, code }))]]),
    );
    const workspace = await openDirectColumnarArtifact(
      encoded.bytes.slice().buffer,
      {
        byteLength: encoded.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema,
      },
    );
    const engine = new ColumnarQueryEngine(
      new OverlayEngine([
        {
          name: "t",
          columns: ["id", "code"],
          base: artifactBase(workspace.table("t"), ["id", "code"]),
          primaryKey: ["id"],
        },
      ]),
      { capacityBytes: 1 << 30 },
    );
    const wanted = ["code-000007", "code-069999", "code-040000", "code-x"];
    const result = engine.query(
      {
        version: 1,
        from: { collection: "t", as: "t" },
        where: {
          kind: "in-list",
          field: { source: "t", column: "code" },
          values: wanted,
        },
        select: [
          { kind: "field", field: { source: "t", column: "id" }, as: "id" },
        ],
        orderBy: [{ by: "id", direction: "asc" }],
      },
      OPTIONS,
    );
    expect(result.rows.map((row) => Number(row[0]))).toEqual([
      7, 40_000, 69_999,
    ]);
    expect(inspectQueryExecution(engine).encodedComparisons).toBeGreaterThan(0);

    const blobs = Array.from({ length: 50 }, (_unused, id) =>
      Uint8Array.from({ length: id % 7 }, (_byte, index) => id + index),
    );
    const plain = new ColumnarQueryEngine(
      new OverlayEngine([
        {
          name: "t",
          columns: ["id", "blob"],
          base: {
            rows: blobs.length,
            lookupKey: () => -1,
            valueAt: (column, row) =>
              column === 0 ? BigInt(row) : (blobs[row] ?? null),
          },
          primaryKey: ["id"],
        },
      ]),
      { capacityBytes: 1 << 30 },
    );
    const listed = [blobs[13] as Uint8Array, new Uint8Array(0), "text"];
    const bytesResult = plain.query(
      {
        version: 1,
        from: { collection: "t", as: "t" },
        where: {
          kind: "in-list",
          field: { source: "t", column: "blob" },
          values: listed,
        },
        select: [
          { kind: "field", field: { source: "t", column: "id" }, as: "id" },
        ],
        orderBy: [{ by: "id", direction: "asc" }],
      },
      OPTIONS,
    );
    expect(bytesResult.rows.map((row) => Number(row[0]))).toEqual(
      blobs
        .map((blob, id) => [blob, id] as const)
        .filter(([blob]) => listed.some((value) => same(blob, value)))
        .map(([, id]) => id),
    );
  });
});

const JOIN_SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "links",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "parent_id", type: "utf8", nullable: false },
        { name: "item_id", type: "int64", nullable: false },
      ],
    },
    {
      name: "items",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "kind", type: "dict", nullable: false },
        { name: "size", type: "int64", nullable: false },
      ],
    },
  ],
};

describe("an in-list predicate in a join", () => {
  test("filters the base before the join and a joined column after it, as the reference does", async () => {
    const links = Array.from({ length: 3_000 }, (_unused, id) => ({
      id: BigInt(id),
      parent_id: `parent-${pad(id % 200)}-${"x".repeat(15)}`,
      item_id: BigInt((id * 7) % 1_650),
    }));
    const items = Array.from({ length: 1_500 }, (_unused, id) => ({
      id: BigInt(id),
      kind: `kind-${id % 5}`,
      size: BigInt(id % 17),
    }));
    const encoded = encodeArtifact(
      JOIN_SCHEMA,
      new Map<string, readonly Record<string, unknown>[]>([
        ["links", links],
        ["items", items],
      ]),
    );
    const workspace = await openDirectColumnarArtifact(
      encoded.bytes.slice().buffer,
      {
        byteLength: encoded.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema: JOIN_SCHEMA,
      },
    );
    const columns = {
      links: ["id", "parent_id", "item_id"],
      items: ["id", "kind", "size"],
    };
    const overlay = new OverlayEngine(
      (["links", "items"] as const).map((name) => ({
        name,
        columns: columns[name],
        base: artifactBase(workspace.table(name), columns[name]),
        primaryKey: ["id"],
      })),
    );
    const engine = new ColumnarQueryEngine(overlay, {
      capacityBytes: 1 << 30,
    });
    const parents = Array.from(
      { length: 40 },
      (_unused, index) => `parent-${pad(index * 5)}-${"x".repeat(15)}`,
    );
    const field = (source: string, column: string) => ({ source, column });
    const query = (
      where: QueryPredicate,
      kind: "inner" | "left",
    ): QuerySpecification => ({
      version: 1,
      from: { collection: "links", as: "l" },
      joins: [
        {
          kind,
          collection: "items",
          as: "i",
          on: { left: field("l", "item_id"), right: field("i", "id") },
        },
      ],
      where,
      select: [
        { kind: "field", field: field("i", "kind"), as: "kind" },
        { kind: "field", field: field("l", "parent_id"), as: "parent" },
        { kind: "aggregate", operator: "count", as: "rows" },
        {
          kind: "aggregate",
          operator: "sum",
          field: field("i", "size"),
          as: "size",
        },
      ],
      groupBy: [field("i", "kind"), field("l", "parent_id")],
      orderBy: [
        { by: "parent", direction: "asc" },
        { by: "kind", direction: "asc" },
      ],
    });
    const listed = (column: string, source: string, values: QueryLiteral[]) =>
      ({
        kind: "in-list",
        field: field(source, column),
        values,
      }) as const;
    const kinds = ["kind-1", "kind-3", "kind-9"];
    for (const kind of ["inner", "left"] as const)
      for (const where of [
        listed("parent_id", "l", parents),
        not(listed("parent_id", "l", parents)),
        listed("kind", "i", kinds),
        not(listed("kind", "i", kinds)),
        and(listed("parent_id", "l", parents), not(listed("kind", "i", kinds))),
        or(listed("parent_id", "l", parents), listed("kind", "i", kinds)),
        listed("parent_id", "l", []),
        not(listed("kind", "i", [])),
      ]) {
        const result = engine.query(query(where, kind), OPTIONS);
        expect(result).toEqual(
          referenceQuery(
            overlay,
            [
              { name: "links", columns: columns.links },
              { name: "items", columns: columns.items },
            ],
            query(where, kind),
          ),
        );
      }
    const listedRows = engine.query(
      query(listed("parent_id", "l", parents), "inner"),
      OPTIONS,
    );
    expect(listedRows.rows.length).toBeGreaterThan(0);
    const filtered = inspectQueryExecution(engine);
    expect(filtered.encodedComparisons).toBeGreaterThan(0);
    expect(filtered.materializedComparisons).toBe(0);
  }, 60_000);
});

describe("an in-list predicate's specification", () => {
  const specification = (where: QueryPredicate): QuerySpecification => ({
    version: 1,
    from: { collection: "t", as: "t" },
    where,
    select: [{ kind: "field", field: { source: "t", column: "id" }, as: "id" }],
  });
  const refused = (where: unknown, message: RegExp) => {
    expect(() =>
      validateQuerySpecification(specification(where as QueryPredicate)),
    ).toThrow(QueryError);
    expect(() =>
      validateQuerySpecification(specification(where as QueryPredicate)),
    ).toThrow(message);
  };

  test("holds 500 text ids of 25 characters, which an or of eq cannot", () => {
    const ids = Array.from(
      { length: 500 },
      (_unused, index) => `${String(index).padStart(5, "0")}-${"i".repeat(19)}`,
    );
    expect(ids[0]?.length).toBe(25);
    expect(() =>
      validateQuerySpecification(specification(inList("tag", ids))),
    ).not.toThrow();
    expect(() =>
      validateQuerySpecification(specification(anyOf("tag", ids))),
    ).toThrow(`query specification exceeds ${MAX_QUERY_SPEC_BYTES} bytes`);
  });

  test("holds 10,000 text keys of 36 characters as one predicate node", () => {
    const keys = Array.from(
      { length: 10_000 },
      (_unused, index) =>
        `${index.toString(16).padStart(8, "0")}-0000-4000-8000-${"0".repeat(12)}`,
    );
    expect(keys[0]?.length).toBe(36);
    const validated = validateQuerySpecification(
      specification(inList("tag", keys)),
    );
    expect(validated.where).toMatchObject({ kind: "in-list" });
    expect(
      canonicalQuerySpecification(specification(inList("tag", keys))),
    ).toContain(keys[9_999] as string);
  });

  test("counts the value lists apart from the rest of the specification", () => {
    const keys = Array.from(
      { length: 5_000 },
      (_unused, index) => `${index}-${"k".repeat(60)}`,
    );
    // The rest of the specification keeps the cap it always had.
    refused(
      and(inList("tag", keys), eq("tag", "x".repeat(MAX_QUERY_SPEC_BYTES))),
      /query specification exceeds 65536 bytes/u,
    );
    expect(() =>
      validateQuerySpecification(
        specification(
          and(
            inList("tag", keys),
            eq("tag", "x".repeat(MAX_QUERY_SPEC_BYTES / 2)),
          ),
        ),
      ),
    ).not.toThrow();
  });

  test("refuses more values than the cap, across lists, before reading them", () => {
    const count = MAX_QUERY_LIST_VALUES + 1;
    refused(
      inList("tag", new Array<string>(count).fill("a")),
      /predicate.values must be an array of at most 50000 items/u,
    );
    const half = new Array<string>(MAX_QUERY_LIST_VALUES / 2 + 1).fill("a");
    refused(
      or(inList("tag", half), inList("note", half)),
      /in-list values exceed 50000/u,
    );
    expect(() =>
      validateQuerySpecification(
        specification(
          or(inList("tag", half.slice(1)), inList("note", half.slice(1))),
        ),
      ),
    ).not.toThrow();
  });

  test("refuses values past the byte cap before serializing them", () => {
    const text = "t".repeat(1_000_000);
    const stringify = vi.spyOn(JSON, "stringify");
    try {
      refused(
        inList(
          "tag",
          Array.from({ length: 5 }, () => text),
        ),
        new RegExp(`in-list values exceed ${MAX_QUERY_LIST_BYTES} bytes`, "u"),
      );
      refused(
        inList("uid", [new Uint8Array(MAX_QUERY_LIST_BYTES)]),
        new RegExp(`in-list values exceed ${MAX_QUERY_LIST_BYTES} bytes`, "u"),
      );
      expect(stringify).not.toHaveBeenCalled();
    } finally {
      stringify.mockRestore();
    }
    // Escapes and hex make the canonical text longer than the values.
    refused(
      inList(
        "tag",
        Array.from({ length: 4 }, () => '"'.repeat(600_000)),
      ),
      /in-list values exceed/u,
    );
  });

  test("refuses a null value, a missing or extra field, and an unknown column of it", () => {
    refused(inList("tag", [null as unknown as string]), /must not be null/u);
    refused(
      { kind: "in-list", values: ["a"] },
      /in-list predicate is invalid/u,
    );
    refused(
      { kind: "in-list", field: { source: "t", column: "tag" } },
      /in-list predicate is invalid/u,
    );
    refused(
      { ...inList("tag", ["a"]), negated: true },
      /in-list predicate is invalid/u,
    );
    refused({ ...inList("tag", ["a"]), values: "abc" }, /must be an array/u);
    refused(
      { ...inList("tag", ["a"]), values: [{ a: 1 }] },
      /must be a Uint8Array or a JSON scalar/u,
    );
    refused(
      { ...inList("tag", ["a"]), field: { source: "t" } },
      /unknown or missing fields/u,
    );
  });

  test("copies its values, so a caller's later change cannot reach a validated specification", () => {
    const bytes = new Uint8Array([1, 2, 3]);
    const values: QueryLiteral[] = ["a", bytes];
    const validated = validateQuerySpecification(
      specification(inList("uid", values)),
    );
    values.push("b");
    bytes[0] = 9;
    const where = validated.where as Extract<
      QueryPredicate,
      { kind: "in-list" }
    >;
    expect(where.values).toHaveLength(2);
    expect(where.values[1]).toEqual(new Uint8Array([1, 2, 3]));
    expect(Object.isFrozen(where.values)).toBe(true);
  });

  test("answers the same canonical text for equal specifications", () => {
    const a = canonicalQuerySpecification(
      specification(inList("tag", ["a", "b"])),
    );
    const b = canonicalQuerySpecification(
      specification(inList("tag", ["a", "b"])),
    );
    const c = canonicalQuerySpecification(
      specification(inList("tag", ["b", "a"])),
    );
    expect(a).toBe(b);
    expect(a).not.toBe(c);
  });

  test("refuses a column the collection lacks and a source it lacks, when run", () => {
    const engine = new ColumnarQueryEngine(plainOverlay([]), {
      capacityBytes: 1 << 30,
    });
    expect(() =>
      engine.query(specification(inList("nope" as Column, ["a"])), OPTIONS),
    ).toThrow(/unknown column t.nope/u);
    expect(() =>
      engine.query(
        specification({
          kind: "in-list",
          field: { source: "other", column: "tag" },
          values: ["a"],
        }),
        OPTIONS,
      ),
    ).toThrow(/unknown query source other/u);
  });
});
