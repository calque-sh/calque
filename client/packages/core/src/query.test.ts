import { describe, expect, test } from "vitest";
import {
  OverlayEngine,
  type KeyPart,
  type OverlayBase,
  type OverlayCollectionSchema,
  type SqlValue,
} from "./overlay.js";
import {
  ColumnarQueryEngine,
  QueryError,
  canonicalQuerySpecification,
  validateQuerySpecification,
  type QueryComparisonOperator,
  type QueryField,
  type QueryJsonValue,
  type QueryPredicate,
  type QueryResult,
  type QuerySpecification,
} from "./query.js";
import {
  referenceQuery,
  type QueryReferenceSchema,
} from "./query-reference.fixture.js";

interface TestCollection extends QueryReferenceSchema {
  readonly rows: readonly (readonly TestValue[])[];
}

type TestValue = SqlValue | QueryJsonValue;

const encoder = new TextEncoder();
const keyBytes = (key: string): Uint8Array => encoder.encode(key);

function keyEqual(left: TestValue, right: KeyPart): boolean {
  if (left instanceof Uint8Array && right instanceof Uint8Array)
    return (
      left.byteLength === right.byteLength &&
      left.every((value, index) => value === right[index])
    );
  return Object.is(left, right);
}

function overlayOf(collections: readonly TestCollection[]): OverlayEngine {
  const schemas: OverlayCollectionSchema[] = collections.map((collection) => {
    const base: OverlayBase = {
      rows: collection.rows.length,
      lookupKey(key) {
        if (key.length !== 1) return -1;
        return collection.rows.findIndex((row) =>
          keyEqual(row[0] ?? null, key[0] as KeyPart),
        );
      },
      valueAt(column, row) {
        return (collection.rows[row]?.[column] ?? null) as SqlValue;
      },
    };
    return {
      name: collection.name,
      columns: collection.columns,
      base,
    };
  });
  return new OverlayEngine(schemas);
}

const COLLECTIONS: readonly TestCollection[] = [
  {
    name: "events",
    columns: ["id", "kind", "score", "big", "nullable", "blob", "json"],
    rows: [
      [
        "a",
        "error",
        Number.NaN,
        3n,
        null,
        Uint8Array.of(1),
        { nested: [{ ok: true }] },
      ],
      ["b", "1", -0, 2n, null, Uint8Array.of(2), { value: 2 }],
      ["c", 1, 0, 1n, null, Uint8Array.of(1), { value: 3 }],
      ["d", "error", 5, 0n, null, Uint8Array.of(3), { value: 4 }],
      ["e", null, -2, -1n, null, new Uint8Array(), { value: 5 }],
      ["f", "warning", Number.NaN, 5n, null, Uint8Array.of(1, 0), { value: 6 }],
    ],
  },
  {
    name: "tags",
    columns: ["id", "event_id", "label"],
    rows: [
      ["t1", "a", "alpha"],
      ["t2", "a", "beta"],
      ["t3", "c", "gamma"],
      ["t4", null, "orphan"],
      ["t5", "1", "typed"],
    ],
  },
  { name: "empty", columns: ["id", "value"], rows: [] },
];

const REFERENCE_SCHEMAS: readonly QueryReferenceSchema[] = COLLECTIONS.map(
  ({ name, columns }) => ({ name, columns }),
);

const source = { collection: "events", as: "e" } as const;
const field = (column: string, alias = "e"): QueryField => ({
  source: alias,
  column,
});
const fieldExpression = (column: string, alias = "e") =>
  ({ kind: "field", field: field(column, alias) }) as const;
const literalExpression = (value: SqlValue) =>
  ({ kind: "literal", value }) as const;

function engine(overlay = overlayOf(COLLECTIONS), capacityBytes = 1 << 30) {
  return new ColumnarQueryEngine(overlay, { capacityBytes });
}

function expectOracle(
  queryEngine: ColumnarQueryEngine,
  overlay: OverlayEngine,
  specification: QuerySpecification,
): QueryResult {
  const result = queryEngine.query(specification, {
    maxRows: 10_000,
    maxBytes: 16 * 1024 * 1024,
  });
  expect(result).toEqual(
    referenceQuery(overlay, REFERENCE_SCHEMAS, specification),
  );
  return result;
}

describe("columnar query specification", () => {
  test("normalizes, compares, and deeply snapshots inert data", () => {
    const first: QuerySpecification = {
      version: 1,
      from: source,
      where: {
        kind: "compare",
        operator: "eq",
        left: fieldExpression("blob"),
        right: literalExpression(Uint8Array.of(1)),
      },
      select: [{ kind: "field", field: field("id"), as: "id" }],
      orderBy: [{ by: "id", direction: "asc" }],
    };
    const second = {
      select: [
        {
          as: "id",
          field: { column: "id", source: "e" },
          kind: "field",
        },
      ],
      from: { as: "e", collection: "events" },
      orderBy: [{ direction: "asc", by: "id" }],
      where: {
        right: { value: Uint8Array.of(1), kind: "literal" },
        operator: "eq",
        left: {
          field: { column: "blob", source: "e" },
          kind: "field",
        },
        kind: "compare",
      },
      version: 1,
    };
    expect(canonicalQuerySpecification(second)).toBe(
      canonicalQuerySpecification(first),
    );
    expect(
      canonicalQuerySpecification({
        ...first,
        where: { ...first.where, right: literalExpression(Uint8Array.of(2)) },
      }),
    ).not.toBe(canonicalQuerySpecification(first));
    const validated = validateQuerySpecification(first);
    expect(Object.isFrozen(validated)).toBe(true);
    expect(Object.isFrozen(validated.where)).toBe(true);
    expect(Object.isFrozen(validated.select[0]?.field)).toBe(true);
  });

  test("rejects callbacks, accessors, SQL text, cycles, and unknown fields", () => {
    const minimal = {
      version: 1,
      from: source,
      select: [{ kind: "field", field: field("id"), as: "id" }],
    };
    expect(() =>
      validateQuerySpecification({ ...minimal, sql: "select 1" }),
    ).toThrow(QueryError);
    expect(() =>
      validateQuerySpecification({
        ...minimal,
        where: {
          kind: "compare",
          operator: "eq",
          left: fieldExpression("id"),
          right: { kind: "literal", value: () => true },
        },
      }),
    ).toThrow(QueryError);
    let getterRan = false;
    const accessor = Object.defineProperty({ ...minimal }, "where", {
      enumerable: true,
      get() {
        getterRan = true;
        return undefined;
      },
    });
    expect(() => validateQuerySpecification(accessor)).toThrow(QueryError);
    expect(getterRan).toBe(false);
    const cyclic: { kind: "not"; predicate?: unknown } = { kind: "not" };
    cyclic.predicate = cyclic;
    expect(() =>
      validateQuerySpecification({ ...minimal, where: cyclic }),
    ).toThrow(QueryError);
  });

  test("refuses a null literal in a comparison, and names the NULL test", () => {
    const queryEngine = engine();
    const operators: readonly QueryComparisonOperator[] = [
      "eq",
      "neq",
      "lt",
      "lte",
      "gt",
      "gte",
    ];
    for (const operator of operators)
      for (const nullLeft of [false, true]) {
        const compared = [fieldExpression("nullable"), literalExpression(null)];
        const [left, right] = nullLeft ? compared.reverse() : compared;
        if (!left || !right) throw new Error("no operands");
        const specification: QuerySpecification = {
          version: 1,
          from: source,
          // Under `not` too, where it would otherwise match every row.
          where: {
            kind: "not",
            predicate: { kind: "compare", operator, left, right },
          },
          select: [{ kind: "field", field: field("id"), as: "id" }],
        };
        for (const run of [
          () => validateQuerySpecification(specification),
          () => queryEngine.query(specification),
        ])
          expect(run).toThrow(
            /comparison literal must not be null; test for NULL with is-null \(isNull or isNotNull\)/u,
          );
      }
    expect(
      queryEngine.query({
        version: 1,
        from: source,
        where: { kind: "is-null", expression: fieldExpression("nullable") },
        select: [{ kind: "field", field: field("id"), as: "id" }],
      }).rows,
    ).toHaveLength(6);
  });
});

describe("columnar query evaluator", () => {
  test("compares values without numeric/string type confusion", () => {
    const overlay = overlayOf(COLLECTIONS);
    const queryEngine = engine(overlay);
    for (const [operator, value] of [
      ["eq", 1],
      ["neq", "error"],
      ["lt", 5],
      ["lte", -0],
      ["gt", -2],
      ["gte", Number.NaN],
    ] as const satisfies readonly (readonly [
      QueryComparisonOperator,
      SqlValue,
    ])[]) {
      expectOracle(queryEngine, overlay, {
        version: 1,
        from: source,
        where: {
          kind: "compare",
          operator,
          left: fieldExpression(
            operator === "eq" || operator === "neq" ? "kind" : "score",
          ),
          right: literalExpression(value),
        },
        select: [
          { kind: "field", field: field("id"), as: "id" },
          { kind: "field", field: field("kind"), as: "kind" },
        ],
      });
    }
    const numeric = queryEngine.query({
      version: 1,
      from: source,
      where: {
        kind: "compare",
        operator: "eq",
        left: fieldExpression("kind"),
        right: literalExpression(1),
      },
      select: [{ kind: "field", field: field("id"), as: "id" }],
    });
    const string = queryEngine.query({
      version: 1,
      from: source,
      where: {
        kind: "compare",
        operator: "eq",
        left: fieldExpression("kind"),
        right: literalExpression("1"),
      },
      select: [{ kind: "field", field: field("id"), as: "id" }],
    });
    expect(numeric.rows).toEqual([["c"]]);
    expect(string.rows).toEqual([["b"]]);
  });

  test("evaluates logical and null predicates through the oracle", () => {
    const overlay = overlayOf(COLLECTIONS);
    const queryEngine = engine(overlay);
    const notError: QueryPredicate = {
      kind: "not",
      predicate: {
        kind: "compare",
        operator: "eq",
        left: fieldExpression("kind"),
        right: literalExpression("error"),
      },
    };
    expectOracle(queryEngine, overlay, {
      version: 1,
      from: source,
      where: {
        kind: "and",
        predicates: [
          { kind: "is-null", expression: fieldExpression("nullable") },
          {
            kind: "or",
            predicates: [
              notError,
              {
                kind: "is-null",
                expression: fieldExpression("kind"),
                negated: false,
              },
            ],
          },
        ],
      },
      select: [{ kind: "field", field: field("id"), as: "id" }],
    });
    expectOracle(queryEngine, overlay, {
      version: 1,
      from: source,
      where: {
        kind: "is-null",
        expression: fieldExpression("kind"),
        negated: true,
      },
      select: [{ kind: "field", field: field("id"), as: "id" }],
    });
  });

  test("sorts null, NaN, and signed zero deterministically before limiting", () => {
    const overlay = overlayOf(COLLECTIONS);
    const queryEngine = engine(overlay);
    const specification: QuerySpecification = {
      version: 1,
      from: source,
      select: [
        { kind: "field", field: field("id"), as: "id" },
        { kind: "field", field: field("score"), as: "score" },
      ],
      orderBy: [{ by: "score", direction: "asc", nulls: "last" }],
      limit: 99,
    };
    const result = expectOracle(queryEngine, overlay, specification);
    expect(result.rows.map((row) => row[0])).toEqual([
      "e",
      "b",
      "c",
      "d",
      "a",
      "f",
    ]);
    expect(Object.is(result.rows[1]?.[1], -0)).toBe(true);
    expect(Object.is(result.rows[2]?.[1], 0)).toBe(true);
    expectOracle(queryEngine, overlay, {
      ...specification,
      orderBy: [{ by: "score", direction: "desc", nulls: "first" }],
      limit: 3,
    });
    expectOracle(queryEngine, overlay, { ...specification, limit: 0 });
  });

  test("groups and evaluates every aggregate, including all-null and empty inputs", () => {
    const overlay = overlayOf(COLLECTIONS);
    const queryEngine = engine(overlay);
    expectOracle(queryEngine, overlay, {
      version: 1,
      from: source,
      select: [
        { kind: "field", field: field("kind"), as: "kind" },
        { kind: "aggregate", operator: "count", as: "rows" },
        {
          kind: "aggregate",
          operator: "count",
          field: field("nullable"),
          as: "present",
        },
        {
          kind: "aggregate",
          operator: "sum",
          field: field("big"),
          as: "sum",
        },
        {
          kind: "aggregate",
          operator: "min",
          field: field("score"),
          as: "minimum",
        },
        {
          kind: "aggregate",
          operator: "max",
          field: field("score"),
          as: "maximum",
        },
      ],
      groupBy: [field("kind")],
      orderBy: [{ by: "kind", direction: "asc", nulls: "first" }],
    });
    const emptySource = { collection: "empty", as: "x" } as const;
    const emptyField = (column: string): QueryField => ({
      source: "x",
      column,
    });
    expectOracle(queryEngine, overlay, {
      version: 1,
      from: emptySource,
      select: [
        { kind: "aggregate", operator: "count", as: "count" },
        {
          kind: "aggregate",
          operator: "sum",
          field: emptyField("value"),
          as: "sum",
        },
        {
          kind: "aggregate",
          operator: "min",
          field: emptyField("value"),
          as: "min",
        },
        {
          kind: "aggregate",
          operator: "max",
          field: emptyField("value"),
          as: "max",
        },
      ],
    });
    expectOracle(queryEngine, overlay, {
      version: 1,
      from: emptySource,
      select: [
        {
          kind: "field",
          field: emptyField("value"),
          as: "value",
        },
        { kind: "aggregate", operator: "count", as: "count" },
      ],
      groupBy: [emptyField("value")],
    });
  });

  test("executes deterministic inner and left equijoins", () => {
    const overlay = overlayOf(COLLECTIONS);
    const queryEngine = engine(overlay);
    for (const kind of ["inner", "left"] as const)
      expectOracle(queryEngine, overlay, {
        version: 1,
        from: source,
        joins: [
          {
            kind,
            collection: "tags",
            as: "t",
            on: {
              left: field("id"),
              right: field("event_id", "t"),
            },
          },
        ],
        where: {
          kind: "not",
          predicate: {
            kind: "compare",
            operator: "eq",
            left: fieldExpression("label", "t"),
            right: literalExpression("beta"),
          },
        },
        select: [
          { kind: "field", field: field("id"), as: "event" },
          {
            kind: "field",
            field: field("label", "t"),
            as: "tag",
          },
        ],
        orderBy: [
          { by: "event", direction: "asc" },
          { by: "tag", direction: "asc", nulls: "last" },
        ],
      });
  });

  test("reads canonical and pending shadow, delete, insert, and reinsert state", () => {
    const overlay = overlayOf(COLLECTIONS);
    overlay.applyCanonical({
      transactions: [
        {
          mutationIds: [],
          operations: [
            {
              kind: "update",
              collection: "events",
              primaryKey: keyBytes("a"),
              key: ["a"],
              changed: [{ column: 2, value: 20 }],
            },
            {
              kind: "delete",
              collection: "events",
              primaryKey: keyBytes("b"),
              key: ["b"],
            },
            {
              kind: "insert",
              collection: "events",
              primaryKey: keyBytes("g"),
              key: ["g"],
              values: ["g", "canonical", 7, 7n, null, Uint8Array.of(7), null],
            },
          ],
        },
      ],
    });
    overlay.applyOptimistic({
      mutationId: "pending-query-state",
      actions: [
        {
          kind: "patch",
          collection: "events",
          primaryKey: keyBytes("c"),
          key: ["c"],
          changes: [{ column: 1, value: "pending", mode: "set" }],
        },
        {
          kind: "delete",
          collection: "events",
          primaryKey: keyBytes("d"),
          key: ["d"],
        },
        {
          kind: "insert",
          collection: "events",
          primaryKey: keyBytes("b"),
          key: ["b"],
          values: ["b", "reinserted", 9, 9n, null, Uint8Array.of(9), null],
        },
        {
          kind: "insert",
          collection: "events",
          primaryKey: keyBytes("h"),
          key: ["h"],
          values: ["h", "local", 8, 8n, null, Uint8Array.of(8), null],
        },
      ],
    });
    const queryEngine = engine(overlay);
    const result = expectOracle(queryEngine, overlay, {
      version: 1,
      from: source,
      select: [
        { kind: "field", field: field("id"), as: "id" },
        { kind: "field", field: field("kind"), as: "kind" },
        { kind: "field", field: field("score"), as: "score" },
      ],
      orderBy: [{ by: "id", direction: "asc" }],
    });
    expect(result.rows).toEqual([
      ["a", "error", 20],
      ["b", "reinserted", 9],
      ["c", "pending", 0],
      ["e", null, -2],
      ["f", "warning", Number.NaN],
      ["g", "canonical", 7],
      ["h", "local", 8],
    ]);
  });

  // JavaScript cannot deeply freeze a non-empty `Uint8Array`, so the contract
  // is frozen containers plus application-owned binary copies, the same
  // ownership the materialization surface already offers.
  test("returns frozen containers and application-owned binary copies", () => {
    const overlay = overlayOf(COLLECTIONS);
    const result = expectOracle(engine(overlay), overlay, {
      version: 1,
      from: source,
      select: [
        { kind: "field", field: field("id"), as: "id" },
        { kind: "field", field: field("blob"), as: "blob" },
        { kind: "field", field: field("json"), as: "json" },
      ],
      limit: 1,
    });
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.columns)).toBe(true);
    expect(Object.isFrozen(result.rows)).toBe(true);
    expect(Object.isFrozen(result.rows[0])).toBe(true);
    expect(Object.isFrozen(result.rows[0]?.[2])).toBe(true);
    const json = result.rows[0]?.[2];
    if (typeof json !== "object" || json === null || !("nested" in json))
      throw new Error("expected projected JSON object");
    const nested = json.nested;
    if (!Array.isArray(nested)) throw new Error("expected nested JSON array");
    expect(Object.isFrozen(nested)).toBe(true);
    expect(Object.isFrozen(nested[0])).toBe(true);
    const blob = result.rows[0]?.[1];
    if (!(blob instanceof Uint8Array))
      throw new Error("expected projected binary bytes");
    expect(blob).toEqual(Uint8Array.of(1));
    // Writing through the returned array must not reach engine-held state.
    blob[0] = 99;
    const again = expectOracle(engine(overlay), overlay, {
      version: 1,
      from: source,
      select: [{ kind: "field", field: field("blob"), as: "blob" }],
      limit: 1,
    });
    expect(again.rows[0]?.[0]).toEqual(Uint8Array.of(1));
  });

  test("precharges a fixed aggregate capacity pool and releases reservations", () => {
    const overlay = overlayOf(COLLECTIONS);
    const specification: QuerySpecification = {
      version: 1,
      from: source,
      select: [{ kind: "field", field: field("id"), as: "id" }],
      limit: 2,
    };
    const sizingEngine = engine(overlay);
    const options = { maxRows: 2, maxBytes: 64 } as const;
    const required = sizingEngine.estimateReservationBytes(
      specification,
      options,
    );
    expect(() =>
      engine(overlay, required - 1).query(specification, options),
    ).toThrowError(
      expect.objectContaining({
        code: "CAPACITY_EXCEEDED",
        requiredBytes: required,
        capacityBytes: required - 1,
      }),
    );
    expect(
      engine(overlay, required).query(specification, options).rows,
    ).toHaveLength(2);

    const rowBoundOptions = { maxRows: 1, maxBytes: 64 } as const;
    expect(() =>
      sizingEngine.query(specification, rowBoundOptions),
    ).toThrowError(expect.objectContaining({ code: "RESULT_TOO_LARGE" }));

    const failureOptions = { maxRows: 1, maxBytes: 1 } as const;
    const byteBoundSpecification = { ...specification, limit: 1 } as const;
    const failingRequired = sizingEngine.estimateReservationBytes(
      byteBoundSpecification,
      failureOptions,
    );
    const reusable = engine(overlay, failingRequired);
    expect(() =>
      reusable.query(byteBoundSpecification, failureOptions),
    ).toThrowError(
      expect.objectContaining({
        code: "RESULT_TOO_LARGE",
        message: expect.stringContaining(
          "exceeds maxBytes (1); select fewer columns",
        ),
      }),
    );
    expect(
      reusable.query({ ...specification, limit: 0 }, failureOptions).rows,
    ).toEqual([]);
  });
});
