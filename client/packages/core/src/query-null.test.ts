/**
 * NULL in predicates, against SQLite: its WHERE evaluates NULL with the same
 * three-valued rules as PostgreSQL's. Random rows over integer and text
 * columns, many of them NULL, and random predicates of every kind run through
 * each filter path: the materialized one over a plain base; the encoded row
 * and block paths over an artifact, with and without chunk pruning; and that
 * artifact under overlay changes, whose cells the encoded paths hand back to
 * the materialized one. Floats are left out because SQLite stores NaN as
 * NULL, and comparisons across types because PostgreSQL refuses them, so it
 * has no answer to hold the engine to.
 */
import { describe, expect, test } from "vitest";

import { OverlayEngine, type SqlValue } from "./overlay.js";
import {
  inspectQueryExecution,
  type QueryComparisonOperator,
  type QueryExpression,
  type QueryPredicate,
  type QuerySpecification,
} from "./query.js";
import { referenceQuery } from "./query-reference.fixture.js";
import {
  baseRows,
  OPTIONS,
  artifactOverlay,
  engineOver,
  sqliteOf,
  sqliteIds,
  field,
  literal,
  engineIds,
  expectPathsSelectAsSqlite,
} from "./query-null.fixture.js";

describe("predicates over NULL", () => {
  test("select the rows SQLite's WHERE selects, on every filter path, for seeds 0 to 149", async () => {
    await expectPathsSelectAsSqlite(0, 150);
  }, 120_000);

  test("compare NULL as UNKNOWN in each operator, field against field, and under not", () => {
    // id, x, y
    const rows: SqlValue[][] = [
      [1n, 1n, 1n],
      [2n, 1n, null],
      [3n, null, 1n],
      [4n, null, null],
      [5n, 2n, 1n],
    ];
    const overlay = new OverlayEngine([
      {
        name: "p",
        columns: ["id", "x", "y"],
        base: {
          rows: rows.length,
          lookupKey: () => -1,
          valueAt: (column, row) => rows[row]?.[column] ?? null,
        },
      },
    ]);
    const engine = engineOver(overlay);
    const x: QueryExpression = {
      kind: "field",
      field: { source: "p", column: "x" },
    };
    const y: QueryExpression = {
      kind: "field",
      field: { source: "p", column: "y" },
    };
    const one = literal(1n);
    const compare = (
      operator: QueryComparisonOperator,
      left: QueryExpression,
      right: QueryExpression,
    ): QueryPredicate => ({ kind: "compare", operator, left, right });
    const not = (predicate: QueryPredicate): QueryPredicate => ({
      kind: "not",
      predicate,
    });
    const cases: readonly (readonly [string, QueryPredicate, number[]])[] = [
      // NULL = NULL is UNKNOWN, as is NULL = 1.
      ["x = y", compare("eq", x, y), [1]],
      ["x <> y", compare("neq", x, y), [5]],
      ["NOT x = y", not(compare("eq", x, y)), [5]],
      ["x <> 1", compare("neq", x, one), [5]],
      ["NOT x > 1", not(compare("gt", x, one)), [1, 2]],
      ["1 >= x", compare("gte", one, x), [1, 2]],
      [
        "NOT (x = 1 AND y = 1)",
        not({
          kind: "and",
          predicates: [compare("eq", x, one), compare("eq", y, one)],
        }),
        [5],
      ],
      [
        "NOT (x = 1 OR y = 1)",
        not({
          kind: "or",
          predicates: [compare("eq", x, one), compare("eq", y, one)],
        }),
        [],
      ],
      [
        "x = 1 OR NOT y = 1",
        {
          kind: "or",
          predicates: [compare("eq", x, one), not(compare("eq", y, one))],
        },
        [1, 2],
      ],
      [
        "NOT (y IS NULL OR NOT x = 1)",
        not({
          kind: "or",
          predicates: [
            { kind: "is-null", expression: y },
            not(compare("eq", x, one)),
          ],
        }),
        [1],
      ],
    ];
    for (const [name, where, expected] of cases) {
      const specification: QuerySpecification = {
        version: 1,
        from: { collection: "p", as: "p" },
        where,
        select: [
          { kind: "field", field: { source: "p", column: "id" }, as: "id" },
        ],
        orderBy: [{ by: "id", direction: "asc" }],
      };
      const ids = (rows: readonly (readonly unknown[])[]) =>
        rows.map((row) => Number(row[0]));
      expect(ids(engine.query(specification, OPTIONS).rows), name).toEqual(
        expected,
      );
      expect(
        ids(
          referenceQuery(
            overlay,
            [{ name: "p", columns: ["id", "x", "y"] }],
            specification,
          ).rows,
        ),
        name,
      ).toEqual(expected);
    }
  });

  test("prune a negated range comparison by chunk statistics", async () => {
    const rows = baseRows();
    const a = field("a");
    const negated: QueryPredicate = {
      kind: "not",
      predicate: {
        kind: "compare",
        operator: "gt",
        left: a,
        right: literal(3n),
      },
    };
    const range: QueryPredicate = {
      kind: "compare",
      operator: "lte",
      left: a,
      right: literal(3n),
    };
    // Statistics are built by the first query over a base, and counted
    // there: each predicate gets a base of its own.
    const [negatedEngine, rangeEngine] = await Promise.all(
      [0, 1].map(async () => engineOver(await artifactOverlay(rows))),
    );
    if (!negatedEngine || !rangeEngine) throw new Error("no engine");
    const selected = engineIds(negatedEngine, negated);
    expect(selected).toEqual(sqliteIds(sqliteOf(rows), negated));
    expect(selected).toEqual(engineIds(rangeEngine, range));
    const inspection = inspectQueryExecution(negatedEngine);
    expect(inspection).toEqual(inspectQueryExecution(rangeEngine));
    // The second chunk holds `a` from 4 to 8 and the third only NULLs.
    expect(inspection.chunksSkipped).toBe(2);
  }, 60_000);
});
