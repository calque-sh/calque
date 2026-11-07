/**
 * Grouping and aggregation over encoded cells against the oracle, and the
 * overlay values and mixed numbers an aggregate meets. They have a file of
 * their own so that vitest runs them beside the other query scan tests.
 */
import { describe, expect, test } from "vitest";

import type { DirectColumnarSchema } from "./artifact.js";
import type { QueryPredicate, QuerySpecification } from "./query.js";
import {
  generator,
  overlayOf,
  baseOverlay,
  eventUpdate,
  STATES,
  overlayIn,
  OPTIONS,
  engineOver,
  expectReference,
  field,
  literal,
  randomPredicate,
  eventFields,
  countAll,
  AGGREGATES,
} from "./query-scan.fixture.js";

describe("query scan kernels", () => {
  const GROUPINGS: readonly (readonly string[])[] = [
    [],
    ["project"],
    ["level"],
    ["tag"],
    ["project", "level"],
    ["level", "project", "tag"],
    ["note"],
    ["label"],
    ["note", "amount"],
    ["amount"],
    ["occurred"],
  ];

  test("groups and aggregates encoded cells as the oracle does", async () => {
    const random = generator(0x6a0b);
    for (const state of STATES) {
      const overlay = await overlayIn(state);
      for (const groupBy of GROUPINGS) {
        for (const where of [undefined, randomPredicate(random, 2)]) {
          const specification: QuerySpecification = {
            version: 1,
            from: { collection: "events", as: "event" },
            ...(where === undefined ? {} : { where }),
            select: [...eventFields(...groupBy), ...AGGREGATES],
            ...(groupBy.length === 0
              ? {}
              : {
                  groupBy: groupBy.map((column) => ({
                    source: "event",
                    column,
                  })),
                }),
          };
          expectReference(overlay, specification);
        }
      }
    }
  }, 120_000);

  test("an encoded extreme is replaced by a larger overlay value, and replaces a smaller one", async () => {
    const overlay = await baseOverlay();
    // Row 20 opens its group with an encoded value; a later overlay row holds
    // the extreme for one group and not for another.
    overlay.applyCanonical({
      transactions: [
        {
          mutationIds: [],
          operations: [
            eventUpdate(30, {
              occurred: "2999-01-01T00:00:00.000Z",
              amount: 2n ** 60n,
            }),
            eventUpdate(31, {
              occurred: "1000-01-01T00:00:00.000Z",
              amount: -(2n ** 60n),
            }),
          ],
        },
      ],
    });
    for (const groupBy of [[], ["tag"], ["level"]]) {
      const result = expectReference(overlay, {
        version: 1,
        from: { collection: "events", as: "event" },
        select: [...eventFields(...groupBy), ...AGGREGATES],
        ...(groupBy.length === 0
          ? {}
          : {
              groupBy: groupBy.map((column) => ({ source: "event", column })),
            }),
      });
      expect(result.rows.length).toBeGreaterThan(0);
    }
  }, 120_000);

  test("an integer group keeps a number apart from the bigint it equals", async () => {
    const overlay = await baseOverlay();
    // Row 0 is in a block of small integers, as 5n is; row 4_500 is in one
    // near the safe limit.
    overlay.applyCanonical({
      transactions: [
        {
          mutationIds: [],
          operations: [
            eventUpdate(0, { amount: 5 }),
            eventUpdate(1, { amount: 5n }),
            eventUpdate(4_500, { amount: Number.MAX_SAFE_INTEGER }),
          ],
        },
      ],
    });
    const result = expectReference(overlay, {
      version: 1,
      from: { collection: "events", as: "event" },
      select: [...eventFields("amount"), countAll],
      groupBy: [{ source: "event", column: "amount" }],
    });
    const counted = (amount: unknown) =>
      result.rows.filter(([value]) => Object.is(value, amount)).length;
    expect([counted(5), counted(5n)]).toEqual([1, 1]);
  }, 120_000);

  test("a sum still refuses to mix numbers and bigints, whichever comes first", async () => {
    for (const id of [0, 4_500]) {
      const overlay = await baseOverlay();
      overlay.applyCanonical({
        transactions: [
          {
            mutationIds: [],
            operations: [eventUpdate(id, { amount: 1.5 })],
          },
        ],
      });
      const specification: QuerySpecification = {
        version: 1,
        from: { collection: "events", as: "event" },
        select: [
          {
            kind: "aggregate",
            operator: "sum",
            field: { source: "event", column: "amount" },
            as: "total",
          },
        ],
      };
      expect(() => engineOver(overlay).query(specification, OPTIONS)).toThrow(
        "sum cannot mix number and bigint values",
      );
    }
  }, 120_000);

  test("an aggregate still reads a cell whose read can throw, and refuses a timestamp sum", async () => {
    const schema: DirectColumnarSchema = {
      format: "calque-direct-columnar",
      version: 4,
      tables: [
        {
          name: "moments",
          primaryKey: ["id"],
          columns: [
            { name: "id", type: "int64", nullable: false },
            { name: "at", type: "timestamp", nullable: true },
          ],
        },
      ],
    };
    // 9.1e18 microseconds is 9.1e15 milliseconds, past 2^53: no exact Date.
    const overlay = await overlayOf(
      schema,
      new Map([
        [
          "moments",
          [
            { id: 0n, at: 0n },
            { id: 1n, at: 9_100_000_000_000_000_000n },
            { id: 2n, at: null },
          ],
        ],
      ]),
    );
    const engine = engineOver(overlay);
    const aggregate = (
      operator: "count" | "sum" | "min" | "max",
      where?: QueryPredicate,
    ): QuerySpecification => ({
      version: 1,
      from: { collection: "moments", as: "moment" },
      ...(where === undefined ? {} : { where }),
      select: [
        {
          kind: "aggregate",
          operator,
          field: { source: "moment", column: "at" },
          as: "value",
        },
      ],
    });
    for (const operator of ["count", "min", "max"] as const)
      expect(() => engine.query(aggregate(operator), OPTIONS)).toThrow(
        "timestamp is outside the exact JavaScript Date range",
      );
    // A timestamp reads as text, which a sum refuses.
    expect(() =>
      engine.query(
        aggregate("sum", {
          kind: "compare",
          operator: "eq",
          left: field("moment", "id"),
          right: literal(0n),
        }),
        OPTIONS,
      ),
    ).toThrow("sum requires a numeric column");
  });
});
