/**
 * The block filter, the encoded grouping and the UUID join against the naive
 * oracle, over one artifact that holds every encoded kind those paths read:
 * UUID, dictionary, int64, timestamp and UTF-8 cells, nullable, around every
 * edge where an encoded value stops standing for the read one. Each fixture is
 * drawn from a fixed seed, so a failure names a case that replays.
 */
import { describe, expect, test } from "vitest";

import {
  artifactQueryVector,
  dictionaryCodesAreText,
  timestampTextOrder,
  directColumnarTimestampText,
} from "./artifact.js";
import {
  inspectQueryExecution,
  validateQuerySpecification,
  type QueryOrder,
  type QueryPredicate,
  type QueryProjection,
  type QuerySpecification,
} from "./query.js";
import { referenceQuery } from "./query-reference.fixture.js";
import {
  SCHEMA,
  SCHEMAS,
  EVENT_ROWS,
  generator,
  pick,
  uuid,
  workspaceOf,
  baseTables,
  baseOverlay,
  STATES,
  overlayIn,
  OPTIONS,
  engineOver,
  expectReference,
  field,
  literal,
  OPERATORS,
  LITERALS,
  eventFields,
  countAll,
  AGGREGATES,
} from "./query-scan.fixture.js";

describe("query scan kernels", () => {
  test("the q-filter shape decides encoded rows in blocks, with the row path's counts", async () => {
    const overlay = await overlayIn("dirty");
    const rowOverlay = await overlayIn("dirty");
    const where: QueryPredicate = {
      kind: "and",
      predicates: [
        {
          kind: "or",
          predicates: [
            {
              kind: "compare",
              operator: "eq",
              left: field("event", "level"),
              right: literal("debug"),
            },
            {
              kind: "compare",
              operator: "eq",
              left: field("event", "level"),
              right: literal("fatal"),
            },
          ],
        },
        {
          kind: "or",
          predicates: [
            {
              kind: "compare",
              operator: "eq",
              left: field("event", "project"),
              right: literal(uuid(1)),
            },
            {
              kind: "compare",
              operator: "eq",
              left: field("event", "project"),
              right: literal(uuid(3)),
            },
          ],
        },
      ],
    };
    const specification: QuerySpecification = {
      version: 1,
      from: { collection: "events", as: "event" },
      where,
      select: [countAll],
    };
    const block = engineOver(overlay);
    const row = engineOver(rowOverlay, { blockFilter: false });
    expect(block.query(specification, OPTIONS)).toEqual(
      referenceQuery(overlay, SCHEMAS, specification),
    );
    row.query(specification, OPTIONS);
    const inspection = inspectQueryExecution(block);
    expect(inspection).toEqual(inspectQueryExecution(row));
    // Encoded decisions happened, and only overlay rows were materialized.
    expect(inspection.encodedComparisons).toBeGreaterThan(EVENT_ROWS);
    expect(inspection.materializedComparisons).toBeGreaterThan(0);
    expect(inspection.materializedComparisons).toBeLessThan(40);
  }, 120_000);

  test("reserves the block filter's scratch for a block and each level of and/or nesting", async () => {
    const engine = engineOver(await baseOverlay());
    const leaf: QueryPredicate = {
      kind: "compare",
      operator: "eq",
      left: field("event", "level"),
      right: literal("debug"),
    };
    const nested = (levels: number): QueryPredicate =>
      levels === 0
        ? leaf
        : {
            kind: levels % 2 === 1 ? "or" : "and",
            predicates: [nested(levels - 1)],
          };
    // The estimate less the specification's own text, which the nesting
    // lengthens too.
    const beyondText = (where?: QueryPredicate) => {
      const specification: QuerySpecification = {
        version: 1,
        from: { collection: "events", as: "event" },
        ...(where === undefined ? {} : { where }),
        select: [countAll],
      };
      const text = new TextEncoder().encode(
        JSON.stringify(validateQuerySpecification(specification)),
      ).byteLength;
      return engine.estimateReservationBytes(specification, OPTIONS) - text;
    };
    // A filter reserves an output byte per row of a 1,024-row block, beside
    // its 4-byte address per event.
    expect(beyondText(leaf) - beyondText()).toBe(1_024 + EVENT_ROWS * 4);
    // A selection and an output byte per row of a 1,024-row block, per level.
    for (const levels of [1, 2, 3])
      expect(beyondText(nested(levels)) - beyondText(leaf)).toBe(
        levels * 5 * 1_024,
      );
  });

  test("a column's block comparison exists exactly when its row one does, and agrees with it", async () => {
    const table = (await workspaceOf(SCHEMA, baseTables())).table("events");
    const rows = table.rows;
    // Every row, then two addresses past them, which only an overlay holds.
    const addresses = Uint32Array.from(
      { length: rows + 2 },
      (_unused, index) => (index < rows ? index : rows + index),
    );
    const selection = Uint32Array.from(addresses, (_unused, index) => index);
    const out = new Uint8Array(addresses.length);
    const mismatches: unknown[] = [];
    for (const [column, literals] of Object.entries(LITERALS)) {
      const vector = artifactQueryVector(table.column(column));
      if (!vector) continue;
      for (const operator of OPERATORS)
        for (const value of literals) {
          const row = vector.prepareCompare(operator, value);
          const block = vector.prepareBlockCompare(operator, value);
          if (!row || !block) {
            if (row || block)
              mismatches.push({ column, operator, value, row: !!row });
            continue;
          }
          const outside = block(addresses, selection, addresses.length, out);
          const expected = Array.from(addresses, (address) =>
            address >= rows ? 1 : row(address) ? 2 : 0,
          );
          if (outside !== 2 || out.join() !== expected.join())
            mismatches.push({ column, operator, value, outside });
        }
    }
    expect(mismatches).toEqual([]);
  }, 120_000);

  const JOINS = [
    ["event", "project", "project", "id", "projects"],
    ["event", "project", "project", "owner", "projects"],
    ["event", "level", "project", "name", "projects"],
  ] as const;

  test("joins on UUID keys as the oracle does, in either direction", async () => {
    for (const state of STATES) {
      const overlay = await overlayIn(state);
      for (const [leftSource, leftColumn, , rightColumn] of JOINS)
        for (const kind of ["inner", "left"] as const) {
          const joins = [
            {
              kind,
              collection: "projects",
              as: "project",
              on: {
                left: { source: leftSource, column: leftColumn },
                right: { source: "project", column: rightColumn },
              },
            },
          ];
          const select: QueryProjection[] = [
            ...eventFields("id", "project", "level"),
            {
              kind: "field",
              field: { source: "project", column: "name" },
              as: "name",
            },
          ];
          expectReference(overlay, {
            version: 1,
            from: { collection: "events", as: "event" },
            joins,
            select,
          });
          expectReference(overlay, {
            version: 1,
            from: { collection: "events", as: "event" },
            joins,
            select: [countAll],
          });
          expectReference(overlay, {
            version: 1,
            from: { collection: "events", as: "event" },
            joins,
            select: [
              {
                kind: "field",
                field: { source: "project", column: "name" },
                as: "name",
              },
              ...AGGREGATES,
            ],
            groupBy: [{ source: "project", column: "name" }],
          });
        }
      // Projects on the left: the right index is the events' UUID column,
      // with its nulls, its changed cells and, in the odd state, values that
      // are not 16 bytes.
      for (const kind of ["inner", "left"] as const)
        expectReference(overlay, {
          version: 1,
          from: { collection: "projects", as: "project" },
          joins: [
            {
              kind,
              collection: "events",
              as: "event",
              on: {
                left: { source: "project", column: "id" },
                right: { source: "event", column: "project" },
              },
            },
          ],
          select: [
            {
              kind: "field",
              field: { source: "project", column: "name" },
              as: "name",
            },
            ...eventFields("id", "level"),
          ],
          where: {
            kind: "compare",
            operator: "neq",
            left: field("project", "name"),
            right: literal("project-1"),
          },
        });
      // A project with no owner matches no event, so its row has no event
      // address to group or aggregate by.
      expectReference(overlay, {
        version: 1,
        from: { collection: "projects", as: "project" },
        joins: [
          {
            kind: "left",
            collection: "events",
            as: "event",
            on: {
              left: { source: "project", column: "owner" },
              right: { source: "event", column: "project" },
            },
          },
        ],
        select: [...eventFields("level", "project"), ...AGGREGATES],
        groupBy: [
          { source: "event", column: "level" },
          { source: "event", column: "project" },
        ],
      });
    }
  }, 120_000);

  test("orders on UUID cells as the oracle does, as a key and as a tie-breaker", async () => {
    const owner: QueryProjection = {
      kind: "field",
      field: { source: "project", column: "owner" },
      as: "owner",
    };
    const projectId: QueryProjection = {
      kind: "field",
      field: { source: "project", column: "id" },
      as: "project_id",
    };
    // Forty projects over 5,000 events: a UUID key leaves long runs of ties,
    // and a UUID tie-breaker meets only the rows the keys before it tie.
    const shapes: readonly (readonly [
      Omit<QuerySpecification, "orderBy">,
      readonly (readonly QueryOrder[])[],
    ])[] = [
      [
        {
          version: 1,
          from: { collection: "events", as: "event" },
          select: eventFields("id", "project", "tag", "level"),
        },
        [
          [{ by: "project", direction: "asc" }],
          [{ by: "project", direction: "desc", nulls: "first" }],
          [
            { by: "tag", direction: "asc" },
            { by: "project", direction: "desc", nulls: "first" },
          ],
          [
            { by: "level", direction: "desc", nulls: "last" },
            { by: "project", direction: "asc", nulls: "first" },
            { by: "id", direction: "desc" },
          ],
        ],
      ],
      [
        {
          version: 1,
          from: { collection: "events", as: "event" },
          joins: [
            {
              kind: "left",
              collection: "projects",
              as: "project",
              on: {
                left: { source: "event", column: "project" },
                right: { source: "project", column: "id" },
              },
            },
          ],
          select: [...eventFields("id", "level"), owner],
        },
        [
          [
            { by: "owner", direction: "desc", nulls: "last" },
            { by: "id", direction: "asc" },
          ],
          [
            { by: "level", direction: "asc" },
            { by: "owner", direction: "asc", nulls: "first" },
          ],
        ],
      ],
      [
        {
          version: 1,
          from: { collection: "projects", as: "project" },
          select: [owner, projectId],
        },
        [
          [
            { by: "owner", direction: "asc" },
            { by: "project_id", direction: "desc" },
          ],
        ],
      ],
    ];
    for (const state of STATES) {
      const overlay = await overlayIn(state);
      const engine = engineOver(overlay);
      for (const [shape, orders] of shapes)
        for (const orderBy of orders) {
          const unlimited: QuerySpecification = { ...shape, orderBy };
          // The oracle cuts its full result to the limit.
          const reference = referenceQuery(overlay, SCHEMAS, unlimited);
          for (const limit of [1, 26, 64, 65, undefined])
            expect(
              engine.query(
                limit === undefined ? unlimited : { ...unlimited, limit },
                OPTIONS,
              ),
            ).toEqual({ ...reference, rows: reference.rows.slice(0, limit) });
        }
    }
  }, 120_000);
});

describe("timestamp text order", () => {
  test("orders safe microseconds as their text does, and is equal only for equal text", () => {
    const random = generator(0x7157);
    const edges = [
      -Number.MAX_SAFE_INTEGER,
      -Number.MAX_SAFE_INTEGER + 999,
      -1_001,
      -1_000,
      -999,
      -1,
      0,
      1,
      999,
      1_000,
      1_001,
      Number.MAX_SAFE_INTEGER - 1_000,
      Number.MAX_SAFE_INTEGER - 991,
      Number.MAX_SAFE_INTEGER,
    ];
    const samples = [
      ...edges,
      ...Array.from({ length: 400 }, () => {
        const millis = Math.floor((random() - 0.5) * 2 * 9e12);
        return millis * 1_000 + pick(random, [0, 0, 1, 500, 998, 999]);
      }),
    ];
    const text = (micros: number) =>
      directColumnarTimestampText(BigInt(micros));
    for (const left of samples)
      for (const right of samples) {
        const byText =
          text(left) < text(right) ? -1 : text(left) > text(right) ? 1 : 0;
        expect(
          Math.sign(timestampTextOrder(left) - timestampTextOrder(right)),
          `${text(left)} vs ${text(right)}`,
        ).toBe(byText);
      }
  });
});

describe("dictionary codes as text", () => {
  /** A dictionary part from its entries' raw bytes, in the given order. */
  function dictionary(...entries: readonly (string | readonly number[])[]) {
    const raw = entries.map((entry) =>
      typeof entry === "string"
        ? new TextEncoder().encode(entry)
        : Uint8Array.from(entry),
    );
    const offsets = new Uint32Array(raw.length + 1);
    raw.forEach((entry, code) => {
      offsets[code + 1] = (offsets[code] ?? 0) + entry.length;
    });
    const bytes = new Uint8Array(offsets[raw.length] ?? 0);
    raw.forEach((entry, code) => bytes.set(entry, offsets[code] ?? 0));
    return [offsets, bytes] as const;
  }

  test("holds only when every entry reads back as a text no other entry reads as", () => {
    const cases: readonly [string, boolean, ReturnType<typeof dictionary>][] = [
      ["no entries", true, dictionary()],
      ["one empty entry and no data", true, dictionary("")],
      [
        "text",
        true,
        dictionary("", "a", "x\uFEFF", "é", "\uFEC0", "\u{1F600}"),
      ],
      ["a byte-order mark alone", false, dictionary("", "\uFEFF")],
      ["a byte-order mark before text", false, dictionary("x", "\uFEFFx")],
      ["half a byte-order mark", false, dictionary([0xef, 0xbb])],
      // The data decodes whole, "aé", but neither entry decodes alone.
      ["a code point split", false, dictionary([0x61, 0xc3], [0xa9])],
      ["a byte no text holds", false, dictionary("a", [0xff])],
    ];
    for (const [name, expected, [offsets, bytes]] of cases)
      expect(dictionaryCodesAreText(offsets, bytes), name).toBe(expected);
  });
});
