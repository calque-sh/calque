import { describe, expect, test } from "vitest";
import {
  OverlayEngine,
  type OverlayBase,
  type OverlayCollectionSchema,
  type SqlValue,
} from "./overlay.js";
import {
  ColumnarQueryEngine,
  type QueryProjection,
  type QuerySpecification,
} from "./query.js";

/**
 * Join cardinality sizing. A reservation that multiplies both sides of a join
 * sizes `issues INNER JOIN projects LEFT JOIN users` at its Cartesian product
 * and refuses it at every tier. These tests pin that query's shape and the
 * refusals it produced.
 */

/** The aggregate query pool the refused runs had. */
const REFUSED_RUNS_POOL_BYTES = 268_435_456;

/** The refusals those runs reported, verbatim. */
const RECORDED_REFUSALS: Readonly<Record<TierName, string>> = Object.freeze({
  "100k":
    "CAPACITY_EXCEEDED: query structures require 249616578531 bytes; pool is 268435456",
  "500k":
    "CAPACITY_EXCEEDED: query structures require 31200208929250 bytes; pool is 268435456",
  "1m": "CAPACITY_EXCEEDED: query structures require 249600809367523 bytes; pool is 268435456",
});

type TierName = "100k" | "500k" | "1m";

/**
 * `scripts/bench-fixture.ts` at full profile, and the two smaller
 * tiers as fractions of it. The 1m tier's seven tables total exactly
 * 1,000,000 rows.
 */
const TIERS: Readonly<
  Record<TierName, { issues: number; projects: number; users: number }>
> = Object.freeze({
  "100k": { issues: 10_000, projects: 100, users: 800 },
  "500k": { issues: 50_000, projects: 500, users: 4_000 },
  "1m": { issues: 100_000, projects: 1_000, users: 8_000 },
});

interface Collection {
  readonly name: string;
  readonly columns: readonly string[];
  readonly rows: number;
  readonly primaryKey?: readonly string[];
  readonly value?: (column: number, row: number) => SqlValue;
}

function base(collection: Collection): OverlayBase {
  const read =
    collection.value ??
    ((column: number, row: number): SqlValue =>
      column === 0 ? BigInt(row) : null);
  return {
    rows: collection.rows,
    lookupKey: (key) =>
      key.length === 1 && typeof key[0] === "bigint" ? Number(key[0]) : -1,
    valueAt: read,
  };
}

function overlayOf(collections: readonly Collection[]): OverlayEngine {
  return new OverlayEngine(
    collections.map((collection): OverlayCollectionSchema => {
      const schema = {
        name: collection.name,
        columns: collection.columns,
        base: base(collection),
      };
      return collection.primaryKey === undefined
        ? schema
        : { ...schema, primaryKey: collection.primaryKey };
    }),
  );
}

function engine(
  overlay: OverlayEngine,
  capacityBytes = REFUSED_RUNS_POOL_BYTES,
): ColumnarQueryEngine {
  return new ColumnarQueryEngine(overlay, { capacityBytes });
}

/**
 * The fixture's issues, projects and users at one tier, with its own key
 * and foreign-key shape: `issues.project_id` references `projects.id`, and
 * `issues.assignee_id` references `users.id` on 70% of rows.
 */
function tierCollections(tier: TierName): readonly Collection[] {
  const { issues, projects, users } = TIERS[tier];
  return [
    {
      name: "issues",
      columns: ["id", "project_id", "assignee_id", "status", "priority"],
      rows: issues,
      primaryKey: ["id"],
      value: (column, row) => {
        if (column === 0) return BigInt(row);
        if (column === 1) return BigInt(row % projects);
        if (column === 2) return row % 1000 >= 700 ? null : BigInt(row % users);
        if (column === 3) return row % 100 < 18 ? "unresolved" : "resolved";
        return BigInt(1 + (row % 5));
      },
    },
    {
      name: "projects",
      columns: ["id", "slug", "name"],
      rows: projects,
      primaryKey: ["id"],
      value: (column, row) => (column === 0 ? BigInt(row) : `p${String(row)}`),
    },
    {
      name: "users",
      columns: ["id", "display_name"],
      rows: users,
      primaryKey: ["id"],
      value: (column, row) => (column === 0 ? BigInt(row) : `u${String(row)}`),
    },
  ];
}

const CANDIDATE_WHERE = {
  kind: "or",
  predicates: [
    {
      kind: "compare",
      operator: "eq",
      left: { kind: "field", field: { source: "issue", column: "status" } },
      right: { kind: "literal", value: "unresolved" },
    },
    {
      kind: "compare",
      operator: "eq",
      left: { kind: "field", field: { source: "issue", column: "status" } },
      right: { kind: "literal", value: "resolved" },
    },
  ],
} as const;

/** `issues INNER JOIN projects LEFT JOIN users`. */
function candidateQuery(
  select: readonly QueryProjection[],
  orderBy?: QuerySpecification["orderBy"],
  limit?: number,
): QuerySpecification {
  return {
    version: 1,
    from: { collection: "issues", as: "issue" },
    joins: [
      {
        kind: "inner",
        collection: "projects",
        as: "project",
        on: {
          left: { source: "issue", column: "project_id" },
          right: { source: "project", column: "id" },
        },
      },
      {
        kind: "left",
        collection: "users",
        as: "assignee",
        on: {
          left: { source: "issue", column: "assignee_id" },
          right: { source: "assignee", column: "id" },
        },
      },
    ],
    where: CANDIDATE_WHERE,
    select,
    ...(orderBy === undefined ? {} : { orderBy }),
    ...(limit === undefined ? {} : { limit }),
  };
}

const CANDIDATE_COUNT = candidateQuery([
  { kind: "aggregate", operator: "count", as: "count" },
]);

const CANDIDATE_ROWS = candidateQuery(
  [
    { kind: "field", field: { source: "issue", column: "id" }, as: "id" },
    {
      kind: "field",
      field: { source: "issue", column: "priority" },
      as: "priority",
    },
    {
      kind: "field",
      field: { source: "issue", column: "status" },
      as: "status",
    },
    {
      kind: "field",
      field: { source: "project", column: "slug" },
      as: "slug",
    },
    {
      kind: "field",
      field: { source: "project", column: "name" },
      as: "name",
    },
    {
      kind: "field",
      field: { source: "assignee", column: "display_name" },
      as: "display_name",
    },
  ],
  [
    { by: "priority", direction: "desc" },
    { by: "id", direction: "asc" },
  ],
  26,
);

describe("join cardinality sizing", () => {
  /**
   * The stated factor. A key equijoin cannot exceed its referencing side, so
   * the true relation cardinality of the candidate query is the issues row
   * count; the reservation is charged per relation row, and this is the
   * envelope those per-row charges must stay inside.
   */
  const BYTES_PER_TRUE_ROW = 512;

  test.each(["100k", "500k", "1m"] as const)(
    "sizes the candidate three-table join at %s against the issues count, not the Cartesian product",
    (tier) => {
      const trueRows = TIERS[tier].issues;
      const sizing = engine(overlayOf(tierCollections(tier)));
      // The same three collections with no rows: what the reservation costs
      // before any relation row is charged.
      const floorSizing = engine(
        overlayOf(
          tierCollections(tier).map((collection) => ({
            ...collection,
            rows: 0,
          })),
        ),
      );
      // The least a joined row can cost. The count keeps three sources'
      // address and filter vectors per row (36 bytes) and a single group; the
      // row plan also projects and orders every row.
      for (const [specification, minimumRowBytes] of [
        [CANDIDATE_COUNT, 32],
        [CANDIDATE_ROWS, 64],
      ] as const) {
        const required = sizing.estimateReservationBytes(specification);
        const floor = floorSizing.estimateReservationBytes(specification);
        // Within the stated factor of the true cardinality.
        expect(required - floor).toBeLessThanOrEqual(
          trueRows * BYTES_PER_TRUE_ROW,
        );
        // And not slack to the point of meaninglessness: it still scales with
        // the rows the join can produce.
        expect(required - floor).toBeGreaterThan(trueRows * minimumRowBytes);
        // The pool the refused runs had serves it.
        expect(required).toBeLessThan(REFUSED_RUNS_POOL_BYTES);
      }
    },
  );

  test.each(["100k", "500k", "1m"] as const)(
    "reaches a result at %s in the refused runs' pool instead of refusing",
    (tier) => {
      const overlay = overlayOf(tierCollections(tier));
      const count = engine(overlay).query(CANDIDATE_COUNT);
      // Every fixture row has one of the two statuses the vector selects, and
      // each issue joins exactly one project and at most one assignee, so the
      // join neither drops nor multiplies rows.
      expect(count.rows[0]?.[0]).toBe(TIERS[tier].issues);
      const rows = engine(overlay).query(CANDIDATE_ROWS);
      expect(rows.rows).toHaveLength(26);
    },
  );

  test.each(["100k", "500k", "1m"] as const)(
    "never reproduces the %s recorded refusal",
    (tier) => {
      const sizing = engine(overlayOf(tierCollections(tier)));
      let message: string | undefined;
      try {
        sizing.query(CANDIDATE_COUNT);
      } catch (error) {
        message = error instanceof Error ? error.message : String(error);
      }
      expect(message).toBeUndefined();
      // The recorded refusal, byte for byte.
      const refusal = RECORDED_REFUSALS[tier];
      const fabricated = Number(
        /require (\d+) bytes/u.exec(refusal)?.[1] ?? "0",
      );
      expect(fabricated).toBeGreaterThan(REFUSED_RUNS_POOL_BYTES);
      // The reservation must be smaller than the recorded figure by at least
      // the factor that figure overshot the pool.
      expect(
        sizing.estimateReservationBytes(CANDIDATE_COUNT) *
          Math.floor(fabricated / REFUSED_RUNS_POOL_BYTES),
      ).toBeLessThan(fabricated);
    },
  );

  test("bounds a primary-key equality join by the referencing side", () => {
    const specification: QuerySpecification = {
      version: 1,
      from: { collection: "issues", as: "issue" },
      joins: [
        {
          kind: "inner",
          collection: "projects",
          as: "project",
          on: {
            left: { source: "issue", column: "project_id" },
            right: { source: "project", column: "id" },
          },
        },
      ],
      select: [
        { kind: "field", field: { source: "issue", column: "id" }, as: "id" },
      ],
    };
    const issues = 4_000;
    const withFew = engine(
      overlayOf([
        {
          name: "issues",
          columns: ["id", "project_id"],
          rows: issues,
          primaryKey: ["id"],
          value: (column, row) => BigInt(column === 0 ? row : row % 50),
        },
        {
          name: "projects",
          columns: ["id"],
          rows: 50,
          primaryKey: ["id"],
        },
      ]),
    ).estimateReservationBytes(specification);
    const withMany = engine(
      overlayOf([
        {
          name: "issues",
          columns: ["id", "project_id"],
          rows: issues,
          primaryKey: ["id"],
          value: (column, row) => BigInt(column === 0 ? row : row % 50),
        },
        {
          name: "projects",
          columns: ["id"],
          rows: 5_000,
          primaryKey: ["id"],
        },
      ]),
    ).estimateReservationBytes(specification);
    // Growing the keyed side by 4,950 rows may only add that side's own hash
    // index, 96 bytes a row. If the fold multiplied, this difference would be
    // 4,000 x 4,950 relation rows instead.
    expect(withMany - withFew).toBe(4_950 * 96);
  });

  test("never estimates a left join below its left side", () => {
    const left = 3_000;
    const collections: readonly Collection[] = [
      {
        name: "issues",
        columns: ["id", "assignee_id"],
        rows: left,
        primaryKey: ["id"],
        value: (column, row) => (column === 0 ? BigInt(row) : null),
      },
      // Empty: every left row still emits one output row with a null right.
      { name: "users", columns: ["id"], rows: 0, primaryKey: ["id"] },
    ];
    const specification: QuerySpecification = {
      version: 1,
      from: { collection: "issues", as: "issue" },
      joins: [
        {
          kind: "left",
          collection: "users",
          as: "assignee",
          on: {
            left: { source: "issue", column: "assignee_id" },
            right: { source: "assignee", column: "id" },
          },
        },
      ],
      select: [
        { kind: "field", field: { source: "issue", column: "id" }, as: "id" },
      ],
    };
    const overlay = overlayOf(collections);
    const options = { maxRows: left } as const;
    const required = engine(overlay).estimateReservationBytes(
      specification,
      options,
    );
    // A pool of exactly the estimate must serve it. An estimate that fell to
    // the empty right side would leave the 3,000 output rows unreserved, and
    // the growth this engine takes before allocating them would not fit.
    const result = engine(overlay, required).query(specification, options);
    expect(result.rows).toHaveLength(left);
  });

  /**
   * Two sides with a composite key over the joined column, every row sharing
   * one join value: nothing in the mount bounds this join, so its cardinality
   * is only knowable by counting.
   */
  function manyToManyCollections(side: number): readonly Collection[] {
    return [
      {
        name: "issue_tags",
        columns: ["issue_id", "tag"],
        rows: side,
        primaryKey: ["issue_id", "tag"],
        value: () => 1n,
      },
      {
        name: "memberships",
        columns: ["user_id", "role"],
        rows: side,
        primaryKey: ["user_id", "role"],
        value: () => 1n,
      },
    ];
  }

  const MANY_TO_MANY: QuerySpecification = {
    version: 1,
    from: { collection: "issue_tags", as: "tag" },
    joins: [
      {
        kind: "inner",
        collection: "memberships",
        as: "member",
        on: {
          left: { source: "tag", column: "issue_id" },
          right: { source: "member", column: "user_id" },
        },
      },
    ],
    select: [{ kind: "aggregate", operator: "count", as: "count" }],
  };

  test("refuses an unbounded many-to-many on measured rows, never a fabricated product", () => {
    const side = 2_000;
    const overlay = overlayOf(manyToManyCollections(side));
    // Large enough for the plan's fixed cost and both hash indexes, far too
    // small for four million relation rows.
    const refused = engine(overlay, 32 * 1024 * 1024);
    let thrown: unknown;
    try {
      refused.query(MANY_TO_MANY);
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toMatchObject({ code: "CAPACITY_EXCEEDED" });
    const message = thrown instanceof Error ? thrown.message : "";
    // Names the join and its inputs, and reports rows it counted.
    expect(message).toContain("inner join member (memberships)");
    expect(message).toContain("on tag.issue_id = member.user_id");
    expect(message).toContain(
      `produced ${side * side} rows from ${side} left rows and ${side} right rows`,
    );
    // Most of what the refusal reports was charged after the engine counted
    // the rows, not reserved before it.
    const measured =
      thrown instanceof Object && "requiredBytes" in thrown
        ? (thrown.requiredBytes as number)
        : 0;
    const estimated = refused.estimateReservationBytes(MANY_TO_MANY);
    expect(measured - estimated).toBeGreaterThan(estimated * 5);
  });

  test("serves the same many-to-many when the pool can actually hold it", () => {
    const side = 300;
    const overlay = overlayOf(manyToManyCollections(side));
    const result = engine(overlay, 64 * 1024 * 1024).query(MANY_TO_MANY);
    expect(result.rows[0]?.[0]).toBe(side * side);
  });
});

/**
 * Group sizing. An aggregate plan's projected rows, group entries and ordering
 * slots are charged per group (`groupRowBytes`), not per relation row.
 */
describe("group sizing", () => {
  const EVENTS = 800_000;
  const PROJECTS = 1_000;
  const LEVELS = ["debug", "info", "notice", "warning", "error"] as const;

  function events(rows: number): Collection {
    return {
      name: "events",
      columns: ["id", "project_id", "level", "occurred_at"],
      rows,
      primaryKey: ["id"],
      value: (column, row) => {
        if (column === 0) return BigInt(row);
        if (column === 1) return BigInt(row % PROJECTS);
        if (column === 2) return LEVELS[row % LEVELS.length] ?? null;
        return BigInt(row);
      },
    };
  }

  /** Events grouped by project and level. */
  const EVENT_GROUPS: QuerySpecification = {
    version: 1,
    from: { collection: "events", as: "event" },
    select: [
      {
        kind: "field",
        field: { source: "event", column: "project_id" },
        as: "project_id",
      },
      {
        kind: "field",
        field: { source: "event", column: "level" },
        as: "level",
      },
      { kind: "aggregate", operator: "count", as: "count" },
      {
        kind: "aggregate",
        operator: "max",
        field: { source: "event", column: "occurred_at" },
        as: "maximum",
      },
    ],
    groupBy: [
      { source: "event", column: "project_id" },
      { source: "event", column: "level" },
    ],
    orderBy: [
      { by: "project_id", direction: "asc" },
      { by: "level", direction: "asc" },
    ],
  };

  test("groups the 1m tier's events in the refused runs' pool", () => {
    const result = engine(overlayOf([events(EVENTS)])).query(EVENT_GROUPS, {
      maxRows: 10_000,
    });
    // project_id and level cycle together: row % 1,000 and row % 5 pair each
    // project with one level, so there is one group per project.
    expect(result.rows).toHaveLength(PROJECTS);
    expect(result.rows[0]?.[2]).toBe(EVENTS / PROJECTS);
  });

  test("reserves a grouped plan's rows without its groups", () => {
    // Two tables that differ only in row count reserve the same bytes: what
    // grows with the relation is the groups it turns out to hold, and those
    // are charged as they are counted.
    const small = engine(overlayOf([events(1_000)]));
    const large = engine(overlayOf([events(EVENTS)]));
    expect(large.estimateReservationBytes(EVENT_GROUPS)).toBe(
      small.estimateReservationBytes(EVENT_GROUPS),
    );
  });

  test("reserves a global aggregate exactly", () => {
    const overlay = overlayOf([events(10_000)]);
    const count: QuerySpecification = {
      version: 1,
      from: { collection: "events", as: "event" },
      select: [{ kind: "aggregate", operator: "count", as: "count" }],
    };
    const required = engine(overlay).estimateReservationBytes(count);
    expect(engine(overlay, required).query(count).rows[0]?.[0]).toBe(10_000);
    expect(() => engine(overlay, required - 1).query(count)).toThrowError(
      expect.objectContaining({ code: "CAPACITY_EXCEEDED" }),
    );
  });

  test("charges each group before it creates it, the first one included", () => {
    // One row, so one group: a grouped plan reserves none, and the pool the
    // estimate names holds no room for it.
    const overlay = overlayOf([events(1)]);
    const required = engine(overlay).estimateReservationBytes(EVENT_GROUPS);
    expect(() => engine(overlay, required).query(EVENT_GROUPS)).toThrowError(
      expect.objectContaining({
        code: "CAPACITY_EXCEEDED",
        message: expect.stringMatching(/grouping produced 1 groups/u),
      }),
    );
    expect(
      engine(overlay, required + 1024 * 1024).query(EVENT_GROUPS).rows,
    ).toHaveLength(1);
  });

  test("refuses groups that outgrow the pool, naming how many it counted", () => {
    // Every row its own group.
    const byId: QuerySpecification = {
      version: 1,
      from: { collection: "events", as: "event" },
      select: [
        { kind: "field", field: { source: "event", column: "id" }, as: "id" },
        { kind: "aggregate", operator: "count", as: "count" },
      ],
      groupBy: [{ source: "event", column: "id" }],
    };
    const overlay = overlayOf([events(100_000)]);
    const required = engine(overlay).estimateReservationBytes(byId);
    const pool = required + 1024 * 1024;
    let thrown: unknown;
    try {
      engine(overlay, pool).query(byId);
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toMatchObject({ code: "CAPACITY_EXCEEDED" });
    const message = thrown instanceof Error ? thrown.message : "";
    expect(message).toMatch(/grouping produced \d+ groups/u);
    const counted = Number(/produced (\d+) groups/u.exec(message)?.[1] ?? "0");
    // Refused while counting, before the 100,000th group was created.
    expect(counted).toBeGreaterThan(1);
    expect(counted).toBeLessThan(100_000);
  });
});
