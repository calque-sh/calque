import { describe, expect, test } from "vitest";

import { openDirectColumnarArtifact } from "./artifact.js";
import { encodeWorkspace, WORKSPACE_CONTRACT } from "./artifact.fixture.js";
import {
  OverlayEngine,
  artifactBase,
  type KeyPart,
  type OverlayBase,
  type SqlValue,
} from "./overlay.js";
import {
  ColumnarQueryEngine,
  configureQueryMeasurement,
  inspectQueryExecution,
  type QueryExpression,
  type QueryJoin,
  type QueryOrder,
  type QueryPredicate,
  type QueryResult,
  type QuerySpecification,
} from "./query.js";
import {
  referenceQuery,
  type QueryReferenceSchema,
} from "./query-reference.fixture.js";

type FixtureRow = Readonly<Record<string, unknown>>;

const LEVELS = ["debug", "info", "warning", "error", "fatal"] as const;
const EVENT_ROWS = 2_500;
const ISSUE_ROWS = 35;
const HOUR = 3_600_000;

function contractColumns(tableName: string): readonly string[] {
  const table = WORKSPACE_CONTRACT.tables.find(
    (candidate) => candidate.name === tableName,
  );
  if (!table) throw new Error(`${tableName} fixture schema is unavailable`);
  return table.columns.map((column) => column.name);
}

const SCHEMAS: readonly QueryReferenceSchema[] = [
  "events",
  "issues",
  "users",
].map((name) => ({ name, columns: contractColumns(name) }));

function column(tableName: string, name: string): number {
  const index = contractColumns(tableName).indexOf(name);
  if (index < 0) throw new Error(`${tableName}.${name} is not a column`);
  return index;
}

function eventRow(id: number): FixtureRow {
  return {
    id: BigInt(id),
    organization_id: "organization-1",
    project_id: `project-${String(id % 7)}`,
    // Every fifth event has no issue, and issues 35 to 39 do not exist, so an
    // inner join drops rows a left join keeps.
    issue_id: id % 5 === 0 ? null : `issue-${String(id % 40)}`,
    level: LEVELS[(id * 7) % LEVELS.length],
    message: `message-${String(id % 13)}`,
    occurred_at: new Date(
      Date.UTC(2026, 7, 1) + (id % 97) * HOUR,
    ).toISOString(),
    payload: { id },
  };
}

function issueRow(id: number): FixtureRow {
  return {
    id: `issue-${String(id)}`,
    organization_id: "organization-1",
    project_id: `project-${String(id % 7)}`,
    // Users 6 to 8 do not exist.
    assignee_id: id % 3 === 0 ? null : `user-${String(id % 9)}`,
    status: ["open", "resolved", "ignored"][id % 3],
    priority: id % 4,
    title: `issue ${String(id)}`,
    event_count: id,
    updated_at: new Date(Date.UTC(2026, 7, 1) + (id % 11) * HOUR).toISOString(),
  };
}

function userRow(id: number): FixtureRow {
  return {
    id: `user-${String(id)}`,
    display_name: `User ${String(id)}`,
    email: `user-${String(id)}@example.test`,
    active: id % 2 === 0,
  };
}

const primaryKey = (key: string): Uint8Array => new TextEncoder().encode(key);

function eventValues(id: number, level: string): readonly SqlValue[] {
  const row: FixtureRow = { ...eventRow(id), level, payload: null };
  return contractColumns("events").map((name) => row[name] as SqlValue);
}

/**
 * Canonical and pending state over the artifact: changed level and project
 * cells on rows whose other predicate columns are clean, a changed issue, a
 * canonical and a pending delete, and a canonical and a pending insert. An
 * encoded decision can only be undefined on these rows.
 */
function applyOverlayState(overlay: OverlayEngine): void {
  const level = column("events", "level");
  const project = column("events", "project_id");
  const issue = column("events", "issue_id");
  const update = (
    id: number,
    changed: readonly { column: number; value: SqlValue }[],
  ) => ({
    kind: "update" as const,
    collection: "events",
    primaryKey: primaryKey(String(id)),
    key: [BigInt(id)],
    changed,
  });
  overlay.applyCanonical({
    transactions: [
      {
        mutationIds: [],
        operations: [
          update(1, [{ column: level, value: "fatal" }]),
          update(2, [{ column: level, value: "debug" }]),
          update(4, [{ column: level, value: "debug" }]),
          update(5, [{ column: level, value: "fatal" }]),
          update(8, [{ column: project, value: "project-4" }]),
          update(10, [{ column: issue, value: "issue-12" }]),
          update(12, [{ column: issue, value: null }]),
          {
            kind: "delete",
            collection: "events",
            primaryKey: primaryKey("15"),
            key: [15n],
          },
          {
            kind: "insert",
            collection: "events",
            primaryKey: primaryKey("90000"),
            key: [90_000n],
            values: eventValues(90_000, "debug"),
          },
        ],
      },
    ],
  });
  overlay.applyOptimistic({
    mutationId: "query-kernel-pending",
    actions: [
      {
        kind: "patch",
        collection: "events",
        primaryKey: primaryKey("22"),
        key: [22n],
        changes: [{ column: level, value: "fatal", mode: "set" }],
      },
      {
        kind: "delete",
        collection: "events",
        primaryKey: primaryKey("23"),
        key: [23n],
      },
      {
        kind: "insert",
        collection: "events",
        primaryKey: primaryKey("90001"),
        key: [90_001n],
        values: eventValues(90_001, "fatal"),
      },
    ],
  });
}

async function physicalOverlay(dirty: boolean): Promise<OverlayEngine> {
  const encoded = encodeWorkspace(
    new Map<string, readonly FixtureRow[]>([
      ["events", Array.from({ length: EVENT_ROWS }, (_u, id) => eventRow(id))],
      ["issues", Array.from({ length: ISSUE_ROWS }, (_u, id) => issueRow(id))],
      ["comments", []],
      ["issue_tags", []],
      ["users", Array.from({ length: 6 }, (_u, id) => userRow(id))],
      ["projects", []],
      ["memberships", []],
    ]),
  );
  const workspace = await openDirectColumnarArtifact(
    encoded.bytes.slice().buffer,
    {
      byteLength: encoded.byteLength,
      digest: encoded.digest,
      identity: encoded.identity,
      schema: WORKSPACE_CONTRACT,
    },
  );
  const overlay = new OverlayEngine(
    SCHEMAS.map(({ name, columns }) => ({
      name,
      columns,
      base: artifactBase(workspace.table(name), columns),
    })),
  );
  if (dirty) applyOverlayState(overlay);
  return overlay;
}

const field = (source: string, name: string): QueryExpression => ({
  kind: "field",
  field: { source, column: name },
});
const eq = (source: string, name: string, value: SqlValue): QueryPredicate => ({
  kind: "compare",
  operator: "eq",
  left: field(source, name),
  right: { kind: "literal", value },
});
const neq = (
  source: string,
  name: string,
  value: SqlValue,
): QueryPredicate => ({
  kind: "compare",
  operator: "neq",
  left: field(source, name),
  right: { kind: "literal", value },
});
const fields = (
  operator: "eq" | "neq",
  left: QueryExpression,
  right: QueryExpression,
): QueryPredicate => ({ kind: "compare", operator, left, right });
const isNull = (
  source: string,
  name: string,
  negated = false,
): QueryPredicate => ({
  kind: "is-null",
  expression: field(source, name),
  negated,
});
const and = (...predicates: QueryPredicate[]): QueryPredicate => ({
  kind: "and",
  predicates,
});
const or = (...predicates: QueryPredicate[]): QueryPredicate => ({
  kind: "or",
  predicates,
});
const not = (predicate: QueryPredicate): QueryPredicate => ({
  kind: "not",
  predicate,
});
const select = (source: string, ...names: string[]) =>
  names.map((name) => ({
    kind: "field" as const,
    field: { source, column: name },
    as: `${source}_${name}`,
  }));

function run(
  overlay: OverlayEngine,
  specification: QuerySpecification,
): { result: QueryResult; engine: ColumnarQueryEngine } {
  const engine = new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 30 });
  const result = engine.query(specification, { maxRows: 10_000 });
  expect(result).toEqual(referenceQuery(overlay, SCHEMAS, specification));
  return { result, engine };
}

/** Whether every field `predicate` reads belongs to the `event` source. */
function readsEventOnly(predicate: QueryPredicate): boolean {
  const reads = (expression: QueryExpression) =>
    expression.kind !== "field" || expression.field.source === "event";
  switch (predicate.kind) {
    case "compare":
      return reads(predicate.left) && reads(predicate.right);
    case "is-null":
      return reads(predicate.expression);
    case "not":
      return readsEventOnly(predicate.predicate);
    default:
      return predicate.predicates.every(readsEventOnly);
  }
}

/** Two dictionary disjunctions under a conjunction. */
const Q_FILTER = and(
  or(eq("event", "level", "debug"), eq("event", "level", "fatal")),
  or(
    eq("event", "project_id", "project-1"),
    eq("event", "project_id", "project-4"),
  ),
);

const PREDICATES: readonly QueryPredicate[] = [
  Q_FILTER,
  or(
    and(eq("event", "level", "error"), isNull("event", "issue_id")),
    not(neq("event", "project_id", "project-2")),
  ),
  not(
    or(
      eq("event", "level", "info"),
      and(isNull("event", "issue_id"), eq("event", "project_id", "project-0")),
    ),
  ),
  and(
    eq("event", "level", "warning"),
    isNull("event", "issue_id", true),
    or(
      eq("event", "message", "message-3"),
      eq("event", "occurred_at", "2026-08-01T05:00:00.000Z"),
    ),
  ),
  or(eq("event", "level", "absent"), eq("event", "project_id", "project-6")),
  and(eq("event", "level", "debug"), neq("event", "level", "debug")),
  or(eq("event", "level", "fatal")),
  and(not(eq("event", "level", "debug"))),
  // A literal of another type than the column never equals it.
  or(eq("event", "level", 3), neq("event", "project_id", 3n)),
  // Row 12's issue became null in the overlay, so this leaf is undecided there.
  isNull("event", "issue_id", true),
  or(isNull("event", "issue_id", true), eq("event", "level", "fatal")),
];

describe("columnar query kernel", () => {
  test("decides composed encoded predicates as the reference does", async () => {
    for (const dirty of [false, true]) {
      const overlay = await physicalOverlay(dirty);
      for (const where of PREDICATES) {
        run(overlay, {
          version: 1,
          from: { collection: "events", as: "event" },
          where,
          select: select("event", "id", "level", "project_id"),
          orderBy: [{ by: "event_id", direction: "asc" }],
        });
        run(overlay, {
          version: 1,
          from: { collection: "events", as: "event" },
          where,
          select: [{ kind: "aggregate", operator: "count", as: "count" }],
        });
      }
    }
  });

  test("reads clean cells encoded and falls back only where the overlay wrote", async () => {
    const counts = async (dirty: boolean) => {
      const { engine } = run(await physicalOverlay(dirty), {
        version: 1,
        from: { collection: "events", as: "event" },
        where: Q_FILTER,
        select: [{ kind: "aggregate", operator: "count", as: "count" }],
      });
      return inspectQueryExecution(engine);
    };
    const clean = await counts(false);
    expect(clean.rowsExamined).toBe(EVENT_ROWS);
    expect(clean.materializedComparisons).toBe(0);
    expect(clean.encodedComparisons).toBeGreaterThan(EVENT_ROWS);
    const dirty = await counts(true);
    // Rows 1, 2, 4, 5 and 22 changed a level, row 8 a project, and rows
    // 90000 and 90001 exist only in the overlay; each takes the materialized
    // path, and no clean row does.
    expect(dirty.materializedComparisons).toBeGreaterThan(0);
    expect(dirty.materializedComparisons).toBeLessThanOrEqual(8 * 4);
    expect(dirty.encodedComparisons).toBeGreaterThan(EVENT_ROWS);
  });

  test("settles a row on a clean decisive leaf beside an undecided one", async () => {
    // Only row 1's level is dirty; its project is project-1.
    const overlay = await physicalOverlay(false);
    overlay.applyCanonical({
      transactions: [
        {
          mutationIds: [],
          operations: [
            {
              kind: "update",
              collection: "events",
              primaryKey: primaryKey("1"),
              key: [1n],
              changed: [{ column: column("events", "level"), value: "fatal" }],
            },
          ],
        },
      ],
    });
    const materialized = (where: QueryPredicate) => {
      const { engine } = run(overlay, {
        version: 1,
        from: { collection: "events", as: "event" },
        where,
        select: select("event", "id", "level"),
        orderBy: [{ by: "event_id", direction: "asc" }],
      });
      return inspectQueryExecution(engine).materializedComparisons;
    };
    const level = eq("event", "level", "error");
    // The dirty leaf before and after the one that decides the row.
    expect(
      materialized(or(level, eq("event", "project_id", "project-1"))),
    ).toBe(0);
    expect(
      materialized(or(eq("event", "project_id", "project-1"), level)),
    ).toBe(0);
    expect(
      materialized(and(level, eq("event", "project_id", "project-2"))),
    ).toBe(0);
    expect(
      materialized(and(eq("event", "project_id", "project-2"), level)),
    ).toBe(0);
    // Nothing clean decides row 1 here, so it alone takes the fallback.
    expect(
      materialized(or(level, eq("event", "project_id", "project-2"))),
    ).toBeGreaterThan(0);
  });

  test("reads no base cell after a truncate", async () => {
    const overlay = await physicalOverlay(true);
    const insert = (id: number, level: string) => ({
      kind: "insert" as const,
      collection: "events",
      primaryKey: primaryKey(String(id)),
      key: [BigInt(id)],
      values: eventValues(id, level),
    });
    overlay.applyCanonical({
      transactions: [
        {
          mutationIds: [],
          operations: [
            { kind: "truncate", collection: "events" },
            insert(3, "debug"),
            insert(91_000, "fatal"),
          ],
        },
      ],
    });
    for (const where of PREDICATES) {
      const { result, engine } = run(overlay, {
        version: 1,
        from: { collection: "events", as: "event" },
        where,
        select: select("event", "id", "level", "project_id"),
        orderBy: [{ by: "event_id", direction: "asc" }],
      });
      expect(result.rows.length).toBeLessThanOrEqual(3);
      expect(inspectQueryExecution(engine).encodedComparisons).toBe(0);
    }
  });

  test("filters the base source before joins without changing rows or order", async () => {
    for (const dirty of [false, true]) {
      const overlay = await physicalOverlay(dirty);
      const joins = {
        inner: [
          {
            kind: "inner" as const,
            collection: "issues",
            as: "issue",
            on: {
              left: { source: "event", column: "issue_id" },
              right: { source: "issue", column: "id" },
            },
          },
        ],
        left: [
          {
            kind: "left" as const,
            collection: "issues",
            as: "issue",
            on: {
              left: { source: "event", column: "issue_id" },
              right: { source: "issue", column: "id" },
            },
          },
        ],
        chained: [
          {
            kind: "left" as const,
            collection: "issues",
            as: "issue",
            on: {
              left: { source: "event", column: "issue_id" },
              right: { source: "issue", column: "id" },
            },
          },
          {
            kind: "inner" as const,
            collection: "users",
            as: "user",
            on: {
              left: { source: "issue", column: "assignee_id" },
              right: { source: "user", column: "id" },
            },
          },
        ],
      };
      const liveEvents = Number(
        referenceQuery(overlay, SCHEMAS, {
          version: 1,
          from: { collection: "events", as: "event" },
          select: [{ kind: "aggregate", operator: "count", as: "count" }],
        }).rows[0]?.[0],
      );
      const wheres = [
        Q_FILTER,
        not(isNull("event", "issue_id")),
        isNull("event", "issue_id"),
        eq("issue", "status", "open"),
        or(eq("event", "level", "fatal"), eq("issue", "status", "open")),
        and(eq("event", "level", "debug"), isNull("issue", "id")),
        // Field against field: the joined source on either side keeps the
        // filter after the joins, and two base fields let it move first.
        fields(
          "eq",
          field("event", "project_id"),
          field("issue", "project_id"),
        ),
        fields(
          "eq",
          field("issue", "project_id"),
          field("event", "project_id"),
        ),
        fields("neq", field("event", "project_id"), field("event", "message")),
      ];
      for (const joinSet of Object.values(joins))
        for (const where of wheres) {
          const base = {
            version: 1 as const,
            from: { collection: "events", as: "event" },
            joins: joinSet,
            where,
          };
          // No order: the result order is the relation's order, which the
          // pushed-down filter must leave as it was.
          run(overlay, {
            ...base,
            select: [
              ...select("event", "id", "level"),
              ...select("issue", "id", "status"),
            ],
          });
          const { engine } = run(overlay, {
            ...base,
            select: [{ kind: "aggregate", operator: "count", as: "count" }],
          });
          // Before the joins the filter examines each live event once; after
          // them, each joined row.
          const examined = readsEventOnly(where)
            ? liveEvents
            : Number(
                referenceQuery(overlay, SCHEMAS, {
                  version: 1,
                  from: { collection: "events", as: "event" },
                  joins: joinSet,
                  select: [
                    { kind: "aggregate", operator: "count", as: "count" },
                  ],
                }).rows[0]?.[0],
              );
          expect(inspectQueryExecution(engine).rowsExamined).toBe(examined);
        }
    }
  });

  test("keeps the first rows of the full sort when a limit is below the row count", async () => {
    const overlay = await physicalOverlay(true);
    const orders = [
      // Thirteen messages over 2,500 rows: nearly every key ties, so only
      // the provenance tie-breaker orders them.
      [{ by: "event_message", direction: "asc" as const }],
      [
        { by: "event_message", direction: "desc" as const },
        { by: "event_occurred_at", direction: "asc" as const },
      ],
      [
        {
          by: "event_issue_id",
          direction: "asc" as const,
          nulls: "first" as const,
        },
      ],
      [
        {
          by: "event_issue_id",
          direction: "desc" as const,
          nulls: "last" as const,
        },
      ],
      [{ by: "event_issue_id", direction: "asc" as const }],
      [{ by: "event_level", direction: "desc" as const }],
    ];
    for (const orderBy of orders)
      for (const limit of [
        0,
        1,
        7,
        26,
        64,
        65,
        EVENT_ROWS - 3,
        EVENT_ROWS,
        EVENT_ROWS + 10,
      ]) {
        run(overlay, {
          version: 1,
          from: { collection: "events", as: "event" },
          select: select(
            "event",
            "id",
            "message",
            "occurred_at",
            "issue_id",
            "level",
          ),
          orderBy,
          limit,
        });
        run(overlay, {
          version: 1,
          from: { collection: "events", as: "event" },
          where: Q_FILTER,
          select: select(
            "event",
            "id",
            "message",
            "occurred_at",
            "issue_id",
            "level",
          ),
          orderBy,
          limit,
        });
      }
    run(overlay, {
      version: 1,
      from: { collection: "events", as: "event" },
      joins: [
        {
          kind: "left",
          collection: "issues",
          as: "issue",
          on: {
            left: { source: "event", column: "issue_id" },
            right: { source: "issue", column: "id" },
          },
        },
      ],
      select: [
        ...select("event", "id", "message"),
        ...select("issue", "priority", "status"),
      ],
      orderBy: [
        { by: "issue_priority", direction: "desc", nulls: "first" },
        { by: "event_message", direction: "asc" },
      ],
      limit: 26,
    });
  });

  test("joins only the base's best rows unless they leave fewer than the limit", async () => {
    const byIssue = {
      left: { source: "event", column: "issue_id" },
      right: { source: "issue", column: "id" },
    };
    const byProject = {
      left: { source: "event", column: "project_id" },
      right: { source: "issue", column: "project_id" },
    };
    const issues = (
      kind: "inner" | "left",
      on: QueryJoin["on"],
    ): QueryJoin => ({
      kind,
      collection: "issues",
      as: "issue",
      on,
    });
    const user: QueryJoin = {
      kind: "left",
      collection: "users",
      as: "user",
      on: {
        left: { source: "issue", column: "assignee_id" },
        right: { source: "user", column: "id" },
      },
    };
    // Every fifth event has no issue and issues 35 to 39 do not exist, so an
    // inner join on the issue can leave fewer rows than the limit and the
    // plan falls back, where a left join keeps them all. Five issues share
    // each project id, so a join on it matches five rows for each event.
    const joinSets: readonly (readonly [string, readonly QueryJoin[]])[] = [
      ["inner issue", [issues("inner", byIssue)]],
      ["left issue", [issues("left", byIssue)]],
      // The shape of the page's open issues.
      ["inner issue, left user", [issues("inner", byIssue), user]],
      ["inner project", [issues("inner", byProject)]],
    ];
    const orders: readonly (readonly QueryOrder[] | undefined)[] = [
      undefined,
      // The overlay's inserted events come first.
      [{ by: "event_id", direction: "desc" }],
      // Thirteen messages: only the row-number tie-breaker orders most rows.
      [{ by: "event_message", direction: "asc" }],
      // A joined key: only the joined relation can rank the rows.
      [
        { by: "issue_priority", direction: "desc", nulls: "first" },
        { by: "event_id", direction: "asc" },
      ],
    ];
    const wheres: readonly (QueryPredicate | undefined)[] = [
      undefined,
      isNull("event", "issue_id", true),
      // Fewer rows than any limit; event 10 has no issue until the overlay
      // gives it one.
      or(eq("event", "id", 7n), eq("event", "id", 9n), eq("event", "id", 10n)),
      // A joined field: only the joined relation can decide the rows.
      eq("issue", "status", "open"),
    ];
    const outcomes: Record<
      string,
      { kept: number; fellBack: number; notTried: number }
    > = {};
    const overlay = await physicalOverlay(true);
    // `query` keeps no result, so each engine runs every case afresh.
    const engineWith = (limitBeforeJoins: boolean) => {
      const engine = new ColumnarQueryEngine(overlay, {
        capacityBytes: 1 << 30,
      });
      configureQueryMeasurement(engine, { limitBeforeJoins });
      return engine;
    };
    const cutFirst = engineWith(true);
    const joinFirst = engineWith(false);
    for (const [name, joins] of joinSets) {
      const tally = { kept: 0, fellBack: 0, notTried: 0 };
      outcomes[name] = tally;
      for (const orderBy of orders)
        for (const where of wheres) {
          const unlimited: QuerySpecification = {
            version: 1,
            from: { collection: "events", as: "event" },
            joins,
            ...(where === undefined ? {} : { where }),
            select: [
              ...select("event", "id", "message", "issue_id"),
              ...select("issue", "id", "priority"),
            ],
            ...(orderBy === undefined ? {} : { orderBy }),
          };
          // The reference cuts its full result to the limit.
          const reference = referenceQuery(overlay, SCHEMAS, unlimited);
          for (const limit of [1, 64, 65]) {
            const specification = { ...unlimited, limit };
            const rowsJoined = (engine: ColumnarQueryEngine) => {
              expect(engine.query(specification, { maxRows: 10_000 })).toEqual({
                ...reference,
                rows: reference.rows.slice(0, limit),
              });
              return inspectQueryExecution(engine).rowsJoined;
            };
            const cut = rowsJoined(cutFirst);
            const whole = rowsJoined(joinFirst);
            const tried =
              limit <= 64 &&
              (where === undefined || readsEventOnly(where)) &&
              !orderBy?.some(({ by }) => by.startsWith("issue_"));
            if (!tried) {
              tally.notTried++;
              expect(cut).toBe(whole);
            } else if (cut > whole) {
              // The best rows' joins, then every base row's.
              tally.fellBack++;
            } else {
              tally.kept++;
              expect(cut).toBeLessThanOrEqual(joins.length * limit);
            }
          }
        }
    }
    expect(outcomes).toEqual({
      "inner issue": { kept: 10, fellBack: 8, notTried: 30 },
      "left issue": { kept: 18, fellBack: 0, notTried: 30 },
      "inner issue, left user": { kept: 10, fellBack: 8, notTried: 30 },
      "inner project": { kept: 18, fellBack: 0, notTried: 30 },
    });
  });

  test("groups on composite keys as the reference does", async () => {
    const overlay = await physicalOverlay(true);
    run(overlay, {
      version: 1,
      from: { collection: "events", as: "event" },
      select: [
        ...select("event", "project_id", "level"),
        { kind: "aggregate", operator: "count", as: "count" },
        {
          kind: "aggregate",
          operator: "max",
          field: { source: "event", column: "occurred_at" },
          as: "latest",
        },
      ],
      groupBy: [
        { source: "event", column: "project_id" },
        { source: "event", column: "level" },
      ],
      orderBy: [
        { by: "event_project_id", direction: "asc" },
        { by: "event_level", direction: "asc" },
      ],
    });
  });
});

/** An in-memory base holding byte strings, which no fixture column type can. */
function bytesOverlay(): OverlayEngine {
  const everyByte = Uint8Array.from({ length: 256 }, (_u, byte) => byte);
  const everyByteChanged = Uint8Array.from(everyByte);
  everyByteChanged[128] = 0;
  // Keys are built in chunks of 4,096 bytes; these differ only past the
  // first chunk, or only in length around a chunk boundary.
  const long = (length: number, last = 7) => {
    const value = Uint8Array.from({ length }, (_u, index) => index % 251);
    if (length > 0) value[length - 1] = last;
    return value;
  };
  const blobs: readonly (readonly SqlValue[])[] = [
    ["b0", new Uint8Array(), null, "empty"],
    ["b1", Uint8Array.of(1), null, "one"],
    ["b2", Uint8Array.of(1, 0), null, "one-zero"],
    ["b3", Uint8Array.of(0, 1), null, "zero-one"],
    ["b4", Uint8Array.of(255), null, "high"],
    ["b5", Uint8Array.of(0x80, 0), null, "mid"],
    ["b6", Uint8Array.of(1), null, "one-again"],
    ["b7", Uint8Array.of(0), null, "nul"],
    ["b8", Uint8Array.of(0, 0), null, "nul-nul"],
    ["b9", Uint8Array.of(0x3a), null, "colon"],
    ["b10", Uint8Array.of(0x3a, 0x3a), null, "colon-colon"],
    ["b11", everyByte, null, "every-byte"],
    ["b12", Uint8Array.from(everyByte), null, "every-byte-again"],
    ["b13", everyByteChanged, null, "every-byte-changed"],
    ["b14", long(4_095), null, "4095"],
    ["b15", long(4_096), null, "4096"],
    ["b16", long(4_097), null, "4097"],
    ["b17", long(8_193), null, "8193"],
    ["b18", long(8_193, 8), null, "8193-tail"],
    ["b19", long(8_193), null, "8193-again"],
    ["b20", null, null, "none"],
    // A string, and bytes spelling that string's key and null's key: only the
    // type prefix on each key keeps these apart.
    ["b21", "\u0001", null, "text"],
    ["b24", Uint8Array.of(0x73, 0x31, 0x3a, 0x01), null, "text-key"],
    ["b25", Uint8Array.of(0x7a), null, "null-key"],
    // Unframed, "x" + "x" + "x" and "x" + "xx" would be one key: only the
    // length prefix on each token keeps these two groups apart.
    ["b22", Uint8Array.of(0x78), new Uint8Array(), "x-then-empty"],
    ["b23", new Uint8Array(), Uint8Array.of(0x78), "empty-then-x"],
  ];
  const refs: readonly (readonly SqlValue[])[] = [
    ["r0", Uint8Array.of(1)],
    ["r1", long(8_193)],
    ["r2", Uint8Array.of(0, 1)],
    ["r3", Uint8Array.of(2)],
    ["r4", null],
    ["r5", Uint8Array.from(everyByte)],
    ["r6", long(4_096)],
  ];
  const base = (rows: readonly (readonly SqlValue[])[]): OverlayBase => ({
    rows: rows.length,
    lookupKey: (key: readonly KeyPart[]) =>
      rows.findIndex((row) => key.length === 1 && row[0] === key[0]),
    valueAt: (index: number, row: number) => rows[row]?.[index] ?? null,
  });
  return new OverlayEngine([
    {
      name: "blobs",
      columns: ["id", "blob", "second", "label"],
      base: base(blobs),
    },
    { name: "refs", columns: ["id", "blob"], base: base(refs) },
  ]);
}

const BYTES_SCHEMAS: readonly QueryReferenceSchema[] = [
  { name: "blobs", columns: ["id", "blob", "second", "label"] },
  { name: "refs", columns: ["id", "blob"] },
];

describe("columnar query byte keys", () => {
  test("group and join byte strings only with equal bytes", () => {
    const overlay = bytesOverlay();
    const engine = new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 30 });
    const groupedLabels = {
      kind: "aggregate" as const,
      operator: "min" as const,
      field: { source: "b", column: "label" },
      as: "label",
    };
    const specifications: readonly QuerySpecification[] = [
      {
        version: 1,
        from: { collection: "blobs", as: "b" },
        select: [
          { kind: "field", field: { source: "b", column: "blob" }, as: "blob" },
          { kind: "aggregate", operator: "count", as: "count" },
          groupedLabels,
        ],
        groupBy: [{ source: "b", column: "blob" }],
      },
      {
        version: 1,
        from: { collection: "blobs", as: "b" },
        select: [
          { kind: "field", field: { source: "b", column: "blob" }, as: "blob" },
          {
            kind: "field",
            field: { source: "b", column: "second" },
            as: "second",
          },
          { kind: "aggregate", operator: "count", as: "count" },
          groupedLabels,
        ],
        groupBy: [
          { source: "b", column: "blob" },
          { source: "b", column: "second" },
        ],
      },
      {
        version: 1,
        from: { collection: "refs", as: "r" },
        joins: [
          {
            kind: "left",
            collection: "blobs",
            as: "b",
            on: {
              left: { source: "r", column: "blob" },
              right: { source: "b", column: "blob" },
            },
          },
        ],
        select: [
          { kind: "field", field: { source: "r", column: "id" }, as: "ref" },
          { kind: "field", field: { source: "b", column: "id" }, as: "blob" },
        ],
      },
    ];
    for (const specification of specifications)
      expect(engine.query(specification, { maxRows: 1_000 })).toEqual(
        referenceQuery(overlay, BYTES_SCHEMAS, specification),
      );
  });
});
