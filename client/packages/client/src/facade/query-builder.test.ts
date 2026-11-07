import { describe, expect, it, vi } from "vitest";
import {
  ColumnarQueryEngine,
  OverlayEngine,
  canonicalQuerySpecification,
  validateQuerySpecification,
  type QuerySpecification,
  type SqlValue,
} from "@calque/core";

import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import { createCollectionFacade } from "./collection.js";
import {
  and,
  eq,
  gt,
  gte,
  isNotNull,
  isNull,
  lt,
  lte,
  neq,
  not,
  or,
} from "./predicates.js";
import type { FacadePort } from "./port.js";
import type { CollectionFacade, CollectionShape } from "./types.js";

interface EventRow {
  readonly id: bigint;
  readonly level: string;
  readonly project_id: bigint;
  readonly score: number | null;
  readonly resolved: boolean;
}
interface ProjectRow {
  readonly id: bigint;
  readonly name: string;
}
type Events = CollectionShape<EventRow, { readonly id: bigint }>;
type Projects = CollectionShape<ProjectRow, { readonly id: bigint }>;

const EVENTS: GeneratedClientCollectionDescriptor = {
  id: "events",
  tableId: "public.events",
  columns: [
    { name: "id", kind: "int64" },
    { name: "level", kind: "text" },
    { name: "project_id", kind: "int64" },
    { name: "score", kind: "f64" },
    { name: "resolved", kind: "bool" },
  ],
  primaryKey: ["id"],
};
const PROJECTS: GeneratedClientCollectionDescriptor = {
  id: "projects",
  tableId: "public.projects",
  columns: [
    { name: "id", kind: "int64" },
    { name: "name", kind: "text" },
  ],
  primaryKey: ["id"],
};
const kinds = new Map(
  [EVENTS, PROJECTS].map(({ id, columns }) => [
    id,
    new Map(columns.map(({ name, kind }) => [name, kind])),
  ]),
);
const columns: Record<string, readonly string[]> = {
  events: EVENTS.columns.map(({ name }) => name),
  projects: PROJECTS.columns.map(({ name }) => name),
};

function fakePort(
  run: FacadePort["runQuery"] = async () => ({
    columns: ["id", "level"],
    rows: [[1n, "error"]],
  }),
) {
  const runQuery = vi.fn<FacadePort["runQuery"]>(run);
  const subscribeQuery = vi.fn<FacadePort["subscribeQuery"]>(() => ({
    getSnapshot: () => ({ status: "loading" }),
    getServerSnapshot: () => ({ status: "loading" }),
    subscribe: () => () => undefined,
    promise: Promise.resolve(),
  }));
  const subscribeWindow = vi.fn<FacadePort["subscribeWindow"]>(() => ({
    getSnapshot: () => ({ status: "loading" }),
    getServerSnapshot: () => ({ status: "loading" }),
    subscribe: () => () => undefined,
    promise: Promise.resolve(),
  }));
  const runWindow = vi.fn<FacadePort["runWindow"]>(async (_spec, window) => ({
    columns: ["id", "level"],
    rows: [[1n, "error"]],
    offset: window.offset,
    total: 800_000,
  }));
  const port: FacadePort = {
    columnsOf: (collection) => kinds.get(collection),
    runQuery,
    runWindow,
    subscribeQuery,
    subscribeWindow,
    slice: vi.fn(async () => []),
    mutate: vi.fn(),
  };
  const events = createCollectionFacade(
    port,
    EVENTS,
  ) as CollectionFacade<Events>;
  const projects = createCollectionFacade(
    port,
    PROJECTS,
  ) as CollectionFacade<Projects>;
  return {
    port,
    runQuery,
    runWindow,
    subscribeQuery,
    subscribeWindow,
    events,
    projects,
  };
}

function lastSpecification(
  runQuery: ReturnType<typeof fakePort>["runQuery"],
): QuerySpecification {
  const call = runQuery.mock.calls.at(-1);
  if (!call) throw new Error("no query ran");
  return call[0];
}

const rows: readonly (readonly SqlValue[])[] = [
  [1n, "error", 10n, 0.9, false],
  [2n, "warn", 10n, 0.4, true],
  [3n, "info", 20n, null, false],
  [4n, "error", 20n, 0.2, true],
];

const projectRows: readonly (readonly SqlValue[])[] = [
  [10n, "api"],
  [20n, "web"],
];

/** Facades whose queries the core engine runs over `rows` and `projectRows`. */
function engineFacades() {
  const engine = new ColumnarQueryEngine(
    new OverlayEngine(
      [
        { name: "events", values: rows },
        { name: "projects", values: projectRows },
      ].map(({ name, values }) => ({
        name,
        columns: columns[name] ?? [],
        base: {
          rows: values.length,
          lookupKey: () => -1,
          valueAt: (column: number, row: number) =>
            values[row]?.[column] ?? null,
        },
        primaryKey: ["id"],
      })),
    ),
    { capacityBytes: 1 << 30 },
  );
  return fakePort(async (specification) => engine.query(specification));
}

describe("facade query builder", () => {
  it("compiles where, select, orderBy, and limit into one inert specification", async () => {
    const { runQuery, events } = fakePort();
    const rows = await events
      .query()
      .where(and(eq("level", "error"), gt("score", 0.5)))
      .where(isNull("score"))
      .select("id", "level")
      .orderBy("id", "desc", "last")
      .limit(10)
      .run({ maxRows: 50 });
    expect(rows).toEqual([{ id: 1n, level: "error" }]);
    expect(Object.isFrozen(rows)).toBe(true);
    expect(Object.getPrototypeOf(rows[0])).toBeNull();
    const specification = lastSpecification(runQuery);
    const expected: QuerySpecification = {
      version: 1,
      from: { collection: "events", as: "events" },
      where: {
        kind: "and",
        predicates: [
          {
            kind: "and",
            predicates: [
              {
                kind: "compare",
                operator: "eq",
                left: {
                  kind: "field",
                  field: { source: "events", column: "level" },
                },
                right: { kind: "literal", value: "error" },
              },
              {
                kind: "compare",
                operator: "gt",
                left: {
                  kind: "field",
                  field: { source: "events", column: "score" },
                },
                right: { kind: "literal", value: 0.5 },
              },
            ],
          },
          {
            kind: "is-null",
            expression: {
              kind: "field",
              field: { source: "events", column: "score" },
            },
          },
        ],
      },
      select: [
        { kind: "field", field: { source: "events", column: "id" }, as: "id" },
        {
          kind: "field",
          field: { source: "events", column: "level" },
          as: "level",
        },
      ],
      orderBy: [{ by: "id", direction: "desc", nulls: "last" }],
      limit: 10,
    };
    expect(canonicalQuerySpecification(specification)).toBe(
      canonicalQuerySpecification(expected),
    );
    expect(runQuery.mock.calls[0]?.[1]).toEqual({ maxRows: 50 });
  });

  it("compiles joins, dotted projections, and grouped aggregates", async () => {
    const { runQuery, events, projects } = fakePort();
    await events
      .query()
      .join(projects, "project", { left: "project_id", right: "id" }, "left")
      .select("id", "project.name")
      .run();
    const joined = lastSpecification(runQuery);
    expect(joined.joins).toEqual([
      {
        kind: "left",
        as: "project",
        collection: "projects",
        on: {
          left: { source: "events", column: "project_id" },
          right: { source: "project", column: "id" },
        },
      },
    ]);
    expect(joined.select).toEqual([
      { kind: "field", field: { source: "events", column: "id" }, as: "id" },
      {
        kind: "field",
        field: { source: "project", column: "name" },
        as: "name",
      },
    ]);

    await events
      .query()
      .groupBy("level")
      .count()
      .max("score", "top")
      .orderBy("count", "desc")
      .run();
    const grouped = lastSpecification(runQuery);
    expect(grouped.groupBy).toEqual([{ source: "events", column: "level" }]);
    expect(grouped.select).toEqual([
      {
        kind: "field",
        field: { source: "events", column: "level" },
        as: "level",
      },
      { kind: "aggregate", operator: "count", as: "count" },
      {
        kind: "aggregate",
        operator: "max",
        field: { source: "events", column: "score" },
        as: "top",
      },
    ]);
    expect(() => validateQuerySpecification(grouped)).not.toThrow();
  });

  it("adds a selectAs alias to every column, or to the selected ones", async () => {
    const engine = engineFacades().events;
    expect(
      await engine
        .query()
        .where(eq("id", 1n))
        .selectAs("score", "confidence")
        .run(),
    ).toEqual([
      {
        id: 1n,
        level: "error",
        project_id: 10n,
        score: 0.9,
        resolved: false,
        confidence: 0.9,
      },
    ]);
    expect(
      await engine
        .query()
        .where(eq("id", 1n))
        .select("id")
        .selectAs("level", "severity")
        .run(),
    ).toEqual([{ id: 1n, severity: "error" }]);

    const { runQuery, events, projects } = fakePort();
    await events
      .query()
      .join(projects, "project", { left: "project_id", right: "id" })
      .selectAs("project.name", "project_name")
      .orderBy("level")
      .run();
    expect(lastSpecification(runQuery).select).toEqual([
      ...(columns.events ?? []).map((column) => ({
        kind: "field",
        field: { source: "events", column },
        as: column,
      })),
      {
        kind: "field",
        field: { source: "project", column: "name" },
        as: "project_name",
      },
    ]);
    expect(() => events.query().selectAs("score", "id")).toThrow(
      "output name id is already projected",
    );
  });

  it("reduces a query without groupBy to one row of aggregates", async () => {
    const { runQuery, events } = fakePort();
    await events
      .query()
      .where(eq("level", "error"))
      .sum("score", "total")
      .run();
    const specification = lastSpecification(runQuery);
    expect(specification.groupBy).toBeUndefined();
    expect(specification.select).toEqual([
      {
        kind: "aggregate",
        operator: "sum",
        field: { source: "events", column: "score" },
        as: "total",
      },
    ]);
    expect(() => validateQuerySpecification(specification)).not.toThrow();

    const { events: engine, projects: engineProjects } = engineFacades();
    const reduced = await engine
      .query()
      .sum("score", "total")
      .min("score", "low")
      .max("score", "high")
      .sum("project_id")
      .run();
    expect(reduced).toEqual([
      { total: 1.5, low: 0.2, high: 0.9, project_id: 60n },
    ]);
    const total: number | null | undefined = reduced[0]?.total;
    const projectTotal: bigint | null | undefined = reduced[0]?.project_id;
    // @ts-expect-error a sum over a float column is a number, never a bigint.
    const bigTotal: bigint | null | undefined = reduced[0]?.total;
    void total;
    void projectTotal;
    void bigTotal;
    const scores = await engineProjects
      .query()
      .join(engine, "event", { left: "id", right: "project_id" })
      .groupBy("name")
      .sum("event.score", "score")
      .orderBy("name")
      .run();
    expect(scores).toEqual([
      { name: "api", score: 1.3 },
      { name: "web", score: 0.2 },
    ]);
    const score: number | null | undefined = scores[0]?.score;
    // @ts-expect-error a sum over a joined float column is a number too.
    const bigScore: bigint | null | undefined = scores[0]?.score;
    void score;
    void bigScore;
    // As in PostgreSQL, the one row is there when nothing matches.
    expect(
      await engine
        .query()
        .where(eq("level", "debug"))
        .sum("score")
        .min("id", "first")
        .max("id", "last")
        .count()
        .run(),
    ).toEqual([{ score: null, first: null, last: null, count: 0 }]);
  });

  it("rejects unknown columns, unprojected orderings, and foreign join targets at the call", () => {
    const { events, projects } = fakePort();
    expect(() =>
      events.query().where(eq("missing" as never, "x" as never)),
    ).toThrow("events has no column missing");
    expect(() =>
      events
        .query()
        .select("id")
        .orderBy("level" as never),
    ).toThrow("orderBy references level, which is not projected");
    expect(() =>
      events.query().join({ query: projects.query } as never, "p", {
        left: "project_id",
        right: "id",
      }),
    ).toThrow("join target must be a generated collection facade");
    expect(() =>
      events
        .query()
        .groupBy("level")
        .orderBy("score" as never),
    ).toThrow("orderBy references score, which is not projected");
    expect(() => events.query().limit(-1)).toThrow("limit");
    expect(() => events.query().run({ timeoutMs: 5 } as never)).rejects.toThrow(
      "unsupported option",
    );
  });

  it("hands subscriptions the sealed live options and dependencies", () => {
    const { subscribeQuery, events, projects } = fakePort();
    events
      .query()
      .join(projects, "project", { left: "project_id", right: "id" })
      .subscribe({ maxRows: 5 });
    const call = subscribeQuery.mock.calls[0];
    expect(call?.[0].from).toEqual({ collection: "events", as: "events" });
    expect(call?.[1]).toEqual(["events", "projects"]);
    expect(call?.[2]).toEqual({ maxRows: 5 });
    expect(call?.[3]).toEqual(new Map());
  });

  it("reads a window of the whole ordered result without its limit cap", async () => {
    const { runWindow, events } = fakePort();
    const controller = new AbortController();
    const window = await events
      .query()
      .where(eq("level", "error"))
      .select("id", "level")
      .orderBy("id", "desc")
      .window(799_999, 1, { maxBytes: 4096, signal: controller.signal });
    expect(window).toEqual({
      offset: 799_999,
      total: 800_000,
      rows: [{ id: 1n, level: "error" }],
    });
    expect(Object.isFrozen(window)).toBe(true);
    const call = runWindow.mock.calls[0];
    expect(call?.[0].orderBy).toEqual([{ by: "id", direction: "desc" }]);
    expect(call?.[0].limit).toBeUndefined();
    expect(call?.[1]).toEqual({ offset: 799_999, count: 1 });
    expect(call?.[2]).toEqual({ maxBytes: 4096, signal: controller.signal });
    await events.query().groupBy("level").count().window(0, 10);
    expect(runWindow.mock.calls[1]?.[0].groupBy).toEqual([
      { source: "events", column: "level" },
    ]);
  });

  it("hands a live window its sealed bounds, options and dependencies", () => {
    const { subscribeWindow, events, projects } = fakePort();
    events
      .query()
      .join(projects, "project", { left: "project_id", right: "id" })
      .orderBy("id", "desc")
      .subscribeWindow(40, 20, { maxBytes: 4096 });
    const call = subscribeWindow.mock.calls[0];
    expect(call?.[0].orderBy).toEqual([{ by: "id", direction: "desc" }]);
    expect(call?.[1]).toEqual(["events", "projects"]);
    expect(call?.[2]).toEqual({ offset: 40, count: 20 });
    expect(call?.[3]).toEqual({ maxBytes: 4096 });
    expect(() => events.query().subscribeWindow(-1, 1)).toThrow(
      /window offset/u,
    );
    expect(() =>
      events.query().subscribeWindow(0, 1, { signal: undefined } as never),
    ).toThrow("unsupported option");
    expect(subscribeWindow).toHaveBeenCalledTimes(1);
  });

  it("refuses a window outside its bounds at the call", async () => {
    const { runWindow, events } = fakePort();
    for (const [offset, count] of [
      [-1, 1],
      [0.5, 1],
      [0, 10_001],
      [0, -1],
    ] as const)
      await expect(events.query().window(offset, count)).rejects.toThrow(
        /window (offset|count)/u,
      );
    await expect(
      events.query().window(0, 1, { maxRows: 5 } as never),
    ).rejects.toThrow("unsupported option");
    expect(runWindow).not.toHaveBeenCalled();
  });
});

describe("facade predicates", () => {
  it.each([
    ["neq", neq<EventRow, "level">("level", "error"), [2n, 3n]],
    // A comparison with NULL is unknown, as in PostgreSQL, and a row is
    // selected only when the predicate is true: no case below selects row 3,
    // whose score is NULL, through a comparison on `score`.
    [
      "neq on a nullable column",
      neq<EventRow, "score">("score", 0.4),
      [1n, 4n],
    ],
    ["lt", lt<EventRow, "score">("score", 0.4), [4n]],
    ["lte", lte<EventRow, "score">("score", 0.4), [2n, 4n]],
    ["isNotNull", isNotNull<EventRow>("score"), [1n, 2n, 4n]],
    ["or", or<EventRow>(eq("level", "warn"), isNull("score")), [2n, 3n]],
    ["not", not<EventRow>(gt("score", 0.3)), [4n]],
    [
      "not over and",
      not<EventRow>(and(eq("level", "info"), gt("score", 0.5))),
      [1n, 2n, 4n],
    ],
    [
      "not over or",
      not<EventRow>(or(eq("level", "warn"), lt("score", 0.3))),
      [1n],
    ],
    // A `bool` column holds JavaScript booleans, so the facade's literal and
    // core's SqlValue both admit one.
    [
      "eq on a bool column",
      eq<EventRow, "resolved">("resolved", true),
      [2n, 4n],
    ],
  ])(
    "%s filters rows when the core engine runs it",
    async (_name, predicate, ids) => {
      const selected = await engineFacades()
        .events.query()
        .where(predicate)
        .select("id")
        .orderBy("id")
        .run();
      expect(selected.map((row) => row.id)).toEqual(ids);
    },
  );

  it("refuses a null literal and names the NULL test", () => {
    for (const comparison of [eq, neq, lt, lte, gt, gte])
      expect(() =>
        comparison<EventRow, "score">("score", null as never),
      ).toThrow(/cannot compare with null; use isNull\(\) or isNotNull\(\)/u);
  });
});
