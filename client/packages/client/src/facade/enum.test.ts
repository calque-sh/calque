import { describe, expect, it, vi } from "vitest";
import {
  ColumnarQueryEngine,
  OverlayEngine,
  type SqlValue,
} from "@calque/core";

import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import { createCollectionFacade } from "./collection.js";
import { and, gte, lt, lte, not } from "./predicates.js";
import type { FacadePort } from "./port.js";
import type {
  CollectionFacade,
  CollectionShape,
  QueryPredicate,
} from "./types.js";

interface TaskRow {
  readonly id: bigint;
  readonly status: string | null;
  readonly title: string;
}
type Tasks = CollectionShape<TaskRow, { readonly id: bigint }>;

const TASKS: GeneratedClientCollectionDescriptor = {
  id: "tasks",
  tableId: "public.tasks",
  columns: [
    { name: "id", kind: "int64" },
    { name: "status", kind: "text" },
    { name: "title", kind: "text" },
  ],
  primaryKey: ["id"],
};

// `status` is `CREATE TYPE status AS ENUM ('todo', 'doing', 'done')`, whose
// label order is the reverse of its text order. `archived` was added after
// the server read the labels.
const LABELS = ["todo", "doing", "done"];
const ROWS: readonly (readonly SqlValue[])[] = [
  [1n, "doing", "doing"],
  [2n, "todo", "todo"],
  [3n, null, "null"],
  [4n, "done", "done"],
  [5n, "archived", "archived"],
  [6n, "todo", "todo"],
];

function tasks(): CollectionFacade<Tasks> {
  const engine = new ColumnarQueryEngine(
    new OverlayEngine([
      {
        name: "tasks",
        columns: TASKS.columns.map(({ name }) => name),
        base: {
          rows: ROWS.length,
          lookupKey: () => -1,
          valueAt: (column: number, row: number) => ROWS[row]?.[column] ?? null,
        },
        primaryKey: TASKS.primaryKey,
      },
    ]),
    { capacityBytes: 1 << 30 },
    [{ collection: "tasks", column: "status", labels: LABELS }],
  );
  const port: FacadePort = {
    columnsOf: () =>
      new Map(TASKS.columns.map(({ name, kind }) => [name, kind])),
    runQuery: async (specification) => engine.query(specification),
    runWindow: async (specification, window) =>
      engine.window(specification, window),
    subscribeQuery: vi.fn(),
    subscribeWindow: vi.fn(),
    slice: vi.fn(),
    mutate: vi.fn(),
  };
  return createCollectionFacade(port, TASKS) as CollectionFacade<Tasks>;
}

async function statuses(query: {
  run: () => Promise<readonly { readonly status: string | null }[]>;
}) {
  return (await query.run()).map(({ status }) => status);
}

describe("an enum column through the facade", () => {
  it("sorts in declared order, a value added since after every declared one", async () => {
    const collection = tasks();
    const sorted = (
      direction: "asc" | "desc",
      nulls: "first" | "last",
      limit?: number,
    ) => {
      const query = collection
        .query()
        .select("id", "status")
        .orderBy("status", direction, nulls)
        .orderBy("id");
      return statuses(limit === undefined ? query : query.limit(limit));
    };
    expect(await sorted("asc", "last")).toEqual([
      "todo",
      "todo",
      "doing",
      "done",
      "archived",
      null,
    ]);
    expect(await sorted("asc", "first")).toEqual([
      null,
      "todo",
      "todo",
      "doing",
      "done",
      "archived",
    ]);
    expect(await sorted("desc", "last")).toEqual([
      "archived",
      "done",
      "doing",
      "todo",
      "todo",
      null,
    ]);
    expect(await sorted("desc", "first", 3)).toEqual([
      null,
      "archived",
      "done",
    ]);
    // A text column still sorts by text.
    expect(
      (await collection.query().select("title").orderBy("title").run()).map(
        ({ title }) => title,
      ),
    ).toEqual(["archived", "doing", "done", "null", "todo", "todo"]);
  });

  it("filters ranges in declared order, an unknown value above every declared one", async () => {
    const collection = tasks();
    const matching = (where: QueryPredicate<TaskRow>) =>
      statuses(
        collection.query().where(where).select("id", "status").orderBy("id"),
      );
    expect(await matching(lt("status", "done"))).toEqual([
      "doing",
      "todo",
      "todo",
    ]);
    expect(await matching(gte("status", "doing"))).toEqual([
      "doing",
      "done",
      "archived",
    ]);
    expect(
      await matching(and(gte("status", "doing"), lte("status", "done"))),
    ).toEqual(["doing", "done"]);
    expect(await matching(not(lte("status", "done")))).toEqual(["archived"]);
    // A literal no label names orders after the declared ones, and among
    // values no label names, by text.
    expect(await matching(gte("status", "absent"))).toEqual(["archived"]);
    expect(await matching(lt("status", "absent"))).toEqual([
      "doing",
      "todo",
      "done",
      "todo",
    ]);
    expect(await matching(lt("status", "todo"))).toEqual([]);
  });

  it("takes min and max in declared order", async () => {
    const collection = tasks();
    expect(
      await collection.query().min("status", "low").max("status", "high").run(),
    ).toEqual([{ low: "todo", high: "archived" }]);
    expect(
      await collection
        .query()
        .where(lte("status", "done"))
        .max("status", "high")
        .run(),
    ).toEqual([{ high: "done" }]);
    expect(
      await collection
        .query()
        .groupBy("status")
        .count()
        .orderBy("status", "desc")
        .run(),
    ).toEqual([
      { status: "archived", count: 1 },
      { status: "done", count: 1 },
      { status: "doing", count: 1 },
      { status: "todo", count: 2 },
      { status: null, count: 1 },
    ]);
  });
});
