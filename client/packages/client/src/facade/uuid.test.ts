import { describe, expect, it, vi } from "vitest";
import {
  ColumnarQueryEngine,
  OverlayEngine,
  type SqlValue,
} from "@calque/core";

import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import { createCollectionFacade } from "./collection.js";
import { and, eq, gt, isNull, neq, not, or } from "./predicates.js";
import type { FacadePort } from "./port.js";
import type { CollectionFacade, CollectionShape } from "./types.js";

interface IssueRow {
  readonly id: string;
  readonly owner: string | null;
  readonly title: string;
}
interface PersonRow {
  readonly id: string;
  readonly name: string;
}
type Issues = CollectionShape<IssueRow, { readonly id: string }>;
type People = CollectionShape<PersonRow, { readonly id: string }>;

const ISSUES: GeneratedClientCollectionDescriptor = {
  id: "issues",
  tableId: "public.issues",
  columns: [
    { name: "id", kind: "uuid" },
    { name: "owner", kind: "uuid" },
    { name: "title", kind: "text" },
  ],
  primaryKey: ["id"],
};
const PEOPLE: GeneratedClientCollectionDescriptor = {
  id: "people",
  tableId: "public.people",
  columns: [
    { name: "id", kind: "uuid" },
    { name: "name", kind: "text" },
  ],
  primaryKey: ["id"],
};

const ADA = "0a1b2c3d-4e5f-4061-8172-839495a6b7c8";
const GRACE = "f0e1d2c3-b4a5-4697-8879-6a5b4c3d2e1f";
const LOGIN = "11111111-2222-4333-8444-555555555555";
const SLOW = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const TYPO = "00000000-0000-4000-8000-00000000000f";

/** The bytes the engine holds for `text`, parsed independently of the client. */
function bytesOf(text: string): Uint8Array {
  const hex = text.replaceAll("-", "");
  return Uint8Array.from({ length: 16 }, (_unused, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

const TABLES: ReadonlyMap<string, readonly (readonly SqlValue[])[]> = new Map([
  [
    "issues",
    [
      [bytesOf(LOGIN), bytesOf(ADA), "Login fails"],
      [bytesOf(SLOW), bytesOf(GRACE), "Slow page"],
      [bytesOf(TYPO), null, "Typo"],
    ],
  ],
  [
    "people",
    [
      [bytesOf(ADA), "Ada"],
      [bytesOf(GRACE), "Grace"],
    ],
  ],
]);

/** Facades whose queries the core engine runs over `TABLES`. */
function facades() {
  const descriptors = [ISSUES, PEOPLE];
  const engine = new ColumnarQueryEngine(
    new OverlayEngine(
      descriptors.map(({ id, columns, primaryKey }) => {
        const rows = TABLES.get(id) ?? [];
        return {
          name: id,
          columns: columns.map(({ name }) => name),
          base: {
            rows: rows.length,
            lookupKey: () => -1,
            valueAt: (column: number, row: number) =>
              rows[row]?.[column] ?? null,
          },
          primaryKey,
        };
      }),
    ),
    { capacityBytes: 1 << 30 },
  );
  const kinds = new Map(
    descriptors.map(({ id, columns }) => [
      id,
      new Map(columns.map(({ name, kind }) => [name, kind])),
    ]),
  );
  const runQuery = vi.fn<FacadePort["runQuery"]>(async (specification) =>
    engine.query(specification),
  );
  const subscribeQuery = vi.fn<FacadePort["subscribeQuery"]>(() => ({
    getSnapshot: () => ({ status: "loading" }),
    getServerSnapshot: () => ({ status: "loading" }),
    subscribe: () => () => undefined,
    promise: Promise.resolve(),
  }));
  const port: FacadePort = {
    columnsOf: (collection) => kinds.get(collection),
    runQuery,
    runWindow: async (specification, window) =>
      engine.window(specification, window),
    subscribeQuery,
    subscribeWindow: vi.fn(),
    slice: async (collection, start, count) =>
      (TABLES.get(collection) ?? []).slice(start, start + count),
    mutate: vi.fn(),
  };
  return {
    runQuery,
    subscribeQuery,
    issues: createCollectionFacade(port, ISSUES) as CollectionFacade<Issues>,
    people: createCollectionFacade(port, PEOPLE) as CollectionFacade<People>,
  };
}

describe("uuid columns through the facade", () => {
  it("read back as lowercase text from run, window, and slice", async () => {
    const { issues, runQuery } = facades();
    const all = await issues.query().orderBy("title").run();
    expect(all).toEqual([
      { id: LOGIN, owner: ADA, title: "Login fails" },
      { id: SLOW, owner: GRACE, title: "Slow page" },
      { id: TYPO, owner: null, title: "Typo" },
    ]);
    // The engine still answers with the 16 bytes; only the facade turns them
    // into text.
    const answer = await runQuery.mock.results[0]?.value;
    expect(answer.rows[0][0]).toEqual(bytesOf(LOGIN));

    expect(
      await issues.query().select("owner").orderBy("owner").window(1, 2),
    ).toEqual({
      offset: 1,
      total: 3,
      rows: [{ owner: GRACE }, { owner: null }],
    });

    expect(await issues.slice(1, 2)).toEqual([
      { id: SLOW, owner: GRACE, title: "Slow page" },
      { id: TYPO, owner: null, title: "Typo" },
    ]);
  });

  it("read back as text through a join, a projection alias, groupBy, min, and max", async () => {
    const { issues, people } = facades();
    expect(
      await issues
        .query()
        .join(people, "person", { left: "owner", right: "id" })
        .select("id", "title")
        .selectAs("person.id", "person_id")
        .orderBy("title")
        .run(),
    ).toEqual([
      { id: LOGIN, title: "Login fails", person_id: ADA },
      { id: SLOW, title: "Slow page", person_id: GRACE },
    ]);

    expect(
      await issues.query().groupBy("owner").count().orderBy("owner").run(),
    ).toEqual([
      { owner: ADA, count: 1 },
      { owner: GRACE, count: 1 },
      { owner: null, count: 1 },
    ]);

    expect(
      await issues
        .query()
        .min("id", "first")
        .max("owner", "last")
        .count("issues")
        .run(),
    ).toEqual([{ first: TYPO, last: GRACE, issues: 3 }]);
  });

  it("hands a live query the uuid output names it turns into text", () => {
    const { issues, people, subscribeQuery } = facades();
    issues
      .query()
      .join(people, "person", { left: "owner", right: "id" })
      .groupBy("title", "person.id")
      .min("id", "first")
      .subscribe();
    expect([...(subscribeQuery.mock.calls[0]?.[3].keys() ?? [])]).toEqual([
      "id",
      "first",
    ]);
  });

  it.each([
    ["eq in lowercase", eq<IssueRow, "owner">("owner", ADA), [LOGIN]],
    [
      "eq in uppercase",
      eq<IssueRow, "owner">("owner", ADA.toUpperCase()),
      [LOGIN],
    ],
    ["neq", neq<IssueRow, "id">("id", LOGIN), [TYPO, SLOW]],
    ["gt", gt<IssueRow, "id">("id", LOGIN.toUpperCase()), [SLOW]],
    ["or", or<IssueRow>(eq("id", TYPO), eq("owner", GRACE)), [TYPO, SLOW]],
    [
      "not over and",
      not<IssueRow>(and(eq("owner", ADA), eq("title", "Login fails"))),
      [TYPO, SLOW],
    ],
    ["isNull", isNull<IssueRow>("owner"), [TYPO]],
  ])("%s matches uuid text", async (_name, predicate, ids) => {
    const { issues } = facades();
    const selected = await issues
      .query()
      .where(predicate)
      .select("id")
      .orderBy("id")
      .run();
    expect(selected.map((row) => row.id)).toEqual(ids);
  });

  it("matches a joined alias.column against uuid text", async () => {
    const { issues, people } = facades();
    const selected = await issues
      .query()
      .join(people, "person", { left: "owner", right: "id" })
      .where(eq("person.id" as never, GRACE.toUpperCase() as never))
      .select("id")
      .run();
    expect(selected).toEqual([{ id: SLOW }]);
  });

  it("refuses malformed uuid text and bytes, naming the column", () => {
    const { issues, people, runQuery } = facades();
    for (const literal of [
      "not-a-uuid",
      ADA.replaceAll("-", ""),
      `${ADA} `,
      `{${ADA}}`,
      ADA.slice(0, -1),
    ])
      expect(() => issues.query().where(eq("owner", literal))).toThrow(
        new TypeError(
          "issues.owner must be a uuid string, xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        ),
      );
    expect(() => issues.query().where(eq("id", bytesOf(ADA) as never))).toThrow(
      TypeError,
    );
    expect(() =>
      issues
        .query()
        .join(people, "person", { left: "owner", right: "id" })
        .where(eq("person.id" as never, "Ada" as never)),
    ).toThrow("person.id must be a uuid string");
    expect(runQuery).not.toHaveBeenCalled();
  });
});
