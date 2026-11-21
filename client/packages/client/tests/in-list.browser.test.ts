import {
  ColumnarQueryEngine,
  MAX_QUERY_SPEC_BYTES,
  OverlayEngine,
  type SqlValue,
} from "@calque/core";
import { describe, expect, it, vi } from "vitest";

import { createCollectionFacade } from "../src/facade/collection.js";
import { eq, inList, not, or } from "../src/facade/predicates.js";
import type { FacadePort } from "../src/facade/port.js";
import type { CollectionFacade, CollectionShape } from "../src/facade/types.js";
import type { GeneratedClientCollectionDescriptor } from "../src/generated-schema.js";

interface LinkRow {
  readonly id: bigint;
  readonly parent_id: string;
  readonly item_id: bigint;
}
interface ItemRow {
  readonly id: bigint;
  readonly kind: string;
  readonly bucket: string;
}
type Links = CollectionShape<LinkRow, { readonly id: bigint }>;
type Items = CollectionShape<ItemRow, { readonly id: bigint }>;

const LINKS: GeneratedClientCollectionDescriptor = {
  id: "links",
  tableId: "public.links",
  columns: [
    { name: "id", kind: "int64" },
    { name: "parent_id", kind: "text" },
    { name: "item_id", kind: "int64" },
  ],
  primaryKey: ["id"],
};
const ITEMS: GeneratedClientCollectionDescriptor = {
  id: "items",
  tableId: "public.items",
  columns: [
    { name: "id", kind: "int64" },
    { name: "kind", kind: "text" },
    { name: "bucket", kind: "text" },
  ],
  primaryKey: ["id"],
};
const kinds = new Map(
  [LINKS, ITEMS].map(({ id, columns }) => [
    id,
    new Map(columns.map(({ name, kind }) => [name, kind])),
  ]),
);

const LINK_ROWS = 21_000;
const ITEM_ROWS = 30_000;
const PARENTS = 3_000;
const parentId = (index: number) =>
  `parent-${String(index).padStart(6, "0")}-${"p".repeat(11)}`;

/** Facades whose queries the core engine runs, in this browser. */
function facades() {
  const links: SqlValue[][] = Array.from({ length: LINK_ROWS }, (_u, id) => [
    BigInt(id),
    parentId((id * 7) % PARENTS),
    BigInt((id * 13) % ITEM_ROWS),
  ]);
  const items: SqlValue[][] = Array.from({ length: ITEM_ROWS }, (_u, id) => [
    BigInt(id),
    `kind-${id % 12}`,
    `bucket-${(id * 7) % 31}`,
  ]);
  const tables = [
    { name: "links", columns: ["id", "parent_id", "item_id"], rows: links },
    { name: "items", columns: ["id", "kind", "bucket"], rows: items },
  ];
  const engine = new ColumnarQueryEngine(
    new OverlayEngine(
      tables.map(({ name, columns, rows }) => ({
        name,
        columns,
        base: {
          rows: rows.length,
          lookupKey: () => -1,
          valueAt: (column: number, row: number) => rows[row]?.[column] ?? null,
        },
        primaryKey: ["id"],
      })),
    ),
    { capacityBytes: 1 << 30 },
  );
  const port: FacadePort = {
    columnsOf: (collection) => kinds.get(collection),
    runQuery: async (specification, options) =>
      engine.query(specification, options),
    runWindow: async (specification, window, options) =>
      engine.window(specification, window, options),
    subscribeQuery: vi.fn(),
    subscribeWindow: vi.fn(),
    slice: vi.fn(async () => []),
    mutate: vi.fn(),
  };
  return {
    links: createCollectionFacade(port, LINKS) as CollectionFacade<Links>,
    items: createCollectionFacade(port, ITEMS) as CollectionFacade<Items>,
  };
}

describe("inList through the public query builder, in this browser", () => {
  const ids = Array.from({ length: 500 }, (_u, index) =>
    parentId((index * 5) % PARENTS),
  );

  it("filters a joined, grouped query by 500 text ids that an or of eq cannot carry", async () => {
    const { links, items } = facades();
    expect(ids[0]?.length).toBe(25);
    const grouped = (where: ReturnType<typeof inList<LinkRow, "parent_id">>) =>
      links
        .query()
        .join(items, "item", { left: "item_id", right: "id" })
        .where(where)
        .groupBy("item.kind", "item.bucket")
        .count()
        .orderBy("kind")
        .run();
    await expect(
      grouped(
        or(...ids.map((id) => eq<LinkRow, "parent_id">("parent_id", id))),
      ),
    ).rejects.toThrow(
      `INVALID_QUERY: query specification exceeds ${MAX_QUERY_SPEC_BYTES} bytes`,
    );

    const single = await grouped(inList("parent_id", ids));
    const pieces = await Promise.all(
      Array.from({ length: 5 }, (_u, piece) =>
        grouped(
          or(
            ...ids
              .slice(piece * 100, piece * 100 + 100)
              .map((id) => eq<LinkRow, "parent_id">("parent_id", id)),
          ),
        ),
      ),
    );
    const merged = new Map<string, number>();
    for (const rows of pieces)
      for (const row of rows) {
        const key = `${String(row["kind"])}|${String(row["bucket"])}`;
        merged.set(key, (merged.get(key) ?? 0) + Number(row["count"]));
      }
    const once = new Map(
      single.map((row) => [
        `${String(row["kind"])}|${String(row["bucket"])}`,
        Number(row["count"]),
      ]),
    );
    expect(once.size).toBeGreaterThan(0);
    expect(once).toEqual(merged);
    // 500 distinct parents of the 3,000, each with 7 of the 21,000 links.
    expect([...once.values()].reduce((sum, count) => sum + count, 0)).toBe(
      500 * 7,
    );
  });

  it("reads a window of an in-list result and counts its negation as the rest", async () => {
    const { links } = facades();
    const listed = await links
      .query()
      .where(inList("parent_id", ids))
      .select("id")
      .orderBy("id")
      .window(0, 10);
    expect(listed.total).toBe(500 * 7);
    expect(listed.rows).toHaveLength(10);
    const rest = await links
      .query()
      .where(not(inList("parent_id", ids)))
      .select("id")
      .window(0, 0);
    expect(rest.total + listed.total).toBe(LINK_ROWS);
  });
});
