import { describe, expect, it, vi } from "vitest";
import {
  ColumnarQueryEngine,
  OverlayEngine,
  type SqlValue,
} from "@calque/core";

import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import { createCollectionFacade } from "./collection.js";
import { eq, gt, gte } from "./predicates.js";
import type { FacadePort } from "./port.js";
import type { CollectionFacade, CollectionShape } from "./types.js";

interface TicketRow {
  readonly id: number;
  readonly priority: number | null;
  readonly account_id: number;
  readonly title: string;
}
interface AccountRow {
  readonly id: bigint;
  readonly name: string;
}
type Tickets = CollectionShape<
  TicketRow,
  { readonly id: number },
  never,
  never,
  false,
  "id" | "priority" | "account_id"
>;
type Accounts = CollectionShape<AccountRow, { readonly id: bigint }>;

const TICKETS: GeneratedClientCollectionDescriptor = {
  id: "tickets",
  tableId: "public.tickets",
  columns: [
    { name: "id", kind: "int32" },
    { name: "priority", kind: "int32" },
    { name: "account_id", kind: "int32" },
    { name: "title", kind: "text" },
  ],
  primaryKey: ["id"],
};
const ACCOUNTS: GeneratedClientCollectionDescriptor = {
  id: "accounts",
  tableId: "public.accounts",
  columns: [
    { name: "id", kind: "int64" },
    { name: "name", kind: "text" },
  ],
  primaryKey: ["id"],
};

// The engine holds an int2 or int4 column as int64 cells.
const TABLES: ReadonlyMap<string, readonly (readonly SqlValue[])[]> = new Map([
  [
    "tickets",
    [
      [1n, 3n, 10n, "Login fails"],
      [2n, 1n, 20n, "Slow page"],
      [3n, null, 10n, "Typo"],
      [4n, 3n, 20n, "Crash"],
    ],
  ],
  [
    "accounts",
    [
      [10n, "Acme"],
      [20n, "Globex"],
    ],
  ],
]);

/** Facades whose queries the core engine runs over `TABLES`. */
function facades() {
  const descriptors = [TICKETS, ACCOUNTS];
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
    tickets: createCollectionFacade(port, TICKETS) as CollectionFacade<Tickets>,
    accounts: createCollectionFacade(
      port,
      ACCOUNTS,
    ) as CollectionFacade<Accounts>,
  };
}

describe("int2 and int4 columns through the facade", () => {
  it("read back as numbers from run, window, and slice", async () => {
    const { tickets } = facades();
    expect(await tickets.query().orderBy("id").limit(2).run()).toEqual([
      { id: 1, priority: 3, account_id: 10, title: "Login fails" },
      { id: 2, priority: 1, account_id: 20, title: "Slow page" },
    ]);
    expect(
      await tickets.query().select("priority").orderBy("priority").window(1, 2),
    ).toEqual({
      offset: 1,
      total: 4,
      rows: [{ priority: 3 }, { priority: 3 }],
    });
    expect(await tickets.slice(2, 1)).toEqual([
      { id: 3, priority: null, account_id: 10, title: "Typo" },
    ]);
  });

  it("read back as numbers through groupBy, min, max, and a join to an int8 key", async () => {
    const { tickets, accounts } = facades();
    expect(
      await tickets
        .query()
        .groupBy("priority")
        .count()
        .min("id", "first")
        .orderBy("priority")
        .run(),
    ).toEqual([
      { priority: 1, count: 1, first: 2 },
      { priority: 3, count: 2, first: 1 },
      { priority: null, count: 1, first: 3 },
    ]);
    const extremes = await tickets
      .query()
      .min("priority", "low")
      .max("id", "last")
      .run();
    expect(extremes).toEqual([{ low: 1, last: 4 }]);
    const low: number | null | undefined = extremes[0]?.low;
    void low;
    expect(
      await accounts
        .query()
        .join(tickets, "ticket", { left: "id", right: "account_id" })
        .select("name")
        .selectAs("ticket.priority", "priority")
        .where(gt("ticket.priority", 2))
        .orderBy("name")
        .run(),
    ).toEqual([
      { name: "Acme", priority: 3 },
      { name: "Globex", priority: 3 },
    ]);
  });

  it("sum to a bigint, as PostgreSQL's sum over an int4 does, even when small", async () => {
    const { tickets, accounts } = facades();
    const totals = await tickets
      .query()
      .sum("priority", "total")
      .sum("account_id", "accounts")
      .run();
    expect(totals).toEqual([{ total: 7n, accounts: 60n }]);
    const total: bigint | null | undefined = totals[0]?.total;
    // @ts-expect-error a sum over an int4 column is a bigint, never a number.
    const asNumber: number | null | undefined = totals[0]?.total;
    void total;
    void asNumber;
    expect(
      await tickets
        .query()
        .groupBy("priority")
        .sum("id", "ids")
        .orderBy("priority")
        .window(0, 3),
    ).toEqual({
      offset: 0,
      total: 3,
      rows: [
        { priority: 1, ids: 2n },
        { priority: 3, ids: 5n },
        { priority: null, ids: 3n },
      ],
    });
    const joined = await accounts
      .query()
      .join(tickets, "ticket", { left: "id", right: "account_id" })
      .groupBy("name")
      .sum("ticket.priority", "priorities")
      .orderBy("name")
      .run();
    expect(joined).toEqual([
      { name: "Acme", priorities: 3n },
      { name: "Globex", priorities: 4n },
    ]);
    const priorities: bigint | null | undefined = joined[0]?.priorities;
    void priorities;
  });

  it("sum past 2 ** 53 exactly", async () => {
    // The fewest rows of the largest int4 whose sum passes 2 ** 53.
    const rows = 4_194_305;
    const engine = new ColumnarQueryEngine(
      new OverlayEngine([
        {
          name: "tickets",
          columns: TICKETS.columns.map(({ name }) => name),
          base: {
            rows,
            lookupKey: () => -1,
            valueAt: (column: number, row: number) =>
              [BigInt(row), null, 2_147_483_647n, "Load"][column] ?? null,
          },
          primaryKey: TICKETS.primaryKey,
        },
      ]),
      { capacityBytes: 1 << 30 },
    );
    const port: FacadePort = {
      columnsOf: () =>
        new Map(TICKETS.columns.map(({ name, kind }) => [name, kind])),
      runQuery: async (specification) => engine.query(specification),
      runWindow: vi.fn(),
      subscribeQuery: vi.fn(),
      subscribeWindow: vi.fn(),
      slice: vi.fn(),
      mutate: vi.fn(),
    };
    const tickets = createCollectionFacade(
      port,
      TICKETS,
    ) as CollectionFacade<Tickets>;
    const [row] = await tickets.query().sum("account_id", "total").run();
    expect(row?.total).toBe(BigInt(rows) * 2_147_483_647n);
    expect(row?.total).toBeGreaterThan(BigInt(Number.MAX_SAFE_INTEGER));
  });

  it("matches a number literal, and refuses one that is not an integer", async () => {
    const { tickets, runQuery } = facades();
    expect(
      await tickets.query().where(eq("priority", 3)).select("id").run(),
    ).toEqual([{ id: 1 }, { id: 4 }]);
    expect(
      await tickets.query().where(gte("id", 3)).select("title").run(),
    ).toEqual([{ title: "Typo" }, { title: "Crash" }]);
    runQuery.mockClear();
    expect(() => tickets.query().where(gt("priority", 2.5))).toThrow(
      new TypeError(
        "tickets.priority holds integers, so it cannot be compared with 2.5",
      ),
    );
    expect(runQuery).not.toHaveBeenCalled();
  });

  it("refuses a join between columns whose values can never be equal", () => {
    const { tickets, accounts } = facades();
    expect(() =>
      tickets.query().join(accounts, "account", {
        left: "title",
        // @ts-expect-error A text column cannot equal an int8 one.
        right: "id",
      }),
    ).toThrow(
      new TypeError(
        "join compares title (text) with accounts.id (int64), so no row would match",
      ),
    );
  });

  it("hands a live query the output names it turns into numbers, and no sum", () => {
    const { tickets, subscribeQuery } = facades();
    tickets
      .query()
      .groupBy("title")
      .max("priority", "top")
      .sum("priority", "total")
      .subscribe();
    expect([...(subscribeQuery.mock.calls[0]?.[3].keys() ?? [])]).toEqual([
      "top",
    ]);
  });
});
