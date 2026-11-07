import { describe, expect, it, vi } from "vitest";

import { SyncError } from "../error.js";
import {
  createCollectionFacade,
  createCollectionFacades,
} from "./collection.js";
import type { FacadePort } from "./port.js";

function fakePort() {
  const mutate = vi.fn<FacadePort["mutate"]>(async (collection, command) =>
    Promise.reject(
      new SyncError(
        "MUTATION_REJECTED",
        "mutation",
        "none",
        `${command} on ${collection} is not served`,
      ),
    ),
  );
  const slice = vi.fn<FacadePort["slice"]>(async () => [[1n, "error"]]);
  const port: FacadePort = {
    columnsOf: () =>
      new Map([
        ["id", "int64"],
        ["level", "text"],
      ]),
    runQuery: vi.fn(),
    runWindow: vi.fn(),
    subscribeQuery: vi.fn(),
    subscribeWindow: vi.fn(),
    slice,
    mutate,
  };
  return { port, mutate, slice };
}

const descriptor = {
  id: "events",
  tableId: "public.events",
  columns: [
    { name: "id", kind: "int64" as const },
    { name: "level", kind: "text" as const },
  ],
  primaryKey: ["id"],
  writes: {
    operations: ["insert" as const, "update" as const],
    writableColumns: ["level"],
  },
};

describe("collection facades", () => {
  it("exposes only the mutation methods the collection's writes allow", async () => {
    const { port, mutate } = fakePort();
    const facade = createCollectionFacade(port, descriptor) as Record<
      string,
      unknown
    >;
    expect(Object.isFrozen(facade)).toBe(true);
    expect(Object.getPrototypeOf(facade)).toBeNull();
    expect(Object.keys(facade).sort()).toEqual([
      "insert",
      "query",
      "slice",
      "update",
    ]);
    expect("delete" in facade).toBe(false);
    const insert = facade.insert as (row: unknown) => Promise<unknown>;
    await expect(insert({ id: 1n, level: "error" })).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
      scope: "mutation",
    });
    expect(mutate).toHaveBeenCalledWith(
      "events",
      "insert",
      { id: 1n },
      { id: 1n, level: "error" },
    );
    await expect(insert("not a row")).rejects.toThrow("insert row");
    // An insert carries every column: the key and each writable column.
    await expect(insert({ level: "error" })).rejects.toThrow(
      "insert row must carry id",
    );
    expect(mutate).toHaveBeenCalledTimes(1);
  });

  it("refuses every insert into a collection that cannot write a non-key column", async () => {
    const { port, mutate } = fakePort();
    const facade = createCollectionFacade(port, {
      ...descriptor,
      columns: [
        ...descriptor.columns,
        { name: "source", kind: "text" as const },
        { name: "host", kind: "text" as const },
      ],
    }) as { insert: (row: unknown) => Promise<unknown> };
    await expect(
      facade.insert({ id: 1n, level: "error", source: "a", host: "b" }),
    ).rejects.toThrow(
      new TypeError(
        "events cannot insert: its writableColumns omit host, source, and an insert writes every non-key column",
      ),
    );
    expect(mutate).not.toHaveBeenCalled();
  });

  it("update validates the key and the writable columns before the port sees them", async () => {
    const { port, mutate } = fakePort();
    const facade = createCollectionFacade(port, descriptor) as {
      update: (key: unknown, patch: unknown) => Promise<unknown>;
    };
    await expect(
      facade.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_REJECTED" });
    expect(mutate).toHaveBeenCalledWith(
      "events",
      "update",
      { id: 1n },
      { level: "warn" },
    );
    const captured = mutate.mock.calls[0];
    expect(Object.isFrozen(captured?.[2])).toBe(true);
    expect(Object.isFrozen(captured?.[3])).toBe(true);
    // The key must be exactly the primary key.
    await expect(facade.update({}, { level: "warn" })).rejects.toThrow(
      "update key must carry id",
    );
    await expect(
      facade.update({ id: 1n, level: "x" }, { level: "warn" }),
    ).rejects.toThrow("update key names a column that is not writable");
    // The patch may name only the writable columns, never the key.
    await expect(facade.update({ id: 1n }, { id: 2n })).rejects.toThrow(
      "update patch names a column that is not writable",
    );
    await expect(facade.update({ id: 1n }, {})).rejects.toThrow(
      "update patch must change at least one column",
    );
    // Accessors never run: a getter is refused rather than invoked.
    const trap = Object.defineProperty({}, "level", {
      get: () => "warn",
      enumerable: true,
    });
    await expect(facade.update({ id: 1n }, trap)).rejects.toThrow(
      "plain data properties",
    );
    expect(mutate).toHaveBeenCalledTimes(1);
  });

  it("omits every mutation method when no command exists", () => {
    const { port } = fakePort();
    const readOnly = {
      id: descriptor.id,
      tableId: descriptor.tableId,
      columns: descriptor.columns,
      primaryKey: descriptor.primaryKey,
    };
    const facades = createCollectionFacades(port, [readOnly]);
    expect(Object.keys(facades)).toEqual(["events"]);
    expect(Object.keys(facades.events ?? {}).sort()).toEqual([
      "query",
      "slice",
    ]);
  });

  it("slices complete rows in engine order and seals the window", async () => {
    const { port, slice } = fakePort();
    const facade = createCollectionFacade(port, descriptor) as {
      slice: (
        start: number,
        count: number,
        options?: unknown,
      ) => Promise<readonly Record<string, unknown>[]>;
    };
    const rows = await facade.slice(0, 1);
    expect(rows).toEqual([{ id: 1n, level: "error" }]);
    expect(slice).toHaveBeenCalledWith(
      "events",
      0,
      1,
      ["id", "level"],
      undefined,
    );
    await expect(facade.slice(-1, 1)).rejects.toThrow("start");
    await expect(facade.slice(0, 10_001)).rejects.toThrow("count");
    await expect(facade.slice(0, 1, { maxRows: 5 })).rejects.toThrow(
      "unsupported option",
    );
  });
});
