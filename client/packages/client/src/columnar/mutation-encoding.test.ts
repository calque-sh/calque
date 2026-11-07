import { fromBinary, toBinary } from "@bufbuild/protobuf";
import type { DirectColumnarTableSchema } from "@calque/core";
import { change, decodePrimaryKey } from "@calque/protocol";
import type { ColumnarChangeTableBinding } from "@calque/worker/columnar";
import { describe, expect, it } from "vitest";

import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import { encodeCrudOperation } from "./mutation-encoding.js";

const descriptor: GeneratedClientCollectionDescriptor = {
  id: "tasks",
  tableId: "public.tasks",
  columns: [
    { name: "id", kind: "int64" },
    { name: "title", kind: "text" },
    { name: "done", kind: "bool" },
    { name: "due", kind: "timestamp" },
    { name: "meta", kind: "json" },
    { name: "owner", kind: "uuid" },
  ],
  primaryKey: ["id"],
};

const schema: DirectColumnarTableSchema = {
  name: "tasks",
  primaryKey: ["id"],
  columns: [
    { name: "id", type: "int64", nullable: false },
    { name: "title", type: "dict", nullable: false },
    { name: "done", type: "bool", nullable: false },
    { name: "due", type: "timestamp", nullable: true },
    { name: "meta", type: "json", nullable: true },
    { name: "owner", type: "uuid", nullable: true },
  ],
};

// Not the descriptor's `public.tasks`: the descriptor names the source
// relation and the plan reports the wire id it resolved to. An operation must
// carry this one, or the mount refuses it as an unsubscribed table.
const WIRE_TABLE_ID = "01J8Z0A2QK7WQ2R4V6M8N0P3XS";

const binding: ColumnarChangeTableBinding = {
  tableId: WIRE_TABLE_ID,
  collection: "tasks",
  columns: [
    { columnId: 1, name: "id" },
    { columnId: 2, name: "title" },
    { columnId: 3, name: "done" },
    { columnId: 4, name: "due" },
    { columnId: 5, name: "meta" },
    { columnId: 6, name: "owner" },
  ],
  primaryKey: [1],
};

function encode(
  operation: "insert" | "update" | "delete",
  key: Record<string, unknown>,
  values?: Record<string, unknown>,
) {
  return encodeCrudOperation({
    descriptor,
    schema,
    binding,
    operation,
    key,
    values,
  });
}

describe("encodeCrudOperation", () => {
  it("encodes a patch as canonical UPDATE_PATCH bytes in descriptor column order", () => {
    const encoded = encode(
      "update",
      { id: 7n },
      {
        owner: "ABABABAB-ABAB-ABAB-ABAB-ABABABABABAB",
        title: "ship",
        due: "2026-09-05T12:00:00.250Z",
        meta: { tags: ["a"] },
      },
    );
    const operation = fromBinary(change.OperationSchema, encoded.bytes);
    expect(toBinary(change.OperationSchema, operation)).toEqual(encoded.bytes);
    expect(operation.tableId).toBe(WIRE_TABLE_ID);
    expect(operation.kind).toBe(change.OperationKind.UPDATE_PATCH);
    expect(operation.primaryKey).toEqual(encoded.primaryKey);
    expect(decodePrimaryKey(operation.primaryKey).parts).toMatchObject([
      { columnId: 1, value: { kind: { case: "int64Value", value: 7n } } },
    ]);
    expect(operation.payload.case).toBe("update");
    if (operation.payload.case !== "update") throw new Error("not a patch");
    expect(
      operation.payload.value.changedColumns.map((column) => [
        column.columnId,
        column.value?.kind.case,
        column.value?.kind.value,
      ]),
    ).toEqual([
      [2, "textValue", "ship"],
      [4, "timestampUnixMicros", 1_788_609_600_250_000n],
      [5, "jsonText", '{"tags":["a"]}'],
      [6, "uuid16", new Uint8Array(16).fill(0xab)],
    ]);
  });

  it("encodes a full row, a null in a nullable column, and a delete", () => {
    const insert = fromBinary(
      change.OperationSchema,
      encode(
        "insert",
        { id: 1n },
        {
          id: 1n,
          title: "t",
          done: false,
          due: null,
          meta: null,
          owner: null,
        },
      ).bytes,
    );
    expect(insert.kind).toBe(change.OperationKind.INSERT_FULL_ROW);
    if (insert.payload.case !== "insert") throw new Error("not an insert");
    expect(
      insert.payload.value.columns.map((column) => [
        column.columnId,
        column.value?.kind.case,
      ]),
    ).toEqual([
      [1, "int64Value"],
      [2, "textValue"],
      [3, "boolValue"],
      [4, "nullValue"],
      [5, "nullValue"],
      [6, "nullValue"],
    ]);
    const remove = fromBinary(
      change.OperationSchema,
      encode("delete", { id: 1n }).bytes,
    );
    expect(remove.kind).toBe(change.OperationKind.DELETE_BY_KEY);
    expect(remove.payload.case).toBe("delete");
  });

  it("refuses values that do not fit the generated column kinds", () => {
    expect(() => encode("update", { id: 1 }, { title: "x" })).toThrow(
      "tasks.id must be a bigint",
    );
    expect(() => encode("update", { id: 1n }, { title: 3 })).toThrow(
      "tasks.title must be a string",
    );
    expect(() => encode("update", { id: 1n }, { title: null })).toThrow(
      "tasks.title is not nullable",
    );
    expect(() => encode("update", { id: 1n }, { due: "yesterday" })).toThrow(
      "ISO 8601",
    );
    expect(() => encode("update", { id: 1n }, { owner: "not-a-uuid" })).toThrow(
      "tasks.owner must be a uuid string",
    );
    expect(() =>
      encode("update", { id: 1n }, { owner: new Uint8Array(16) }),
    ).toThrow("tasks.owner must be a uuid string");
    expect(() => encode("update", { id: 1n }, { meta: 1n })).toThrow(
      "JSON-serializable",
    );
    expect(() => encode("delete", { id: null })).toThrow("must not be null");
  });

  it("carries a timestamp at PostgreSQL's own microsecond resolution", () => {
    const micros = (value: string) => {
      const operation = fromBinary(
        change.OperationSchema,
        encode("update", { id: 1n }, { due: value }).bytes,
      );
      if (operation.payload.case !== "update") throw new Error("not a patch");
      return operation.payload.value.changedColumns[0]?.value?.kind.value;
    };
    // `Date.parse` alone resolves milliseconds, which would drop the 250 µs.
    expect(micros("2026-09-05T12:00:00.000250Z")).toBe(1_788_609_600_000_250n);
    expect(micros("2026-09-05T12:00:00.25Z")).toBe(1_788_609_600_250_000n);
    expect(micros("2026-09-05T14:00:00.000250+02:00")).toBe(
      1_788_609_600_000_250n,
    );
    expect(micros("2026-09-05T12:00:00Z")).toBe(1_788_609_600_000_000n);
    // Before the epoch the sub-millisecond digits still run forwards.
    expect(micros("1969-12-31T23:59:59.999750Z")).toBe(-250n);
    // Finer than the column can hold is refused rather than truncated.
    expect(() =>
      encode("update", { id: 1n }, { due: "2026-09-05T12:00:00.0002505Z" }),
    ).toThrow("microsecond resolution");
  });

  it("encodes an int4 number, in a key or a value, as the int64 cell the server reads", () => {
    const encodeSeat = (
      operation: "insert" | "delete",
      key: Record<string, unknown>,
      values?: Record<string, unknown>,
    ) =>
      encodeCrudOperation({
        descriptor: {
          id: "seats",
          tableId: "public.seats",
          columns: [
            { name: "number", kind: "int32" },
            { name: "row", kind: "int32" },
          ],
          primaryKey: ["number"],
        },
        schema: {
          name: "seats",
          primaryKey: ["number"],
          columns: [
            { name: "number", type: "int64", nullable: false },
            { name: "row", type: "int64", nullable: true },
          ],
        },
        binding: {
          tableId: "public.seats",
          collection: "seats",
          columns: [
            { columnId: 1, name: "number" },
            { columnId: 2, name: "row" },
          ],
          primaryKey: [1],
        },
        operation,
        key,
        values,
      });
    const insert = fromBinary(
      change.OperationSchema,
      encodeSeat("insert", { number: 12 }, { number: 12, row: -3 }).bytes,
    );
    expect(decodePrimaryKey(insert.primaryKey).parts).toMatchObject([
      { columnId: 1, value: { kind: { case: "int64Value", value: 12n } } },
    ]);
    if (insert.payload.case !== "insert") throw new Error("not an insert");
    expect(
      insert.payload.value.columns.map((column) => column.value?.kind.value),
    ).toEqual([12n, -3n]);

    expect(() => encodeSeat("delete", { number: 12n })).toThrow(
      "seats.number must be an integer number",
    );
    expect(() => encodeSeat("delete", { number: 1.5 })).toThrow(
      "seats.number must be an integer number",
    );
    expect(() =>
      encodeSeat("insert", { number: 1 }, { number: 1, row: 2 ** 31 }),
    ).toThrow("seats.row is outside the int4 range");
  });

  it("encodes a text key part as text, as the echo will", () => {
    const encodeTag = (name: string) =>
      encodeCrudOperation({
        descriptor: {
          id: "tags",
          tableId: "public.tags",
          columns: [{ name: "name", kind: "text" }],
          primaryKey: ["name"],
        },
        schema: {
          name: "tags",
          primaryKey: ["name"],
          columns: [{ name: "name", type: "utf8", nullable: false }],
        },
        binding: {
          tableId: "public.tags",
          collection: "tags",
          columns: [{ columnId: 1, name: "name" }],
          primaryKey: [1],
        },
        operation: "delete",
        key: { name },
        values: undefined,
      });
    expect(
      decodePrimaryKey(encodeTag("ünï-\u{1d11e}").primaryKey).parts,
    ).toMatchObject([
      {
        columnId: 1,
        value: { kind: { case: "textValue", value: "ünï-\u{1d11e}" } },
      },
    ]);
    // A lone surrogate has no UTF-8 form; encoding it would substitute U+FFFD.
    expect(() => encodeTag("a\ud800")).toThrow("must be well-formed Unicode");
  });
});
