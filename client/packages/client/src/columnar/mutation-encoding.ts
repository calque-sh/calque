import { create, toBinary } from "@bufbuild/protobuf";
import type { DirectColumnarTableSchema } from "@calque/core";
import { change, encodePrimaryKey } from "@calque/protocol";
import type { ColumnarChangeTableBinding } from "@calque/worker/columnar";

import type {
  GeneratedClientCollectionDescriptor,
  GeneratedClientColumnKind,
  GeneratedClientCrudOperation,
} from "../generated-schema.js";
import { uuidBytes } from "../uuid.js";

/**
 * Encodes one typed facade mutation as the wire `Operation` the server
 * validates and the change stream's echo replays, so the overlay, the
 * server's footprint check and the echo read the same bytes. A value that
 * does not fit its column throws a `TypeError` before anything is applied.
 */

interface EncodedCrudOperation {
  readonly tableId: string;
  /** Canonical `PrimaryKey` bytes, the footprint row the server checks. */
  readonly primaryKey: Uint8Array;
  /** Canonical `Operation` bytes. */
  readonly bytes: Uint8Array;
}

interface CrudEncoding {
  readonly descriptor: GeneratedClientCollectionDescriptor;
  readonly schema: DirectColumnarTableSchema;
  readonly binding: ColumnarChangeTableBinding;
  readonly operation: GeneratedClientCrudOperation;
  /** The key columns, exactly. */
  readonly key: Readonly<Record<string, unknown>>;
  /** The patch for `update`, the whole row for `insert`, absent for `delete`. */
  readonly values: Readonly<Record<string, unknown>> | undefined;
}

interface BoundColumn {
  readonly name: string;
  readonly kind: GeneratedClientColumnKind;
  readonly nullable: boolean;
  readonly columnId: number;
  readonly physical: string;
}

export function encodeCrudOperation(input: CrudEncoding): EncodedCrudOperation {
  const { descriptor, schema, binding } = input;
  const columns = bindColumns(descriptor, schema, binding);
  const label = descriptor.id;
  const values = input.values ?? {};
  const keyParts: change.ColumnValue[] = [];
  for (const name of descriptor.primaryKey) {
    const column = columns.get(name);
    if (!column) throw new TypeError(`${label}.${name} is not a served column`);
    const value = input.key[name];
    if (value === null || value === undefined)
      throw new TypeError(`${label}.${name} key part must not be null`);
    keyParts.push(
      create(change.ColumnValueSchema, {
        columnId: column.columnId,
        value: keyPart(column, value, label),
      }),
    );
  }
  const primaryKey = encodePrimaryKey(
    create(change.PrimaryKeySchema, { codecVersion: 1, parts: keyParts }),
  );
  // The wire table id comes from the binding: the descriptor's `tableId` is
  // the source relation, a table the mount never subscribed.
  const operation = create(change.OperationSchema, {
    tableId: binding.tableId,
    primaryKey,
  });
  switch (input.operation) {
    case "insert": {
      operation.kind = change.OperationKind.INSERT_FULL_ROW;
      operation.payload = {
        case: "insert",
        value: create(change.FullRowSchema, {
          columns: cells(columns, values, descriptor),
        }),
      };
      break;
    }
    case "update": {
      operation.kind = change.OperationKind.UPDATE_PATCH;
      operation.payload = {
        case: "update",
        value: create(change.RowPatchSchema, {
          changedColumns: cells(columns, values, descriptor),
        }),
      };
      break;
    }
    case "delete": {
      operation.kind = change.OperationKind.DELETE_BY_KEY;
      operation.payload = {
        case: "delete",
        value: create(change.DeleteSchema, {}),
      };
      break;
    }
  }
  return Object.freeze({
    tableId: binding.tableId,
    primaryKey,
    bytes: toBinary(change.OperationSchema, operation, {
      writeUnknownFields: false,
    }),
  });
}

function bindColumns(
  descriptor: GeneratedClientCollectionDescriptor,
  schema: DirectColumnarTableSchema,
  binding: ColumnarChangeTableBinding,
): ReadonlyMap<string, BoundColumn> {
  const bound = new Map<string, BoundColumn>();
  for (const column of descriptor.columns) {
    const physical = schema.columns.find(({ name }) => name === column.name);
    const wire = binding.columns.find(({ name }) => name === column.name);
    if (!physical || !wire)
      throw new TypeError(
        `${descriptor.id}.${column.name} is not bound to the change stream`,
      );
    bound.set(column.name, {
      name: column.name,
      kind: column.kind,
      nullable: physical.nullable,
      columnId: wire.columnId,
      physical: physical.type,
    });
  }
  return bound;
}

/** Value cells in descriptor column order, whatever order the object had. */
function cells(
  columns: ReadonlyMap<string, BoundColumn>,
  values: Readonly<Record<string, unknown>>,
  descriptor: GeneratedClientCollectionDescriptor,
): change.ColumnValue[] {
  const out: change.ColumnValue[] = [];
  for (const { name } of descriptor.columns) {
    if (!Object.hasOwn(values, name)) continue;
    const column = columns.get(name);
    if (!column)
      throw new TypeError(`${descriptor.id}.${name} is not a served column`);
    out.push(
      create(change.ColumnValueSchema, {
        columnId: column.columnId,
        value: cell(column, values[name], descriptor.id),
      }),
    );
  }
  return out;
}

function cell(
  column: BoundColumn,
  value: unknown,
  label: string,
): change.CellValue {
  if (value === null) {
    if (!column.nullable)
      throw new TypeError(`${label}.${column.name} is not nullable`);
    return create(change.CellValueSchema, {
      kind: { case: "nullValue", value: change.NullValue.NULL_VALUE },
    });
  }
  const name = `${label}.${column.name}`;
  switch (column.kind) {
    case "int32":
      return create(change.CellValueSchema, {
        kind: { case: "int64Value", value: int32(value, name) },
      });
    case "int64":
      return create(change.CellValueSchema, {
        kind: { case: "int64Value", value: int64(value, name) },
      });
    case "f64":
      if (typeof value !== "number" || !Number.isFinite(value))
        throw new TypeError(`${name} must be a finite number`);
      return create(change.CellValueSchema, {
        kind: { case: "float64Value", value },
      });
    case "bool":
      if (typeof value !== "boolean")
        throw new TypeError(`${name} must be a boolean`);
      return create(change.CellValueSchema, {
        kind: { case: "boolValue", value },
      });
    case "text":
      return create(change.CellValueSchema, {
        kind: { case: "textValue", value: text(value, name) },
      });
    case "decimal":
      return create(change.CellValueSchema, {
        kind: { case: "decimalText", value: text(value, name) },
      });
    case "date":
      return create(change.CellValueSchema, {
        kind: { case: "dateIso8601", value: text(value, name) },
      });
    case "time":
      return create(change.CellValueSchema, {
        kind: { case: "timeIso8601", value: text(value, name) },
      });
    case "timestamp":
      return create(change.CellValueSchema, {
        kind: {
          case: "timestampUnixMicros",
          value: timestampMicros(value, name),
        },
      });
    case "json":
      return create(change.CellValueSchema, {
        kind: { case: "jsonText", value: jsonText(value, name) },
      });
    case "bytes":
      if (!(value instanceof Uint8Array))
        throw new TypeError(`${name} must be a Uint8Array`);
      return create(change.CellValueSchema, {
        kind: { case: "blobValue", value: value.slice() },
      });
    case "uuid":
      return create(change.CellValueSchema, {
        kind: { case: "uuid16", value: uuidBytes(value, name) },
      });
  }
}

/**
 * A key part in the v1 key grammar: text key columns travel as `text_value`
 * and timestamps as microseconds, exactly as the server encodes the same
 * row's key on the echo.
 */
function keyPart(
  column: BoundColumn,
  value: unknown,
  label: string,
): change.CellValue {
  const name = `${label}.${column.name}`;
  switch (column.physical) {
    case "int64":
    case "uint64":
      return create(change.CellValueSchema, {
        kind: {
          case: "int64Value",
          value:
            column.kind === "int32" ? int32(value, name) : int64(value, name),
        },
      });
    case "timestamp":
      return create(change.CellValueSchema, {
        kind: { case: "int64Value", value: timestampMicros(value, name) },
      });
    case "bool":
      if (typeof value !== "boolean")
        throw new TypeError(`${name} must be a boolean`);
      return create(change.CellValueSchema, {
        kind: { case: "boolValue", value },
      });
    case "bytes":
      if (!(value instanceof Uint8Array))
        throw new TypeError(`${name} must be a Uint8Array`);
      return create(change.CellValueSchema, {
        kind: { case: "blobValue", value: value.slice() },
      });
    case "uuid":
      return create(change.CellValueSchema, {
        kind: { case: "uuid16", value: uuidBytes(value, name) },
      });
    case "utf8":
    case "dict":
      return create(change.CellValueSchema, {
        kind: { case: "textValue", value: text(value, name) },
      });
    default:
      throw new TypeError(`${name} is not a primary-key column`);
  }
}

function int64(value: unknown, name: string): bigint {
  if (typeof value !== "bigint")
    throw new TypeError(`${name} must be a bigint`);
  if (value < -(2n ** 63n) || value > 2n ** 63n - 1n)
    throw new TypeError(`${name} is outside the int64 range`);
  return value;
}

/** An `int2` or `int4` value, which travels as an int64 cell. */
function int32(value: unknown, name: string): bigint {
  if (typeof value !== "number" || !Number.isInteger(value))
    throw new TypeError(`${name} must be an integer number`);
  if (value < -(2 ** 31) || value > 2 ** 31 - 1)
    throw new TypeError(`${name} is outside the int4 range`);
  return BigInt(value);
}

function text(value: unknown, name: string): string {
  if (typeof value !== "string")
    throw new TypeError(`${name} must be a string`);
  // UTF-8 encoding would replace a lone surrogate with U+FFFD, and so would
  // the echo that confirms the write, so nothing would report the change; in
  // a key two strings would name the same row.
  if (!value.isWellFormed())
    throw new TypeError(`${name} must be well-formed Unicode`);
  return value;
}

/** The seconds field and its fractional digits, which `Date.parse` truncates. */
const FRACTIONAL_SECONDS = /^(.*\d\d:\d\d:\d\d)\.(\d+)(.*)$/;

function timestampMicros(value: unknown, name: string): bigint {
  if (typeof value !== "string")
    throw new TypeError(`${name} must be an ISO 8601 timestamp string`);
  // PostgreSQL's timestamp types resolve microseconds and `Date.parse`
  // resolves milliseconds, so the sub-millisecond digits are read here. A
  // value truncated on the way in would come back truncated in the echo, and
  // nothing would report the loss.
  const fractional = FRACTIONAL_SECONDS.exec(value);
  let micros = 0n;
  let millisecondForm = value;
  if (fractional) {
    // The regex guarantees all three, and the defaults satisfy the checker.
    const [, head = "", digits = "", tail = ""] = fractional;
    if (digits.length > 6)
      throw new TypeError(
        `${name} must be an ISO 8601 timestamp with at most microsecond resolution`,
      );
    const padded = digits.padEnd(6, "0");
    micros = BigInt(padded.slice(3));
    millisecondForm = `${head}.${padded.slice(0, 3)}${tail}`;
  }
  const millis = Date.parse(millisecondForm);
  if (!Number.isFinite(millis))
    throw new TypeError(`${name} must be an ISO 8601 timestamp string`);
  return BigInt(millis) * 1_000n + micros;
}

function jsonText(value: unknown, name: string): string {
  let serialized: string | undefined;
  try {
    serialized = JSON.stringify(value);
  } catch (error) {
    throw new TypeError(`${name} must be JSON-serializable`, { cause: error });
  }
  if (typeof serialized !== "string")
    throw new TypeError(`${name} must be JSON-serializable`);
  return serialized;
}
