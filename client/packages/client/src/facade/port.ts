import {
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  type QueryExecutionOptions,
  type QueryResult,
  type QuerySpecification,
  type QueryWindow,
  type QueryWindowResult,
} from "@calque/core";

import { sealOptionalAbortSignal } from "../abort-signal.js";
import type {
  GeneratedClientColumnKind,
  GeneratedClientCrudOperation,
} from "../generated-schema.js";
import type { MutationResult } from "../mutation.js";
import type { LiveQueryResource } from "../resource.js";
import { uuidText } from "../uuid.js";

/** One materialized result row: output names to sealed values. */
export type QueryRow = Readonly<Record<string, unknown>>;

/**
 * The output columns whose engine values the application reads in another
 * form, each with the function that converts one value.
 */
export type CellReaders = ReadonlyMap<string, (value: unknown) => unknown>;

export interface QueryWindowRows {
  readonly offset: number;
  readonly total: number;
  readonly rows: readonly QueryRow[];
}

export interface SealedQueryRunOptions {
  readonly maxRows?: number;
  readonly maxBytes?: number;
  readonly signal?: AbortSignal;
}

export interface SealedQueryWindowOptions {
  readonly maxBytes?: number;
  readonly signal?: AbortSignal;
}

/**
 * What a collection facade needs from the session that owns it. The facade
 * never sees the worker, the mount, or a generation; it hands the session an
 * inert specification and the collections it depends on, and gets back sealed
 * rows or a sealed live resource.
 */
export interface FacadePort {
  /** Column kinds of one served collection by name, in declaration order. */
  readonly columnsOf: (
    collection: string,
  ) => ReadonlyMap<string, GeneratedClientColumnKind> | undefined;
  readonly runQuery: (
    specification: QuerySpecification,
    options: SealedQueryRunOptions,
  ) => Promise<QueryResult>;
  readonly runWindow: (
    specification: QuerySpecification,
    window: QueryWindow,
    options: SealedQueryWindowOptions,
  ) => Promise<QueryWindowResult>;
  /**
   * `dependencies` are the collections whose publications re-run the query;
   * `readers` convert the output columns `materializeRows` hands over.
   */
  readonly subscribeQuery: (
    specification: QuerySpecification,
    dependencies: readonly string[],
    options: QueryExecutionOptions,
    readers: CellReaders,
  ) => LiveQueryResource<readonly QueryRow[]>;
  readonly subscribeWindow: (
    specification: QuerySpecification,
    dependencies: readonly string[],
    window: QueryWindow,
    options: QueryExecutionOptions,
    readers: CellReaders,
  ) => LiveQueryResource<QueryWindowRows>;
  readonly slice: (
    collection: string,
    start: number,
    count: number,
    columns: readonly string[],
    signal: AbortSignal | undefined,
  ) => Promise<readonly (readonly unknown[])[]>;
  /**
   * `key` holds exactly the collection's key columns; `values` is the patch
   * for `update`, the whole row for `insert`, and absent for `delete`. Both
   * are captured plain data the facade already checked against the
   * collection's writes.
   */
  readonly mutate: (
    collection: string,
    operation: GeneratedClientCrudOperation,
    key: Readonly<Record<string, unknown>>,
    values: Readonly<Record<string, unknown>> | undefined,
  ) => Promise<MutationResult>;
}

const RUN_OPTION_KEYS = new Set(["maxRows", "maxBytes", "signal"]);
const LIVE_OPTION_KEYS = new Set(["maxRows", "maxBytes"]);
const LIVE_WINDOW_OPTION_KEYS = new Set(["maxBytes"]);
const WINDOW_OPTION_KEYS = new Set(["maxBytes", "signal"]);

function boundedInteger(
  value: unknown,
  maximum: number,
  label: string,
): number | undefined {
  if (value === undefined) return undefined;
  if (
    !Number.isSafeInteger(value) ||
    (value as number) < 1 ||
    (value as number) > maximum
  )
    throw new TypeError(
      `${label} must be an integer from 1 through ${maximum}`,
    );
  return value as number;
}

function captureOptions(
  value: unknown,
  allowed: ReadonlySet<string>,
  label: string,
): Readonly<Record<string, unknown>> {
  if (value === undefined) return Object.freeze({});
  if (typeof value !== "object" || value === null)
    throw new TypeError(`${label} must be an object`);
  const captured: Record<string, unknown> = Object.create(null) as Record<
    string,
    unknown
  >;
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !allowed.has(key))
      throw new TypeError(`${label} contains an unsupported option`);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !("value" in descriptor))
      throw new TypeError(`${label} options must be plain data properties`);
    captured[key] = descriptor.value;
  }
  return captured;
}

export function sealRunOptions(value: unknown): SealedQueryRunOptions {
  const captured = captureOptions(value, RUN_OPTION_KEYS, "query options");
  const maxRows = boundedInteger(
    captured.maxRows,
    MAX_QUERY_RESULT_ROWS,
    "maxRows",
  );
  const maxBytes = boundedInteger(
    captured.maxBytes,
    MAX_QUERY_RESULT_BYTES,
    "maxBytes",
  );
  const signal = sealOptionalAbortSignal(captured.signal);
  return Object.freeze({
    ...(maxRows === undefined ? {} : { maxRows }),
    ...(maxBytes === undefined ? {} : { maxBytes }),
    ...(signal === undefined ? {} : { signal }),
  });
}

export function sealWindowOptions(value: unknown): SealedQueryWindowOptions {
  const captured = captureOptions(value, WINDOW_OPTION_KEYS, "window options");
  const maxBytes = boundedInteger(
    captured.maxBytes,
    MAX_QUERY_RESULT_BYTES,
    "maxBytes",
  );
  const signal = sealOptionalAbortSignal(captured.signal);
  return Object.freeze({
    ...(maxBytes === undefined ? {} : { maxBytes }),
    ...(signal === undefined ? {} : { signal }),
  });
}

export function sealWindow(offset: unknown, count: unknown): QueryWindow {
  if (!Number.isSafeInteger(offset) || (offset as number) < 0)
    throw new TypeError("window offset must be a non-negative safe integer");
  if (
    !Number.isSafeInteger(count) ||
    (count as number) < 0 ||
    (count as number) > MAX_QUERY_RESULT_ROWS
  )
    throw new TypeError(
      `window count must be an integer from 0 through ${MAX_QUERY_RESULT_ROWS}`,
    );
  return Object.freeze({ offset: offset as number, count: count as number });
}

export function sealLiveOptions(value: unknown): QueryExecutionOptions {
  const captured = captureOptions(
    value,
    LIVE_OPTION_KEYS,
    "live query options",
  );
  const maxRows = boundedInteger(
    captured.maxRows,
    MAX_QUERY_RESULT_ROWS,
    "maxRows",
  );
  const maxBytes = boundedInteger(
    captured.maxBytes,
    MAX_QUERY_RESULT_BYTES,
    "maxBytes",
  );
  return Object.freeze({
    ...(maxRows === undefined ? {} : { maxRows }),
    ...(maxBytes === undefined ? {} : { maxBytes }),
  });
}

export function sealLiveWindowOptions(value: unknown): QueryExecutionOptions {
  const captured = captureOptions(
    value,
    LIVE_WINDOW_OPTION_KEYS,
    "live window options",
  );
  const maxBytes = boundedInteger(
    captured.maxBytes,
    MAX_QUERY_RESULT_BYTES,
    "maxBytes",
  );
  return Object.freeze(maxBytes === undefined ? {} : { maxBytes });
}

export function materializeWindow(
  result: QueryWindowResult,
  readers: CellReaders,
): QueryWindowRows {
  return Object.freeze({
    offset: result.offset,
    total: result.total,
    rows: materializeRows(result, readers),
  });
}

/** A uuid cell as the application reads it: text, or null. */
function uuidCell(value: unknown): unknown {
  return value instanceof Uint8Array ? uuidText(value) : value;
}

/**
 * An `int2` or `int4` cell, which the engine holds as an int64, as the
 * application reads it: a number, or null.
 */
function int32Cell(value: unknown): unknown {
  return typeof value === "bigint" ? Number(value) : value;
}

/**
 * How the application reads a column of `kind`, when the engine holds it in
 * another form: a uuid as its text, an `int32` as a number.
 */
export function cellReader(
  kind: GeneratedClientColumnKind | undefined,
): ((value: unknown) => unknown) | undefined {
  if (kind === "uuid") return uuidCell;
  if (kind === "int32") return int32Cell;
  return undefined;
}

/**
 * Turns a sealed columnar result into frozen output-name keyed rows, with
 * each column `readers` names converted.
 */
export function materializeRows(
  result: QueryResult,
  readers: CellReaders,
): readonly QueryRow[] {
  const columns = result.columns;
  const read = columns.map((column) => readers.get(column));
  return Object.freeze(
    result.rows.map((values) => {
      const row: Record<string, unknown> = Object.create(null) as Record<
        string,
        unknown
      >;
      for (let index = 0; index < columns.length; index += 1) {
        const column = columns[index];
        if (column === undefined) continue;
        const value = values[index];
        const reader = read[index];
        row[column] = reader === undefined ? value : reader(value);
      }
      return Object.freeze(row);
    }),
  );
}
