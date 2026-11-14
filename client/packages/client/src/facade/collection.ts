import { sealOptionalAbortSignal } from "../abort-signal.js";
import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import type { MutationAccepted } from "../mutation.js";
import { cellReader, type FacadePort, type QueryRow } from "./port.js";
import { createQueryBuilder } from "./query-builder.js";
import { registerFacade } from "./registry.js";

const MAX_SLICE_ROWS = 10_000;

function sliceBound(value: unknown, label: string, maximum: number): number {
  if (
    !Number.isSafeInteger(value) ||
    (value as number) < 0 ||
    (value as number) > maximum
  )
    throw new TypeError(
      `${label} must be an integer from 0 through ${maximum}`,
    );
  return value as number;
}

function requireRecord(value: unknown, label: string): void {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new TypeError(`${label} must be an object`);
}

/**
 * Captures the own data properties of one application record whose keys are
 * all in `allowed`. Accessors never run and the prototype is never read, so
 * the session receives only what the caller wrote down.
 */
function captureRecord(
  value: unknown,
  allowed: ReadonlySet<string>,
  label: string,
): Readonly<Record<string, unknown>> {
  requireRecord(value, label);
  const captured: Record<string, unknown> = Object.create(null) as Record<
    string,
    unknown
  >;
  for (const key of Reflect.ownKeys(value as object)) {
    if (typeof key !== "string" || !allowed.has(key))
      throw new TypeError(`${label} names a column that is not writable`);
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !("value" in descriptor))
      throw new TypeError(`${label} columns must be plain data properties`);
    captured[key] = descriptor.value;
  }
  return Object.freeze(captured);
}

/** Refuses a call that leaves out any column in `required`. */
function requireEvery(
  captured: Readonly<Record<string, unknown>>,
  required: readonly string[],
  label: string,
): void {
  for (const column of required)
    if (!Object.hasOwn(captured, column))
      throw new TypeError(`${label} must carry ${column}`);
}

/**
 * Builds the frozen facade for one generated collection. Its members close
 * over the port; nothing on the object names the session, the worker, or the
 * mount, and a method exists only for an operation the collection's writes
 * allow.
 */
export function createCollectionFacade(
  port: FacadePort,
  descriptor: GeneratedClientCollectionDescriptor,
): object {
  const columns = Object.freeze(descriptor.columns.map(({ name }) => name));
  const kinds = new Map(
    descriptor.columns.map(({ name, kind }) => [name, kind] as const),
  );
  const readers = descriptor.columns.map(({ kind }) => cellReader(kind));
  const facade: Record<string, unknown> = Object.create(null) as Record<
    string,
    unknown
  >;
  facade.query = () => createQueryBuilder(port, descriptor.id, kinds);
  facade.slice = async (
    start: unknown,
    count: unknown,
    options?: unknown,
  ): Promise<readonly QueryRow[]> => {
    const from = sliceBound(start, "start", Number.MAX_SAFE_INTEGER);
    const rows = sliceBound(count, "count", MAX_SLICE_ROWS);
    let signal: AbortSignal | undefined;
    if (options !== undefined) {
      requireRecord(options, "slice options");
      const keys = Reflect.ownKeys(options as object);
      if (keys.some((key) => key !== "signal"))
        throw new TypeError("slice options contain an unsupported option");
      signal = sealOptionalAbortSignal(
        (options as { signal?: unknown }).signal,
      );
    }
    const values = await port.slice(descriptor.id, from, rows, columns, signal);
    return Object.freeze(
      values.map((row) => {
        const record: Record<string, unknown> = Object.create(null) as Record<
          string,
          unknown
        >;
        for (let index = 0; index < columns.length; index += 1) {
          const column = columns[index];
          if (column === undefined) continue;
          const reader = readers[index];
          record[column] =
            reader === undefined ? row[index] : reader(row[index]);
        }
        return Object.freeze(record);
      }),
    );
  };
  const operations = new Set(descriptor.writes?.operations);
  const writable = new Set(descriptor.writes?.writableColumns);
  const keyColumns = new Set(descriptor.primaryKey);
  const captureKey = (value: unknown, label: string) => {
    const key = captureRecord(value, keyColumns, label);
    requireEvery(key, descriptor.primaryKey, label);
    return key;
  };
  if (operations.has("insert")) {
    // An insert carries every column: the key and everything the collection
    // may write. A column that is neither can never be inserted.
    const omitted = columns
      .filter((column) => !keyColumns.has(column) && !writable.has(column))
      .sort();
    const allowed = new Set([...keyColumns, ...writable]);
    facade.insert = async (row: unknown): Promise<MutationAccepted> => {
      if (omitted.length > 0)
        throw new TypeError(
          `${descriptor.id} cannot insert: its writableColumns omit ${omitted.join(", ")}, and an insert writes every non-key column`,
        );
      const values = captureRecord(row, allowed, "insert row");
      requireEvery(values, columns, "insert row");
      const key: Record<string, unknown> = Object.create(null) as Record<
        string,
        unknown
      >;
      for (const column of descriptor.primaryKey) key[column] = values[column];
      return port.mutate(descriptor.id, "insert", Object.freeze(key), values);
    };
  }
  if (operations.has("update"))
    facade.update = async (
      key: unknown,
      patch: unknown,
    ): Promise<MutationAccepted> => {
      const captured = captureKey(key, "update key");
      const values = captureRecord(patch, writable, "update patch");
      if (Object.keys(values).length === 0)
        throw new TypeError("update patch must change at least one column");
      return port.mutate(descriptor.id, "update", captured, values);
    };
  if (operations.has("delete"))
    facade.delete = async (key: unknown): Promise<MutationAccepted> =>
      port.mutate(
        descriptor.id,
        "delete",
        captureKey(key, "delete key"),
        undefined,
      );
  Object.freeze(facade);
  registerFacade(facade, descriptor.id);
  return facade;
}

/** The frozen `db` record: one facade per generated collection. */
export function createCollectionFacades(
  port: FacadePort,
  descriptors: readonly GeneratedClientCollectionDescriptor[],
): Readonly<Record<string, object>> {
  const facades: Record<string, object> = Object.create(null) as Record<
    string,
    object
  >;
  for (const descriptor of descriptors)
    facades[descriptor.id] = createCollectionFacade(port, descriptor);
  return Object.freeze(facades);
}
