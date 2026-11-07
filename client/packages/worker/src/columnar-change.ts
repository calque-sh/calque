/**
 * Turns one change response, framed `sync.change.v1.ChangeStreamFrame`s, into
 * one `CanonicalBatch` for `OverlayEngine`. `applyCanonical` moves the mount
 * from one canonical cursor to the next, so a batch that does not end on a
 * transaction fence, or that carries a value the artifact's column cannot
 * hold, is refused before it reaches the engine.
 */
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ChangeStreamState,
  change,
  decodePrimaryKey,
  decodeReceivedChangeFrames,
  type ExpectedSourceGeneration,
  type ExpectedTableStart,
  type RevocationBarrierVerificationKey,
} from "@calque/protocol";
import type {
  CellValue,
  Operation,
} from "@calque/protocol/generated/change_stream";
import { OperationKind } from "@calque/protocol/generated/change_stream";
import { directColumnarTimestampText, equalBytes } from "@calque/core";
import type {
  CanonicalBatch,
  CanonicalOperation,
  ColumnChange,
  DirectColumnarColumnKind,
  DirectColumnarSchema,
  KeyPart,
  OptimisticAction,
  OptimisticMutation,
  SqlValue,
} from "@calque/core";

import { RuntimeError } from "./runtime-error.js";

const MAX_COLUMNAR_CHANGE_BATCH_BYTES = 16 * 1024 * 1024;
const MAX_COLUMNAR_CHANGE_FRAME_BYTES = 4 * 1024 * 1024;

/** One wire column bound to one direct-columnar column by name. */
interface ColumnarChangeColumnBinding {
  readonly columnId: number;
  readonly name: string;
}

/**
 * One wire table bound to one direct-columnar table. `tableId` is the server's
 * identifier and `collection` is the artifact's table name; they are separate
 * because the artifact is built for a scope, not for a stream.
 */
export interface ColumnarChangeTableBinding {
  readonly tableId: string;
  readonly collection: string;
  readonly columns: readonly ColumnarChangeColumnBinding[];
  /** Wire column ids in the table's key order. */
  readonly primaryKey: readonly number[];
}

export interface ColumnarChangeContract {
  readonly planId: string;
  readonly generation: ExpectedSourceGeneration;
  readonly tables: readonly ColumnarChangeTableBinding[];
  /**
   * The authorization authority's Ed25519 key for revocation barriers. It must
   * come from the application's configuration, never from the plan: a key the
   * change server chose would let that server mint the revocations it
   * delivers. Absent, every barrier is refused as a stream violation.
   */
  readonly revocationBarrierKey?: RevocationBarrierVerificationKey;
}

interface CompiledColumn {
  readonly ordinal: number;
  readonly name: string;
  readonly kind: DirectColumnarColumnKind;
  readonly nullable: boolean;
}

interface CompiledTable {
  readonly tableId: string;
  readonly collection: string;
  readonly width: number;
  readonly byColumnId: ReadonlyMap<number, CompiledColumn>;
  /**
   * Wire column ids in key order, matched against the decoded key part order.
   */
  readonly primaryKey: readonly number[];
}

export interface CompiledColumnarChangeBinding {
  readonly planId: string;
  readonly generation: ExpectedSourceGeneration;
  readonly tableIds: readonly string[];
  readonly byTableId: ReadonlyMap<string, CompiledTable>;
  readonly revocationBarrierKey?: RevocationBarrierVerificationKey;
}

interface DecodedColumnarChange {
  readonly batch: CanonicalBatch;
  readonly throughCursorInclusive: Uint8Array;
  readonly currentlyUpToDate: boolean;
  readonly transactionCount: number;
  readonly operationCount: number;
  /**
   * The newest `commitTimeUnixMicros` in the batch, or `null` for a batch
   * without transactions, so a caller can time commit-to-publication without
   * a second decode.
   */
  readonly latestCommitTimeUnixMicros: bigint | null;
}

/**
 * A fenced catch-up request's terms: the activation cursor it was fenced at
 * and the per-table starts it named. `ChangeStreamState` holds the response to
 * them exactly; a live poll carries none.
 */
export interface ColumnarChangeFence {
  readonly cursor: Uint8Array;
  readonly tableStarts: readonly ExpectedTableStart[];
}

const KEY_KINDS_BY_COLUMN: Readonly<
  Partial<
    Record<DirectColumnarColumnKind, readonly CellValue["kind"]["case"][]>
  >
> = Object.freeze({
  int64: ["int64Value"],
  uint64: ["int64Value"],
  timestamp: ["int64Value"],
  bool: ["boolValue"],
  utf8: ["textValue"],
  dict: ["textValue"],
  bytes: ["blobValue"],
  uuid: ["uuid16"],
});

function invalid(message: string): never {
  throw new RuntimeError("CHANGE_STREAM_INVALID", message);
}

/**
 * Binds a change contract to an opened artifact's schema once, at mount time,
 * so the live path need not check it per operation. It also refuses an
 * artifact column with no wire column, which would leave a hole in every
 * inserted row.
 */
export function compileColumnarChangeBinding(
  schema: DirectColumnarSchema,
  contract: ColumnarChangeContract,
): CompiledColumnarChangeBinding {
  if (contract.planId.length === 0) invalid("change contract has no plan id");
  if (contract.tables.length === 0) invalid("change contract binds no tables");
  const byTableId = new Map<string, CompiledTable>();
  const collections = new Set<string>();
  for (const binding of contract.tables) {
    if (byTableId.has(binding.tableId))
      invalid(`change contract binds ${binding.tableId} twice`);
    if (collections.has(binding.collection))
      invalid(`change contract binds collection ${binding.collection} twice`);
    collections.add(binding.collection);
    const table = schema.tables.find(
      (candidate) => candidate.name === binding.collection,
    );
    if (!table)
      invalid(
        `change contract binds ${binding.tableId} to unknown collection ${binding.collection}`,
      );
    const ordinals = new Map(
      table.columns.map((column, ordinal) => [column.name, ordinal] as const),
    );
    const byColumnId = new Map<number, CompiledColumn>();
    const boundOrdinals = new Set<number>();
    for (const column of binding.columns) {
      if (!Number.isSafeInteger(column.columnId) || column.columnId <= 0)
        invalid(`${binding.tableId}.${column.name} has an invalid column id`);
      if (byColumnId.has(column.columnId))
        invalid(`${binding.tableId} binds column id ${column.columnId} twice`);
      const ordinal = ordinals.get(column.name);
      if (ordinal === undefined)
        invalid(
          `${binding.tableId} binds ${column.name}, which ${binding.collection} does not have`,
        );
      if (boundOrdinals.has(ordinal))
        invalid(`${binding.tableId} binds ${column.name} twice`);
      boundOrdinals.add(ordinal);
      const schemaColumn = table.columns[ordinal];
      if (!schemaColumn) invalid(`${binding.collection} lost a column`);
      byColumnId.set(column.columnId, {
        ordinal,
        name: column.name,
        kind: schemaColumn.type,
        nullable: schemaColumn.nullable,
      });
    }
    if (boundOrdinals.size !== table.columns.length)
      invalid(
        `${binding.tableId} binds ${boundOrdinals.size} of ${table.columns.length} ${binding.collection} columns; an insert could not carry a complete row image`,
      );
    if (binding.primaryKey.length !== table.primaryKey.length)
      invalid(
        `${binding.tableId} declares a ${binding.primaryKey.length}-part key; ${binding.collection} has ${table.primaryKey.length}`,
      );
    for (const [index, columnId] of binding.primaryKey.entries()) {
      const column = byColumnId.get(columnId);
      if (!column)
        invalid(`${binding.tableId} key part ${index} is not a bound column`);
      if (column.name !== table.primaryKey[index])
        invalid(
          `${binding.tableId} key part ${index} is ${column.name}; ${binding.collection} orders ${table.primaryKey[index]} there`,
        );
      if (column.nullable)
        invalid(`${binding.tableId} key part ${index} is nullable`);
      if (!KEY_KINDS_BY_COLUMN[column.kind])
        invalid(
          `${binding.collection}.${column.name} is ${column.kind}, which is not a primary-key type`,
        );
    }
    byTableId.set(binding.tableId, {
      tableId: binding.tableId,
      collection: binding.collection,
      width: table.columns.length,
      byColumnId,
      primaryKey: Object.freeze([...binding.primaryKey]),
    });
  }
  return Object.freeze({
    planId: contract.planId,
    generation: contract.generation,
    tableIds: Object.freeze([...byTableId.keys()].sort()),
    byTableId,
    ...(contract.revocationBarrierKey
      ? { revocationBarrierKey: contract.revocationBarrierKey }
      : {}),
  });
}

/**
 * Decodes one change response into one canonical batch. Nothing is returned
 * until the last frame has validated, so a response that is truncated,
 * reordered, mis-hashed, or ends mid-transaction costs a rejected request
 * rather than a torn mount.
 */
export async function decodeColumnarChangeBatch(
  bytes: Uint8Array,
  binding: CompiledColumnarChangeBinding,
  fromCursorExclusive: Uint8Array,
  fence?: ColumnarChangeFence,
): Promise<DecodedColumnarChange> {
  const tableStarts = expectedTableStarts(binding, fence?.tableStarts);
  if (bytes.byteLength === 0)
    invalid("change batch must contain at least one frame");
  if (bytes.byteLength > MAX_COLUMNAR_CHANGE_BATCH_BYTES)
    throw new RuntimeError(
      "CAPACITY_EXCEEDED",
      `change batch has ${bytes.byteLength} bytes, exceeding ${MAX_COLUMNAR_CHANGE_BATCH_BYTES}`,
    );
  let frames: ReturnType<typeof decodeReceivedChangeFrames>;
  try {
    frames = decodeReceivedChangeFrames(bytes, MAX_COLUMNAR_CHANGE_FRAME_BYTES);
  } catch (error) {
    invalid(String(error));
  }
  const state = new ChangeStreamState({
    planId: binding.planId,
    fromCursorExclusive,
    ...(fence ? { requestedFence: fence.cursor } : {}),
    tableStarts,
    generation: binding.generation,
    ...(binding.revocationBarrierKey
      ? { revocationBarrierKey: binding.revocationBarrierKey }
      : {}),
  });
  const transactions: CanonicalBatch["transactions"][number][] = [];
  let operations: CanonicalOperation[] = [];
  let completed: Awaited<ReturnType<ChangeStreamState["accept"]>>;
  let operationCount = 0;
  let latestCommitTimeUnixMicros: bigint | null = null;
  try {
    for (const { frame, body } of frames) {
      completed = (await state.accept(frame, body)) ?? completed;
      if (frame.body.case === "error")
        throw new RuntimeError(
          frame.body.value.resnapshotRequired
            ? "RESNAPSHOT_REQUIRED"
            : "CHANGE_STREAM_INVALID",
          frame.body.value.message,
        );
      if (frame.body.case !== "transactionFragment") continue;
      const fragment = frame.body.value;
      if (
        fragment.commitTimeUnixMicros > 0n &&
        (latestCommitTimeUnixMicros === null ||
          fragment.commitTimeUnixMicros > latestCommitTimeUnixMicros)
      )
        latestCommitTimeUnixMicros = fragment.commitTimeUnixMicros;
      operationCount += fragment.operations.length;
      for (const operation of fragment.operations)
        operations.push(decodeOperation(operation, binding));
      if (fragment.finalFragment) {
        transactions.push(
          Object.freeze({
            operations: Object.freeze(operations),
            mutationIds: Object.freeze([]),
          }),
        );
        operations = [];
      }
    }
  } catch (error) {
    if (error instanceof RuntimeError) throw error;
    invalid(String(error));
  }
  // `ChangeStreamState` refuses a `batchEnd` while a fragment is open; this
  // catches a response that stops inside a transaction.
  if (!completed || !state.terminal)
    invalid("change batch does not end at a complete batch boundary");
  // Checked after the whole batch has validated, so no row of a batch that
  // carries a barrier is applied, not even the authorized rows before it: the
  // mount that would hold them is about to be destroyed.
  const barrier = completed.revocationBarrier;
  if (barrier)
    throw new RuntimeError(
      "AUTHORIZATION_REVOKED",
      `authorization generation ${barrier.revokedAuthorizationGeneration} was revoked by barrier ${barrier.barrierId}; generation ${barrier.effectiveAuthorizationGeneration} replaces it`,
    );
  const transactionCount = transactions.length;
  // The batch end acknowledges the batch's mutations, and the overlay drops
  // an acknowledged mutation once the batch's operations are under it, so the
  // acknowledgements follow them as a transaction of no operations.
  if (completed.acknowledgedMutationIds.length > 0)
    transactions.push(
      Object.freeze({
        operations: Object.freeze([]),
        mutationIds: Object.freeze([...completed.acknowledgedMutationIds]),
      }),
    );
  return Object.freeze({
    batch: Object.freeze({ transactions: Object.freeze(transactions) }),
    throughCursorInclusive: completed.throughCursorInclusive.slice(),
    currentlyUpToDate: completed.currentlyUpToDate,
    transactionCount,
    operationCount,
    latestCommitTimeUnixMicros,
  });
}

/**
 * Decodes batches a stream carried back to back into one batch, each against
 * the cursor the previous one ends at. A batch that is refused refuses the
 * group, so its caller applies every batch or none of them.
 */
export async function decodeColumnarChangeBatches(
  batches: readonly Uint8Array[],
  binding: CompiledColumnarChangeBinding,
  fromCursorExclusive: Uint8Array,
  fence?: ColumnarChangeFence,
): Promise<DecodedColumnarChange> {
  let cursor = fromCursorExclusive;
  let last: DecodedColumnarChange | undefined;
  const transactions: CanonicalBatch["transactions"][number][] = [];
  let transactionCount = 0;
  let operationCount = 0;
  let latestCommitTimeUnixMicros: bigint | null = null;
  for (const bytes of batches) {
    last = await decodeColumnarChangeBatch(bytes, binding, cursor, fence);
    cursor = last.throughCursorInclusive;
    transactions.push(...last.batch.transactions);
    transactionCount += last.transactionCount;
    operationCount += last.operationCount;
    const latest = last.latestCommitTimeUnixMicros;
    if (
      latest !== null &&
      (latestCommitTimeUnixMicros === null ||
        latest > latestCommitTimeUnixMicros)
    )
      latestCommitTimeUnixMicros = latest;
  }
  if (!last) invalid("a change group must hold at least one batch");
  if (batches.length === 1) return last;
  return Object.freeze({
    batch: Object.freeze({ transactions: Object.freeze(transactions) }),
    throughCursorInclusive: cursor,
    currentlyUpToDate: last.currentlyUpToDate,
    transactionCount,
    operationCount,
    latestCommitTimeUnixMicros,
  });
}

/**
 * The per-table starts the response must echo: for a live poll, every bound
 * table with no snapshot cursor; with a fence, exactly one start per bound
 * table.
 */
function expectedTableStarts(
  binding: CompiledColumnarChangeBinding,
  starts: readonly ExpectedTableStart[] | undefined,
): readonly ExpectedTableStart[] {
  if (starts === undefined)
    return binding.tableIds.map((tableId) => ({ tableId }));
  const byTableId = new Map<string, ExpectedTableStart>();
  for (const start of starts) {
    if (!binding.byTableId.has(start.tableId))
      invalid(`fence names ${start.tableId}, which the contract does not bind`);
    if (byTableId.has(start.tableId))
      invalid(`fence names ${start.tableId} twice`);
    byTableId.set(start.tableId, start);
  }
  return binding.tableIds.map((tableId) => {
    const start = byTableId.get(tableId);
    if (!start) invalid(`fence names no start for ${tableId}`);
    return start.snapshotCursor
      ? { tableId, snapshotCursor: start.snapshotCursor.slice() }
      : { tableId };
  });
}

/**
 * Decodes a mutation's `canonical_input`, one canonical
 * `sync.change.v1.Operation` per affected row, into the optimistic actions the
 * overlay applies, through the decoder canonical changes use. A truncate has
 * no optimistic form and is refused, and so is a non-canonical encoding,
 * which the server would refuse too.
 */
export function decodeOptimisticMutation(
  mutationId: string,
  operations: readonly Uint8Array[],
  binding: CompiledColumnarChangeBinding,
): OptimisticMutation {
  if (operations.length === 0)
    throw new RuntimeError(
      "INVALID_REQUEST",
      "a mutation must carry at least one operation",
    );
  const actions = operations.map((bytes, index): OptimisticAction => {
    let operation: Operation;
    try {
      operation = fromBinary(change.OperationSchema, bytes, {
        readUnknownFields: false,
      });
    } catch (error) {
      throw new RuntimeError(
        "INVALID_REQUEST",
        `mutation operation ${index} is malformed: ${String(error)}`,
      );
    }
    const canonical = toBinary(change.OperationSchema, operation, {
      writeUnknownFields: false,
    });
    if (!equalBytes(canonical, bytes))
      throw new RuntimeError(
        "INVALID_REQUEST",
        `mutation operation ${index} is not canonical protobuf`,
      );
    const decoded = decodeOperation(operation, binding);
    switch (decoded.kind) {
      // `insert` and `delete` already are the optimistic shape, frozen by the
      // decoder; only `update` is renamed to the overlay's `patch`.
      case "insert":
      case "delete":
        return decoded;
      case "update":
        return Object.freeze({
          kind: "patch",
          collection: decoded.collection,
          primaryKey: decoded.primaryKey,
          key: decoded.key,
          changes: Object.freeze(
            decoded.changed.map((change_) =>
              Object.freeze({
                column: change_.column,
                value: change_.value,
                mode: "set" as const,
              }),
            ),
          ),
        });
      case "truncate":
        throw new RuntimeError(
          "INVALID_REQUEST",
          `mutation operation ${index} is a truncate, which has no optimistic form`,
        );
    }
  });
  return Object.freeze({ mutationId, actions: Object.freeze(actions) });
}

function decodeOperation(
  operation: Operation,
  binding: CompiledColumnarChangeBinding,
): CanonicalOperation {
  const table = binding.byTableId.get(operation.tableId);
  if (!table)
    invalid(`operation references unsubscribed table ${operation.tableId}`);
  if (operation.kind === OperationKind.TRUNCATE_AUTHORIZED_TABLE) {
    if (operation.payload.case !== "truncate")
      invalid(`${operation.tableId} truncate has no truncate payload`);
    return Object.freeze({ kind: "truncate", collection: table.collection });
  }
  const key = decodeKey(operation.primaryKey, table);
  switch (operation.kind) {
    case OperationKind.INSERT_FULL_ROW: {
      if (operation.payload.case !== "insert")
        invalid(`${operation.tableId} insert has no full row`);
      const values: SqlValue[] = new Array<SqlValue>(table.width);
      const seen = new Set<number>();
      for (const column of operation.payload.value.columns) {
        const bound = requireColumn(table, column.columnId);
        if (seen.has(bound.ordinal))
          invalid(`${operation.tableId} insert repeats column ${bound.name}`);
        seen.add(bound.ordinal);
        values[bound.ordinal] = cellFor(bound, column.value, operation.tableId);
      }
      if (seen.size !== table.width)
        invalid(
          `${operation.tableId} insert carries ${seen.size} of ${table.width} columns`,
        );
      return Object.freeze({
        kind: "insert",
        collection: table.collection,
        primaryKey: operation.primaryKey.slice(),
        key,
        values: Object.freeze(values),
      });
    }
    case OperationKind.UPDATE_PATCH: {
      if (operation.payload.case !== "update")
        invalid(`${operation.tableId} update has no patch`);
      const changed: ColumnChange[] = [];
      const seen = new Set<number>();
      for (const column of operation.payload.value.changedColumns) {
        const bound = requireColumn(table, column.columnId);
        if (seen.has(bound.ordinal))
          invalid(`${operation.tableId} patch repeats column ${bound.name}`);
        seen.add(bound.ordinal);
        changed.push(
          Object.freeze({
            column: bound.ordinal,
            value: cellFor(bound, column.value, operation.tableId),
          }),
        );
      }
      if (changed.length === 0)
        invalid(`${operation.tableId} patch changes no column`);
      return Object.freeze({
        kind: "update",
        collection: table.collection,
        primaryKey: operation.primaryKey.slice(),
        key,
        changed: Object.freeze(changed),
      });
    }
    case OperationKind.DELETE_BY_KEY: {
      if (operation.payload.case !== "delete")
        invalid(`${operation.tableId} delete has no delete payload`);
      return Object.freeze({
        kind: "delete",
        collection: table.collection,
        primaryKey: operation.primaryKey.slice(),
        key,
      });
    }
    default:
      invalid(`${operation.tableId} carries an unsupported operation kind`);
  }
}

function requireColumn(table: CompiledTable, columnId: number): CompiledColumn {
  const column = table.byColumnId.get(columnId);
  if (!column) invalid(`${table.tableId} has no column with id ${columnId}`);
  return column;
}

/**
 * Decodes the canonical primary key into the parts `OverlayBase.lookupKey`
 * takes. The part order is checked rather than assumed, because a permuted key
 * resolves to no row: a silent `RESNAPSHOT_REQUIRED` instead of a named
 * protocol violation.
 */
function decodeKey(
  encoded: Uint8Array,
  table: CompiledTable,
): readonly KeyPart[] {
  let parts;
  try {
    parts = decodePrimaryKey(encoded).parts;
  } catch (error) {
    invalid(`${table.tableId} primary key is invalid: ${String(error)}`);
  }
  if (
    parts.length !== table.primaryKey.length ||
    parts.some((part, index) => part.columnId !== table.primaryKey[index])
  )
    invalid(`${table.tableId} primary key does not match the bound key order`);
  return Object.freeze(
    parts.map((part) => {
      const column = requireColumn(table, part.columnId);
      const value = part.value;
      if (!value) invalid(`${table.tableId} key part has no value`);
      const permitted = KEY_KINDS_BY_COLUMN[column.kind];
      if (!permitted || !permitted.includes(value.kind.case))
        invalid(
          `${table.tableId}.${column.name} key part is ${String(value.kind.case)}, which a ${column.kind} column does not accept`,
        );
      return keyPartFor(column, value, table.tableId);
    }),
  );
}

function keyPartFor(
  column: CompiledColumn,
  value: CellValue,
  tableId: string,
): KeyPart {
  const kind = value.kind;
  switch (column.kind) {
    case "int64":
      if (kind.case !== "int64Value") invalid("unreachable key part kind");
      return kind.value;
    case "timestamp":
      // A timestamp key part is microseconds on the wire and the artifact's
      // own key index hashes the text form, so it is converted here rather
      // than probed as an integer that would resolve to no row.
      if (kind.case !== "int64Value") invalid("unreachable key part kind");
      return directColumnarTimestampText(kind.value);
    case "uint64":
      if (kind.case !== "int64Value") invalid("unreachable key part kind");
      if (kind.value < 0n)
        invalid(`${tableId}.${column.name} key part is negative`);
      return kind.value;
    case "bool":
      if (kind.case !== "boolValue") invalid("unreachable key part kind");
      return kind.value;
    case "bytes":
      if (kind.case !== "blobValue") invalid("unreachable key part kind");
      return kind.value.slice();
    case "uuid":
      if (kind.case !== "uuid16") invalid("unreachable key part kind");
      return kind.value.slice();
    case "utf8":
    case "dict":
      // `decodePrimaryKey` has already refused a text part that is not UTF-8.
      if (kind.case !== "textValue") invalid("unreachable key part kind");
      return kind.value;
    default:
      invalid("unreachable key part kind");
  }
}

/**
 * Converts one wire cell into the value the artifact's own column reader
 * produces for that kind. A close conversion, such as a `number` where the
 * base yields a `bigint`, would make the overlay answer differently from a
 * fresh build of the same rows.
 */
function cellFor(
  column: CompiledColumn,
  value: CellValue | undefined,
  tableId: string,
): SqlValue {
  if (!value) invalid(`${tableId}.${column.name} has no cell value`);
  const kind = value.kind;
  if (kind.case === "nullValue") {
    if (!column.nullable) invalid(`${tableId}.${column.name} is not nullable`);
    return null;
  }
  const mismatch = (): never =>
    invalid(
      `${tableId}.${column.name} is ${column.kind} and cannot hold ${String(kind.case)}`,
    );
  switch (column.kind) {
    case "int64":
      return kind.case === "int64Value" ? kind.value : mismatch();
    case "uint64":
      if (kind.case === "uint64Value") return kind.value;
      if (kind.case === "int64Value" && kind.value >= 0n) return kind.value;
      return mismatch();
    case "timestamp":
      // The column reads back text, not microseconds. Storing the integer
      // would make the same row answer differently through the overlay than
      // through a fresh build of it.
      if (kind.case === "timestampUnixMicros")
        return directColumnarTimestampText(kind.value);
      if (kind.case === "int64Value")
        return directColumnarTimestampText(kind.value);
      return mismatch();
    case "f64":
      if (kind.case !== "float64Value") return mismatch();
      if (!Number.isFinite(kind.value))
        invalid(`${tableId}.${column.name} is not finite`);
      return kind.value;
    case "bool":
      return kind.case === "boolValue" ? kind.value : mismatch();
    case "utf8":
    case "dict":
      return kind.case === "textValue" ? kind.value : mismatch();
    case "decimal":
      return kind.case === "decimalText" ? kind.value : mismatch();
    case "date":
      return kind.case === "dateIso8601" ? kind.value : mismatch();
    case "time":
      return kind.case === "timeIso8601" ? kind.value : mismatch();
    case "interval":
      return kind.case === "intervalIso8601" ? kind.value : mismatch();
    case "bytes":
      return kind.case === "blobValue" ? kind.value.slice() : mismatch();
    case "uuid":
      if (kind.case !== "uuid16") return mismatch();
      if (kind.value.byteLength !== 16)
        invalid(`${tableId}.${column.name} is not 16 bytes`);
      return kind.value.slice();
    case "json": {
      if (kind.case !== "jsonText") return mismatch();
      try {
        // The base column parses its stored text, so the overlay must hold the
        // parsed value too: a query that reads through the overlay would
        // otherwise see a string where the base gives an object.
        return JSON.parse(kind.value) as SqlValue;
      } catch {
        return invalid(`${tableId}.${column.name} is not valid JSON`);
      }
    }
  }
}
