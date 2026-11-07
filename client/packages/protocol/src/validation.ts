import type {
  BatchEnd,
  BatchStart,
  CellValue,
  ChangeRequest,
  ChangeStreamFrame,
  ColumnValue,
  Cursor as ChangeCursor,
  Operation,
  RevocationBarrier,
  SourceGeneration,
  StreamError,
  TransactionFragment,
} from "./gen/change_stream_pb.js";
import {
  NullValue,
  OperationKind,
  StreamErrorCode,
} from "./gen/change_stream_pb.js";
import type {
  Cursor as MutationCursor,
  MutationBatchRequest,
  MutationBatchResponse,
  MutationEnvelope,
  MutationOutcome,
} from "./gen/mutation_pb.js";
import { MutationOutcomeCode } from "./gen/mutation_pb.js";
import { bytesKey, sameStringSet } from "./compare.js";
import { decodePrimaryKey } from "./primary-key.js";

const PROTOCOL_VERSION = 1;

const MAX_IDENTIFIER_BYTES = 512;
const MAX_CURSOR_TOKEN_BYTES = 2_048;
const MAX_REQUEST_TABLES = 1_024;
const MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES = 8 * 1_024;
const MIN_CHANGE_BATCH_BYTES = 1_024n;
const MAX_CHANGE_BATCH_BYTES = 16n * 1_024n * 1_024n;
const MAX_CHANGE_TRANSACTIONS = 10_000;
const MAX_CHANGE_BATCH_OPERATIONS = 100_000;
const MIN_LONG_POLL_TIMEOUT_MS = 1_000;
const MAX_LONG_POLL_TIMEOUT_MS = 120_000;
const MAX_PENDING_MUTATION_IDS = 256;
const MAX_STREAM_ID_BYTES = 64;
const MAX_CLIENT_IDENTIFIER_BYTES = 1_024;
const MAX_BATCH_MUTATIONS = 100;
const MAX_CANONICAL_INPUT_BYTES = 1_024 * 1_024;
const MAX_BATCH_INPUT_BYTES = 4 * 1_024 * 1_024;
const MAX_FOOTPRINT_ROWS = 1_024;
const MAX_FOOTPRINT_TABLES = 256;
const MAX_EXPECTED_VERSIONS = 1_024;
const MAX_MUTATION_SCOPE_TOKEN_BYTES = 1_024 * 1_024;
const MAX_COMMAND_NAME_BYTES = 128;
const MAX_CONFLICT_KIND_BYTES = 128;
const MAX_CONFLICT_PAYLOAD_BYTES = 8 * 1_024;
const UUID_BYTES = 16;
const SHA256_BYTES = 32;
const MAX_BARRIER_IDENTIFIER_BYTES = 128;
export const REVOCATION_BARRIER_SIGNATURE_BYTES = 64;

export class ProtocolValidationError extends Error {
  readonly path: string;

  constructor(path: string, message: string) {
    super(`${path}: ${message}`);
    this.name = "ProtocolValidationError";
    this.path = path;
  }
}

function fail(path: string, message: string): never {
  throw new ProtocolValidationError(path, message);
}

function requireVersion(value: number, path: string): void {
  if (value !== PROTOCOL_VERSION)
    fail(path, `unsupported protocol version ${value}`);
}

function requireString(value: string, path: string): void {
  if (value.length === 0) fail(path, "must not be empty");
}

function requireIdentifier(value: string, path: string): void {
  if (
    value.length === 0 ||
    value.length > MAX_IDENTIFIER_BYTES ||
    !/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/u.test(value)
  ) {
    fail(
      path,
      `must be a ${MAX_IDENTIFIER_BYTES}-byte ASCII identifier beginning with an alphanumeric character`,
    );
  }
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function requireClientIdentifier(value: string, path: string): void {
  if (
    value.length === 0 ||
    utf8Length(value) > MAX_CLIENT_IDENTIFIER_BYTES ||
    /\p{Cc}/u.test(value)
  ) {
    fail(
      path,
      `must contain 1..${MAX_CLIENT_IDENTIFIER_BYTES} UTF-8 bytes without control characters`,
    );
  }
}

function requireCommandName(value: string, path: string): void {
  if (
    value.length === 0 ||
    value.length > MAX_COMMAND_NAME_BYTES ||
    !/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/u.test(value)
  ) {
    fail(path, "contains an invalid command name");
  }
}

function requireBytes(value: Uint8Array, path: string, maximum?: number): void {
  if (value.byteLength === 0) fail(path, "must not be empty");
  if (maximum !== undefined && value.byteLength > maximum) {
    fail(path, `must contain at most ${maximum} bytes`);
  }
}

function requireLength(
  value: Uint8Array,
  expected: number,
  path: string,
): void {
  if (value.byteLength !== expected)
    fail(path, `must contain exactly ${expected} bytes`);
}

function requireUniqueStrings(values: readonly string[], path: string): void {
  const seen = new Set<string>();
  for (const [index, value] of values.entries()) {
    requireString(value, `${path}[${index}]`);
    if (seen.has(value)) fail(`${path}[${index}]`, "must be unique");
    seen.add(value);
  }
}

function validateCursor(
  cursor: ChangeCursor | MutationCursor | undefined,
  path: string,
): void {
  if (cursor === undefined) fail(path, "is required");
  requireBytes(cursor.token, `${path}.token`, MAX_CURSOR_TOKEN_BYTES);
}

function validateGeneration(
  generation: SourceGeneration | undefined,
  path: string,
): void {
  if (generation === undefined) fail(path, "is required");
  requireIdentifier(generation.sourceEpoch, `${path}.sourceEpoch`);
  requireLength(
    generation.authorizationScopeHash,
    SHA256_BYTES,
    `${path}.authorizationScopeHash`,
  );
}

function validateColumn(column: ColumnValue, path: string): void {
  if (column.columnId === 0) fail(`${path}.columnId`, "must not be zero");
  if (column.value === undefined) fail(`${path}.value`, "is required");
  validateCell(column.value, `${path}.value`);
}

function validateColumns(columns: readonly ColumnValue[], path: string): void {
  const seen = new Set<number>();
  for (const [index, column] of columns.entries()) {
    validateColumn(column, `${path}[${index}]`);
    if (seen.has(column.columnId))
      fail(`${path}[${index}].columnId`, "must be unique in a row");
    seen.add(column.columnId);
  }
}

function validateCell(cell: CellValue, path: string): void {
  if (cell.kind.case === undefined) fail(`${path}.kind`, "is required");
  if (
    cell.kind.case === "nullValue" &&
    cell.kind.value !== NullValue.NULL_VALUE
  ) {
    fail(`${path}.kind.nullValue`, "contains an unknown enum value");
  }
  if (cell.kind.case === "uuid16")
    requireLength(cell.kind.value, UUID_BYTES, `${path}.kind.uuid16`);
  if (cell.kind.case === "float64Value" && !Number.isFinite(cell.kind.value)) {
    fail(
      `${path}.kind.float64Value`,
      "must be finite until special float semantics are specified",
    );
  }
}

function validateOperation(operation: Operation, path: string): void {
  requireIdentifier(operation.tableId, `${path}.tableId`);
  const expectedPayload = new Map<OperationKind, Operation["payload"]["case"]>([
    [OperationKind.INSERT_FULL_ROW, "insert"],
    [OperationKind.UPDATE_PATCH, "update"],
    [OperationKind.DELETE_BY_KEY, "delete"],
    [OperationKind.TRUNCATE_AUTHORIZED_TABLE, "truncate"],
  ]).get(operation.kind);
  if (expectedPayload === undefined)
    fail(`${path}.kind`, `unknown or unspecified value ${operation.kind}`);
  if (operation.payload.case !== expectedPayload) {
    fail(
      `${path}.payload`,
      `must be ${expectedPayload} for operation kind ${operation.kind}`,
    );
  }

  if (operation.kind === OperationKind.TRUNCATE_AUTHORIZED_TABLE) {
    if (operation.primaryKey.byteLength !== 0)
      fail(`${path}.primaryKey`, "must be empty for truncate");
  } else {
    decodePrimaryKey(operation.primaryKey, `${path}.primaryKey`);
  }

  if (operation.payload.case === "insert") {
    if (operation.payload.value.columns.length === 0) {
      fail(`${path}.insert.columns`, "must contain a full row");
    }
    validateColumns(operation.payload.value.columns, `${path}.insert.columns`);
  }
  if (operation.payload.case === "update") {
    if (operation.payload.value.changedColumns.length === 0) {
      fail(
        `${path}.update.changedColumns`,
        "must contain at least one changed column",
      );
    }
    validateColumns(
      operation.payload.value.changedColumns,
      `${path}.update.changedColumns`,
    );
  }
  if (operation.payload.case === "truncate") {
    requireString(operation.payload.value.reason, `${path}.truncate.reason`);
  }
}

function validateBatchStart(batch: BatchStart, path: string): void {
  validateGeneration(batch.generation, `${path}.generation`);
  validateCursor(batch.fromCursorExclusive, `${path}.fromCursorExclusive`);
  if (batch.requestedFence !== undefined)
    validateCursor(batch.requestedFence, `${path}.requestedFence`);
  if (batch.tableIds.length === 0 || batch.tableIds.length > MAX_REQUEST_TABLES)
    fail(`${path}.tableIds`, `must contain 1..${MAX_REQUEST_TABLES} tables`);
  batch.tableIds.forEach((tableId, index) =>
    requireIdentifier(tableId, `${path}.tableIds[${index}]`),
  );
  requireUniqueStrings(batch.tableIds, `${path}.tableIds`);
  requireIdentifier(batch.planId, `${path}.planId`);
  requireBytes(batch.streamId, `${path}.streamId`, MAX_STREAM_ID_BYTES);
  if (
    batch.tableStarts.length === 0 ||
    batch.tableStarts.length > MAX_REQUEST_TABLES
  )
    fail(`${path}.tableStarts`, `must contain 1..${MAX_REQUEST_TABLES} tables`);
  const startTableIds = batch.tableStarts.map((start, index) => {
    requireIdentifier(start.tableId, `${path}.tableStarts[${index}].tableId`);
    if (start.shardSnapshotCursor !== undefined)
      validateCursor(
        start.shardSnapshotCursor,
        `${path}.tableStarts[${index}].shardSnapshotCursor`,
      );
    return start.tableId;
  });
  requireUniqueStrings(startTableIds, `${path}.tableStarts.tableId`);
  if (!sameStringSet(batch.tableIds, startTableIds))
    fail(`${path}.tableStarts`, "must cover exactly tableIds");
}

function validateTransactionFragment(
  fragment: TransactionFragment,
  path: string,
): void {
  requireBytes(fragment.transactionId, `${path}.transactionId`);
  validateCursor(fragment.transactionCursor, `${path}.transactionCursor`);
  if (fragment.postgresXid === 0n)
    fail(`${path}.postgresXid`, "must not be zero");
  requireString(fragment.beginLsn, `${path}.beginLsn`);
  requireString(fragment.commitLsn, `${path}.commitLsn`);
  if (fragment.firstFragment !== (fragment.fragmentIndex === 0)) {
    fail(
      `${path}.firstFragment`,
      "must be true exactly when fragmentIndex is zero",
    );
  }
  if (fragment.operations.length > MAX_CHANGE_BATCH_OPERATIONS) {
    fail(
      `${path}.operations`,
      `must contain at most ${MAX_CHANGE_BATCH_OPERATIONS} values`,
    );
  }
  fragment.operations.forEach((operation, index) =>
    validateOperation(operation, `${path}.operations[${index}]`),
  );
  if (fragment.operations.length === 0)
    fail(`${path}.operations`, "must not be empty");
}

function validateBatchEnd(batch: BatchEnd, path: string): void {
  validateCursor(
    batch.throughCursorInclusive,
    `${path}.throughCursorInclusive`,
  );
  if (batch.acknowledgedMutationIds.length > MAX_PENDING_MUTATION_IDS) {
    fail(
      `${path}.acknowledgedMutationIds`,
      `must contain at most ${MAX_PENDING_MUTATION_IDS} values`,
    );
  }
  batch.acknowledgedMutationIds.forEach((id, index) =>
    requireIdentifier(id, `${path}.acknowledgedMutationIds[${index}]`),
  );
  requireUniqueStrings(
    batch.acknowledgedMutationIds,
    `${path}.acknowledgedMutationIds`,
  );
  if (batch.frameCount === 0n)
    fail(`${path}.frameCount`, "must count at least the batch start");
  requireLength(batch.framesSha256, SHA256_BYTES, `${path}.framesSha256`);
}

function validateStreamError(error: StreamError, path: string): void {
  if (
    !Object.values(StreamErrorCode).includes(error.code) ||
    error.code === StreamErrorCode.STREAM_ERROR_CODE_UNSPECIFIED
  ) {
    fail(`${path}.code`, `unknown or unspecified value ${error.code}`);
  }
  requireString(error.message, `${path}.message`);
  if (error.minimumRetainedCursor !== undefined) {
    validateCursor(
      error.minimumRetainedCursor,
      `${path}.minimumRetainedCursor`,
    );
  }
  if (error.durableHead !== undefined)
    validateCursor(error.durableHead, `${path}.durableHead`);
}

export function validateChangeRequest(request: ChangeRequest): void {
  requireVersion(request.protocolVersion, "changeRequest.protocolVersion");
  requireIdentifier(request.planId, "changeRequest.planId");
  validateCursor(
    request.fromCursorExclusive,
    "changeRequest.fromCursorExclusive",
  );
  if (request.throughCursorInclusive !== undefined) {
    validateCursor(
      request.throughCursorInclusive,
      "changeRequest.throughCursorInclusive",
    );
  }
  if (request.tables.length === 0 || request.tables.length > MAX_REQUEST_TABLES)
    fail(
      "changeRequest.tables",
      `must contain 1..${MAX_REQUEST_TABLES} tables`,
    );
  const tableIds = request.tables.map((table, index) => {
    requireIdentifier(table.tableId, `changeRequest.tables[${index}].tableId`);
    if (table.shardSnapshotCursor !== undefined) {
      validateCursor(
        table.shardSnapshotCursor,
        `changeRequest.tables[${index}].shardSnapshotCursor`,
      );
    }
    return table.tableId;
  });
  requireUniqueStrings(tableIds, "changeRequest.tables.tableId");
  requireBytes(
    request.authorizationScopeToken,
    "changeRequest.authorizationScopeToken",
    MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES,
  );
  if (
    request.maxBatchBytes < MIN_CHANGE_BATCH_BYTES ||
    request.maxBatchBytes > MAX_CHANGE_BATCH_BYTES
  ) {
    fail(
      "changeRequest.maxBatchBytes",
      `must be within ${MIN_CHANGE_BATCH_BYTES}..${MAX_CHANGE_BATCH_BYTES}`,
    );
  }
  if (
    request.maxTransactions < 1 ||
    request.maxTransactions > MAX_CHANGE_TRANSACTIONS
  ) {
    fail(
      "changeRequest.maxTransactions",
      `must be within 1..${MAX_CHANGE_TRANSACTIONS}`,
    );
  }
  if (
    request.longPollTimeoutMs !== 0 &&
    (request.longPollTimeoutMs < MIN_LONG_POLL_TIMEOUT_MS ||
      request.longPollTimeoutMs > MAX_LONG_POLL_TIMEOUT_MS)
  ) {
    fail(
      "changeRequest.longPollTimeoutMs",
      `must be zero or within ${MIN_LONG_POLL_TIMEOUT_MS}..${MAX_LONG_POLL_TIMEOUT_MS}`,
    );
  }
  // A response stays open only as long as its wait, so a zero wait is no
  // lifetime.
  if (request.stream && request.longPollTimeoutMs === 0)
    fail("changeRequest.stream", "requires a non-zero longPollTimeoutMs");
  requireClientIdentifier(
    request.clientInstanceId,
    "changeRequest.clientInstanceId",
  );
  requireClientIdentifier(request.deviceId, "changeRequest.deviceId");
  if (request.pendingMutationIds.length > MAX_PENDING_MUTATION_IDS)
    fail(
      "changeRequest.pendingMutationIds",
      `must contain at most ${MAX_PENDING_MUTATION_IDS} values`,
    );
  request.pendingMutationIds.forEach((id, index) =>
    requireIdentifier(id, `changeRequest.pendingMutationIds[${index}]`),
  );
  requireUniqueStrings(
    request.pendingMutationIds,
    "changeRequest.pendingMutationIds",
  );
}

/**
 * Checks a revocation barrier's shape, which authorizes nothing: it bounds the
 * untrusted fields so the signing preimage can be built, and refuses a barrier
 * that retires nothing or whose replacement does not follow the generation it
 * retires.
 */
function validateRevocationBarrier(
  barrier: RevocationBarrier,
  path: string,
): void {
  validateCursor(barrier.barrierCursor, `${path}.barrierCursor`);
  requireBarrierIdentifier(barrier.barrierId, `${path}.barrierId`);
  requireBarrierIdentifier(barrier.signingKeyId, `${path}.signingKeyId`);
  requireLength(
    barrier.revokedAuthorizationScopeHash,
    SHA256_BYTES,
    `${path}.revokedAuthorizationScopeHash`,
  );
  requireLength(
    barrier.signature,
    REVOCATION_BARRIER_SIGNATURE_BYTES,
    `${path}.signature`,
  );
  if (barrier.revokedAuthorizationGeneration === 0n)
    fail(`${path}.revokedAuthorizationGeneration`, "must be greater than zero");
  if (
    barrier.effectiveAuthorizationGeneration <=
    barrier.revokedAuthorizationGeneration
  ) {
    fail(
      `${path}.effectiveAuthorizationGeneration`,
      `is ${barrier.effectiveAuthorizationGeneration}, which does not follow the revoked generation ${barrier.revokedAuthorizationGeneration}`,
    );
  }
  if (barrier.issuedAtUnixMicros === 0n)
    fail(`${path}.issuedAtUnixMicros`, "must be greater than zero");
}

function requireBarrierIdentifier(value: string, path: string): void {
  if (
    value.length === 0 ||
    utf8Length(value) > MAX_BARRIER_IDENTIFIER_BYTES ||
    /\p{Cc}/u.test(value)
  ) {
    fail(
      path,
      `must contain 1..${MAX_BARRIER_IDENTIFIER_BYTES} UTF-8 bytes without control characters`,
    );
  }
}

export function validateChangeStreamFrame(frame: ChangeStreamFrame): void {
  requireVersion(frame.protocolVersion, "changeStreamFrame.protocolVersion");
  if (frame.body.case === undefined)
    fail("changeStreamFrame.body", "is required");
  requireLength(frame.bodySha256, SHA256_BYTES, "changeStreamFrame.bodySha256");
  switch (frame.body.case) {
    case "batchStart":
      validateBatchStart(frame.body.value, "changeStreamFrame.batchStart");
      break;
    case "transactionFragment":
      validateTransactionFragment(
        frame.body.value,
        "changeStreamFrame.transactionFragment",
      );
      break;
    case "batchEnd":
      validateBatchEnd(frame.body.value, "changeStreamFrame.batchEnd");
      break;
    case "heartbeat":
      validateCursor(
        frame.body.value.durableHead,
        "changeStreamFrame.heartbeat.durableHead",
      );
      break;
    case "error":
      validateStreamError(frame.body.value, "changeStreamFrame.error");
      break;
    case "revocationBarrier":
      validateRevocationBarrier(
        frame.body.value,
        "changeStreamFrame.revocationBarrier",
      );
      break;
  }
}

function validateMutationEnvelope(
  mutation: MutationEnvelope,
  path: string,
): void {
  requireIdentifier(mutation.mutationId, `${path}.mutationId`);
  requireCommandName(mutation.commandName, `${path}.commandName`);
  if (mutation.commandVersion === 0)
    fail(`${path}.commandVersion`, "must be greater than zero");
  requireBytes(
    mutation.canonicalInput,
    `${path}.canonicalInput`,
    MAX_CANONICAL_INPUT_BYTES,
  );
  validateCursor(mutation.baseCursor, `${path}.baseCursor`);
  if (mutation.createdAtUnixMicros === 0n)
    fail(`${path}.createdAtUnixMicros`, "must be greater than zero");
  if (mutation.expectedVersions.length > MAX_EXPECTED_VERSIONS) {
    fail(
      `${path}.expectedVersions`,
      `must contain at most ${MAX_EXPECTED_VERSIONS} values`,
    );
  }
  const expectedKeys = new Set<string>();
  for (const [index, expected] of mutation.expectedVersions.entries()) {
    const expectedPath = `${path}.expectedVersions[${index}]`;
    requireIdentifier(expected.tableId, `${expectedPath}.tableId`);
    decodePrimaryKey(expected.primaryKey, `${expectedPath}.primaryKey`);
    requireBytes(expected.expectedVersion, `${expectedPath}.expectedVersion`);
    const key = `${expected.tableId}\0${bytesKey(expected.primaryKey)}`;
    if (expectedKeys.has(key))
      fail(expectedPath, "duplicates an expected table/key version");
    expectedKeys.add(key);
  }
  const footprint = mutation.declaredFootprint;
  if (footprint === undefined) fail(`${path}.declaredFootprint`, "is required");
  if (
    footprint.tableIds.length === 0 ||
    footprint.tableIds.length > MAX_FOOTPRINT_TABLES
  ) {
    fail(
      `${path}.declaredFootprint.tableIds`,
      `must contain 1..${MAX_FOOTPRINT_TABLES} values`,
    );
  }
  footprint.tableIds.forEach((tableId, index) =>
    requireIdentifier(tableId, `${path}.declaredFootprint.tableIds[${index}]`),
  );
  requireUniqueStrings(
    footprint.tableIds,
    `${path}.declaredFootprint.tableIds`,
  );
  if (footprint.rows.length > MAX_FOOTPRINT_ROWS) {
    fail(
      `${path}.declaredFootprint.rows`,
      `must contain at most ${MAX_FOOTPRINT_ROWS} values`,
    );
  }
  const footprintTables = new Set(footprint.tableIds);
  const footprintRows = new Set<string>();
  footprint.rows.forEach((row, index) => {
    const rowPath = `${path}.declaredFootprint.rows[${index}]`;
    requireIdentifier(row.tableId, `${rowPath}.tableId`);
    if (!footprintTables.has(row.tableId))
      fail(`${rowPath}.tableId`, "must be present in footprint tableIds");
    decodePrimaryKey(row.primaryKey, `${rowPath}.primaryKey`);
    const key = `${row.tableId}\0${bytesKey(row.primaryKey)}`;
    if (footprintRows.has(key))
      fail(rowPath, "duplicates an affected table/key row");
    footprintRows.add(key);
  });
}

export function validateMutationBatchRequest(
  request: MutationBatchRequest,
): void {
  requireVersion(
    request.protocolVersion,
    "mutationBatchRequest.protocolVersion",
  );
  requireClientIdentifier(
    request.clientInstanceId,
    "mutationBatchRequest.clientInstanceId",
  );
  requireClientIdentifier(request.deviceId, "mutationBatchRequest.deviceId");
  requireBytes(
    request.authorizationScopeToken,
    "mutationBatchRequest.authorizationScopeToken",
    MAX_MUTATION_SCOPE_TOKEN_BYTES,
  );
  if (
    request.mutations.length === 0 ||
    request.mutations.length > MAX_BATCH_MUTATIONS
  ) {
    fail(
      "mutationBatchRequest.mutations",
      `must contain 1..${MAX_BATCH_MUTATIONS} values`,
    );
  }
  let totalInputBytes = 0;
  const ids = request.mutations.map((mutation, index) => {
    totalInputBytes += mutation.canonicalInput.byteLength;
    if (totalInputBytes > MAX_BATCH_INPUT_BYTES) {
      fail(
        "mutationBatchRequest.mutations.canonicalInput",
        `must total at most ${MAX_BATCH_INPUT_BYTES} bytes`,
      );
    }
    validateMutationEnvelope(
      mutation,
      `mutationBatchRequest.mutations[${index}]`,
    );
    return mutation.mutationId;
  });
  requireUniqueStrings(ids, "mutationBatchRequest.mutations.mutationId");
}

function validateMutationOutcome(outcome: MutationOutcome, path: string): void {
  requireIdentifier(outcome.mutationId, `${path}.mutationId`);
  if (
    !Object.values(MutationOutcomeCode).includes(outcome.code) ||
    outcome.code === MutationOutcomeCode.MUTATION_OUTCOME_CODE_UNSPECIFIED
  ) {
    fail(`${path}.code`, `unknown or unspecified value ${outcome.code}`);
  }
  requireString(outcome.message, `${path}.message`);
  if (outcome.code === MutationOutcomeCode.CONFLICT) {
    if (outcome.conflict === undefined)
      fail(`${path}.conflict`, "is required for a conflict outcome");
    requireClientIdentifier(outcome.conflict.kind, `${path}.conflict.kind`);
    if (utf8Length(outcome.conflict.kind) > MAX_CONFLICT_KIND_BYTES) {
      fail(
        `${path}.conflict.kind`,
        `must contain at most ${MAX_CONFLICT_KIND_BYTES} UTF-8 bytes`,
      );
    }
    if (
      outcome.conflict.applicationPayload.byteLength >
      MAX_CONFLICT_PAYLOAD_BYTES
    ) {
      fail(
        `${path}.conflict.applicationPayload`,
        `must contain at most ${MAX_CONFLICT_PAYLOAD_BYTES} bytes`,
      );
    }
    const conflictRows = new Set<string>();
    outcome.conflict.rows.forEach((row, index) => {
      const rowPath = `${path}.conflict.rows[${index}]`;
      requireIdentifier(row.tableId, `${rowPath}.tableId`);
      decodePrimaryKey(row.primaryKey, `${rowPath}.primaryKey`);
      requireBytes(row.encodedFullRow, `${rowPath}.encodedFullRow`);
      requireBytes(row.version, `${rowPath}.version`);
      const key = `${row.tableId}\0${bytesKey(row.primaryKey)}`;
      if (conflictRows.has(key)) fail(rowPath, "duplicates a canonical row");
      conflictRows.add(key);
    });
  } else if (outcome.conflict !== undefined) {
    fail(`${path}.conflict`, "is only legal for a conflict outcome");
  }
  const accepted = [
    MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
    MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
    MutationOutcomeCode.ALREADY_CANONICAL,
  ].includes(outcome.code);
  if (accepted && outcome.serverTransactionId.byteLength !== 16)
    fail(`${path}.serverTransactionId`, "must contain exactly 16 bytes");
  if (!accepted && outcome.serverTransactionId.byteLength !== 0)
    fail(
      `${path}.serverTransactionId`,
      "is only legal for an accepted outcome",
    );
  if (outcome.code === MutationOutcomeCode.ALREADY_CANONICAL) {
    if (outcome.observedCanonicalCursor === undefined)
      fail(
        `${path}.observedCanonicalCursor`,
        "is required when already canonical",
      );
    validateCursor(
      outcome.observedCanonicalCursor,
      `${path}.observedCanonicalCursor`,
    );
  } else if (outcome.observedCanonicalCursor !== undefined)
    fail(
      `${path}.observedCanonicalCursor`,
      "is only legal when already canonical",
    );
  if (outcome.code === MutationOutcomeCode.RETRYABLE_FAILURE) {
    if (outcome.retryAfterMs === 0n)
      fail(`${path}.retryAfterMs`, "must be greater than zero when retryable");
  } else if (outcome.retryAfterMs !== 0n) {
    fail(`${path}.retryAfterMs`, "is only legal for a retryable failure");
  }
}

export function validateMutationBatchResponse(
  response: MutationBatchResponse,
): void {
  requireVersion(
    response.protocolVersion,
    "mutationBatchResponse.protocolVersion",
  );
  if (
    response.outcomes.length === 0 ||
    response.outcomes.length > MAX_BATCH_MUTATIONS
  ) {
    fail(
      "mutationBatchResponse.outcomes",
      `must contain 1..${MAX_BATCH_MUTATIONS} values`,
    );
  }
  if (response.serverTimeUnixMicros === 0n)
    fail(
      "mutationBatchResponse.serverTimeUnixMicros",
      "must be greater than zero",
    );
  const ids = response.outcomes.map((outcome, index) => {
    validateMutationOutcome(
      outcome,
      `mutationBatchResponse.outcomes[${index}]`,
    );
    return outcome.mutationId;
  });
  requireUniqueStrings(ids, "mutationBatchResponse.outcomes.mutationId");
}
