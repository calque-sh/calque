import type {
  ChangeStreamFrame,
  RevocationBarrier,
  SourceGeneration,
  TransactionFragment,
} from "./gen/change_stream_pb.js";
import { bytesKey, equalBytes, sameStringSet } from "./compare.js";
import { BatchFramesDigest, verifyChangeFrameBodySha256 } from "./integrity.js";
import {
  verifyRevocationBarrier,
  type RevocationBarrierVerificationKey,
} from "./revocation.js";
import {
  ProtocolValidationError,
  validateChangeStreamFrame,
} from "./validation.js";

export interface ChangeStreamExpectation {
  readonly planId: string;
  readonly fromCursorExclusive: Uint8Array;
  readonly requestedFence?: Uint8Array;
  readonly tableStarts: readonly ExpectedTableStart[];
  readonly generation: ExpectedSourceGeneration;
  /**
   * The authorization authority's Ed25519 key this receiver accepts
   * revocation barriers from. Without it every barrier is refused, since an
   * unauthenticated barrier cannot be told from one a hostile stream invented.
   */
  readonly revocationBarrierKey?: RevocationBarrierVerificationKey;
}

export interface ExpectedTableStart {
  readonly tableId: string;
  readonly snapshotCursor?: Uint8Array;
}

export type ExpectedSourceGeneration = Pick<
  SourceGeneration,
  | "sourceEpoch"
  | "publicationGeneration"
  | "schemaGeneration"
  | "authorizationGeneration"
  | "authorizationScopeHash"
>;

export interface CompletedBatch {
  readonly throughCursorInclusive: Uint8Array;
  readonly relevantTransactionCount: bigint;
  readonly relevantOperationCount: bigint;
  readonly reachedRequestedFence: boolean;
  readonly currentlyUpToDate: boolean;
  /** The request's pending mutations this batch acknowledged. */
  readonly acknowledgedMutationIds: readonly string[];
  /** The authenticated barrier this batch ended on, when it ended on one. */
  readonly revocationBarrier?: RevocationBarrier;
}

interface ActiveTransaction {
  readonly transactionId: Uint8Array;
  readonly transactionCursor: Uint8Array;
  readonly postgresXid: bigint;
  readonly beginLsn: string;
  readonly commitLsn: string;
  readonly commitTimeUnixMicros: bigint;
  nextFragmentIndex: number;
}

const MAX_BATCH_OPERATIONS = 100_000n;
const MAX_BATCH_TRANSACTIONS = 10_000n;

/** Stateful fail-closed validator above protobuf framing and body integrity. */
export class ChangeStreamState {
  readonly #expectation: ChangeStreamExpectation;
  readonly #tableIds: ReadonlySet<string>;
  #phase: "awaiting-start" | "batch" | "complete" | "error" = "awaiting-start";
  /**
   * The body digests of the batch's frames so far, which `batchEnd`'s count
   * and digest must match.
   */
  readonly #frames = new BatchFramesDigest();
  #activeTransaction: ActiveTransaction | undefined;
  readonly #transactionIds = new Set<string>();
  readonly #transactionCursors = new Set<string>();
  #transactionCount = 0n;
  #operationCount = 0n;
  #revocationBarrier: RevocationBarrier | undefined;

  constructor(expectation: ChangeStreamExpectation) {
    const tableIds = expectation.tableStarts.map((start) => start.tableId);
    this.#tableIds = new Set(tableIds);
    if (expectation.planId.length === 0) {
      fail("changeStreamExpectation.planId", "must not be empty");
    }
    if (tableIds.length === 0 || tableIds.length > 1_024) {
      fail(
        "changeStreamExpectation.tableStarts",
        "must contain 1..1024 tables",
      );
    }
    if (new Set(tableIds).size !== tableIds.length) {
      fail("changeStreamExpectation.tableStarts.tableId", "must be unique");
    }
    if (
      expectation.tableStarts.some(
        (start) =>
          start.tableId.length === 0 || start.snapshotCursor?.byteLength === 0,
      )
    ) {
      fail("changeStreamExpectation.tableStarts", "contains an invalid start");
    }
    this.#expectation = {
      planId: expectation.planId,
      fromCursorExclusive: expectation.fromCursorExclusive.slice(),
      ...(expectation.requestedFence
        ? { requestedFence: expectation.requestedFence.slice() }
        : {}),
      tableStarts: expectation.tableStarts.map((start) =>
        start.snapshotCursor
          ? {
              tableId: start.tableId,
              snapshotCursor: start.snapshotCursor.slice(),
            }
          : { tableId: start.tableId },
      ),
      generation: {
        ...expectation.generation,
        authorizationScopeHash:
          expectation.generation.authorizationScopeHash.slice(),
      },
      ...(expectation.revocationBarrierKey
        ? { revocationBarrierKey: expectation.revocationBarrierKey }
        : {}),
    };
  }

  get terminal(): boolean {
    return this.#phase === "complete" || this.#phase === "error";
  }

  /**
   * Accepts the next frame. `receivedBody` is its body as it arrived
   * (`decodeReceivedChangeFrames`), which the digest is then checked over.
   */
  async accept(
    frame: ChangeStreamFrame,
    receivedBody?: Uint8Array,
  ): Promise<CompletedBatch | undefined> {
    if (this.terminal) fail("changeStream", "is already terminal");
    try {
      validateChangeStreamFrame(frame);
      await verifyChangeFrameBodySha256(frame, receivedBody);

      switch (frame.body.case) {
        case "batchStart":
          this.#acceptBatchStart(frame.body.value);
          this.#frames.push(frame.bodySha256);
          return undefined;
        case "transactionFragment":
          this.#acceptTransactionFragment(frame.body.value);
          this.#frames.push(frame.bodySha256);
          return undefined;
        case "revocationBarrier":
          await this.#acceptRevocationBarrier(frame.body.value);
          this.#frames.push(frame.bodySha256);
          return undefined;
        case "heartbeat":
          this.#requireBatchWithoutFragment("heartbeat");
          this.#frames.push(frame.bodySha256);
          return undefined;
        case "batchEnd": {
          this.#requireBatchWithoutFragment("batchEnd");
          const batch = frame.body.value;
          if (batch.frameCount !== BigInt(this.#frames.frames)) {
            fail(
              "changeStreamFrame.batchEnd.frameCount",
              `is ${batch.frameCount}, observed ${this.#frames.frames}`,
            );
          }
          if (!equalBytes(batch.framesSha256, await this.#frames.finish())) {
            fail(
              "changeStreamFrame.batchEnd.framesSha256",
              "does not cover the frames this batch carried, in their order",
            );
          }
          if (batch.relevantTransactionCount !== this.#transactionCount) {
            fail(
              "changeStreamFrame.batchEnd.relevantTransactionCount",
              `is ${batch.relevantTransactionCount}, observed ${this.#transactionCount}`,
            );
          }
          if (batch.relevantOperationCount !== this.#operationCount) {
            fail(
              "changeStreamFrame.batchEnd.relevantOperationCount",
              `is ${batch.relevantOperationCount}, observed ${this.#operationCount}`,
            );
          }
          const throughCursor = batch.throughCursorInclusive;
          if (throughCursor === undefined) {
            fail(
              "changeStreamFrame.batchEnd.throughCursorInclusive",
              "is required",
            );
          }
          const through = throughCursor.token;
          const barrierCursor = this.#revocationBarrier?.barrierCursor?.token;
          if (
            barrierCursor !== undefined &&
            !equalBytes(barrierCursor, through)
          ) {
            fail(
              "changeStreamFrame.batchEnd.throughCursorInclusive",
              "must equal the revocation barrier's cursor",
            );
          }
          const requested = this.#expectation.requestedFence;
          const reachedExpectedFence =
            requested !== undefined && equalBytes(through, requested);
          if (batch.reachedRequestedFence !== reachedExpectedFence) {
            fail(
              "changeStreamFrame.batchEnd.reachedRequestedFence",
              "must exactly match throughCursorInclusive equality with the requested fence",
            );
          }
          this.#phase = "complete";
          return {
            throughCursorInclusive: through.slice(),
            relevantTransactionCount: this.#transactionCount,
            relevantOperationCount: this.#operationCount,
            reachedRequestedFence: batch.reachedRequestedFence,
            currentlyUpToDate: batch.currentlyUpToDate,
            acknowledgedMutationIds: [...batch.acknowledgedMutationIds],
            ...(this.#revocationBarrier
              ? { revocationBarrier: this.#revocationBarrier }
              : {}),
          };
        }
        case "error":
          // A batch that carries a barrier ends on it or is refused whole. A
          // stream error after the barrier would turn the revocation into a
          // transport failure the caller may retry.
          if (this.#revocationBarrier !== undefined) {
            fail(
              "changeStreamFrame.error",
              "must not follow a revocation barrier in the same batch",
            );
          }
          this.#phase = "error";
          return undefined;
        case undefined:
          fail("changeStreamFrame.body", "is required");
      }
    } catch (error) {
      this.#phase = "error";
      throw error;
    }
  }

  #acceptBatchStart(
    batch: Extract<ChangeStreamFrame["body"], { case: "batchStart" }>["value"],
  ): void {
    if (this.#phase !== "awaiting-start") {
      fail("changeStreamFrame.batchStart", "must be the first frame");
    }
    const fromCursor = batch.fromCursorExclusive;
    if (fromCursor === undefined) {
      fail("changeStreamFrame.batchStart.fromCursorExclusive", "is required");
    }
    if (!equalBytes(fromCursor.token, this.#expectation.fromCursorExclusive)) {
      fail(
        "changeStreamFrame.batchStart.fromCursorExclusive",
        "does not equal the requested cursor",
      );
    }
    const requested = batch.requestedFence?.token;
    if (!equalOptionalBytes(requested, this.#expectation.requestedFence)) {
      fail(
        "changeStreamFrame.batchStart.requestedFence",
        "does not equal the requested fence",
      );
    }
    const expectedTableIds = this.#expectation.tableStarts.map(
      (start) => start.tableId,
    );
    if (!sameStringSet(batch.tableIds, expectedTableIds)) {
      fail(
        "changeStreamFrame.batchStart.tableIds",
        "does not equal the requested table set",
      );
    }
    if (batch.planId !== this.#expectation.planId) {
      fail(
        "changeStreamFrame.batchStart.planId",
        "does not equal the authenticated bootstrap plan",
      );
    }
    if (!sameTableStarts(batch.tableStarts, this.#expectation.tableStarts)) {
      fail(
        "changeStreamFrame.batchStart.tableStarts",
        "does not equal the authenticated per-table starts",
      );
    }
    if (batch.generation === undefined) {
      fail("changeStreamFrame.batchStart.generation", "is required");
    }
    validateExpectedGeneration(batch.generation, this.#expectation.generation);
    this.#phase = "batch";
  }

  #acceptTransactionFragment(fragment: TransactionFragment): void {
    if (this.#revocationBarrier !== undefined) {
      fail(
        "changeStreamFrame.transactionFragment",
        "must not follow a revocation barrier in the same batch",
      );
    }
    if (this.#phase !== "batch") {
      fail(
        "changeStreamFrame.transactionFragment",
        "requires a preceding batch start",
      );
    }
    const unexpectedOperation = fragment.operations.find(
      (operation) => !this.#tableIds.has(operation.tableId),
    );
    if (unexpectedOperation) {
      fail(
        "changeStreamFrame.transactionFragment.operations.tableId",
        `is not subscribed: ${unexpectedOperation.tableId}`,
      );
    }
    const active = this.#activeTransaction;
    const transactionCursor = fragment.transactionCursor;
    if (transactionCursor === undefined) {
      fail(
        "changeStreamFrame.transactionFragment.transactionCursor",
        "is required",
      );
    }
    if (active === undefined) {
      if (!fragment.firstFragment || fragment.fragmentIndex !== 0) {
        fail(
          "changeStreamFrame.transactionFragment",
          "a transaction must begin at fragment zero",
        );
      }
      const transactionIdKey = bytesKey(fragment.transactionId);
      if (this.#transactionIds.has(transactionIdKey)) {
        fail(
          "changeStreamFrame.transactionFragment.transactionId",
          "was already completed in this batch",
        );
      }
      const transactionCursorKey = bytesKey(transactionCursor.token);
      if (this.#transactionCursors.has(transactionCursorKey)) {
        fail(
          "changeStreamFrame.transactionFragment.transactionCursor",
          "was already completed in this batch",
        );
      }
      this.#transactionIds.add(transactionIdKey);
      this.#transactionCursors.add(transactionCursorKey);
      this.#activeTransaction = {
        transactionId: fragment.transactionId.slice(),
        transactionCursor: transactionCursor.token.slice(),
        postgresXid: fragment.postgresXid,
        beginLsn: fragment.beginLsn,
        commitLsn: fragment.commitLsn,
        commitTimeUnixMicros: fragment.commitTimeUnixMicros,
        nextFragmentIndex: 1,
      };
    } else {
      if (
        fragment.firstFragment ||
        fragment.fragmentIndex !== active.nextFragmentIndex
      ) {
        fail(
          "changeStreamFrame.transactionFragment.fragmentIndex",
          `expected consecutive fragment ${active.nextFragmentIndex}`,
        );
      }
      if (
        !equalBytes(fragment.transactionId, active.transactionId) ||
        !equalBytes(transactionCursor.token, active.transactionCursor) ||
        fragment.postgresXid !== active.postgresXid ||
        fragment.beginLsn !== active.beginLsn ||
        fragment.commitLsn !== active.commitLsn ||
        fragment.commitTimeUnixMicros !== active.commitTimeUnixMicros
      ) {
        fail(
          "changeStreamFrame.transactionFragment",
          "changed transaction identity or PostgreSQL provenance mid-fragment",
        );
      }
      active.nextFragmentIndex += 1;
    }
    this.#operationCount += BigInt(fragment.operations.length);
    if (this.#operationCount > MAX_BATCH_OPERATIONS) {
      fail(
        "changeStreamFrame.transactionFragment.operations",
        `batch exceeds ${MAX_BATCH_OPERATIONS} operations`,
      );
    }
    if (fragment.finalFragment) {
      this.#transactionCount += 1n;
      if (this.#transactionCount > MAX_BATCH_TRANSACTIONS) {
        fail(
          "changeStreamFrame.transactionFragment",
          `batch exceeds ${MAX_BATCH_TRANSACTIONS} transactions`,
        );
      }
      this.#activeTransaction = undefined;
    }
  }

  /**
   * Accepts a revocation barrier. It may not interleave with an open
   * transaction, it must be the last body before `batchEnd` (the `error` case
   * in `accept` refuses a stream error after it), and
   * `batchEnd.throughCursorInclusive` must equal its cursor, so nothing the
   * batch delivers lies after it. Before it is remembered, its signature must
   * verify against the configured key, and it must retire the generation and
   * scope this stream is bound to and name this stream's plan.
   */
  async #acceptRevocationBarrier(barrier: RevocationBarrier): Promise<void> {
    // This also refuses a second barrier: once one has been seen, every body
    // but `batchEnd` is refused.
    this.#requireBatchWithoutFragment("revocationBarrier");
    const key = this.#expectation.revocationBarrierKey;
    if (key === undefined) {
      fail(
        "changeStreamFrame.revocationBarrier",
        "cannot be authenticated: this receiver has no revocation barrier key",
      );
    }
    await verifyRevocationBarrier(barrier, key);
    if (
      barrier.revokedAuthorizationGeneration !==
      this.#expectation.generation.authorizationGeneration
    ) {
      fail(
        "changeStreamFrame.revocationBarrier.revokedAuthorizationGeneration",
        `is ${barrier.revokedAuthorizationGeneration}, but this stream runs authorization generation ${this.#expectation.generation.authorizationGeneration}`,
      );
    }
    if (
      !equalBytes(
        barrier.revokedAuthorizationScopeHash,
        this.#expectation.generation.authorizationScopeHash,
      )
    ) {
      fail(
        "changeStreamFrame.revocationBarrier.revokedAuthorizationScopeHash",
        "does not equal this stream's authorization scope",
      );
    }
    this.#revocationBarrier = barrier;
  }

  #requireBatchWithoutFragment(body: string): void {
    if (this.#revocationBarrier !== undefined && body !== "batchEnd") {
      fail(
        `changeStreamFrame.${body}`,
        "must not follow a revocation barrier in the same batch",
      );
    }
    if (this.#phase !== "batch") {
      fail(`changeStreamFrame.${body}`, "requires a preceding batch start");
    }
    if (this.#activeTransaction !== undefined) {
      fail(
        `changeStreamFrame.${body}`,
        "cannot interleave with transaction fragments",
      );
    }
  }
}

function validateExpectedGeneration(
  actual: SourceGeneration,
  expected: ExpectedSourceGeneration,
): void {
  if (
    actual.sourceEpoch !== expected.sourceEpoch ||
    actual.publicationGeneration !== expected.publicationGeneration ||
    actual.schemaGeneration !== expected.schemaGeneration ||
    actual.authorizationGeneration !== expected.authorizationGeneration ||
    !equalBytes(actual.authorizationScopeHash, expected.authorizationScopeHash)
  ) {
    fail(
      "changeStreamFrame.batchStart.generation",
      "does not equal the expected source/authorization generation",
    );
  }
}

function sameTableStarts(
  actual: readonly {
    readonly tableId: string;
    readonly shardSnapshotCursor?: { readonly token: Uint8Array } | undefined;
  }[],
  expected: readonly ExpectedTableStart[],
): boolean {
  if (actual.length !== expected.length) return false;
  const expectedByTable = new Map(
    expected.map((start) => [start.tableId, start.snapshotCursor] as const),
  );
  return actual.every((start) => {
    if (!expectedByTable.has(start.tableId)) return false;
    return equalOptionalBytes(
      start.shardSnapshotCursor?.token,
      expectedByTable.get(start.tableId),
    );
  });
}

function equalOptionalBytes(
  left: Uint8Array | undefined,
  right: Uint8Array | undefined,
): boolean {
  return left === undefined && right === undefined
    ? true
    : left !== undefined && right !== undefined && equalBytes(left, right);
}

function fail(path: string, message: string): never {
  throw new ProtocolValidationError(path, message);
}
