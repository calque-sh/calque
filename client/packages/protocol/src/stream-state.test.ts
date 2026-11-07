import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  BatchEndSchema,
  BatchStartSchema,
  ChangeStreamFrameSchema,
  CursorSchema,
  HeartbeatSchema,
  OperationKind,
  OperationSchema,
  SourceGenerationSchema,
  StreamErrorCode,
  StreamErrorSchema,
  TransactionFragmentSchema,
  TruncateSchema,
  type BatchEnd,
  type ChangeStreamFrame,
} from "./gen/change_stream_pb.js";
import { BatchFramesDigest, changeFrameBodySha256 } from "./integrity.js";
import { ChangeStreamState } from "./stream-state.js";

const bytes = (value: string) => new TextEncoder().encode(value);
const cursor = (value: string) => create(CursorSchema, { token: bytes(value) });
const generation = () =>
  create(SourceGenerationSchema, {
    sourceEpoch: "epoch-1",
    publicationGeneration: 2n,
    schemaGeneration: 3n,
    authorizationGeneration: 4n,
    authorizationScopeHash: new Uint8Array(32).fill(5),
  });

async function signed(frame: ChangeStreamFrame): Promise<ChangeStreamFrame> {
  frame.bodySha256 = await changeFrameBodySha256(frame);
  return frame;
}

function frame(body: ChangeStreamFrame["body"]) {
  return create(ChangeStreamFrameSchema, { protocolVersion: 1, body });
}

/** The `batchEnd` closing a batch of exactly `previous`, in order. */
async function ended(
  previous: readonly ChangeStreamFrame[],
  fields: Partial<
    Pick<
      BatchEnd,
      | "reachedRequestedFence"
      | "relevantTransactionCount"
      | "relevantOperationCount"
      | "acknowledgedMutationIds"
    >
  > = {},
): Promise<ChangeStreamFrame> {
  const digest = new BatchFramesDigest();
  for (const sent of previous) digest.push(sent.bodySha256);
  return signed(
    frame({
      case: "batchEnd",
      value: create(BatchEndSchema, {
        throughCursorInclusive: cursor("cursor-h"),
        reachedRequestedFence: true,
        ...fields,
        frameCount: BigInt(digest.frames),
        framesSha256: await digest.finish(),
      }),
    }),
  );
}

/** Accepts every frame of `frames` and returns them signed, in order. */
async function acceptAll(
  stream: ChangeStreamState,
  frames: readonly ChangeStreamFrame[],
): Promise<ChangeStreamFrame[]> {
  const sent = [];
  for (const unsigned of frames) {
    const next = await signed(unsigned);
    await stream.accept(next);
    sent.push(next);
  }
  return sent;
}

function start() {
  return frame({
    case: "batchStart",
    value: create(BatchStartSchema, {
      generation: generation(),
      fromCursorExclusive: cursor("cursor-0"),
      requestedFence: cursor("cursor-h"),
      tableIds: ["public.tasks"],
      planId: "plan-1",
      tableStarts: [
        { tableId: "public.tasks", shardSnapshotCursor: cursor("cursor-p") },
      ],
      streamId: new Uint8Array(16).fill(1),
    }),
  });
}

function fragment(
  fragmentIndex: number,
  finalFragment: boolean,
  transactionId = "tx-1",
  transactionCursor = "cursor-1",
) {
  return frame({
    case: "transactionFragment",
    value: create(TransactionFragmentSchema, {
      transactionId: bytes(transactionId),
      transactionCursor: cursor(transactionCursor),
      fragmentIndex,
      firstFragment: fragmentIndex === 0,
      finalFragment,
      postgresXid: 7n,
      beginLsn: "0/10",
      commitLsn: "0/20",
      commitTimeUnixMicros: 9n,
      operations: [
        create(OperationSchema, {
          tableId: "public.tasks",
          kind: OperationKind.TRUNCATE_AUTHORIZED_TABLE,
          payload: {
            case: "truncate",
            value: create(TruncateSchema, { reason: "authorized reset" }),
          },
        }),
      ],
    }),
  });
}

function state() {
  return new ChangeStreamState({
    planId: "plan-1",
    fromCursorExclusive: bytes("cursor-0"),
    requestedFence: bytes("cursor-h"),
    tableStarts: [
      { tableId: "public.tasks", snapshotCursor: bytes("cursor-p") },
    ],
    generation: generation(),
  });
}

describe("change stream state", () => {
  it("accepts one contiguous fragmented transaction through a fixed fence", async () => {
    const stream = state();
    const sent = await acceptAll(stream, [
      start(),
      fragment(0, false),
      fragment(1, true),
    ]);
    const completed = await stream.accept(
      await ended(sent, {
        relevantTransactionCount: 1n,
        relevantOperationCount: 2n,
      }),
    );

    expect(completed).toMatchObject({
      relevantTransactionCount: 1n,
      relevantOperationCount: 2n,
      reachedRequestedFence: true,
      acknowledgedMutationIds: [],
    });
    expect(stream.terminal).toBe(true);
  });

  it("reports the acknowledgements its batch end carries", async () => {
    const stream = state();
    const sent = await acceptAll(stream, [start(), fragment(0, true)]);
    const completed = await stream.accept(
      await ended(sent, {
        relevantTransactionCount: 1n,
        relevantOperationCount: 1n,
        acknowledgedMutationIds: ["mutation-1", "mutation-2"],
      }),
    );
    expect(completed?.acknowledgedMutationIds).toEqual([
      "mutation-1",
      "mutation-2",
    ]);
  });

  it("refuses a batch whose frames were lost, reordered or miscounted", async () => {
    const counts = {
      relevantTransactionCount: 1n,
      relevantOperationCount: 2n,
    };
    const lost = state();
    const [first, second, third] = await acceptAll(lost, [
      start(),
      fragment(0, false),
      fragment(1, true),
    ]);
    if (!first || !second || !third) throw new Error("expected three frames");
    await expect(
      lost.accept(await ended([first, third], counts)),
    ).rejects.toThrow(/frameCount/u);
    await expect(lost.accept(await signed(fragment(0, true)))).rejects.toThrow(
      /already terminal/u,
    );

    const reordered = state();
    await acceptAll(reordered, [
      start(),
      fragment(0, false),
      fragment(1, true),
    ]);
    await expect(
      reordered.accept(await ended([first, third, second], counts)),
    ).rejects.toThrow(/does not cover the frames/u);

    const miscounted = state();
    await acceptAll(miscounted, [
      start(),
      fragment(0, false),
      fragment(1, true),
    ]);
    const end = await ended([first, second, third], counts);
    if (end.body.case !== "batchEnd") throw new Error("expected batch end");
    end.body.value.frameCount = 4n;
    await expect(miscounted.accept(await signed(end))).rejects.toThrow(
      /is 4, observed 3/u,
    );
  });

  it("poisons on a skipped fragment and rejects any continuation", async () => {
    const stream = state();
    await stream.accept(await signed(start()));
    await expect(
      stream.accept(await signed(fragment(1, true))),
    ).rejects.toThrow(/must begin at fragment zero/u);
    await expect(
      stream.accept(await signed(fragment(0, true))),
    ).rejects.toThrow(/already terminal/u);
  });

  it("rejects a fence cursor paired with reachedRequestedFence false", async () => {
    const stream = state();
    const sent = await acceptAll(stream, [start()]);
    await expect(
      stream.accept(await ended(sent, { reachedRequestedFence: false })),
    ).rejects.toThrow(/must exactly match/u);
  });

  it("rejects interleaving and transaction identity changes", async () => {
    const interleaved = state();
    await acceptAll(interleaved, [start(), fragment(0, false)]);
    await expect(
      interleaved.accept(
        await signed(
          frame({
            case: "heartbeat",
            value: create(HeartbeatSchema, { durableHead: cursor("cursor-1") }),
          }),
        ),
      ),
    ).rejects.toThrow(/cannot interleave/u);

    const changed = state();
    await acceptAll(changed, [start(), fragment(0, false)]);
    await expect(
      changed.accept(await signed(fragment(1, true, "tx-2"))),
    ).rejects.toThrow(/changed transaction identity/u);
  });

  it("rejects an empty fragment and a repeated acknowledgement", async () => {
    const empty = state();
    await empty.accept(await signed(start()));
    const emptyFragment = fragment(0, true);
    if (emptyFragment.body.case !== "transactionFragment")
      throw new Error("expected fragment");
    emptyFragment.body.value.operations = [];
    await expect(empty.accept(await signed(emptyFragment))).rejects.toThrow(
      /must not be empty/u,
    );

    const repeated = state();
    const sent = await acceptAll(repeated, [start()]);
    await expect(
      repeated.accept(
        await ended(sent, {
          acknowledgedMutationIds: ["mutation-1", "mutation-1"],
        }),
      ),
    ).rejects.toThrow(/must be unique/u);
  });

  it("rejects duplicate transaction and cursor identities", async () => {
    const duplicateCursor = state();
    await acceptAll(duplicateCursor, [start(), fragment(0, true)]);
    await expect(
      duplicateCursor.accept(
        await signed(fragment(0, true, "tx-2", "cursor-1")),
      ),
    ).rejects.toThrow(/transactionCursor.*already completed/u);

    const duplicateTransaction = state();
    await acceptAll(duplicateTransaction, [start(), fragment(0, true)]);
    await expect(
      duplicateTransaction.accept(
        await signed(fragment(0, true, "tx-1", "cursor-2")),
      ),
    ).rejects.toThrow(/transactionId.*already completed/u);
  });

  it("accepts a standalone terminal error and rejects continuation", async () => {
    const stream = state();
    await stream.accept(
      await signed(
        frame({
          case: "error",
          value: create(StreamErrorSchema, {
            code: StreamErrorCode.INTERNAL_RETRYABLE,
            message: "retry later",
            retryAfterMs: 1_000n,
          }),
        }),
      ),
    );
    expect(stream.terminal).toBe(true);
    await expect(stream.accept(await signed(start()))).rejects.toThrow(
      /already terminal/u,
    );
  });

  it("verifies the body digest before mutating stream state", async () => {
    const stream = state();
    const tampered = await signed(start());
    if (tampered.body.case !== "batchStart") throw new Error("expected start");
    tampered.body.value.tableIds.push("public.users");
    await expect(stream.accept(tampered)).rejects.toThrow(
      /must cover exactly tableIds/u,
    );
    expect(stream.terminal).toBe(true);
  });

  it("rejects a batch start for a different plan or per-table cursor", async () => {
    const wrongPlan = start();
    if (wrongPlan.body.case !== "batchStart") throw new Error("expected start");
    wrongPlan.body.value.planId = "plan-2";
    await expect(state().accept(await signed(wrongPlan))).rejects.toThrow(
      /authenticated bootstrap plan/u,
    );

    const wrongStart = start();
    if (wrongStart.body.case !== "batchStart")
      throw new Error("expected start");
    const firstStart = wrongStart.body.value.tableStarts[0];
    if (!firstStart) throw new Error("expected table start");
    firstStart.shardSnapshotCursor = cursor("cursor-other");
    await expect(state().accept(await signed(wrongStart))).rejects.toThrow(
      /authenticated per-table starts/u,
    );
  });

  it("rejects operations outside the exact subscribed table set", async () => {
    const stream = state();
    await stream.accept(await signed(start()));
    const unexpected = fragment(0, true);
    if (unexpected.body.case !== "transactionFragment")
      throw new Error("expected fragment");
    unexpected.body.value.operations.push(
      create(OperationSchema, {
        tableId: "public.users",
        kind: OperationKind.TRUNCATE_AUTHORIZED_TABLE,
        payload: {
          case: "truncate",
          value: create(TruncateSchema, { reason: "authorized reset" }),
        },
      }),
    );
    await expect(stream.accept(await signed(unexpected))).rejects.toThrow(
      /is not subscribed/u,
    );
  });
});
