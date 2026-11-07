import { create } from "@bufbuild/protobuf";
import {
  BatchFramesDigest,
  change,
  changeFrameBodySha256,
  encodeFrame,
} from "@calque/protocol";

import type { ColumnarChangeContract } from "./columnar-change.js";

const text = new TextEncoder();

/**
 * One framed change response for `contract`: a single transaction from
 * `from` through `through` carrying `operations`, in a batch acknowledging
 * `mutationIds`. With no operations the batch carries no transaction.
 */
export async function changeBatch(
  contract: ColumnarChangeContract,
  from: string,
  through: string,
  operations: readonly change.Operation[],
  mutationIds: readonly string[],
): Promise<Uint8Array> {
  const cursor = (token: string) =>
    create(change.CursorSchema, { token: text.encode(token) });
  const frames = [
    create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: {
        case: "batchStart",
        value: create(change.BatchStartSchema, {
          generation: create(
            change.SourceGenerationSchema,
            contract.generation,
          ),
          fromCursorExclusive: cursor(from),
          tableIds: contract.tables.map((table) => table.tableId).sort(),
          serverTimeUnixMicros: 1n,
          planId: contract.planId,
          tableStarts: contract.tables
            .map((table) => ({ tableId: table.tableId }))
            .sort((left, right) => left.tableId.localeCompare(right.tableId)),
          streamId: text.encode(`stream-${contract.planId}`),
        }),
      },
    }),
  ];
  if (operations.length > 0)
    frames.push(
      create(change.ChangeStreamFrameSchema, {
        protocolVersion: 1,
        body: {
          case: "transactionFragment",
          value: create(change.TransactionFragmentSchema, {
            transactionId: text.encode(through),
            transactionCursor: cursor(through),
            fragmentIndex: 0,
            firstFragment: true,
            finalFragment: true,
            postgresXid: 100n,
            beginLsn: "0/10",
            commitLsn: "0/20",
            commitTimeUnixMicros: 2n,
            operations: [...operations],
          }),
        },
      }),
    );
  const digest = new BatchFramesDigest();
  for (const frame of frames) {
    frame.bodySha256 = await changeFrameBodySha256(frame);
    digest.push(frame.bodySha256);
  }
  const end = create(change.ChangeStreamFrameSchema, {
    protocolVersion: 1,
    body: {
      case: "batchEnd",
      value: create(change.BatchEndSchema, {
        throughCursorInclusive: cursor(through),
        currentlyUpToDate: true,
        relevantTransactionCount: operations.length > 0 ? 1n : 0n,
        relevantOperationCount: BigInt(operations.length),
        serverTimeUnixMicros: 3n,
        acknowledgedMutationIds: [...mutationIds],
        frameCount: BigInt(digest.frames),
        framesSha256: await digest.finish(),
      }),
    },
  });
  end.bodySha256 = await changeFrameBodySha256(end);
  frames.push(end);
  const encoded = frames.map((frame) =>
    encodeFrame(change.ChangeStreamFrameSchema, frame),
  );
  const bytes = new Uint8Array(
    encoded.reduce((sum, part) => sum + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of encoded) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return bytes;
}
