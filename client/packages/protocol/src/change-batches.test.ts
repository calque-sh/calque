import { create, toBinary } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  ChangeBatchSplitter,
  decodeReceivedChangeFrames,
} from "./change-batches.js";
import { encodeFrame, FrameError } from "./framing.js";
import {
  BatchEndSchema,
  BatchStartSchema,
  ChangeStreamFrameSchema,
  RevocationBarrierSchema,
  StreamErrorSchema,
  TransactionFragmentSchema,
  type ChangeStreamFrame,
} from "./gen/change_stream_pb.js";

type Body = ChangeStreamFrame["body"];

function frame(body: Body): Uint8Array {
  return encodeFrame(
    ChangeStreamFrameSchema,
    create(ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body,
      bodySha256: new Uint8Array(32).fill(7),
    }),
  );
}

const start: Body = { case: "batchStart", value: create(BatchStartSchema) };
const fragmentValue = create(TransactionFragmentSchema, {
  transactionId: new Uint8Array([1, 2, 3]),
  finalFragment: true,
});
const fragment: Body = { case: "transactionFragment", value: fragmentValue };
const endValue = create(BatchEndSchema, { currentlyUpToDate: true });
const end: Body = { case: "batchEnd", value: endValue };

function concat(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(
    parts.reduce((total, part) => total + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.byteLength;
  }
  return result;
}

/** Feeds `body` in chunks of `size` bytes and collects every batch. */
function split(
  splitter: ChangeBatchSplitter,
  body: Uint8Array,
  size: number,
): Uint8Array[] {
  const batches: Uint8Array[] = [];
  for (let offset = 0; offset < body.byteLength; offset += size)
    batches.push(...splitter.push(body.subarray(offset, offset + size)));
  return batches;
}

function caught(action: () => unknown): FrameError {
  try {
    action();
  } catch (error) {
    if (error instanceof FrameError) return error;
    throw error;
  }
  throw new Error("expected a FrameError");
}

describe("a streamed change body is cut into its batches", () => {
  const first = concat(frame(start), frame(fragment), frame(end));
  const second = concat(frame(start), frame(end));
  const third = concat(
    frame(start),
    frame(fragment),
    frame(fragment),
    frame(end),
  );
  const body = concat(first, second, third);

  it.each([1, 3, 7, body.byteLength])(
    "at each batch end, whatever the chunks (%i bytes each)",
    (size) => {
      const splitter = new ChangeBatchSplitter(1024);
      expect(split(splitter, body, size)).toEqual([first, second, third]);
      expect(splitter.pendingBytes).toBe(0);
    },
  );

  it("holds a batch that has not ended and returns it once it does", () => {
    const splitter = new ChangeBatchSplitter(1024);
    const cut = first.byteLength + 5;
    expect(splitter.push(body.subarray(0, cut))).toEqual([first]);
    expect(splitter.pendingBytes).toBe(5);
    expect(splitter.push(body.subarray(cut))).toEqual([second, third]);
    expect(splitter.pendingBytes).toBe(0);
  });

  it("returns each batch in a buffer of its own", () => {
    const splitter = new ChangeBatchSplitter(1024);
    const [batch] = splitter.push(first.slice());
    splitter.push(second.slice());
    expect(batch).toEqual(first);
    expect(batch?.byteOffset).toBe(0);
    expect(batch?.buffer.byteLength).toBe(first.byteLength);
  });

  it("keeps a revocation barrier in the batch its batch end closes", () => {
    const barrier = concat(
      frame(start),
      frame({
        case: "revocationBarrier",
        value: create(RevocationBarrierSchema, { barrierId: "b" }),
      }),
      frame(end),
    );
    const splitter = new ChangeBatchSplitter(1024);
    expect(splitter.push(barrier)).toEqual([barrier]);
  });

  it("ends a batch at a stream error, wherever it stands", () => {
    const error = frame({
      case: "error",
      value: create(StreamErrorSchema, { message: "refused" }),
    });
    const splitter = new ChangeBatchSplitter(1024);
    expect(splitter.push(concat(first, frame(start), error))).toEqual([
      first,
      concat(frame(start), error),
    ]);
    expect(splitter.pendingBytes).toBe(0);
  });

  it("does not end a batch at a frame that selects no body", () => {
    const bodiless = frame({ case: undefined });
    const splitter = new ChangeBatchSplitter(1024);
    expect(splitter.push(concat(frame(start), bodiless))).toEqual([]);
    expect(splitter.push(frame(end))).toEqual([
      concat(frame(start), bodiless, frame(end)),
    ]);
  });
});

describe("a body the splitter cannot cut is refused and poisons it", () => {
  it.each([
    ["ZERO_LENGTH", new Uint8Array(4)],
    ["PAYLOAD_TOO_LARGE", new Uint8Array([0, 0, 1, 1])],
    ["MALFORMED_PROTOBUF", new Uint8Array([0, 0, 0, 2, 0x0a, 0x05])],
  ] as const)("%s", (code, body) => {
    const splitter = new ChangeBatchSplitter(1024, 256);
    expect(caught(() => splitter.push(body)).code).toBe(code);
    expect(caught(() => splitter.push(frame(end))).code).toBe(
      "DECODER_POISONED",
    );
  });

  it("a batch longer than the bound, as soon as a prefix says so", () => {
    const fragmentFrame = frame(fragment);
    const bound = frame(start).byteLength + fragmentFrame.byteLength;
    const splitter = new ChangeBatchSplitter(bound);
    expect(splitter.push(concat(frame(start), fragmentFrame))).toEqual([]);
    // Only the next frame's prefix has arrived; it already cannot fit.
    expect(caught(() => splitter.push(frame(end).subarray(0, 4))).code).toBe(
      "BATCH_TOO_LARGE",
    );
  });

  it("but not a batch that exactly fills it", () => {
    const exact = concat(frame(start), frame(end));
    const splitter = new ChangeBatchSplitter(exact.byteLength);
    expect(splitter.push(concat(exact, exact))).toEqual([exact, exact]);
  });
});

describe("a batch is decoded with each frame's body as it arrived", () => {
  it("keeps every frame's body bytes beside it", () => {
    const received = decodeReceivedChangeFrames(
      concat(frame(start), frame(fragment), frame(end)),
    );
    expect(received.map(({ frame }) => frame.body.case)).toEqual([
      "batchStart",
      "transactionFragment",
      "batchEnd",
    ]);
    expect(received[1]?.body).toEqual(
      toBinary(TransactionFragmentSchema, fragmentValue),
    );
  });

  it.each([
    ["ZERO_LENGTH", new Uint8Array(4)],
    ["PAYLOAD_TOO_LARGE", new Uint8Array([0, 0, 1, 1])],
    ["TRUNCATED_LENGTH_PREFIX", new Uint8Array([0, 0])],
    ["TRUNCATED_PAYLOAD", frame(end).subarray(0, 8)],
    ["MALFORMED_PROTOBUF", new Uint8Array([0, 0, 0, 2, 0x0a, 0x05])],
  ] as const)("refuses %s", (code, body) => {
    expect(caught(() => decodeReceivedChangeFrames(body, 256)).code).toBe(code);
  });

  it.each([
    [
      "the same body twice",
      11,
      toBinary(TransactionFragmentSchema, fragmentValue),
    ],
    ["a second, different body", 12, toBinary(BatchEndSchema, endValue)],
  ] as const)("refuses a frame that selects %s", (_, field, again) => {
    const payload = frame(fragment).subarray(4);
    const twice = concat(
      payload,
      new Uint8Array([(field << 3) | 2, again.byteLength]),
      again,
    );
    const prefix = new Uint8Array(4);
    new DataView(prefix.buffer).setUint32(0, twice.byteLength, false);
    expect(
      caught(() => decodeReceivedChangeFrames(concat(prefix, twice))).code,
    ).toBe("MALFORMED_PROTOBUF");
  });
});
