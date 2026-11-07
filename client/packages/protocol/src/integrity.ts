import { toBinary } from "@bufbuild/protobuf";

import type { ChangeStreamFrame } from "./gen/change_stream_pb.js";
import {
  BatchEndSchema,
  ChangeStreamFrameSchema,
  BatchStartSchema,
  HeartbeatSchema,
  RevocationBarrierSchema,
  StreamErrorSchema,
  TransactionFragmentSchema,
} from "./gen/change_stream_pb.js";
import { ProtocolValidationError } from "./validation.js";

const CHECKSUM_DOMAIN = new TextEncoder().encode(
  "calque.change.body-sha256.v2\0",
);
const BATCH_FRAMES_DOMAIN = new TextEncoder().encode(
  "calque.change.batch-frames-sha256.v1\0",
);

export async function changeFrameBodySha256(
  frame: ChangeStreamFrame,
): Promise<Uint8Array<ArrayBuffer>> {
  const [bodyTag, bodyBytes] = encodeKnownBody(frame);
  return bodySha256(bodyTag, bodyBytes);
}

async function bodySha256(
  bodyTag: number,
  bodyBytes: Uint8Array,
): Promise<Uint8Array<ArrayBuffer>> {
  if (bodyBytes.byteLength > 0xffff_ffff) {
    throw new ProtocolValidationError(
      "changeStreamFrame.body",
      "encoded body exceeds the u32 checksum length field",
    );
  }
  const preimage = new Uint8Array(
    CHECKSUM_DOMAIN.byteLength + 1 + 4 + bodyBytes.byteLength,
  );
  preimage.set(CHECKSUM_DOMAIN, 0);
  const offset = CHECKSUM_DOMAIN.byteLength;
  preimage[offset] = bodyTag;
  new DataView(preimage.buffer).setUint32(
    offset + 1,
    bodyBytes.byteLength,
    false,
  );
  preimage.set(bodyBytes, offset + 5);
  return new Uint8Array(await crypto.subtle.digest("SHA-256", preimage));
}

/**
 * `BatchEnd.framesSha256` over the frames that precede it, fed their body
 * digests in order: SHA-256 of the domain followed by each digest.
 */
export class BatchFramesDigest {
  readonly #digests: Uint8Array[] = [];

  push(bodySha256: Uint8Array): void {
    if (bodySha256.byteLength !== 32) {
      throw new ProtocolValidationError(
        "changeStreamFrame.bodySha256",
        "must contain exactly 32 bytes",
      );
    }
    this.#digests.push(bodySha256.slice());
  }

  get frames(): number {
    return this.#digests.length;
  }

  async finish(): Promise<Uint8Array<ArrayBuffer>> {
    const preimage = new Uint8Array(
      BATCH_FRAMES_DOMAIN.byteLength + 32 * this.#digests.length,
    );
    preimage.set(BATCH_FRAMES_DOMAIN, 0);
    this.#digests.forEach((digest, index) =>
      preimage.set(digest, BATCH_FRAMES_DOMAIN.byteLength + 32 * index),
    );
    return new Uint8Array(await crypto.subtle.digest("SHA-256", preimage));
  }
}

/**
 * Checks `body_sha256`. With `receivedBody`, the frame's body as it arrived
 * (`decodeReceivedChangeFrames`), the digest is taken over those bytes rather
 * than over a fresh encoding of the decoded body; for a conformant sender's
 * frame the two are the same bytes.
 */
export async function verifyChangeFrameBodySha256(
  frame: ChangeStreamFrame,
  receivedBody?: Uint8Array,
): Promise<void> {
  if (frame.bodySha256.byteLength !== 32) {
    throw new ProtocolValidationError(
      "changeStreamFrame.bodySha256",
      "must contain exactly 32 bytes",
    );
  }
  const expected =
    receivedBody === undefined
      ? await changeFrameBodySha256(frame)
      : await bodySha256(selectedBodyTag(frame), receivedBody);
  let difference = 0;
  for (let index = 0; index < expected.byteLength; index += 1) {
    const expectedByte = expected[index];
    const actualByte = frame.bodySha256[index];
    if (expectedByte === undefined || actualByte === undefined) {
      throw new ProtocolValidationError(
        "changeStreamFrame.bodySha256",
        "must contain exactly 32 bytes",
      );
    }
    difference |= expectedByte ^ actualByte;
  }
  if (difference !== 0) {
    throw new ProtocolValidationError(
      "changeStreamFrame.bodySha256",
      "does not match the v2 body preimage",
    );
  }
}

function selectedBodyTag(frame: ChangeStreamFrame): number {
  if (frame.body.case === undefined)
    throw new ProtocolValidationError("changeStreamFrame.body", "is required");
  return ChangeStreamFrameSchema.field[frame.body.case].number;
}

function encodeKnownBody(
  frame: ChangeStreamFrame,
): readonly [number, Uint8Array<ArrayBuffer>] {
  const options = { writeUnknownFields: false } as const;
  switch (frame.body.case) {
    case "batchStart":
      return [10, toBinary(BatchStartSchema, frame.body.value, options)];
    case "transactionFragment":
      return [
        11,
        toBinary(TransactionFragmentSchema, frame.body.value, options),
      ];
    case "batchEnd":
      return [12, toBinary(BatchEndSchema, frame.body.value, options)];
    case "heartbeat":
      return [13, toBinary(HeartbeatSchema, frame.body.value, options)];
    case "error":
      return [14, toBinary(StreamErrorSchema, frame.body.value, options)];
    case "revocationBarrier":
      return [15, toBinary(RevocationBarrierSchema, frame.body.value, options)];
    case undefined:
      throw new ProtocolValidationError(
        "changeStreamFrame.body",
        "is required",
      );
  }
}
