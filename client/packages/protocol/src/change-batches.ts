import { fromBinary } from "@bufbuild/protobuf";
import { BinaryReader, WireType } from "@bufbuild/protobuf/wire";

import {
  DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
  FRAME_LENGTH_PREFIX_BYTES,
  FrameError,
} from "./framing.js";
import {
  ChangeStreamFrameSchema,
  type ChangeStreamFrame,
} from "./gen/change_stream_pb.js";

const BODY_FIELDS = new Set(
  ChangeStreamFrameSchema.oneofs
    .find((oneof) => oneof.localName === "body")
    ?.fields.map((field) => field.number),
);
const BATCH_END = ChangeStreamFrameSchema.field.batchEnd.number;
const ERROR = ChangeStreamFrameSchema.field.error.number;

/**
 * Cuts a streamed change response (`ChangeRequest.stream`) into batches, each
 * in its own buffer, as the bytes arrive. A batch ends at a `BatchEnd` frame
 * or at a `StreamError`, which ends a response wherever it stands. Only length
 * prefixes and top-level field tags are read here; whoever applies a batch
 * still decodes and verifies every frame.
 *
 * `pendingBytes` lets a reader whose body ended tell a stop between batches
 * from a cut inside one. A batch over `maxBatchBytes` is refused as soon as a
 * prefix announces it, and any failure poisons the splitter, because the next
 * boundary is no longer trustworthy.
 */
export class ChangeBatchSplitter {
  readonly #maxBatchBytes: number;
  readonly #maxFramePayloadBytes: number;
  #buffer = new Uint8Array(0);
  #length = 0;
  /**
   * Where the first frame not yet read starts; every byte before it belongs
   * to the batch being assembled.
   */
  #scanned = 0;
  #poisoned = false;

  constructor(
    maxBatchBytes: number,
    maxFramePayloadBytes = DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
  ) {
    if (!Number.isSafeInteger(maxBatchBytes) || maxBatchBytes < 1)
      throw new RangeError("maxBatchBytes must be a positive integer");
    if (
      !Number.isSafeInteger(maxFramePayloadBytes) ||
      maxFramePayloadBytes < 1 ||
      maxFramePayloadBytes > 0xffff_ffff
    )
      throw new RangeError(
        "maxFramePayloadBytes must be an integer between 1 and 2^32-1",
      );
    this.#maxBatchBytes = maxBatchBytes;
    this.#maxFramePayloadBytes = maxFramePayloadBytes;
  }

  /** Bytes held of a batch that has not ended. */
  get pendingBytes(): number {
    return this.#length;
  }

  /** Returns every batch `chunk` completes, in order. */
  push(chunk: Uint8Array): Uint8Array[] {
    if (this.#poisoned)
      throw new FrameError(
        "DECODER_POISONED",
        "batch splitter cannot continue after a terminal failure",
      );
    try {
      this.#append(chunk);
      const view = new DataView(
        this.#buffer.buffer,
        this.#buffer.byteOffset,
        this.#buffer.byteLength,
      );
      const batches: Uint8Array[] = [];
      let start = 0;
      while (this.#length - this.#scanned >= FRAME_LENGTH_PREFIX_BYTES) {
        const declared = view.getUint32(this.#scanned, false);
        if (declared === 0)
          throw new FrameError("ZERO_LENGTH", "zero-length frames are invalid");
        if (declared > this.#maxFramePayloadBytes)
          throw new FrameError(
            "PAYLOAD_TOO_LARGE",
            `frame payload length ${declared} exceeds configured maximum ${this.#maxFramePayloadBytes}`,
          );
        const end = this.#scanned + FRAME_LENGTH_PREFIX_BYTES + declared;
        if (end - start > this.#maxBatchBytes)
          throw new FrameError(
            "BATCH_TOO_LARGE",
            `change batch exceeds ${this.#maxBatchBytes} bytes`,
          );
        if (end > this.#length) break;
        const body = bodyField(
          this.#buffer.subarray(this.#scanned + FRAME_LENGTH_PREFIX_BYTES, end),
        );
        this.#scanned = end;
        if (body === BATCH_END || body === ERROR) {
          batches.push(this.#buffer.slice(start, end));
          start = end;
        }
      }
      if (start > 0) {
        this.#buffer.copyWithin(0, start, this.#length);
        this.#length -= start;
        this.#scanned -= start;
      }
      return batches;
    } catch (error) {
      this.#poisoned = true;
      throw error;
    }
  }

  #append(chunk: Uint8Array): void {
    const needed = this.#length + chunk.byteLength;
    if (needed > this.#buffer.byteLength) {
      const grown = new Uint8Array(
        Math.max(needed, this.#buffer.byteLength * 2),
      );
      grown.set(this.#buffer.subarray(0, this.#length));
      this.#buffer = grown;
    }
    this.#buffer.set(chunk, this.#length);
    this.#length = needed;
  }
}

/**
 * The number of the body field a frame selects, or `undefined` if it selects
 * none.
 */
function bodyField(payload: Uint8Array): number | undefined {
  const reader = new BinaryReader(payload);
  let body: number | undefined;
  try {
    while (reader.pos < reader.len) {
      const [fieldNo, wireType] = reader.tag();
      reader.skip(wireType, fieldNo);
      // A oneof member that appears twice is decoded as the last one, so the
      // last one is the frame's body here too.
      if (BODY_FIELDS.has(fieldNo)) body = fieldNo;
    }
  } catch (error) {
    throw new FrameError(
      "MALFORMED_PROTOBUF",
      "frame contains malformed protobuf",
      { cause: error },
    );
  }
  return body;
}

/** A change frame as it arrived: the decoded frame and its body's own bytes. */
export interface ReceivedChangeFrame {
  readonly frame: ChangeStreamFrame;
  /**
   * The selected body field's bytes as they arrived, which `body_sha256`
   * covers. For a conformant sender's frame they are the deterministic
   * serialization the v1 preimage names, so the digest is checked over them
   * without encoding the decoded body again.
   */
  readonly body: Uint8Array;
}

/**
 * Decodes a complete buffer of change frames with the checks `FrameDecoder`
 * makes, keeping each frame's body bytes beside it. A frame that selects a
 * body more than once is refused: its decoded body is a merge that no single
 * run of its bytes carries.
 */
export function decodeReceivedChangeFrames(
  bytes: Uint8Array,
  maxPayloadBytes = DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
): ReceivedChangeFrame[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const frames: ReceivedChangeFrame[] = [];
  let offset = 0;
  while (offset < bytes.byteLength) {
    if (bytes.byteLength - offset < FRAME_LENGTH_PREFIX_BYTES)
      throw new FrameError(
        "TRUNCATED_LENGTH_PREFIX",
        `frame length prefix is truncated: received ${bytes.byteLength - offset} of ${FRAME_LENGTH_PREFIX_BYTES} bytes`,
      );
    const declared = view.getUint32(offset, false);
    offset += FRAME_LENGTH_PREFIX_BYTES;
    if (declared === 0)
      throw new FrameError("ZERO_LENGTH", "zero-length frames are invalid");
    if (declared > maxPayloadBytes)
      throw new FrameError(
        "PAYLOAD_TOO_LARGE",
        `frame payload length ${declared} exceeds configured maximum ${maxPayloadBytes}`,
      );
    if (declared > bytes.byteLength - offset)
      throw new FrameError(
        "TRUNCATED_PAYLOAD",
        `frame payload is truncated: received ${bytes.byteLength - offset} of ${declared} bytes`,
      );
    // Each frame gets its own buffer, as `FrameDecoder` gives it, so a decoded
    // bytes field that is kept holds its frame and not the whole batch.
    const payload = bytes.slice(offset, offset + declared);
    offset += declared;
    try {
      frames.push({
        frame: fromBinary(ChangeStreamFrameSchema, payload),
        body: selectedBody(payload),
      });
    } catch (error) {
      throw new FrameError(
        "MALFORMED_PROTOBUF",
        "frame contains malformed protobuf",
        { cause: error },
      );
    }
  }
  return frames;
}

/** The one body field's bytes, or none when the frame selects no body. */
function selectedBody(payload: Uint8Array): Uint8Array {
  const reader = new BinaryReader(payload);
  let body: Uint8Array | undefined;
  while (reader.pos < reader.len) {
    const [fieldNo, wireType] = reader.tag();
    if (!BODY_FIELDS.has(fieldNo)) {
      reader.skip(wireType, fieldNo);
      continue;
    }
    if (wireType !== WireType.LengthDelimited || body !== undefined)
      throw new Error("frame selects its body more than once");
    body = reader.bytes();
  }
  return body ?? new Uint8Array(0);
}
