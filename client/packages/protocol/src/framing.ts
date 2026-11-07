import {
  fromBinary,
  toBinary,
  type DescMessage,
  type MessageShape,
} from "@bufbuild/protobuf";

export const FRAME_LENGTH_PREFIX_BYTES = 4;
export const DEFAULT_MAX_FRAME_PAYLOAD_BYTES = 4 * 1024 * 1024;

export type FrameErrorCode =
  | "BATCH_TOO_LARGE"
  | "DECODER_POISONED"
  | "MALFORMED_PROTOBUF"
  | "PAYLOAD_TOO_LARGE"
  | "TRUNCATED_LENGTH_PREFIX"
  | "TRUNCATED_PAYLOAD"
  | "ZERO_LENGTH";

export class FrameError extends Error {
  readonly code: FrameErrorCode;

  constructor(code: FrameErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "FrameError";
    this.code = code;
  }
}

function validateLimit(maxPayloadBytes: number): void {
  if (
    !Number.isSafeInteger(maxPayloadBytes) ||
    maxPayloadBytes < 1 ||
    maxPayloadBytes > 0xffff_ffff
  ) {
    throw new RangeError(
      "maxPayloadBytes must be an integer between 1 and 2^32-1",
    );
  }
}

export function encodeFrame<Schema extends DescMessage>(
  schema: Schema,
  message: MessageShape<Schema>,
  maxPayloadBytes = DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
): Uint8Array {
  validateLimit(maxPayloadBytes);
  const payload = toBinary(schema, message);
  if (payload.byteLength === 0) {
    throw new FrameError("ZERO_LENGTH", "zero-length frames are invalid");
  }
  if (payload.byteLength > maxPayloadBytes) {
    throw new FrameError(
      "PAYLOAD_TOO_LARGE",
      `frame payload length ${payload.byteLength} exceeds configured maximum ${maxPayloadBytes}`,
    );
  }

  const result = new Uint8Array(FRAME_LENGTH_PREFIX_BYTES + payload.byteLength);
  new DataView(result.buffer).setUint32(0, payload.byteLength, false);
  result.set(payload, FRAME_LENGTH_PREFIX_BYTES);
  return result;
}

/**
 * Incrementally decodes a byte stream without buffering more than one bounded
 * payload. Any terminal error poisons the decoder because the next boundary is
 * no longer trustworthy.
 */
export class FrameDecoder<Schema extends DescMessage> {
  readonly #schema: Schema;
  readonly #maxPayloadBytes: number;
  readonly #prefix = new Uint8Array(FRAME_LENGTH_PREFIX_BYTES);
  #prefixLength = 0;
  #payload: Uint8Array | undefined;
  #payloadLength = 0;
  #poisoned = false;

  constructor(
    schema: Schema,
    maxPayloadBytes = DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
  ) {
    validateLimit(maxPayloadBytes);
    this.#schema = schema;
    this.#maxPayloadBytes = maxPayloadBytes;
  }

  push(chunk: Uint8Array): MessageShape<Schema>[] {
    if (this.#poisoned) {
      throw new FrameError(
        "DECODER_POISONED",
        "frame decoder cannot continue after a terminal failure",
      );
    }

    try {
      const messages: MessageShape<Schema>[] = [];
      let offset = 0;
      while (offset < chunk.byteLength) {
        if (this.#payload === undefined) {
          const prefixBytes = Math.min(
            FRAME_LENGTH_PREFIX_BYTES - this.#prefixLength,
            chunk.byteLength - offset,
          );
          this.#prefix.set(
            chunk.subarray(offset, offset + prefixBytes),
            this.#prefixLength,
          );
          this.#prefixLength += prefixBytes;
          offset += prefixBytes;

          if (this.#prefixLength < FRAME_LENGTH_PREFIX_BYTES) {
            continue;
          }

          const declared = new DataView(this.#prefix.buffer).getUint32(
            0,
            false,
          );
          if (declared === 0) {
            throw new FrameError(
              "ZERO_LENGTH",
              "zero-length frames are invalid",
            );
          }
          if (declared > this.#maxPayloadBytes) {
            throw new FrameError(
              "PAYLOAD_TOO_LARGE",
              `frame payload length ${declared} exceeds configured maximum ${this.#maxPayloadBytes}`,
            );
          }
          this.#payload = new Uint8Array(declared);
          this.#payloadLength = 0;
        }

        const payload = this.#payload;
        const payloadBytes = Math.min(
          payload.byteLength - this.#payloadLength,
          chunk.byteLength - offset,
        );
        payload.set(
          chunk.subarray(offset, offset + payloadBytes),
          this.#payloadLength,
        );
        this.#payloadLength += payloadBytes;
        offset += payloadBytes;

        if (this.#payloadLength === payload.byteLength) {
          try {
            messages.push(fromBinary(this.#schema, payload));
          } catch (error) {
            throw new FrameError(
              "MALFORMED_PROTOBUF",
              "frame contains malformed protobuf",
              {
                cause: error,
              },
            );
          }
          this.#payload = undefined;
          this.#payloadLength = 0;
          this.#prefixLength = 0;
        }
      }
      return messages;
    } catch (error) {
      this.#poisoned = true;
      throw error;
    }
  }

  finish(): void {
    if (this.#poisoned) {
      throw new FrameError(
        "DECODER_POISONED",
        "frame decoder cannot continue after a terminal failure",
      );
    }
    if (this.#payload !== undefined) {
      this.#poisoned = true;
      throw new FrameError(
        "TRUNCATED_PAYLOAD",
        `frame payload is truncated: received ${this.#payloadLength} of ${this.#payload.byteLength} bytes`,
      );
    }
    if (this.#prefixLength > 0) {
      this.#poisoned = true;
      throw new FrameError(
        "TRUNCATED_LENGTH_PREFIX",
        `frame length prefix is truncated: received ${this.#prefixLength} of ${FRAME_LENGTH_PREFIX_BYTES} bytes`,
      );
    }
  }
}
