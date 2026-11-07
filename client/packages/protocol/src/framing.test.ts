import {
  create,
  toBinary,
  type DescMessage,
  type MessageShape,
} from "@bufbuild/protobuf";
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  ChangeStreamFrameSchema,
  HeartbeatSchema,
} from "./gen/change_stream_pb.js";
import { CursorSchema as ChangeCursorSchema } from "./gen/change_stream_pb.js";
import {
  CursorSchema as MutationCursorSchema,
  MutationBatchRequestSchema,
  MutationEnvelopeSchema,
} from "./gen/mutation_pb.js";
import {
  DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
  encodeFrame,
  FrameDecoder,
  FrameError,
} from "./framing.js";

const rustGoldenPath = new URL(
  "../../../../server/crates/sync-protocol/tests/fixtures/heartbeat.frame.hex",
  import.meta.url,
);

function rustGoldenBytes(): Uint8Array {
  return Uint8Array.from(
    readFileSync(rustGoldenPath, "utf8")
      .trim()
      .split(/\s+/u)
      .map((byte) => Number.parseInt(byte, 16)),
  );
}

/** The one message `frame` holds. */
function decodeFrame<Schema extends DescMessage>(
  schema: Schema,
  frame: Uint8Array,
  maxPayloadBytes?: number,
): MessageShape<Schema> {
  const decoder = new FrameDecoder(schema, maxPayloadBytes);
  const messages = decoder.push(frame);
  decoder.finish();
  expect(messages).toHaveLength(1);
  return messages[0] as MessageShape<Schema>;
}

function heartbeatFrame() {
  return create(ChangeStreamFrameSchema, {
    protocolVersion: 1,
    body: {
      case: "heartbeat",
      value: create(HeartbeatSchema, {
        durableHead: create(ChangeCursorSchema, { token: Uint8Array.of(0xaa) }),
        serverTimeUnixMicros: 9n,
      }),
    },
  });
}

describe("framing", () => {
  it("matches and decodes the Rust heartbeat golden frame", () => {
    const expected = rustGoldenBytes();
    const encoded = encodeFrame(ChangeStreamFrameSchema, heartbeatFrame());

    expect(encoded).toEqual(expected);
    expect([...encoded.subarray(0, 4)]).toEqual([0, 0, 0, 11]);
    expect(decodeFrame(ChangeStreamFrameSchema, expected)).toEqual(
      heartbeatFrame(),
    );
  });

  it("round-trips 64-bit mutation values without number coercion", () => {
    const request = create(MutationBatchRequestSchema, {
      protocolVersion: 1,
      clientInstanceId: "client-1",
      deviceId: "device-1",
      authorizationScopeToken: Uint8Array.of(1, 2, 3),
      mutations: [
        create(MutationEnvelopeSchema, {
          mutationId: "mutation-1",
          commandName: "tasks.rename",
          commandVersion: 1,
          canonicalInput: new TextEncoder().encode('{"title":"next"}'),
          baseCursor: create(MutationCursorSchema, {
            token: Uint8Array.of(4, 5, 6),
          }),
          createdAtUnixMicros: 9_007_199_254_740_993n,
        }),
      ],
    });

    const decoded = decodeFrame(
      MutationBatchRequestSchema,
      encodeFrame(MutationBatchRequestSchema, request),
    );
    expect(decoded).toEqual(request);
    expect(decoded.mutations[0]?.createdAtUnixMicros).toBe(
      9_007_199_254_740_993n,
    );
  });

  it("accepts fragmented input and multiple frames", () => {
    const frame = encodeFrame(ChangeStreamFrameSchema, heartbeatFrame());
    const bytes = new Uint8Array(frame.byteLength * 2);
    bytes.set(frame, 0);
    bytes.set(frame, frame.byteLength);
    const decoder = new FrameDecoder(ChangeStreamFrameSchema);
    const messages = [
      ...decoder.push(bytes.subarray(0, 2)),
      ...decoder.push(bytes.subarray(2, 7)),
      ...decoder.push(bytes.subarray(7)),
    ];
    decoder.finish();
    expect(messages).toEqual([heartbeatFrame(), heartbeatFrame()]);
  });

  it.each([1, 2, 3])("rejects a %i-byte truncated prefix", (length) => {
    const decoder = new FrameDecoder(ChangeStreamFrameSchema);
    decoder.push(new Uint8Array(length));
    expect(() => decoder.finish()).toThrowError(
      expect.objectContaining({ code: "TRUNCATED_LENGTH_PREFIX" }),
    );
    expect(() => decoder.push(Uint8Array.of(1))).toThrowError(
      expect.objectContaining({ code: "DECODER_POISONED" }),
    );
  });

  it("rejects zero length", () => {
    const decoder = new FrameDecoder(ChangeStreamFrameSchema);
    expect(() => decoder.push(new Uint8Array(4))).toThrowError(
      expect.objectContaining({ code: "ZERO_LENGTH" }),
    );
  });

  it("rejects oversize from the prefix before copying the payload", () => {
    const decoder = new FrameDecoder(ChangeStreamFrameSchema, 4);
    expect(() =>
      decoder.push(Uint8Array.of(0, 0, 0, 5, 0xaa, 0xbb)),
    ).toThrowError(expect.objectContaining({ code: "PAYLOAD_TOO_LARGE" }));
  });

  it("enforces the default four MiB bound", () => {
    const declared = DEFAULT_MAX_FRAME_PAYLOAD_BYTES + 1;
    const prefix = new Uint8Array(4);
    new DataView(prefix.buffer).setUint32(0, declared, false);
    const decoder = new FrameDecoder(ChangeStreamFrameSchema);
    expect(() => decoder.push(prefix)).toThrowError(
      expect.objectContaining({ code: "PAYLOAD_TOO_LARGE" }),
    );
  });

  it("accepts a payload exactly at its configured limit", () => {
    const payloadLength = toBinary(
      ChangeStreamFrameSchema,
      heartbeatFrame(),
    ).byteLength;
    expect(
      decodeFrame(
        ChangeStreamFrameSchema,
        encodeFrame(ChangeStreamFrameSchema, heartbeatFrame(), payloadLength),
        payloadLength,
      ),
    ).toEqual(heartbeatFrame());
  });

  it("rejects truncated payload and malformed protobuf", () => {
    const truncated = new FrameDecoder(ChangeStreamFrameSchema);
    truncated.push(Uint8Array.of(0, 0, 0, 3, 0x08, 0x01));
    expect(() => truncated.finish()).toThrowError(
      expect.objectContaining({ code: "TRUNCATED_PAYLOAD" }),
    );

    expect(() =>
      decodeFrame(ChangeStreamFrameSchema, Uint8Array.of(0, 0, 0, 1, 0xff)),
    ).toThrowError(expect.objectContaining({ code: "MALFORMED_PROTOBUF" }));
  });

  it("preserves an unknown optional protobuf field", () => {
    const knownPayload = toBinary(ChangeStreamFrameSchema, heartbeatFrame());
    // Unknown field 100, varint value 1.
    const unknown = Uint8Array.of(0xa0, 0x06, 0x01);
    const framed = new Uint8Array(
      4 + knownPayload.byteLength + unknown.byteLength,
    );
    new DataView(framed.buffer).setUint32(
      0,
      knownPayload.byteLength + unknown.byteLength,
      false,
    );
    framed.set(knownPayload, 4);
    framed.set(unknown, 4 + knownPayload.byteLength);

    const decoded = decodeFrame(ChangeStreamFrameSchema, framed);
    expect(decoded.$unknown).toHaveLength(1);
    expect(toBinary(ChangeStreamFrameSchema, decoded).subarray(-3)).toEqual(
      unknown,
    );
  });

  it("rejects an empty protobuf message during framing", () => {
    expect(() =>
      encodeFrame(ChangeStreamFrameSchema, create(ChangeStreamFrameSchema)),
    ).toThrowError(FrameError);
  });
});
