import { create, toBinary } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  ChangeStreamFrameSchema,
  CursorSchema,
  HeartbeatSchema,
} from "./gen/change_stream_pb.js";
import { decodeReceivedChangeFrames } from "./change-batches.js";
import { encodeFrame } from "./framing.js";
import {
  BatchFramesDigest,
  changeFrameBodySha256,
  verifyChangeFrameBodySha256,
} from "./integrity.js";

function heartbeatFrame() {
  return create(ChangeStreamFrameSchema, {
    protocolVersion: 1,
    body: {
      case: "heartbeat",
      value: create(HeartbeatSchema, {
        durableHead: create(CursorSchema, {
          token: new TextEncoder().encode("cursor-9"),
        }),
        serverTimeUnixMicros: 42n,
      }),
    },
  });
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

describe("change frame body integrity", () => {
  it("matches the Rust heartbeat checksum golden vector", async () => {
    expect(hex(await changeFrameBodySha256(heartbeatFrame()))).toBe(
      "3354e0618a1b1bc8398e9b660a5f6af1f02c6a2702f8f2dc4f4e4a67edab420e",
    );
  });

  it("matches the Rust batch frames digest golden vectors", async () => {
    const heartbeat = await changeFrameBodySha256(heartbeatFrame());
    const frames = new BatchFramesDigest();
    expect(hex(await frames.finish())).toBe(
      "0038f268dd7e088a6f73143de821a658ecb6fcf0238f50f837e85c4ca73d8ba2",
    );
    frames.push(heartbeat);
    frames.push(heartbeat);
    expect(frames.frames).toBe(2);
    expect(hex(await frames.finish())).toBe(
      "4e3e251e441747e7acca24728d1d2b71b04638551ebcb611bbc1c3541c9a24ca",
    );
  });

  it("accepts the exact digest and rejects body tampering", async () => {
    const frame = heartbeatFrame();
    frame.bodySha256 = await changeFrameBodySha256(frame);
    await expect(verifyChangeFrameBodySha256(frame)).resolves.toBeUndefined();

    if (frame.body.case !== "heartbeat") throw new Error("not a heartbeat");
    frame.body.value.serverTimeUnixMicros += 1n;
    await expect(verifyChangeFrameBodySha256(frame)).rejects.toThrow(
      /does not match/u,
    );
  });

  it("checks the digest over a body as it arrived", async () => {
    const sent = heartbeatFrame();
    sent.bodySha256 = await changeFrameBodySha256(sent);
    const [received] = decodeReceivedChangeFrames(
      encodeFrame(ChangeStreamFrameSchema, sent),
    );
    if (!received) throw new Error("no frame decoded");
    await expect(
      verifyChangeFrameBodySha256(received.frame, received.body),
    ).resolves.toBeUndefined();

    const changed = received.body.slice();
    changed[changed.byteLength - 1] = (changed.at(-1) ?? 0) ^ 1;
    await expect(
      verifyChangeFrameBodySha256(received.frame, changed),
    ).rejects.toThrow(/does not match/u);
  });

  it("covers a received body's fields this receiver does not know", async () => {
    // A sender that knows a field this receiver does not hashes it with the
    // rest. Over the bytes as they arrived the digest matches, and the
    // decoded frame simply does not carry the field.
    const frame = heartbeatFrame();
    if (frame.body.case !== "heartbeat") throw new Error("not a heartbeat");
    // Field 111, a varint 1, after the fields this receiver knows.
    const body = new Uint8Array([
      ...toBinary(HeartbeatSchema, frame.body.value),
      0xf8,
      0x06,
      0x01,
    ]);
    const preimage = new Uint8Array([
      ...new TextEncoder().encode("calque.change.body-sha256.v2\0"),
      13,
      0,
      0,
      0,
      body.byteLength,
      ...body,
    ]);
    frame.bodySha256 = new Uint8Array(
      await crypto.subtle.digest("SHA-256", preimage),
    );
    await expect(
      verifyChangeFrameBodySha256(frame, body),
    ).resolves.toBeUndefined();
    // Encoding the decoded body again drops the field, so that digest differs.
    await expect(verifyChangeFrameBodySha256(frame)).rejects.toThrow(
      /does not match/u,
    );
  });
});
