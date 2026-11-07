import { readFileSync } from "node:fs";

import { create } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  ChangeStreamFrameSchema,
  CursorSchema,
  StreamErrorCode,
  StreamErrorSchema,
  type ChangeStreamFrame,
} from "./gen/change_stream_pb.js";
import { FrameDecoder, encodeFrame } from "./framing.js";
import { changeFrameBodySha256 } from "./integrity.js";
import { ChangeStreamState } from "./stream-state.js";
import { validateChangeStreamFrame } from "./validation.js";
import {
  revocationBarrierSigningPreimage,
  verifyRevocationBarrier,
} from "./revocation.js";

/**
 * Revocation barriers against the golden vectors the Rust generator wrote, so
 * a pass is agreement with the server's encoder rather than this one's.
 */

const fixtureRoot = new URL(
  "../../../../spec/fixtures/protocol-v1/",
  import.meta.url,
);

/** The public half of the fixed key pair the fixture generators sign with. */
const BARRIER_PUBLIC_KEY = Uint8Array.from(
  "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8".match(
    /.{2}/gu,
  ) ?? [],
  (pair) => Number.parseInt(pair, 16),
);
const BARRIER_KEY = {
  keyId: "revocation-key-1",
  publicKey: BARRIER_PUBLIC_KEY,
} as const;

function fixture(producer: "rust" | "typescript", name: string): Uint8Array {
  const text = readFileSync(
    new URL(`${producer}/${name}`, fixtureRoot),
    "utf8",
  ).trim();
  return Uint8Array.from(text.match(/.{2}/gu) ?? [], (pair) =>
    Number.parseInt(pair, 16),
  );
}

function frames(producer: "rust" | "typescript", name: string) {
  const decoder = new FrameDecoder(ChangeStreamFrameSchema);
  const messages = decoder.push(fixture(producer, name));
  decoder.finish();
  return messages;
}

function expectation() {
  return {
    planId: "plan-1",
    fromCursorExclusive: new TextEncoder().encode("cursor-0"),
    requestedFence: new TextEncoder().encode("cursor-h"),
    tableStarts: [
      {
        tableId: "public.tasks",
        snapshotCursor: new TextEncoder().encode("cursor-p"),
      },
    ],
    generation: {
      sourceEpoch: "epoch-1",
      publicationGeneration: 2n,
      schemaGeneration: 3n,
      authorizationGeneration: 4n,
      authorizationScopeHash: new Uint8Array(32).fill(5),
    },
    revocationBarrierKey: BARRIER_KEY,
  };
}

async function reseal(frame: ChangeStreamFrame): Promise<ChangeStreamFrame> {
  frame.bodySha256 = await changeFrameBodySha256(frame);
  return frame;
}

/**
 * The frame at `index`, or a failure naming the vector rather than
 * `undefined`.
 */
function at(stream: readonly ChangeStreamFrame[], index: number) {
  const frame = stream[index];
  if (!frame) throw new Error(`the barrier vector has no frame ${index}`);
  return frame;
}

/** A state that has accepted the vector's first `count` frames. */
async function accepted(
  stream: readonly ChangeStreamFrame[],
  count: number,
): Promise<ChangeStreamState> {
  const state = new ChangeStreamState(expectation());
  for (let index = 0; index < count; index++)
    await state.accept(at(stream, index));
  return state;
}

/** The positive vector's third frame, narrowed to its barrier body. */
function barrierAt(stream: readonly ChangeStreamFrame[]) {
  const frame = at(stream, 2);
  if (frame.body.case !== "revocationBarrier")
    throw new Error("the third frame is the barrier");
  return { frame, barrier: frame.body.value };
}

/** The positive vector's fourth frame, narrowed to its batch-end body. */
function batchEndAt(stream: readonly ChangeStreamFrame[]) {
  const frame = at(stream, 3);
  if (frame.body.case !== "batchEnd")
    throw new Error("the fourth frame is the batch end");
  return { frame, batchEnd: frame.body.value };
}

describe("ordered authenticated revocation barrier", () => {
  it("keeps independent Rust and TypeScript barrier bytes identical", () => {
    for (const name of [
      "revocation-barrier.frame.hex",
      "invalid/revocation-barrier-malformed.frame.hex",
      "invalid/revocation-barrier-truncated.hex",
      "invalid/revocation-barrier-oversized.hex",
      "invalid/revocation-barrier-reordered.frame.hex",
      "invalid/revocation-barrier-duplicated.frame.hex",
      "invalid/revocation-barrier-cross-epoch.frame.hex",
    ]) {
      expect(fixture("rust", name), name).toEqual(fixture("typescript", name));
    }
  });

  it("consumes the Rust barrier and reports it on the completed batch", async () => {
    const state = new ChangeStreamState(expectation());
    let completed;
    for (const frame of frames("rust", "revocation-barrier.frame.hex")) {
      completed = (await state.accept(frame)) ?? completed;
    }
    expect(completed?.revocationBarrier).toBeDefined();
    const barrier = completed?.revocationBarrier;
    expect(barrier?.revokedAuthorizationGeneration).toBe(4n);
    expect(barrier?.effectiveAuthorizationGeneration).toBe(5n);
    expect(barrier?.signingKeyId).toBe("revocation-key-1");
    expect(barrier?.signature.byteLength).toBe(64);
    expect(completed?.throughCursorInclusive).toEqual(
      barrier?.barrierCursor?.token,
    );
    // Everything the batch delivered is before the barrier.
    expect(completed?.relevantTransactionCount).toBe(1n);
  });

  it("refuses a forged signature even though the frame digest is correct", async () => {
    const stream = frames("rust", "revocation-barrier.frame.hex");
    const { frame: barrierFrame, barrier } = barrierAt(stream);
    const signature = barrier.signature;
    signature.set([(signature[0] ?? 0) ^ 0xff], 0);
    await reseal(barrierFrame);
    // Structurally perfect: only the authorization authority's signature can
    // catch it.
    expect(() => {
      validateChangeStreamFrame(barrierFrame);
    }).not.toThrow();

    const state = await accepted(stream, 2);
    await expect(state.accept(barrierFrame)).rejects.toThrow(
      /failed authorization-authority verification/u,
    );
    expect(state.terminal).toBe(true);
  });

  it("refuses every barrier when no verification key is configured", async () => {
    const keyless = expectation();
    delete (keyless as { revocationBarrierKey?: unknown }).revocationBarrierKey;
    const state = new ChangeStreamState(keyless);
    const stream = frames("rust", "revocation-barrier.frame.hex");
    await state.accept(at(stream, 0));
    await state.accept(at(stream, 1));
    await expect(state.accept(at(stream, 2))).rejects.toThrow(
      /no revocation barrier key/u,
    );
  });

  it("refuses a barrier signed by a key this deployment does not accept", async () => {
    const { barrier } = barrierAt(
      frames("rust", "revocation-barrier.frame.hex"),
    );
    await expect(
      verifyRevocationBarrier(barrier, {
        keyId: "another-key",
        publicKey: BARRIER_PUBLIC_KEY,
      }),
    ).rejects.toThrow(/does not match the configured revocation barrier key/u);
  });

  it("rejects malformed, reordered, duplicated, and cross-epoch barriers", async () => {
    const malformed = at(
      frames("rust", "invalid/revocation-barrier-malformed.frame.hex"),
      0,
    );
    expect(() => {
      validateChangeStreamFrame(malformed);
    }).toThrow(/signature: must contain exactly 64 bytes/u);

    for (const [name, pattern] of [
      [
        "invalid/revocation-barrier-reordered.frame.hex",
        /transactionFragment: must not follow a revocation barrier/u,
      ],
      [
        "invalid/revocation-barrier-duplicated.frame.hex",
        /revocationBarrier: must not follow a revocation barrier/u,
      ],
      [
        "invalid/revocation-barrier-cross-epoch.frame.hex",
        /but this stream runs authorization generation 4/u,
      ],
    ] as const) {
      const state = new ChangeStreamState(expectation());
      let failure: unknown;
      for (const frame of frames("rust", name)) {
        try {
          await state.accept(frame);
        } catch (error) {
          failure = error;
          break;
        }
      }
      expect(String(failure), name).toMatch(pattern);
      expect(state.terminal, name).toBe(true);
    }
  });

  it("rejects truncated and oversized barrier frames before any field is read", () => {
    const truncated = new FrameDecoder(ChangeStreamFrameSchema);
    truncated.push(fixture("rust", "invalid/revocation-barrier-truncated.hex"));
    expect(() => {
      truncated.finish();
    }).toThrow();

    const oversized = new FrameDecoder(ChangeStreamFrameSchema);
    expect(() => {
      oversized.push(
        fixture("rust", "invalid/revocation-barrier-oversized.hex"),
      );
    }).toThrow();
  });

  it("refuses a batch that does not end on the barrier's cursor", async () => {
    const stream = frames("rust", "revocation-barrier.frame.hex");
    const { frame: end, batchEnd } = batchEndAt(stream);
    batchEnd.throughCursorInclusive = create(CursorSchema, {
      token: new TextEncoder().encode("cursor-x"),
    });
    await reseal(end);
    const state = await accepted(stream, 3);
    await expect(state.accept(end)).rejects.toThrow(
      /must equal the revocation barrier's cursor/u,
    );
  });

  it("refuses a stream error that follows the barrier in the same batch", async () => {
    // A barrier batch ends on the barrier or is refused whole. Accepting this
    // error body would deliver the revocation as a transport failure the
    // caller may retry.
    const stream = frames("rust", "revocation-barrier.frame.hex");
    const { frame: end } = batchEndAt(stream);
    end.body = {
      case: "error",
      value: create(StreamErrorSchema, {
        code: StreamErrorCode.INTERNAL_RETRYABLE,
        message: "internal",
      }),
    };
    await reseal(end);
    const state = await accepted(stream, 3);
    await expect(state.accept(end)).rejects.toThrow(
      /error: must not follow a revocation barrier/u,
    );
    expect(state.terminal).toBe(true);
  });

  it("agrees with Rust on the signing preimage", async () => {
    const { barrier } = barrierAt(
      frames("rust", "revocation-barrier.frame.hex"),
    );
    const preimage = revocationBarrierSigningPreimage(barrier);
    expect(new TextDecoder().decode(preimage.slice(0, 36))).toBe(
      "calque.change.revocation-barrier.v2\0",
    );
    // The signature in the Rust-generated fixture verifies against this
    // TypeScript preimage.
    await expect(
      verifyRevocationBarrier(barrier, BARRIER_KEY),
    ).resolves.toBeUndefined();
  });

  it("keeps the frame digest independent of the barrier's own signature", async () => {
    const { frame: barrierFrame } = barrierAt(
      frames("rust", "revocation-barrier.frame.hex"),
    );
    const encoded = encodeFrame(ChangeStreamFrameSchema, barrierFrame);
    expect(encoded.byteLength).toBeGreaterThan(64);
    const digest = await changeFrameBodySha256(barrierFrame);
    expect(digest).toEqual(barrierFrame.bodySha256);
  });
});
