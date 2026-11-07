import { readFileSync } from "node:fs";

import type { DescMessage, MessageShape } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import {
  ChangeRequestSchema,
  ChangeStreamFrameSchema,
  OperationKind,
} from "./gen/change_stream_pb.js";
import {
  MutationBatchRequestSchema,
  MutationBatchResponseSchema,
  MutationLogicalMarkerSchema,
} from "./gen/mutation_pb.js";
import { FrameDecoder, FrameError } from "./framing.js";
import { verifyChangeFrameBodySha256 } from "./integrity.js";
import { decodePrimaryKey } from "./primary-key.js";
import { ChangeStreamState } from "./stream-state.js";
import {
  validateChangeRequest,
  validateChangeStreamFrame,
  validateMutationBatchRequest,
  validateMutationBatchResponse,
} from "./validation.js";

const fixtureRoot = new URL(
  "../../../../spec/fixtures/protocol-v1/",
  import.meta.url,
);

function fixture(producer: "rust" | "typescript", name: string): Uint8Array {
  const text = readFileSync(
    new URL(`${producer}/${name}`, fixtureRoot),
    "utf8",
  ).trim();
  if (
    text.length === 0 ||
    text.length % 2 !== 0 ||
    !/^[0-9a-f]+$/u.test(text)
  ) {
    throw new Error(`${producer}/${name} is not canonical lowercase hex`);
  }
  return Uint8Array.from(text.match(/.{2}/gu) ?? [], (pair) =>
    Number.parseInt(pair, 16),
  );
}

function decodeAll<Schema extends DescMessage>(
  schema: Schema,
  producer: "rust" | "typescript",
  name: string,
): MessageShape<Schema>[] {
  const decoder = new FrameDecoder(schema);
  const messages = decoder.push(fixture(producer, name));
  decoder.finish();
  return messages;
}

describe("protocol-v1 bidirectional semantic fixtures", () => {
  it("keeps independent Rust and TypeScript bytes identical", () => {
    for (const name of [
      "change-request.frame.hex",
      "change-stream.frame.hex",
      "mutation-request.frame.hex",
      "mutation-response.frame.hex",
      "mutation-marker.frame.hex",
      "primary-key.hex",
      "primary-key-text.hex",
    ]) {
      expect(fixture("rust", name), name).toEqual(fixture("typescript", name));
    }
  });

  it("consumes Rust-origin minimal, full and streaming change requests", () => {
    const requests = decodeAll(
      ChangeRequestSchema,
      "rust",
      "change-request.frame.hex",
    );
    expect(requests).toHaveLength(3);
    for (const request of requests)
      expect(() => validateChangeRequest(request)).not.toThrow();
    expect(requests[0]?.throughCursorInclusive).toBeUndefined();
    // Both sides of the revocation-barrier negotiation are on the wire: the
    // minimal request leaves it unset, the full one sets it.
    expect(requests[0]?.acceptRevocationBarrier).toBe(false);
    expect(requests[1]).toMatchObject({
      acceptTransactionFragmentation: true,
      acceptRevocationBarrier: true,
      maxBatchBytes: 16n * 1024n * 1024n,
      maxTransactions: 10_000,
      longPollTimeoutMs: 120_000,
    });
    // Only the live request with a wait asks for a stream.
    expect(requests.map((request) => request.stream)).toEqual([
      false,
      false,
      true,
    ]);
    expect(requests[2]?.throughCursorInclusive).toBeUndefined();
    expect(requests[2]?.longPollTimeoutMs).toBe(1_000);
  });

  it("consumes every Rust-origin frame body, operation, row cell, and error", async () => {
    const frames = decodeAll(
      ChangeStreamFrameSchema,
      "rust",
      "change-stream.frame.hex",
    );
    expect(frames).toHaveLength(15);
    for (const frame of frames) {
      expect(() => validateChangeStreamFrame(frame)).not.toThrow();
      await expect(verifyChangeFrameBodySha256(frame)).resolves.toBeUndefined();
    }
    expect(new Set(frames.map((frame) => frame.body.case))).toEqual(
      new Set([
        "batchStart",
        "transactionFragment",
        "batchEnd",
        "heartbeat",
        "error",
        "revocationBarrier",
      ]),
    );
    const end = frames.find((frame) => frame.body.case === "batchEnd");
    if (end?.body.case !== "batchEnd") throw new Error("missing batch end");
    expect(end.body.value.acknowledgedMutationIds).toEqual([
      "mutation-1",
      "mutation-2",
    ]);
    expect(end.body.value.frameCount).toBe(3n);
    const transaction = frames.find(
      (frame) => frame.body.case === "transactionFragment",
    );
    if (transaction?.body.case !== "transactionFragment")
      throw new Error("missing transaction");
    expect(
      transaction.body.value.operations.map((operation) => operation.kind),
    ).toEqual([
      OperationKind.INSERT_FULL_ROW,
      OperationKind.UPDATE_PATCH,
      OperationKind.DELETE_BY_KEY,
      OperationKind.TRUNCATE_AUTHORIZED_TABLE,
    ]);
    const insert = transaction.body.value.operations[0];
    if (insert?.payload.case !== "insert") throw new Error("missing insert");
    expect(
      insert.payload.value.columns.map((column) => column.value?.kind.case),
    ).toEqual([
      "nullValue",
      "int64Value",
      "uint64Value",
      "float64Value",
      "textValue",
      "blobValue",
      "boolValue",
      "decimalText",
      "timestampUnixMicros",
      "uuid16",
      "jsonText",
      "dateIso8601",
      "timeIso8601",
      "intervalIso8601",
    ]);
    expect(
      new Set(
        frames
          .filter((frame) => frame.body.case === "error")
          .map((frame) =>
            frame.body.case === "error" ? frame.body.value.code : 0,
          ),
      ),
    ).toEqual(new Set([1, 2, 3, 4, 5, 6, 7, 8, 9]));
  });

  it("consumes Rust-origin mutation requests, all outcomes, and marker", () => {
    const requests = decodeAll(
      MutationBatchRequestSchema,
      "rust",
      "mutation-request.frame.hex",
    );
    expect(requests).toHaveLength(2);
    for (const request of requests) {
      expect(() => validateMutationBatchRequest(request)).not.toThrow();
    }
    expect(requests[1]?.mutations[0]?.createdAtUnixMicros).toBe(
      9_007_199_254_740_993n,
    );

    const [response] = decodeAll(
      MutationBatchResponseSchema,
      "rust",
      "mutation-response.frame.hex",
    );
    if (!response) throw new Error("missing response");
    expect(() => validateMutationBatchResponse(response)).not.toThrow();
    expect(new Set(response.outcomes.map((outcome) => outcome.code))).toEqual(
      new Set([1, 2, 3, 4, 5, 6, 7, 8]),
    );

    const [marker] = decodeAll(
      MutationLogicalMarkerSchema,
      "rust",
      "mutation-marker.frame.hex",
    );
    if (!marker) throw new Error("missing marker");
    // The client never receives a marker; the server validates it. Here it
    // only has to decode to what the Rust generator wrote.
    expect(marker.protocolVersion).toBe(1);
    expect(marker.mutationIds).toEqual(["mutation-1", "mutation-2"]);
    expect(marker.serverTransactionId).toEqual(
      Uint8Array.from({ length: 16 }, (_unused, index) => 80 + index),
    );
  });

  it("consumes the Rust-origin v1 key whitelist", () => {
    const key = decodePrimaryKey(fixture("rust", "primary-key.hex"));
    expect(key.parts.map((part) => part.value?.kind.case)).toEqual([
      "int64Value",
      "boolValue",
      "blobValue",
      "uuid16",
    ]);
  });

  it("consumes a Rust-origin text key and rejects one that is not UTF-8", () => {
    const key = decodePrimaryKey(fixture("rust", "primary-key-text.hex"));
    expect(key.parts[1]?.value?.kind).toEqual({
      case: "textValue",
      value: "é-\u{1d11e}",
    });
    expect(() =>
      decodePrimaryKey(
        fixture("rust", "invalid/primary-key-text-invalid-utf8.hex"),
      ),
    ).toThrow(/canonical|malformed/u);
  });

  it("rejects Rust-origin malformed, truncated, and oversized framing", () => {
    for (const [name, expectedCode] of [
      ["framing-truncated-prefix.hex", "TRUNCATED_LENGTH_PREFIX"],
      ["framing-oversized.hex", "PAYLOAD_TOO_LARGE"],
      ["framing-malformed-protobuf.hex", "MALFORMED_PROTOBUF"],
    ] as const) {
      const decoder = new FrameDecoder(ChangeRequestSchema);
      let failure: unknown;
      try {
        decoder.push(fixture("rust", `invalid/${name}`));
        decoder.finish();
      } catch (error) {
        failure = error;
      }
      expect(failure, name).toBeInstanceOf(FrameError);
      expect((failure as FrameError).code, name).toBe(expectedCode);
      expect(() => decoder.push(new Uint8Array([0])), name).toThrow(
        /cannot continue/u,
      );
    }
  });

  it("rejects Rust-origin noncanonical key, checksum, enum, oneof, and duplicate", async () => {
    expect(() =>
      decodePrimaryKey(fixture("rust", "invalid/primary-key-noncanonical.hex")),
    ).toThrow(/canonical/u);

    const [checksum] = decodeAll(
      ChangeStreamFrameSchema,
      "rust",
      "invalid/wrong-checksum.frame.hex",
    );
    if (!checksum) throw new Error("missing checksum fixture");
    expect(() => validateChangeStreamFrame(checksum)).not.toThrow();
    await expect(verifyChangeFrameBodySha256(checksum)).rejects.toThrow(
      /does not match/u,
    );

    const [unknown] = decodeAll(
      ChangeStreamFrameSchema,
      "rust",
      "invalid/unknown-enum.frame.hex",
    );
    if (!unknown) throw new Error("missing enum fixture");
    expect(() => validateChangeStreamFrame(unknown)).toThrow(/unknown/u);

    const [missing] = decodeAll(
      ChangeStreamFrameSchema,
      "rust",
      "invalid/missing-oneof.frame.hex",
    );
    if (!missing) throw new Error("missing oneof fixture");
    expect(() => validateChangeStreamFrame(missing)).toThrow(/required/u);

    const [duplicate] = decodeAll(
      ChangeRequestSchema,
      "rust",
      "invalid/duplicate-change-request.frame.hex",
    );
    if (!duplicate) throw new Error("missing duplicate fixture");
    expect(() => validateChangeRequest(duplicate)).toThrow(/unique/u);
  });

  it("rejects Rust-origin stream order and fragment gaps", async () => {
    const expectation = {
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
    };
    const [beforeStart] = decodeAll(
      ChangeStreamFrameSchema,
      "rust",
      "invalid/order-before-start.frame.hex",
    );
    if (!beforeStart) throw new Error("missing order fixture");
    await expect(
      new ChangeStreamState(expectation).accept(beforeStart),
    ).rejects.toThrow(/preceding batch start/u);

    const fragments = decodeAll(
      ChangeStreamFrameSchema,
      "rust",
      "invalid/fragment-gap.frame.hex",
    );
    expect(fragments).toHaveLength(2);
    const state = new ChangeStreamState(expectation);
    await state.accept(fragments[0] as (typeof fragments)[number]);
    await expect(
      state.accept(fragments[1] as (typeof fragments)[number]),
    ).rejects.toThrow(/fragment zero/u);
  });

  it("accepts Rust-origin exact public limits and rejects plus one", () => {
    const [exact] = decodeAll(
      ChangeRequestSchema,
      "rust",
      "invalid/exact-limit-change-request.frame.hex",
    );
    if (!exact) throw new Error("missing exact-limit fixture");
    expect(() => validateChangeRequest(exact)).not.toThrow();
    expect(exact.tables).toHaveLength(1_024);
    expect(exact.pendingMutationIds).toHaveLength(256);
    expect(exact.authorizationScopeToken).toHaveLength(8 * 1_024);
    expect(exact.clientInstanceId).toHaveLength(1_024);
    expect(exact.deviceId).toHaveLength(1_024);
    expect(exact.planId).toHaveLength(512);
    expect(exact.fromCursorExclusive?.token).toHaveLength(2_048);

    const [over] = decodeAll(
      ChangeRequestSchema,
      "rust",
      "invalid/over-limit-change-request.frame.hex",
    );
    if (!over) throw new Error("missing over-limit fixture");
    expect(() => validateChangeRequest(over)).toThrow(/8192/u);
  });
});
