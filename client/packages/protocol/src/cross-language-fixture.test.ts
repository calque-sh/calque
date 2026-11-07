import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { ChangeStreamFrameSchema } from "./gen/change_stream_pb.js";
import { FrameDecoder } from "./framing.js";
import { ChangeStreamState } from "./stream-state.js";

const SOURCE_WIDE_AUTHORIZATION_SCOPE_SHA256 = Uint8Array.from(
  "f9154db512dfe802aa41f358909f833e798c077f0bfa6a2af71c91ab2797cf36".match(
    /.{2}/gu,
  ) ?? [],
  (pair) => Number.parseInt(pair, 16),
);

function fixtureBytes(): Uint8Array {
  const hex = readFileSync(
    new URL(
      "../../../../spec/fixtures/change-batch.frame.hex",
      import.meta.url,
    ),
    "utf8",
  ).trim();
  if (hex.length % 2 !== 0 || !/^[0-9a-f]+$/u.test(hex)) {
    throw new Error(
      "cross-language change fixture is not canonical lowercase hex",
    );
  }
  return Uint8Array.from(hex.match(/.{2}/gu) ?? [], (pair) =>
    Number.parseInt(pair, 16),
  );
}

describe("Rust change batch fixture", () => {
  it("passes TypeScript framing, integrity, generation, and stream-state validation", async () => {
    const decoder = new FrameDecoder(ChangeStreamFrameSchema);
    const frames = decoder.push(fixtureBytes());
    decoder.finish();
    expect(frames).toHaveLength(3);

    const first = frames[0];
    if (first?.body.case !== "batchStart") {
      throw new Error("fixture must start with BatchStart");
    }
    const start = first.body.value;
    expect(start.generation).toMatchObject({
      sourceEpoch: "epoch-1",
      publicationGeneration: 2n,
      schemaGeneration: 3n,
      authorizationGeneration: 0n,
    });
    expect(start.generation?.authorizationScopeHash).toEqual(
      SOURCE_WIDE_AUTHORIZATION_SCOPE_SHA256,
    );
    expect(start.tableIds).toEqual(["public.tasks", "public.users"]);
    expect(start.planId).toBe("plan-1");
    expect(start.tableStarts.map((table) => table.tableId)).toEqual([
      "public.tasks",
      "public.users",
    ]);
    if (
      start.generation === undefined ||
      start.fromCursorExclusive === undefined ||
      start.requestedFence === undefined
    ) {
      throw new Error("fixture BatchStart is incomplete");
    }

    const stream = new ChangeStreamState({
      planId: "plan-1",
      fromCursorExclusive: start.fromCursorExclusive.token,
      requestedFence: start.requestedFence.token,
      tableStarts: start.tableStarts.map((table) =>
        table.shardSnapshotCursor
          ? {
              tableId: table.tableId,
              snapshotCursor: table.shardSnapshotCursor.token,
            }
          : { tableId: table.tableId },
      ),
      generation: start.generation,
    });
    let completed;
    for (const frame of frames)
      completed = (await stream.accept(frame)) ?? completed;

    expect(completed).toMatchObject({
      reachedRequestedFence: true,
      relevantTransactionCount: 1n,
      relevantOperationCount: 1n,
    });
    const transaction = frames[1];
    if (transaction?.body.case !== "transactionFragment") {
      throw new Error("fixture must contain one transaction fragment");
    }
    expect(transaction.body.value.operations).toMatchObject([
      {
        tableId: "public.users",
        payload: { case: "truncate", value: { reason: "x" } },
      },
    ]);
    expect(stream.terminal).toBe(true);
  });
});
