/**
 * A hasher worker, driven through the scope a `DedicatedWorker` gives it,
 * with its port to the engine worker a real `MessageChannel`.
 */
import { createHash } from "node:crypto";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import type {
  HasherFailure,
  HasherRequest,
} from "./columnar-hasher-protocol.js";
import type { WorkerArtifactChunk } from "./columnar-worker-protocol.js";

interface Scope {
  onmessage: ((event: { data: HasherRequest }) => void) | null;
  postMessage(message: HasherFailure): void;
}

const scope = globalThis as unknown as Scope;
let failures: HasherFailure[] = [];
let engine: MessageChannel;
let received: WorkerArtifactChunk[] = [];

function send(request: HasherRequest): void {
  scope.onmessage?.({ data: request });
}

function sha256(bytes: Uint8Array): string {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

describe("a hasher worker", () => {
  beforeEach(async () => {
    failures = [];
    received = [];
    scope.postMessage = (message) => failures.push(message);
    engine = new MessageChannel();
    engine.port2.onmessage = (event: MessageEvent<WorkerArtifactChunk>) => {
      received.push(event.data);
    };
    performance.clearMarks();
    vi.resetModules();
    await import("./columnar-hasher-entry.js");
  });

  afterEach(() => {
    engine.port1.close();
    engine.port2.close();
  });

  test("passes each chunk on in the buffer it was sent, with the SHA-256 of exactly those bytes", async () => {
    expect(
      performance.getEntriesByName("calque:hasher:evaluated"),
    ).toHaveLength(1);
    send({ type: "port", port: engine.port1 });
    const chunks = [
      Uint8Array.from({ length: 300 }, (_, index) => index % 251),
      Uint8Array.from({ length: 17 }, (_, index) => 255 - index),
    ];
    const buffers = chunks.map((chunk) => chunk.slice().buffer);
    for (const [index, bytes] of buffers.entries())
      send({ type: "chunk", index: index + 5, bytes });
    await vi.waitFor(() => {
      expect(received).toHaveLength(2);
    });
    // In the order their digests finish, which need not be the order sent.
    expect(
      received
        .map(({ index, bytes, sha256: digest }) => ({
          index,
          bytes: new Uint8Array(bytes),
          digest,
        }))
        .sort((a, b) => a.index - b.index),
    ).toEqual(
      chunks.map((chunk, index) => ({
        index: index + 5,
        bytes: chunk,
        digest: sha256(chunk),
      })),
    );
    // Transferred on, not copied: the hasher holds none of it afterwards.
    expect(buffers.map((buffer) => buffer.byteLength)).toEqual([0, 0]);
    expect(failures).toEqual([]);
    expect(
      performance.getEntriesByName("calque:hasher:first-chunk"),
    ).toHaveLength(1);
  });

  test("tells the page when it cannot take a digest, and passes nothing on", async () => {
    send({ type: "port", port: engine.port1 });
    const digest = vi
      .spyOn(crypto.subtle, "digest")
      .mockRejectedValueOnce(new Error("digest unavailable"));
    try {
      send({ type: "chunk", index: 3, bytes: new ArrayBuffer(8) });
      await vi.waitFor(() => {
        expect(failures).toEqual([
          { type: "failed", index: 3, message: "digest unavailable" },
        ]);
      });
    } finally {
      digest.mockRestore();
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(received).toEqual([]);
  });

  test("tells the page about a chunk that arrives before its port", async () => {
    send({ type: "chunk", index: 0, bytes: new ArrayBuffer(8) });
    expect(failures).toEqual([
      {
        type: "failed",
        index: 0,
        message: "the hasher has no port to the engine worker",
      },
    ]);
  });
});
