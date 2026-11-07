/**
 * The page's side of the decoder worker, against a worker the test answers
 * for: what it sends, how it bounds what it holds, and that every failure
 * ends it with one error.
 */
import { describe, expect, test } from "vitest";

import { ArtifactBodyError, ArtifactDecoder } from "./columnar-decoder.js";
import {
  DECODER_INPUT_WINDOW_BYTES,
  DECODER_TAKEN_BYTES,
  type DecoderRequest,
  type DecoderResponse,
} from "./columnar-decoder-protocol.js";

const CHUNK_BYTES = 8;

class FakeWorker extends EventTarget {
  readonly requests: DecoderRequest[] = [];
  terminated = false;

  postMessage(message: DecoderRequest, transfer: Transferable[]): void {
    this.requests.push(structuredClone(message, { transfer }));
  }

  terminate(): void {
    this.terminated = true;
  }

  send(response: DecoderResponse): void {
    this.dispatchEvent(new MessageEvent("message", { data: response }));
  }
}

function decoder(
  decodedBytes = 2 * CHUNK_BYTES + 3,
  skipChunks = 0,
  packed = false,
) {
  const worker = new FakeWorker();
  const artifact = new ArtifactDecoder(() => worker, {
    decodedBytes,
    chunkBytes: CHUNK_BYTES,
    skipChunks,
    packed,
  });
  return { worker, artifact };
}

/** Whether `promise` settled within a turn of the event loop. */
async function settles(promise: Promise<unknown>): Promise<boolean> {
  let settled = false;
  const mark = (): void => {
    settled = true;
  };
  promise.then(mark, mark);
  await new Promise((resolve) => setTimeout(resolve, 0));
  return settled;
}

describe("the page's decoder", () => {
  test("starts the worker with what the body decodes to, and whether to a packed body", () => {
    for (const packed of [false, true]) {
      const { worker } = decoder(19, 1, packed);
      expect(worker.requests).toEqual([
        {
          type: "start",
          decodedBytes: 19,
          chunkBytes: CHUNK_BYTES,
          skipChunks: 1,
          packed,
        },
      ]);
    }
  });

  test("transfers the body's bytes and marks its last", () => {
    const { worker, artifact } = decoder();
    const first = new Uint8Array([1, 2, 3]).buffer;
    artifact.write(first, false);
    artifact.write(new Uint8Array([4]).buffer, true);
    expect(first.byteLength).toBe(0);
    expect(
      worker.requests
        .slice(1)
        .map((request) =>
          request.type === "bytes"
            ? [[...new Uint8Array(request.bytes)], request.end]
            : request,
        ),
    ).toEqual([
      [[1, 2, 3], false],
      [[4], true],
    ]);
  });

  test("has room until the worker holds a window of bytes it has not read", async () => {
    const { worker, artifact } = decoder();
    await artifact.room();
    artifact.write(new ArrayBuffer(DECODER_INPUT_WINDOW_BYTES - 1), false);
    await artifact.room();
    artifact.write(new ArrayBuffer(1), false);
    const room = artifact.room();
    expect(await settles(room)).toBe(false);
    worker.send({ type: "taken", bytes: DECODER_TAKEN_BYTES });
    await room;
  });

  test("returns the chunks in order, whether or not one was asked for first", async () => {
    const { worker, artifact } = decoder();
    const asked = artifact.next();
    worker.send({
      type: "chunk",
      index: 0,
      bytes: new ArrayBuffer(CHUNK_BYTES),
    });
    expect((await asked).byteLength).toBe(CHUNK_BYTES);
    worker.send({
      type: "chunk",
      index: 1,
      bytes: new ArrayBuffer(CHUNK_BYTES),
    });
    worker.send({ type: "chunk", index: 2, bytes: new ArrayBuffer(3) });
    expect((await artifact.next()).byteLength).toBe(CHUNK_BYTES);
    expect((await artifact.next()).byteLength).toBe(3);
  });

  test("expects the first chunk an earlier response did not deliver", async () => {
    const { worker, artifact } = decoder(2 * CHUNK_BYTES + 3, 2);
    worker.send({ type: "chunk", index: 2, bytes: new ArrayBuffer(3) });
    expect((await artifact.next()).byteLength).toBe(3);
  });

  test("returns a credit to the worker", () => {
    const { worker, artifact } = decoder();
    artifact.credit();
    expect(worker.requests.at(-1)).toEqual({ type: "credit" });
  });

  for (const [what, response] of [
    ["out of order", { type: "chunk", index: 1, bytes: new ArrayBuffer(8) }],
    [
      "of another length",
      { type: "chunk", index: 0, bytes: new ArrayBuffer(7) },
    ],
  ] as const)
    test(`fails on a chunk ${what}`, async () => {
      const { worker, artifact } = decoder();
      const asked = artifact.next();
      worker.send(response);
      const error = await asked.catch((reason: unknown) => reason);
      expect(error).toBeInstanceOf(Error);
      expect(error).not.toBeInstanceOf(ArtifactBodyError);
      expect((error as Error).message).toBe(
        "the decoder worker sent something other than the next chunk",
      );
      await expect(artifact.failed).rejects.toBe(error);
      expect(worker.terminated).toBe(true);
    });

  test("ends with an ArtifactBodyError when the worker refuses the body", async () => {
    const { worker, artifact } = decoder();
    artifact.write(new ArrayBuffer(DECODER_INPUT_WINDOW_BYTES), false);
    const room = artifact.room();
    const asked = artifact.next();
    worker.send({
      type: "chunk",
      index: 0,
      bytes: new ArrayBuffer(CHUNK_BYTES),
    });
    await asked;
    worker.send({
      type: "chunk",
      index: 1,
      bytes: new ArrayBuffer(CHUNK_BYTES),
    });
    worker.send({
      type: "failed",
      corrupt: true,
      message: "the body ends before its brotli stream does",
    });
    const error = await artifact.failed.catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(ArtifactBodyError);
    expect((error as Error).message).toBe(
      "the body ends before its brotli stream does",
    );
    await expect(room).rejects.toBe(error);
    // The chunk that had arrived is not returned after the refusal.
    await expect(artifact.next()).rejects.toBe(error);
    await expect(artifact.room()).rejects.toBe(error);
    expect(worker.terminated).toBe(true);
    // Nothing more is sent to a worker that is gone.
    const sent = worker.requests.length;
    artifact.write(new ArrayBuffer(1), true);
    artifact.credit();
    expect(worker.requests).toHaveLength(sent);
  });

  test("ends with a plain error when the worker fails for a reason of its own", async () => {
    for (const fail of [
      (worker: FakeWorker) => {
        worker.send({
          type: "failed",
          corrupt: false,
          message: "the decoder module did not load: HTTP 404",
        });
        return "the decoder module did not load: HTTP 404";
      },
      (worker: FakeWorker) => {
        worker.dispatchEvent(
          Object.assign(new Event("error"), { message: "worker crashed" }),
        );
        return "worker crashed";
      },
      (worker: FakeWorker) => {
        worker.dispatchEvent(new Event("error"));
        return "the decoder worker failed";
      },
      (worker: FakeWorker) => {
        worker.dispatchEvent(new MessageEvent("messageerror"));
        return "a decoder worker message could not be decoded";
      },
    ]) {
      const { worker, artifact } = decoder();
      const asked = artifact.next();
      const message = fail(worker);
      const error = await asked.catch((reason: unknown) => reason);
      expect(error).not.toBeInstanceOf(ArtifactBodyError);
      expect((error as Error).message).toBe(message);
      await expect(artifact.failed).rejects.toBe(error);
      expect(worker.terminated).toBe(true);
    }
  });

  test("closing terminates the worker and rejects what waits, without a failure", async () => {
    const { worker, artifact } = decoder();
    artifact.write(new ArrayBuffer(DECODER_INPUT_WINDOW_BYTES), false);
    const room = artifact.room();
    const asked = artifact.next();
    artifact.close();
    artifact.close();
    expect(worker.terminated).toBe(true);
    await expect(room).rejects.toThrow("the decoder is closed");
    await expect(asked).rejects.toThrow("the decoder is closed");
    await expect(artifact.next()).rejects.toThrow("the decoder is closed");
    expect(await settles(artifact.failed)).toBe(false);
    // A message that crossed the close changes nothing.
    worker.send({ type: "failed", corrupt: true, message: "late" });
    expect(await settles(artifact.failed)).toBe(false);
  });

  test("throws what the worker's constructor throws", () => {
    expect(
      () =>
        new ArtifactDecoder(
          () => {
            throw new TypeError("no Worker constructor");
          },
          { decodedBytes: 1, chunkBytes: 1, skipChunks: 0, packed: false },
        ),
    ).toThrow("no Worker constructor");
  });
});
