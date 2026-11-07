import { createHash, randomFillSync } from "node:crypto";
import {
  brotliCompressSync,
  brotliDecompressSync,
  constants as zlib,
} from "node:zlib";

import { DECODER_INPUT_WINDOW_BYTES } from "@calque/worker/columnar";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  byteStream,
  until,
  CHUNK_BYTES,
  LONG_ARTIFACT_BYTES,
  longPlan,
  patternBytes,
  stubThreads,
  servePaced,
  settled,
} from "./session.fixture.js";
import {
  FIXTURE_BYTES,
  FIXTURE_CHUNKS,
  DECODED,
  ENCODED,
  DECODER_MODULE,
  brotliPlan,
  serveBody,
  makeDecodingClient,
  joined,
  marks,
  flipped,
  refused,
} from "./session-brotli.fixture.js";

describe("a brotli-large-window artifact", () => {
  beforeEach(() => {
    stubThreads(8);
    performance.clearMarks();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("is the fixture the brotli tool wrote, a stream only a large-window decoder reads", () => {
    expect(createHash("sha256").update(ENCODED).digest("hex")).toBe(
      "19a1df47a15941ccf96fe60d86e087388c221198af5e261a1001f6d3804d89ad",
    );
    // Node's build of the same decoder reads it to the same bytes, and
    // refuses it unless told to take a large window.
    expect(
      Buffer.compare(
        brotliDecompressSync(ENCODED, {
          params: { [zlib.BROTLI_DECODER_PARAM_LARGE_WINDOW]: 1 },
        }),
        DECODED,
      ),
    ).toBe(0);
    expect(() => brotliDecompressSync(ENCODED)).toThrow();
  });

  it("loads the artifact its decoder worker decodes, in the plan's chunks", async () => {
    const { client, worker, hashers, decoders, requests } = makeDecodingClient(
      serveBody(ENCODED, 1024),
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(requests("/artifact-1")).toBe(1);
    expect(worker.received.map((bytes) => bytes.byteLength)).toEqual([
      CHUNK_BYTES,
      CHUNK_BYTES,
      CHUNK_BYTES,
      CHUNK_BYTES,
      FIXTURE_BYTES - 4 * CHUNK_BYTES,
    ]);
    expect(Buffer.compare(joined(worker.received), DECODED)).toBe(0);
    // Each chunk went through exactly one hasher, and the artifact ended.
    expect(
      hashers.flatMap(({ chunks }) => chunks).sort((a, b) => a - b),
    ).toEqual(FIXTURE_CHUNKS);
    expect(
      worker.requests.filter(({ type }) => type === "artifact-end"),
    ).toEqual([expect.objectContaining({ chunks: 5 })]);
    // One decoder worker, told what the body decodes to, given all of the
    // body, and terminated once it has returned the last chunk.
    expect(decoders).toHaveLength(1);
    expect(decoders[0]?.start).toEqual({
      type: "start",
      decodedBytes: FIXTURE_BYTES,
      chunkBytes: CHUNK_BYTES,
      skipChunks: 0,
      packed: false,
    });
    expect(decoders[0]?.bodyBytes).toBe(ENCODED.byteLength);
    expect(decoders[0]?.sent).toEqual(FIXTURE_CHUNKS);
    expect(decoders[0]?.terminated).toBe(true);
    // The worker is spawned before the artifact is requested, so it and its
    // module load while the request is out.
    const order = marks();
    expect(order).toContain("calque:decoder:spawned");
    expect(order.indexOf("calque:decoder:spawned")).toBeLessThan(
      order.indexOf("calque:artifact:request"),
    );
    expect(order).toContain("calque:artifact:downloaded");
    expect(order).toContain("calque:artifact:decoded");
    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "error" },
    ]);
    await client.close();
  });

  it.each([
    [
      "names the two brotli encodings first in the bootstrap request where the decoder worker can run",
      () => vi.stubGlobal("Worker", vi.fn()),
      ["packed-brotli-large-window", "brotli-large-window", "zstd", "identity"],
    ],
    [
      "names only zstd and identity where it cannot",
      () => vi.stubGlobal("Worker", undefined),
      ["zstd", "identity"],
    ],
  ] as const)("%s", async (_case, stub, encodings) => {
    stub();
    const { client, fetcher } = makeDecodingClient(serveBody(ENCODED));
    expect(await settled(client)).toEqual({ state: "ready" });
    const bootstrap = fetcher.mock.calls.find(([input]) =>
      String(input).endsWith("/v1/bootstrap"),
    );
    const body = JSON.parse(String(bootstrap?.[1]?.body)) as {
      acceptedEncodings?: unknown;
    };
    expect(body.acceptedEncodings).toEqual(encodings);
    await client.close();
  });

  it("starts no decoder worker for an identity or a zstd plan", async () => {
    const body = patternBytes(LONG_ARTIFACT_BYTES);
    for (const encoding of ["identity", "zstd"]) {
      const planValue = longPlan(LONG_ARTIFACT_BYTES);
      const artifact = (planValue.artifacts as Record<string, unknown>[])[0];
      Object.assign(artifact?.selectedRepresentation as object, { encoding });
      const { client, worker, decoderFactory } = makeDecodingClient(
        (inner) => async (input, init) => {
          if (!String(input).endsWith("/artifact-1")) return inner(input, init);
          // A browser has removed the zstd coding by the time the page reads
          // the body, and still reports the header.
          return new Response(byteStream(body), {
            status: 200,
            headers: {
              "content-type": "application/vnd.calque.direct-columnar.v4",
              "content-length": String(body.byteLength),
              ...(encoding === "zstd" ? { "content-encoding": "zstd" } : {}),
            },
          });
        },
        planValue,
      );
      expect(await settled(client), encoding).toEqual({ state: "ready" });
      expect(Buffer.compare(joined(worker.received), body)).toBe(0);
      expect(decoderFactory, encoding).not.toHaveBeenCalled();
      expect(marks()).not.toContain("calque:decoder:spawned");
      await client.close();
    }
  });

  it("holds the body for a decoder module that arrives after all of it", async () => {
    let arrive!: (module: WebAssembly.Module) => void;
    const module = new Promise<WebAssembly.Module>((resolve) => {
      arrive = resolve;
    });
    const { client, worker, decoders } = makeDecodingClient(
      serveBody(ENCODED, 1024),
      brotliPlan(),
      () => module,
    );
    await until(
      () => marks().includes("calque:artifact:downloaded"),
      "the downloaded mark",
    );
    await until(
      () => decoders[0]?.bodyBytes === ENCODED.byteLength,
      "the whole body to reach the decoder",
    );
    // The read ended and was marked without the decoder; nothing is decoded.
    expect(decoders[0]?.sent).toEqual([]);
    expect(client.status).toEqual({ state: "initializing" });
    arrive(DECODER_MODULE);
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(Buffer.compare(joined(worker.received), DECODED)).toBe(0);
    await client.close();
  });

  describe("is refused, with no row shown and nothing asked for again,", () => {
    const refusals: ReadonlyArray<{
      readonly name: string;
      readonly body: Uint8Array;
      readonly decodedBytes?: number;
      readonly reason: string;
    }> = [
      {
        name: "when its body is cut off halfway",
        body: ENCODED.subarray(0, ENCODED.byteLength >> 1),
        reason: "the body ends before its brotli stream does",
      },
      {
        name: "when its body lacks its last byte",
        body: ENCODED.subarray(0, ENCODED.byteLength - 1),
        reason: "the body ends before its brotli stream does",
      },
      {
        name: "when its body runs a byte past its stream",
        body: Uint8Array.of(...ENCODED, 0),
        reason: "the body continues after its brotli stream ends",
      },
      {
        name: "when one flipped bit leaves a stream the decoder cannot read",
        body: flipped(6756, 3),
        reason: "the body is not a brotli stream (decoder error -10)",
      },
      {
        name: "when it decodes to more bytes than the plan's",
        body: ENCODED,
        decodedBytes: FIXTURE_BYTES - 5,
        reason: `the body decodes to more than ${FIXTURE_BYTES - 5} bytes`,
      },
      {
        name: "when it decodes to fewer bytes than the plan's",
        body: ENCODED,
        decodedBytes: FIXTURE_BYTES + 5,
        reason: `the body decodes to ${FIXTURE_BYTES} bytes, not ${FIXTURE_BYTES + 5}`,
      },
      {
        // A 2^24 window covers four chunks; the stream declares 2^25.
        name: "when its stream declares a window larger than the artifact needs",
        body: ENCODED,
        decodedBytes: 4 * CHUNK_BYTES,
        reason:
          "the brotli stream declares a larger window than its artifact needs",
      },
    ];

    for (const { name, body, decodedBytes, reason } of refusals)
      it(name, () =>
        refused(
          body,
          1024,
          brotliPlan(decodedBytes, body.byteLength),
          "brotli-large-window",
          reason,
        ),
      );
  });

  it("leaves a flipped bit the decoder cannot see to the chunk's digest", async () => {
    // This bit changes what the stream decodes to and not that it decodes,
    // to the artifact's own length. The engine worker's comparison of each
    // chunk's digest with the plan's is what refuses it; the fake is given
    // that comparison here.
    const body = flipped(6650, 0);
    const digest = (bytes: Uint8Array): string =>
      createHash("sha256").update(bytes).digest("hex");
    const digests = FIXTURE_CHUNKS.map((index) =>
      digest(DECODED.subarray(index * CHUNK_BYTES, (index + 1) * CHUNK_BYTES)),
    );
    const { client, worker, decoders } = makeDecodingClient(serveBody(body));
    worker.verifyChunk = (index, bytes) => digest(bytes) === digests[index];
    expect(await settled(client)).toMatchObject({
      state: "error",
      error: { message: "artifact digest binding mismatch" },
    });
    expect(
      worker.requests.some((request) => request.type === "artifact-end"),
    ).toBe(false);
    expect(worker.terminated).toBe(true);
    expect(decoders.map(({ terminated }) => terminated)).toEqual([true]);
    await client.close();
  });

  it("asks again for a body the network cut, and its new decoder skips the chunks already handed over", async () => {
    // Half of the body decodes to two and a quarter chunks, so the first
    // response hands over chunks 0 and 1 and is cut inside chunk 2.
    const at = ENCODED.byteLength >> 1;
    let drop!: () => void;
    const when = new Promise<void>((resolve) => {
      drop = resolve;
    });
    const { client, worker, hashers, decoders, requests } = makeDecodingClient(
      serveBody(ENCODED, 512, (response) =>
        response === 1 ? { at, when } : undefined,
      ),
    );
    await until(
      () =>
        decoders[0]?.bodyBytes === at &&
        hashers.some(({ chunks }) => chunks.includes(1)),
      "the first response's chunks to reach a hasher",
    );
    drop();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(requests("/v1/bootstrap")).toBe(1);
    expect(requests("/artifact-1")).toBe(2);
    expect(marks()).toContain("calque:artifact:retry:1");
    // A decoder for each response, each read from the body's first byte.
    // The second sends nothing of the chunks the first response delivered.
    expect(
      decoders.map(({ start, bodyBytes, sent, terminated }) => ({
        skipChunks: start?.skipChunks,
        bodyBytes,
        sent,
        terminated,
      })),
    ).toEqual([
      { skipChunks: 0, bodyBytes: at, sent: [0, 1], terminated: true },
      {
        skipChunks: 2,
        bodyBytes: ENCODED.byteLength,
        sent: [2, 3, 4],
        terminated: true,
      },
    ]);
    // Each chunk reached a hasher once.
    expect(
      hashers.flatMap(({ chunks }) => chunks).sort((a, b) => a - b),
    ).toEqual(FIXTURE_CHUNKS);
    expect(Buffer.compare(joined(worker.received), DECODED)).toBe(0);
    expect(
      worker.requests.filter(({ type }) => type === "artifact-end"),
    ).toEqual([expect.objectContaining({ chunks: 5 })]);
    await client.close();
  });

  it("reads no further than its decoder's window while every hasher holds all it may", async () => {
    stubThreads(5);
    // Bytes brotli cannot shorten, so the body is as long as the artifact
    // and how far the page has read shows in how much it was given.
    const bytes = 8 * CHUNK_BYTES + 3;
    const artifact = randomFillSync(new Uint8Array(bytes));
    const body = new Uint8Array(
      brotliCompressSync(artifact, {
        params: {
          [zlib.BROTLI_PARAM_QUALITY]: 0,
          [zlib.BROTLI_PARAM_LARGE_WINDOW]: 1,
          [zlib.BROTLI_PARAM_LGWIN]: 26,
          [zlib.BROTLI_PARAM_SIZE_HINT]: bytes,
        },
      }),
    );
    const given = { bytes: 0 };
    const { client, worker, hashers, decoders } = makeDecodingClient(
      servePaced(body, given),
      brotliPlan(bytes, body.byteLength),
    );
    worker.holdCredits = true;
    await until(
      () => decoders[0]?.sent.length === 3,
      "the third chunk to leave the decoder",
    );
    // Time for a page that did not wait to read on.
    await new Promise((resolve) => setTimeout(resolve, 100));
    // The one hasher holds two chunks and the page a third, which waits for
    // room. The decoder has filled a fourth and may not send it.
    expect(hashers.map(({ chunks }) => chunks)).toEqual([[0, 1]]);
    expect(decoders[0]?.sent).toEqual([0, 1, 2]);
    // So the decoder has taken in four chunks of the body, and the page has
    // read its window past that: under six of the body's eight chunks.
    expect(given.bytes).toBeGreaterThanOrEqual(
      4 * CHUNK_BYTES + DECODER_INPUT_WINDOW_BYTES,
    );
    expect(given.bytes).toBeLessThan(6 * CHUNK_BYTES);
    expect(client.status.state).toBe("initializing");
    worker.releaseCredits();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(given.bytes).toBe(body.byteLength);
    expect(Buffer.compare(joined(worker.received), artifact)).toBe(0);
    await client.close();
  });

  it("ends the load with the decoder's own failure when its module does not load", async () => {
    const { client, worker, decoders } = makeDecodingClient(
      serveBody(ENCODED),
      brotliPlan(),
      async () => {
        throw new Error("the module request answered HTTP 404");
      },
    );
    // Not the body's fault, so not the error a wrong body gets.
    expect(await settled(client)).toMatchObject({
      state: "error",
      error: {
        code: "INTERNAL_ERROR",
        message:
          "the decoder module did not load: the module request answered HTTP 404",
      },
    });
    expect(
      worker.requests.some((request) => request.type === "artifact-end"),
    ).toBe(false);
    expect(worker.terminated).toBe(true);
    expect(decoders.map(({ terminated }) => terminated)).toEqual([true]);
    await client.close();
  });

  it("terminates a decoder still waiting when the client closes", async () => {
    const { client, decoders } = makeDecodingClient(
      serveBody(ENCODED),
      brotliPlan(),
      () => new Promise(() => undefined),
    );
    await until(
      () => decoders[0]?.bodyBytes === ENCODED.byteLength,
      "the whole body to reach the decoder",
    );
    expect(decoders[0]?.terminated).toBe(false);
    await client.close();
    expect(decoders.map(({ terminated }) => terminated)).toEqual([true]);
  });
});
