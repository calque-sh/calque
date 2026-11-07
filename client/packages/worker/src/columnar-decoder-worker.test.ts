/**
 * The decoder worker, driven through the scope a `DedicatedWorker` gives it,
 * with the committed module and bodies node's own brotli encoder made. Every
 * message crosses as a worker's does: cloned, its buffers transferred.
 *
 * The packed bodies are the packer's, from `spec/fixtures/packed-artifact`.
 * What the unpacker makes of each kind of body is its own test's; here it is
 * the worker's part: the chunks it sends and when, and a refusal's message.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { brotliCompressSync, constants } from "node:zlib";
import { describe, expect, test } from "vitest";

import {
  DECODER_TAKEN_BYTES,
  type DecoderRequest,
  type DecoderResponse,
} from "./columnar-decoder-protocol.js";
import {
  decoderWindowBytes,
  serveArtifactDecoder,
  type DecoderScope,
} from "./columnar-decoder-worker.js";

const MODULE = new WebAssembly.Module(
  readFileSync(new URL("../wasm/brotli-decoder.wasm", import.meta.url)),
);

/** Small, so an artifact of a few hundred kilobytes is several chunks. */
const CHUNK_BYTES = 64 * 1024;

/**
 * Bytes brotli shrinks a little: a pattern with a pseudo-random byte in
 * every eighth place, so a stream of them is long enough to cut and flip.
 */
function artifactBytes(length: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(length);
  let state = 0x9e3779b9;
  for (let index = 0; index < length; index += 1) {
    if (index % 8 !== 0) bytes[index] = (index * 31) % 251;
    else {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      bytes[index] = state >>> 24;
    }
  }
  return bytes;
}

/** Bytes brotli cannot shrink. */
function noise(length: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(length);
  let state = 0x2545f491;
  for (let index = 0; index < length; index += 1) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    bytes[index] = state & 0xff;
  }
  return bytes;
}

/** `bytes` as a large-window brotli stream with a window of `2 ** bits`. */
function encode(bytes: Uint8Array, bits?: number): Uint8Array<ArrayBuffer> {
  return new Uint8Array(
    brotliCompressSync(bytes, {
      params: {
        [constants.BROTLI_PARAM_LARGE_WINDOW]: 1,
        [constants.BROTLI_PARAM_LGWIN]:
          bits ?? Math.log2(decoderWindowBytes(bytes.byteLength)),
        [constants.BROTLI_PARAM_QUALITY]: 9,
      },
    }),
  );
}

interface Served {
  readonly responses: DecoderResponse[];
  send(request: DecoderRequest): void;
  /** The decoded chunks sent so far, by index. */
  chunks(): Map<number, Uint8Array>;
  failure(): Extract<DecoderResponse, { type: "failed" }> | undefined;
}

/** A decoder worker on a scope of its own. */
function serve(
  instantiate: () => Promise<WebAssembly.Instance> = () =>
    Promise.resolve(new WebAssembly.Instance(MODULE, {})),
): Served {
  const responses: DecoderResponse[] = [];
  const scope: DecoderScope = {
    onmessage: null,
    postMessage(message, transfer) {
      responses.push(structuredClone(message, { transfer }));
    },
  };
  serveArtifactDecoder(scope, instantiate);
  return {
    responses,
    send(request) {
      const transfer = request.type === "bytes" ? [request.bytes] : [];
      scope.onmessage?.({
        data: structuredClone(request, { transfer }),
      } as MessageEvent<DecoderRequest>);
    },
    chunks: () =>
      new Map(
        responses.flatMap((response) =>
          response.type === "chunk"
            ? [[response.index, new Uint8Array(response.bytes)] as const]
            : [],
        ),
      ),
    failure: () =>
      responses.find((response) => response.type === "failed") as
        Extract<DecoderResponse, { type: "failed" }> | undefined,
  };
}

/** Lets the module's promise, and what waits on it, settle. */
async function settle(): Promise<void> {
  for (let turn = 0; turn < 5; turn += 1)
    await new Promise((resolve) => setImmediate(resolve));
}

/** Sends `body` in pieces of `pieceBytes`, the last flagged as the end. */
function sendBody(served: Served, body: Uint8Array, pieceBytes: number): void {
  for (let at = 0; at < body.byteLength; at += pieceBytes) {
    const piece = body.slice(at, at + pieceBytes);
    served.send({
      type: "bytes",
      bytes: piece.buffer,
      end: at + pieceBytes >= body.byteLength,
    });
  }
}

/**
 * Decodes `body` as an artifact of `decodedBytes`, crediting every chunk as
 * it arrives, and returns what the worker sent.
 */
async function decodeAll(
  body: Uint8Array,
  decodedBytes: number,
  pieceBytes = 4096,
  skipChunks = 0,
  packed = false,
  chunkBytes = CHUNK_BYTES,
): Promise<Served> {
  const served = serve();
  served.send({
    type: "start",
    decodedBytes,
    chunkBytes,
    skipChunks,
    packed,
  });
  await settle();
  sendBody(served, body, pieceBytes);
  // One credit for each chunk it could send, whether or not it does.
  for (let credit = 0; credit < Math.ceil(decodedBytes / chunkBytes); credit++)
    served.send({ type: "credit" });
  return served;
}

/**
 * What the worker makes of `body` when the response goes on after it: the
 * whole of it in one piece and then a byte in another, with a credit for
 * every chunk.
 */
async function decodeFollowed(
  body: Uint8Array,
  decodedBytes: number,
  packed: boolean,
): Promise<Served> {
  const served = serve();
  served.send({
    type: "start",
    decodedBytes,
    chunkBytes: CHUNK_BYTES,
    skipChunks: 0,
    packed,
  });
  await settle();
  served.send({ type: "bytes", bytes: body.slice().buffer, end: false });
  served.send({ type: "bytes", bytes: new ArrayBuffer(1), end: true });
  for (let credit = 0; credit < Math.ceil(decodedBytes / CHUNK_BYTES); credit++)
    served.send({ type: "credit" });
  return served;
}

function joined(chunks: Map<number, Uint8Array>): Uint8Array {
  const ordered = [...chunks.entries()].sort(([a], [b]) => a - b);
  const bytes = new Uint8Array(
    ordered.reduce((total, [, chunk]) => total + chunk.byteLength, 0),
  );
  let offset = 0;
  for (const [, chunk] of ordered) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

describe("the decoder worker", () => {
  const LENGTH = 4 * CHUNK_BYTES + 4099;
  const artifact = artifactBytes(LENGTH);
  const body = encode(artifact);

  test("sends the artifact in chunks of exactly chunkBytes, the last one shorter, whatever sizes the body arrives in", async () => {
    for (const pieceBytes of [1, 7, 4096, body.byteLength]) {
      const served = await decodeAll(body, LENGTH, pieceBytes);
      expect(served.failure(), String(pieceBytes)).toBeUndefined();
      const chunks = served.chunks();
      expect([...chunks.keys()]).toEqual([0, 1, 2, 3, 4]);
      expect([...chunks.values()].map(({ byteLength }) => byteLength)).toEqual([
        CHUNK_BYTES,
        CHUNK_BYTES,
        CHUNK_BYTES,
        CHUNK_BYTES,
        4099,
      ]);
      expect(Buffer.compare(joined(chunks), artifact)).toBe(0);
    }
  });

  test("sends an artifact that is a whole number of chunks, and one shorter than a chunk", async () => {
    for (const length of [2 * CHUNK_BYTES, 16]) {
      const bytes = artifactBytes(length);
      const served = await decodeAll(encode(bytes), length);
      expect(served.failure()).toBeUndefined();
      expect(Buffer.compare(joined(served.chunks()), bytes)).toBe(0);
      expect(served.chunks().size).toBe(Math.ceil(length / CHUNK_BYTES));
    }
  });

  test("sends one chunk for each credit, and starts with one", async () => {
    const served = serve();
    served.send({
      type: "start",
      decodedBytes: LENGTH,
      chunkBytes: CHUNK_BYTES,
      skipChunks: 0,
      packed: false,
    });
    await settle();
    sendBody(served, body, 4096);
    // The whole body is with it, and it has sent what its one credit allows.
    expect([...served.chunks().keys()]).toEqual([0]);
    served.send({ type: "credit" });
    expect([...served.chunks().keys()]).toEqual([0, 1]);
    served.send({ type: "credit" });
    served.send({ type: "credit" });
    expect([...served.chunks().keys()]).toEqual([0, 1, 2, 3]);
    // The last chunk waits for a credit like any other.
    served.send({ type: "credit" });
    expect([...served.chunks().keys()]).toEqual([0, 1, 2, 3, 4]);
    expect(Buffer.compare(joined(served.chunks()), artifact)).toBe(0);
  });

  test("keeps the bytes that arrive before its module, in order, and decodes them once it has loaded", async () => {
    let load!: (instance: WebAssembly.Instance) => void;
    const served = serve(
      () =>
        new Promise((resolve) => {
          load = resolve;
        }),
    );
    served.send({
      type: "start",
      decodedBytes: LENGTH,
      chunkBytes: CHUNK_BYTES,
      skipChunks: 0,
      packed: false,
    });
    sendBody(served, body, 1000);
    for (let credit = 0; credit < 5; credit += 1)
      served.send({ type: "credit" });
    await settle();
    expect(served.responses).toEqual([]);
    load(new WebAssembly.Instance(MODULE, {}));
    await settle();
    expect(served.failure()).toBeUndefined();
    expect(Buffer.compare(joined(served.chunks()), artifact)).toBe(0);
  });

  test("decodes the chunks an earlier response delivered and sends none of them", async () => {
    const served = await decodeAll(body, LENGTH, 4096, 3);
    expect(served.failure()).toBeUndefined();
    expect([...served.chunks().keys()]).toEqual([3, 4]);
    expect(
      Buffer.compare(
        joined(served.chunks()),
        artifact.subarray(3 * CHUNK_BYTES),
      ),
    ).toBe(0);
    // With every chunk delivered already, it reads the body and sends nothing.
    const none = await decodeAll(body, LENGTH, 4096, 5);
    expect(none.responses).toEqual([]);
  });

  test("says how much of the body it has read, DECODER_TAKEN_BYTES at a time", async () => {
    const length = 2.5 * DECODER_TAKEN_BYTES;
    const bytes = noise(length);
    const incompressible = encode(bytes);
    expect(incompressible.byteLength).toBeGreaterThan(length);
    const served = serve();
    served.send({
      type: "start",
      decodedBytes: length,
      chunkBytes: 4 * 1024 * 1024,
      skipChunks: 0,
      packed: false,
    });
    await settle();
    sendBody(served, incompressible, 100_000);
    const taken = served.responses.flatMap((response) =>
      response.type === "taken" ? [response.bytes] : [],
    );
    expect(taken).toHaveLength(2);
    for (const count of taken) {
      expect(count).toBeGreaterThanOrEqual(DECODER_TAKEN_BYTES);
      expect(count).toBeLessThan(DECODER_TAKEN_BYTES + 262_144);
    }
    expect(Buffer.compare(joined(served.chunks()), bytes)).toBe(0);
  });

  describe("refuses", () => {
    /** The refusal of `refused` as an artifact of `decodedBytes`. */
    async function refusal(
      refused: Uint8Array,
      decodedBytes = LENGTH,
    ): Promise<string> {
      const served = await decodeAll(refused, decodedBytes);
      const failure = served.failure();
      expect(failure).toMatchObject({ type: "failed", corrupt: true });
      // Nothing follows a refusal, and the last chunk never went.
      expect(served.responses.at(-1)).toBe(failure);
      expect(
        served.chunks().has(Math.ceil(decodedBytes / CHUNK_BYTES) - 1),
      ).toBe(false);
      return failure?.message ?? "";
    }

    test("a body that ends before its stream does", async () => {
      for (const length of [1, 10, body.byteLength >> 1, body.byteLength - 1])
        expect(await refusal(body.subarray(0, length)), String(length)).toBe(
          "the body ends before its brotli stream does",
        );
    });

    test("a body that continues after its stream ends", async () => {
      for (const extra of [1, 4, 5000]) {
        const longer = new Uint8Array(body.byteLength + extra);
        longer.set(body);
        expect(await refusal(longer), String(extra)).toBe(
          "the body continues after its brotli stream ends",
        );
      }
    });

    test("a body that goes on in another piece after the one its stream ended with", async () => {
      // The decoder is never given the byte, so it does not see it.
      const served = await decodeFollowed(body, LENGTH, false);
      expect(served.failure()).toEqual({
        type: "failed",
        corrupt: true,
        message: "the body continues after its brotli stream ends",
      });
      expect([...served.chunks().keys()]).toEqual([0, 1, 2, 3]);
    });

    test("a body that decodes to more bytes than the artifact", async () => {
      // One byte more, and a whole chunk and more.
      expect(await refusal(body, LENGTH - 1)).toBe(
        `the body decodes to more than ${LENGTH - 1} bytes`,
      );
      expect(await refusal(body, LENGTH - 4099)).toBe(
        `the body decodes to more than ${LENGTH - 4099} bytes`,
      );
      expect(await refusal(body, 3 * CHUNK_BYTES)).toBe(
        `the body decodes to more than ${3 * CHUNK_BYTES} bytes`,
      );
    });

    test("a body that decodes to fewer bytes than the artifact", async () => {
      expect(await refusal(body, LENGTH + 1)).toBe(
        `the body decodes to ${LENGTH} bytes, not ${LENGTH + 1}`,
      );
      expect(await refusal(body, LENGTH + 2 * CHUNK_BYTES)).toBe(
        `the body decodes to ${LENGTH} bytes, not ${LENGTH + 2 * CHUNK_BYTES}`,
      );
    });

    test("a body that is not a brotli stream", async () => {
      expect(await refusal(noise(5000))).toMatch(
        /^the body is not a brotli stream \(decoder error -\d+\)$/u,
      );
    });

    test("a stream that declares a larger window than its artifact needs, without allocating it", async () => {
      // 16 bytes need the smallest window; this stream declares 1 GiB.
      const small = artifactBytes(16);
      let instance: WebAssembly.Instance | undefined;
      const served = serve(() => {
        instance = new WebAssembly.Instance(MODULE, {});
        return Promise.resolve(instance);
      });
      served.send({
        type: "start",
        decodedBytes: 16,
        chunkBytes: CHUNK_BYTES,
        skipChunks: 0,
        packed: false,
      });
      await settle();
      sendBody(served, encode(small, 30), 4096);
      expect(served.responses).toEqual([
        {
          type: "failed",
          corrupt: true,
          message:
            "the brotli stream declares a larger window than its artifact needs",
        },
      ]);
      const memory = instance?.exports.memory as WebAssembly.Memory;
      expect(memory.buffer.byteLength).toBeLessThan(16 * 1024 * 1024);
    });
  });

  test("never sends the last chunk of a body one flipped bit kept from decoding to the artifact's length", async () => {
    const length = CHUNK_BYTES + 4099;
    const bytes = artifactBytes(length);
    const stream = encode(bytes);
    let refused = 0;
    let complete = 0;
    for (let bit = 0; bit < stream.byteLength * 8; bit += 101) {
      const flipped = stream.slice();
      flipped[bit >> 3] = (flipped[bit >> 3] ?? 0) ^ (1 << (bit & 7));
      const served = await decodeAll(flipped, length, stream.byteLength);
      const failure = served.failure();
      if (failure) {
        refused += 1;
        expect(failure.corrupt, String(bit)).toBe(true);
        expect(served.chunks().has(1), String(bit)).toBe(false);
      } else {
        // The stream still ends where the body does, on an artifact of the
        // same length. Whether its bytes are the artifact's is for the
        // chunk's digest, which the engine worker compares with the plan's.
        complete += 1;
        expect(joined(served.chunks()).byteLength, String(bit)).toBe(length);
      }
    }
    expect(refused).toBeGreaterThan(0);
    expect(complete).toBeGreaterThan(0);
  });

  test("reports a module that did not load as its own failure, not the body's", async () => {
    const served = serve(() => Promise.reject(new Error("HTTP 404")));
    served.send({
      type: "start",
      decodedBytes: LENGTH,
      chunkBytes: CHUNK_BYTES,
      skipChunks: 0,
      packed: false,
    });
    sendBody(served, body, 4096);
    await settle();
    expect(served.responses).toEqual([
      {
        type: "failed",
        corrupt: false,
        message: "the decoder module did not load: HTTP 404",
      },
    ]);
  });
});

describe("the decoder worker, told its body is packed", () => {
  const FIXTURES = new URL(
    "../../../../spec/fixtures/packed-artifact/",
    import.meta.url,
  );
  const fixture = (
    name: string,
    file: "artifact.bin" | "packed.bin",
  ): Uint8Array<ArrayBuffer> =>
    new Uint8Array(readFileSync(new URL(`${name}/${file}`, FIXTURES)));

  /** Small, so the fixture's 72,714 bytes are 18 chunks. */
  const SMALL_BYTES = 4096;
  const artifact = fixture("every-kind", "artifact.bin");
  const packed = fixture("every-kind", "packed.bin");
  const LENGTH = artifact.byteLength;
  const CHUNKS = Math.ceil(LENGTH / SMALL_BYTES);
  const body = encode(packed);

  test("sends the artifact each of the packer's bodies stands for, whatever sizes the body arrives in", async () => {
    for (const name of [
      "emit-fixture",
      "emit-key-fixture",
      "every-kind",
      "stored",
      "tables-smallest-first",
    ]) {
      const expected = fixture(name, "artifact.bin");
      const encoded = encode(fixture(name, "packed.bin"));
      for (const [pieceBytes, chunkBytes] of [
        [1, CHUNK_BYTES],
        [7, SMALL_BYTES],
        [4096, 512],
        [encoded.byteLength, CHUNK_BYTES],
      ] as const) {
        const served = await decodeAll(
          encoded,
          expected.byteLength,
          pieceBytes,
          0,
          true,
          chunkBytes,
        );
        const what = `${name} ${pieceBytes} ${chunkBytes}`;
        expect(served.failure(), what).toBeUndefined();
        const chunks = served.chunks();
        const count = Math.ceil(expected.byteLength / chunkBytes);
        // In order, and each of chunkBytes but the last.
        expect([...chunks.keys()], what).toEqual(
          Array.from({ length: count }, (_unused, index) => index),
        );
        expect(
          [...chunks.values()].every(
            ({ byteLength }, index) =>
              byteLength ===
              Math.min(chunkBytes, expected.byteLength - index * chunkBytes),
          ),
          what,
        ).toBe(true);
        expect(Buffer.compare(joined(chunks), expected), what).toBe(0);
      }
    }
  });

  test("sends one chunk for each credit, and starts with one", async () => {
    const served = serve();
    served.send({
      type: "start",
      decodedBytes: LENGTH,
      chunkBytes: SMALL_BYTES,
      skipChunks: 0,
      packed: true,
    });
    await settle();
    sendBody(served, body, 4096);
    expect([...served.chunks().keys()]).toEqual([0]);
    for (let sent = 2; sent <= CHUNKS; sent += 1) {
      served.send({ type: "credit" });
      expect(served.chunks().size).toBe(sent);
    }
    expect(served.failure()).toBeUndefined();
    expect(Buffer.compare(joined(served.chunks()), artifact)).toBe(0);
  });

  test("unpacks the chunks an earlier response delivered and sends none of them", async () => {
    const served = await decodeAll(body, LENGTH, 1000, 5, true, SMALL_BYTES);
    expect(served.failure()).toBeUndefined();
    expect([...served.chunks().keys()]).toEqual(
      Array.from({ length: CHUNKS - 5 }, (_unused, index) => index + 5),
    );
    expect(
      Buffer.compare(
        joined(served.chunks()),
        artifact.subarray(5 * SMALL_BYTES),
      ),
    ).toBe(0);
    const none = await decodeAll(body, LENGTH, 1000, CHUNKS, true, SMALL_BYTES);
    expect(none.responses).toEqual([]);
  });

  test("leaves a packed body as it is when it is not told the body is packed", async () => {
    const served = await decodeAll(body, packed.byteLength);
    expect(served.failure()).toBeUndefined();
    expect(Buffer.compare(joined(served.chunks()), packed)).toBe(0);
  });

  describe("refuses, as a corrupt body and before its last chunk,", () => {
    /** `packed` with the bytes from `at` replaced by `bytes`. */
    const changed = (at: number, ...bytes: number[]): Uint8Array => {
      const copy = packed.slice();
      copy.set(bytes, at < 0 ? copy.byteLength + at : at);
      return copy;
    };
    /** `bytes` with `extra` zero bytes after them. */
    const followed = (bytes: Uint8Array, extra: number): Uint8Array => {
      const longer = new Uint8Array(bytes.byteLength + extra);
      longer.set(bytes);
      return longer;
    };
    /** The first place `text` is found in the preamble. */
    const within = (text: string): number =>
      Buffer.from(packed).indexOf(text, 12, "latin1");

    const refusals: readonly (readonly [
      name: string,
      body: Uint8Array,
      decodedBytes: number,
      message: string,
    ])[] = [
      [
        "a body that begins with neither magic",
        changed(4, 0x51),
        LENGTH,
        "the body begins with neither a packed nor a stored artifact's magic",
      ],
      [
        "a preamble it cannot read",
        changed(12, 0x5b),
        LENGTH,
        "the body's preamble cannot be read",
      ],
      [
        "a preamble longer than its artifact",
        changed(8, 0xff, 0xff, 0x1f, 0x00),
        LENGTH,
        "the body is longer than its artifact",
      ],
      [
        "a body of an artifact a byte shorter than the plan's",
        packed,
        LENGTH + 1,
        `the body restores ${LENGTH} bytes, not ${LENGTH + 1}`,
      ],
      [
        "a body of an artifact a byte longer than the plan's",
        packed,
        LENGTH - 1,
        `the body restores ${LENGTH} bytes, not ${LENGTH - 1}`,
      ],
      [
        // items.owner_id is in rows of 300 owners, which take two bytes.
        "a column whose stream is not its transform's",
        changed(within('"width":2'), ...Buffer.from('"width":3')),
        LENGTH,
        "the body's column 1 of table 0 has the defect Width",
      ],
      [
        "a body that ends inside its preamble",
        packed.subarray(0, 100),
        LENGTH,
        "the body ends before its last part",
      ],
      [
        "a body that lacks its last byte",
        packed.subarray(0, packed.byteLength - 1),
        LENGTH,
        "the body ends before its last part",
      ],
      [
        "a body with a byte after its last part",
        followed(packed, 1),
        LENGTH,
        "bytes follow the body's last part",
      ],
      [
        "a body with as many bytes after its last part as the artifact has",
        followed(packed, LENGTH),
        LENGTH,
        "bytes follow the body's last part",
      ],
    ];

    test.each(refusals)("%s", async (_name, refused, decodedBytes, message) => {
      for (const pieceBytes of [1, 4096]) {
        const served = await decodeAll(
          encode(refused),
          decodedBytes,
          pieceBytes,
          0,
          true,
          SMALL_BYTES,
        );
        expect(served.failure()).toEqual({
          type: "failed",
          corrupt: true,
          message,
        });
        // Nothing follows a refusal, and the last chunk never went.
        expect(served.responses.at(-1)).toBe(served.failure());
        expect(
          served.chunks().has(Math.ceil(decodedBytes / SMALL_BYTES) - 1),
        ).toBe(false);
      }
    });

    test("a body whose last byte is wrong, having sent every chunk before the last", async () => {
      // The body ends with zones.label's last separator.
      expect(packed.at(-1)).toBe(0xff);
      const served = await decodeAll(
        encode(changed(-1, 0x78)),
        LENGTH,
        4096,
        0,
        true,
        SMALL_BYTES,
      );
      expect(served.failure()).toEqual({
        type: "failed",
        corrupt: true,
        message: "the body's column 1 of table 6 has the defect PartLength",
      });
      expect(served.responses.at(-1)).toBe(served.failure());
      // The label's values are in the last chunk alone.
      expect([...served.chunks().keys()]).toEqual(
        Array.from({ length: CHUNKS - 1 }, (_unused, index) => index),
      );
      expect(
        Buffer.compare(
          joined(served.chunks()),
          artifact.subarray(0, (CHUNKS - 1) * SMALL_BYTES),
        ),
      ).toBe(0);
    });

    test("a brotli stream that ends short of the body's, or runs past it", async () => {
      const cut = await decodeAll(
        body.subarray(0, body.byteLength - 1),
        LENGTH,
        4096,
        0,
        true,
        SMALL_BYTES,
      );
      expect(cut.failure()).toEqual({
        type: "failed",
        corrupt: true,
        message: "the body ends before its brotli stream does",
      });
      expect(cut.chunks().has(CHUNKS - 1)).toBe(false);
      const longer = await decodeAll(
        followed(body, 1),
        LENGTH,
        4096,
        0,
        true,
        SMALL_BYTES,
      );
      expect(longer.failure()).toEqual({
        type: "failed",
        corrupt: true,
        message: "the body continues after its brotli stream ends",
      });
      expect(longer.chunks().has(CHUNKS - 1)).toBe(false);
    });
  });

  test("refuses a body that goes on in another piece after the one its stream ended with", async () => {
    const served = await decodeFollowed(body, LENGTH, true);
    expect(served.failure()).toEqual({
      type: "failed",
      corrupt: true,
      message: "the body continues after its brotli stream ends",
    });
    // Two chunks of 65,536 bytes, and the first went.
    expect([...served.chunks().keys()]).toEqual([0]);
  });

  test("never sends the last chunk of a body one flipped bit kept from unpacking, and never the artifact from another body", async () => {
    let refused = 0;
    let complete = 0;
    for (let bit = 0; bit < packed.byteLength * 8; bit += 1783) {
      const flipped = packed.slice();
      flipped[bit >> 3] = (flipped[bit >> 3] ?? 0) ^ (1 << (bit & 7));
      const served = await decodeAll(
        encode(flipped),
        LENGTH,
        16_384,
        0,
        true,
        SMALL_BYTES,
      );
      const failure = served.failure();
      if (failure) {
        refused += 1;
        expect(failure.corrupt, String(bit)).toBe(true);
        expect(served.chunks().has(CHUNKS - 1), String(bit)).toBe(false);
      } else {
        // The body still unpacks, to an artifact of the same length and
        // other bytes, which a chunk's digest refuses.
        complete += 1;
        const restored = joined(served.chunks());
        expect(restored.byteLength, String(bit)).toBe(LENGTH);
        expect(Buffer.compare(restored, artifact), String(bit)).not.toBe(0);
      }
    }
    expect(refused).toBeGreaterThan(0);
    expect(complete).toBeGreaterThan(0);
  }, 60_000);
});

describe("decoderWindowBytes", () => {
  test("is the smallest window that covers the artifact, within brotli's bounds", () => {
    expect(decoderWindowBytes(1)).toBe(1024);
    expect(decoderWindowBytes(1024)).toBe(1024);
    expect(decoderWindowBytes(1025)).toBe(2048);
    expect(decoderWindowBytes(31_066_017)).toBe(2 ** 25);
    expect(decoderWindowBytes(2 ** 25)).toBe(2 ** 25);
    expect(decoderWindowBytes(2 ** 25 + 1)).toBe(2 ** 26);
    expect(decoderWindowBytes(2 ** 31)).toBe(2 ** 30);
  });
});

describe("the fixtures the server reads too", () => {
  const FIXTURES = new URL(
    "../../../../spec/fixtures/brotli-large-window/",
    import.meta.url,
  );

  test("decoderWindowBytes is two to the power of each row's window", () => {
    const table = JSON.parse(
      readFileSync(new URL("windows.json", FIXTURES), "utf8"),
    ) as { rows: { decodedBytes: number; windowBits: number }[] };
    expect(table.rows.length).toBeGreaterThanOrEqual(12);
    for (const row of table.rows)
      expect(
        decoderWindowBytes(row.decodedBytes),
        String(row.decodedBytes),
      ).toBe(2 ** row.windowBits);
  });

  test("the server's checked-in object decodes to its manifest with the pinned module", async () => {
    const manifest = JSON.parse(
      readFileSync(new URL("manifest.json", FIXTURES), "utf8"),
    ) as {
      decodedBytes: number;
      decodedSha256: string;
      encodedBytes: number;
      encodedSha256: string;
      windowBits: number;
    };
    const object = new Uint8Array(readFileSync(new URL("object.br", FIXTURES)));
    expect(object.byteLength).toBe(manifest.encodedBytes);
    expect(createHash("sha256").update(object).digest("hex")).toBe(
      manifest.encodedSha256,
    );
    expect(decoderWindowBytes(manifest.decodedBytes)).toBe(
      2 ** manifest.windowBits,
    );
    const served = await decodeAll(object, manifest.decodedBytes);
    expect(served.failure()).toBeUndefined();
    const decoded = joined(served.chunks());
    expect(decoded.byteLength).toBe(manifest.decodedBytes);
    expect(createHash("sha256").update(decoded).digest("hex")).toBe(
      manifest.decodedSha256,
    );
  });

  test("the server's checked-in packed object decodes and unpacks to its artifact", async () => {
    const SPEC = new URL("../../../../spec/fixtures/", import.meta.url);
    const manifest = JSON.parse(
      readFileSync(
        new URL("packed-brotli-large-window/manifest.json", SPEC),
        "utf8",
      ),
    ) as {
      artifact: string;
      decodedBytes: number;
      decodedSha256: string;
      bodyVersion: number;
      bodyBytes: number;
      encodedBytes: number;
      encodedSha256: string;
      windowBits: number;
    };
    const object = new Uint8Array(
      readFileSync(new URL("packed-brotli-large-window/object.br", SPEC)),
    );
    const artifact = readFileSync(new URL(manifest.artifact, SPEC));
    expect(object.byteLength).toBe(manifest.encodedBytes);
    expect(createHash("sha256").update(object).digest("hex")).toBe(
      manifest.encodedSha256,
    );
    expect(manifest.bodyVersion).toBe(1);
    // The stream's window covers the body, which is never longer than the
    // artifact the decoder sizes its own window from.
    expect(2 ** manifest.windowBits).toBeLessThanOrEqual(
      decoderWindowBytes(manifest.decodedBytes),
    );
    expect(2 ** manifest.windowBits).toBeGreaterThanOrEqual(manifest.bodyBytes);
    for (const pieceBytes of [1, 4096, object.byteLength]) {
      const served = await decodeAll(
        object,
        manifest.decodedBytes,
        pieceBytes,
        0,
        true,
      );
      expect(served.failure(), String(pieceBytes)).toBeUndefined();
      const decoded = joined(served.chunks());
      expect(Buffer.compare(decoded, artifact), String(pieceBytes)).toBe(0);
      expect(createHash("sha256").update(decoded).digest("hex")).toBe(
        manifest.decodedSha256,
      );
    }
    // Not told the body is packed, the worker sends the body itself.
    const unpacked = await decodeAll(object, manifest.bodyBytes);
    expect(unpacked.failure()).toBeUndefined();
    expect(joined(unpacked.chunks()).byteLength).toBe(manifest.bodyBytes);
  });
});
