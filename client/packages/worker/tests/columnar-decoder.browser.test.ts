/**
 * A `brotli-large-window` body decoded in Chromium by the real decoder
 * worker, and a `packed-brotli-large-window` one decoded and unpacked by it:
 * `columnar-decoder-entry.ts`, which fetches the committed module from
 * beside itself and compiles it as it streams.
 *
 * The page's part is `ArtifactDecoder`, as the session drives it: it writes
 * the body as it would arrive, takes the chunks in order and returns a credit
 * for each. The session's own use of it is unit-tested in Node; this suite
 * shows that the worker and its module load and decode in Chromium.
 */
import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";

import {
  ArtifactBodyError,
  ArtifactDecoder,
} from "../src/columnar-worker-client.js";
// Imported for their declarations of the commands; the modules need Node.
import type {} from "./brotli-fixture.js";
import type {} from "./packed-fixture.js";

const CHUNK_BYTES = 4 * 1024 * 1024;
const BLOCK_BYTES = 8192;
/** Four and a half chunks: more than any window under 2^25 covers. */
const FIXTURE_BYTES = 4.5 * CHUNK_BYTES + 3;

/**
 * What the fixture decodes to: blocks that repeat the one before but for
 * four bytes each.
 */
function fixtureBytes(length: number): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(length);
  let state = 0x9e3779b9;
  for (let index = 0; index < length; index += 1) {
    if (index < BLOCK_BYTES) bytes[index] = (index * 31) % 251;
    else if (index % BLOCK_BYTES < 4) {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      bytes[index] = state >>> 24;
    } else bytes[index] = bytes[index - BLOCK_BYTES] ?? 0;
  }
  return bytes;
}

function bytesOf(base64: string): Uint8Array<ArrayBuffer> {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1)
    bytes[index] = binary.charCodeAt(index);
  return bytes;
}

async function sha256(bytes: BufferSource): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

const decoders: ArtifactDecoder[] = [];

afterEach(() => {
  for (const decoder of decoders.splice(0)) decoder.close();
});

/**
 * A decoder for a body that decodes to `decodedBytes`, or that unpacks to
 * them when it is `packed`, on the real worker.
 */
function decoder(
  decodedBytes: number,
  skipChunks = 0,
  packed = false,
  chunkBytes = CHUNK_BYTES,
): ArtifactDecoder {
  const spawned = new ArtifactDecoder(
    () =>
      new Worker(new URL("../src/columnar-decoder-entry.ts", import.meta.url), {
        type: "module",
        name: "calque-decoder",
      }),
    { decodedBytes, chunkBytes, skipChunks, packed },
  );
  decoders.push(spawned);
  return spawned;
}

/** Writes `body` as a response delivers it, `pieceBytes` at a time. */
async function write(
  to: ArtifactDecoder,
  body: Uint8Array,
  pieceBytes: number,
): Promise<void> {
  for (let start = 0; start < body.byteLength; start += pieceBytes) {
    const end = Math.min(start + pieceBytes, body.byteLength);
    to.write(body.slice(start, end).buffer, end === body.byteLength);
    await to.room();
  }
}

describe("the decoder worker in Chromium", () => {
  it("decodes a large-window body into the plan's chunks with the module it fetched", async () => {
    const body = bytesOf(await commands.readBrotliFixture());
    const expected = fixtureBytes(FIXTURE_BYTES);
    const decoding = decoder(FIXTURE_BYTES);
    const writing = write(decoding, body, 1000);
    const chunks: ArrayBuffer[] = [];
    for (let index = 0; index < 5; index += 1) {
      chunks.push(await Promise.race([decoding.next(), decoding.failed]));
      decoding.credit();
    }
    await writing;
    expect(chunks.map(({ byteLength }) => byteLength)).toEqual([
      CHUNK_BYTES,
      CHUNK_BYTES,
      CHUNK_BYTES,
      CHUNK_BYTES,
      FIXTURE_BYTES - 4 * CHUNK_BYTES,
    ]);
    // Compared by digest: 18 MiB is slow to compare element by element.
    expect(await Promise.all(chunks.map((chunk) => sha256(chunk)))).toEqual(
      await Promise.all(
        chunks.map((_chunk, index) =>
          sha256(
            expected.subarray(index * CHUNK_BYTES, (index + 1) * CHUNK_BYTES),
          ),
        ),
      ),
    );
  });

  it("skips the chunks an earlier response delivered", async () => {
    const body = bytesOf(await commands.readBrotliFixture());
    const expected = fixtureBytes(FIXTURE_BYTES);
    const decoding = decoder(FIXTURE_BYTES, 3);
    void write(decoding, body, body.byteLength).catch(() => undefined);
    const fourth = await Promise.race([decoding.next(), decoding.failed]);
    decoding.credit();
    const last = await Promise.race([decoding.next(), decoding.failed]);
    expect(await sha256(fourth)).toBe(
      await sha256(expected.subarray(3 * CHUNK_BYTES, 4 * CHUNK_BYTES)),
    );
    expect(await sha256(last)).toBe(
      await sha256(expected.subarray(4 * CHUNK_BYTES)),
    );
  });

  it("unpacks a packed body into the plan's chunks, and refuses one cut short of its last part", async () => {
    const fixture = await commands.encodePackedFixture("every-kind");
    // Small, so the fixture's 72,714 bytes are five chunks.
    const chunkBytes = 16_384;
    const count = Math.ceil(fixture.decodedBytes / chunkBytes);
    const decoding = decoder(fixture.decodedBytes, 0, true, chunkBytes);
    const writing = write(decoding, bytesOf(fixture.body), 1000);
    const restored = new Uint8Array(fixture.decodedBytes);
    for (let index = 0; index < count; index += 1) {
      const chunk = await Promise.race([decoding.next(), decoding.failed]);
      decoding.credit();
      expect(chunk.byteLength).toBe(
        Math.min(chunkBytes, fixture.decodedBytes - index * chunkBytes),
      );
      restored.set(new Uint8Array(chunk), index * chunkBytes);
    }
    await writing;
    expect(await sha256(restored)).toBe(fixture.sha256);

    // The stream ends where its body does, a byte before the last part's.
    const short = decoder(fixture.decodedBytes, 0, true, chunkBytes);
    void write(short, bytesOf(fixture.cut), 1000).catch(() => undefined);
    let taken = 0;
    const cut = await (async () => {
      for (;;) {
        await short.next();
        short.credit();
        taken += 1;
      }
    })().catch((error: unknown) => error);
    expect(taken).toBe(count - 1);
    expect(cut).toBeInstanceOf(ArtifactBodyError);
    expect((cut as Error).message).toBe("the body ends before its last part");
  });

  it("refuses a body cut short of its stream, and a stream wider than its artifact", async () => {
    const body = bytesOf(await commands.readBrotliFixture());
    const short = decoder(FIXTURE_BYTES);
    void write(short, body.subarray(0, body.byteLength - 1), 1000).catch(
      () => undefined,
    );
    // The chunks before the cut are taken, as a load takes them; the worker
    // reads on only against the credit for each.
    let taken = 0;
    const cut = await (async () => {
      for (;;) {
        await short.next();
        short.credit();
        taken += 1;
      }
    })().catch((error: unknown) => error);
    // Every chunk but the last, which the worker holds until the stream ends.
    expect(taken).toBe(4);
    expect(cut).toBeInstanceOf(ArtifactBodyError);
    expect((cut as Error).message).toBe(
      "the body ends before its brotli stream does",
    );

    // The plan says four chunks, which a 2^24 window covers; the stream
    // declares 2^25, and the worker allocates none of it.
    const wide = decoder(4 * CHUNK_BYTES);
    void write(wide, body, 1000).catch(() => undefined);
    const refused = await wide.failed.catch((error: unknown) => error);
    expect(refused).toBeInstanceOf(ArtifactBodyError);
    expect((refused as Error).message).toBe(
      "the brotli stream declares a larger window than its artifact needs",
    );
  });
});
