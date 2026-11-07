/**
 * The brotli-large-window session tests' artifact, decoder and client: the
 * worker's own decoder code, driving the committed module in this thread.
 */
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

import {
  serveArtifactDecoder,
  type DecoderScope,
} from "@calque/worker/columnar-decoder-worker";
import { expect, vi } from "vitest";

import { createInternalColumnarClient } from "../client.js";
import { createGeneratedClientSchema } from "../client-options.js";
import { SyncError } from "../error.js";
import type { ColumnarDecoderWorker, ColumnarEngineWorker } from "./worker.js";
import {
  type Collections,
  FakeWorker,
  fakeHashers,
  descriptor,
  fakeFetch,
  validatedFetch,
  CHUNK_BYTES,
  longPlan,
  settled,
} from "./session.fixture.js";

// The response carries this body under no Content-Encoding, so the browser
// hands it over as it is stored and the session's decoder worker decodes it
// into the plan's chunks.

export const BLOCK_BYTES = 8192;

/**
 * The fixture's decoded bytes: blocks that repeat the one before but for
 * four bytes each, so a few kilobytes of brotli decode to megabytes whose
 * every block differs from the others.
 */
export function fixtureBytes(length: number): Uint8Array<ArrayBuffer> {
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

/**
 * Four and a half chunks: past 16 MiB, the largest window a stream that
 * is not large-window declares, so the smallest window that covers the
 * fixture is one only a large-window stream has.
 */
export const FIXTURE_BYTES = 4.5 * CHUNK_BYTES + 3;
export const FIXTURE_CHUNKS = [0, 1, 2, 3, 4];
export const DECODED = fixtureBytes(FIXTURE_BYTES);
/**
 * `DECODED` as `brotli -q 11 --large_window=25` wrote it, with the brotli
 * 1.2.0 command-line tool: 2^25 is the smallest window that covers it.
 */
export const ENCODED = new Uint8Array(
  readFileSync(
    new URL(
      "../../tests/fixtures/artifact.brotli-large-window",
      import.meta.url,
    ),
  ),
);
export const DECODER_MODULE = new WebAssembly.Module(
  readFileSync(
    createRequire(import.meta.url).resolve(
      "@calque/worker/brotli-decoder.wasm",
    ),
  ),
);

export type DecoderRequest = Parameters<
  NonNullable<DecoderScope["onmessage"]>
>[0]["data"];

/**
 * The decoder worker, served in this thread by the code the real worker
 * runs. Messages are cloned and their buffers transferred, and each is
 * delivered in a later task, as between a page and a worker.
 */
export class InThreadDecoder extends EventTarget {
  terminated = false;
  /** What the page said its body decodes to. */
  start: Extract<DecoderRequest, { type: "start" }> | undefined;
  /** How many bytes of the body the page has sent. */
  bodyBytes = 0;
  /** The indices of the chunks sent to the page. */
  readonly sent: number[] = [];
  readonly #scope: DecoderScope;

  constructor(module: () => Promise<WebAssembly.Module>) {
    super();
    this.#scope = {
      onmessage: null,
      postMessage: (message, transfer) => {
        const cloned = structuredClone(message, { transfer });
        if (cloned.type === "chunk") this.sent.push(cloned.index);
        this.#later(() =>
          this.dispatchEvent(new MessageEvent("message", { data: cloned })),
        );
      },
    };
    serveArtifactDecoder(
      this.#scope,
      async () => new WebAssembly.Instance(await module(), {}),
    );
  }

  postMessage(message: DecoderRequest, transfer: Transferable[] = []) {
    const cloned = structuredClone(message, { transfer });
    if (cloned.type === "start") this.start = cloned;
    else if (cloned.type === "bytes") this.bodyBytes += cloned.bytes.byteLength;
    this.#later(() =>
      this.#scope.onmessage?.({
        data: cloned,
      } as MessageEvent<DecoderRequest>),
    );
  }

  #later(deliver: () => void): void {
    setImmediate(() => {
      if (!this.terminated) deliver();
    });
  }

  terminate() {
    this.terminated = true;
  }
}

export type DecodedEncoding =
  "brotli-large-window" | "packed-brotli-large-window";

/**
 * The fixture plan for `decodedBytes`, selecting a body of `encodedBytes`
 * in `encoding`.
 */
export function brotliPlan(
  decodedBytes = FIXTURE_BYTES,
  encodedBytes = ENCODED.byteLength,
  encoding: DecodedEncoding = "brotli-large-window",
): Record<string, unknown> {
  const value = longPlan(decodedBytes);
  const artifact = (value.artifacts as Record<string, unknown>[])[0];
  if (!artifact) throw new Error("fixture artifact missing");
  Object.assign(artifact.selectedRepresentation as object, {
    encoding,
    encodedBytes,
  });
  return value;
}

/**
 * Serves `body` as the artifact, `pieceBytes` to a read and under no
 * Content-Encoding. `cut` names, for the response of each number, where
 * its body ends as a dropped connection ends one, once `when` settles.
 */
export function serveBody(
  body: Uint8Array,
  pieceBytes = body.byteLength,
  cut?: (
    response: number,
  ) => { readonly at: number; readonly when: Promise<void> } | undefined,
): (inner: typeof fetch) => typeof fetch {
  let responses = 0;
  return (inner) => async (input, init) => {
    if (!String(input).endsWith("/artifact-1")) return inner(input, init);
    responses += 1;
    const ending = cut?.(responses);
    const signal = init?.signal;
    let given = 0;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        signal?.addEventListener("abort", () => {
          controller.error(signal.reason);
        });
      },
      async pull(controller) {
        if (ending && given >= ending.at) {
          await ending.when;
          controller.error(new TypeError("network error"));
          return;
        }
        if (given >= body.byteLength) {
          controller.close();
          return;
        }
        const end = Math.min(
          given + pieceBytes,
          ending?.at ?? body.byteLength,
          body.byteLength,
        );
        controller.enqueue(body.slice(given, end));
        given = end;
      },
    });
    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "application/vnd.calque.direct-columnar.v4",
        "content-length": String(body.byteLength),
      },
    });
  };
}

/**
 * A client that decodes with `InThreadDecoder`s, one for each the session
 * spawns, whose module arrives when `module` resolves.
 */
export function makeDecodingClient(
  wrapFetch: (inner: typeof fetch) => typeof fetch,
  planValue: Record<string, unknown> = brotliPlan(),
  module: () => Promise<WebAssembly.Module> = async () => DECODER_MODULE,
) {
  const worker = new FakeWorker();
  const hashers = fakeHashers();
  const decoders: InThreadDecoder[] = [];
  const decoderFactory = vi.fn(() => {
    const decoder = new InThreadDecoder(module);
    decoders.push(decoder);
    return decoder as unknown as ColumnarDecoderWorker;
  });
  const fetcher = vi.fn(validatedFetch(wrapFetch(fakeFetch(planValue))));
  const client = createInternalColumnarClient<Collections>(
    {
      endpoint: "https://sync.test/root",
      auth: () => ({ bearerToken: "token" }),
      schema: createGeneratedClientSchema<Collections>(descriptor),
      memoryBudgetBytes: 32 * 1024 * 1024,
      deviceId: "device-a",
      fetch: fetcher,
      clientHeapReserveBytes: 4 * 1024 * 1024,
    },
    () => worker as unknown as ColumnarEngineWorker,
    hashers.create,
    decoderFactory,
  );
  /** How many requests the client sent to a URL ending in `suffix`. */
  const requests = (suffix: string): number =>
    fetcher.mock.calls.filter(([input]) => String(input).endsWith(suffix))
      .length;
  return {
    client,
    worker,
    hashers: hashers.spawned,
    decoders,
    decoderFactory,
    fetcher,
    requests,
  };
}

/** The chunks an engine worker received, end to end. */
export function joined(received: readonly Uint8Array[]): Uint8Array {
  return Buffer.concat(received);
}

export function marks(): string[] {
  return performance.getEntriesByType("mark").map(({ name }) => name);
}

/** `ENCODED` with one bit flipped. */
export function flipped(byte: number, bit: number): Uint8Array {
  const body = ENCODED.slice();
  body[byte] = (body[byte] ?? 0) ^ (1 << bit);
  return body;
}

/**
 * Loads `body`, in pieces of `pieceBytes`, as the body `planValue` selects
 * in `encoding`, and expects the load to end refused for `reason`, with no
 * row shown and nothing asked for again.
 */
export async function refused(
  body: Uint8Array,
  pieceBytes: number,
  planValue: Record<string, unknown>,
  encoding: DecodedEncoding,
  reason: string,
): Promise<void> {
  const { client, worker, hashers, decoders, requests } = makeDecodingClient(
    serveBody(body, pieceBytes),
    planValue,
  );
  const status = await settled(client);
  // The error a body of the wrong length gets: a response that is not
  // the artifact its plan describes.
  expect(status).toMatchObject({
    state: "error",
    error: {
      code: "INVALID_RESPONSE",
      scope: "sync",
      recovery: "reset",
      message: `artifact response is not its plan's ${encoding} body: ${reason}`,
    },
  });
  expect(status.state === "error" && status.error).toBeInstanceOf(SyncError);
  // The artifact never ended, so the engine worker opened nothing.
  expect(
    worker.requests.some((request) => request.type === "artifact-end"),
  ).toBe(false);
  expect(worker.terminated).toBe(true);
  expect(hashers.map(({ terminated }) => terminated)).toEqual([
    true,
    true,
    true,
  ]);
  expect(decoders.map(({ terminated }) => terminated)).toEqual([true]);
  await expect(client.db.events.query().run()).rejects.toMatchObject({
    code: "INVALID_RESPONSE",
  });
  // A body that is wrong is not one the network cut.
  await new Promise((resolve) => setTimeout(resolve, 300));
  expect(requests("/artifact-1")).toBe(1);
  expect(marks()).not.toContain("calque:artifact:retry:1");
  await client.close();
}
