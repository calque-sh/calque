/**
 * The decoder worker's entry, evaluated against the scope a
 * `DedicatedWorker` gives it, with the module request answered from the
 * committed module.
 */
import { readFileSync } from "node:fs";
import { brotliCompressSync, constants } from "node:zlib";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import type {
  DecoderRequest,
  DecoderResponse,
} from "./columnar-decoder-protocol.js";

interface Scope {
  onmessage: ((event: { data: DecoderRequest }) => void) | null;
  postMessage(message: DecoderResponse, transfer: Transferable[]): void;
}

const scope = globalThis as unknown as Scope;
const wasm = readFileSync(
  new URL("../wasm/brotli-decoder.wasm", import.meta.url),
);
const artifact = Uint8Array.from({ length: 5000 }, (_, index) => index % 251);
const body = new Uint8Array(
  brotliCompressSync(artifact, {
    params: {
      [constants.BROTLI_PARAM_LARGE_WINDOW]: 1,
      [constants.BROTLI_PARAM_LGWIN]: 13,
    },
  }),
);

let responses: DecoderResponse[] = [];
let requested: string[] = [];

/** Evaluates the entry with its module request answered by `answer`. */
async function evaluate(answer: () => Response): Promise<void> {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      requested.push(String(input));
      return answer();
    }),
  );
  vi.resetModules();
  await import("./columnar-decoder-entry.js");
}

function decodeBody(): void {
  scope.onmessage?.({
    data: {
      type: "start",
      decodedBytes: artifact.byteLength,
      chunkBytes: 4096,
      skipChunks: 0,
      packed: false,
    },
  });
  scope.onmessage?.({
    data: { type: "bytes", bytes: body.slice().buffer, end: true },
  });
  scope.onmessage?.({ data: { type: "credit" } });
}

describe("the decoder worker's entry", () => {
  beforeEach(() => {
    responses = [];
    requested = [];
    scope.postMessage = (message) => responses.push(message);
    performance.clearMarks();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  for (const type of ["application/wasm", "application/octet-stream"])
    test(`loads the module beside it, sent as ${type}, and decodes a body with it`, async () => {
      await evaluate(
        () => new Response(wasm, { headers: { "content-type": type } }),
      );
      expect(
        performance.getEntriesByName("calque:decoder:evaluated"),
      ).toHaveLength(1);
      // One request, for the module, at this package's own URL.
      expect(requested).toHaveLength(1);
      expect(requested[0]).toMatch(/\/wasm\/brotli-decoder\.wasm$/u);
      decodeBody();
      await vi.waitFor(() => {
        expect(responses).toHaveLength(2);
      });
      expect(
        responses.map((response) =>
          response.type === "chunk"
            ? [response.index, new Uint8Array(response.bytes)]
            : response,
        ),
      ).toEqual([
        [0, artifact.subarray(0, 4096)],
        [1, artifact.subarray(4096)],
      ]);
    });

  test("reports a module the host does not have as its own failure", async () => {
    await evaluate(() => new Response("not found", { status: 404 }));
    decodeBody();
    await vi.waitFor(() => {
      expect(responses).toEqual([
        {
          type: "failed",
          corrupt: false,
          message:
            "the decoder module did not load: the module request answered HTTP 404",
        },
      ]);
    });
  });
});
