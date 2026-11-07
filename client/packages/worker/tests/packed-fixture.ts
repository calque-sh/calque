/**
 * A packed body from `spec/fixtures/packed-artifact`, as a
 * `packed-brotli-large-window` representation holds it, made on the Node
 * side for a suite that runs in the browser: the packer's body under Node's
 * large-window brotli encoder, at the window the worker opens for an
 * artifact of that length.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { brotliCompressSync, constants } from "node:zlib";

import type { BrowserCommand } from "vitest/node";

import { decoderWindowBytes } from "../src/columnar-decoder-worker.js";

declare module "vitest/browser" {
  interface BrowserCommands {
    encodePackedFixture: (name: string) => Promise<PackedFixture>;
  }
}

export interface PackedFixture {
  /** The packed body as a brotli stream, base64. */
  readonly body: string;
  /** The same body less its last byte, as a stream of its own, base64. */
  readonly cut: string;
  /** The length of the artifact the body stands for, and its SHA-256. */
  readonly decodedBytes: number;
  readonly sha256: string;
}

const FIXTURES = new URL(
  "../../../../spec/fixtures/packed-artifact/",
  import.meta.url,
);

export const encodePackedFixture: BrowserCommand<[name: string]> = (
  _context,
  name,
): PackedFixture => {
  const artifact = readFileSync(new URL(`${name}/artifact.bin`, FIXTURES));
  const packed = readFileSync(new URL(`${name}/packed.bin`, FIXTURES));
  const encode = (bytes: Uint8Array): string =>
    brotliCompressSync(bytes, {
      params: {
        [constants.BROTLI_PARAM_LARGE_WINDOW]: 1,
        [constants.BROTLI_PARAM_LGWIN]: Math.log2(
          decoderWindowBytes(artifact.byteLength),
        ),
        [constants.BROTLI_PARAM_QUALITY]: 9,
      },
    }).toString("base64");
  return {
    body: encode(packed),
    cut: encode(packed.subarray(0, packed.byteLength - 1)),
    decodedBytes: artifact.byteLength,
    sha256: createHash("sha256").update(artifact).digest("hex"),
  };
};
