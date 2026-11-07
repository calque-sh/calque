/**
 * The decoder module this package ships was built by
 * `wasm/build-brotli-decoder.sh` from brotli 1.2.0: its bytes are held to the
 * SHA-256 recorded beside it, so a module that was rebuilt, patched or
 * replaced without that record changing fails here.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { expect, test } from "vitest";

const wasm = readFileSync(
  new URL("../wasm/brotli-decoder.wasm", import.meta.url),
);

test("the committed decoder module has the recorded SHA-256", () => {
  const record = readFileSync(
    new URL("../wasm/brotli-decoder.wasm.sha256", import.meta.url),
    "utf8",
  );
  // `shasum -a 256`'s own line, which the build script checks the same way.
  expect(record).toBe(
    `${createHash("sha256").update(wasm).digest("hex")}  brotli-decoder.wasm\n`,
  );
});

test("the decoder module imports nothing and exports the interface the worker drives", () => {
  const module = new WebAssembly.Module(wasm);
  expect(WebAssembly.Module.imports(module)).toEqual([]);
  expect(
    WebAssembly.Module.exports(module)
      .map(({ name }) => name)
      .sort(),
  ).toEqual([
    "_initialize",
    "dec_begin",
    "dec_feed",
    "dec_input",
    "dec_input_capacity",
    "dec_output",
    "dec_output_length",
    "dec_pull",
    "memory",
  ]);
});
