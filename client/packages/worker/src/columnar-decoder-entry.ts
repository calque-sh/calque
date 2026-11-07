// The decoder worker: one `brotli-large-window` or
// `packed-brotli-large-window` artifact body in, the decoded artifact's
// chunks out (`columnar-decoder-worker.ts`). The page spawns it only for a
// plan that selects one of those encodings. Its module is brotli's reference
// decoder, an asset of this package beside the worker's own script, and
// starts loading as the script evaluates.
import {
  serveArtifactDecoder,
  type DecoderScope,
} from "./columnar-decoder-worker.js";

/**
 * The module, compiled as it downloads when the host sends it as
 * `application/wasm`, and from its whole body when the host does not.
 */
async function instantiate(): Promise<WebAssembly.Instance> {
  const response = await fetch(
    new URL("../wasm/brotli-decoder.wasm", import.meta.url),
  );
  if (!response.ok)
    throw new Error(`the module request answered HTTP ${response.status}`);
  const type = response.headers.get("content-type")?.split(";", 1)[0]?.trim();
  const { instance } =
    type?.toLowerCase() === "application/wasm"
      ? await WebAssembly.instantiateStreaming(response, {})
      : await WebAssembly.instantiate(await response.arrayBuffer(), {});
  return instance;
}

serveArtifactDecoder(globalThis as unknown as DecoderScope, instantiate);
