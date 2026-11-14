// Runs one packaged direct-columnar worker entry on a Node worker thread: the
// engine worker (`columnar-worker-entry.js`), a hasher worker
// (`columnar-hasher-entry.js`) or the brotli decoder worker
// (`columnar-decoder-entry.js`), named by `workerData.entry`.
//
// An entry reads `globalThis.postMessage` and assigns `globalThis.onmessage`,
// DOM worker globals Node lacks, so this module supplies them over
// `parentPort` and then imports the entry by absolute path, so its
// `@calque/core` imports resolve from the worker package. The decoder fetches
// its WebAssembly module beside its script, a `file:` URL here, which Node's
// `fetch` refuses, so this module answers that one request from disk.
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parentPort, workerData } from "node:worker_threads";

if (!parentPort) throw new Error("this module only runs on a worker thread");

const ENTRIES = new Set([
  "columnar-worker-entry.js",
  "columnar-hasher-entry.js",
  "columnar-decoder-entry.js",
]);
const name = workerData?.entry;
if (!ENTRIES.has(name))
  throw new Error(`no packaged columnar worker entry is named ${String(name)}`);

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const entry = pathToFileURL(
  join(root, "client/packages/worker/dist", name),
).href;

const networkFetch = globalThis.fetch;
globalThis.fetch = async (input, init) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  if (url.protocol !== "file:") return networkFetch(input, init);
  return new Response(await readFile(url), {
    headers: { "content-type": "application/wasm" },
  });
};

globalThis.postMessage = (message, transfer) => {
  parentPort.postMessage(message, transfer);
};

await import(entry);

if (typeof globalThis.onmessage !== "function")
  throw new Error(`${name} installed no message handler`);

parentPort.on("message", (data) => {
  globalThis.onmessage({ data });
});
