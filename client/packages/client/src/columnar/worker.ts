/** The engine worker the direct-columnar session drives; a DOM `Worker` fits. */
export type ColumnarEngineWorker = Pick<
  Worker,
  "addEventListener" | "postMessage" | "terminate"
>;

/** A hasher worker the direct-columnar session spawns; a DOM `Worker` fits. */
export type ColumnarHasherWorker = ColumnarEngineWorker;

/** A decoder worker the direct-columnar session spawns; a DOM `Worker` fits. */
export type ColumnarDecoderWorker = ColumnarEngineWorker;

/** The platform's `Worker`, read once; a runtime without one is refused. */
function workerConstructor(): typeof globalThis.Worker {
  const WorkerConstructor = globalThis.Worker;
  if (typeof WorkerConstructor !== "function")
    throw new TypeError(
      "this runtime has no Worker constructor; Calque runs only in Chromium-based browsers",
    );
  return WorkerConstructor;
}

export function createColumnarBrowserWorker(): ColumnarEngineWorker {
  return instantiateColumnarBrowserWorker(workerConstructor());
}

// Spelled `new Worker(new URL("…", import.meta.url), …)` verbatim, the only
// shape a bundler recognises as a worker entry and emits as a worker chunk.
// Any other spelling turns the URL into an asset reference, which inlines the
// unbundled entry, whose bare `@calque/worker` import no worker can resolve.
function instantiateColumnarBrowserWorker(
  Worker: typeof globalThis.Worker,
): ColumnarEngineWorker {
  return new Worker(
    new URL("../columnar-browser-worker-entry.js", import.meta.url),
    { type: "module", name: "calque-columnar" },
  );
}

/**
 * One of the hasher workers that take the artifact's chunk digests off the
 * engine worker; the session spawns them when it activates.
 */
export function createColumnarBrowserHasher(): ColumnarHasherWorker {
  return instantiateColumnarBrowserHasher(workerConstructor());
}

// Spelled verbatim for the bundler, as `instantiateColumnarBrowserWorker` is.
function instantiateColumnarBrowserHasher(
  Worker: typeof globalThis.Worker,
): ColumnarHasherWorker {
  return new Worker(
    new URL("../columnar-browser-hasher-entry.js", import.meta.url),
    { type: "module", name: "calque-hasher" },
  );
}

/**
 * The decoder worker of one `brotli-large-window` or
 * `packed-brotli-large-window` response; the session spawns it once a plan
 * selects either encoding, and for no other plan.
 */
export function createColumnarBrowserDecoder(): ColumnarDecoderWorker {
  return instantiateColumnarBrowserDecoder(workerConstructor());
}

// Spelled verbatim for the bundler, as `instantiateColumnarBrowserWorker` is.
function instantiateColumnarBrowserDecoder(
  Worker: typeof globalThis.Worker,
): ColumnarDecoderWorker {
  return new Worker(
    new URL("../columnar-browser-decoder-entry.js", import.meta.url),
    { type: "module", name: "calque-decoder" },
  );
}
