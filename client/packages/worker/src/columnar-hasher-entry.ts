// A hasher worker: the SHA-256 of each artifact chunk the page sends it,
// passed on with the chunk to the engine worker. It holds no plan digest and
// imports nothing, so it is a small chunk that loads during the bootstrap.
import type {
  HasherFailure,
  HasherRequest,
} from "./columnar-hasher-protocol.js";
import type { WorkerArtifactChunk } from "./columnar-worker-protocol.js";

interface HasherScope {
  onmessage: ((event: MessageEvent<HasherRequest>) => void) | null;
  postMessage(message: HasherFailure): void;
}

const scope = globalThis as unknown as HasherScope;

/** A user-timing mark on the cold path, for a trace or a harness to read. */
function mark(name: string): void {
  globalThis.performance?.mark?.(`calque:${name}`);
}
mark("hasher:evaluated");

let engine: MessagePort | undefined;
let first = true;

function hex(digest: ArrayBuffer): string {
  return Array.from(new Uint8Array(digest), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
}

scope.onmessage = (event) => {
  const request = event.data;
  if (request.type === "port") {
    engine = request.port;
    return;
  }
  const { index, bytes } = request;
  if (first) {
    first = false;
    mark("hasher:first-chunk");
  }
  const port = engine;
  if (!port) {
    scope.postMessage({
      type: "failed",
      index,
      message: "the hasher has no port to the engine worker",
    });
    return;
  }
  crypto.subtle.digest("SHA-256", bytes).then(
    (digest) => {
      // The chunk goes on in the buffer it was hashed from, transferred, so
      // the engine worker holds the only copy of the bytes the digest is of.
      port.postMessage(
        {
          index,
          bytes,
          sha256: `sha256:${hex(digest)}`,
        } satisfies WorkerArtifactChunk,
        [bytes],
      );
    },
    (error: unknown) => {
      scope.postMessage({
        type: "failed",
        index,
        message: error instanceof Error ? error.message : String(error),
      });
    },
  );
};
