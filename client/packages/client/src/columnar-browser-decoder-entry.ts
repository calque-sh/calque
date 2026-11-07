// The decoder worker for a `brotli-large-window` or
// `packed-brotli-large-window` artifact, bundled at the client's own URL
// beside the engine worker's entry, so an application constructs it through
// `createSyncClient` and never by naming the worker package. A page loads it
// only for a plan that selects one of those encodings.
import "@calque/worker/columnar-decoder-entry";
