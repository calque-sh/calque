// The direct-columnar engine worker, bundled at the client's own URL so an
// application constructs it through `createSyncClient` and never by naming
// the worker package.
import "@calque/worker/columnar-worker-entry";
