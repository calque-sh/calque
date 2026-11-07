// A hasher worker for the direct-columnar session, bundled at the client's
// own URL beside the engine worker's entry, so an application constructs it
// through `createSyncClient` and never by naming the worker package.
import "@calque/worker/columnar-hasher-entry";
