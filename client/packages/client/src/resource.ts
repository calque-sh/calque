import type { SyncError } from "./error.js";

/**
 * What a live query currently publishes. `Result` is the typed row array the
 * generated facade the query was built from produces; there is no untyped
 * default, because there is no untyped query.
 */
export type LiveQuerySnapshot<Result> =
  | { readonly status: "loading" }
  | { readonly status: "ready"; readonly result: Result }
  | { readonly status: "error"; readonly error: SyncError };

/**
 * A live query, in the shape React's `useSyncExternalStore` reads. While a
 * listener is subscribed, each publication that moves the query's collections
 * re-reads it; once the last one leaves, publications skip it.
 * `getServerSnapshot` is always `loading`, and `promise` settles when the
 * read in flight does, or at once when none is.
 */
export interface LiveQueryResource<Result> {
  readonly getSnapshot: () => LiveQuerySnapshot<Result>;
  readonly getServerSnapshot: () => LiveQuerySnapshot<Result>;
  readonly subscribe: (listener: () => void) => () => void;
  readonly promise: Promise<void>;
}
