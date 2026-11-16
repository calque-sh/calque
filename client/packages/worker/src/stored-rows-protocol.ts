/**
 * The messages between the page and the store worker, which seals and writes
 * the artifact a page keeps between visits. The engine worker sends it each
 * chunk it accepted on `port`, transferred, as a `StoredRowsChunk`.
 */
import type { StoredArtifact } from "./stored-rows.js";

export type StoreRequest =
  | {
      readonly type: "begin";
      readonly key: CryptoKey;
      readonly keyId: string;
      readonly artifact: StoredArtifact;
      /** The other end of the channel whose first end the engine worker has. */
      readonly port: MessagePort;
    }
  /** The page activated: write the record once every chunk is flushed. */
  | { readonly type: "commit" }
  /** The page did not activate: delete what was written. */
  | { readonly type: "abandon" };

/** The store worker's one answer, to `commit` or `abandon`. */
export interface StoreOutcome {
  readonly type: "committed" | "abandoned";
  /** Why the copy was not kept, for an abandoned one. */
  readonly reason?: string;
}

/**
 * What the store worker posts: `begun` once it has read the store's epochs,
 * which the page waits for before it sends `commit`, then its outcome.
 */
export type StoreReply = { readonly type: "begun" } | StoreOutcome;

/** A chunk the engine worker accepted, in the buffer it arrived in. */
export interface StoredRowsChunk {
  readonly index: number;
  readonly bytes: ArrayBuffer;
}
