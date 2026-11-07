/**
 * The messages between the page and a hasher worker. A hasher takes the
 * SHA-256 of each chunk the page sends it and passes the chunk on, with its
 * digest, to the engine worker (`WorkerArtifactChunk`). It never sees the
 * plan's digests; the engine worker compares them.
 */

/**
 * The page's messages to a hasher: first the port to the engine worker, then
 * the chunks, each transferred.
 */
export type HasherRequest =
  | { readonly type: "port"; readonly port: MessagePort }
  | {
      readonly type: "chunk";
      readonly index: number;
      readonly bytes: ArrayBuffer;
    };

/** A hasher's only message to the page: it could not digest chunk `index`. */
export interface HasherFailure {
  readonly type: "failed";
  readonly index: number;
  readonly message: string;
}
