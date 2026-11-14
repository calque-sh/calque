import type {
  ArtifactCapacity,
  QueryExecutionOptions,
  QueryResult,
  QuerySpecification,
  QueryWindow,
  QueryWindowOptions,
  QueryWindowResult,
  TrustedArtifact,
} from "@calque/core";

import type {
  ColumnarChangeContract,
  ColumnarChangeFence,
} from "./columnar-change.js";

export type WorkerRequest =
  | {
      readonly id: number;
      readonly type: "open";
      /**
       * The artifact arrives after this request, one `WorkerArtifactChunk`
       * at a time on the `hashers` ports, and the worker checks each chunk's
       * digest against `trusted.chunks` as it does.
       */
      readonly trusted: TrustedArtifact;
      /**
       * One port per hasher worker, each the other end of a channel the page
       * gave that hasher. The worker takes chunks only from these.
       */
      readonly hashers: readonly MessagePort[];
      readonly capacity?: ArtifactCapacity;
      /** Pool allocated to query structures by the caller's memory plan. */
      readonly queryCapacityBytes?: number;
      /**
       * Makes the mount live. Absent, the mount is frozen at the artifact's
       * cursor and `change` is refused.
       */
      readonly changeStream?: ColumnarChangeContract;
    }
  | {
      readonly id: number;
      readonly type: "change";
      /**
       * Change batches in the order the stream carried them, each a complete
       * frame sequence. They are applied as one batch, all or none.
       */
      readonly batches: readonly Uint8Array[];
      /** Present for a fenced catch-up response; absent for a live poll. */
      readonly fence?: ColumnarChangeFence;
      /**
       * The plan the request named, which every batch must name. Absent, the
       * plan the mount opened under.
       */
      readonly planId?: string;
    }
  | {
      readonly id: number;
      readonly type: "state";
    }
  | {
      readonly id: number;
      readonly type: "rows";
      readonly table: string;
      readonly start: number;
      readonly count: number;
      readonly fields: readonly string[];
    }
  | {
      readonly id: number;
      readonly type: "query";
      readonly specification: QuerySpecification;
      readonly options?: QueryExecutionOptions;
    }
  | {
      readonly id: number;
      readonly type: "window";
      readonly specification: QuerySpecification;
      readonly window: QueryWindow;
      readonly options?: QueryWindowOptions;
    }
  | {
      readonly id: number;
      readonly type: "frame";
      /** Answered together, every result at the one state the answer names. */
      readonly members: readonly FrameMember[];
    }
  | {
      readonly id: number;
      readonly type: "mutate";
      readonly mutationId: string;
      /**
       * The mutation's `canonical_input`: one encoded
       * `sync.change.v1.Operation` per affected row, applied through the
       * decoder live changes use, so the client reads the grammar the server
       * validates.
       */
      readonly operations: readonly Uint8Array[];
    }
  | {
      readonly id: number;
      readonly type: "drop";
      readonly mutationId: string;
    };

export type WorkerSuccess =
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "open";
      readonly rowCount: number;
      readonly artifactByteLength: number;
      readonly tableRows: Readonly<Record<string, number>>;
      readonly state: ColumnarMountState;
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "change";
      readonly state: ColumnarMountState;
      readonly changedCollections: readonly string[];
      readonly acknowledgedMutationIds: readonly string[];
      readonly transactionCount: number;
      readonly operationCount: number;
      readonly currentlyUpToDate: boolean;
      readonly latestCommitTimeUnixMicros: bigint | null;
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "state";
      readonly state: ColumnarMountState;
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "rows";
      readonly table: string;
      readonly start: number;
      readonly fields: readonly string[];
      readonly rows: readonly (readonly unknown[])[];
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "query";
      readonly result: QueryResult;
      /** The collection generations the whole execution read at. */
      readonly generations: Readonly<Record<string, number>>;
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "window";
      readonly result: QueryWindowResult;
      /** The collection generations the whole execution read at. */
      readonly generations: Readonly<Record<string, number>>;
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "frame";
      /** Every collection's generation at the state all `results` hold at. */
      readonly generations: Readonly<Record<string, number>>;
      /** One per member, in order; `null` for a member still at `seen`. */
      readonly results: readonly (FrameResult | null)[];
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "mutate";
      readonly state: ColumnarMountState;
    }
  | {
      readonly id: number;
      readonly ok: true;
      readonly type: "drop";
      readonly state: ColumnarMountState;
    };

/**
 * One live query of a `frame`. A member with `seen` is run only if a
 * collection it names has moved past that generation by the time the frame
 * answers.
 */
export type FrameMember =
  | {
      readonly type: "query";
      readonly specification: QuerySpecification;
      readonly options?: QueryExecutionOptions;
      readonly seen?: Readonly<Record<string, number>>;
    }
  | {
      readonly type: "window";
      readonly specification: QuerySpecification;
      readonly window: QueryWindow;
      readonly options?: QueryWindowOptions;
      readonly seen?: Readonly<Record<string, number>>;
    };

export type FrameResult =
  | { readonly ok: true; readonly result: QueryResult | QueryWindowResult }
  | { readonly ok: false; readonly error: WorkerError };

/**
 * Withdraws the `query`, `window` or `frame` request `target`, whose caller
 * no longer wants its answer. The worker stops executing it and answers
 * neither it nor this; a target already answered or unknown is ignored.
 */
export interface WorkerCancel {
  readonly type: "cancel";
  readonly target: number;
}

/**
 * One chunk of the artifact an `open` is receiving, from a hasher worker on
 * one of the open's `hashers` ports: chunk `index` of `trusted.chunks`, and
 * the SHA-256 the hasher took of exactly `bytes`, as `sha256:<hex>`. The
 * worker copies the chunk into place only once that digest is the plan's,
 * then credits the page with a `WorkerArtifactCredit`.
 */
export interface WorkerArtifactChunk {
  readonly index: number;
  readonly bytes: ArrayBuffer;
  readonly sha256: string;
}

/**
 * The page has read the whole artifact and sent its `chunks` chunks to the
 * hashers. It is not answered: the `open` answers once every one of them has
 * been accepted and the artifact mounted, or once one was refused.
 */
export interface WorkerArtifactEnd {
  readonly type: "artifact-end";
  readonly chunks: number;
}

/**
 * The worker accepted chunk `index`: the hasher that sent it may be sent
 * another. Unlike a response it has no `id`.
 */
export interface WorkerArtifactCredit {
  readonly type: "artifact-credit";
  readonly index: number;
}

/**
 * What the mount currently publishes. One cursor covers every collection,
 * because the whole mount advances from one canonical cursor to the next.
 */
export interface ColumnarMountState {
  /** UTF-8 change-stream cursor bytes, or `undefined` for a frozen mount. */
  readonly cursor: Uint8Array | undefined;
  readonly live: boolean;
  readonly collectionRows: Readonly<Record<string, number>>;
  /**
   * Per-collection publication generation; a publication bumps every changed
   * one.
   */
  readonly collectionGenerations: Readonly<Record<string, number>>;
  /**
   * Mutations applied optimistically and not yet echoed or dropped, in
   * submission order. The live poll carries them so the server can attribute
   * the echo; empty for a mount without an overlay.
   */
  readonly pendingMutationIds: readonly string[];
}

export interface WorkerError {
  readonly name: string;
  readonly message: string;
  readonly code?: string;
}

interface WorkerFailure {
  readonly id: number;
  readonly ok: false;
  readonly error: WorkerError;
}

export type WorkerResponse = WorkerSuccess | WorkerFailure;
