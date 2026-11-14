import type {
  ArtifactCapacity,
  QueryCompletion,
  QueryExecutionOptions,
  QueryJsonValue,
  QueryOutputValue,
  QueryResult,
  QuerySpecification,
  QueryWindow,
  QueryWindowOptions,
  QueryWindowResult,
  TrustedArtifact,
} from "@calque/core";
import type {
  ColumnarMountState,
  FrameMember,
  WorkerArtifactCredit,
  WorkerArtifactEnd,
  WorkerCancel,
  WorkerError,
  WorkerRequest,
  WorkerResponse,
  WorkerSuccess,
} from "./columnar-worker-protocol.js";
import type {
  ColumnarChangeContract,
  ColumnarChangeFence,
} from "./columnar-change.js";

export type {
  ColumnarMountState,
  FrameMember,
} from "./columnar-worker-protocol.js";
export {
  ArtifactHashers,
  HASHER_CREDIT_MS,
  hasherCount,
} from "./columnar-hashers.js";
export { ArtifactBodyError, ArtifactDecoder } from "./columnar-decoder.js";
export { DECODER_INPUT_WINDOW_BYTES } from "./columnar-decoder-protocol.js";
export { awaitedLater, delay, stopReading } from "./async.js";
// `ColumnarLiveSession` has its own entry point,
// `@calque/worker/columnar-live`: it carries the protobuf runtime and the
// change-stream schemas, which a mount that never goes live should not load.
export type {
  ColumnarChangeContract,
  ColumnarChangeFence,
  ColumnarChangeTableBinding,
} from "./columnar-change.js";

export interface OpenedWorkspace {
  readonly rowCount: number;
  readonly artifactByteLength: number;
  readonly tableRows: Readonly<Record<string, number>>;
  readonly state: ColumnarMountState;
}

/**
 * Where an open's chunks come from: hasher workers holding the other ends of
 * the ports it takes. `ArtifactHashers` is one.
 */
export interface ArtifactChunkSource {
  /** The ports the engine worker takes the hashed chunks from; taken once. */
  takePorts(): MessagePort[];
  /** The engine worker accepted chunk `index`. */
  credit(index: number): void;
}

/** An open whose artifact reaches the worker while it downloads. */
export interface ReceivingWorkspace {
  /**
   * Says every one of the artifact's `chunks` chunks has been sent to a
   * hasher; the open answers once the worker has accepted each of them.
   */
  end(chunks: number): void;
  readonly opened: Promise<OpenedWorkspace>;
}

/**
 * One publication of tail batches: the mount's new position and what moved
 * with it.
 */
export interface AppliedChangeBatch {
  readonly state: ColumnarMountState;
  readonly changedCollections: readonly string[];
  readonly acknowledgedMutationIds: readonly string[];
  readonly transactionCount: number;
  readonly operationCount: number;
  readonly currentlyUpToDate: boolean;
  /** The newest source commit time applied; `null` when there was none. */
  readonly latestCommitTimeUnixMicros: bigint | null;
}

export interface ApplyChangeOptions {
  /** The terms of the fenced catch-up request the batches answer. */
  readonly fence?: ColumnarChangeFence;
  /**
   * The plan the request named, once a renewal has replaced the one the
   * mount opened under.
   */
  readonly planId?: string;
}

interface MaterializedRows {
  readonly table: string;
  readonly start: number;
  readonly fields: readonly string[];
  readonly rows: readonly (readonly unknown[])[];
}

function sealOutputValue(value: QueryOutputValue): QueryOutputValue {
  if (value === null || typeof value !== "object") return value;
  // A binary column arrives as the same `Uint8Array` the materialization
  // surface produces. Structured clone already gave this thread its own copy,
  // and walking it as a record would shred it into numeric keys.
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value))
    return Object.freeze(
      value.map((child) => sealOutputValue(child) as QueryJsonValue),
    );
  const result: Record<string, QueryJsonValue> = Object.create(null) as Record<
    string,
    QueryJsonValue
  >;
  for (const [key, child] of Object.entries(value))
    result[key] = sealOutputValue(child) as QueryJsonValue;
  return Object.freeze(result);
}

function sealQueryResult(result: QueryResult): QueryResult {
  return Object.freeze({
    columns: Object.freeze([...result.columns]),
    rows: Object.freeze(
      result.rows.map((row) =>
        Object.freeze(row.map((value) => sealOutputValue(value))),
      ),
    ),
  });
}

function workerError(failure: WorkerError): Error {
  const error = new Error(failure.message);
  error.name = failure.name;
  if (failure.code)
    Object.defineProperty(error, "code", {
      value: failure.code,
      enumerable: true,
    });
  return error;
}

/**
 * A frame's answer: `generations` names the one state every result holds at,
 * and a member still at its `seen` has no result.
 */
export interface FrameCompletion {
  readonly generations: Readonly<Record<string, number>>;
  readonly results: readonly (
    | { readonly ok: true; readonly result: QueryResult | QueryWindowResult }
    | { readonly ok: false; readonly error: Error }
    | null
  )[];
}

type Pending = {
  resolve(value: WorkerSuccess): void;
  reject(reason: Error): void;
  cleanup(): void;
};
type RequestWithoutId = WorkerRequest extends infer Request
  ? Request extends WorkerRequest
    ? Omit<Request, "id">
    : never
  : never;

export class EngineWorkerClient {
  #nextId = 1;
  readonly #pending = new Map<number, Pending>();
  /** The source of the artifact being received, credited as chunks land. */
  #chunks: ArtifactChunkSource | undefined;

  constructor(readonly worker: Worker) {
    worker.addEventListener(
      "message",
      (event: MessageEvent<WorkerResponse | WorkerArtifactCredit>) => {
        const response = event.data;
        if ("type" in response && response.type === "artifact-credit") {
          this.#chunks?.credit(response.index);
          return;
        }
        const pending = this.#pending.get(response.id);
        if (!pending) return;
        this.#pending.delete(response.id);
        pending.cleanup();
        if (response.ok) pending.resolve(response);
        else pending.reject(workerError(response.error));
      },
    );
    worker.addEventListener("error", (event) =>
      this.#failAll(new Error(event.message || "engine worker failed")),
    );
    worker.addEventListener("messageerror", () =>
      this.#failAll(new Error("engine worker message could not be decoded")),
    );
  }

  /**
   * Opens a workspace whose artifact arrives after this call, chunk by chunk
   * from `chunks`' hashers. The worker takes a chunk only once its digest
   * matches `trusted.chunks`, credits `chunks` for each, and opens nothing
   * until every chunk has matched.
   */
  receive(
    trusted: TrustedArtifact,
    chunks: ArtifactChunkSource,
    capacity?: ArtifactCapacity,
    queryCapacityBytes?: number,
    signal?: AbortSignal,
    changeStream?: ColumnarChangeContract,
  ): ReceivingWorkspace {
    this.#chunks = chunks;
    const opened = this.#open(
      trusted,
      chunks.takePorts(),
      capacity,
      queryCapacityBytes,
      signal,
      changeStream,
    );
    return {
      end: (count) => {
        this.worker.postMessage({
          type: "artifact-end",
          chunks: count,
        } satisfies WorkerArtifactEnd);
      },
      opened,
    };
  }

  async #open(
    trusted: TrustedArtifact,
    hashers: MessagePort[],
    capacity: ArtifactCapacity | undefined,
    queryCapacityBytes: number | undefined,
    signal: AbortSignal | undefined,
    changeStream: ColumnarChangeContract | undefined,
  ): Promise<OpenedWorkspace> {
    const response = await this.#request(
      {
        type: "open",
        trusted,
        hashers,
        ...(capacity === undefined ? {} : { capacity }),
        ...(queryCapacityBytes === undefined ? {} : { queryCapacityBytes }),
        ...(changeStream === undefined ? {} : { changeStream }),
      },
      hashers,
      signal,
      true,
    ).finally(() => {
      this.#chunks = undefined;
    });
    if (response.type !== "open")
      throw new Error("unexpected engine worker response");
    return {
      rowCount: response.rowCount,
      artifactByteLength: response.artifactByteLength,
      tableRows: response.tableRows,
      state: response.state,
    };
  }

  /**
   * Applies change batches as one publication, every batch or none. It
   * resolves only once they are published, so a caller that awaits it and
   * then reads is reading at the returned cursor and no other.
   */
  async applyChangeBatches(
    batches: readonly Uint8Array[],
    options: ApplyChangeOptions = {},
  ): Promise<AppliedChangeBatch> {
    const response = await this.#request(
      {
        type: "change",
        batches,
        ...(options.fence === undefined ? {} : { fence: options.fence }),
        ...(options.planId === undefined ? {} : { planId: options.planId }),
      },
      [],
    );
    if (response.type !== "change")
      throw new Error("unexpected engine worker response");
    return {
      state: response.state,
      changedCollections: response.changedCollections,
      acknowledgedMutationIds: response.acknowledgedMutationIds,
      transactionCount: response.transactionCount,
      operationCount: response.operationCount,
      currentlyUpToDate: response.currentlyUpToDate,
      latestCommitTimeUnixMicros: response.latestCommitTimeUnixMicros,
    };
  }

  async state(): Promise<ColumnarMountState> {
    const response = await this.#request({ type: "state" }, []);
    if (response.type !== "state")
      throw new Error("unexpected engine worker response");
    return response.state;
  }

  /**
   * Applies one mutation's wire operations optimistically. The mount's state
   * afterwards lists the mutation as pending; only a canonical echo naming it
   * (`applyChangeBatches`) or `drop` removes it.
   */
  async mutate(
    mutationId: string,
    operations: readonly Uint8Array[],
  ): Promise<{ readonly state: ColumnarMountState }> {
    const response = await this.#request(
      { type: "mutate", mutationId, operations },
      [],
    );
    if (response.type !== "mutate")
      throw new Error("unexpected engine worker response");
    return { state: response.state };
  }

  /** Unwinds one pending mutation; the survivors are refolded. */
  async drop(
    mutationId: string,
  ): Promise<{ readonly state: ColumnarMountState }> {
    const response = await this.#request({ type: "drop", mutationId }, []);
    if (response.type !== "drop")
      throw new Error("unexpected engine worker response");
    return { state: response.state };
  }

  async rows(
    table: string,
    start: number,
    count: number,
    fields: readonly string[],
    signal?: AbortSignal,
  ): Promise<MaterializedRows> {
    const response = await this.#request(
      {
        type: "rows",
        table,
        start,
        count,
        fields,
      },
      [],
      signal,
    );
    if (response.type !== "rows")
      throw new Error("unexpected engine worker response");
    return {
      table: response.table,
      start: response.start,
      fields: response.fields,
      rows: response.rows,
    };
  }

  /**
   * Runs a query in the worker, in slices between change batches and
   * optimistic writes. Aborting withdraws it there too. The completion names
   * the collection generations the result was computed at.
   */
  async query(
    specification: QuerySpecification,
    options?: QueryExecutionOptions,
    signal?: AbortSignal,
  ): Promise<QueryCompletion<QueryResult>> {
    const response = await this.#request(
      {
        type: "query",
        specification,
        ...(options === undefined ? {} : { options }),
      },
      [],
      signal,
      false,
      true,
    );
    if (response.type !== "query")
      throw new Error("unexpected engine worker response");
    return {
      result: sealQueryResult(response.result),
      generations: response.generations,
    };
  }

  /**
   * Rows `offset` to `offset + count` of a query's whole result, as `query`.
   */
  async window(
    specification: QuerySpecification,
    window: QueryWindow,
    options?: QueryWindowOptions,
    signal?: AbortSignal,
  ): Promise<QueryCompletion<QueryWindowResult>> {
    const response = await this.#request(
      {
        type: "window",
        specification,
        window,
        ...(options === undefined ? {} : { options }),
      },
      [],
      signal,
      false,
      true,
    );
    if (response.type !== "window")
      throw new Error("unexpected engine worker response");
    return {
      result: Object.freeze({
        ...sealQueryResult(response.result),
        offset: response.result.offset,
        total: response.result.total,
      }),
      generations: response.generations,
    };
  }

  /**
   * Reads every member at one state, in slices like `query`, and answers once
   * all of them hold there. Aborting withdraws the whole frame.
   */
  async frame(
    members: readonly FrameMember[],
    signal?: AbortSignal,
  ): Promise<FrameCompletion> {
    const response = await this.#request(
      { type: "frame", members },
      [],
      signal,
      false,
      true,
    );
    if (response.type !== "frame")
      throw new Error("unexpected engine worker response");
    return {
      generations: response.generations,
      results: response.results.map((outcome) => {
        if (outcome === null) return null;
        if (!outcome.ok)
          return { ok: false, error: workerError(outcome.error) };
        const sealed = sealQueryResult(outcome.result);
        return {
          ok: true,
          result:
            "offset" in outcome.result
              ? Object.freeze({
                  ...sealed,
                  offset: outcome.result.offset,
                  total: outcome.result.total,
                })
              : sealed,
        };
      }),
    };
  }

  close(): void {
    this.worker.terminate();
    this.#failAll(new Error("engine worker closed"));
  }

  #request(
    request: RequestWithoutId,
    transfer: Transferable[],
    signal?: AbortSignal,
    closeOnAbort = false,
    cancelOnAbort = false,
  ): Promise<WorkerSuccess> {
    const id = this.#nextId++;
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(new DOMException("The operation was aborted", "AbortError"));
        return;
      }
      const abort = () => {
        const pending = this.#pending.get(id);
        if (!pending) return;
        this.#pending.delete(id);
        pending.cleanup();
        pending.reject(
          new DOMException("The operation was aborted", "AbortError"),
        );
        if (closeOnAbort) this.close();
        if (cancelOnAbort)
          this.worker.postMessage({
            type: "cancel",
            target: id,
          } satisfies WorkerCancel);
      };
      const cleanup = () => signal?.removeEventListener("abort", abort);
      this.#pending.set(id, { resolve, reject, cleanup });
      signal?.addEventListener("abort", abort, { once: true });
      this.worker.postMessage({ ...request, id } as WorkerRequest, transfer);
    });
  }

  #failAll(error: Error): void {
    for (const pending of this.#pending.values()) {
      pending.cleanup();
      pending.reject(error);
    }
    this.#pending.clear();
  }
}
