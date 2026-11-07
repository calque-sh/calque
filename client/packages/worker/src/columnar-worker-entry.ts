import {
  ColumnarQueryEngine,
  MAX_QUERY_RESTARTS,
  OverlayEngine,
  artifactBase,
  receiveDirectColumnarArtifact,
  type ArtifactReceiver,
  type CanonicalBatch,
  type DirectColumnarWorkspace,
  type QueryCompletion,
  type QueryResult,
  type QueryTask,
  type QueryWindowResult,
} from "@calque/core";

import type {
  CompiledColumnarChangeBinding,
  decodeColumnarChangeBatches,
  decodeOptimisticMutation,
} from "./columnar-change.js";
import type {
  ColumnarMountState,
  FrameMember,
  FrameResult,
  WorkerArtifactChunk,
  WorkerArtifactCredit,
  WorkerArtifactEnd,
  WorkerCancel,
  WorkerRequest,
  WorkerResponse,
} from "./columnar-worker-protocol.js";
import { awaitedLater } from "./async.js";
import { RuntimeError } from "./runtime-error.js";

interface WorkerScope {
  onmessage:
    | ((
        event: MessageEvent<WorkerRequest | WorkerCancel | WorkerArtifactEnd>,
      ) => void)
    | null;
  postMessage(message: WorkerResponse | WorkerArtifactCredit): void;
}

type QueryRequest = Extract<
  WorkerRequest,
  { type: "query" | "window" | "frame" }
>;
type LiveRequest = Exclude<WorkerRequest, QueryRequest>;

/**
 * A live mount's change state. `cursor` is the whole mount's canonical
 * position. It moves only after a complete group has decoded and applied, and
 * together with the collections it describes. Applying a decoded group is one
 * synchronous call, so no `rows`, `state` or query slice can land inside it.
 */
interface ChangeState {
  readonly binding: CompiledColumnarChangeBinding;
  readonly decode: typeof decodeColumnarChangeBatches;
  readonly decodeMutation: typeof decodeOptimisticMutation;
  cursor: Uint8Array;
}

const scope = globalThis as unknown as WorkerScope;
const encoder = new TextEncoder();

/** A user-timing mark on the cold path, for a trace or a harness to read. */
function mark(name: string): void {
  globalThis.performance?.mark?.(`calque:${name}`);
}
mark("worker:evaluated");
const tableColumns = new Map<string, readonly string[]>();
let workspace: DirectColumnarWorkspace | null = null;
let overlay: OverlayEngine | null = null;
let queryEngine: ColumnarQueryEngine | null = null;
let changeState: ChangeState | null = null;
let queue = Promise.resolve();

/**
 * The artifact an `open` is receiving. Its chunks arrive from the hashers'
 * ports beside `queue`, in any order: the `open` waits in the queue until
 * every chunk the page sent has been accepted or one has been refused, and
 * nothing else is queued before an `open`.
 */
interface Receiving {
  readonly receiver: ArtifactReceiver | undefined;
  /** Why the artifact cannot open, once something has refused it. */
  failure: { readonly error: unknown } | undefined;
  readonly ports: readonly MessagePort[];
  /** How many chunks the receiver has accepted. */
  accepted: number;
  /** How many chunks the page sent, once `artifact-end` has said. */
  chunks: number | undefined;
  /** Resolved once the artifact is whole or refused. */
  readonly settled: PromiseWithResolvers<void>;
}
let receiving: Receiving | null = null;

function startReceiving(request: Extract<WorkerRequest, { type: "open" }>) {
  const settled: PromiseWithResolvers<void> = Promise.withResolvers();
  let receiver: ArtifactReceiver | undefined;
  let failure: Receiving["failure"];
  const ports = Array.isArray(request.hashers) ? request.hashers : [];
  try {
    if (
      ports.length === 0 ||
      !ports.every((port) => port instanceof MessagePort)
    )
      throw new Error("the open names no hasher ports to receive chunks on");
    receiver = receiveDirectColumnarArtifact(request.trusted, request.capacity);
  } catch (error) {
    failure = { error };
    settled.resolve();
  }
  const current: Receiving = {
    receiver,
    failure,
    ports,
    accepted: 0,
    chunks: undefined,
    settled,
  };
  receiving = current;
  if (failure) return;
  for (const port of ports)
    port.onmessage = (event: MessageEvent<WorkerArtifactChunk>) => {
      accept(current, event.data);
    };
}

function refuse(current: Receiving, error: unknown): void {
  current.failure = { error };
  current.settled.resolve();
}

/**
 * Takes one hashed chunk: the receiver compares its digest with the plan's
 * and copies it into place, and the page is credited for it.
 */
function accept(current: Receiving, chunk: WorkerArtifactChunk): void {
  if (receiving !== current || !current.receiver || current.failure) return;
  try {
    if (!(chunk.bytes instanceof ArrayBuffer))
      throw new Error("an artifact chunk arrived without its bytes");
    current.receiver.accept(
      chunk.index,
      new Uint8Array(chunk.bytes),
      chunk.sha256,
    );
  } catch (error) {
    refuse(current, error);
    return;
  }
  current.accepted += 1;
  scope.postMessage({ type: "artifact-credit", index: chunk.index });
  if (current.accepted === current.chunks) current.settled.resolve();
}

/** The page has sent every chunk: `chunks` of them. */
function end(chunks: number): void {
  const current = receiving;
  if (!current?.receiver || current.failure || current.chunks !== undefined)
    return;
  if (chunks !== current.receiver.chunks) {
    refuse(current, new Error("artifact length binding mismatch"));
    return;
  }
  current.chunks = chunks;
  if (current.accepted === chunks) current.settled.resolve();
}

/** The received artifact, opened once it is whole, or why it cannot be. */
async function received(): Promise<DirectColumnarWorkspace> {
  const current = receiving;
  if (!current) throw new Error("no artifact is arriving");
  await current.settled.promise;
  receiving = null;
  for (const port of current.ports) port.close();
  if (current.failure) throw current.failure.error;
  if (!current.receiver) throw new Error("no artifact is arriving");
  return current.receiver.open();
}

/**
 * A query executing in slices. Queries do not wait in `queue`, where a long
 * sort would hold every change batch and optimistic write behind it.
 */
interface RunningQuery {
  /** `QueryTask.step`, answering the request when the query finishes. */
  step(pause: () => boolean): WorkerResponse | undefined;
  cancel(): void;
  /** Milliseconds spent in its slices so far. */
  spent: number;
  /** Whether the page paints its answer. */
  readonly frame: boolean;
}

function runningQuery<Result>(
  task: QueryTask<Result>,
  answer: (completion: QueryCompletion<Result>) => WorkerResponse,
): RunningQuery {
  return {
    step: (pause) => {
      const completion = task.step(pause);
      return completion && answer(completion);
    },
    cancel: () => task.cancel(),
    spent: 0,
    frame: false,
  };
}

interface FrameRead {
  readonly member: FrameMember;
  /**
   * The generations its rows hold at, the page's for a member sent with
   * `seen`; `undefined` while it has none.
   */
  at: Readonly<Record<string, number>> | undefined;
  task: QueryTask<QueryResult | QueryWindowResult> | undefined;
  result: FrameResult | null;
}

/**
 * The read in flight, else the first with no rows, so that one task at a
 * time holds query pool.
 */
const nextRead = (reads: readonly FrameRead[]) =>
  reads.find((read) => read.task) ??
  reads.find((read) => !read.at && !read.result);

/**
 * Reads `members` so that every result holds at one state. A member whose
 * collections moved after it finished is read again, and after
 * `MAX_QUERY_RESTARTS` such reads the rest of the frame runs in one step.
 */
function runningFrame(
  engine: ColumnarQueryEngine,
  mount: OverlayEngine,
  id: number,
  members: readonly FrameMember[],
): RunningQuery {
  const reads: FrameRead[] = members.map((member) => ({
    member,
    at: member.seen,
    task: undefined,
    result: null,
  }));
  const holds = (at: Readonly<Record<string, number>>) =>
    Object.entries(at).every(
      ([collection, generation]) => mount.generation(collection) === generation,
    );
  let restarts = 0;
  return {
    step: (pause) => {
      for (const read of reads)
        if (read.at && !holds(read.at)) {
          if (read.result) restarts++;
          read.at = undefined;
          read.result = null;
        }
      const sliced = restarts < MAX_QUERY_RESTARTS;
      for (let read = nextRead(reads); read; read = nextRead(reads)) {
        try {
          const { member } = read;
          read.task ??=
            member.type === "query"
              ? engine.start(member.specification, member.options)
              : engine.startWindow(
                  member.specification,
                  member.window,
                  member.options,
                );
          const completion = read.task.step(sliced ? pause : () => false);
          if (!completion) return undefined;
          read.task = undefined;
          read.at = completion.generations;
          read.result = { ok: true, result: completion.result };
        } catch (error) {
          read.task = undefined;
          read.result = { ok: false, error: workerError(error) };
        }
      }
      const generations: Record<string, number> = {};
      for (const name of mount.collectionNames())
        generations[name] = mount.generation(name);
      return {
        id,
        ok: true,
        type: "frame",
        generations,
        results: reads.map((read) => read.result),
      };
    },
    cancel: () => {
      for (const read of reads) read.task?.cancel();
    },
    spent: 0,
    frame: true,
  };
}

/**
 * How long one slice of query work runs before the worker takes its next
 * message. A change batch or optimistic write waits for at most this, plus the
 * one step of work that crosses it, per slice ahead of it: the one running
 * when it arrived, and the first slice of each query admitted before it. A
 * change batch `holdForFrame` holds waits for the running frame instead, and
 * the requests queued behind it wait with it.
 */
const QUERY_SLICE_MS = 8;
/** Queries waiting in `queue` for the live requests sent before them. */
const admitting = new Set<number>();
const running = new Map<number, RunningQuery>();
let sliceScheduled = false;
let slicePort: MessagePort | undefined;
/** Optimistic writes applied since the last frame answered. */
const unanswered = new Set<string>();
let frameEnd: PromiseWithResolvers<void> | undefined;

function frameEnded(answered: boolean): void {
  if (answered) unanswered.clear();
  frameEnd?.resolve();
  frameEnd = undefined;
}

/**
 * Holds, while a frame is running, a batch that echoes a write applied since
 * the last frame answered: applied now, it would send that frame back to read
 * the confirmed state, and the page would never paint the write before
 * confirming it.
 */
async function holdForFrame(batch: CanonicalBatch): Promise<void> {
  const echoesUnanswered = () =>
    batch.transactions.some((transaction) =>
      transaction.mutationIds.some((id) => unanswered.has(id)),
    );
  while (
    echoesUnanswered() &&
    [...running.values()].some((query) => query.frame)
  ) {
    frameEnd ??= Promise.withResolvers();
    await frameEnd.promise;
  }
}

function failure(id: number, error: unknown): WorkerResponse {
  return { id, ok: false, error: workerError(error) };
}

function workerError(error: unknown) {
  const value = error instanceof Error ? error : new Error(String(error));
  const code =
    "code" in value && typeof value.code === "string" ? value.code : undefined;
  return {
    name: value.name,
    message: value.message,
    ...(code === undefined ? {} : { code }),
  };
}

function mountState(): ColumnarMountState {
  const collectionRows: Record<string, number> = {};
  const collectionGenerations: Record<string, number> = {};
  if (overlay)
    for (const name of overlay.collectionNames()) {
      collectionRows[name] = overlay.rowCount(name);
      collectionGenerations[name] = overlay.generation(name);
    }
  else if (workspace)
    for (const name of tableColumns.keys())
      collectionRows[name] = workspace.table(name).rows;
  return {
    cursor: changeState ? changeState.cursor.slice() : undefined,
    live: changeState !== null,
    collectionRows,
    collectionGenerations,
    pendingMutationIds: overlay ? [...overlay.pendingMutationIds()] : [],
  };
}

/**
 * Reads a window of rows, through the overlay whenever one exists: once a tail
 * lands, the artifact's own columns are not the published state.
 */
function materialize(
  table: string,
  start: number,
  count: number,
  fields: readonly string[],
): readonly (readonly unknown[])[] {
  const opened = workspace;
  if (!opened) throw new Error("worker workspace is not open");
  if (overlay) {
    const columns = tableColumns.get(table);
    if (!columns) throw new Error("unknown table");
    const ordinals = fields.map((field) => {
      const ordinal = columns.indexOf(field);
      if (ordinal < 0) throw new Error("unknown column");
      return ordinal;
    });
    return overlay.rows(table, start, count, ordinals);
  }
  const accessors = fields.map((field) => opened.table(table).column(field));
  return Array.from({ length: count }, (_unused, offset) =>
    accessors.map((column) => column.valueAt(start + offset)),
  );
}

async function handle(request: LiveRequest): Promise<WorkerResponse> {
  if (request.type === "open") {
    if (workspace) throw new Error("worker workspace is already open");
    mark("open:received");
    // Loaded only for a live mount, because the decoder pulls in the protobuf
    // runtime. It loads while the artifact is verified, not after.
    const changeModule = request.changeStream
      ? awaitedLater(import("./columnar-change.js"))
      : undefined;
    const opened = await received();
    workspace = opened;
    for (const table of request.trusted.schema.tables)
      tableColumns.set(
        table.name,
        table.columns.map((column) => column.name),
      );
    if (request.queryCapacityBytes !== undefined || request.changeStream)
      overlay = new OverlayEngine(
        request.trusted.schema.tables.map((schema) => {
          const table = opened.table(schema.name);
          const columns = schema.columns.map((column) => column.name);
          return {
            name: schema.name,
            columns,
            base: artifactBase(table, columns),
            // Checked by `parseDirectColumnarSchema` and matched against the
            // manifest's expected schema when the artifact opened.
            primaryKey: schema.primaryKey,
          };
        }),
      );
    if (request.queryCapacityBytes !== undefined && overlay)
      queryEngine = new ColumnarQueryEngine(overlay, {
        capacityBytes: request.queryCapacityBytes,
      });
    mark("open:engine");
    if (request.changeStream) {
      // The cursor is not empty: `receiveDirectColumnarArtifact` refused an
      // identity without one before the first chunk arrived.
      const module = await (changeModule ?? import("./columnar-change.js"));
      mark("open:change-module");
      changeState = {
        binding: module.compileColumnarChangeBinding(
          request.trusted.schema,
          request.changeStream,
        ),
        decode: module.decodeColumnarChangeBatches,
        decodeMutation: module.decodeOptimisticMutation,
        cursor: encoder.encode(request.trusted.identity.cursor),
      };
    }
    const tableRows = Object.fromEntries(
      request.trusted.schema.tables.map((table) => [
        table.name,
        opened.table(table.name).rows,
      ]),
    );
    return {
      id: request.id,
      ok: true,
      type: "open",
      rowCount: opened.rowCount,
      artifactByteLength: opened.artifactByteLength,
      tableRows,
      state: mountState(),
    };
  }
  if (!workspace) throw new Error("worker workspace is not open");
  if (request.type === "state")
    return { id: request.id, ok: true, type: "state", state: mountState() };
  if (request.type === "change") {
    if (!changeState || !overlay)
      throw new RuntimeError(
        "INVALID_LIFECYCLE",
        "this mount was opened without a live change stream",
      );
    // Every batch of the group decodes and validates before the first plane
    // is touched, so a rejected group leaves the mount where it was and an
    // accepted one is one publication.
    const decoded = await changeState.decode(
      request.batches,
      changeState.binding,
      changeState.cursor,
      request.fence,
    );
    await holdForFrame(decoded.batch);
    const applied = overlay.applyCanonical(decoded.batch);
    changeState.cursor = decoded.throughCursorInclusive;
    for (const id of applied.acknowledgedMutationIds) unanswered.delete(id);
    return {
      id: request.id,
      ok: true,
      type: "change",
      state: mountState(),
      changedCollections: applied.changedCollections,
      acknowledgedMutationIds: applied.acknowledgedMutationIds,
      transactionCount: decoded.transactionCount,
      operationCount: decoded.operationCount,
      currentlyUpToDate: decoded.currentlyUpToDate,
      latestCommitTimeUnixMicros: decoded.latestCommitTimeUnixMicros,
    };
  }
  if (request.type === "mutate" || request.type === "drop") {
    // A frozen mount has no cursor to submit against and no echo to confirm
    // with, so it refuses optimistic writes as it refuses a change.
    if (!changeState || !overlay)
      throw new RuntimeError(
        "INVALID_LIFECYCLE",
        "this mount was opened without a live change stream",
      );
    if (request.type === "mutate") {
      overlay.applyOptimistic(
        changeState.decodeMutation(
          request.mutationId,
          request.operations,
          changeState.binding,
        ),
      );
      unanswered.add(request.mutationId);
    } else {
      overlay.dropMutation(request.mutationId);
      unanswered.delete(request.mutationId);
    }
    return {
      id: request.id,
      ok: true,
      type: request.type,
      state: mountState(),
    };
  }
  if (
    !Number.isSafeInteger(request.start) ||
    request.start < 0 ||
    !Number.isSafeInteger(request.count) ||
    request.count < 0 ||
    request.fields.length === 0
  )
    throw new RangeError("invalid materialized row window");
  const rowsAvailable = overlay
    ? overlay.rowCount(request.table)
    : workspace.table(request.table).rows;
  // The caller bounds its window by the row count it last saw published, and
  // a delete applied since then shortens the table under it: the window then
  // ends at the table's end, as a short read.
  const count = Math.max(
    0,
    Math.min(request.count, rowsAvailable - request.start),
  );
  return {
    id: request.id,
    ok: true,
    type: "rows",
    table: request.table,
    start: request.start,
    fields: [...request.fields],
    rows: materialize(request.table, request.start, count, request.fields),
  };
}

/**
 * Starts a query once every live request sent before it has been handled, so
 * it sees at least the state those left, and runs its first slice before the
 * queue takes the next live request. A query that fits in one slice is
 * answered at the state it was admitted to; a longer one runs its remaining
 * slices beside the queue.
 */
function admit(request: QueryRequest): void {
  if (!admitting.delete(request.id)) return;
  let query: RunningQuery;
  try {
    if (!queryEngine || !overlay)
      throw new Error("worker was opened without a query capacity pool");
    const { id } = request;
    query =
      request.type === "frame"
        ? runningFrame(queryEngine, overlay, id, request.members)
        : request.type === "query"
          ? runningQuery(
              queryEngine.start(request.specification, request.options),
              ({ result, generations }) => ({
                id,
                ok: true,
                type: "query",
                result,
                generations,
              }),
            )
          : runningQuery(
              queryEngine.startWindow(
                request.specification,
                request.window,
                request.options,
              ),
              ({ result, generations }) => ({
                id,
                ok: true,
                type: "window",
                result,
                generations,
              }),
            );
  } catch (error) {
    scope.postMessage(failure(request.id, error));
    return;
  }
  running.set(request.id, query);
  slice(request.id, query);
  scheduleSlice();
}

function withdraw(target: number): void {
  if (admitting.delete(target)) return;
  const query = running.get(target);
  if (!query) return;
  query.cancel();
  running.delete(target);
  if (query.frame) frameEnded(false);
}

/**
 * Runs the next slice in a task of its own, after whatever messages arrived
 * meanwhile. In a worker that is a message to itself, which shares one task
 * queue with incoming requests; a nested timer would be clamped to 4 ms. Node,
 * where the unit tests run, drains a port's messages without running timers in
 * between, so there it is `setImmediate`.
 */
function scheduleSlice(): void {
  if (sliceScheduled || running.size === 0) return;
  sliceScheduled = true;
  const { setImmediate } = globalThis as {
    setImmediate?: (callback: () => void) => unknown;
  };
  if (setImmediate) {
    setImmediate(runSlice);
    return;
  }
  if (!slicePort) {
    const channel = new MessageChannel();
    channel.port1.onmessage = runSlice;
    slicePort = channel.port2;
  }
  slicePort.postMessage(null);
}

/**
 * One slice of the query that has had the least time so far, so a query
 * that outlived its first slice does not starve one that outlived its own
 * behind it.
 */
function runSlice(): void {
  sliceScheduled = false;
  let next: [number, RunningQuery] | undefined;
  for (const entry of running)
    if (!next || entry[1].spent < next[1].spent) next = entry;
  if (!next) return;
  slice(...next);
  scheduleSlice();
}

/** Runs `query` for one slice, and answers it if it finished. */
function slice(id: number, query: RunningQuery): void {
  const started = performance.now();
  const deadline = started + QUERY_SLICE_MS;
  try {
    const answer = query.step(() => performance.now() >= deadline);
    if (answer) {
      running.delete(id);
      scope.postMessage(answer);
      if (query.frame) frameEnded(true);
    }
  } catch (error) {
    running.delete(id);
    scope.postMessage(failure(id, error));
    if (query.frame) frameEnded(false);
  }
  query.spent += performance.now() - started;
}

scope.onmessage = (event) => {
  const request = event.data;
  if (request.type === "cancel") {
    withdraw(request.target);
    return;
  }
  if (request.type === "artifact-end") {
    end(request.chunks);
    return;
  }
  if (request.type === "open" && !workspace && !receiving)
    startReceiving(request);
  if (
    request.type === "query" ||
    request.type === "window" ||
    request.type === "frame"
  ) {
    admitting.add(request.id);
    queue = queue.then(() => {
      try {
        admit(request);
      } catch (error) {
        withdraw(request.id);
        scope.postMessage(failure(request.id, error));
      }
    });
    return;
  }
  queue = queue.then(async () => {
    try {
      scope.postMessage(await handle(request));
    } catch (error) {
      scope.postMessage(failure(request.id, error));
    }
  });
};
