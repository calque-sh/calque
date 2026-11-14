/**
 * The fakes and builders the columnar session's tests share: a worker, hashers,
 * plans, artifact and change responses, and clients wired to them.
 */
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { type QuerySpecification } from "@calque/core";
import {
  change,
  encodeFrame,
  mutation,
  validateChangeRequest,
  validateMutationBatchRequest,
} from "@calque/protocol";
import { onTestFinished, vi } from "vitest";

import {
  createInternalColumnarClient,
  type SyncAuth,
  type SyncClient,
  type SyncPublication,
} from "../client.js";
import { stubBrowserFeatures } from "../browser.fixture.js";
import { createGeneratedClientSchema } from "../client-options.js";
import type { CollectionShape } from "../facade/types.js";
import type { GeneratedClientSchemaDescriptor } from "../generated-schema.js";
import type { LiveQuerySnapshot } from "../resource.js";
import type { ColumnarEngineWorker, ColumnarHasherWorker } from "./worker.js";

export interface EventRow {
  readonly id: bigint;
  readonly level: string;
}
export interface IssueRow {
  readonly id: bigint;
  readonly title: string | null;
}
export type Collections = {
  readonly events: CollectionShape<
    EventRow,
    { readonly id: bigint },
    EventRow,
    { readonly level?: string },
    true
  >;
  readonly issues: CollectionShape<IssueRow, { readonly id: bigint }>;
};

export interface WorkerRequest {
  readonly id: number;
  readonly type: string;
  readonly [key: string]: unknown;
}

export const encoder = new TextEncoder();
export const decoder = new TextDecoder();
/** The cursor a mount opens on, which is also a mutation's base cursor. */
export const MOUNT_CURSOR = encoder.encode("cursor-p");

export class FakeWorker extends EventTarget {
  terminated = false;
  readonly requests: WorkerRequest[] = [];
  readonly held: WorkerRequest[] = [];
  holdQueries = false;
  /**
   * Holds only this collection's queries, so a test can let one collection's
   * read land while another's is still in the worker; otherwise every
   * response settles on the same microtask turn.
   */
  holdQueriesFor: string | undefined;
  /**
   * The cursor the mount reports at open: the artifact's, unless a test moves
   * it.
   */
  openCursor = "cursor-p";
  /**
   * What a `change` request lands the mount on, or the failure it reports.
   * A refusal carries the code the real engine would, because the session
   * routes `RESNAPSHOT_REQUIRED` differently from every other refusal.
   */
  changeOutcome: { cursor: string } | { error: string; code?: string } = {
    cursor: "cursor-p",
  };
  /** The overlay's pending mutation ids, in application order. */
  readonly pendingIds: string[] = [];
  /** What the next applied change batch acknowledges. */
  echo: "pending" | readonly string[] = "pending";
  generation = 1;
  /**
   * What a query answers with. One mount's rows, so a test that replaces the
   * worker can tell the two mounts apart rather than assuming they differ.
   */
  queryRows: readonly (readonly unknown[])[] = [[1n, "error"]];
  /**
   * Per-collection query answers, for a test that needs the two collections to
   * hold different rows and to move together. A collection absent here falls
   * back to `queryRows`.
   */
  readonly queryAnswers = new Map<
    string,
    { columns: readonly string[]; rows: readonly (readonly unknown[])[] }
  >();
  /** Which collections a change batch reports, and whose generation it moves. */
  changedCollections: readonly string[] = ["events"];
  /** `issues`' generation, moved only by a batch that says it changed. */
  issuesGeneration = 1;
  /**
   * Run when a change batch is applied, before the response is built: where a
   * test makes the source rows of every collection in the batch move at once,
   * the way one committed transaction moves them.
   */
  onChange: (() => void) | undefined;
  /** Why a query is refused, as the real worker would report it. */
  queryError: { name: string; message: string; code?: string } | undefined;
  /** Why a whole frame fails, before any of its members is read. */
  frameError: { name: string; message: string } | undefined;
  /** What a `rows` window answers with, on the same per-mount rule. */
  sliceRows: readonly (readonly unknown[])[] = [[2n, "warn"]];
  /** Where the mount stands: `openCursor` until a change batch moves it. */
  #cursor: string | undefined;
  /** The chunks the hashers passed the current `open`, by index. */
  readonly received: Uint8Array[] = [];
  /** The `open`, answered once every chunk has arrived and the end has. */
  #receiving: WorkerRequest | undefined;
  /** The ports the current `open` takes its chunks from. */
  #hasherPorts: MessagePort[] = [];
  #accepted = 0;
  /** The chunk count the page's end named, once it has sent one. */
  #ended: number | undefined;
  /**
   * Leaves the chunks that arrive uncredited, the way an engine worker that
   * has stalled does, until `releaseCredits`.
   */
  holdCredits = false;
  readonly #uncredited: number[] = [];
  /** Why the next `open` is refused, as the real worker would say it. */
  openError: string | undefined;
  /**
   * A chunk the worker refuses as it arrives, answering the open with
   * `openError` at once, the way the real worker refuses a digest that is
   * not the plan's.
   */
  refuseChunk: number | undefined;
  /**
   * Refuses a chunk whose bytes fail this check, as `refuseChunk` refuses
   * one by index: where a test gives the fake the comparison the real
   * worker makes between a chunk's digest and the plan's.
   */
  verifyChunk: ((index: number, bytes: Uint8Array) => boolean) | undefined;
  /**
   * The generations each held query was posted at. A held query stands for
   * one the worker computed then and answers late, so it answers at them.
   */
  readonly #heldAt = new Map<WorkerRequest, Record<string, number>>();

  #generations(): Record<string, number> {
    return { events: this.generation, issues: this.issuesGeneration };
  }

  /**
   * The mount state every response carries, so a pending id applied by
   * `mutate` is visible to the next `change` poll and to the status the
   * session reads.
   */
  #state(live: boolean, rows: number) {
    return {
      cursor: encoder.encode(this.#cursor ?? this.openCursor),
      live,
      collectionRows: { events: rows, issues: 1 },
      collectionGenerations: {
        events: this.generation,
        issues: this.issuesGeneration,
      },
      pendingMutationIds: [...this.pendingIds],
    };
  }

  postMessage(message: WorkerRequest, transfer: Transferable[] = []) {
    const cloned = structuredClone(message, { transfer }) as WorkerRequest;
    this.requests.push(cloned);
    if (cloned.type === "artifact-end") {
      this.#ended = cloned.chunks as number;
      this.#openOnceWhole();
      return;
    }
    if (cloned.type === "open") {
      this.#receiving = cloned;
      this.received.length = 0;
      this.#accepted = 0;
      this.#ended = undefined;
      this.#hasherPorts = cloned.hashers as MessagePort[];
      for (const port of this.#hasherPorts)
        port.onmessage = (
          event: MessageEvent<{ index: number; bytes: ArrayBuffer }>,
        ) => {
          this.#chunk(event.data.index, event.data.bytes);
        };
      return;
    }
    if (
      (cloned.type === "query" || cloned.type === "frame") &&
      this.#holds(cloned)
    ) {
      this.held.push(cloned);
      this.#heldAt.set(cloned, this.#generations());
      return;
    }
    queueMicrotask(() => this.answer(cloned));
  }

  #chunk(index: number, bytes: ArrayBuffer): void {
    const open = this.#receiving;
    if (!open) return;
    if (
      index === this.refuseChunk ||
      this.verifyChunk?.(index, new Uint8Array(bytes)) === false
    ) {
      this.#receiving = undefined;
      this.#closeHasherPorts();
      this.respond({
        id: open.id,
        ok: false,
        error: {
          name: "Error",
          message: this.openError ?? "artifact digest binding mismatch",
        },
      });
      return;
    }
    this.received[index] = new Uint8Array(bytes);
    this.#accepted += 1;
    if (this.holdCredits) this.#uncredited.push(index);
    else this.respond({ type: "artifact-credit", index });
    this.#openOnceWhole();
  }

  #openOnceWhole(): void {
    const open = this.#receiving;
    if (!open || this.#ended === undefined || this.#accepted < this.#ended)
      return;
    this.#receiving = undefined;
    this.#closeHasherPorts();
    queueMicrotask(() => this.answer(open));
  }

  /** How many chunks the current `open` has taken. */
  get accepted(): number {
    return this.#accepted;
  }

  /** Credits every chunk held uncredited, and stops holding. */
  releaseCredits(): void {
    this.holdCredits = false;
    for (const index of this.#uncredited.splice(0))
      this.respond({ type: "artifact-credit", index });
  }

  #closeHasherPorts(): void {
    for (const port of this.#hasherPorts.splice(0)) port.close();
  }

  #holds(request: WorkerRequest): boolean {
    if (this.holdQueries) return true;
    if (this.holdQueriesFor === undefined) return false;
    return queriedCollections(request).includes(this.holdQueriesFor);
  }

  /** Answers every held query, in the order the session posted them. */
  releaseHeld() {
    const held = this.held.splice(0, this.held.length);
    for (const request of held) this.answer(request);
  }

  /**
   * Answers only one collection's held queries and leaves the rest held, so
   * one read can land while the other is still in the worker.
   */
  releaseHeldFor(collection: string) {
    const released: WorkerRequest[] = [];
    const kept: WorkerRequest[] = [];
    for (const request of this.held)
      (queriedCollections(request).includes(collection) ? released : kept).push(
        request,
      );
    if (released.length === 0)
      throw new Error(`no held query for ${collection}`);
    this.held.splice(0, this.held.length, ...kept);
    for (const request of released) this.answer(request);
  }

  terminate() {
    this.terminated = true;
    this.#closeHasherPorts();
  }

  answer(request: WorkerRequest) {
    if (request.type === "open" && this.openError !== undefined)
      this.respond({
        id: request.id,
        ok: false,
        error: { name: "Error", message: this.openError },
      });
    else if (request.type === "open") {
      this.#cursor = this.openCursor;
      this.respond({
        id: request.id,
        ok: true,
        type: "open",
        rowCount: 4,
        artifactByteLength: 16,
        tableRows: { events: 3, issues: 1 },
        state: this.#state(false, 3),
      });
    } else if (request.type === "mutate") {
      this.pendingIds.push(request.mutationId as string);
      this.generation += 1;
      this.respond({
        id: request.id,
        ok: true,
        type: "mutate",
        state: this.#state(false, 3),
      });
    } else if (request.type === "drop") {
      const index = this.pendingIds.indexOf(request.mutationId as string);
      // The overlay refuses to drop what it does not hold, and an echo that
      // named the mutation has already removed it.
      if (index < 0) {
        this.respond({
          id: request.id,
          ok: false,
          error: {
            name: "OverlayError",
            message: "mutation is not pending",
            code: "INVALID_REQUEST",
          },
        });
        return;
      }
      this.pendingIds.splice(index, 1);
      this.generation += 1;
      this.respond({
        id: request.id,
        ok: true,
        type: "drop",
        state: this.#state(false, 3),
      });
    } else if (request.type === "change") {
      if ("error" in this.changeOutcome) {
        this.respond({
          id: request.id,
          ok: false,
          error: {
            name: "RuntimeError",
            message: this.changeOutcome.error,
            code: this.changeOutcome.code ?? "CHANGE_STREAM_INVALID",
          },
        });
        return;
      }
      this.onChange?.();
      const acknowledged =
        this.echo === "pending" ? [...this.pendingIds] : [...this.echo];
      for (const id of acknowledged) {
        const index = this.pendingIds.indexOf(id);
        if (index >= 0) this.pendingIds.splice(index, 1);
      }
      if (this.changedCollections.includes("events")) this.generation += 1;
      if (this.changedCollections.includes("issues"))
        this.issuesGeneration += 1;
      this.#cursor = this.changeOutcome.cursor;
      this.respond({
        id: request.id,
        ok: true,
        type: "change",
        state: this.#state(true, 4),
        changedCollections: [...this.changedCollections],
        acknowledgedMutationIds: acknowledged,
        transactionCount: 1,
        operationCount: 1,
        currentlyUpToDate: true,
        latestCommitTimeUnixMicros: 1_700_000_000_000_000n,
      });
    } else if (request.type === "query" && this.queryError) {
      this.respond({ id: request.id, ok: false, error: this.queryError });
    } else if (request.type === "query") {
      const generations = this.#heldAt.get(request) ?? this.#generations();
      this.#heldAt.delete(request);
      this.respond({
        id: request.id,
        ok: true,
        type: "query",
        result: this.#queryResult(request),
        generations,
      });
    } else if (request.type === "window") {
      this.respond({
        id: request.id,
        ok: true,
        type: "window",
        result: this.#windowResult(request),
        generations: this.#generations(),
      });
    } else if (request.type === "frame" && this.frameError) {
      this.respond({ id: request.id, ok: false, error: this.frameError });
    } else if (request.type === "frame") {
      const generations = this.#generations();
      this.respond({
        id: request.id,
        ok: true,
        type: "frame",
        generations,
        results: (request.members as WorkerRequest[]).map((member) => {
          const seen = member.seen as Record<string, number> | undefined;
          if (
            seen &&
            Object.entries(seen).every(
              ([collection, generation]) =>
                generations[collection] === generation,
            )
          )
            return null;
          if (this.queryError) return { ok: false, error: this.queryError };
          return {
            ok: true,
            result:
              member.type === "window"
                ? this.#windowResult(member)
                : this.#queryResult(member),
          };
        }),
      });
    } else if (request.type === "rows")
      this.respond({
        id: request.id,
        ok: true,
        type: "rows",
        table: request.table,
        start: request.start,
        fields: request.fields,
        rows: this.sliceRows,
      });
    else
      this.respond({
        id: request.id,
        ok: false,
        error: { name: "Error", message: `unexpected ${request.type}` },
      });
  }

  #queryResult(request: WorkerRequest) {
    const collection = queriedCollections(request)[0];
    const answer =
      collection === undefined ? undefined : this.queryAnswers.get(collection);
    return answer ?? { columns: ["id", "level"], rows: this.queryRows };
  }

  #windowResult(request: WorkerRequest) {
    const { offset, count } = request.window as {
      offset: number;
      count: number;
    };
    return {
      columns: ["id", "level"],
      rows: this.queryRows.slice(offset, offset + count),
      offset,
      total: this.queryRows.length,
    };
  }

  respond(response: unknown) {
    this.dispatchEvent(new MessageEvent("message", { data: response }));
  }
}

/**
 * The digest a fake hasher passes on with each chunk. The fake engine worker
 * compares none; the real worker's comparison is its own package's test.
 */
export const HASHED_DIGEST = `sha256:${"f".repeat(64)}`;

/**
 * A hasher worker as the session drives it: it passes each chunk it is sent
 * on to the engine worker's port, transferred, the way the real one does.
 */
export class FakeHasher extends EventTarget {
  terminated = false;
  /** The chunk indices the page sent it, in order. */
  readonly chunks: number[] = [];
  /** Keeps the chunks it is sent rather than passing them on. */
  keepChunks = false;
  #port: MessagePort | undefined;

  postMessage(
    message: { readonly type: string; readonly [key: string]: unknown },
    transfer: Transferable[] = [],
  ) {
    const cloned = structuredClone(message, { transfer });
    if (cloned.type === "port") {
      this.#port = cloned.port as MessagePort;
      return;
    }
    const index = cloned.index as number;
    const bytes = cloned.bytes as ArrayBuffer;
    this.chunks.push(index);
    if (this.keepChunks) return;
    this.#port?.postMessage({ index, bytes, sha256: HASHED_DIGEST }, [bytes]);
  }

  terminate() {
    this.terminated = true;
    this.#port?.close();
  }

  /** Reports what a real hasher's `error` event would. */
  crash(message: string) {
    this.dispatchEvent(Object.assign(new Event("error"), { message }));
  }
}

/** A hasher factory that keeps every hasher it spawns, in order. */
export function fakeHashers() {
  const spawned: FakeHasher[] = [];
  const create = vi.fn(() => {
    const hasher = new FakeHasher();
    spawned.push(hasher);
    return hasher as unknown as ColumnarHasherWorker;
  });
  return { spawned, create };
}

/** The collection a query request reads, when it reads one directly. */
/** The collections a query, window or frame request reads from. */
export function queriedCollections(request: WorkerRequest): string[] {
  if (request.type === "frame")
    return (request.members as WorkerRequest[]).flatMap(queriedCollections);
  const source = (request.specification as QuerySpecification).from;
  return "collection" in source ? [source.collection] : [];
}

export const descriptor: GeneratedClientSchemaDescriptor = {
  collections: [
    {
      id: "events",
      tableId: "public.events",
      columns: [
        { name: "id", kind: "int64" },
        { name: "level", kind: "text" },
      ],
      primaryKey: ["id"],
      writes: {
        operations: ["insert", "update", "delete"],
        writableColumns: ["level"],
      },
    },
    {
      id: "issues",
      tableId: "public.issues",
      columns: [
        { name: "id", kind: "int64" },
        { name: "title", kind: "text" },
      ],
      primaryKey: ["id"],
    },
  ],
};

export const ARTIFACT_BYTES = 16;
/** The one digest a plan names for an artifact under 4 MiB. */
export const CHUNK_DIGEST = `sha256:${"5".repeat(64)}`;

export function plan(): Record<string, unknown> {
  const table = (tableId: string, localTable: string) => ({
    tableId,
    sourceSchema: "public",
    sourceTable: localTable,
    localTable,
    schemaHash: `sha256:${"1".repeat(64)}`,
    columnProfileHash: `sha256:${"2".repeat(64)}`,
    rowCount: 0,
    primaryKeyCodec: "pk-v1/protobuf",
    columns: [
      { columnId: 1, name: "id" },
      { columnId: 2, name: localTable === "events" ? "level" : "title" },
    ],
  });
  return {
    protocolVersion: 1,
    planId: "plan-1",
    expiresAt: "2099-01-01T00:00:00Z",
    source: {
      sourceEpoch: "epoch-1",
      publicationGeneration: 1,
      schemaGeneration: 1,
      authorizationGeneration: 1,
      authorizationPolicyHash: `sha256:${"9".repeat(64)}`,
    },
    authorization: {
      policyHash: `sha256:${"9".repeat(64)}`,
      scopeHash: `sha256:${"a".repeat(64)}`,
      scopeToken: "scope-secret",
    },
    requiredTables: ["public.events", "public.issues"],
    residentTables: [],
    activationCursor: "cursor-p",
    nextResidentStateProof: "proof-1",
    artifacts: [
      {
        artifactId: "artifact-1",
        attachAs: "stage_0000",
        snapshotCursor: "cursor-p",
        tables: ["public.events", "public.issues"],
        decodedSha256: "b".repeat(64),
        decodedBytes: ARTIFACT_BYTES,
        bufferCapacityBytes: ARTIFACT_BYTES,
        installOrder: 0,
        runtime: {
          format: "directColumnar",
          formatVersion: 4,
          tableSetHash: `sha256:${"c".repeat(64)}`,
          shardProfileHash: `sha256:${"d".repeat(64)}`,
          indexProfileHash: `sha256:${"e".repeat(64)}`,
          schemaHash: `sha256:${"3".repeat(64)}`,
          collectionSetHash: `sha256:${"4".repeat(64)}`,
          identity: {
            manifestVersion: "manifest-1",
            authorizationScopeVersion: "scope-1",
          },
          chunks: { bytes: 4 * 1024 * 1024, sha256: [CHUNK_DIGEST] },
          schema: {
            format: "calque-direct-columnar",
            version: 4,
            tables: [
              {
                name: "events",
                primaryKey: ["id"],
                columns: [
                  { name: "id", type: "int64", nullable: false },
                  { name: "level", type: "dict", nullable: false },
                ],
              },
              {
                name: "issues",
                primaryKey: ["id"],
                columns: [
                  { name: "id", type: "int64", nullable: false },
                  { name: "title", type: "utf8", nullable: true },
                ],
              },
            ],
          },
          tables: [
            table("public.events", "events"),
            table("public.issues", "issues"),
          ],
        },
        selectedRepresentation: {
          representationId: `sha256:${"b".repeat(64)}`,
          encoding: "identity",
          encodedBytes: ARTIFACT_BYTES,
          downloadUrl: "https://sync.test/artifact-1",
        },
      },
    ],
    changeStream: {
      endpoint: "https://sync.test/change",
      wireProtocol: "sync.change.v1",
      fromCursorExclusive: "cursor-0",
      throughCursorInclusive: "cursor-p",
      longPollTimeoutMs: 1_000,
      maxBatchBytes: 1024 * 1024,
    },
    memory: {
      minimumSafeBudgetBytes: 16 * 1024 * 1024,
      stagingOverlapBytes: 1024 * 1024,
      queryResultReserveBytes: 4 * 1024 * 1024,
      catchUpReserveBytes: 2 * 1024 * 1024,
      clientHeapReserveBytes: 4 * 1024 * 1024,
    },
  };
}

export function byteStream(bytes: Uint8Array): ReadableStream<Uint8Array> {
  let sent = false;
  return new ReadableStream({
    type: "bytes",
    pull(controller) {
      if (sent) {
        controller.close();
        controller.byobRequest?.respond(0);
        return;
      }
      sent = true;
      controller.enqueue(bytes.slice());
    },
  });
}

/**
 * One batch the change stream can cut from a response: a lone `BatchEnd`
 * frame. The fake worker never decodes it.
 */
export function opaqueBatch(): Uint8Array<ArrayBuffer> {
  const frame = encodeFrame(
    change.ChangeStreamFrameSchema,
    create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: { case: "batchEnd", value: create(change.BatchEndSchema) },
    }),
  );
  return new Uint8Array(frame);
}

/**
 * A long-poll that never answers and, like a real fetch, reports its abort on
 * a later macrotask rather than on the abort's own microtask queue.
 */
export function abortingLongPoll(
  _input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  return new Promise((_resolve, reject) => {
    init?.signal?.addEventListener(
      "abort",
      () =>
        setTimeout(() => reject(new DOMException("aborted", "AbortError")), 30),
      { once: true },
    );
  });
}

/**
 * A change endpoint that answers its first `count` polls with one opaque
 * batch each, each only once the test releases it, and long-polls forever
 * after that.
 */
export function releasableChangePolls(count: number) {
  const releases: (() => void)[] = [];
  const bodies: Uint8Array[] = [];
  const changeFetch: typeof fetch = (input, init) => {
    const body = init?.body;
    if (body instanceof Uint8Array) bodies.push(body);
    if (releases.length >= count) return abortingLongPoll(input, init);
    return new Promise((resolve, reject) => {
      releases.push(() => resolve(new Response(opaqueBatch())));
      init?.signal?.addEventListener(
        "abort",
        () =>
          setTimeout(
            () => reject(new DOMException("aborted", "AbortError")),
            30,
          ),
        { once: true },
      );
    });
  };
  return {
    changeFetch,
    bodies,
    release: (index: number) => {
      const release = releases[index];
      if (!release) throw new Error(`no change poll ${index} to release`);
      release();
    },
    get polls() {
      return releases.length;
    },
  };
}

/**
 * A long-poll the test answers by hand. Each request is decoded and kept, so
 * a test can see which pending ids a poll carried and which poll the nudge
 * restarted.
 */
export function controllableLongPoll() {
  const waiting: {
    resolve: (response: Response) => void;
    reject: (reason: unknown) => void;
  }[] = [];
  const requests: change.ChangeRequest[] = [];
  const fetcher: typeof fetch = (_input, init) =>
    new Promise((resolve, reject) => {
      requests.push(
        fromBinary(
          change.ChangeRequestSchema,
          new Uint8Array(init?.body as ArrayBuffer),
        ),
      );
      const entry = { resolve, reject };
      waiting.push(entry);
      init?.signal?.addEventListener(
        "abort",
        () => {
          const index = waiting.indexOf(entry);
          if (index >= 0) waiting.splice(index, 1);
          setTimeout(
            () => reject(new DOMException("aborted", "AbortError")),
            30,
          );
        },
        { once: true },
      );
    });
  return {
    fetcher,
    requests,
    get open() {
      return waiting.length;
    },
    /** Answers the oldest open poll with an opaque non-empty batch. */
    answer() {
      const entry = waiting.shift();
      if (!entry) throw new Error("no poll is waiting");
      entry.resolve(
        new Response(opaqueBatch(), {
          status: 200,
          headers: { "content-type": "application/octet-stream" },
        }),
      );
    },
  };
}

export function decodeChangeRequest(body: Uint8Array) {
  const request = fromBinary(change.ChangeRequestSchema, body);
  return {
    from: decoder.decode(
      request.fromCursorExclusive?.token ?? new Uint8Array(),
    ),
    through: request.throughCursorInclusive
      ? decoder.decode(request.throughCursorInclusive.token)
      : undefined,
    tables: request.tables.map((table) => ({
      tableId: table.tableId,
      snapshotCursor: table.shardSnapshotCursor
        ? decoder.decode(table.shardSnapshotCursor.token)
        : undefined,
    })),
  };
}

/** The fixture plan whose artifact was cut behind the activation cursor. */
export function fencedPlan(): Record<string, unknown> {
  const value = plan();
  const artifact = (value.artifacts as Record<string, unknown>[])[0];
  if (!artifact) throw new Error("fixture artifact missing");
  artifact.snapshotCursor = "cursor-a";
  (value.changeStream as Record<string, unknown>).fromCursorExclusive =
    "cursor-a";
  return value;
}

export type OutcomePatch = {
  readonly conflict?: mutation.Conflict;
  readonly retryAfterMs?: bigint;
};

/** Answers `/v1/mutations` with one outcome for the submitted mutation id. */
export function outcomeResponder(
  code: mutation.MutationOutcomeCode,
  patch: OutcomePatch = {},
): typeof fetch {
  return async (_input, init) => {
    const request = fromBinary(
      mutation.MutationBatchRequestSchema,
      new Uint8Array(init?.body as ArrayBuffer),
    );
    const mutationId = request.mutations[0]?.mutationId ?? "";
    const canonical = code === mutation.MutationOutcomeCode.ALREADY_CANONICAL;
    const accepted =
      canonical ||
      [
        mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
        mutation.MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
      ].includes(code);
    const outcome = create(mutation.MutationOutcomeSchema, {
      mutationId,
      code,
      message: mutation.MutationOutcomeCode[code] ?? "",
      serverTransactionId: accepted ? new Uint8Array(16) : new Uint8Array(),
    });
    // The receipt's cursor: opaque to the client, and required on the wire.
    if (canonical)
      outcome.observedCanonicalCursor = create(mutation.CursorSchema, {
        token: encoder.encode("cursor-receipt"),
      });
    if (patch.conflict) outcome.conflict = patch.conflict;
    if (patch.retryAfterMs !== undefined)
      outcome.retryAfterMs = patch.retryAfterMs;
    const response = create(mutation.MutationBatchResponseSchema, {
      protocolVersion: 1,
      serverTimeUnixMicros: 1n,
      outcomes: [outcome],
    });
    return new Response(
      toBinary(mutation.MutationBatchResponseSchema, response),
      {
        status: 200,
        headers: {
          "content-type": "application/vnd.calque.mutation.v1+protobuf",
        },
      },
    );
  };
}

/**
 * `makeClient`'s `extraOptions` for a test of `retryMutation`, where the
 * client must leave an undecided write to the caller.
 */
export const NO_AUTOMATIC_RESENDS = { automaticMutationResends: 0 } as const;

export const acceptedResponder = outcomeResponder(
  mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
);

export async function until(
  condition: () => boolean,
  what: string,
): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`timed out waiting for ${what}`);
}

/**
 * `inner`, behind the protocol's own validators: every change and mutation
 * request the session sends is decoded and validated before `inner` sees it.
 * A request they refuse is rejected, and fails the running test once it
 * finishes, so a session that would retry past the refusal still fails.
 */
export function validatedFetch(inner: typeof fetch): typeof fetch {
  let refusal: unknown;
  onTestFinished(() => {
    if (refusal !== undefined) throw refusal;
  });
  return (input, init) => {
    const url = String(input instanceof Request ? input.url : input);
    const body = () => new Uint8Array(init?.body as ArrayBuffer);
    try {
      if (url.endsWith("/change"))
        validateChangeRequest(fromBinary(change.ChangeRequestSchema, body()));
      else if (url.endsWith("/v1/mutations"))
        validateMutationBatchRequest(
          fromBinary(mutation.MutationBatchRequestSchema, body()),
        );
    } catch (error) {
      refusal ??= error;
      return Promise.reject(error);
    }
    return inner(input, init);
  };
}

export function fakeFetch(
  planValue: Record<string, unknown>,
  changeFetch: typeof fetch = abortingLongPoll,
  mutationFetch: typeof fetch = acceptedResponder,
) {
  return vi.fn<typeof fetch>(async (input, init) => {
    const url = String(input instanceof Request ? input.url : input);
    if (url.endsWith("/change")) return changeFetch(input, init);
    if (url.endsWith("/v1/mutations")) return mutationFetch(input, init);
    if (url.endsWith("/v1/bootstrap"))
      return new Response(JSON.stringify(planValue), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    if (url.endsWith("/artifact-1"))
      return new Response(byteStream(new Uint8Array(ARTIFACT_BYTES)), {
        status: 200,
        headers: {
          "content-type": "application/vnd.calque.direct-columnar.v4",
          "content-length": String(ARTIFACT_BYTES),
        },
      });
    return new Response("not found", { status: 404 });
  });
}

/** The chunk size every plan names. */
export const CHUNK_BYTES = 4 * 1024 * 1024;

/** An artifact two and a half chunks long, so the last chunk is short. */
export const LONG_ARTIFACT_BYTES = 2.5 * CHUNK_BYTES + 3;

/** A plan's digests for an artifact of `count` chunks. */
export function chunkDigests(count: number): string[] {
  return Array.from(
    { length: count },
    (_unused, index) => `sha256:${index.toString(16).padStart(64, "0")}`,
  );
}

/** The fixture plan for an artifact of `bytes`. */
export function longPlan(bytes: number): Record<string, unknown> {
  const value = plan();
  const artifact = (value.artifacts as Record<string, unknown>[])[0];
  if (!artifact) throw new Error("fixture artifact missing");
  artifact.decodedBytes = bytes;
  artifact.bufferCapacityBytes = bytes;
  (artifact.selectedRepresentation as Record<string, unknown>).encodedBytes =
    bytes;
  (artifact.runtime as Record<string, unknown>).chunks = {
    bytes: CHUNK_BYTES,
    sha256: chunkDigests(Math.ceil(bytes / CHUNK_BYTES)),
  };
  return value;
}

/** Bytes that differ from their neighbours, so a misplaced one shows. */
export function patternBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  for (let index = 0; index < length; index += 1)
    bytes[index] = (index * 31) % 251;
  return bytes;
}

/** A browser that reports `threads` hardware threads. */
export function stubThreads(threads: number): void {
  stubBrowserFeatures(threads);
}

/** How many bytes a paced artifact body gives for each read the page makes. */
export const PACED_BYTES = 1024 * 1024;

/**
 * Serves `body` as the artifact, `PACED_BYTES` at a time and only as the page
 * reads, counting in `given` how many bytes it has handed over. Past
 * `gate.at` bytes it gives nothing more until `gate.held` settles. Aborting
 * the request errors the body, as a real fetch does.
 */
export function servePaced(
  body: Uint8Array,
  given: { bytes: number },
  gate?: { readonly at: number; readonly held: Promise<void> },
): (inner: typeof fetch) => typeof fetch {
  return (inner) => async (input, init) => {
    if (!String(input).endsWith("/artifact-1")) return inner(input, init);
    const signal = init?.signal;
    const stream = new ReadableStream({
      type: "bytes",
      start(controller) {
        signal?.addEventListener("abort", () => {
          controller.error(signal.reason);
        });
      },
      async pull(controller) {
        if (gate && given.bytes >= gate.at) await gate.held;
        if (given.bytes >= body.byteLength) {
          controller.close();
          controller.byobRequest?.respond(0);
          return;
        }
        const next = body.slice(given.bytes, given.bytes + PACED_BYTES);
        given.bytes += next.byteLength;
        controller.enqueue(next);
      },
    });
    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "application/vnd.calque.direct-columnar.v4",
        "content-length": String(body.byteLength),
      },
    });
  };
}

/**
 * Serves `body` as the artifact, once `held` settles, under a Content-Length
 * of `declared` bytes: a body that differs from its header is one the
 * header check cannot catch, only the count of the bytes themselves.
 */
export function serveArtifact(
  body: Uint8Array,
  held: Promise<void> = Promise.resolve(),
  declared = body.byteLength,
): (inner: typeof fetch) => typeof fetch {
  return (inner) => async (input, init) => {
    if (!String(input).endsWith("/artifact-1")) return inner(input, init);
    await held;
    return new Response(byteStream(body), {
      status: 200,
      headers: {
        "content-type": "application/vnd.calque.direct-columnar.v4",
        "content-length": String(declared),
      },
    });
  };
}

export function makeClient(
  planValue: Record<string, unknown> = plan(),
  changeFetch?: typeof fetch,
  mutationFetch?: typeof fetch,
  wrapFetch: (inner: typeof fetch) => typeof fetch = (inner) => inner,
  extraOptions: Record<string, unknown> = {},
) {
  const worker = new FakeWorker();
  const fetcher = vi.fn(
    validatedFetch(wrapFetch(fakeFetch(planValue, changeFetch, mutationFetch))),
  );
  const workerFactory = vi.fn(() => worker as unknown as ColumnarEngineWorker);
  const hashers = fakeHashers();
  const client = createInternalColumnarClient<Collections>(
    {
      endpoint: "https://sync.test/root",
      auth: () => ({ bearerToken: "token" }),
      schema: createGeneratedClientSchema<Collections>(descriptor),
      memoryBudgetBytes: 32 * 1024 * 1024,
      deviceId: "device-a",
      fetch: fetcher,
      clientHeapReserveBytes: 4 * 1024 * 1024,
      ...extraOptions,
    },
    workerFactory,
    hashers.create,
  );
  return {
    client,
    worker,
    fetcher,
    workerFactory,
    hashers: hashers.spawned,
    hasherFactory: hashers.create,
  };
}

/**
 * `makeClient` over a change endpoint the test answers by hand. `publish`
 * answers the open poll with one batch, which the fake worker applies under
 * its `changeOutcome` and `changedCollections`, and resolves once the client
 * has reported the publication to its listeners.
 */
export function makePublishingClient() {
  const poll = controllableLongPoll();
  const made = makeClient(plan(), poll.fetcher);
  let published = 0;
  made.client.subscribePublications(() => {
    published += 1;
  });
  return {
    ...made,
    poll,
    async publish(): Promise<void> {
      await until(() => poll.open > 0, "an open change poll");
      const before = published;
      poll.answer();
      await until(() => published > before, "the publication");
    },
  };
}

/**
 * A client whose every activation attempt spawns an engine worker of its own,
 * as a browser's does, served the fixture through `wrapFetch`, with an
 * `auth()` that counts the snapshots the session takes.
 */
export function makeAttemptedClient(
  wrapFetch: (inner: typeof fetch) => typeof fetch,
  planValue: Record<string, unknown> = plan(),
) {
  const workers: FakeWorker[] = [];
  const workerFactory = vi.fn(() => {
    const worker = new FakeWorker();
    workers.push(worker);
    return worker as unknown as ColumnarEngineWorker;
  });
  const auth = vi.fn(() => ({ bearerToken: "token" }));
  const fetcher = vi.fn(validatedFetch(wrapFetch(fakeFetch(planValue))));
  const hashers = fakeHashers();
  const client = createInternalColumnarClient<Collections>(
    {
      endpoint: "https://sync.test/root",
      auth,
      schema: createGeneratedClientSchema<Collections>(descriptor),
      memoryBudgetBytes: 32 * 1024 * 1024,
      deviceId: "device-a",
      fetch: fetcher,
      clientHeapReserveBytes: 4 * 1024 * 1024,
    },
    workerFactory,
    hashers.create,
  );
  /** How many requests the client sent to a URL ending in `suffix`. */
  const requests = (suffix: string): number =>
    fetcher.mock.calls.filter(([input]) => String(input).endsWith(suffix))
      .length;
  return { client, workers, auth, hashers: hashers.spawned, requests };
}

/** The fixture plan, cut at `cursor` rather than at the fixture's own. */
export function planAt(cursor: string): Record<string, unknown> {
  const value = plan();
  value.activationCursor = cursor;
  const artifact = (value.artifacts as Record<string, unknown>[])[0];
  if (!artifact) throw new Error("fixture artifact missing");
  artifact.snapshotCursor = cursor;
  const changeStream = value.changeStream as Record<string, unknown>;
  changeStream.throughCursorInclusive = cursor;
  return value;
}

/** One activation the rebuild harness serves, in order. */
export interface RebuildEpoch {
  /** The cursor this epoch's plan, artifact and mount all stand at. */
  readonly cursor: string;
  /** The marker every row this mount answers with carries. */
  readonly marker: string;
  /**
   * The rows this mount's queries answer with, where a test needs one row to
   * differ from the marker: a write the source committed before the snapshot
   * this mount was cut from.
   */
  readonly queryRows?: readonly (readonly unknown[])[];
  /** What this mount does with a change batch: move, or refuse with a code. */
  readonly change: { cursor: string } | { error: string; code: string };
}

/**
 * A client whose engine worker is minted fresh for every activation, as the
 * browser does, and whose bootstrap endpoint answers a different plan each
 * time. Every mount stands at its own cursor and answers rows carrying its
 * own marker, so a facade result naming two markers is a mixed epoch.
 */
export function makeRebuildableClient(
  epochs: readonly RebuildEpoch[],
  /**
   * Awaited before the bootstrap of the activation with this index answers,
   * so a test can watch the facades while a rebuild is in flight.
   */
  beforeBootstrap: (index: number) => Promise<void> | undefined = () =>
    undefined,
  /**
   * The application's `auth()` and the server's answer to one bootstrap,
   * where a test needs either to refuse: an application that cannot renew a
   * grant, and a server that does not accept the one it got.
   */
  overrides: {
    readonly auth?: () => SyncAuth | Promise<SyncAuth>;
    readonly bootstrap?: (index: number) => Response | undefined;
    readonly mutation?: typeof fetch;
    /**
     * Called with each artifact request's index before it is answered; a
     * test that needs the network to cut one throws from here.
     */
    readonly artifact?: (index: number) => void;
  } = {},
) {
  const workers: FakeWorker[] = [];
  /**
   * Whether the previous mount's worker was terminated when each mount sent
   * `open`, the request that lets a mount answer; `undefined` for the first.
   */
  const predecessorClosedAtOpen: (boolean | undefined)[] = [];
  let bootstraps = 0;
  let artifacts = 0;
  const poll = controllableLongPoll();
  const at = (index: number): RebuildEpoch => {
    const epoch = epochs[Math.min(index, epochs.length - 1)];
    if (!epoch) throw new Error("the rebuild harness has no epochs");
    return epoch;
  };
  const workerFactory = vi.fn(() => {
    const epoch = at(workers.length);
    const previous = workers.at(-1);
    const worker = new FakeWorker();
    worker.openCursor = epoch.cursor;
    worker.queryRows = epoch.queryRows ?? [
      [1n, epoch.marker],
      [2n, epoch.marker],
    ];
    worker.sliceRows = [[3n, epoch.marker]];
    worker.changeOutcome = epoch.change;
    const post = worker.postMessage.bind(worker);
    worker.postMessage = (
      message: WorkerRequest,
      transfer: Transferable[] = [],
    ) => {
      if (message.type === "open")
        predecessorClosedAtOpen.push(previous?.terminated);
      post(message, transfer);
    };
    workers.push(worker);
    return worker as unknown as ColumnarEngineWorker;
  });
  const serve: typeof fetch = async (input, init) => {
    const url = String(input instanceof Request ? input.url : input);
    if (url.endsWith("/v1/bootstrap")) {
      const index = bootstraps;
      bootstraps += 1;
      await beforeBootstrap(index);
      const refused = overrides.bootstrap?.(index);
      if (refused) return refused;
      const epoch = at(index);
      return new Response(JSON.stringify(planAt(epoch.cursor)), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (url.endsWith("/change")) return poll.fetcher(input, init);
    // A rebuild test that needs a write in flight needs somewhere to submit
    // it; every outcome here is ACCEPTED, so what the test observes after a
    // barrier is the session's own handling and not a server refusal.
    if (url.endsWith("/v1/mutations"))
      return (overrides.mutation ?? acceptedResponder)(input, init);
    if (url.endsWith("/artifact-1")) {
      const index = artifacts;
      artifacts += 1;
      overrides.artifact?.(index);
      return new Response(byteStream(new Uint8Array(ARTIFACT_BYTES)), {
        status: 200,
        headers: {
          "content-type": "application/vnd.calque.direct-columnar.v4",
          "content-length": String(ARTIFACT_BYTES),
        },
      });
    }
    return new Response("not found", { status: 404 });
  };
  const fetcher = vi.fn<typeof fetch>(validatedFetch(serve));
  const client = createInternalColumnarClient<Collections>(
    {
      endpoint: "https://sync.test/root",
      auth: overrides.auth ?? (() => ({ bearerToken: "token" })),
      schema: createGeneratedClientSchema<Collections>(descriptor),
      memoryBudgetBytes: 32 * 1024 * 1024,
      deviceId: "device-a",
      fetch: fetcher,
      clientHeapReserveBytes: 4 * 1024 * 1024,
    },
    workerFactory,
    fakeHashers().create,
  );
  return {
    client,
    workers,
    workerFactory,
    poll,
    predecessorClosedAtOpen,
    get bootstraps() {
      return bootstraps;
    },
    get artifacts() {
      return artifacts;
    },
  };
}

export async function settled(
  client: SyncClient<Collections>,
): Promise<SyncClient<Collections>["status"]> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const status = client.status;
    if (status.state !== "initializing") return status;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("the client did not settle");
}

/** What both tables held at one observation, as `id:value` per collection. */
export interface Frame {
  readonly events: string;
  readonly issues: string;
}

/** One frame the dashboard painted, and the cursor its batch reported. */
export interface PaintedFrame extends Frame {
  readonly cursor: string;
}

/** One collection's first row, as `id:value`, or why there is no row yet. */
export function cell<Row>(
  snapshot: LiveQuerySnapshot<readonly Row[]>,
  identity: (row: Row) => bigint,
  value: (row: Row) => string,
): string {
  if (snapshot.status !== "ready") return snapshot.status;
  const row = snapshot.result[0];
  return row === undefined ? "empty" : `${identity(row)}:${value(row)}`;
}

/**
 * An application that renders two collections, written against the public
 * surface only: the `client.db.<collection>` facades for the rows and
 * `client.subscribePublications` for the batch that moved them. It paints
 * inside the publication, after awaiting both resources, so the two tables
 * reach the screen at one cursor.
 *
 * `perCollection` is the render an application gets from the live-query
 * callbacks alone: one frame per callback, each taken while the other
 * collection may still be re-reading.
 */
export function renderTwoCollections(
  client: SyncClient<Collections>,
  painted: PaintedFrame[],
  perCollection: Frame[],
) {
  const events = client.db.events.query().orderBy("id").subscribe();
  const issues = client.db.issues.query().orderBy("id").subscribe();
  const frame = (): Frame => ({
    events: cell(
      events.getSnapshot(),
      (row) => row.id,
      (row) => row.level,
    ),
    issues: cell(
      issues.getSnapshot(),
      (row) => row.id,
      (row) => row.title ?? "null",
    ),
  });
  const stopEvents = events.subscribe(() => perCollection.push(frame()));
  const stopIssues = issues.subscribe(() => perCollection.push(frame()));
  const publications: SyncPublication[] = [];
  // Batches paint in the order the client published them, so a batch that
  // arrives mid-paint waits rather than overtaking the one on screen.
  let queue = Promise.resolve();
  const stop = client.subscribePublications((publication) => {
    publications.push(publication);
    // Taken on the listener's own turn: every live query the batch moved is
    // already re-reading when the listener runs, so these are the batch's
    // reads rather than the previous ones.
    const reads = Promise.all([events.promise, issues.promise]);
    queue = queue.then(async () => {
      await reads;
      painted.push({ cursor: publication.cursor, ...frame() });
    });
  });
  return {
    publications,
    firstRead: Promise.all([events.promise, issues.promise]),
    drained: () => queue,
    stop: () => {
      stop();
      stopEvents();
      stopIssues();
    },
  };
}
