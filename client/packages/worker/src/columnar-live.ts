/**
 * Live change delivery for a direct-columnar mount.
 *
 * One request loop against the authorized change endpoint, feeding whole
 * batches to a mount that applies them whole. Each request asks from the
 * cursor the mount reported after its last publication, so a refused batch
 * cannot advance the request and an accepted one cannot be asked for twice.
 *
 * A request asks for one open response that the server keeps writing batches
 * to. Batches that arrive while the mount applies a group are held and applied
 * together as one publication.
 *
 * A mount opened from a bootstrap plan may start behind the plan's activation
 * cursor, and the endpoint serves that stretch only to a fenced request. Given
 * an `activationFence`, the session catches up to the fence first and polls
 * live once the mount stands on it.
 */
import { create, toBinary } from "@bufbuild/protobuf";
import { equalBytes } from "@calque/core";
import {
  ChangeBatchSplitter,
  FrameDecoder,
  FrameError,
  change,
} from "@calque/protocol";

import { awaitedLater, BoundedQueue, delay, stopReading } from "./async.js";
import type {
  AppliedChangeBatch,
  ApplyChangeOptions,
  ColumnarChangeFence,
} from "./columnar-worker-client.js";
import { RuntimeError } from "./runtime-error.js";

const CHANGE_MEDIA_TYPE = "application/vnd.calque.change.v1+protobuf";
const RETRY_INITIAL_MS = 100;
const RETRY_MAX_MS = 5_000;
/**
 * The most transactions a request asks for in one batch. A batch applies only
 * once all of it has arrived, so this bounds the work left after a catch-up's
 * last byte.
 */
const MAX_BATCH_TRANSACTIONS = 250;
/**
 * A catch-up that has not reached its fence after this many published batches
 * is chasing a fence the endpoint keeps moving, so the session stops.
 */
const MAX_CATCH_UP_BATCHES = 16_384;
/** The longest `long_poll_timeout_ms` the change endpoint accepts. */
const MAX_LONG_POLL_TIMEOUT_MS = 120_000;
const MAX_ERROR_BODY_BYTES = 64 * 1024;

const encoder = new TextEncoder();

/**
 * What one applied batch tells the session. The session forwards
 * `latestCommitTimeUnixMicros` without reading it, so a mount may omit it.
 */
export type ColumnarLivePublication = Omit<
  AppliedChangeBatch,
  "latestCommitTimeUnixMicros"
> &
  Partial<Pick<AppliedChangeBatch, "latestCommitTimeUnixMicros">>;

/** The mount surface the session needs. `EngineWorkerClient` satisfies it. */
export interface ColumnarLiveMount {
  /** Applies the batches as one publication, every batch or none. */
  applyChangeBatches(
    batches: readonly Uint8Array[],
    options?: ApplyChangeOptions,
  ): Promise<ColumnarLivePublication>;
}

export interface ColumnarLiveSessionOptions {
  readonly endpoint: string;
  readonly planId: string;
  readonly authorizationScopeToken: string;
  readonly tableIds: readonly string[];
  readonly clientInstanceId: string;
  readonly deviceId: string;
  /** The mount's cursor at open, from the `open` response's state. */
  readonly cursor: Uint8Array;
  readonly maxBatchBytes: number;
  readonly longPollTimeoutMs: number;
  /**
   * Whether each poll declares that this mount can consume a
   * `RevocationBarrier` body, which the server sends only to a mount that
   * declares it. Set it only when a barrier verification key is configured: a
   * mount with no key refuses every barrier.
   */
  readonly acceptRevocationBarrier?: boolean;
  /**
   * The plan's activation cursor and per-table starts. When the mount's
   * cursor is behind it, the session catches up with fenced requests before it
   * polls live; when they are equal, the first poll is already live.
   */
  readonly activationFence?: ColumnarChangeFence;
  /**
   * The mutation ids the mount holds as pending, read when each poll is
   * built. The server attributes an echo only to ids the request named, so a
   * mutation submitted while a poll is in flight is carried by the next one;
   * `nudge()` makes that next one start at once.
   */
  readonly pendingMutationIds?: () => readonly string[];
  readonly fetch?: typeof globalThis.fetch;
  /** Called after each published batch, on the same turn the mount advanced. */
  readonly onPublished?: (applied: ColumnarLivePublication) => void;
  /** Called once when the loop stops for a reason other than `stop()`. */
  readonly onFailed?: (error: unknown) => void;
}

type ColumnarLivePhase = "catch-up" | "live";

export interface ColumnarLiveStatus {
  readonly running: boolean;
  readonly phase: ColumnarLivePhase;
  readonly cursor: Uint8Array;
  readonly publishedBatches: number;
  readonly publishedTransactions: number;
  readonly publishedOperations: number;
  readonly failure: string | null;
}

export class ColumnarLiveSession {
  readonly #mount: ColumnarLiveMount;
  readonly #options: ColumnarLiveSessionOptions;
  readonly #fetch: typeof globalThis.fetch;
  #controller: AbortController | undefined;
  #completion: Promise<void> = Promise.resolve();
  readonly #live: PromiseWithResolvers<void> = Promise.withResolvers();
  /**
   * Resolves once the mount stands on the activation fence, after the
   * publication that landed it there, or at once for a mount that started
   * there. Rejects when the loop ends first, including through `stop()`.
   */
  readonly live = awaitedLater(this.#live.promise);
  #cursor: Uint8Array;
  /**
   * The fence still ahead of the mount, cleared when a fenced batch lands the
   * mount on it. Kept as state rather than compared, because a mount that went
   * live and moved past the fence is not behind it again.
   */
  #fence: ColumnarChangeFence | undefined;
  /** Aborts only the request in flight; the loop's own controller stays. */
  #poll: AbortController | undefined;
  /**
   * Set by `nudge()`; a request that fails while it is set restarts at once.
   */
  #nudged = false;
  #retryDelayMs = RETRY_INITIAL_MS;
  #catchUpBatches = 0;
  #batches = 0;
  #transactions = 0;
  #operations = 0;
  #failure: string | null = null;

  constructor(mount: ColumnarLiveMount, options: ColumnarLiveSessionOptions) {
    this.#mount = mount;
    this.#options = options;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#cursor = options.cursor.slice();
    const fence = options.activationFence;
    if (fence) {
      if (fence.cursor.byteLength === 0)
        throw new RuntimeError(
          "INVALID_LIFECYCLE",
          "the activation fence has no cursor",
        );
      const named = new Set(fence.tableStarts.map((start) => start.tableId));
      if (
        named.size !== fence.tableStarts.length ||
        named.size !== options.tableIds.length ||
        options.tableIds.some((tableId) => !named.has(tableId))
      )
        throw new RuntimeError(
          "INVALID_LIFECYCLE",
          "the activation fence must name exactly the subscribed tables",
        );
    }
    this.#fence =
      fence && !equalBytes(fence.cursor, this.#cursor) ? fence : undefined;
    if (!this.#fence) this.#live.resolve();
  }

  get status(): ColumnarLiveStatus {
    return {
      running: this.#controller !== undefined,
      phase: this.#fence ? "catch-up" : "live",
      cursor: this.#cursor.slice(),
      publishedBatches: this.#batches,
      publishedTransactions: this.#transactions,
      publishedOperations: this.#operations,
      failure: this.#failure,
    };
  }

  start(): void {
    if (this.#controller) return;
    if (this.#failure !== null)
      throw new RuntimeError(
        "INVALID_LIFECYCLE",
        `the live session already failed: ${this.#failure}`,
      );
    const controller = new AbortController();
    this.#controller = controller;
    this.#completion = this.#run(controller);
  }

  stop(): void {
    const controller = this.#controller;
    this.#controller = undefined;
    controller?.abort();
    this.#live.reject(
      new RuntimeError(
        "INVALID_LIFECYCLE",
        "the live session stopped before it reached the activation fence",
      ),
    );
  }

  /** Resolves once the loop has left. */
  async stopped(): Promise<void> {
    this.stop();
    await this.#completion;
  }

  /**
   * Restarts the request in flight so the next one carries the current
   * pending mutation ids. The batches that had arrived whole are applied
   * first, and the next request starts without a retry delay.
   */
  nudge(): void {
    if (!this.#controller) return;
    this.#nudged = true;
    this.#poll?.abort();
  }

  async #run(controller: AbortController): Promise<void> {
    this.#retryDelayMs = RETRY_INITIAL_MS;
    this.#catchUpBatches = 0;
    while (this.#controller === controller && !controller.signal.aborted) {
      try {
        await this.#follow(controller, this.#fence);
      } catch (error) {
        if (this.#controller !== controller || controller.signal.aborted)
          return;
        // A nudged request was abandoned on purpose: ask again now, with the
        // ids that prompted the nudge, rather than back off from it.
        if (this.#nudged) continue;
        if (retryable(error)) {
          await delay(this.#retryDelayMs, controller.signal);
          this.#retryDelayMs = Math.min(this.#retryDelayMs * 2, RETRY_MAX_MS);
          continue;
        }
        // Everything else is terminal for this mount: a torn or unlineaged
        // batch means the base is wrong, and retrying it would only ask the
        // server for the same impossible continuation.
        this.#controller = undefined;
        this.#failure = error instanceof Error ? error.message : String(error);
        this.#live.reject(error);
        this.#options.onFailed?.(error);
        return;
      }
    }
  }

  /**
   * One request under its own controller, chained to the loop's: `stop()`
   * aborts it through the chain, `nudge()` aborts it alone. The nudge flag is
   * cleared as the request is built, so a nudge that lands during an apply is
   * satisfied by this request. Whatever ends the read, the batches that had
   * arrived whole are applied before this returns or throws.
   */
  async #follow(
    loop: AbortController,
    fence: ColumnarChangeFence | undefined,
  ): Promise<void> {
    const poll = new AbortController();
    const forward = () => poll.abort();
    loop.signal.addEventListener("abort", forward, { once: true });
    this.#poll = poll;
    this.#nudged = false;
    try {
      const response = await this.#request(poll.signal, fence);
      const reader = response.body?.getReader();
      const splitter = new ChangeBatchSplitter(this.#options.maxBatchBytes);
      // The reader stops while it holds more than one batch's bound, so a
      // mount that applies slower than the server writes holds the server
      // back rather than this page's memory.
      const arrived = new BoundedQueue<Uint8Array>(
        this.#options.maxBatchBytes,
        (batch) => batch.byteLength,
      );
      let received = 0;
      const reading = (async () => {
        try {
          for (;;) {
            await arrived.room();
            const next = await reader?.read();
            if (!next || next.done) break;
            // One chunk's batches are applied as one group.
            const batches = splitter.push(next.value);
            arrived.push(...batches);
            received += batches.length;
          }
          arrived.close();
        } catch (error) {
          // A body cut by the transport is a hiccup: the batch it cut is
          // dropped and the next request asks again from the mount's cursor.
          arrived.fail(
            error instanceof FrameError
              ? new RuntimeError(
                  error.code === "BATCH_TOO_LARGE"
                    ? "CAPACITY_EXCEEDED"
                    : "CHANGE_STREAM_INVALID",
                  `change response is malformed: ${error.message}`,
                )
              : error,
          );
        }
      })();
      try {
        for (;;) {
          const batches = await arrived.take();
          // A stopped session applies nothing more.
          if (this.#controller !== loop || batches === undefined) break;
          await this.#publish(batches, fence);
        }
      } finally {
        // Whatever ended the loop, the read ends with it: a mount that
        // refused a batch can apply nothing after it either.
        arrived.cancel(new Error("the change response is no longer read"));
        poll.abort();
        if (reader) await stopReading(reader);
        await reading;
      }
      // A body the server ended inside a batch, or ended with none, is not a
      // response.
      if (splitter.pendingBytes > 0)
        throw new RuntimeError(
          "CHANGE_STREAM_INVALID",
          "change response ended inside a batch",
        );
      if (received === 0)
        throw new RuntimeError(
          "CHANGE_STREAM_INVALID",
          "change endpoint returned an empty response",
        );
    } finally {
      loop.signal.removeEventListener("abort", forward);
      if (this.#poll === poll) this.#poll = undefined;
    }
  }

  /** Applies one group and moves the session to where the mount now is. */
  async #publish(
    batches: readonly Uint8Array[],
    fence: ColumnarChangeFence | undefined,
  ): Promise<void> {
    const applied = await this.#mount.applyChangeBatches(
      batches,
      fence ? { fence } : {},
    );
    // The mount is the authority on where it now is. Trusting the response
    // instead would let a rejected batch and an accepted one look alike.
    const cursor = applied.state.cursor;
    if (!cursor)
      throw new RuntimeError(
        "INVALID_LIFECYCLE",
        "the mount published a batch without a cursor",
      );
    let reachedFence = false;
    if (fence) {
      // The catch-up may take several fenced batches, but each must move the
      // mount: asking again from the same cursor gets the same answer.
      if (equalBytes(cursor, this.#cursor))
        throw new RuntimeError(
          "CHANGE_STREAM_INVALID",
          "the fenced catch-up made no cursor progress",
        );
      this.#catchUpBatches += batches.length;
      if (equalBytes(cursor, fence.cursor)) {
        this.#fence = undefined;
        reachedFence = true;
      } else if (this.#catchUpBatches >= MAX_CATCH_UP_BATCHES)
        throw new RuntimeError(
          "CHANGE_STREAM_INVALID",
          `the fenced catch-up did not reach the activation cursor within ${MAX_CATCH_UP_BATCHES} batches`,
        );
    }
    this.#cursor = cursor.slice();
    this.#batches += batches.length;
    this.#transactions += applied.transactionCount;
    this.#operations += applied.operationCount;
    this.#options.onPublished?.(applied);
    if (reachedFence) this.#live.resolve();
    this.#retryDelayMs = RETRY_INITIAL_MS;
  }

  async #request(
    signal: AbortSignal,
    fence: ColumnarChangeFence | undefined,
  ): Promise<Response> {
    const startsByTable = new Map(
      fence?.tableStarts.map((start) => [start.tableId, start] as const),
    );
    const pendingMutationIds = [
      ...new Set(this.#options.pendingMutationIds?.() ?? []),
    ].sort();
    // A fenced catch-up never waits for the head, so its wait only bounds how
    // long its response may run. A shorter one would split the catch-up into
    // requests with a gap between each.
    const wait =
      fence && this.#options.longPollTimeoutMs > 0
        ? MAX_LONG_POLL_TIMEOUT_MS
        : this.#options.longPollTimeoutMs;
    const request = create(change.ChangeRequestSchema, {
      protocolVersion: 1,
      planId: this.#options.planId,
      fromCursorExclusive: create(change.CursorSchema, {
        token: this.#cursor,
      }),
      ...(fence
        ? {
            throughCursorInclusive: create(change.CursorSchema, {
              token: fence.cursor,
            }),
          }
        : {}),
      tables: [...this.#options.tableIds].sort().map((tableId) => {
        const snapshotCursor = startsByTable.get(tableId)?.snapshotCursor;
        return create(change.TableSubscriptionSchema, {
          tableId,
          ...(snapshotCursor
            ? {
                shardSnapshotCursor: create(change.CursorSchema, {
                  token: snapshotCursor,
                }),
              }
            : {}),
        });
      }),
      authorizationScopeToken: encoder.encode(
        this.#options.authorizationScopeToken,
      ),
      maxBatchBytes: BigInt(this.#options.maxBatchBytes),
      maxTransactions: MAX_BATCH_TRANSACTIONS,
      longPollTimeoutMs: wait,
      clientInstanceId: this.#options.clientInstanceId,
      deviceId: this.#options.deviceId,
      acceptTransactionFragmentation: true,
      acceptRevocationBarrier: this.#options.acceptRevocationBarrier === true,
      pendingMutationIds,
      // The wait is how long an open response may stay open, so a request
      // without one asks for one batch.
      stream: wait > 0,
    });
    const response = await this.#fetch(this.#options.endpoint, {
      method: "POST",
      headers: {
        "content-type": CHANGE_MEDIA_TYPE,
        accept: CHANGE_MEDIA_TYPE,
      },
      body: toBinary(change.ChangeRequestSchema, request),
      signal,
    });
    if (!response.ok) {
      const refusal = await readErrorFrame(response);
      // The refusal's `resnapshotRequired` outranks the status: the server
      // sends those refusals as 401, 409 or 410, and a status rule would
      // report a client that merely fell behind as an integrity failure. An
      // authorization refusal is named apart, because its rebuild needs a
      // fresh grant.
      throw new RuntimeError(
        refusal?.code === change.StreamErrorCode.AUTHORIZATION_CHANGED
          ? "AUTHORIZATION_EXPIRED"
          : refusal?.resnapshotRequired
            ? "RESNAPSHOT_REQUIRED"
            : response.status >= 500 || response.status === 429
              ? "CANCELLED"
              : "CHANGE_STREAM_INVALID",
        `change request failed with HTTP ${response.status}${describeRefusal(refusal)}`,
      );
    }
    return response;
  }
}

/**
 * A transport hiccup is retryable; a protocol or state violation is not.
 * `CANCELLED` is the code `#request` uses for a server that asked to be tried
 * again, so it is retryable here and nowhere else.
 */
function retryable(error: unknown): boolean {
  if (error instanceof RuntimeError) return error.code === "CANCELLED";
  return error instanceof TypeError;
}

/**
 * Reads the one framed `StreamError` the endpoint answers a refused request
 * with. A body that is missing, oversized, or not exactly one error frame
 * yields nothing, so an unreadable refusal cannot decide whether a rebuild is
 * required.
 */
async function readErrorFrame(
  response: Response,
): Promise<change.StreamError | undefined> {
  try {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength === 0 || bytes.byteLength > MAX_ERROR_BODY_BYTES)
      return undefined;
    const decoder = new FrameDecoder(
      change.ChangeStreamFrameSchema,
      MAX_ERROR_BODY_BYTES,
    );
    const frames = decoder.push(bytes);
    decoder.finish();
    const error = frames[0]?.body;
    if (frames.length !== 1 || error?.case !== "error") return undefined;
    return error.value;
  } catch {
    return undefined;
  }
}

function describeRefusal(error: change.StreamError | undefined): string {
  if (!error) return "";
  return `: ${change.StreamErrorCode[error.code] ?? error.code} ${error.message}`;
}
