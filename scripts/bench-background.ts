/**
 * Background clients for the realtime session (`--background-clients`): many
 * plans held at once, each following the change stream as a browser does,
 * while the page's probes measure commit to stable paint.
 *
 * Each client bootstraps through the client's own `renderBootstrap`, then asks
 * the plan's change endpoint for one open response from the activation cursor,
 * as `ColumnarLiveSession` does, and asks again from where it ended. It decodes
 * only the last frame of each batch, for the cursor. A few clients are
 * observers: they decode every batch and record when each transaction arrived
 * against PostgreSQL's commit time.
 *
 * The clients run in child processes (`main` below), `CLIENTS_PER_PROCESS` to
 * each, so their event loops are not the bench's; the observers are all in
 * the first. The report keeps the children's CPU and event-loop delay, so a
 * run can tell calque's delay from its own clients' falling behind.
 */
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createRequire } from "node:module";
import { join } from "node:path";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { createInterface } from "node:readline";
import { fileURLToPath, pathToFileURL } from "node:url";

import { BROWSER_ACCEPT_ENCODING } from "./bench-brotli.ts";
import {
  NetworkMemoryGuard,
  NetworkMemoryVoid,
} from "./bench-network-memory.ts";

const root = fileURLToPath(new URL("..", import.meta.url));
const CHANGE_MEDIA_TYPE = "application/vnd.calque.change.v1+protobuf";
const FRAME_PREFIX_BYTES = 4;
/** Bootstraps in flight at once; the server plans two at a time anyway. */
const BOOTSTRAP_CONCURRENCY = 8;
const RETRY_MS = 250;
export const CLIENTS_PER_PROCESS = 250;
/** How long `stop` waits for every client to stand on the same cursor. */
const SETTLE_TIMEOUT_MS = 60_000;
/** How long a child has to answer anything after it is ready. */
const ANSWER_TIMEOUT_MS = 60_000;
/**
 * How long a child bootstraps before it gives up on the clients it has not
 * bootstrapped: a browser that waits this long for a plan has failed.
 */
const BOOTSTRAP_DEADLINE_MS = 10 * 60_000;
const READY_TIMEOUT_MS = BOOTSTRAP_DEADLINE_MS + 5 * 60_000;
/** Observer latencies are counted in 1 ms bins up to this, and above it once. */
const HISTOGRAM_MS = 60_000;

/** What `renderBootstrap` takes: the page's connection and its parameters. */
export type BootstrapStart = Record<string, unknown> & {
  readonly endpoint: string;
};

/** A `taskpolicy -c` clamp the clients' processes run under. */
export type BackgroundQos = "utility";

export interface BackgroundOptions {
  readonly clients: number;
  readonly observers: number;
  readonly start: BootstrapStart;
  readonly qos?: BackgroundQos;
  /**
   * Told the reason once the network memory guard stops the clients after
   * they were ready; one stopped before is `start`'s `NetworkMemoryVoid`.
   */
  readonly onVoid?: (reason: string) => void;
}

/** The clamp the children ran under, and each one's priority once ready. */
export interface Placement {
  readonly qos: BackgroundQos | null;
  readonly priorities: readonly number[];
}

interface ChildOptions extends Omit<BackgroundOptions, "onVoid"> {
  readonly firstClient: number;
}

/** How many clients each child follows: `CLIENTS_PER_PROCESS` but the last. */
export function processShares(clients: number): number[] {
  const processes = Math.ceil(clients / CLIENTS_PER_PROCESS);
  return Array.from({ length: processes }, (_, k) =>
    Math.min(CLIENTS_PER_PROCESS, clients - k * CLIENTS_PER_PROCESS),
  );
}

/** The plan fields a follower reads. */
interface FollowedPlan {
  readonly planId: string;
  readonly scopeToken: string;
  readonly tableIds: readonly string[];
  readonly endpoint: string;
  readonly activationCursor: string;
  readonly longPollTimeoutMs: number;
  readonly maxBatchBytes: number;
}

/**
 * The client instance and plan in what `renderBootstrap` returned: its
 * `calque-plan` element, whose `<` is written `\u003c`.
 */
export function renderedPlan(html: string): {
  readonly clientInstanceId: string;
  readonly plan: FollowedPlan;
} {
  const match =
    /<script type="application\/json" id="calque-plan">([^<]*)<\/script>/u.exec(
      html,
    );
  if (!match?.[1]) throw new Error("the bootstrap rendered no plan");
  const element = JSON.parse(match[1]) as {
    readonly clientInstanceId?: unknown;
    readonly plan?: unknown;
  };
  if (
    typeof element.clientInstanceId !== "string" ||
    typeof element.plan !== "string"
  )
    throw new Error("the rendered plan has no client instance or plan");
  const plan = JSON.parse(element.plan) as {
    readonly planId: string;
    readonly authorization: { readonly scopeToken: string };
    readonly artifacts: readonly { readonly tables: readonly string[] }[];
    readonly changeStream: {
      readonly endpoint: string;
      readonly throughCursorInclusive: string;
      readonly longPollTimeoutMs: number;
      readonly maxBatchBytes: number;
    };
  };
  return {
    clientInstanceId: element.clientInstanceId,
    plan: {
      planId: plan.planId,
      scopeToken: plan.authorization.scopeToken,
      tableIds: [
        ...new Set(plan.artifacts.flatMap((artifact) => artifact.tables)),
      ].sort(),
      endpoint: plan.changeStream.endpoint,
      activationCursor: plan.changeStream.throughCursorInclusive,
      longPollTimeoutMs: plan.changeStream.longPollTimeoutMs,
      maxBatchBytes: plan.changeStream.maxBatchBytes,
    },
  };
}

/** The payload of the last length-prefixed frame in `batch`. */
export function lastFramePayload(batch: Uint8Array): Uint8Array {
  const view = new DataView(batch.buffer, batch.byteOffset, batch.byteLength);
  let offset = 0;
  let last: Uint8Array | null = null;
  while (offset + FRAME_PREFIX_BYTES <= batch.byteLength) {
    const length = view.getUint32(offset, false);
    const start = offset + FRAME_PREFIX_BYTES;
    if (length === 0 || start + length > batch.byteLength) break;
    last = batch.subarray(start, start + length);
    offset = start + length;
  }
  if (last === null || offset !== batch.byteLength)
    throw new Error("a change batch is not a whole sequence of frames");
  return last;
}

/**
 * `url`'s path on `base`'s origin. The plan names the browser's change URL,
 * behind the edge when there is one, and the edge forwards it to calque
 * unchanged, so the clients ask calque's loopback address for the same path.
 */
export function onOrigin(url: string, base: string): string {
  const parsed = new URL(url);
  return new URL(`${parsed.pathname}${parsed.search}`, base).href;
}

/** Which of `clients` observe: `observers` of them, spread evenly by index. */
export function observerIndices(clients: number, observers: number): number[] {
  const count = Math.min(clients, observers);
  return Array.from({ length: count }, (_, k) =>
    Math.floor((k * clients) / count),
  );
}

/** Nearest-rank quantile of an ascending histogram of 1 ms bins. */
export function histogramQuantile(
  bins: readonly number[],
  overflow: number,
  q: number,
): number | null {
  const total = bins.reduce((sum, count) => sum + count, 0) + overflow;
  if (total === 0) return null;
  const rank = Math.max(1, Math.ceil(q * total));
  let seen = 0;
  for (const [ms, count] of bins.entries()) {
    seen += count;
    if (seen >= rank) return ms;
  }
  return Number.POSITIVE_INFINITY;
}

/** A nearest-rank summary of times in ms. */
export interface Latency {
  readonly p50: number;
  readonly p95: number;
  readonly max: number;
}

/** `values`' nearest-rank p50 and p95 and their maximum, or null. */
export function latency(values: readonly number[]): Latency | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const rank = (q: number) =>
    sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)] ?? 0;
  return { p50: rank(0.5), p95: rank(0.95), max: sorted.at(-1) ?? 0 };
}

export interface BackgroundReport {
  readonly clients: number;
  readonly processes: number;
  readonly bootstrapped: number;
  readonly bootstrapFailures: Readonly<Record<string, number>>;
  readonly bootstrapMs: number;
  /** Each bootstrapped client's own bootstrap, or null when none was. */
  readonly bootstrapLatencyMs: Latency | null;
  /** From the first request sent to every client's first batch, or null. */
  readonly allLiveMs: number | null;
  readonly requests: number;
  readonly refusals: Readonly<Record<string, number>>;
  readonly transportErrors: number;
  /** The first transport error's message, or null. */
  readonly firstTransportError: string | null;
  readonly batches: number;
  readonly bytes: number;
  /** Whether every client reached the same cursor before `stop` gave up. */
  readonly settled: boolean;
  readonly settleMs: number;
  readonly distinctFinalCursors: number;
  readonly observers: {
    readonly count: number;
    /** Transactions observed after every client was live. */
    readonly transactions: number;
    readonly commitToArrivalMs: {
      readonly p50: number | null;
      readonly p95: number | null;
      readonly p99: number | null;
      readonly max: number | null;
    };
    /**
     * Per transaction seen by every observer, its last arrival minus its
     * first, in ms; no clock but this process's.
     */
    readonly spreadMs: {
      readonly p50: number | null;
      readonly p95: number | null;
      readonly max: number | null;
    };
  };
  /**
   * The children from ready to the load's stop: their CPU together, and the
   * worst child's event-loop delay.
   */
  readonly loadClient: {
    readonly cpuSeconds: number;
    readonly wallSeconds: number;
    readonly eventLoopDelayMs: {
      readonly p50: number;
      readonly p99: number;
      readonly max: number;
    };
  };
}

type Protocol = {
  readonly change: {
    readonly ChangeRequestSchema: unknown;
    readonly CursorSchema: unknown;
    readonly TableSubscriptionSchema: unknown;
    readonly ChangeStreamFrameSchema: unknown;
  };
  readonly ChangeBatchSplitter: new (maxBatchBytes: number) => {
    push(chunk: Uint8Array): Uint8Array[];
  };
};
type Protobuf = {
  create(schema: unknown, init?: Record<string, unknown>): unknown;
  toBinary(schema: unknown, message: unknown): Uint8Array<ArrayBuffer>;
  fromBinary(schema: unknown, bytes: Uint8Array): Frame;
};
interface Frame {
  readonly body:
    | {
        readonly case: "batchEnd";
        readonly value: {
          readonly throughCursorInclusive?: { readonly token: Uint8Array };
        };
      }
    | {
        readonly case: "transactionFragment";
        readonly value: {
          readonly finalFragment: boolean;
          readonly commitLsn: string;
          readonly commitTimeUnixMicros: bigint;
        };
      }
    | { readonly case: string | undefined; readonly value?: unknown };
}

const nowMs = () => performance.timeOrigin + performance.now();
const delay = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
const encoder = new TextEncoder();

/** The arrivals the observers saw, kept as counts rather than samples. */
class Observations {
  readonly bins = new Array<number>(HISTOGRAM_MS).fill(0);
  overflow = 0;
  maxMs: number | null = null;
  transactions = 0;
  /** By commit LSN: first and last arrival and how many observers saw it. */
  readonly arrivals = new Map<
    string,
    { first: number; last: number; seen: number }
  >();

  record(commitLsn: string, latencyMs: number, at: number, live: boolean) {
    const entry = this.arrivals.get(commitLsn);
    if (entry) {
      entry.first = Math.min(entry.first, at);
      entry.last = Math.max(entry.last, at);
      entry.seen += 1;
    } else if (live)
      this.arrivals.set(commitLsn, { first: at, last: at, seen: 1 });
    if (!live) return;
    this.transactions += 1;
    this.maxMs = Math.max(this.maxMs ?? latencyMs, latencyMs);
    const bin = Math.floor(Math.max(0, latencyMs));
    if (bin < HISTOGRAM_MS) this.bins[bin] = (this.bins[bin] ?? 0) + 1;
    else this.overflow += 1;
  }
}

class Follower {
  cursor: Uint8Array;
  firstBatchAt: number | null = null;
  requests = 0;
  batches = 0;
  bytes = 0;
  transportErrors = 0;
  firstTransportError: string | null = null;
  /** By status and the refusal's message, as `503 change source is ...`. */
  readonly refusals = new Map<string, number>();

  constructor(
    private readonly plan: FollowedPlan,
    private readonly clientInstanceId: string,
    private readonly deviceId: string,
    private readonly protocol: Protocol,
    private readonly protobuf: Protobuf,
    private readonly observe: ((batch: Uint8Array, at: number) => void) | null,
  ) {
    this.cursor = encoder.encode(plan.activationCursor);
  }

  async run(signal: AbortSignal): Promise<void> {
    const { change, ChangeBatchSplitter } = this.protocol;
    const { create, toBinary } = this.protobuf;
    while (!signal.aborted) {
      const request = create(change.ChangeRequestSchema, {
        protocolVersion: 1,
        planId: this.plan.planId,
        fromCursorExclusive: create(change.CursorSchema, {
          token: this.cursor,
        }),
        tables: this.plan.tableIds.map((tableId) =>
          create(change.TableSubscriptionSchema, { tableId }),
        ),
        authorizationScopeToken: encoder.encode(this.plan.scopeToken),
        maxBatchBytes: BigInt(this.plan.maxBatchBytes),
        maxTransactions: 1_000,
        longPollTimeoutMs: this.plan.longPollTimeoutMs,
        clientInstanceId: this.clientInstanceId,
        deviceId: this.deviceId,
        acceptTransactionFragmentation: true,
        acceptRevocationBarrier: false,
        pendingMutationIds: [],
        stream: true,
      });
      this.requests += 1;
      let response: Response;
      try {
        response = await fetch(this.plan.endpoint, {
          method: "POST",
          headers: {
            "content-type": CHANGE_MEDIA_TYPE,
            accept: CHANGE_MEDIA_TYPE,
          },
          body: toBinary(change.ChangeRequestSchema, request),
          signal,
        });
      } catch (error) {
        if (signal.aborted) return;
        this.failed(error);
        await delay(RETRY_MS);
        continue;
      }
      if (!response.ok) {
        const reason = `${String(response.status)}${await this.refusalMessage(response)}`;
        this.refusals.set(reason, (this.refusals.get(reason) ?? 0) + 1);
        await delay(RETRY_MS);
        continue;
      }
      const splitter = new ChangeBatchSplitter(this.plan.maxBatchBytes);
      try {
        for await (const chunk of response.body ?? []) {
          const at = nowMs();
          for (const batch of splitter.push(chunk as Uint8Array)) {
            this.batches += 1;
            this.bytes += batch.byteLength;
            this.firstBatchAt ??= at;
            this.observe?.(batch, at);
            this.advance(batch);
          }
        }
      } catch (error) {
        if (signal.aborted) return;
        this.failed(error);
      }
    }
  }

  /** The message of a refusal's error frame, after a space, or nothing. */
  private async refusalMessage(response: Response): Promise<string> {
    try {
      const frame = this.protobuf.fromBinary(
        this.protocol.change.ChangeStreamFrameSchema,
        lastFramePayload(new Uint8Array(await response.arrayBuffer())),
      );
      return frame.body.case === "streamError"
        ? ` ${(frame.body.value as { readonly message: string }).message}`
        : "";
    } catch {
      return "";
    }
  }

  private failed(error: unknown): void {
    this.transportErrors += 1;
    this.firstTransportError ??=
      error instanceof Error
        ? `${error.message}${error.cause instanceof Error ? `: ${error.cause.message}` : ""}`
        : String(error);
  }

  /** Moves to the cursor the batch ends on, read from its last frame. */
  private advance(batch: Uint8Array): void {
    const frame = this.protobuf.fromBinary(
      this.protocol.change.ChangeStreamFrameSchema,
      lastFramePayload(batch),
    );
    if (frame.body.case !== "batchEnd") return;
    const token = (
      frame.body.value as {
        readonly throughCursorInclusive?: { readonly token: Uint8Array };
      }
    ).throughCursorInclusive?.token;
    if (token) this.cursor = token.slice();
  }
}

async function loadModules(): Promise<{
  renderBootstrap: (
    start: BootstrapStart,
    options: {
      readonly acceptEncoding: string;
      readonly fetch: typeof fetch;
      readonly signal?: AbortSignal;
    },
  ) => Promise<string>;
  protocol: Protocol;
  protobuf: Protobuf;
}> {
  const protocolRoot = join(root, "client/packages/protocol");
  const { renderBootstrap } = (await import(
    pathToFileURL(
      join(root, "client/packages/client/dist/bootstrap-render-entry.js"),
    ).href
  )) as {
    renderBootstrap: (
      start: BootstrapStart,
      options: {
        readonly acceptEncoding: string;
        readonly fetch: typeof fetch;
        readonly signal?: AbortSignal;
      },
    ) => Promise<string>;
  };
  const protocol = (await import(
    pathToFileURL(join(protocolRoot, "dist/index.js")).href
  )) as Protocol;
  const protobuf = (await import(
    pathToFileURL(
      createRequire(join(protocolRoot, "package.json")).resolve(
        "@bufbuild/protobuf",
      ),
    ).href
  )) as Protobuf;
  return { renderBootstrap, protocol, protobuf };
}

/** What one child says once its clients have bootstrapped or its deadline passed. */
export interface ChildReady {
  readonly bootstrapped: number;
  readonly bootstrapFailures: Readonly<Record<string, number>>;
  readonly bootstrapMs: number;
  /** Each bootstrapped client's own bootstrap, in ms. */
  readonly bootstrapLatenciesMs: readonly number[];
}

/** One child's part of the report, before the bench joins them. */
export type ChildReport = ChildReady &
  Omit<
    BackgroundReport,
    | "clients"
    | "processes"
    | "settled"
    | "settleMs"
    | "distinctFinalCursors"
    | "bootstrapLatencyMs"
  >;

type Ready = BackgroundClients["ready"];

/**
 * The child: bootstraps its clients and follows. Asked for `cursors`, it
 * answers where its clients stand, and the first ask ends the window its
 * CPU and event-loop delay are read over; asked for its `report`, it stops.
 */
async function main(): Promise<void> {
  const lines = createInterface({ input: process.stdin });
  const next = (() => {
    const iterator = lines[Symbol.asyncIterator]();
    return async () => {
      const { value, done } = await iterator.next();
      if (done)
        throw new Error("the bench closed the background clients' input");
      return JSON.parse(value as string) as Record<string, unknown>;
    };
  })();
  const say = (message: object) =>
    process.stdout.write(`${JSON.stringify(message)}\n`);
  const options = (await next()) as unknown as ChildOptions;
  const { renderBootstrap, protocol, protobuf } = await loadModules();
  const observers = new Set(
    observerIndices(options.clients, options.observers),
  );
  const observations = new Observations();
  let allLiveAt: number | null = null;
  const controller = new AbortController();
  const followers: Follower[] = [];
  const running: Promise<void>[] = [];
  const bootstrapFailures = new Map<string, number>();
  const bootstrapLatenciesMs: number[] = [];
  const firstRequestAt = nowMs();
  const deadline = AbortSignal.timeout(BOOTSTRAP_DEADLINE_MS);
  const late = `not bootstrapped within ${String(BOOTSTRAP_DEADLINE_MS / 1_000)} s`;

  const observe = (batch: Uint8Array, at: number) => {
    // Every frame of the batch, length-prefixed one after another.
    const view = new DataView(batch.buffer, batch.byteOffset, batch.byteLength);
    for (let offset = 0; offset < batch.byteLength;) {
      const length = view.getUint32(offset, false);
      const payload = batch.subarray(
        offset + FRAME_PREFIX_BYTES,
        offset + FRAME_PREFIX_BYTES + length,
      );
      offset += FRAME_PREFIX_BYTES + length;
      const frame = protobuf.fromBinary(
        protocol.change.ChangeStreamFrameSchema,
        payload,
      );
      if (frame.body.case !== "transactionFragment") continue;
      const fragment = frame.body.value as {
        readonly finalFragment: boolean;
        readonly commitLsn: string;
        readonly commitTimeUnixMicros: bigint;
      };
      if (!fragment.finalFragment) continue;
      observations.record(
        fragment.commitLsn,
        at - Number(fragment.commitTimeUnixMicros) / 1_000,
        at,
        allLiveAt !== null,
      );
    }
  };

  let index = 0;
  const bootstrapOne = async () => {
    for (;;) {
      const k = index;
      index += 1;
      if (k >= options.clients) return;
      if (deadline.aborted) {
        bootstrapFailures.set(late, (bootstrapFailures.get(late) ?? 0) + 1);
        continue;
      }
      let status = "no response";
      const startedAt = nowMs();
      const deviceId = `background-device-${String(options.firstClient + k)}`;
      const html = await renderBootstrap(
        { ...options.start, deviceId },
        {
          acceptEncoding: BROWSER_ACCEPT_ENCODING,
          signal: deadline,
          fetch: async (input, init) => {
            const response = await fetch(input, init);
            status = String(response.status);
            // A refusal names its reason in the body's code.
            if (!response.ok) {
              const body = (await response
                .clone()
                .json()
                .catch(() => null)) as { readonly code?: unknown } | null;
              if (typeof body?.code === "string") status += ` ${body.code}`;
            }
            return response;
          },
        },
      );
      if (html === "") {
        const reason = deadline.aborted ? late : status;
        bootstrapFailures.set(reason, (bootstrapFailures.get(reason) ?? 0) + 1);
        continue;
      }
      bootstrapLatenciesMs.push(nowMs() - startedAt);
      const { clientInstanceId, plan } = renderedPlan(html);
      const follower = new Follower(
        { ...plan, endpoint: onOrigin(plan.endpoint, options.start.endpoint) },
        clientInstanceId,
        deviceId,
        protocol,
        protobuf,
        observers.has(k) ? observe : null,
      );
      followers.push(follower);
      running.push(follower.run(controller.signal));
    }
  };
  await Promise.all(
    Array.from({ length: BOOTSTRAP_CONCURRENCY }, () => bootstrapOne()),
  );
  const bootstrapMs = nowMs() - firstRequestAt;
  const ready: ChildReady = {
    bootstrapped: followers.length,
    bootstrapFailures: Object.fromEntries(bootstrapFailures),
    bootstrapMs,
    bootstrapLatenciesMs,
  };
  say({ event: "ready", ...ready });
  const cpuAtReady = process.cpuUsage();
  const readyAt = nowMs();
  const loopDelay = monitorEventLoopDelay({ resolution: 10 });
  loopDelay.enable();
  const measured = (): ChildReport["loadClient"] => {
    loopDelay.disable();
    const cpu = process.cpuUsage(cpuAtReady);
    return {
      cpuSeconds: (cpu.user + cpu.system) / 1e6,
      wallSeconds: (nowMs() - readyAt) / 1_000,
      eventLoopDelayMs: {
        p50: loopDelay.percentile(50) / 1e6,
        p99: loopDelay.percentile(99) / 1e6,
        max: loopDelay.max / 1e6,
      },
    };
  };

  // Every client is live once each has had a batch; the observers count from
  // then, so a transaction some clients never asked for is not read.
  const liveWatch = (async () => {
    while (!controller.signal.aborted && allLiveAt === null) {
      if (
        followers.length > 0 &&
        followers.every((follower) => follower.firstBatchAt !== null)
      )
        allLiveAt = nowMs();
      else await delay(50);
    }
  })();

  let loadClient: ChildReport["loadClient"] | null = null;
  const key = (cursor: Uint8Array) => Buffer.from(cursor).toString("base64");
  for (;;) {
    const message = await next();
    if (message["command"] === "report") break;
    if (message["command"] !== "cursors")
      throw new Error(`unknown command ${JSON.stringify(message)}`);
    loadClient ??= measured();
    say({
      event: "cursors",
      cursors: [...new Set(followers.map((follower) => key(follower.cursor)))],
    });
  }
  loadClient ??= measured();
  controller.abort();
  await Promise.allSettled(running);
  await liveWatch;

  const refusals = new Map<string, number>();
  for (const follower of followers)
    for (const [status, count] of follower.refusals)
      refusals.set(status, (refusals.get(status) ?? 0) + count);
  const spreads = [...observations.arrivals.values()]
    .filter((entry) => entry.seen === observers.size)
    .map((entry) => entry.last - entry.first)
    .sort((a, b) => a - b);
  const nearest = (values: readonly number[], q: number) =>
    values.length === 0
      ? null
      : (values[Math.max(0, Math.ceil(q * values.length) - 1)] ?? null);
  const report: ChildReport = {
    ...ready,
    allLiveMs: allLiveAt === null ? null : allLiveAt - firstRequestAt,
    requests: followers.reduce((sum, follower) => sum + follower.requests, 0),
    refusals: Object.fromEntries(refusals),
    transportErrors: followers.reduce(
      (sum, follower) => sum + follower.transportErrors,
      0,
    ),
    firstTransportError:
      followers.find((follower) => follower.firstTransportError !== null)
        ?.firstTransportError ?? null,
    batches: followers.reduce((sum, follower) => sum + follower.batches, 0),
    bytes: followers.reduce((sum, follower) => sum + follower.bytes, 0),
    observers: {
      count: observers.size,
      transactions: observations.transactions,
      commitToArrivalMs: {
        p50: histogramQuantile(observations.bins, observations.overflow, 0.5),
        p95: histogramQuantile(observations.bins, observations.overflow, 0.95),
        p99: histogramQuantile(observations.bins, observations.overflow, 0.99),
        max: observations.maxMs,
      },
      spreadMs: {
        p50: nearest(spreads, 0.5),
        p95: nearest(spreads, 0.95),
        max: spreads.at(-1) ?? null,
      },
    },
    loadClient,
  };
  say({ event: "report", report });
  lines.close();
}

/** Status counts added up across children. */
function addedCounts(
  counts: readonly Readonly<Record<string, number>>[],
): Record<string, number> {
  const total: Record<string, number> = {};
  for (const each of counts)
    for (const [key, count] of Object.entries(each))
      total[key] = (total[key] ?? 0) + count;
  return total;
}

/** The children's ready messages as one: they bootstrap at the same time. */
export function joinReady(readies: readonly ChildReady[]): Ready {
  return {
    bootstrapped: readies.reduce((sum, ready) => sum + ready.bootstrapped, 0),
    bootstrapFailures: addedCounts(
      readies.map((ready) => ready.bootstrapFailures),
    ),
    bootstrapMs: Math.max(0, ...readies.map((ready) => ready.bootstrapMs)),
    bootstrapLatencyMs: latency(
      readies.flatMap((ready) => ready.bootstrapLatenciesMs),
    ),
  };
}

/**
 * The children's reports as one: counts added, times the slowest child's,
 * the observers the first child's, CPU added and event-loop delay the worst.
 */
export function joinReports(
  clients: number,
  parts: readonly ChildReport[],
  settle: Pick<
    BackgroundReport,
    "settled" | "settleMs" | "distinctFinalCursors"
  >,
): BackgroundReport {
  const [first] = parts;
  if (first === undefined) throw new Error("no background client reported");
  const sum = (read: (part: ChildReport) => number) =>
    parts.reduce((total, part) => total + read(part), 0);
  const most = (read: (part: ChildReport) => number) =>
    Math.max(...parts.map(read));
  const live = parts.map((part) => part.allLiveMs);
  return {
    clients,
    processes: parts.length,
    ...joinReady(parts),
    allLiveMs: live.includes(null) ? null : Math.max(...(live as number[])),
    requests: sum((part) => part.requests),
    refusals: addedCounts(parts.map((part) => part.refusals)),
    transportErrors: sum((part) => part.transportErrors),
    firstTransportError:
      parts.find((part) => part.firstTransportError !== null)
        ?.firstTransportError ?? null,
    batches: sum((part) => part.batches),
    bytes: sum((part) => part.bytes),
    ...settle,
    observers: first.observers,
    loadClient: {
      cpuSeconds: sum((part) => part.loadClient.cpuSeconds),
      wallSeconds: most((part) => part.loadClient.wallSeconds),
      eventLoopDelayMs: {
        p50: most((part) => part.loadClient.eventLoopDelayMs.p50),
        p99: most((part) => part.loadClient.eventLoopDelayMs.p99),
        max: most((part) => part.loadClient.eventLoopDelayMs.max),
      },
    },
  };
}

/** `command` under `taskpolicy` when it has a clamp. */
export function placed(
  qos: BackgroundQos | undefined,
  command: readonly string[],
): string[] {
  return qos === undefined
    ? [...command]
    : ["taskpolicy", "-c", qos, ...command];
}

/** Each process's scheduling priority as `ps` reads it, in `pids`' order. */
export function schedulingPriorities(pids: readonly number[]): number[] {
  const listed = new Map(
    execFileSync("ps", ["-o", "pid=,pri=", "-p", pids.join(",")], {
      encoding: "utf8",
    })
      .trim()
      .split("\n")
      .map((line) => line.trim().split(/\s+/u).map(Number) as [number, number]),
  );
  return pids.map((pid) => {
    const priority = listed.get(pid);
    if (priority === undefined)
      throw new Error(`ps read no process ${String(pid)}`);
    return priority;
  });
}

/** One child process and its messages. */
class Child {
  private constructor(
    private readonly child: ChildProcess,
    private readonly messages: AsyncIterator<string>,
  ) {}

  static spawn(options: ChildOptions, qos: BackgroundQos | undefined): Child {
    const [command, ...args] = placed(qos, [
      process.execPath,
      "--import",
      "./scripts/register.mjs",
      "scripts/bench-background.ts",
    ]);
    const child = spawn(command!, args, {
      cwd: root,
      stdio: ["pipe", "pipe", "inherit"],
    });
    const messages = createInterface({ input: child.stdout! })[
      Symbol.asyncIterator
    ]();
    child.stdin!.write(`${JSON.stringify(options)}\n`);
    return new Child(child, messages);
  }

  get pid(): number {
    return this.child.pid!;
  }

  async ask(command: string, event: string): Promise<Record<string, unknown>> {
    this.child.stdin!.write(`${JSON.stringify({ command })}\n`);
    return this.read(event, ANSWER_TIMEOUT_MS);
  }

  async read(
    event: string,
    timeoutMs: number,
  ): Promise<Record<string, unknown>> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`the background clients sent no ${event}`)),
        timeoutMs,
      );
    });
    try {
      for (;;) {
        const { value, done } = await Promise.race([
          this.messages.next(),
          timeout,
        ]);
        if (done)
          throw new Error(`the background clients exited before ${event}`);
        const message = JSON.parse(value) as Record<string, unknown>;
        if (message["event"] === event) return message;
      }
    } finally {
      clearTimeout(timer);
    }
  }

  end(): void {
    this.child.stdin!.end();
  }

  kill(): void {
    if (this.child.exitCode === null) this.child.kill("SIGTERM");
  }
}

/**
 * The bench's side: the children, each told its share of the clients, and
 * the network memory guard, which stops them all on its first problem.
 */
export class BackgroundClients {
  private constructor(
    private readonly children: readonly Child[],
    private readonly clients: number,
    private readonly guard: NetworkMemoryGuard,
    readonly ready: Pick<
      BackgroundReport,
      | "bootstrapped"
      | "bootstrapFailures"
      | "bootstrapMs"
      | "bootstrapLatencyMs"
    >,
    readonly placement: Placement,
  ) {}

  /**
   * Starts the children once the guard has read the host. Throws
   * `NetworkMemoryVoid` when the guard refuses the host or stops the
   * children before they are ready.
   */
  static async start(options: BackgroundOptions): Promise<BackgroundClients> {
    const guard = await NetworkMemoryGuard.before();
    let firstClient = 0;
    const children = processShares(options.clients).map((clients, k) => {
      const child = Child.spawn(
        {
          clients,
          observers: k === 0 ? options.observers : 0,
          start: options.start,
          firstClient,
        },
        options.qos,
      );
      firstClient += clients;
      return child;
    });
    let ready = false;
    guard.watch((reason) => {
      for (const child of children) child.kill();
      if (ready) options.onVoid?.(reason);
    });
    try {
      const readies = await Promise.all(
        children.map((child) => child.read("ready", READY_TIMEOUT_MS)),
      );
      ready = true;
      return new BackgroundClients(
        children,
        options.clients,
        guard,
        joinReady(readies as unknown as ChildReady[]),
        {
          qos: options.qos ?? null,
          priorities: schedulingPriorities(children.map((child) => child.pid)),
        },
      );
    } catch (error) {
      guard.stop();
      for (const child of children) child.kill();
      if (guard.reason !== null) throw new NetworkMemoryVoid(guard.reason);
      throw error;
    }
  }

  /** Why the guard stopped the clients, or null while it has not. */
  get voidReason(): string | null {
    return this.guard.reason;
  }

  /**
   * Waits for the clients to stand on one cursor, then their report. Throws
   * `NetworkMemoryVoid` when the guard stops them first.
   */
  async stop(): Promise<BackgroundReport> {
    try {
      return await this.settleAndReport();
    } catch (error) {
      if (this.guard.reason !== null)
        throw new NetworkMemoryVoid(this.guard.reason);
      throw error;
    } finally {
      this.guard.stop();
    }
  }

  private async settleAndReport(): Promise<BackgroundReport> {
    if (this.guard.reason !== null)
      throw new NetworkMemoryVoid(this.guard.reason);
    const started = nowMs();
    let distinct = new Set<string>();
    for (;;) {
      const answers = await Promise.all(
        this.children.map((child) => child.ask("cursors", "cursors")),
      );
      distinct = new Set(
        answers.flatMap((answer) => answer["cursors"] as string[]),
      );
      if (distinct.size <= 1 || nowMs() - started >= SETTLE_TIMEOUT_MS) break;
      await delay(100);
    }
    const settleMs = nowMs() - started;
    const parts = await Promise.all(
      this.children.map((child) => child.ask("report", "report")),
    );
    for (const child of this.children) child.end();
    return joinReports(
      this.clients,
      parts.map((part) => part["report"] as ChildReport),
      {
        settled: distinct.size === 1,
        settleMs,
        distinctFinalCursors: distinct.size,
      },
    );
  }

  kill(): void {
    this.guard.stop();
    for (const child of this.children) child.kill();
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}
