/** What the browser presents and receives during a page load: screencast frames and the request ledger. */
import type { CDPSession } from "playwright";
import { startScreencast } from "./bench-browser.ts";
import {
  type DroppedFrameSignal,
  FrameHasher,
  type HashedFrame,
  mapEpochSecondsToMonotonicMilliseconds,
  type Roi,
  type StablePaintJudgement,
} from "./bench-frames.ts";
import { hrtimeMs } from "./bench-host.ts";
import { redactedUrl } from "./bench-record.ts";

type CapturedFrame = Omit<HashedFrame, "timestampMs">;

/**
 * Every presented frame of one page, acknowledged on receipt and hashed in
 * the worker pool. `prefix()` is the frames hashed so far without a gap, on
 * Chrome's monotonic clock once `offset()` knows the mapping from its
 * wall-clock stamps, and none before then; a new offset maps every frame
 * again.
 */
export class Capture {
  readonly #frames: (CapturedFrame | undefined)[] = [];
  readonly #mapped: HashedFrame[] = [];
  #mappedOffset: number | null = null;
  readonly signals: {
    readonly atMs: number;
    readonly signal: DroppedFrameSignal;
  }[] = [];
  readonly #hashes: Promise<void>[] = [];
  readonly #failures: Error[] = [];
  #received = 0;
  #contiguous = 0;
  #stopped = false;
  #lastPng: { readonly seq: number; readonly data: string } | null = null;
  readonly #ackedMs: (number | undefined)[] = [];
  #unacknowledged = 0;
  ackRoundTripMaxMs = 0;
  /**
   * The most frames received and not yet acknowledged at once, counted from
   * a frame's arrival on the host to Chrome's reply to its acknowledgement.
   * Chrome counts a frame from before it is sent, so its own count can be
   * higher.
   */
  unacknowledgedMax = 0;

  constructor(
    private readonly cdp: CDPSession,
    private readonly hasher: FrameHasher,
    private readonly grid: Roi | null,
    private readonly keepLastPng: boolean,
    private readonly offset: () => number | null,
  ) {}

  async start(): Promise<void> {
    this.cdp.on("Page.screencastFrame", (payload) => {
      if (this.#stopped) return;
      const seq = this.#received;
      this.#received += 1;
      const sessionId = payload.sessionId;
      const receivedMs = hrtimeMs();
      this.#unacknowledged += 1;
      this.unacknowledgedMax = Math.max(
        this.unacknowledgedMax,
        this.#unacknowledged,
      );
      void this.cdp
        .send("Page.screencastFrameAck", { sessionId })
        .then(() => {
          const ackedMs = hrtimeMs();
          this.#ackedMs[seq] = ackedMs;
          this.ackRoundTripMaxMs = Math.max(
            this.ackRoundTripMaxMs,
            ackedMs - receivedMs,
          );
        })
        .catch(() => {
          if (!this.#stopped)
            this.signals.push({
              atMs: hrtimeMs(),
              signal: { kind: "ack-failed", sessionId },
            });
        })
        .finally(() => {
          this.#unacknowledged -= 1;
        });
      const timestamp = payload.metadata.timestamp;
      if (typeof timestamp !== "number") {
        this.#failures.push(new Error("a screencast frame has no timestamp"));
        return;
      }
      if (this.keepLastPng && (this.#lastPng?.seq ?? -1) < seq)
        this.#lastPng = { seq, data: payload.data };
      this.#hashes.push(
        this.hasher.hash(payload.data, this.grid).then(
          (hashes) => {
            this.#frames[seq] = Object.freeze({
              ...hashes,
              seq,
              sessionId,
              epochSeconds: timestamp,
              receivedMs,
            });
            while (this.#frames[this.#contiguous] !== undefined)
              this.#contiguous += 1;
          },
          (error: unknown) => {
            this.#failures.push(
              error instanceof Error ? error : new Error(String(error)),
            );
          },
        ),
      );
    });
    this.cdp.on("Page.screencastVisibilityChanged", (payload) => {
      if (!payload.visible && !this.#stopped)
        this.signals.push({
          atMs: hrtimeMs(),
          signal: { kind: "screencast-hidden" },
        });
    });
    await startScreencast(this.cdp);
  }

  get complete(): boolean {
    return this.#contiguous === this.#received;
  }

  prefix(): HashedFrame[] {
    this.#throwIfFailed();
    const offset = this.offset();
    if (offset === null) return [];
    if (offset !== this.#mappedOffset) {
      this.#mapped.length = 0;
      this.#mappedOffset = offset;
    }
    while (this.#mapped.length < this.#contiguous) {
      const frame = this.#frames[this.#mapped.length];
      if (!frame) throw new Error("a contiguous frame is missing");
      this.#mapped.push(
        Object.freeze({
          ...frame,
          timestampMs: mapEpochSecondsToMonotonicMilliseconds(
            frame.epochSeconds,
            offset,
          ),
        }),
      );
    }
    return this.#mapped.slice();
  }

  lastPng(): Buffer | null {
    return this.#lastPng ? Buffer.from(this.#lastPng.data, "base64") : null;
  }

  /** When Chrome answered frame `seq`'s acknowledgement, on the host clock. */
  ackedMs(seq: number): number | null {
    return this.#ackedMs[seq] ?? null;
  }

  async drain(): Promise<HashedFrame[]> {
    await Promise.all(this.#hashes);
    return this.prefix();
  }

  async stop(): Promise<HashedFrame[]> {
    if (!this.#stopped) {
      this.#stopped = true;
      await this.cdp.send("Page.stopScreencast").catch(() => undefined);
    }
    return await this.drain();
  }

  #throwIfFailed(): void {
    const failure = this.#failures[0];
    if (failure) throw failure;
  }
}

/** One request as the page target's Network domain reported it. */
export interface RequestRecord {
  readonly url: string;
  readonly method: string;
  readonly type: string | null;
  /**
   * `Network.requestWillBeSent`'s `initiator.type`: `parser` for a request
   * the document's parser or its preload scanner started, `script` for one a
   * script started.
   */
  readonly initiator: string;
  status: number | null;
  /**
   * The response's protocol as Chrome negotiated it, `Network.Response`'s
   * `protocol`: `h2` or `http/1.1` here.
   */
  protocol: string | null;
  /** The response's `Content-Encoding` and `Content-Length` headers. */
  contentEncoding: string | null;
  contentLength: number | null;
  /**
   * `Network.loadingFinished`'s `encodedDataLength`: every byte the request
   * took on the wire, headers included. A request that never finished has 0.
   */
  encoded: number;
  /**
   * The sum of `Network.dataReceived`'s `encodedDataLength`. Chrome can leave
   * the tail of a body for `loadingFinished` to report, so this falls short of
   * the body by that tail.
   */
  chunkEncoded: number;
  /** The sum of `Network.dataReceived`'s `dataLength`. */
  decoded: number;
  state: "pending" | "finished" | "failed";
  failure: string | null;
  /**
   * When Chrome sent the request, received its response's headers, delivered
   * its last body bytes, and reported its end, in CDP monotonic milliseconds,
   * the timebase of a sample's `tNavMs`; null until each happens.
   */
  sentMs: number;
  respondedMs: number | null;
  lastDataMs: number | null;
  endedMs: number | null;
}

function headerValue(
  headers: Record<string, unknown>,
  name: string,
): string | null {
  for (const [key, value] of Object.entries(headers))
    if (key.toLowerCase() === name) return String(value);
  return null;
}

/** The document request, its clock mapping, and the bytes the page received. */
export class NetworkLedger {
  documentTimestampMs: number | null = null;
  /**
   * Chrome's monotonic clock less its wall clock at the document request. A
   * page's own stamps, `performance.timeOrigin` plus `performance.now()`, run
   * on the monotonic clock from there and map through this unchanged.
   */
  monotonicMinusEpochSeconds: number | null = null;
  /**
   * How far Chrome's wall clock has moved against its monotonic clock since
   * the page loaded, as the latest probe read it. Frame stamps are wall-clock,
   * so a probe maps them through the document request's offset less this.
   */
  wallShiftMs = 0;
  documentReceivedMs: number | null = null;
  /** The document response's `Cache-Control` header. */
  documentCacheControl: string | null = null;
  #documentRequestId: string | null = null;
  readonly #requests = new Map<string, RequestRecord>();

  constructor(
    cdp: Pick<CDPSession, "on">,
    private readonly url: string,
  ) {
    cdp.on("Network.requestWillBeSent", (payload) => {
      this.#requests.set(payload.requestId, {
        url: redactedUrl(payload.request.url),
        method: payload.request.method,
        type: payload.type ?? null,
        initiator: payload.initiator.type,
        status: null,
        protocol: null,
        contentEncoding: null,
        contentLength: null,
        encoded: 0,
        chunkEncoded: 0,
        decoded: 0,
        state: "pending",
        failure: null,
        sentMs: payload.timestamp * 1_000,
        respondedMs: null,
        lastDataMs: null,
        endedMs: null,
      });
      if (
        this.documentTimestampMs === null &&
        payload.type === "Document" &&
        payload.request.url === this.url
      ) {
        this.documentReceivedMs = hrtimeMs();
        this.#documentRequestId = payload.requestId;
        this.documentTimestampMs = payload.timestamp * 1_000;
        this.monotonicMinusEpochSeconds = payload.timestamp - payload.wallTime;
      }
    });
    cdp.on("Network.responseReceived", (payload) => {
      const request = this.#requests.get(payload.requestId);
      if (!request) return;
      const { headers, status } = payload.response;
      const length = headerValue(headers, "content-length");
      request.status = status;
      request.protocol = payload.response.protocol ?? null;
      request.respondedMs = payload.timestamp * 1_000;
      if (payload.requestId === this.#documentRequestId)
        this.documentCacheControl = headerValue(headers, "cache-control");
      request.contentEncoding = headerValue(headers, "content-encoding");
      request.contentLength =
        length !== null && /^[0-9]+$/u.test(length.trim())
          ? Number(length.trim())
          : null;
    });
    cdp.on("Network.dataReceived", (payload) => {
      const request = this.#requests.get(payload.requestId);
      if (!request) return;
      request.decoded += payload.dataLength;
      request.chunkEncoded += payload.encodedDataLength;
      request.lastDataMs = payload.timestamp * 1_000;
    });
    cdp.on("Network.loadingFinished", (payload) => {
      const request = this.#requests.get(payload.requestId);
      if (!request) return;
      request.encoded = payload.encodedDataLength;
      request.state = "finished";
      request.endedMs = payload.timestamp * 1_000;
    });
    cdp.on("Network.loadingFailed", (payload) => {
      const request = this.#requests.get(payload.requestId);
      if (!request) return;
      request.state = "failed";
      request.failure = payload.canceled
        ? `canceled: ${payload.errorText}`
        : payload.errorText;
      request.endedMs = payload.timestamp * 1_000;
    });
  }

  /** The wall-to-monotonic offset for frames stamped now, or null before the document request. */
  frameOffsetSeconds(): number | null {
    return this.monotonicMinusEpochSeconds === null
      ? null
      : this.monotonicMinusEpochSeconds - this.wallShiftMs / 1_000;
  }

  /**
   * Each wire figure counts a request once, by how it ended.
   * `unfinishedWireBytes` leaves out the headers of failed and pending
   * requests and any tail Chrome held back for a `loadingFinished` that never
   * came, so it is a lower bound.
   */
  totals() {
    const requests = [...this.#requests.values()];
    const finished = requests.filter((request) => request.state === "finished");
    const unfinished = requests.filter(
      (request) => request.state !== "finished",
    );
    const sum = (
      over: readonly RequestRecord[],
      bytes: (request: RequestRecord) => number,
    ) => over.reduce((total, request) => total + bytes(request), 0);
    const finishedWireBytes = sum(finished, (request) => request.encoded);
    const unfinishedWireBytes = sum(
      unfinished,
      (request) => request.chunkEncoded,
    );
    return {
      requests: requests.length,
      unfinished: unfinished.length,
      failed: requests.filter((request) => request.state === "failed").length,
      wireBytes: finishedWireBytes + unfinishedWireBytes,
      finishedWireBytes,
      unfinishedWireBytes,
      declaredBodyBytes: sum(requests, (request) => request.contentLength ?? 0),
      undeclaredBodies: requests.filter(
        (request) => request.contentLength === null,
      ).length,
      decodedBodyBytes: sum(requests, (request) => request.decoded),
      byRequest: requests.map((request) => ({ ...request })),
    };
  }
}

export function framesRecord(
  frames: readonly HashedFrame[],
  capture?: Capture,
) {
  return frames.map((frame) => ({
    seq: frame.seq,
    sessionId: frame.sessionId,
    epochSeconds: frame.epochSeconds,
    timestampMs: frame.timestampMs,
    receivedMs: frame.receivedMs,
    ...(capture ? { ackedMs: capture.ackedMs(frame.seq) } : {}),
    pngSha256: frame.pngSha256,
    full: frame.full,
    grid: frame.grid,
  }));
}

export function judgementRecord(judgement: StablePaintJudgement | null) {
  return judgement === null
    ? null
    : {
        matchMs: judgement.matchMs,
        contradiction: judgement.contradiction,
        holdWindowFrameCount: judgement.holdWindowFrameCount,
        droppedFrameSignals: judgement.droppedFrameSignals,
        framesJudged: judgement.framesJudged,
      };
}
