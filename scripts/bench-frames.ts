/**
 * Screencast frames for the benchmark: hashing them off the main
 * thread, and judging them under its stable-paint rule.
 *
 * A 1600x900 PNG takes tens of milliseconds to decode in JavaScript. Decoding
 * on the thread that acknowledges frames would delay the acknowledgements, and
 * Chrome skips capture while more than two frames are unacknowledged, so a
 * pool of worker threads decodes and hashes while the main thread acknowledges
 * each frame as it arrives.
 *
 * The judging is pure: the stable-paint detector is replayed over the hashed
 * frames, so a verdict is a function of the frames a sample record keeps.
 */
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  isMainThread,
  parentPort,
  Worker,
  workerData,
} from "node:worker_threads";
import { inflateSync } from "node:zlib";

/** The page viewport, in CSS pixels at device scale factor 1. */
export const VIEWPORT = Object.freeze({ width: 1600, height: 900 });

/** A region of the viewport, in CSS pixels. */
export interface Roi {
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
}

/** The stable-paint end event, as a run's summary records it. */
interface StablePaintRule {
  readonly kind: "first-uncontradicted-presented-frame";
  readonly holdWindowMilliseconds: number;
}

// The first reference-equal frame at or after T_nav, uncontradicted for 2,000 ms.
export const STABLE_PAINT_RULE: StablePaintRule = Object.freeze({
  kind: "first-uncontradicted-presented-frame",
  holdWindowMilliseconds: 2_000,
});

export const FULL_VIEWPORT_ROI: Roi = Object.freeze({
  x: 0,
  y: 0,
  width: VIEWPORT.width,
  height: VIEWPORT.height,
});

/**
 * The integer region covering an element's bounding rectangle, clipped to the
 * viewport. `hashDecodedRoi` indexes rows and columns, so a fractional edge is
 * rounded outwards rather than dropped.
 */
export function roiFromRect(rect: {
  readonly left: number;
  readonly top: number;
  readonly right: number;
  readonly bottom: number;
}): Roi {
  const x = Math.max(0, Math.floor(rect.left));
  const y = Math.max(0, Math.floor(rect.top));
  const right = Math.min(VIEWPORT.width, Math.ceil(rect.right));
  const bottom = Math.min(VIEWPORT.height, Math.ceil(rect.bottom));
  if (
    ![x, y, right, bottom].every(Number.isFinite) ||
    right <= x ||
    bottom <= y
  )
    throw new Error(
      `the region ${JSON.stringify(rect)} has no pixels inside the viewport`,
    );
  return Object.freeze({ x, y, width: right - x, height: bottom - y });
}

interface DecodedPng {
  readonly width: number;
  readonly height: number;
  readonly rgba: Buffer;
}

/** The SHA-256 of a region's RGBA rows, scaled to the PNG's device pixels. */
export function hashDecodedRoi(decoded: DecodedPng, roi: Roi): string {
  if (
    decoded.width % VIEWPORT.width !== 0 ||
    decoded.height % VIEWPORT.height !== 0 ||
    decoded.width / VIEWPORT.width !== decoded.height / VIEWPORT.height
  ) {
    throw new Error(
      `screencast PNG ${decoded.width}x${decoded.height} does not scale exactly from the frozen viewport`,
    );
  }
  const scale = decoded.width / VIEWPORT.width;
  const x = roi.x * scale;
  const y = roi.y * scale;
  const width = roi.width * scale;
  const height = roi.height * scale;
  const hash = createHash("sha256");
  for (let row = y; row < y + height; row += 1) {
    const start = (row * decoded.width + x) * 4;
    hash.update(decoded.rgba.subarray(start, start + width * 4));
  }
  return hash.digest("hex");
}

export function decodePng(png: Buffer): DecodedPng {
  const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  if (png.length < 33 || !png.subarray(0, 8).equals(signature)) {
    throw new Error("screencast payload is not a PNG");
  }
  let offset = 8;
  let width: number | undefined;
  let height: number | undefined;
  let colorType: number | undefined;
  const compressed: Buffer[] = [];
  let ended = false;
  while (offset + 12 <= png.length) {
    const length = png.readUInt32BE(offset);
    const type = png.toString("ascii", offset + 4, offset + 8);
    const dataStart = offset + 8;
    const dataEnd = dataStart + length;
    if (dataEnd + 4 > png.length) throw new Error("PNG chunk is truncated");
    const data = png.subarray(dataStart, dataEnd);
    if (type === "IHDR") {
      if (length !== 13 || width !== undefined)
        throw new Error("invalid PNG IHDR");
      width = data.readUInt32BE(0);
      height = data.readUInt32BE(4);
      const bitDepth = data[8];
      colorType = data[9];
      if (
        bitDepth !== 8 ||
        ![0, 2, 4, 6].includes(colorType ?? -1) ||
        data[10] !== 0 ||
        data[11] !== 0 ||
        data[12] !== 0
      ) {
        throw new Error("unsupported screencast PNG encoding");
      }
    } else if (type === "IDAT") {
      compressed.push(data);
    } else if (type === "IEND") {
      ended = true;
      offset = dataEnd + 4;
      break;
    }
    offset = dataEnd + 4;
  }
  if (
    !ended ||
    offset !== png.length ||
    width === undefined ||
    height === undefined ||
    colorType === undefined ||
    width <= 0 ||
    height <= 0 ||
    compressed.length === 0
  ) {
    throw new Error("incomplete screencast PNG");
  }
  const channels =
    colorType === 0 ? 1 : colorType === 2 ? 3 : colorType === 4 ? 2 : 4;
  const rowBytes = width * channels;
  const raw = inflateSync(Buffer.concat(compressed));
  if (raw.length !== (rowBytes + 1) * height) {
    throw new Error("screencast PNG scanline length is invalid");
  }
  const pixels = Buffer.allocUnsafe(rowBytes * height);
  let inputOffset = 0;
  for (let y = 0; y < height; y += 1) {
    const filter = raw[inputOffset] ?? -1;
    inputOffset += 1;
    const rowOffset = y * rowBytes;
    for (let x = 0; x < rowBytes; x += 1) {
      const source = raw[inputOffset + x] ?? 0;
      const left = x >= channels ? (pixels[rowOffset + x - channels] ?? 0) : 0;
      const above = y > 0 ? (pixels[rowOffset - rowBytes + x] ?? 0) : 0;
      const upperLeft =
        y > 0 && x >= channels
          ? (pixels[rowOffset - rowBytes + x - channels] ?? 0)
          : 0;
      pixels[rowOffset + x] = unfilterByte(
        filter,
        source,
        left,
        above,
        upperLeft,
      );
    }
    inputOffset += rowBytes;
  }
  const rgba = Buffer.allocUnsafe(width * height * 4);
  for (let pixel = 0; pixel < width * height; pixel += 1) {
    const source = pixel * channels;
    const target = pixel * 4;
    if (colorType === 0) {
      const gray = pixels[source] ?? 0;
      rgba[target] = gray;
      rgba[target + 1] = gray;
      rgba[target + 2] = gray;
      rgba[target + 3] = 255;
    } else if (colorType === 2) {
      rgba[target] = pixels[source] ?? 0;
      rgba[target + 1] = pixels[source + 1] ?? 0;
      rgba[target + 2] = pixels[source + 2] ?? 0;
      rgba[target + 3] = 255;
    } else if (colorType === 4) {
      const gray = pixels[source] ?? 0;
      rgba[target] = gray;
      rgba[target + 1] = gray;
      rgba[target + 2] = gray;
      rgba[target + 3] = pixels[source + 1] ?? 0;
    } else {
      rgba[target] = pixels[source] ?? 0;
      rgba[target + 1] = pixels[source + 1] ?? 0;
      rgba[target + 2] = pixels[source + 2] ?? 0;
      rgba[target + 3] = pixels[source + 3] ?? 0;
    }
  }
  return Object.freeze({ width, height, rgba });
}

function unfilterByte(
  filter: number,
  source: number,
  left: number,
  above: number,
  upperLeft: number,
): number {
  if (filter === 0) return source;
  if (filter === 1) return (source + left) & 0xff;
  if (filter === 2) return (source + above) & 0xff;
  if (filter === 3) return (source + Math.floor((left + above) / 2)) & 0xff;
  if (filter === 4) return (source + paeth(left, above, upperLeft)) & 0xff;
  throw new Error(`unsupported PNG filter ${filter}`);
}

function paeth(left: number, above: number, upperLeft: number): number {
  const prediction = left + above - upperLeft;
  const leftDistance = Math.abs(prediction - left);
  const aboveDistance = Math.abs(prediction - above);
  const upperLeftDistance = Math.abs(prediction - upperLeft);
  if (leftDistance <= aboveDistance && leftDistance <= upperLeftDistance)
    return left;
  return aboveDistance <= upperLeftDistance ? above : upperLeft;
}

interface FrameHashes {
  readonly pngSha256: string;
  /** The whole viewport's ROI hash. */
  readonly full: string;
  /** The grid panel's ROI hash, when the sample has a grid region. */
  readonly grid: string | null;
}

/** Which of Chrome's pictures a PNG is, so that a refusal can say. */
export type PictureSource =
  "screencast frame" | "restarted screencast frame" | "screenshot";

/**
 * `hashFrame`'s refusal of a picture for its size, as data: the error it
 * throws carries it as `fault`, and a worker's reply carries it beside the
 * message, since an error's own class does not survive the reply.
 */
export interface PictureSizeFault {
  readonly kind: "picture-size";
  readonly source: PictureSource;
  readonly width: number;
  readonly height: number;
}

export function hashFrame(
  png: Buffer,
  grid: Roi | null,
  source: PictureSource = "screencast frame",
): FrameHashes {
  const decoded = decodePng(png);
  // `hashDecodedRoi` refuses the same pictures, but names every one a
  // screencast PNG.
  if (
    decoded.width % VIEWPORT.width !== 0 ||
    decoded.height % VIEWPORT.height !== 0 ||
    decoded.width / VIEWPORT.width !== decoded.height / VIEWPORT.height
  ) {
    const fault: PictureSizeFault = {
      kind: "picture-size",
      source,
      width: decoded.width,
      height: decoded.height,
    };
    throw Object.assign(
      new Error(
        `${source} PNG ${String(decoded.width)}x${String(decoded.height)} does not scale exactly from the frozen ${String(VIEWPORT.width)}x${String(VIEWPORT.height)} viewport`,
      ),
      { fault },
    );
  }
  return Object.freeze({
    pngSha256: createHash("sha256").update(png).digest("hex"),
    full: hashDecodedRoi(decoded, FULL_VIEWPORT_ROI),
    grid: grid === null ? null : hashDecodedRoi(decoded, grid),
  });
}

interface HashRequest {
  readonly id: number;
  readonly base64: string;
  readonly grid: Roi | null;
  readonly source: PictureSource;
}

type HashReply =
  | ({ readonly id: number } & FrameHashes)
  | {
      readonly id: number;
      readonly error: string;
      readonly fault: PictureSizeFault | null;
    };

/** The size refusal an error carries, from `hashFrame` or a worker's reply. */
function pictureSizeFault(error: unknown): PictureSizeFault | null {
  const fault: unknown =
    typeof error === "object" && error !== null && "fault" in error
      ? error.fault
      : null;
  if (
    typeof fault !== "object" ||
    fault === null ||
    !("kind" in fault) ||
    fault.kind !== "picture-size"
  )
    return null;
  return fault as PictureSizeFault;
}

const WORKER_ROLE = "bench-frame-hasher";

if (
  !isMainThread &&
  parentPort &&
  (workerData as { role?: unknown } | null)?.role === WORKER_ROLE
) {
  const port = parentPort;
  port.on("message", (request: HashRequest) => {
    let reply: HashReply;
    try {
      reply = {
        id: request.id,
        ...hashFrame(
          Buffer.from(request.base64, "base64"),
          request.grid,
          request.source,
        ),
      };
    } catch (error) {
      reply = {
        id: request.id,
        error: error instanceof Error ? error.message : String(error),
        fault: pictureSizeFault(error),
      };
    }
    port.postMessage(reply);
  });
}

/** A fixed pool of worker threads running `hashFrame`. */
export class FrameHasher {
  readonly #workers: Worker[];
  readonly #pending = new Map<
    number,
    {
      readonly resolve: (hashes: FrameHashes) => void;
      readonly reject: (error: Error) => void;
    }
  >();
  #next = 0;

  constructor(size: number) {
    const register = pathToFileURL(
      resolve(fileURLToPath(import.meta.url), "../register.mjs"),
    ).href;
    this.#workers = Array.from({ length: size }, () => {
      const worker = new Worker(new URL(import.meta.url), {
        workerData: { role: WORKER_ROLE },
        // Only the TypeScript loader: a parent run under `node --test`
        // carries flags a worker must not inherit.
        execArgv: ["--import", register],
      });
      worker.on("message", (reply: HashReply) => {
        const waiter = this.#pending.get(reply.id);
        if (!waiter) return;
        this.#pending.delete(reply.id);
        if ("error" in reply)
          waiter.reject(
            Object.assign(
              new Error(`frame hashing failed: ${reply.error}`),
              reply.fault === null ? {} : { fault: reply.fault },
            ),
          );
        else
          waiter.resolve(
            Object.freeze({
              pngSha256: reply.pngSha256,
              full: reply.full,
              grid: reply.grid,
            }),
          );
      });
      worker.on("error", (error) => {
        for (const waiter of this.#pending.values()) waiter.reject(error);
        this.#pending.clear();
      });
      return worker;
    });
  }

  get pending(): number {
    return this.#pending.size;
  }

  hash(
    base64: string,
    grid: Roi | null,
    source: PictureSource = "screencast frame",
  ): Promise<FrameHashes> {
    const id = this.#next;
    this.#next += 1;
    const worker = this.#workers[id % this.#workers.length];
    if (!worker) throw new Error("the frame hasher has no workers");
    return new Promise<FrameHashes>((resolveHash, rejectHash) => {
      this.#pending.set(id, { resolve: resolveHash, reject: rejectHash });
      worker.postMessage({ id, base64, grid, source } satisfies HashRequest);
    });
  }

  async close(): Promise<void> {
    await Promise.all(this.#workers.map((worker) => worker.terminate()));
  }
}

/**
 * A Chrome timestamp in seconds since the epoch, on Chrome's monotonic clock
 * in milliseconds, given the monotonic clock minus the epoch clock in seconds.
 */
export function mapEpochSecondsToMonotonicMilliseconds(
  epochSeconds: number,
  monotonicMinusEpochSeconds: number,
): number {
  if (
    !Number.isFinite(epochSeconds) ||
    !Number.isFinite(monotonicMinusEpochSeconds)
  ) {
    throw new Error("Chrome clock mapping inputs must be finite");
  }
  return (epochSeconds + monotonicMinusEpochSeconds) * 1000;
}

/**
 * One presented frame. Chrome stamps a screencast frame with wall-clock
 * seconds since the epoch (`epochSeconds`); `timestampMs` is that stamp on
 * Chrome's monotonic clock, and `receivedMs` is the host's clock when the
 * frame arrived.
 */
export interface HashedFrame extends FrameHashes {
  readonly seq: number;
  readonly sessionId: number;
  readonly epochSeconds: number;
  readonly timestampMs: number;
  readonly receivedMs: number;
}

/**
 * The most a frame's arrival may trail its mapped presentation time. A frame
 * stamped on the wrong clock lands decades away; one that is merely slow to
 * arrive stays well inside this.
 */
export const FRAME_TRANSIT_LIMIT_MS = 60_000;

export interface FrameClockCheck {
  readonly frames: number;
  readonly minTransitMs: number | null;
  readonly maxTransitMs: number | null;
  readonly passed: boolean;
}

/**
 * Whether every frame's mapped timestamp falls at or before its arrival on
 * the host and no more than `FRAME_TRANSIT_LIMIT_MS` before it. A check over
 * no frames does not pass: it would say nothing about the clock.
 */
export function frameClockCheck(
  frames: readonly HashedFrame[],
): FrameClockCheck {
  let min: number | null = null;
  let max: number | null = null;
  for (const frame of frames) {
    const transit = frame.receivedMs - frame.timestampMs;
    min = min === null ? transit : Math.min(min, transit);
    max = max === null ? transit : Math.max(max, transit);
  }
  return Object.freeze({
    frames: frames.length,
    minTransitMs: min,
    maxTransitMs: max,
    passed:
      min !== null && max !== null && min >= 0 && max <= FRAME_TRANSIT_LIMIT_MS,
  });
}

/**
 * What the screencast can show about lost delivery. `Page.screencastFrame`'s
 * `sessionId` names the screencast session, not the frame: Chromium sets it
 * once per `StartScreencast`, so a frame with a different id comes from
 * another session. A capture Chrome skips because more than two frames are
 * unacknowledged is invisible here.
 */
export type DroppedFrameSignal =
  | {
      readonly kind: "session-id-changed";
      readonly expectedSessionId: number;
      readonly sessionId: number;
    }
  | {
      readonly kind: "non-increasing-timestamp";
      readonly previousTimestampMilliseconds: number;
      readonly timestampMilliseconds: number;
    }
  | { readonly kind: "screencast-hidden" }
  | { readonly kind: "ack-failed"; readonly sessionId: number };

interface StableFrameObservation {
  readonly sessionId: number;
  readonly monotonicTimestampMilliseconds: number;
  readonly roiSha256: string;
  readonly pageContractQualified: boolean;
  /**
   * `T_nav` on the same timeline, for a caller that times frames against one:
   * `null` while navigation has not been timed yet, and omitted by a caller
   * that decides which frames to feed in itself. The match may only be taken
   * from a frame at or after it.
   */
  readonly navigationTimestampMilliseconds?: number | null;
}

/**
 * What refused a sample whose match was contradicted inside the hold window:
 * the browser presented the reference and then something else, so the sample
 * has no `T_present` to report. It is neither a dropped frame nor a timeout,
 * and it keeps the frame that contradicted the match.
 */
interface HoldWindowContradiction {
  readonly holdWindowMilliseconds: number;
  readonly matchTimestampMilliseconds: number;
  readonly contradictingTimestampMilliseconds: number;
  readonly roiSha256: string;
  readonly pageContractQualified: boolean;
}

/**
 * The stable-paint rule as a state machine. It returns `T_present` on the
 * first reference-equal, page-contract-qualified frame at or after the
 * caller's `T_nav`, and then keeps judging: every later frame presented within
 * the hold window after that match must itself be reference-equal and
 * qualified, or the sample is refused with a `HoldWindowContradiction`. The
 * window is measured in the frames' own presentation timeline, never in host
 * wall time, so a slow read on the Node side can neither shorten nor lengthen
 * it.
 */
export class StableFrameDetector {
  readonly #expectedHash: string;
  readonly #droppedFrameSignals: DroppedFrameSignal[] = [];
  #previousSessionId: number | undefined;
  #previousTimestampMilliseconds: number | undefined;
  #matchTimestampMilliseconds: number | undefined;
  #holdWindowFrameCount = 0;
  #holdWindowContradiction: HoldWindowContradiction | null = null;

  constructor(expectedHash: string) {
    if (!/^[0-9a-f]{64}$/u.test(expectedHash)) {
      throw new Error("expected ROI SHA-256 must be lowercase hexadecimal");
    }
    this.#expectedHash = expectedHash;
  }

  get droppedFrameSignals(): readonly DroppedFrameSignal[] {
    return this.#droppedFrameSignals;
  }

  get invalid(): boolean {
    return this.#droppedFrameSignals.length !== 0;
  }

  /** The match `T_present`, before the hold window ends. */
  get matchTimestampMilliseconds(): number | null {
    return this.#matchTimestampMilliseconds ?? null;
  }

  /** Frames presented inside the hold window, contradicting or not. */
  get holdWindowFrameCount(): number {
    return this.#holdWindowFrameCount;
  }

  get holdWindowContradiction(): HoldWindowContradiction | null {
    return this.#holdWindowContradiction;
  }

  invalidate(signal: DroppedFrameSignal): void {
    this.#droppedFrameSignals.push(signal);
  }

  observe(observation: StableFrameObservation): number | null {
    const navigation = observation.navigationTimestampMilliseconds;
    if (
      !Number.isSafeInteger(observation.sessionId) ||
      observation.sessionId < 0 ||
      !Number.isFinite(observation.monotonicTimestampMilliseconds) ||
      (navigation !== undefined &&
        navigation !== null &&
        !Number.isFinite(navigation)) ||
      !/^[0-9a-f]{64}$/u.test(observation.roiSha256)
    ) {
      throw new Error("invalid screencast frame observation");
    }
    const previousSessionId = this.#previousSessionId;
    if (
      previousSessionId !== undefined &&
      observation.sessionId !== previousSessionId
    ) {
      this.#droppedFrameSignals.push({
        kind: "session-id-changed",
        expectedSessionId: previousSessionId,
        sessionId: observation.sessionId,
      });
    }
    const previousTimestampMilliseconds = this.#previousTimestampMilliseconds;
    if (
      previousTimestampMilliseconds !== undefined &&
      observation.monotonicTimestampMilliseconds <=
        previousTimestampMilliseconds
    ) {
      this.#droppedFrameSignals.push({
        kind: "non-increasing-timestamp",
        previousTimestampMilliseconds,
        timestampMilliseconds: observation.monotonicTimestampMilliseconds,
      });
    }
    this.#previousSessionId = observation.sessionId;
    this.#previousTimestampMilliseconds =
      observation.monotonicTimestampMilliseconds;

    const qualified =
      !this.invalid &&
      observation.roiSha256 === this.#expectedHash &&
      observation.pageContractQualified;
    const match = this.#matchTimestampMilliseconds;
    if (match === undefined) {
      // Every frame after the match can only confirm or contradict it, so it is
      // taken only from a frame at or after T_nav: latching on an earlier one,
      // which the caller discards, would leave the sample unable to match.
      const beforeNavigation =
        navigation === null ||
        (navigation !== undefined &&
          observation.monotonicTimestampMilliseconds < navigation);
      if (!qualified || beforeNavigation) return null;
      this.#matchTimestampMilliseconds =
        observation.monotonicTimestampMilliseconds;
      return observation.monotonicTimestampMilliseconds;
    }
    const sinceMatch = observation.monotonicTimestampMilliseconds - match;
    if (
      sinceMatch <= 0 ||
      sinceMatch > STABLE_PAINT_RULE.holdWindowMilliseconds
    ) {
      return null;
    }
    this.#holdWindowFrameCount += 1;
    if (!qualified && this.#holdWindowContradiction === null) {
      this.#holdWindowContradiction = Object.freeze({
        holdWindowMilliseconds: STABLE_PAINT_RULE.holdWindowMilliseconds,
        matchTimestampMilliseconds: match,
        contradictingTimestampMilliseconds:
          observation.monotonicTimestampMilliseconds,
        roiSha256: observation.roiSha256,
        pageContractQualified: observation.pageContractQualified,
      });
    }
    return null;
  }
}

export type RoiName = "full" | "grid";

export interface StablePaintJudgement {
  /** `T_present`, or null when no qualifying frame was presented. */
  readonly matchMs: number | null;
  readonly contradiction: HoldWindowContradiction | null;
  readonly holdWindowFrameCount: number;
  readonly droppedFrameSignals: readonly DroppedFrameSignal[];
  readonly framesJudged: number;
}

/**
 * The stable-paint rule replayed over recorded frames. Frames presented before
 * `fromMs` are left out; `navigationMs` is `T_nav` for a cold load. Every
 * frame is page-contract qualified, because the reference hash is taken only
 * from a render the oracle verified. `signals`, the capture's own dropped-frame
 * signals, are recorded after the frames, so the match a sample would have had
 * stays readable while the signal makes it `INVALID_RUN`.
 */
export function judgeStablePaint(
  frames: readonly HashedFrame[],
  roi: RoiName,
  expected: string,
  options: {
    readonly fromMs?: number;
    readonly navigationMs?: number;
    readonly signals?: readonly DroppedFrameSignal[];
  } = {},
): StablePaintJudgement {
  const detector = new StableFrameDetector(expected);
  let framesJudged = 0;
  for (const frame of frames) {
    if (options.fromMs !== undefined && frame.timestampMs < options.fromMs)
      continue;
    const hash = frame[roi];
    if (hash === null)
      throw new Error(`frame ${String(frame.seq)} has no ${roi} hash`);
    framesJudged += 1;
    detector.observe({
      sessionId: frame.sessionId,
      monotonicTimestampMilliseconds: frame.timestampMs,
      roiSha256: hash,
      pageContractQualified: true,
      ...(options.navigationMs === undefined
        ? {}
        : { navigationTimestampMilliseconds: options.navigationMs }),
    });
  }
  for (const signal of options.signals ?? []) detector.invalidate(signal);
  return Object.freeze({
    matchMs: detector.matchTimestampMilliseconds,
    contradiction: detector.holdWindowContradiction,
    holdWindowFrameCount: detector.holdWindowFrameCount,
    droppedFrameSignals: Object.freeze([...detector.droppedFrameSignals]),
    framesJudged,
  });
}

type Settle =
  | { readonly settled: false; readonly changed: boolean }
  | {
      readonly settled: true;
      readonly reference: string;
      readonly lastChangeMs: number;
    };

/**
 * Whether a region changed after `fromMs` and has since held still for
 * `quietMs` by `nowMs`. `before` is the region's hash when the change was
 * caused; a region that changed and came back to it has not settled on a new
 * state, so the caller keeps waiting and times out.
 */
export function settle(
  frames: readonly HashedFrame[],
  roi: RoiName,
  before: string,
  fromMs: number,
  nowMs: number,
  quietMs: number,
): Settle {
  let current = before;
  let lastChangeMs: number | null = null;
  for (const frame of frames) {
    if (frame.timestampMs < fromMs) continue;
    const hash = frame[roi];
    if (hash === null)
      throw new Error(`frame ${String(frame.seq)} has no ${roi} hash`);
    if (hash !== current) {
      current = hash;
      lastChangeMs = frame.timestampMs;
    }
  }
  if (lastChangeMs === null) return { settled: false, changed: false };
  if (current === before || nowMs - lastChangeMs < quietMs)
    return { settled: false, changed: true };
  return { settled: true, reference: current, lastChangeMs };
}
