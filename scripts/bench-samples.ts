/** How samples are judged and taken again: invalidity, retakes and the value a sample reached. */
import {
  type HashedFrame,
  type RoiName,
  type StablePaintJudgement,
} from "./bench-frames.ts";
import { HOST_SLEEP_TOLERANCE_MS } from "./bench-host.ts";
import type { Verdict } from "./bench-results.ts";

/** How many samples of one kind a run takes again. */
export const RETAKES = 3;

/**
 * How far Chrome's wall clock may move against its monotonic clock during a
 * probe before the probe stops trusting its frame mapping.
 */
const WALL_DRIFT_LIMIT_MS = 5;

/** One reason a sample is `INVALID_RUN`. */
interface Invalidity {
  readonly reason: string;
  /**
   * Whether the reason speaks of the instrument alone: a dropped-frame signal
   * or a failed clock check. A frame that contradicts the match speaks of the
   * page, and is not one.
   */
  readonly instrument: boolean;
  /**
   * A reason read once for a whole realtime session, which every probe of it
   * would meet again: such a probe is not taken again.
   */
  readonly sessionWide?: boolean;
}

/** What `takeSamples` reads of a sample to judge it. */
export interface TakenSample {
  readonly verdict: Verdict;
  readonly invalid: readonly Invalidity[];
  /** A probe whose grid did not settle in time: the loop ends there. */
  readonly timedOut?: boolean;
}

/** What `takeSamples` made of one sample it took. */
export interface Taken {
  /** The sample's number, counting every sample the loop took. */
  readonly n: number;
  readonly warmup: boolean;
  /** The judged slot the sample was taken to fill, or null for a warm-up. */
  readonly slot: number | null;
  /** Why the sample is taken again, or null when it is not. */
  readonly retake: readonly string[] | null;
}

export interface Retaken {
  readonly n: number;
  /** The judged slot it was taken to fill, which a later sample took. */
  readonly slot: number;
  readonly reasons: readonly string[];
  /**
   * What the sample reached, whatever its verdict (`reachedMs`): from `T_nav`
   * for a cold sample and from `t_before` for a probe, to its match or, when a
   * dropped-frame signal stopped the match, to the earliest-stamped frame
   * showing the expected state; otherwise null.
   */
  readonly valueMs: number | null;
}

/**
 * Takes `plan.warmups` warm-ups, then samples until `plan.judged` of them are
 * judged. A sample that is `INVALID_RUN` for instrument reasons alone is taken
 * again under the next number, up to `plan.retakes` times; past that bound it
 * is judged as it came and ranks +∞. The loop ends at the first sample that
 * timed out: the load behind a probe runs until the probes end, so a session
 * that went on would keep loading a page that is already behind.
 */
export async function takeSamples<S extends TakenSample>(
  plan: {
    readonly warmups: number;
    readonly judged: number;
    readonly retakes: number;
    /**
     * Typed at the call, as a `TakenSample`: an untyped parameter would fix
     * `S` before `take` is read.
     */
    readonly valueMs: (sample: S) => number | null;
  },
  take: (n: number, slot: number | null) => Promise<S>,
  settle: (sample: S, taken: Taken) => void = () => undefined,
): Promise<{
  /** Every sample taken, warm-ups first. */
  readonly samples: S[];
  readonly judged: S[];
  readonly retaken: Retaken[];
  readonly timedOutAt: number | null;
}> {
  const samples: S[] = [];
  const judged: S[] = [];
  const retaken: Retaken[] = [];
  for (let n = 0; ; n += 1) {
    const warmup = n < plan.warmups;
    if (!warmup && judged.length === plan.judged)
      return { samples, judged, retaken, timedOutAt: null };
    const slot = warmup ? null : judged.length;
    const sample = await take(n, slot);
    samples.push(sample);
    const retake =
      slot === null || retaken.length === plan.retakes
        ? null
        : retakeReasons(sample);
    settle(sample, { n, warmup, slot, retake });
    if (slot !== null && retake)
      retaken.push({
        n,
        slot,
        reasons: retake,
        valueMs: plan.valueMs(sample),
      });
    else if (slot !== null) judged.push(sample);
    if (sample.timedOut === true)
      return { samples, judged, retaken, timedOutAt: n };
  }
}

const CONTRADICTED: Invalidity = {
  reason: "stable presented frame contradicted within the hold window",
  instrument: false,
};

const DROPPED_FRAME: Invalidity = {
  reason: "a dropped-frame signal fired",
  instrument: true,
};

type HoldJudgement = Pick<
  StablePaintJudgement,
  "contradiction" | "droppedFrameSignals"
> | null;

/** Why a cold sample is `INVALID_RUN`, if it is, whatever else it is. */
export function coldInvalidity(
  judgement: HoldJudgement,
  clockPassed: boolean,
): Invalidity[] {
  return [
    ...(judgement?.contradiction ? [CONTRADICTED] : []),
    ...((judgement?.droppedFrameSignals.length ?? 0) > 0
      ? [DROPPED_FRAME]
      : []),
    ...(clockPassed
      ? []
      : [{ reason: "the clock check failed", instrument: true }]),
  ];
}

/**
 * Why a probe is `INVALID_RUN`, if it is. Its frames map to the probe's clock
 * through the offset read when the page loaded, moved by the wall clock's
 * drift read as the probe began, so Chrome's wall clock drifting from its
 * monotonic clock during the probe fails a clock check too, and so does the
 * host sleeping since the previous probe.
 */
export function probeInvalidity(
  judgement: HoldJudgement,
  checks: {
    readonly sessionClockPassed: boolean;
    readonly frameClockPassed: boolean;
    readonly wallDriftMs: number;
    readonly hostSleptMs: number;
  },
): Invalidity[] {
  return [
    ...(judgement?.contradiction ? [CONTRADICTED] : []),
    ...((judgement?.droppedFrameSignals.length ?? 0) > 0
      ? [DROPPED_FRAME]
      : []),
    ...(checks.sessionClockPassed
      ? []
      : [
          {
            reason: "the session's clock check failed",
            instrument: true,
            sessionWide: true,
          },
        ]),
    ...(checks.frameClockPassed
      ? []
      : [{ reason: "the probe's frame clock check failed", instrument: true }]),
    ...(Math.abs(checks.wallDriftMs) > WALL_DRIFT_LIMIT_MS
      ? [
          {
            reason: `Chrome's wall clock moved ${checks.wallDriftMs.toFixed(1)} ms against its monotonic clock during the probe`,
            instrument: true,
          },
        ]
      : []),
    ...(checks.hostSleptMs < HOST_SLEEP_TOLERANCE_MS
      ? []
      : [
          {
            reason: `the host slept ${checks.hostSleptMs.toFixed(0)} ms since the previous probe`,
            instrument: true,
          },
        ]),
  ];
}

/**
 * The reasons to take a sample again: its invalidity, if every reason is the
 * instrument's and none was read once for the whole session.
 */
function retakeReasons(sample: TakenSample): readonly string[] | null {
  return sample.verdict === "INVALID_RUN" &&
    sample.invalid.length > 0 &&
    sample.invalid.every(
      (entry) => entry.instrument && entry.sessionWide !== true,
    )
    ? sample.invalid.map((entry) => entry.reason)
    : null;
}

/**
 * What a sample reached, from `fromMs`: its `T_present`, or, when a
 * dropped-frame signal stopped the match, the earliest-stamped frame at or
 * after `fromMs` whose region shows the expected state. Without a match or a
 * dropped-frame signal it is null: a contradicted or unreached state has no
 * value to report. The list of samples taken again reports it; no verdict
 * reads it.
 */
export function reachedMs(
  judgement: Pick<
    StablePaintJudgement,
    "matchMs" | "droppedFrameSignals"
  > | null,
  frames: readonly HashedFrame[],
  roi: RoiName,
  expected: string | null,
  fromMs: number | null,
): number | null {
  if (fromMs === null || judgement === null) return null;
  if (judgement.matchMs !== null) return judgement.matchMs - fromMs;
  if (judgement.droppedFrameSignals.length === 0 || expected === null)
    return null;
  let earliest: number | null = null;
  for (const frame of frames)
    if (
      frame.timestampMs >= fromMs &&
      frame[roi] === expected &&
      (earliest === null || frame.timestampMs < earliest)
    )
      earliest = frame.timestampMs;
  return earliest === null ? null : earliest - fromMs;
}
