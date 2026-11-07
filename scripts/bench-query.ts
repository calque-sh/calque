/// <reference lib="dom" />
/** The post-load query: one interaction with the loaded page, timed from the input event to a stable paint of its result. */
import { Capture, framesRecord, judgementRecord } from "./bench-capture.ts";
import type { Run } from "./bench-context.ts";
import {
  frameClockCheck,
  type Roi,
  judgeStablePaint,
  mapEpochSecondsToMonotonicMilliseconds,
  settle,
  STABLE_PAINT_RULE,
} from "./bench-frames.ts";
import { hrtimeMs, rereadHostSleep } from "./bench-host.ts";
import { QUERY_VIEW, type QueryExpectation } from "./bench-oracle.ts";
import {
  type GridReading,
  readGridView,
  readPageState,
  reportedError,
} from "./bench-page.ts";
import { probeInvalidity } from "./bench-samples.ts";
import type { Verdict } from "./bench-results.ts";
import {
  HOLD_MARGIN_MS,
  readWallDriftMs,
  type Session,
} from "./bench-session.ts";
import { delay } from "./demo-process.ts";

/** How long the page has to show the query's result before the sample is a `DNC`. */
const QUERY_TIMEOUT_MS = 60_000;

/** How long the restarted screencast has to present its first frame. */
const FIRST_FRAME_TIMEOUT_MS = 10_000;

/** How long the grid's accessibility table has to catch up with the painted result. */
const GRID_READ_TIMEOUT_MS = 10_000;

/**
 * How far the input event's stamp, mapped onto Chrome's monotonic clock, may
 * lie outside the runner's calls that made it.
 */
const INPUT_STAMP_TOLERANCE_MS = 5;

/**
 * How far inside the grid panel the pictures judged begin. The panel's own
 * border is antialiased differently from one run to the next at the edge of a
 * repaint, by a few pixels, so the rows and controls inside it are judged.
 */
const PANEL_INSET_PX = 12;

/** Which of the page's globals holds the first input event's wall-clock stamp. */
const INPUT_STAMP = "__benchInputAt";

/**
 * Why a settled grid is not what PostgreSQL says the query returns, or
 * nothing: the controls, the total, and the ids in view against the first ids
 * of PostgreSQL's sorted matches.
 */
export function queryProblems(
  expected: QueryExpectation,
  grid: GridReading | null,
): string[] {
  if (grid === null) return ["the grid shows no result"];
  const problems: string[] = [];
  if (
    grid.view.level !== QUERY_VIEW.level ||
    grid.view.sort !== QUERY_VIEW.sort
  )
    problems.push(
      `the grid's controls read ${grid.view.level} and ${grid.view.sort}, not ${QUERY_VIEW.level} and ${QUERY_VIEW.sort}`,
    );
  if (grid.rows !== expected.rows)
    problems.push(
      `the grid shows ${String(grid.rows)} matches and PostgreSQL holds ${String(expected.rows)}`,
    );
  if (grid.top !== 0)
    problems.push(`the grid is scrolled to row ${String(grid.top)}, not 0`);
  if (grid.ids.length === 0 || grid.ids.length > expected.ids.length)
    problems.push(
      `the grid shows ${String(grid.ids.length)} ids and PostgreSQL was asked for ${String(expected.ids.length)}`,
    );
  else if (grid.ids.some((id, index) => id !== expected.ids[index]))
    problems.push("the ids in view differ from PostgreSQL's newest errors");
  return problems;
}

/**
 * The reasons a query sample is `INVALID_RUN`: a probe's, and the input
 * event's stamp lying outside the calls that issued it.
 */
export function queryInvalidity(
  judgement: Parameters<typeof probeInvalidity>[0],
  checks: Parameters<typeof probeInvalidity>[1] & {
    readonly inputAtMs: number | null;
    readonly calledAtMs: number;
    readonly returnedAtMs: number;
  },
) {
  const { inputAtMs, calledAtMs, returnedAtMs, ...probe } = checks;
  return [
    ...probeInvalidity(judgement, probe),
    ...(inputAtMs !== null &&
    inputAtMs >= calledAtMs - INPUT_STAMP_TOLERANCE_MS &&
    inputAtMs <= returnedAtMs + INPUT_STAMP_TOLERANCE_MS
      ? []
      : [
          {
            reason:
              "the input event's stamp lies outside the calls that made it",
            instrument: true,
          },
        ]),
  ];
}

export interface QueryOutcome {
  readonly verdict: Verdict;
  /** The input event to the stable paint of the result, or null. */
  readonly queryMs: number | null;
  readonly failure: string | null;
  readonly problems: readonly string[];
  /** The settled picture of the grid, which the reference pass keeps. */
  readonly settledGrid: string | null;
  readonly record: Readonly<Record<string, unknown>>;
}

/**
 * Sets the grid's Level control to `error` and its Order control to newest
 * first, on a page that has painted its working set completely, and times the
 * first presented frame that shows the result and stays uncontradicted for
 * the hold window, from the first `input` event's own stamp.
 *
 * A screencast of its own runs for the query: the cold sample's stopped when
 * its sample ended. Frames are compared by the inside of the grid panel alone. With no `reference`, the picture the
 * page settles on is kept as the reference for later samples and nothing is
 * timed against it.
 */
export async function queryPage(
  run: Run,
  session: Session,
  options: {
    readonly expected: QueryExpectation;
    /** The grid panel's region; the pictures judged are of its inside. */
    readonly gridRoi: Roi;
    /** The cold paint, which the page must still show before the query. */
    readonly before: string;
    /** The inside's picture of the result, or null in the reference pass. */
    readonly reference: string | null;
    readonly sessionClockPassed: boolean;
  },
): Promise<QueryOutcome> {
  const { page } = session;
  const { gridRoi } = options;
  const judgedRoi: Roi = {
    x: gridRoi.x + PANEL_INSET_PX,
    y: gridRoi.y + PANEL_INSET_PX,
    width: gridRoi.width - 2 * PANEL_INSET_PX,
    height: gridRoi.height - 2 * PANEL_INSET_PX,
  };
  const capture = new Capture(session.cdp, run.hasher, judgedRoi, false, () =>
    session.network.frameOffsetSeconds(),
  );
  const errorsBefore = session.pageErrors.length;
  const quietMs = STABLE_PAINT_RULE.holdWindowMilliseconds + HOLD_MARGIN_MS;
  const fail = async (failure: string): Promise<QueryOutcome> => ({
    verdict: "DNC",
    queryMs: null,
    failure,
    problems: [],
    settledGrid: null,
    record: {
      verdict: "DNC",
      failure,
      framesSeen: framesRecord(capture.prefix().slice(-5)),
      grid: await readGridView(page).catch(() => null),
    },
  });
  try {
    await capture.start();
    const firstFrameBy = hrtimeMs() + FIRST_FRAME_TIMEOUT_MS;
    while (!(capture.complete && capture.prefix().length > 0)) {
      if (hrtimeMs() > firstFrameBy)
        return await fail("the restarted screencast presented no frame");
      await delay(10);
    }
    const first = capture.prefix().at(-1);
    if (first?.full !== options.before || !first.grid)
      return await fail("the page no longer showed its complete paint");
    const beforeGrid = first.grid;
    const driftAtLoadMs = await readWallDriftMs(page);
    await page.evaluate((name) => {
      const target = globalThis as unknown as Record<string, number | null>;
      target[name] = null;
      document.addEventListener(
        "input",
        (event) => {
          target[name] ??= performance.timeOrigin + event.timeStamp;
        },
        { capture: true, once: false },
      );
    }, INPUT_STAMP);
    const wallShiftMs = (await readWallDriftMs(page)) - driftAtLoadMs;
    session.network.wallShiftMs = wallShiftMs;
    const signalsBefore = capture.signals.length;
    rereadHostSleep();
    const calledAtMs = hrtimeMs();
    await page.selectOption("select[data-control=level]", QUERY_VIEW.level);
    await page.selectOption("select[data-control=sort]", QUERY_VIEW.sort);
    const returnedAtMs = hrtimeMs();
    const stamp = await page.evaluate(
      (name) =>
        (globalThis as unknown as Record<string, number | null>)[name] ?? null,
      INPUT_STAMP,
    );
    const offset = session.network.frameOffsetSeconds();
    const inputAtMs =
      stamp === null || offset === null
        ? null
        : mapEpochSecondsToMonotonicMilliseconds(stamp / 1_000, offset);
    const fromMs = inputAtMs ?? calledAtMs;
    let reference = options.reference;
    let matchSeenAt: number | null = null;
    for (;;) {
      await delay(20);
      const now = hrtimeMs();
      const frames = capture.prefix();
      if (reference === null) {
        const state = settle(frames, "grid", beforeGrid, fromMs, now, quietMs);
        if (state.settled && capture.complete) {
          reference = state.reference;
          break;
        }
      } else {
        const seen = judgeStablePaint(frames, "grid", reference, { fromMs });
        if (seen.matchMs !== null) {
          matchSeenAt ??= now;
          if (now - matchSeenAt >= quietMs && capture.complete) break;
        }
      }
      if (now - calledAtMs > QUERY_TIMEOUT_MS)
        return await fail(
          `no frame showed the result within ${String(QUERY_TIMEOUT_MS)} ms of the input`,
        );
    }
    await capture.stop();
    const window = capture
      .prefix()
      .filter((frame) => frame.timestampMs >= fromMs);
    const signals = capture.signals
      .slice(signalsBefore)
      .map((entry) => entry.signal);
    const judgement = judgeStablePaint(window, "grid", reference, {
      fromMs,
      signals,
    });
    const wallDriftMs =
      (await readWallDriftMs(page)) - driftAtLoadMs - wallShiftMs;
    const frameClock = frameClockCheck(window);
    let grid = await readGridView(page);
    const gridBy = hrtimeMs() + GRID_READ_TIMEOUT_MS;
    while (
      queryProblems(options.expected, grid).length > 0 &&
      hrtimeMs() < gridBy
    ) {
      await delay(100);
      grid = await readGridView(page);
    }
    const problems = queryProblems(options.expected, grid);
    const error = reportedError(
      await readPageState(page),
      session.pageErrors.slice(errorsBefore),
    );
    const invalid = queryInvalidity(judgement, {
      sessionClockPassed: options.sessionClockPassed,
      frameClockPassed: frameClock.passed,
      wallDriftMs,
      hostSleptMs: rereadHostSleep(),
      inputAtMs,
      calledAtMs,
      returnedAtMs,
    });
    const verdict: Verdict =
      error !== null
        ? "DNC"
        : problems.length > 0
          ? "INCORRECT"
          : invalid.length > 0
            ? "INVALID_RUN"
            : judgement.matchMs === null
              ? "DNC"
              : "PASS";
    const queryMs =
      judgement.matchMs === null ? null : judgement.matchMs - fromMs;
    return {
      verdict,
      queryMs,
      failure: error,
      problems,
      settledGrid: reference,
      record: {
        verdict,
        failure: error,
        problems,
        invalid: invalid.map((entry) => entry.reason),
        query_ms: queryMs,
        view: QUERY_VIEW,
        calledAtMs,
        returnedAtMs,
        inputAtMs,
        wallShiftMs,
        wallDriftMs,
        frameClock,
        judgement: judgementRecord(judgement),
        grid: grid === null ? null : { ...grid, ids: grid.ids.length },
        frames: framesRecord(window),
      },
    };
  } finally {
    await capture.stop().catch(() => []);
  }
}
