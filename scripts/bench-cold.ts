/// <reference lib="dom" />
/** The reference pass and the cold samples. */
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { CDPSession } from "playwright";
import { brotliSample, type BrotliSample } from "./bench-brotli.ts";
import { startScreencast } from "./bench-browser.ts";
import {
  framesRecord,
  judgementRecord,
  type RequestRecord,
} from "./bench-capture.ts";
import {
  referenceOf,
  type ReferencePass,
  type Run,
  servedBrotli,
  showPage,
} from "./bench-context.ts";
import {
  type HashedFrame,
  hashFrame,
  judgeStablePaint,
  type PictureSource,
  roiFromRect,
  STABLE_PAINT_RULE,
} from "./bench-frames.ts";
import { hrtimeMs, readEnvironment } from "./bench-host.ts";
import {
  pageScreenshot,
  screenshotCalibration,
  withDeadline,
} from "./bench-launch.ts";
import { chartText, pageOracle } from "./bench-oracle.ts";
import { type QueryOutcome, queryPage } from "./bench-query.ts";
import {
  artifactRetryMarks,
  longTaskSummary,
  oraclePage,
  readPageFacts,
  readShown,
  reportedError,
  resourceTotals,
} from "./bench-page.ts";
import {
  log,
  redactedUrl,
  withoutHostPaths,
  writeRecord,
} from "./bench-record.ts";
import {
  summarizeTrace,
  type TraceSummary,
  type Verdict,
} from "./bench-results.ts";
import {
  coldInvalidity,
  reachedMs,
  type TakenSample,
} from "./bench-samples.ts";
import { RunnerStalls } from "./bench-sampling.ts";
import {
  awaitStablePaint,
  clockCheck,
  closeSession,
  COLD_TIMEOUT_MS,
  HOLD_MARGIN_MS,
  navigate,
  NO_PRESENT_FAILURE,
  openSession,
  type Session,
  tNavMs,
} from "./bench-session.ts";
import { delay } from "./demo-process.ts";

const TRACE_CATEGORIES = ["devtools.timeline", "v8", "toplevel"];

/**
 * An unmeasured load whose settled frame, once the oracle has verified the
 * page behind it, becomes the run's reference.
 */
export async function referencePass(run: Run): Promise<void> {
  const source = run.source;
  if (!source) throw new Error("the reference pass needs the source state");
  const before = readEnvironment(run.postgres);
  showPage(run);
  const session = await openSession(run, true);
  try {
    await navigate(run, session);
    const quietMs = STABLE_PAINT_RULE.holdWindowMilliseconds + HOLD_MARGIN_MS;
    const started = hrtimeMs();
    let lastHash: string | null = null;
    let lastChangeSeenAt = hrtimeMs();
    for (;;) {
      await delay(100);
      if (hrtimeMs() - started > COLD_TIMEOUT_MS)
        throw new Error("the reference pass did not settle within 120 s");
      const frames = session.capture.prefix();
      const hash = frames.at(-1)?.full ?? null;
      if (hash !== lastHash) {
        lastHash = hash;
        lastChangeSeenAt = hrtimeMs();
      }
      const state = await readPageFacts(session.page);
      const error = reportedError(state, session.pageErrors);
      if (error) throw new Error(`the reference pass failed: ${error}`);
      if (
        state.coldLoad?.chartAt != null &&
        state.issueShown &&
        hash !== null &&
        session.capture.complete &&
        hrtimeMs() - lastChangeSeenAt >= quietMs
      )
        break;
    }
    const frames = await session.capture.stop();
    await session.rss.stop();
    const facts = await readPageFacts(session.page);
    if (!facts.gridRect)
      throw new Error("the reference page has no grid panel");
    const gridRoi = roiFromRect(facts.gridRect);
    const png = session.capture.lastPng();
    const last = frames.at(-1);
    if (!png || !last) throw new Error("the reference pass captured no frame");
    const hashes = hashFrame(png, gridRoi);
    const calibration = await screenshotCalibration(session.cdp, gridRoi, {
      full: last.full,
      grid: hashes.grid,
    });
    const oracle = await pageOracle(
      oraclePage(session.page),
      run.tables,
      source.reference,
    );
    if (oracle.unsettled !== null)
      throw new Error(
        `the reference pass's working set did not hold still: ${oracle.unsettled}`,
      );
    const problems = [...oracle.problems];
    if (facts.gridRows !== source.events)
      problems.push(
        `the grid shows ${String(facts.gridRows)} events and PostgreSQL holds ${String(source.events)}`,
      );
    if (chartText(facts.chart) !== chartText(source.buckets))
      problems.push("the chart differs from GROUP BY status, priority");
    const after = readEnvironment(run.postgres);
    run.readings.push(before, after);
    writeRecord(run, "reference-0", {
      kind: "reference",
      browser: {
        version: session.browser.version(),
        launches: session.browserLaunches,
      },
      verdict: problems.length === 0 ? "PASS" : "INCORRECT",
      problems,
      fullReference: last.full,
      gridReference: hashes.grid,
      gridRoi,
      screenshotCalibration: calibration,
      gridRows: facts.gridRows,
      chart: facts.chart,
      oracle,
      coldLoad: facts.coldLoad,
      pageErrors: session.pageErrors,
      frames: framesRecord(frames),
      environment: { before, after },
    });
    if (problems.length > 0)
      throw new Error(
        `the reference pass failed the oracle: ${problems.join("; ")}`,
      );
    const queried = await queryPage(run, session, {
      expected: queryExpectationOf(run),
      gridRoi,
      before: last.full,
      reference: null,
      sessionClockPassed: true,
    });
    writeRecord(run, "reference-query", {
      kind: "reference-query",
      ...queried.record,
    });
    if (queried.verdict !== "PASS" && queried.verdict !== "INVALID_RUN")
      throw new Error(
        `the reference pass's query failed: ${queried.failure ?? queried.problems.join("; ")}`,
      );
    if (queried.settledGrid === null)
      throw new Error("the reference pass's query settled on no picture");
    run.reference = {
      full: last.full,
      grid: hashes.grid,
      gridRoi,
      png,
      screenshotCalibration: calibration,
      gridRows: facts.gridRows,
      chart: chartText(facts.chart),
      queryGrid: queried.settledGrid,
    };
  } finally {
    await closeSession(session);
  }
}

function queryExpectationOf(run: Run) {
  if (!run.queryExpectation)
    throw new Error("the run has no expectation of the post-load query");
  return run.queryExpectation;
}

/** One picture of the page at a missed deadline, hashed as the frames are. */
interface DeadlineCapture {
  readonly pngSha256: string | null;
  readonly full: string | null;
  readonly grid: string | null;
  /** Chrome's wall-clock stamp, for a screencast frame. */
  readonly epochSeconds: number | null;
  readonly matchesReference: boolean | null;
  readonly matchesGridReference: boolean | null;
  readonly matchesLastFrame: boolean | null;
  readonly error: string | null;
}

async function deadlineCapture(
  run: Run,
  reference: ReferencePass,
  last: HashedFrame | undefined,
  source: PictureSource,
  read: () => Promise<{ data: string; epochSeconds: number | null }>,
): Promise<{ readonly record: DeadlineCapture; readonly png: Buffer | null }> {
  try {
    const { data, epochSeconds } = await read();
    const hashes = await run.hasher.hash(data, reference.gridRoi, source);
    return {
      png: Buffer.from(data, "base64"),
      record: {
        pngSha256: hashes.pngSha256,
        full: hashes.full,
        grid: hashes.grid,
        epochSeconds,
        matchesReference: hashes.full === reference.full,
        matchesGridReference:
          reference.grid === null || hashes.grid === null
            ? null
            : hashes.grid === reference.grid,
        matchesLastFrame: last === undefined ? null : hashes.full === last.full,
        error: null,
      },
    };
  } catch (error) {
    return {
      png: null,
      record: {
        pngSha256: null,
        full: null,
        grid: null,
        epochSeconds: null,
        matchesReference: null,
        matchesGridReference: null,
        matchesLastFrame: null,
        error: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

interface ScreencastFrame {
  readonly data: string;
  readonly sessionId: number;
  readonly metadata: { readonly timestamp?: number };
}

/**
 * The first frame of a screencast started again on the stopped page. A
 * static page presents nothing new, so this frame is Chrome's picture of its
 * current state through the same path the judged frames took.
 */
async function restartedScreencastFrame(
  cdp: Pick<CDPSession, "send" | "on" | "off">,
): Promise<{ data: string; epochSeconds: number | null }> {
  let onFrame: ((payload: ScreencastFrame) => void) | undefined;
  try {
    const frame = new Promise<ScreencastFrame>((resolveFrame) => {
      onFrame = resolveFrame;
      cdp.on("Page.screencastFrame", resolveFrame);
    });
    await startScreencast(cdp);
    const payload = await withDeadline(frame, "frame after the restart");
    await cdp
      .send("Page.screencastFrameAck", { sessionId: payload.sessionId })
      .catch(() => undefined);
    return {
      data: payload.data,
      epochSeconds: payload.metadata.timestamp ?? null,
    };
  } finally {
    if (onFrame) cdp.off("Page.screencastFrame", onFrame);
    await cdp.send("Page.stopScreencast").catch(() => undefined);
  }
}

/**
 * The page at a cold sample's missed deadline, read after the screencast
 * stopped, so the judged frames stay as they were. A capture that matches
 * the reference while the last screencast frame does not says the page
 * reached the reference and the screencast did not deliver it; one that
 * matches the last frame says the page stopped short of it.
 *
 * - `screenshot`: `Page.captureScreenshot`.
 * - `restartedScreencast`: the first frame of a screencast started again,
 *   which checks the screenshot through the capture path itself.
 * - `animationFrameMs`: how long the page took to run two
 *   `requestAnimationFrame` callbacks after both, or null if it did not
 *   within 10 s: whether its rendering loop was still running.
 */
async function deadlineReadings(
  run: Run,
  session: Session,
  last: HashedFrame | undefined,
) {
  const reference = referenceOf(run);
  const screenshot = await deadlineCapture(
    run,
    reference,
    last,
    "screenshot",
    async () => ({
      data: await pageScreenshot(session.cdp),
      epochSeconds: null,
    }),
  );
  const restartedScreencast = await deadlineCapture(
    run,
    reference,
    last,
    "restarted screencast frame",
    () => restartedScreencastFrame(session.cdp),
  );
  const animationFrameMs = await withDeadline(
    session.page.evaluate(
      () =>
        new Promise<number>((resolveFrames) => {
          const started = performance.now();
          requestAnimationFrame(() =>
            requestAnimationFrame(() =>
              resolveFrames(performance.now() - started),
            ),
          );
        }),
    ),
    "animation frame",
  ).catch(() => null);
  return { screenshot, restartedScreencast, animationFrameMs };
}

/**
 * A `DNC` sample's pictures under the run's `frames/`: the screenshot and the
 * restarted screencast's frame at its deadline, its last screencast frame,
 * and the run's reference, written once. Returns each one's path in the run
 * directory, or null for one it did not have.
 */
function saveMissedFrames(
  run: Run,
  name: string,
  pictures: {
    readonly screenshot: Buffer | null;
    readonly restartedScreencast: Buffer | null;
    readonly lastFrame: Buffer | null;
  },
) {
  const directory = join(run.directory, "frames");
  mkdirSync(directory, { recursive: true });
  const save = (file: string, png: Buffer | null) => {
    if (!png) return null;
    writeFileSync(join(directory, file), png);
    return `frames/${file}`;
  };
  const reference = "reference.png";
  return {
    screenshot: save(`${name}-screenshot.png`, pictures.screenshot),
    restartedScreencast: save(
      `${name}-restarted-screencast.png`,
      pictures.restartedScreencast,
    ),
    lastFrame: save(`${name}-last-frame.png`, pictures.lastFrame),
    reference: existsSync(join(directory, reference))
      ? `frames/${reference}`
      : save(reference, referenceOf(run).png),
  };
}

type ColdKind = "warmup" | "cold" | "traced";

export interface ColdResult extends TakenSample {
  readonly kind: ColdKind;
  readonly n: number;
  /** The record's name under `samples/`. */
  readonly name: string;
  readonly coldMs: number | null;
  /** `reachedMs` from `T_nav`, which the list of samples taken again reports. */
  readonly reachedMs: number | null;
  /** What ended the sample before its match, if anything did. */
  readonly failure: string | null;
  /** The sample's record, which `settleCold` writes. */
  readonly record: Readonly<Record<string, unknown>>;
  readonly artifactRetryMarks: readonly string[];
  readonly peakRssBytes: number | null;
  readonly longestTaskMs: number | null;
  readonly coldLoad: Record<string, number | null> | null;
  /** The ledger's figures of the same names. */
  readonly wireBytes: number;
  readonly unfinishedWireBytes: number;
  readonly declaredBodyBytes: number;
  readonly decodedBodyBytes: number;
  /**
   * The ledger's requests, where the preflight, page coding and edge
   * protocol witnesses look.
   */
  readonly requests: readonly RequestRecord[];
  readonly trace: TraceSummary | null;
  /**
   * The post-load query on the sample's loaded page, or null where the sample
   * took none: a warm-up, a traced sample, or one that did not finish.
   */
  readonly query: QueryOutcome | null;
  /**
   * What the sample received of calque's brotli object, packed or plain;
   * absent where the run's pages load none (`servedBrotli`).
   */
  readonly brotli?: BrotliSample;
}

export async function coldSample(
  run: Run,
  kind: ColdKind,
  n: number,
): Promise<ColdResult> {
  const source = run.source;
  if (!source) throw new Error("a cold sample needs the source state");
  const reference = referenceOf(run);
  const expected = reference.full;
  const before = readEnvironment(run.postgres);
  const name = `${kind}-${String(n)}`;
  showPage(run);
  const session = await openSession(run, true);
  const stalls = new RunnerStalls();
  try {
    if (kind === "traced")
      await session.browser.startTracing(session.page, {
        categories: run.options.diagnostic
          ? [
              ...TRACE_CATEGORIES,
              "blink.user_timing",
              "disabled-by-default-v8.cpu_profiler",
            ]
          : TRACE_CATEGORIES,
      });
    stalls.start();
    const beforeGotoMs = await navigate(run, session);
    const failure = await awaitStablePaint(session, expected);
    const frames = await session.capture.stop();
    const runner = stalls.stop();
    await session.rss.stop();
    const rawTrace =
      kind === "traced"
        ? (await session.browser.stopTracing()).toString("utf8")
        : null;
    if (rawTrace !== null && run.options.diagnostic)
      writeFileSync(
        join(run.directory, "samples", `${name}.trace.json`),
        withoutHostPaths(rawTrace),
      );
    const trace =
      rawTrace === null ? null : summarizeTrace(JSON.parse(rawTrace));
    const deadline =
      failure === NO_PRESENT_FAILURE
        ? await deadlineReadings(run, session, frames.at(-1))
        : null;
    const facts = await readPageFacts(session.page);
    const shown = await readShown(session.page);
    // A page that carries a bearer must never be cached.
    const { documentCacheControl } = session.network;
    if (documentCacheControl !== "no-store")
      throw new Error(
        `${name}'s page was sent with Cache-Control ${String(documentCacheControl)}, not no-store`,
      );
    const navigation = tNavMs(session, facts);
    const clock = clockCheck(session, beforeGotoMs, frames);
    const signals = session.capture.signals.map((entry) => entry.signal);
    const judgement =
      navigation === null
        ? null
        : judgeStablePaint(frames, "full", expected, {
            navigationMs: navigation,
            signals,
          });
    const stopped = failure ?? reportedError(facts, session.pageErrors);
    const oracle =
      stopped === null
        ? await pageOracle(
            oraclePage(session.page),
            run.tables,
            source.reference,
          )
        : null;
    const ended = stopped ?? oracle?.unsettled ?? null;
    const problems = [...(oracle?.problems ?? [])];
    if (ended === null && facts.gridRows !== reference.gridRows)
      problems.push(
        `the grid shows ${String(facts.gridRows)} events, the reference pass ${String(reference.gridRows)}`,
      );
    if (ended === null && chartText(facts.chart) !== reference.chart)
      problems.push("the chart differs from the reference pass");
    const invalid = coldInvalidity(judgement, clock.passed);
    const verdict: Verdict =
      ended !== null
        ? "DNC"
        : problems.length > 0
          ? "INCORRECT"
          : invalid.length > 0
            ? "INVALID_RUN"
            : judgement?.matchMs == null
              ? "DNC"
              : "PASS";
    const coldMs =
      judgement?.matchMs != null && navigation !== null
        ? judgement.matchMs - navigation
        : null;
    const longTasks = longTaskSummary(facts);
    const rss = session.rss.summary();
    const network = session.network.totals();
    const published = servedBrotli(run);
    const brotliRead =
      published === null || session.decoderRequests === null
        ? null
        : brotliSample(published, {
            requests: network.byRequest,
            decoder: await session.decoderRequests.read(facts.timeOrigin),
            navigationMs: navigation,
          });
    const savedFrames =
      verdict === "DNC"
        ? saveMissedFrames(run, name, {
            screenshot: deadline?.screenshot.png ?? null,
            restartedScreencast: deadline?.restartedScreencast.png ?? null,
            lastFrame: session.capture.lastPng(),
          })
        : null;
    const after = readEnvironment(run.postgres);
    run.readings.push(before, after);
    // Everything above was read from the cold load. The query comes last, so
    // it moves none of those readings.
    const query =
      kind === "cold" && ended === null
        ? await queryPage(run, session, {
            expected: queryExpectationOf(run),
            gridRoi: reference.gridRoi,
            before: expected,
            reference: reference.queryGrid,
            sessionClockPassed: clock.passed,
          })
        : null;
    const retryMarks = artifactRetryMarks(facts.marks);
    const record = {
      kind,
      n,
      documentCacheControl,
      browser: {
        version: session.browser.version(),
        launches: session.browserLaunches,
      },
      verdict,
      failure: ended,
      problems,
      invalid: invalid.map((entry) => entry.reason),
      artifactRetryMarks: retryMarks,
      complete_stable_paint_ms: coldMs,
      tNavMs: navigation,
      timeOrigin: facts.timeOrigin,
      clock,
      judgement: judgementRecord(judgement),
      milestones: facts.coldLoad,
      marks: facts.marks,
      shown,
      status: facts.status,
      pageErrors: session.pageErrors,
      memory: rss,
      longTasks,
      network,
      resourceTiming: {
        totals: resourceTotals(facts),
        entries: facts.resources.map((entry) => ({
          ...entry,
          name: redactedUrl(entry.name),
        })),
      },
      trace,
      oracle,
      ackRoundTripMaxMs: session.capture.ackRoundTripMaxMs,
      unacknowledgedMax: session.capture.unacknowledgedMax,
      runner,
      deadline:
        deadline === null
          ? null
          : {
              screenshot: deadline.screenshot.record,
              restartedScreencast: deadline.restartedScreencast.record,
              animationFrameMs: deadline.animationFrameMs,
            },
      savedFrames,
      frames: framesRecord(frames, session.capture),
      environment: { before, after },
      query: query?.record ?? null,
      ...(brotliRead === null ? {} : { brotli: brotliRead }),
    };
    return {
      kind,
      n,
      name,
      verdict,
      invalid,
      coldMs,
      reachedMs: reachedMs(judgement, frames, "full", expected, navigation),
      failure: ended,
      record,
      artifactRetryMarks: retryMarks,
      peakRssBytes: rss.browserPeakBytes,
      longestTaskMs: longTasks.longestMs,
      coldLoad: facts.coldLoad,
      wireBytes: network.wireBytes,
      unfinishedWireBytes: network.unfinishedWireBytes,
      declaredBodyBytes: network.declaredBodyBytes,
      decodedBodyBytes: network.decodedBodyBytes,
      requests: network.byRequest,
      trace,
      query,
      ...(brotliRead === null ? {} : { brotli: brotliRead }),
    };
  } finally {
    stalls.stop();
    await closeSession(session);
  }
}

/** How many of `samples` left a mark of asking for the artifact again. */
export function withRetryMarks(samples: readonly ColdResult[]): number {
  return samples.filter((sample) => sample.artifactRetryMarks.length > 0)
    .length;
}

/**
 * Writes a cold sample's record, saying whether it is taken again, and logs
 * it. A sample taken again stays on disk under its own number.
 */
export function settleCold(
  run: Run,
  sample: ColdResult,
  retake: readonly string[] | null,
): void {
  writeRecord(run, sample.name, { ...sample.record, retaken: retake !== null });
  log(
    `${sample.kind} ${String(sample.n)}: ${sample.verdict}${sample.coldMs === null ? "" : ` ${sample.coldMs.toFixed(1)} ms`}${sample.failure ? ` (${sample.failure})` : ""}${retake ? `, taken again (${retake.join("; ")})` : ""}`,
  );
}
