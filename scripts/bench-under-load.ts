/// <reference lib="dom" />
/**
 * `--cold-under-load`'s samples: a first visit on a fresh profile with an
 * empty HTTP cache, taken while the realtime session's writes run. Its page
 * activates at the fence of the cut calque plans it, after the artifact and
 * the catch-up behind it, and is judged on the events grid, which only the
 * probes change: the load writes issues and comments.
 */
import { framesRecord, judgementRecord } from "./bench-capture.ts";
import { type Run, showPage } from "./bench-context.ts";
import { judgeStablePaint } from "./bench-frames.ts";
import { hrtimeMs, readEnvironment } from "./bench-host.ts";
import { readPageFacts, reportedError } from "./bench-page.ts";
import { runSql } from "./bench-postgres.ts";
import { log, redactedUrl, writeRecord } from "./bench-record.ts";
import {
  distribution,
  rankedValue,
  type Verdict,
  wireByPurpose,
  type WireByPurpose,
} from "./bench-results.ts";
import { coldInvalidity, type Invalidity, reachedMs } from "./bench-samples.ts";
import {
  awaitStablePaint,
  clockCheck,
  closeSession,
  navigate,
  openSession,
  tNavMs,
} from "./bench-session.ts";
import { delay } from "./demo-process.ts";

/**
 * How long the load runs before the first sample: past the re-cut the load's
 * first transactions start, and the one after it, so the samples see the
 * cuts the load keeps making and not the quiet one before it.
 */
export const UNDER_LOAD_LEAD_IN_MS = 120_000;

/**
 * The least time from one sample's start to the next one's, so the samples
 * fall at different points between two re-cuts.
 */
export const UNDER_LOAD_SPACING_MS = 15_000;

/**
 * Takes `count` samples, the first `UNDER_LOAD_LEAD_IN_MS` after the load
 * started at `loadStartedMs` and each at least `UNDER_LOAD_SPACING_MS` after
 * the one before it, while the load runs. `grid` is the realtime page's grid
 * once its probes ended.
 */
export async function underLoadSamples(
  run: Run,
  grid: string,
  loadStartedMs: number,
  count: number,
): Promise<UnderLoadResult[]> {
  const expected = {
    grid,
    events: Number(
      runSql(
        "SELECT count(*) FROM incidents.events;\n",
        run.sourceEnvironment,
      ).trim(),
    ),
  };
  const samples: UnderLoadResult[] = [];
  for (let n = 0; n < count; n += 1) {
    const wait =
      loadStartedMs +
      UNDER_LOAD_LEAD_IN_MS +
      n * UNDER_LOAD_SPACING_MS -
      hrtimeMs();
    if (wait > 0) await delay(wait);
    const sample = await underLoadSample(run, n, expected);
    writeRecord(run, sample.name, sample.record);
    const objects = sample.objects
      .map((object) => object.contentEncoding ?? "brotli")
      .join(", ");
    log(
      `under load ${String(n)}: ${sample.verdict}${sample.stablePaintMs === null ? "" : ` ${sample.stablePaintMs.toFixed(1)} ms`}, artifact ${String(sample.wire.artifact)} bytes (${objects || "no object"}), catch-up ${String(sample.wire.catchUp)} bytes${sample.failure ? ` (${sample.failure})` : ""}`,
    );
    samples.push(sample);
  }
  return samples;
}

/** What every sample's page must show, read before the first one. */
export interface UnderLoadExpectation {
  /** The grid's hash on the realtime page once its probes ended. */
  readonly grid: string;
  /** The events PostgreSQL holds, which no write of the load changes. */
  readonly events: number;
}

/** The artifact object a page read, as its response named it. */
export interface UnderLoadObject {
  /** `zstd`, or null for a brotli object, which is sent as it is stored. */
  readonly contentEncoding: string | null;
  readonly contentLength: number | null;
}

export interface UnderLoadResult {
  readonly name: string;
  readonly verdict: Verdict;
  readonly invalid: readonly Invalidity[];
  /** From `T_nav` to the grid's stable paint. */
  readonly stablePaintMs: number | null;
  readonly reachedMs: number | null;
  readonly failure: string | null;
  readonly wire: WireByPurpose;
  readonly objects: readonly UnderLoadObject[];
  readonly record: object;
}

async function underLoadSample(
  run: Run,
  n: number,
  expected: UnderLoadExpectation,
): Promise<UnderLoadResult> {
  const name = `under-load-${String(n)}`;
  const before = readEnvironment(run.postgres);
  showPage(run);
  const session = await openSession(run, true);
  try {
    const beforeGotoMs = await navigate(run, session);
    const failure = await awaitStablePaint(session, expected.grid, "grid");
    const frames = await session.capture.stop();
    await session.rss.stop();
    const facts = await readPageFacts(session.page);
    const navigation = tNavMs(session, facts);
    const clock = clockCheck(session, beforeGotoMs, frames);
    const judgement =
      navigation === null
        ? null
        : judgeStablePaint(frames, "grid", expected.grid, {
            navigationMs: navigation,
            signals: session.capture.signals.map((entry) => entry.signal),
          });
    const ended = failure ?? reportedError(facts, session.pageErrors);
    const problems =
      ended === null && facts.gridRows !== expected.events
        ? [
            `the grid shows ${String(facts.gridRows)} events and PostgreSQL holds ${String(expected.events)}`,
          ]
        : [];
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
    const stablePaintMs =
      judgement?.matchMs != null && navigation !== null
        ? judgement.matchMs - navigation
        : null;
    const network = session.network.totals();
    const objects = network.byRequest
      .filter((request) =>
        new URL(request.url).pathname.includes("/v1/artifact-objects/"),
      )
      .map((request) => ({
        contentEncoding: request.contentEncoding,
        contentLength: request.contentLength,
      }));
    const after = readEnvironment(run.postgres);
    run.readings.push(before, after);
    const record = {
      kind: "under-load",
      n,
      verdict,
      failure: ended,
      problems,
      invalid: invalid.map((entry) => entry.reason),
      stable_paint_ms: stablePaintMs,
      tNavMs: navigation,
      clock,
      judgement: judgementRecord(judgement),
      milestones: facts.coldLoad,
      marks: facts.marks,
      status: facts.status,
      pageErrors: session.pageErrors,
      memory: session.rss.summary(),
      network: {
        ...network,
        byRequest: network.byRequest.map((request) => ({
          ...request,
          url: redactedUrl(request.url),
        })),
      },
      wire: wireByPurpose(network.byRequest),
      objects,
      frames: framesRecord(frames, session.capture),
      environment: { before, after },
    };
    return {
      name,
      verdict,
      invalid,
      stablePaintMs,
      reachedMs: reachedMs(
        judgement,
        frames,
        "grid",
        expected.grid,
        navigation,
      ),
      failure: ended,
      wire: wireByPurpose(network.byRequest),
      objects,
      record,
    };
  } finally {
    await closeSession(session);
  }
}

/**
 * The samples in a run's summary: the grid's stable paint, a sample that did
 * not pass ranked as a failure, and, over the passed ones, the wire bytes by
 * what they carried and the artifact objects read by encoding.
 */
export function underLoadSummary(samples: readonly UnderLoadResult[]) {
  const passed = samples.filter((sample) => sample.verdict === "PASS");
  const bytes = (purpose: keyof WireByPurpose) =>
    distribution(passed.map((sample) => sample.wire[purpose]));
  const objects: Record<string, number> = {};
  for (const sample of passed)
    for (const object of sample.objects) {
      const key = object.contentEncoding ?? "brotli";
      objects[key] = (objects[key] ?? 0) + 1;
    }
  return {
    stable_paint_ms: distribution(
      samples.map((sample) =>
        rankedValue({ verdict: sample.verdict, valueMs: sample.stablePaintMs }),
      ),
    ),
    verdicts: samples.map((sample) => sample.verdict),
    bytes: {
      artifact: bytes("artifact"),
      catchUp: bytes("catchUp"),
      total: bytes("total"),
    },
    objects,
  };
}
