/// <reference lib="dom" />
/** The realtime session: the load, the probes, the final oracle and the background clients. */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Page } from "playwright";
import {
  BackgroundClients,
  type BackgroundReport,
  type BootstrapStart,
} from "./bench-background.ts";
import type { CalqueServer } from "./bench-calque.ts";
import { framesRecord, judgementRecord } from "./bench-capture.ts";
import {
  judgeShaping,
  type KernelShapingWitness,
  loadTransactionsPerSecond,
  shapingFloorMs,
  shapingLine,
  type ShapingWitness,
  unshapedFailure,
} from "./bench-conditions.ts";
import { referenceOf, type Run, showPage } from "./bench-context.ts";
import {
  frameClockCheck,
  judgeStablePaint,
  mapEpochSecondsToMonotonicMilliseconds,
  settle,
  STABLE_PAINT_RULE,
} from "./bench-frames.ts";
import {
  hrtimeMs,
  processCpuSeconds,
  readEnvironment,
  rereadHostSleep,
} from "./bench-host.ts";
import { NetworkMemoryVoid } from "./bench-network-memory.ts";
import {
  BENCH_ROW_COUNTS,
  chartText,
  LOAD_ROW_CHANGES_PER_TRANSACTION,
  loadTransactionSql,
  type OracleResult,
  pageOracle,
  probeInsertSql,
  probeSessionSql,
  readSourceState,
} from "./bench-oracle.ts";
import { bootstrapElement, PAGE_ROOT } from "./bench-page-server.ts";
import {
  artifactRetryMarks,
  type BenchWindow,
  type Engagement,
  engagementLine,
  type EngagementStart,
  longTaskSummary,
  oraclePage,
  type PageFacts,
  readEngagement,
  readGridRows,
  readPageFacts,
  readPageState,
  readPublications,
  reportedError,
  startEngagement,
} from "./bench-page.ts";
import { PsqlSession } from "./bench-postgres.ts";
import { log, writeRecord } from "./bench-record.ts";
import {
  achievedTransactionsPerSecond,
  type Verdict,
} from "./bench-results.ts";
import {
  probeInvalidity,
  reachedMs,
  type Retaken,
  RETAKES,
  type TakenSample,
  takeSamples,
} from "./bench-samples.ts";
import {
  MONITOR_INTERVAL_MS,
  type MonitorSources,
  RealtimeMonitor,
  SLOT_LAG_SQL,
} from "./bench-sampling.ts";
import {
  awaitStablePaint,
  type ClockCheck,
  clockCheck,
  closeSession,
  DriftAtChanges,
  HOLD_MARGIN_MS,
  navigate,
  openSession,
  readWallDriftMs,
  type Session,
  shapeNetwork,
  shapingWitness,
} from "./bench-session.ts";
import { delay } from "./demo-process.ts";

const PROBE_TIMEOUT_MS = 30_000;

const LOAD_SESSIONS = 2;

const LOAD_OUTSTANDING = 40;

/** Quiet time before the final oracle reads a page the load was changing. */
const PUBLICATION_QUIET_MS = 2_000;

/** How long the page may keep publishing after the load stops. */
const DRAIN_TIMEOUT_MS = 60_000;

/** The background writer: `transactionsPerSecond` over two sessions. */
class LoadWriter {
  readonly #sessions: PsqlSession[];
  readonly #transactionsPerSecond: number;
  #issued = 0;
  #completed = 0;
  #startedMs = 0;
  #firstCompletedMs = 0;
  #lastCompletedMs = 0;
  #timer: NodeJS.Timeout | undefined;
  readonly #outstanding = new Set<Promise<void>>();
  readonly failures: string[] = [];

  constructor(environment: NodeJS.ProcessEnv, transactionsPerSecond: number) {
    this.#transactionsPerSecond = transactionsPerSecond;
    this.#sessions = Array.from(
      { length: LOAD_SESSIONS },
      (_, index) => new PsqlSession(environment, `bench-load-${String(index)}`),
    );
  }

  start(): void {
    this.#startedMs = hrtimeMs();
    this.#timer = setInterval(() => this.#tick(), 5);
  }

  get issued(): number {
    return this.#issued;
  }

  get completed(): number {
    return this.#completed;
  }

  #tick(): void {
    const due = Math.floor(
      ((hrtimeMs() - this.#startedMs) * this.#transactionsPerSecond) / 1_000,
    );
    while (
      this.#issued < due &&
      this.#issued - this.#completed < LOAD_OUTSTANDING
    ) {
      const n = this.#issued;
      this.#issued += 1;
      const session = this.#sessions[n % this.#sessions.length];
      if (!session) return;
      const done: Promise<void> = session
        .run(loadTransactionSql(n), `bench-load-${String(n)}`)
        .then(
          () => {
            this.#completed += 1;
            this.#lastCompletedMs = hrtimeMs();
            if (this.#completed === 1)
              this.#firstCompletedMs = this.#lastCompletedMs;
          },
          (error: unknown) => {
            this.failures.push(
              error instanceof Error ? error.message : String(error),
            );
          },
        )
        .finally(() => this.#outstanding.delete(done));
      this.#outstanding.add(done);
    }
  }

  async stop() {
    if (this.#timer) clearInterval(this.#timer);
    await Promise.all(this.#outstanding);
    await Promise.all(this.#sessions.map((session) => session.close()));
    const transactionsPerSecond = achievedTransactionsPerSecond(
      this.#completed,
      this.#firstCompletedMs,
      this.#lastCompletedMs,
    );
    return {
      issued: this.#issued,
      completed: this.#completed,
      // After the writer's start, so either estimator can be recomputed.
      firstCommitMs:
        this.#completed > 0 ? this.#firstCompletedMs - this.#startedMs : null,
      lastCommitMs:
        this.#completed > 0 ? this.#lastCompletedMs - this.#startedMs : null,
      transactionsPerSecond,
      rowChangesPerSecond:
        transactionsPerSecond === null
          ? null
          : transactionsPerSecond * LOAD_ROW_CHANGES_PER_TRANSACTION,
      failures: [...this.failures],
    };
  }
}

interface ProbeResult {
  readonly verdict: Verdict;
  readonly upperMs: number | null;
}

interface RealtimeResult {
  readonly probes: readonly ProbeResult[];
  /** The probes taken again, each under its own number. */
  readonly retaken: readonly Retaken[];
  readonly artifactRetryMarks: readonly string[];
  readonly peakRssBytes: number | null;
  readonly longestTaskMs: number | null;
  readonly rowChangesPerSecond: number | null;
  readonly finalOracle: OracleResult | null;
  readonly finalProblems: readonly string[];
  /** Why the final oracle did not run or did not settle: its `DNC`. */
  readonly finalFailure: string | null;
  /** The shaping witness, null if the session ended before it. */
  readonly shaping: ShapingWitness | KernelShapingWitness | null;
  /**
   * Why the probes may not have run under `--network`: an instrument
   * failure, kept apart so that the final oracle still runs and the probes
   * keep their verdicts, and failing the run all the same.
   */
  readonly shapingFailure: string | null;
  /** The change requests against the publications, null if not read. */
  readonly engagement: Engagement | null;
  /**
   * Why the network memory guard voided the session's probes and stopped
   * the background clients, or null.
   */
  readonly voidReason: string | null;
}

/** The realtime session's half of a run's pass: its final state and its link. */
export function realtimePasses(
  realtime: Pick<RealtimeResult, "finalFailure" | "shapingFailure">,
): boolean {
  return realtime.finalFailure === null && realtime.shapingFailure === null;
}

/** A monitor over the live session: the page, `calque dev` and PostgreSQL. */
function liveMonitorSources(
  page: Page,
  metricsEndpoint: string,
  writer: LoadWriter,
  psql: PsqlSession,
): MonitorSources {
  let queries = 0;
  return {
    load: () => ({ issued: writer.issued, completed: writer.completed }),
    publications: () => readPublications(page),
    metrics: async () => {
      const response = await fetch(`${metricsEndpoint}/metrics`, {
        signal: AbortSignal.timeout(MONITOR_INTERVAL_MS),
      });
      if (!response.ok)
        throw new Error(`/metrics answered ${String(response.status)}`);
      return await response.text();
    },
    slots: () => {
      queries += 1;
      return psql.query(SLOT_LAG_SQL, `bench-monitor-${String(queries)}`);
    },
  };
}

export async function realtimeSession(run: Run): Promise<RealtimeResult> {
  const reference = referenceOf(run);
  const expected = reference.full;
  const server = run.server;
  if (!reference.grid || !run.source || !server)
    throw new Error("the realtime session needs the run's reference");
  const before = readEnvironment(run.postgres);
  showPage(run);
  const session = await openSession(run, false);
  const probes: ProbeResult[] = [];
  const probeRecords: object[] = [];
  const retaken: Retaken[] = [];
  let clock: ClockCheck | null = null;
  let wallDriftAtLoadMs: number | null = null;
  let load: Awaited<ReturnType<LoadWriter["stop"]>> | null = null;
  let loadStoppedMs: number | null = null;
  let drain: PublicationDrain | null = null;
  let monitor: RealtimeMonitor | null = null;
  let monitorPsql: PsqlSession | null = null;
  let probesStoppedAfter: number | null = null;
  let facts: PageFacts | null = null;
  let finalOracle: OracleResult | null = null;
  const finalProblems: string[] = [];
  let finalFailure: string | null = null;
  let sessionFailure: string | null = null;
  let shaping: ShapingWitness | KernelShapingWitness | null = null;
  let shapingFailure: string | null = null;
  let engagement: Engagement | null = null;
  let background: BackgroundClients | null = null;
  let backgroundReport: BackgroundReport | null = null;
  let backgroundFailure: string | null = null;
  const voiding: { reason: string | null } = { reason: null };
  const voided = (reason: string) => {
    if (voiding.reason !== null) return;
    voiding.reason = reason;
    log(`void: ${reason}; the background clients were stopped`);
  };
  let calqueCpu: {
    readonly seconds: number;
    readonly wallMs: number;
  } | null = null;
  try {
    const beforeGotoMs = await navigate(run, session);
    const loadFailure = await awaitStablePaint(session, expected);
    clock = clockCheck(session, beforeGotoMs, session.capture.prefix());
    wallDriftAtLoadMs = await readWallDriftMs(session.page);
    await session.cdp.send("Network.disable");
    await shapeNetwork(run, session);
    if (loadFailure) sessionFailure = `the page did not load: ${loadFailure}`;
    if (sessionFailure === null) {
      const writer = new LoadWriter(
        run.sourceEnvironment,
        loadTransactionsPerSecond(run.options),
      );
      const prober = new PsqlSession(run.sourceEnvironment, "bench-probe");
      const setup = probeSessionSql(run.options.dataset);
      if (setup.length > 0) await prober.run(setup, "bench-probe-setup");
      if (run.options.backgroundClients !== undefined) {
        log(
          `starting ${String(run.options.backgroundClients)} background clients`,
        );
        try {
          background = await BackgroundClients.start({
            clients: run.options.backgroundClients,
            observers: BACKGROUND_OBSERVERS,
            start: backgroundStart(run, server),
            ...(run.options.backgroundQos === undefined
              ? {}
              : { qos: run.options.backgroundQos }),
            onVoid: voided,
          });
          log(backgroundReadyLine(background.ready));
        } catch (error) {
          if (!(error instanceof NetworkMemoryVoid)) throw error;
          voided(error.reason);
        }
      }
      monitorPsql = new PsqlSession(run.adminEnvironment, "bench-monitor");
      monitor = new RealtimeMonitor(
        liveMonitorSources(
          session.page,
          server.metricsEndpoint,
          writer,
          monitorPsql,
        ),
      );
      monitor.start();
      const cpuStart = {
        seconds: await processCpuSeconds(server.pid),
        ms: hrtimeMs(),
      };
      writer.start();
      const clockPassed = clock.passed;
      const driftAtLoad = wallDriftAtLoadMs;
      let engagementStart: EngagementStart | null = null;
      try {
        engagementStart = await startEngagement(session.page);
        const ran = await takeSamples(
          {
            warmups: run.plan.probeWarmups,
            judged: run.plan.probes,
            retakes: RETAKES,
            valueMs: (
              record: TakenSample & { readonly reached_ms: number | null },
            ) => record.reached_ms,
          },
          async (k) => {
            if (voiding.reason !== null)
              throw new NetworkMemoryVoid(voiding.reason);
            const record = await probe(
              run,
              session,
              prober,
              k,
              clockPassed,
              driftAtLoad,
            );
            if (!record.timedOut) await delay(250);
            return record;
          },
          (record, { n, warmup, slot, retake }) => {
            probeRecords.push({
              ...record,
              invalid: record.invalid.map((entry) => entry.reason),
              retaken: retake !== null,
              warmup,
            });
            const upperMs = record.commit_to_stable_paint_ms;
            if (slot !== null && retake)
              retaken.push({
                n,
                slot,
                reasons: retake,
                valueMs: record.reached_ms,
              });
            log(
              `probe ${String(n)}${warmup ? " (warm-up)" : ""}: ${record.verdict}${upperMs === null ? "" : ` ${upperMs.toFixed(1)} ms`}${retake ? `, taken again (${retake.join("; ")})` : ""}`,
            );
          },
        );
        probesStoppedAfter = ran.timedOutAt;
        for (const record of ran.judged)
          probes.push({
            verdict: record.verdict,
            upperMs: record.commit_to_stable_paint_ms,
          });
        if (ran.timedOutAt !== null) {
          const reason = `not run: probe ${String(ran.timedOutAt)} timed out and the load stopped`;
          const warmupsMissing = Math.max(
            0,
            run.plan.probeWarmups - ran.samples.length,
          );
          const missing = warmupsMissing + run.plan.probes - ran.judged.length;
          for (let index = 0; index < missing; index += 1) {
            const warmup = index < warmupsMissing;
            probeRecords.push({
              kind: "probe",
              k: ran.samples.length + index,
              warmup,
              verdict: "DNC",
              failure: reason,
              commit_to_stable_paint_ms: null,
            });
            if (!warmup) probes.push({ verdict: "DNC", upperMs: null });
          }
          log(reason);
        }
      } catch (error) {
        if (!(error instanceof NetworkMemoryVoid))
          sessionFailure = `a probe failed: ${error instanceof Error ? error.message : String(error)}`;
      } finally {
        if (engagementStart !== null)
          engagement = await readEngagement(
            session.page,
            engagementStart,
          ).catch(() => null);
        load = await writer.stop();
        loadStoppedMs = hrtimeMs();
        await prober.close();
      }
      log(engagementLine(engagement));
      drain = await awaitQuietPublications(() =>
        readPublications(session.page),
      );
      await monitor.stop();
      calqueCpu = {
        seconds: (await processCpuSeconds(server.pid)) - cpuStart.seconds,
        wallMs: hrtimeMs() - cpuStart.ms,
      };
      if (background !== null && voiding.reason === null) {
        try {
          backgroundReport = await background.stop();
          backgroundFailure = backgroundProblem(
            run.options.backgroundClients ?? 0,
            backgroundReport,
          );
          log(backgroundReportLine(backgroundReport));
          if (backgroundFailure !== null) log(backgroundFailure);
        } catch (error) {
          if (!(error instanceof NetworkMemoryVoid)) throw error;
          voided(error.reason);
        }
      }
      log(
        `load: ${String(load.completed)} transactions at ${load.transactionsPerSecond?.toFixed(1) ?? "?"} per second; the page ${drain.quiet ? `stopped publishing ${drain.lastChangeMs.toFixed(0)} ms after the load` : `kept publishing for ${String(DRAIN_TIMEOUT_MS)} ms after the load`}`,
      );
      if (!drain.quiet)
        finalFailure = `the page kept publishing for ${String(DRAIN_TIMEOUT_MS)} ms after the load stopped`;
      try {
        shaping =
          shapingFloorMs(run.options.network) === null
            ? judgeShaping(run.options.network, [])
            : await shapingWitness(run, session);
        log(shapingLine(shaping));
        if (shaping.shaped === false) shapingFailure = unshapedFailure(shaping);
      } catch (error) {
        shapingFailure = `the shaping witness failed: ${error instanceof Error ? error.message : String(error)}`;
      }
      if (shapingFailure !== null) log(shapingFailure);
    }
    if (sessionFailure !== null) finalFailure = sessionFailure;
    await session.capture.stop();
    await session.rss.stop();
    facts = await readPageFacts(session.page);
    if (finalFailure === null) {
      const state = readSourceState(run.tables, run.sourceEnvironment);
      finalOracle = await pageOracle(
        oraclePage(session.page),
        run.tables,
        state.reference,
      );
      if (finalOracle.unsettled !== null)
        finalFailure = `the final oracle: ${finalOracle.unsettled}`;
      else {
        finalProblems.push(...finalOracle.problems);
        if (facts.gridRows !== state.events)
          finalProblems.push(
            `the grid shows ${String(facts.gridRows)} events and PostgreSQL holds ${String(state.events)}`,
          );
        if (chartText(facts.chart) !== chartText(state.buckets))
          finalProblems.push(
            "the chart differs from GROUP BY status, priority",
          );
      }
    }
    const rss = session.rss.summary();
    const longTasks = longTaskSummary(facts);
    return {
      probes:
        sessionFailure === null && voiding.reason === null
          ? probes
          : Array.from({ length: run.plan.probes }, () => ({
              verdict: "DNC" as const,
              upperMs: null,
            })),
      retaken,
      artifactRetryMarks: artifactRetryMarks(facts.marks),
      peakRssBytes: rss.browserPeakBytes,
      longestTaskMs: longTasks.longestMs,
      rowChangesPerSecond: load?.rowChangesPerSecond ?? null,
      finalOracle,
      finalProblems,
      finalFailure,
      shaping,
      shapingFailure,
      engagement,
      voidReason: voiding.reason,
    };
  } finally {
    // Written on every path: a failed session's load, probes and series
    // explain why it failed.
    background?.kill();
    await monitor?.stop().catch(() => undefined);
    await monitorPsql?.close();
    const after = readEnvironment(run.postgres);
    run.readings.push(before, after);
    writeRecord(run, "realtime-0", {
      kind: "realtime",
      browserLaunches: session.browserLaunches,
      artifactRetryMarks:
        facts === null ? null : artifactRetryMarks(facts.marks),
      sessionFailure,
      probesStoppedAfter,
      retaken,
      clock,
      wallDriftAtLoadMs,
      shaping,
      shapingFailure,
      engagement,
      load,
      loadStoppedMs,
      loadSchedule: {
        transactionsPerSecond: loadTransactionsPerSecond(run.options),
        rowChangesPerTransaction: LOAD_ROW_CHANGES_PER_TRANSACTION,
        sessions: LOAD_SESSIONS,
        maxOutstanding: LOAD_OUTSTANDING,
      },
      drain,
      ...(run.options.backgroundClients === undefined
        ? {}
        : {
            background: {
              clients: run.options.backgroundClients,
              placement: background?.placement ?? null,
              ready: background?.ready ?? null,
              report: backgroundReport,
              failure: backgroundFailure,
              void: voiding.reason,
            },
          }),
      calqueCpu,
      memory: session.rss.summary(),
      longTasks: facts === null ? null : longTaskSummary(facts),
      finalOracle,
      finalProblems,
      finalFailure,
      pageErrors: session.pageErrors,
      series: monitor?.samples ?? [],
      environment: { before, after },
    });
    for (const [index, record] of probeRecords.entries())
      writeRecord(run, `probe-${String(index)}`, record);
    await closeSession(session);
  }
}

/** Background clients whose transactions the observers decode. */
const BACKGROUND_OBSERVERS = 8;

/**
 * What a background client bootstraps with: the page's own request
 * (`requestPlan`), posted to calque's loopback address.
 */
function backgroundStart(run: Run, server: CalqueServer): BootstrapStart {
  if (!run.connection)
    throw new Error("background clients need the run's connection");
  return {
    ...run.connection,
    ...bootstrapElement(readFileSync(join(PAGE_ROOT, "index.html"), "utf8")),
    endpoint: `${server.endpoint}/`,
  };
}

function backgroundReadyLine(ready: BackgroundClients["ready"]): string {
  const failures = Object.entries(ready.bootstrapFailures)
    .map(([status, count]) => `${status} x${String(count)}`)
    .join(", ");
  const each = ready.bootstrapLatencyMs;
  return `background clients: ${String(ready.bootstrapped)} bootstrapped in ${(ready.bootstrapMs / 1_000).toFixed(1)} s${each === null ? "" : `, each p50 ${each.p50.toFixed(0)} ms, p95 ${each.p95.toFixed(0)} ms, max ${each.max.toFixed(0)} ms`}${failures === "" ? "" : `; refused: ${failures}`}`;
}

function backgroundReportLine(report: BackgroundReport): string {
  const ms = (value: number | null) =>
    value === null ? "-" : `${value.toFixed(0)} ms`;
  const latency = report.observers.commitToArrivalMs;
  return `background clients: ${String(report.requests)} requests, ${String(report.batches)} batches, ${report.settled ? `settled in ${ms(report.settleMs)}` : `${String(report.distinctFinalCursors)} cursors after ${ms(report.settleMs)}`}; observers saw ${String(report.observers.transactions)} transactions, commit to arrival p50 ${ms(latency.p50)}, p95 ${ms(latency.p95)}, max ${ms(latency.max)}; spread p50 ${ms(report.observers.spreadMs.p50)}, p95 ${ms(report.observers.spreadMs.p95)}; the clients' process took ${report.loadClient.cpuSeconds.toFixed(1)} s of CPU in ${report.loadClient.wallSeconds.toFixed(1)} s, event-loop delay p99 ${ms(report.loadClient.eventLoopDelayMs.p99)}`;
}

/** Why the background clients do not count as following the stream, or null. */
export function backgroundProblem(
  clients: number,
  report: BackgroundReport,
): string | null {
  if (report.bootstrapped !== clients)
    return `${String(clients - report.bootstrapped)} of ${String(clients)} background clients got no plan`;
  if (report.allLiveMs === null)
    return "not every background client received a batch";
  if (!report.settled)
    return `the background clients ended on ${String(report.distinctFinalCursors)} cursors`;
  return null;
}

interface PublicationDrain {
  readonly quiet: boolean;
  /** From the load's stop to the last change in the page's count. */
  readonly lastChangeMs: number;
  readonly publicationsAtLoadStop: number | null;
  readonly publicationsAtEnd: number | null;
}

/**
 * Wait up to `timeoutMs` for the page's publication count to hold still for
 * `quietMs`, starting when the load stopped. A page still publishing at the
 * end is reported, not thrown, so the session's record keeps it.
 */
export async function awaitQuietPublications(
  read: () => Promise<number | null>,
  quietMs: number = PUBLICATION_QUIET_MS,
  timeoutMs: number = DRAIN_TIMEOUT_MS,
): Promise<PublicationDrain> {
  const started = hrtimeMs();
  const atLoadStop = await read();
  let last = atLoadStop;
  let changedAt = started;
  while (hrtimeMs() - started < timeoutMs) {
    await delay(Math.min(100, quietMs));
    const count = await read();
    if (count !== last) {
      last = count;
      changedAt = hrtimeMs();
    } else if (hrtimeMs() - changedAt >= quietMs)
      return {
        quiet: true,
        lastChangeMs: changedAt - started,
        publicationsAtLoadStop: atLoadStop,
        publicationsAtEnd: count,
      };
  }
  return {
    quiet: false,
    lastChangeMs: changedAt - started,
    publicationsAtLoadStop: atLoadStop,
    publicationsAtEnd: last,
  };
}

async function probe(
  run: Run,
  session: Session,
  prober: PsqlSession,
  k: number,
  clockPassed: boolean,
  wallDriftAtLoadMs: number,
) {
  const capture = session.capture;
  const drainDeadline = hrtimeMs() + 5_000;
  while (!capture.complete && hrtimeMs() < drainDeadline) await delay(10);
  const frames = capture.prefix();
  const beforeHash = frames.at(-1)?.grid ?? referenceOf(run).grid;
  if (!beforeHash) throw new Error("no grid hash before the probe");
  const signalsBefore = capture.signals.length;
  const errorsBefore = session.pageErrors.length;
  const wallShiftMs = (await readWallDriftMs(session.page)) - wallDriftAtLoadMs;
  session.network.wallShiftMs = wallShiftMs;
  const drift = new DriftAtChanges(() => readWallDriftMs(session.page), frames);
  await prober.run(
    `BEGIN;\n${probeInsertSql(run.options.dataset, k)}`,
    `bench-probe-${String(k)}-begin`,
  );
  const tBefore = hrtimeMs();
  await prober.run("COMMIT;\n", `bench-probe-${String(k)}-commit`);
  const tAfter = hrtimeMs();
  const quietMs = STABLE_PAINT_RULE.holdWindowMilliseconds + HOLD_MARGIN_MS;
  let reference: string | null = null;
  for (;;) {
    await delay(25);
    await drift.observe(capture.prefix());
    const now = hrtimeMs();
    const state = settle(
      capture.prefix(),
      "grid",
      beforeHash,
      tBefore,
      now,
      quietMs,
    );
    if (state.settled && capture.complete) {
      reference = state.reference;
      break;
    }
    if (now - tBefore > PROBE_TIMEOUT_MS) break;
  }
  const window = capture
    .prefix()
    .filter((frame) => frame.timestampMs >= tBefore);
  const signals = capture.signals
    .slice(signalsBefore)
    .map((entry) => entry.signal);
  const judgement =
    reference === null
      ? null
      : judgeStablePaint(window, "grid", reference, {
          fromMs: tBefore,
          signals,
        });
  const match = window.find(
    (frame) => frame.timestampMs === judgement?.matchMs,
  );
  const wallDriftMs =
    ((match && drift.after(match.receivedMs)) ??
      (await readWallDriftMs(session.page))) -
    wallDriftAtLoadMs -
    wallShiftMs;
  const gridRows = await readGridRows(session.page);
  const error = reportedError(
    await readPageState(session.page),
    session.pageErrors.slice(errorsBefore),
  );
  const publications = await publicationsBetween(
    session,
    tBefore,
    judgement?.matchMs ?? hrtimeMs(),
  );
  const expectedRows = BENCH_ROW_COUNTS.events + 1 + k + 1;
  const frameClock = frameClockCheck(window);
  const hostSleptMs = rereadHostSleep();
  const invalid = probeInvalidity(judgement, {
    sessionClockPassed: clockPassed,
    frameClockPassed: frameClock.passed,
    wallDriftMs,
    hostSleptMs,
  });
  const verdict: Verdict =
    reference === null || error !== null
      ? "DNC"
      : gridRows !== expectedRows
        ? "INCORRECT"
        : invalid.length > 0
          ? "INVALID_RUN"
          : judgement?.matchMs == null
            ? "DNC"
            : "PASS";
  const upperMs =
    judgement?.matchMs == null ? null : judgement.matchMs - tBefore;
  return {
    kind: "probe",
    k,
    verdict,
    timedOut: reference === null,
    failure:
      error ??
      (reference === null
        ? `the grid did not settle within ${String(PROBE_TIMEOUT_MS)} ms of t_before`
        : null),
    invalid,
    commit_to_stable_paint_ms: upperMs,
    // What the list of probes taken again reports; see `reachedMs`.
    reached_ms: reachedMs(judgement, window, "grid", reference, tBefore),
    lower_bound_ms:
      judgement?.matchMs == null ? null : judgement.matchMs - tAfter,
    tBeforeMs: tBefore,
    tAfterMs: tAfter,
    wallShiftMs,
    wallDriftMs,
    hostSleptMs,
    frameClock,
    beforeHash,
    reference,
    gridRows,
    expectedGridRows: expectedRows,
    judgement: judgementRecord(judgement),
    publications,
    frames: framesRecord(window),
  };
}

/**
 * The publications the page's main thread was told of from `fromMs` to `toMs`
 * on the frame clock, each with the newest source commit it carried, or null
 * when the page keeps no log. A publication splits a probe's time at the page:
 * calque, the transport and the worker before it, the page's paint and the
 * screencast after.
 */
export async function publicationsBetween(
  session: Session,
  fromMs: number,
  toMs: number,
): Promise<
  | {
      readonly atMs: number;
      readonly commitTimeUnixMicros: string | null;
      readonly changedCollections: readonly string[];
    }[]
  | null
> {
  const offset = session.network.monotonicMinusEpochSeconds;
  const read = await session.page.evaluate(() => ({
    timeOrigin: performance.timeOrigin,
    log:
      (globalThis as unknown as BenchWindow).__incidents?.publicationLog ??
      null,
  }));
  if (offset === null || read.log === null) return null;
  return read.log.flatMap((publication) => {
    const atMs = mapEpochSecondsToMonotonicMilliseconds(
      (read.timeOrigin + publication.at) / 1_000,
      offset,
    );
    return atMs >= fromMs && atMs <= toMs
      ? [
          {
            atMs,
            commitTimeUnixMicros: publication.commitTimeUnixMicros,
            changedCollections: publication.changedCollections,
          },
        ]
      : [];
  });
}
