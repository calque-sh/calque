/** Samplers that run beside a page load or the realtime session: memory, the runner's own stalls, and the realtime series. */
import { monitorEventLoopDelay, PerformanceObserver } from "node:perf_hooks";
import { hrtimeMs } from "./bench-host.ts";
import { readProcessTable, selectProcessTree } from "./bench-processes.ts";
import {
  parseMetric,
  parseSlotRows,
  type ReplicationSlotReading,
} from "./bench-results.ts";
import { delay } from "./demo-process.ts";

const RSS_INTERVAL_MS = 250;

interface RssSample {
  readonly atMs: number;
  readonly browserBytes: number;
  readonly browserProcesses: number;
  readonly calqueBytes: number | null;
}

/** The browser's process-tree RSS and calque's, from one `ps` every 250 ms. */
export class RssSampler {
  readonly samples: RssSample[] = [];
  readonly errors: string[] = [];
  #timer: NodeJS.Timeout | undefined;
  #inFlight: Promise<void> | null = null;

  constructor(
    private readonly rootPid: number,
    private readonly calquePid: number,
  ) {}

  start(): void {
    this.#tick();
    this.#timer = setInterval(() => this.#tick(), RSS_INTERVAL_MS);
  }

  #tick(): void {
    if (this.#inFlight) return;
    this.#inFlight = readProcessTable({ timeoutMilliseconds: 2_000 })
      .then((rows) => {
        const tree = selectProcessTree(rows, this.rootPid);
        if (tree.length === 0) return;
        this.samples.push({
          atMs: hrtimeMs(),
          browserBytes: tree.reduce((sum, row) => sum + row.rssBytes, 0),
          browserProcesses: tree.length,
          calqueBytes:
            rows.find((row) => row.pid === this.calquePid)?.rssBytes ?? null,
        });
      })
      .catch((error: unknown) => {
        this.errors.push(
          error instanceof Error ? error.message : String(error),
        );
      })
      .finally(() => {
        this.#inFlight = null;
      });
  }

  async stop(): Promise<void> {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    await this.#inFlight;
  }

  summary() {
    const browser = this.samples.map((sample) => sample.browserBytes);
    const calque = this.samples
      .map((sample) => sample.calqueBytes)
      .filter((value): value is number => value !== null);
    return {
      samples: this.samples.length,
      browserPeakBytes: browser.length === 0 ? null : Math.max(...browser),
      calquePeakBytes: calque.length === 0 ? null : Math.max(...calque),
      errors: [...this.errors],
    };
  }
}

/** The resolution of the runner's event-loop delay histogram. */
const EVENT_LOOP_TICK_MS = 10;

/** A tick interval at least this long is listed with its time. */
const STALL_LISTED_MS = 50;

/** A collection at least this long is listed with its time. */
const GC_LISTED_MS = 10;

/** The most stalls or collections one sample lists. */
const STALLS_LISTED = 200;

interface RunnerStallSummary {
  /**
   * The interval between consecutive 10 ms timer ticks on the runner's main
   * thread, so the loop was blocked for the interval less 10 ms. `stalls` is
   * stamped on the host clock that stamps a frame's arrival.
   */
  readonly eventLoopDelay: {
    readonly resolutionMs: number;
    readonly ticks: number;
    readonly maxMs: number | null;
    readonly p99Ms: number | null;
    readonly meanMs: number | null;
    /** Every interval of at least 50 ms; `stalls` lists the first 200. */
    readonly stallCount: number;
    readonly stalls: readonly {
      readonly endedMs: number;
      readonly intervalMs: number;
    }[];
  };
  /**
   * The runner's own garbage collections, from its `gc` performance entries;
   * `long` lists those of at least 10 ms by their start on the host clock.
   */
  readonly gc: {
    readonly count: number;
    readonly totalMs: number;
    readonly longestMs: number | null;
    /** Every collection of at least 10 ms; `long` lists the first 200. */
    readonly longCount: number;
    readonly long: readonly {
      readonly startMs: number;
      readonly durationMs: number;
    }[];
  };
}

/**
 * How long the runner's own main thread stalled while Chrome sent frames. It
 * acknowledges every screencast frame, and Chrome skips capture while more
 * than two frames are unacknowledged, so a stall there can lose a frame the
 * page presented.
 */
export class RunnerStalls {
  readonly #histogram = monitorEventLoopDelay({
    resolution: EVENT_LOOP_TICK_MS,
  });
  readonly #gc: { readonly startMs: number; readonly durationMs: number }[] =
    [];
  readonly #observer = new PerformanceObserver((list) => {
    this.#record(list.getEntries());
  });
  readonly #stalls: {
    readonly endedMs: number;
    readonly intervalMs: number;
  }[] = [];
  #stallCount = 0;
  #timer: NodeJS.Timeout | undefined;
  #summary: RunnerStallSummary | null = null;

  start(): void {
    this.#histogram.enable();
    this.#observer.observe({ entryTypes: ["gc"] });
    let lastTickMs = hrtimeMs();
    this.#timer = setInterval(() => {
      const now = hrtimeMs();
      const intervalMs = now - lastTickMs;
      lastTickMs = now;
      if (intervalMs < STALL_LISTED_MS) return;
      this.#stallCount += 1;
      if (this.#stalls.length < STALLS_LISTED)
        this.#stalls.push({ endedMs: now, intervalMs });
    }, EVENT_LOOP_TICK_MS);
  }

  #record(
    entries: readonly {
      readonly startTime: number;
      readonly duration: number;
    }[],
  ): void {
    // `performance.now()` reads Node's hrtime, which `hrtimeMs()` corrects by
    // the sleep time as last read, so the mapping is taken when recorded.
    const performanceToHostMs = hrtimeMs() - performance.now();
    for (const entry of entries)
      this.#gc.push({
        startMs: entry.startTime + performanceToHostMs,
        durationMs: entry.duration,
      });
  }

  /** Stop observing; a second call returns the first call's summary. */
  stop(): RunnerStallSummary {
    if (this.#summary) return this.#summary;
    clearInterval(this.#timer);
    this.#histogram.disable();
    this.#record(this.#observer.takeRecords());
    this.#observer.disconnect();
    const ticks = this.#histogram.count;
    const ms = (nanoseconds: number) =>
      ticks === 0 ? null : nanoseconds / 1e6;
    const durations = this.#gc.map((entry) => entry.durationMs);
    const long = this.#gc.filter((entry) => entry.durationMs >= GC_LISTED_MS);
    this.#summary = {
      eventLoopDelay: {
        resolutionMs: EVENT_LOOP_TICK_MS,
        ticks,
        maxMs: ms(this.#histogram.max),
        p99Ms: ms(this.#histogram.percentile(99)),
        meanMs: ms(this.#histogram.mean),
        stallCount: this.#stallCount,
        stalls: [...this.#stalls],
      },
      gc: {
        count: durations.length,
        totalMs: durations.reduce((sum, duration) => sum + duration, 0),
        longestMs: durations.length === 0 ? null : Math.max(...durations),
        longCount: long.length,
        long: long.slice(0, STALLS_LISTED),
      },
    };
    return this.#summary;
  }
}

export const MONITOR_INTERVAL_MS = 1_000;

interface RealtimeSample {
  readonly atMs: number;
  readonly loadIssued: number;
  readonly loadCompleted: number;
  readonly publications: number | null;
  readonly cdcLagBytes: number | null;
  /** Canonical-log entries appended, one per source transaction. */
  readonly canonicalEntries: number | null;
  /** Fenced append commits; one carries a group of entries. */
  readonly canonicalAppendCommits: number | null;
  readonly cdcReconnects: number | null;
  /** Artifacts calque cut again, and whether one is being cut or encoded. */
  readonly artifactRecuts: number | null;
  readonly recutInProgress: number | null;
  /** The log's entries and bytes after the newest cut: what a cold load replays. */
  readonly tailEntries: number | null;
  readonly tailBytes: number | null;
  readonly slots: readonly ReplicationSlotReading[] | null;
  readonly failures: readonly string[];
}

export const SLOT_LAG_SQL = `SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_type = 'logical' ORDER BY slot_name;
`;

/** Where a realtime monitor reads each value it samples. */
export interface MonitorSources {
  readonly load: () => { readonly issued: number; readonly completed: number };
  readonly publications: () => Promise<number | null>;
  /** `calque`'s OpenMetrics text. */
  readonly metrics: () => Promise<string>;
  /** `SLOT_LAG_SQL`'s rows. */
  readonly slots: () => Promise<readonly string[]>;
}

/**
 * Once an interval through the realtime session and its drain: the writer's
 * progress, the page's publications, `calque`'s CDC gauges and PostgreSQL's
 * view of the slot. A page that keeps publishing after the load stops is
 * then behind WAL the slot still holds, or behind `calque` with the slot
 * confirmed, and the series says which. A source that fails is recorded in
 * its sample and read again at the next one.
 */
export class RealtimeMonitor {
  readonly samples: RealtimeSample[] = [];
  #stopped = false;
  #loop: Promise<void> | null = null;

  constructor(
    private readonly sources: MonitorSources,
    private readonly intervalMs: number = MONITOR_INTERVAL_MS,
  ) {}

  start(): void {
    const started = hrtimeMs();
    this.#loop = (async () => {
      while (!this.#stopped) {
        this.samples.push(await this.#sample());
        const due = started + this.samples.length * this.intervalMs;
        await delay(Math.max(0, due - hrtimeMs()));
      }
    })();
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    await this.#loop;
  }

  async #sample(): Promise<RealtimeSample> {
    const atMs = hrtimeMs();
    const failures: string[] = [];
    const failed = (what: string) => (error: unknown) => {
      failures.push(
        `${what}: ${error instanceof Error ? error.message : String(error)}`,
      );
      return null;
    };
    const attempt = <T>(read: () => Promise<T>) => {
      try {
        return read();
      } catch (error) {
        return Promise.reject(error);
      }
    };
    const [publications, metrics, slots] = await Promise.all([
      attempt(this.sources.publications).catch(failed("publications")),
      attempt(this.sources.metrics).catch(failed("metrics")),
      attempt(this.sources.slots).then(parseSlotRows).catch(failed("slots")),
    ]);
    const load = this.sources.load();
    return {
      atMs,
      loadIssued: load.issued,
      loadCompleted: load.completed,
      publications,
      cdcLagBytes:
        metrics === null ? null : parseMetric(metrics, "calque_cdc_lag_bytes"),
      canonicalEntries:
        metrics === null
          ? null
          : parseMetric(metrics, "calque_canonical_entries_appended_total"),
      canonicalAppendCommits:
        metrics === null
          ? null
          : parseMetric(
              metrics,
              "calque_canonical_append_duration_seconds_count",
            ),
      cdcReconnects:
        metrics === null
          ? null
          : parseMetric(metrics, "calque_cdc_reconnects_total"),
      artifactRecuts:
        metrics === null
          ? null
          : parseMetric(metrics, "calque_artifact_recuts_total"),
      recutInProgress:
        metrics === null
          ? null
          : parseMetric(metrics, "calque_artifact_recut_in_progress"),
      tailEntries:
        metrics === null
          ? null
          : parseMetric(metrics, "calque_canonical_log_tail_entries"),
      tailBytes:
        metrics === null
          ? null
          : parseMetric(metrics, "calque_canonical_log_tail_bytes"),
      slots,
      failures,
    };
  }
}
