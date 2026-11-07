/**
 * The benchmark's read-out: sample verdicts, nearest-rank quantiles in
 * which a failed sample ranks as +∞, the threshold table, the directional
 * label, and what a CDP trace says about GC and long tasks.
 *
 * Everything here but `DescendantCpu` is a pure function, so a summary can
 * be recomputed from the JSON a run leaves behind.
 */

export type Verdict = "PASS" | "INCORRECT" | "INVALID_RUN" | "DNC";

/** Nearest-rank quantile; leaves `values` in its order. */
export function nearestRank(
  values: readonly number[],
  quantile: number,
): number {
  if (
    values.length === 0 ||
    !values.every(Number.isFinite) ||
    !Number.isFinite(quantile) ||
    quantile <= 0 ||
    quantile > 1
  ) {
    throw new Error("nearest-rank inputs are invalid");
  }
  const ordered = [...values].sort((left, right) => left - right);
  const rank = Math.ceil(quantile * ordered.length);
  const result = ordered[rank - 1];
  if (result === undefined) throw new Error("nearest-rank result is missing");
  return result;
}

/**
 * Nearest-rank quantile over samples whose failures (`null`) rank above every
 * number. Returns `null` when the rank lands on a failure, so one failure in
 * 30 leaves p95 finite and two do not.
 */
export function nearestRankWithFailures(
  values: readonly (number | null)[],
  quantile: number,
): number | null {
  if (
    values.length === 0 ||
    !Number.isFinite(quantile) ||
    quantile <= 0 ||
    quantile > 1
  )
    throw new Error("nearest-rank inputs are invalid");
  const finite = values.filter((value): value is number => value !== null);
  if (!finite.every(Number.isFinite))
    throw new Error("a measured value must be finite");
  const ordered = [...finite].sort((left, right) => left - right);
  const rank = Math.ceil(quantile * values.length);
  return ordered[rank - 1] ?? null;
}

interface Distribution {
  readonly samples: number;
  readonly failures: number;
  readonly min: number | null;
  readonly p50: number | null;
  readonly p95: number | null;
  readonly p99: number | null;
  readonly max: number | null;
}

/** min, p50, p95, p99 and max, with failures ranked as +∞. */
export function distribution(values: readonly (number | null)[]): Distribution {
  const failures = values.filter((value) => value === null).length;
  if (values.length === 0)
    return {
      samples: 0,
      failures: 0,
      min: null,
      p50: null,
      p95: null,
      p99: null,
      max: null,
    };
  const finite = values.filter((value): value is number => value !== null);
  return {
    samples: values.length,
    failures,
    min: finite.length === 0 ? null : Math.min(...finite),
    p50: nearestRankWithFailures(values, 0.5),
    p95: nearestRankWithFailures(values, 0.95),
    p99: nearestRankWithFailures(values, 0.99),
    max: failures > 0 ? null : Math.max(...finite),
  };
}

/** A timed sample's value for the quantiles: its time when it passed. */
export function rankedValue(sample: {
  readonly verdict: Verdict;
  readonly valueMs: number | null;
}): number | null {
  return sample.verdict === "PASS" ? sample.valueMs : null;
}

/**
 * The write load's achieved rate: N − 1 intervals between the first and last
 * commit. Dividing N by the time from the writer's start to the last commit
 * would also count the first pacing tick and the last commit's latency.
 */
export function achievedTransactionsPerSecond(
  completed: number,
  firstCommitMs: number,
  lastCommitMs: number,
): number | null {
  const spanMs = lastCommitMs - firstCommitMs;
  return completed >= 2 && spanMs > 0
    ? ((completed - 1) * 1_000) / spanMs
    : null;
}

type ThresholdVerdict = "pass" | "fail" | "void" | "unmeasured";

interface ThresholdRow {
  readonly metric: string;
  readonly statistic: string;
  readonly threshold: string;
  /** The judged reading. */
  readonly value: number | null;
  /** Both readings' verdict: a row passes only when each of them passes. */
  readonly verdict: ThresholdVerdict;
  /**
   * The second reading of a quantile row over which a sample was taken
   * again, or absent when none was.
   */
  readonly secondReading?: {
    readonly value: number | null;
    readonly verdict: ThresholdVerdict;
  };
  readonly note?: string;
}

export const THRESHOLDS = Object.freeze({
  preparationP95Ms: 10_000,
  coldP95Ms: 2_000,
  commitP95Ms: 250,
  commitP99Ms: 1_000,
  peakRssBytes: 8_589_934_592,
  longTaskMs: 50,
  sourceRowChangesPerSecond: 10_000,
});

/** A sample taken again: the judged slot it left, and what it reached. */
interface RetakenValue {
  readonly slot: number;
  /** Its `valueMs`, null when it reached nothing. */
  readonly valueMs: number | null;
}

export interface Measurements {
  /** Preparation times, `null` for a preparation that did not complete. */
  readonly preparationsMs: readonly (number | null)[];
  /** Measured cold samples' `complete_stable_paint_ms`, `null` for a failure. */
  readonly coldMs: readonly (number | null)[];
  /** The cold samples taken again, in the order they were taken. */
  readonly coldRetaken: readonly RetakenValue[];
  /** Measured probes' upper bounds, `null` for a failure. */
  readonly commitMs: readonly (number | null)[];
  /** The probes taken again, in the order they were taken. */
  readonly commitRetaken: readonly RetakenValue[];
  /** Peak browser process-tree RSS per measured session, `null` if unsampled. */
  readonly peakRssBytes: readonly (number | null)[];
  /** Longest main-thread long task per measured session, 0 when none. */
  readonly longestTaskMs: readonly (number | null)[];
  readonly achievedRowChangesPerSecond: number | null;
  /** Any INCORRECT sample, or a PostgreSQL state that moved under the run. */
  readonly correctnessFailed: boolean;
  /** The network memory guard voided the realtime session's probes. */
  readonly realtimeVoided: boolean;
}

function below(value: number | null, limit: number): ThresholdVerdict {
  return value === null ? "fail" : value < limit ? "pass" : "fail";
}

function atMost(values: readonly (number | null)[], limit: number) {
  if (values.length === 0 || values.some((value) => value === null))
    return { value: null, verdict: "unmeasured" as const };
  const value = Math.max(...(values as number[]));
  return {
    value,
    verdict: value <= limit ? ("pass" as const) : ("fail" as const),
  };
}

/**
 * The second reading of judged values: the slot each sample taken again left
 * counts that sample at its `valueMs`, +∞ (null) when it reached nothing, in
 * place of the sample that took the slot. Of several taken for one slot, the
 * first counts. Null when none was taken again.
 */
export function secondReading(
  judged: readonly (number | null)[],
  retaken: readonly RetakenValue[],
): (number | null)[] | null {
  if (retaken.length === 0) return null;
  const values = [...judged];
  const counted = new Set<number>();
  for (const { slot, valueMs } of retaken) {
    if (!Number.isInteger(slot) || slot < 0 || slot >= values.length)
      throw new Error(
        `a sample taken again left slot ${String(slot)}, outside the ${String(values.length)} judged`,
      );
    if (counted.has(slot)) continue;
    counted.add(slot);
    values[slot] = valueMs;
  }
  return values;
}

/**
 * The benchmark's threshold table, each row judged on its own. A run that
 * failed correctness reports its timings as void, and a realtime session the
 * network memory guard voided its probes' timings. A quantile row over which
 * a sample was taken again is read twice, as judged and by `secondReading`,
 * and passes only when both readings pass.
 */
export function thresholdRows(measured: Measurements): ThresholdRow[] {
  const timing = (
    metric: string,
    statistic: string,
    threshold: string,
    values: readonly (number | null)[],
    retaken: readonly RetakenValue[],
    quantile: number,
    limit: number,
    voided = false,
  ): ThresholdRow => {
    if (values.length === 0)
      return {
        metric,
        statistic,
        threshold,
        value: null,
        verdict: "unmeasured",
      };
    const judge = (value: number | null): ThresholdVerdict =>
      measured.correctnessFailed || voided ? "void" : below(value, limit);
    const value = nearestRankWithFailures(values, quantile);
    const second = secondReading(values, retaken);
    if (second === null)
      return { metric, statistic, threshold, value, verdict: judge(value) };
    const secondValue = nearestRankWithFailures(second, quantile);
    const verdicts = [judge(value), judge(secondValue)];
    return {
      metric,
      statistic,
      threshold,
      value,
      verdict: verdicts.includes("void")
        ? "void"
        : verdicts.every((verdict) => verdict === "pass")
          ? "pass"
          : "fail",
      secondReading: { value: secondValue, verdict: judge(secondValue) },
    };
  };
  const rss = atMost(measured.peakRssBytes, THRESHOLDS.peakRssBytes);
  const longTask = atMost(measured.longestTaskMs, THRESHOLDS.longTaskMs);
  const rate = measured.achievedRowChangesPerSecond;
  return [
    timing(
      "server_preparation_ms",
      `p95 of ${String(measured.preparationsMs.length)}`,
      "< 10,000 ms",
      measured.preparationsMs,
      [],
      0.95,
      THRESHOLDS.preparationP95Ms,
    ),
    timing(
      "complete_stable_paint_ms",
      `p95 of ${String(measured.coldMs.length)}`,
      "< 2,000 ms",
      measured.coldMs,
      measured.coldRetaken,
      0.95,
      THRESHOLDS.coldP95Ms,
    ),
    timing(
      "commit_to_stable_paint_ms",
      `p95 of ${String(measured.commitMs.length)}`,
      "< 250 ms",
      measured.commitMs,
      measured.commitRetaken,
      0.95,
      THRESHOLDS.commitP95Ms,
      measured.realtimeVoided,
    ),
    timing(
      "commit_to_stable_paint_ms",
      `p99 of ${String(measured.commitMs.length)}`,
      "< 1,000 ms",
      measured.commitMs,
      measured.commitRetaken,
      0.99,
      THRESHOLDS.commitP99Ms,
      measured.realtimeVoided,
    ),
    {
      metric: "browser_process_tree_peak_rss_bytes",
      statistic: "max",
      threshold: "≤ 8,589,934,592",
      ...rss,
    },
    {
      metric: "main_thread_long_task_ms",
      statistic: "max",
      threshold: "≤ 50 ms",
      ...longTask,
    },
    {
      metric: "achieved source row changes/s",
      statistic: "first to last commit",
      threshold: "≥ 10,000",
      value: rate,
      verdict:
        rate === null
          ? "unmeasured"
          : rate >= THRESHOLDS.sourceRowChangesPerSecond
            ? "pass"
            : "fail",
      ...(rate !== null && rate < THRESHOLDS.sourceRowChangesPerSecond
        ? {
            note: "the realtime numbers were measured under a lighter load than the one stated",
          }
        : {}),
    },
  ];
}

/**
 * A run passes when every row passes and correctness holds. An unjudged run
 * (`unjudgedReasons`) never passes.
 */
export function runPasses(
  rows: readonly ThresholdRow[],
  correctnessFailed: boolean,
  unjudged: boolean,
): boolean {
  return (
    !unjudged &&
    !correctnessFailed &&
    rows.every((row) => row.verdict === "pass")
  );
}

// The environment ---------------------------------------------------------------

export interface EnvironmentReading {
  readonly loadAverage: readonly [number, number, number];
  readonly freeMemoryBytes: number;
  /** `memory_pressure`'s "System-wide memory free percentage", or null. */
  readonly memoryFreePercent: number | null;
  /** `pmset -g batt`'s power source, or null. */
  readonly powerSource: string | null;
  /**
   * Whether a container other than the benchmark's own was running, or null
   * when Docker was not read: it did not answer, or the run's PostgreSQL is
   * native. Their names and their number say what else the host runs, so
   * they stay out of the record.
   */
  readonly otherContainersRunning: boolean | null;
  /** The benchmark's own CPU time; it decides nothing. */
  readonly benchCpu: BenchCpuReading;
}

/**
 * The benchmark's own CPU time at an environment reading, in seconds, so that
 * the difference between two readings says how much of the host's load in
 * between was the benchmark's.
 */
export interface BenchCpuReading {
  /** The host's monotonic clock in milliseconds when it was read. */
  readonly atMs: number;
  /** The runner, every thread included, from `process.cpuUsage()`. */
  readonly runnerSeconds: number;
  /**
   * The runner's descendants (Chrome, `calque dev`, the page server, `psql`),
   * each at the last reading that saw it: one that has exited keeps what it
   * had used by then, and what it used after that reading is not counted.
   * Null when `ps` could not be read.
   */
  readonly descendantsSeconds: number | null;
  readonly descendantsAlive: number | null;
  /**
   * The PostgreSQL container's cgroup `usage_usec`, or a native server's
   * process tree as `NativePostgres.cpuSeconds` reads it; null when unread.
   */
  readonly postgresSeconds: number | null;
}

/** `ps`'s `time` column: `[[dd-]hh:]mm:ss.ss`, minutes past 59 on macOS. */
export function parseCpuTime(text: string): number | null {
  const match =
    /^(?:([0-9]+)-)?(?:([0-9]+):)?([0-9]+):([0-9]+(?:\.[0-9]+)?)$/u.exec(
      text.trim(),
    );
  if (!match) return null;
  const [, days = "0", hours = "0", minutes = "0", seconds = "0"] = match;
  return (
    ((Number(days) * 24 + Number(hours)) * 60 + Number(minutes)) * 60 +
    Number(seconds)
  );
}

export interface CpuRow {
  readonly pid: number;
  readonly parentPid: number;
  readonly cpuSeconds: number;
  readonly command: string;
}

/** `ps -axo pid=,ppid=,time=,comm=` output; an unreadable line is skipped. */
export function parseCpuTable(stdout: string): CpuRow[] {
  const rows: CpuRow[] = [];
  for (const line of stdout.split("\n")) {
    const match = /^\s*([0-9]+)\s+([0-9]+)\s+(\S+)\s+(.*)$/u.exec(line);
    if (!match) continue;
    const [, pid = "", parentPid = "", time = "", command = ""] = match;
    const cpuSeconds = parseCpuTime(time);
    if (cpuSeconds === null) continue;
    rows.push({
      pid: Number(pid),
      parentPid: Number(parentPid),
      cpuSeconds,
      command,
    });
  }
  return rows;
}

/** A cgroup v2 `cpu.stat`'s `usage_usec`, in seconds. */
export function parseCgroupCpuSeconds(text: string): number | null {
  const match = /^usage_usec ([0-9]+)$/mu.exec(text);
  return match?.[1] === undefined ? null : Number(match[1]) / 1_000_000;
}

/**
 * The CPU time of every process that has been the runner's descendant at a
 * reading. A process is known by its pid and command; one whose time goes
 * down is a new process on a reused pid, and the old one's time is kept.
 */
export class DescendantCpu {
  readonly #last = new Map<string, number>();
  #retired = 0;

  observe(descendants: readonly CpuRow[]): {
    readonly seconds: number;
    readonly alive: number;
  } {
    for (const row of descendants) {
      const key = `${String(row.pid)} ${row.command}`;
      const previous = this.#last.get(key);
      if (previous !== undefined && row.cpuSeconds < previous)
        this.#retired += previous;
      this.#last.set(key, row.cpuSeconds);
    }
    let seconds = this.#retired;
    for (const value of this.#last.values()) seconds += value;
    return { seconds, alive: descendants.length };
  }
}

const DIRECTIONAL_LOAD_AVERAGE = 4;

/**
 * Why a run's numbers are directional: another container ran or Docker was
 * not read, or the one-minute load average was above 4, before or after any
 * of its samples. An empty list means the environment gave no reason.
 */
export function directionalReasons(
  readings: readonly EnvironmentReading[],
): string[] {
  const reasons = new Set<string>();
  for (const reading of readings) {
    if (reading.loadAverage[0] > DIRECTIONAL_LOAD_AVERAGE)
      reasons.add(
        `one-minute load average above ${String(DIRECTIONAL_LOAD_AVERAGE)}`,
      );
    if (reading.otherContainersRunning === true)
      reasons.add("another container was running");
    if (reading.otherContainersRunning === null)
      reasons.add("the running containers were not read");
  }
  return [...reasons].sort();
}

export function parseMemoryFreePercent(output: string): number | null {
  const match = /free percentage:\s*(\d+)%/u.exec(output);
  return match?.[1] === undefined ? null : Number(match[1]);
}

export function parsePowerSource(output: string): string | null {
  const match = /drawing from '([^']+)'/u.exec(output);
  return match?.[1] ?? null;
}

/**
 * `docker ps --format '{{.Names}}\t{{.Label "calque.bench"}}'` without the
 * benchmark's own container.
 */
export function parseOtherContainers(output: string): string[] {
  return output
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => line.split("\t"))
    .filter(([, label]) => label !== "true")
    .map(([name]) => name ?? "")
    .sort();
}

/** One request in a cold sample's `network.byRequest`, as far as it is read here. */
interface CodedRequest {
  readonly url: string;
  readonly status: number | null;
  readonly contentEncoding: string | null;
}

/**
 * Whether every page response in every judged cold sample arrived in brotli,
 * as the page server sends the page to Chrome. A page response is a request
 * to the page's origin that Chrome got a 200 for; a worker's script, which
 * loads on the worker's own target, shows no status in the page's ledger and
 * is not one. `brotli` is null, with the reason, when a judged cold sample's
 * ledger shows no page response.
 */
interface PageCodingWitness {
  readonly brotli: boolean | null;
  readonly samples: readonly {
    readonly name: string;
    readonly responses: number;
    /** Each page response not in brotli: its path and its coding. */
    readonly notBrotli: readonly string[];
  }[];
  readonly reason: string | null;
}

function originOf(url: string): string | null {
  try {
    return new URL(url).origin;
  } catch {
    return null;
  }
}

export function judgePageCoding(
  pageOrigin: string,
  samples: readonly {
    readonly name: string;
    readonly requests: readonly CodedRequest[];
  }[],
): PageCodingWitness {
  const read = samples.map((sample) => {
    const responses = sample.requests.filter(
      (request) =>
        request.status === 200 && originOf(request.url) === pageOrigin,
    );
    return {
      name: sample.name,
      responses: responses.length,
      notBrotli: responses
        .filter(
          (request) => request.contentEncoding?.trim().toLowerCase() !== "br",
        )
        .map(
          (request) =>
            `${new URL(request.url).pathname} (${request.contentEncoding ?? "none"})`,
        ),
    };
  });
  const empty = read.filter((sample) => sample.responses === 0).length;
  if (read.length === 0 || empty > 0)
    return {
      brotli: null,
      samples: read,
      reason:
        read.length === 0
          ? "no judged cold sample"
          : `${String(empty)} of ${String(read.length)} judged cold samples' ledgers show no page response`,
    };
  return {
    brotli: read.every((sample) => sample.notBrotli.length === 0),
    samples: read,
    reason: null,
  };
}

/** The read-out's line for the page coding witness. */
export function pageCodingLine(witness: PageCodingWitness): string {
  if (witness.brotli === null)
    return `page coding: not shown: ${witness.reason ?? "?"}`;
  const responses = witness.samples.reduce(
    (total, sample) => total + sample.responses,
    0,
  );
  const of = `${String(responses)} page responses in ${String(witness.samples.length)} judged cold samples`;
  if (witness.brotli) return `page coding: all ${of} arrived in brotli`;
  const notBrotli = witness.samples.flatMap((sample) =>
    sample.notBrotli.map((path) => `${sample.name} ${path}`),
  );
  return `page coding: ${String(notBrotli.length)} of ${of} did not arrive in brotli, so this run's page bytes are not the brotli-compressed ones the benchmark measures: ${notBrotli.join(", ")}`;
}

/** One response in a cold sample's ledger, as far as the edge witness reads it. */
interface ProtocolRequest {
  readonly url: string;
  readonly status: number | null;
  readonly protocol: string | null;
}

/**
 * Whether every response in every judged cold sample arrived over HTTP/2, as
 * the edge serves the page and calque to Chrome. A response is a request the
 * page's ledger has a status for. `h2` is null, with the reason, for a run
 * without the edge, or when a judged cold sample's ledger shows no response.
 */
interface EdgeProtocolWitness {
  readonly h2: boolean | null;
  readonly samples: readonly {
    readonly name: string;
    readonly responses: number;
    /** Each response not over HTTP/2: its path and its protocol. */
    readonly notH2: readonly string[];
  }[];
  readonly reason: string | null;
}

export function judgeEdgeProtocol(
  edge: boolean,
  samples: readonly {
    readonly name: string;
    readonly requests: readonly ProtocolRequest[];
  }[],
): EdgeProtocolWitness {
  if (!edge) return { h2: null, samples: [], reason: "the run has no edge" };
  const read = samples.map((sample) => {
    const responses = sample.requests.filter(
      (request) => request.status !== null,
    );
    return {
      name: sample.name,
      responses: responses.length,
      notH2: responses
        .filter((request) => request.protocol !== "h2")
        .map(
          (request) =>
            `${new URL(request.url).pathname} (${request.protocol ?? "none"})`,
        ),
    };
  });
  const empty = read.filter((sample) => sample.responses === 0).length;
  if (read.length === 0 || empty > 0)
    return {
      h2: null,
      samples: read,
      reason:
        read.length === 0
          ? "no judged cold sample"
          : `${String(empty)} of ${String(read.length)} judged cold samples' ledgers show no response`,
    };
  return {
    h2: read.every((sample) => sample.notH2.length === 0),
    samples: read,
    reason: null,
  };
}

/** The read-out's line for the edge protocol witness. */
export function edgeProtocolLine(witness: EdgeProtocolWitness): string {
  if (witness.h2 === null)
    return `edge protocol: not shown: ${witness.reason ?? "?"}`;
  const responses = witness.samples.reduce(
    (total, sample) => total + sample.responses,
    0,
  );
  const of = `${String(responses)} responses in ${String(witness.samples.length)} judged cold samples`;
  if (witness.h2) return `edge protocol: all ${of} arrived over HTTP/2`;
  const notH2 = witness.samples.flatMap((sample) =>
    sample.notH2.map((path) => `${sample.name} ${path}`),
  );
  return `edge protocol: ${String(notH2.length)} of ${of} did not arrive over HTTP/2, so this run's transport is not the edge's HTTP/2: ${notH2.join(", ")}`;
}

/**
 * Whether every judged cold sample asked for the one artifact object the run
 * expects its pages to load, and for no other artifact. calque publishes a
 * cut's representations one after another, so a run whose hold ends before
 * the last of them loads one object in its early samples and another in its
 * late ones. `one` is null, with the reason, where the run expects no object
 * or took no judged cold sample.
 */
interface ArtifactObjectWitness {
  readonly one: boolean | null;
  /** The expected object's URL as a ledger keeps it. */
  readonly expected: string | null;
  /** Each artifact URL a judged cold sample asked for, and those samples. */
  readonly objects: readonly {
    readonly url: string;
    readonly samples: readonly string[];
  }[];
  /** The judged cold samples whose ledgers show no artifact request. */
  readonly without: readonly string[];
  readonly reason: string | null;
}

/**
 * `expected` is the object's URL as a ledger keeps it and calque's artifact
 * route as an absolute URL without an object: a request under that route is
 * one for an artifact.
 */
export function judgeArtifactObject(
  expected: {
    readonly objectUrl: string;
    readonly artifactRoute: string;
  } | null,
  samples: readonly {
    readonly name: string;
    readonly requests: readonly { readonly url: string }[];
  }[],
): ArtifactObjectWitness {
  if (expected === null || samples.length === 0)
    return {
      one: null,
      expected: expected?.objectUrl ?? null,
      objects: [],
      without: [],
      reason:
        expected === null
          ? "the run expects no artifact object"
          : "no judged cold sample",
    };
  const objects = new Map<string, string[]>();
  const without: string[] = [];
  for (const sample of samples) {
    const urls = new Set(
      sample.requests
        .map((request) => request.url)
        .filter((url) => url.startsWith(expected.artifactRoute)),
    );
    if (urls.size === 0) without.push(sample.name);
    for (const url of urls)
      objects.set(url, [...(objects.get(url) ?? []), sample.name]);
  }
  return {
    one:
      without.length === 0 &&
      objects.size === 1 &&
      objects.has(expected.objectUrl),
    expected: expected.objectUrl,
    objects: [...objects].map(([url, names]) => ({ url, samples: names })),
    without,
    reason: null,
  };
}

/** What a cold sample's requests took on the wire, by what they carried. */
export interface WireByPurpose {
  /** The bootstrap request; zero where the plan is rendered into the page. */
  readonly plan: number;
  /** The artifact's object. */
  readonly artifact: number;
  /** The change requests: the catch-up to the plan's cursor and the live stream. */
  readonly catchUp: number;
  /** Everything else: the page, its scripts, the decoder. */
  readonly page: number;
  readonly total: number;
}

/**
 * Each request's encoded bytes, headers included (`loadingFinished`'s
 * `encodedDataLength`), filed by the route it asked: `v1/bootstrap`,
 * `v1/artifact-objects/` and `v1/changes`. Chromium ends a large streamed body
 * as canceled once the page has read it and reports only some of its chunks,
 * so a request that answered 200 and never finished counts at its declared
 * `content-length` where that exceeds the chunks seen; the sample passed, so
 * the page had every byte.
 */
export function wireByPurpose(
  requests: readonly {
    readonly url: string;
    readonly state: string;
    readonly encoded: number;
    readonly chunkEncoded: number;
    readonly status: number | null;
    readonly contentLength: number | null;
  }[],
): WireByPurpose {
  const bytes = { plan: 0, artifact: 0, catchUp: 0, page: 0 };
  for (const request of requests) {
    const path = new URL(request.url).pathname;
    const purpose = path.endsWith("/v1/bootstrap")
      ? "plan"
      : path.includes("/v1/artifact-objects/")
        ? "artifact"
        : path.endsWith("/v1/changes")
          ? "catchUp"
          : "page";
    bytes[purpose] +=
      request.state === "finished"
        ? request.encoded
        : request.status === 200 && request.contentLength !== null
          ? Math.max(request.chunkEncoded, request.contentLength)
          : request.chunkEncoded;
  }
  return {
    ...bytes,
    total: bytes.plan + bytes.artifact + bytes.catchUp + bytes.page,
  };
}

/** The read-out's line for the artifact object witness. */
export function artifactObjectLine(witness: ArtifactObjectWitness): string {
  if (witness.one === null)
    return `artifact object: not shown: ${witness.reason ?? "?"}`;
  const expected = witness.expected ?? "?";
  if (witness.one)
    return `artifact object: all ${String(witness.objects[0]?.samples.length ?? 0)} judged cold samples asked for ${expected} and no other artifact`;
  const asked = [
    ...witness.objects.map(
      (object) => `${object.url} in ${object.samples.join(", ")}`,
    ),
    ...(witness.without.length === 0
      ? []
      : [`none in ${witness.without.join(", ")}`]),
  ];
  return `artifact object: the judged cold samples did not each ask for ${expected} alone, so this run's cold distribution is not of that one object: ${asked.join("; ")}`;
}

/**
 * Why a run that mixed its artifacts is not judged, as one more of its
 * unjudged reasons, or none.
 */
export function artifactObjectReasons(
  witness: Pick<ArtifactObjectWitness, "one">,
): string[] {
  return witness.one === false
    ? ["cold samples did not each load the one artifact object the run expects"]
    : [];
}

/**
 * One unlabelled sample from `calque`'s OpenMetrics text, such as
 * `calque_cdc_lag_bytes` or `calque_canonical_entries_appended_total`.
 */
export function parseMetric(text: string, name: string): number | null {
  for (const line of text.split("\n")) {
    if (!line.startsWith(`${name} `)) continue;
    const token =
      line
        .slice(name.length + 1)
        .trim()
        .split(" ")[0] ?? "";
    const value = token === "" ? Number.NaN : Number(token);
    return Number.isFinite(value) ? value : null;
  }
  return null;
}

export interface ReplicationSlotReading {
  readonly name: string;
  readonly active: boolean;
  /** WAL between PostgreSQL's current position and the slot's confirmed flush. */
  readonly lagBytes: number | null;
}

/**
 * `slot_name`, `active` and the lag in bytes, tab-separated, one logical
 * slot per row. A slot that has confirmed nothing has no lag.
 */
export function parseSlotRows(
  lines: readonly string[],
): ReplicationSlotReading[] {
  return lines
    .filter((line) => line.length > 0)
    .map((line) => {
      const [name = "", active = "", lag = ""] = line.split("\t");
      if (name === "" || (active !== "t" && active !== "f"))
        throw new Error(`unreadable replication slot row: ${line}`);
      const lagBytes = lag === "" ? null : Number(lag);
      if (lagBytes !== null && !Number.isSafeInteger(lagBytes))
        throw new Error(`unreadable replication slot lag: ${line}`);
      return { name, active: active === "t", lagBytes };
    });
}

// Traces ------------------------------------------------------------------------

interface TraceEvent {
  readonly name?: unknown;
  readonly ph?: unknown;
  readonly pid?: unknown;
  readonly tid?: unknown;
  readonly ts?: unknown;
  readonly dur?: unknown;
  readonly args?: unknown;
}

interface ThreadTasks {
  readonly process: string;
  readonly thread: string;
  readonly tasksOver50Ms: number;
  readonly longestTaskMs: number;
}

export interface TraceSummary {
  /** `null` when the trace holds no GC event: GC is unmeasured, not zero. */
  readonly gc: {
    readonly minor: number;
    readonly major: number;
    readonly totalMs: number;
  } | null;
  /** Renderer threads that ran at least one task over 50 ms. */
  readonly longTaskThreads: readonly ThreadTasks[];
  readonly events: number;
}

const MINOR_GC_EVENTS = new Set(["MinorGC", "ScavengeGC"]);
const MAJOR_GC_EVENTS = new Set(["MajorGC"]);
const TASK_EVENTS = new Set(["RunTask", "ThreadControllerImpl::RunTask"]);
const LONG_TASK_MICROSECONDS = 50_000;

function traceEvents(trace: unknown): readonly TraceEvent[] {
  if (Array.isArray(trace)) return trace as TraceEvent[];
  if (
    typeof trace === "object" &&
    trace !== null &&
    Array.isArray((trace as { traceEvents?: unknown }).traceEvents)
  )
    return (trace as { traceEvents: TraceEvent[] }).traceEvents;
  throw new Error("the trace holds no event array");
}

function metadataName(event: TraceEvent): string | null {
  const args = event.args as { name?: unknown } | undefined;
  return typeof args?.name === "string" ? args.name : null;
}

/**
 * GC events (`MinorGC`, `ScavengeGC`, `MajorGC`) and tasks over 50 ms on
 * every renderer thread, from a `devtools.timeline,v8,toplevel` trace.
 * Complete events (`ph: "X"`) carry their duration; begin and end pairs are
 * matched per thread and name.
 */
export function summarizeTrace(trace: unknown): TraceSummary {
  const events = traceEvents(trace);
  const processNames = new Map<string, string>();
  const threadNames = new Map<string, string>();
  for (const event of events) {
    if (event.ph !== "M") continue;
    const name = metadataName(event);
    if (name === null) continue;
    if (event.name === "process_name")
      processNames.set(String(event.pid), name);
    if (event.name === "thread_name")
      threadNames.set(`${String(event.pid)}:${String(event.tid)}`, name);
  }
  let minor = 0;
  let major = 0;
  let gcMicros = 0;
  const tasks = new Map<string, { count: number; longest: number }>();
  const open = new Map<string, number[]>();
  const complete = (event: TraceEvent, micros: number): void => {
    const name = String(event.name);
    if (MINOR_GC_EVENTS.has(name) || MAJOR_GC_EVENTS.has(name)) {
      if (MINOR_GC_EVENTS.has(name)) minor += 1;
      else major += 1;
      gcMicros += micros;
    }
    if (
      TASK_EVENTS.has(name) &&
      micros > LONG_TASK_MICROSECONDS &&
      processNames.get(String(event.pid)) === "Renderer"
    ) {
      const key = `${String(event.pid)}:${String(event.tid)}`;
      const entry = tasks.get(key) ?? { count: 0, longest: 0 };
      entry.count += 1;
      entry.longest = Math.max(entry.longest, micros);
      tasks.set(key, entry);
    }
  };
  for (const event of events) {
    const name = String(event.name);
    const key = `${String(event.pid)}:${String(event.tid)}:${name}`;
    if (event.ph === "X" && typeof event.dur === "number")
      complete(event, event.dur);
    else if (event.ph === "B" && typeof event.ts === "number")
      open.set(key, [...(open.get(key) ?? []), event.ts]);
    else if (event.ph === "E" && typeof event.ts === "number") {
      const stack = open.get(key);
      const began = stack?.pop();
      if (began !== undefined) complete(event, event.ts - began);
    }
  }
  return {
    gc:
      minor + major === 0 ? null : { minor, major, totalMs: gcMicros / 1_000 },
    longTaskThreads: [...tasks.entries()]
      .map(([key, entry]) => {
        const pid = key.split(":")[0] ?? "";
        return {
          process: processNames.get(pid) ?? pid,
          thread: threadNames.get(key) ?? key,
          tasksOver50Ms: entry.count,
          longestTaskMs: entry.longest / 1_000,
        };
      })
      .sort((left, right) => left.thread.localeCompare(right.thread)),
    events: events.length,
  };
}
