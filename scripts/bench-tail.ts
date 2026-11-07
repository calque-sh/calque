/** `--tail`: the transactions a run commits after the hold, and the re-cuts calque makes while it catches up. */
import type { CalqueServer } from "./bench-calque.ts";
import type { Run } from "./bench-context.ts";
import { execFileAsync, hrtimeMs } from "./bench-host.ts";
import { tailSql } from "./bench-oracle.ts";
import { runSql } from "./bench-postgres.ts";
import { log } from "./bench-record.ts";
import { parseCpuTime, parseMetric } from "./bench-results.ts";
import { delay } from "./demo-process.ts";

/** What `--tail` wrote and how long calque took to append it. */
export interface TailRecord {
  readonly transactions: number;
  /** From the first statement to the last commit. */
  readonly commitMs: number;
  /** From the last commit to calque's log holding every transaction. */
  readonly appendMs: number;
}

/** The longest a run waits for calque to append its tail. */
const TAIL_APPEND_TIMEOUT_MS = 30 * 60_000;

/**
 * Commits `transactions` of the load's transactions (`tailSql`), then waits
 * until calque has appended one canonical-log entry for each, so the plans the
 * run's pages get end after the tail and catch up through it from the
 * artifact's cut.
 */
export async function writeTail(
  metricsEndpoint: string,
  run: Pick<Run, "sourceEnvironment">,
  transactions: number,
): Promise<TailRecord> {
  const appended = async (): Promise<number> => {
    const response = await fetch(`${metricsEndpoint}/metrics`, {
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok)
      throw new Error(`/metrics answered ${String(response.status)}`);
    const value = parseMetric(
      await response.text(),
      "calque_canonical_entries_appended_total",
    );
    if (value === null)
      throw new Error(
        "/metrics has no calque_canonical_entries_appended_total",
      );
    return value;
  };
  const before = await appended();
  log(`tail: committing ${String(transactions)} transactions`);
  const started = hrtimeMs();
  runSql(tailSql(transactions), run.sourceEnvironment);
  const committed = hrtimeMs();
  const deadline = committed + TAIL_APPEND_TIMEOUT_MS;
  for (;;) {
    if ((await appended()) >= before + transactions) break;
    if (hrtimeMs() > deadline)
      throw new Error(
        `calque did not append the tail's ${String(transactions)} transactions within ${String(TAIL_APPEND_TIMEOUT_MS / 60_000)} min`,
      );
    await delay(250);
  }
  const record = {
    transactions,
    commitMs: committed - started,
    appendMs: hrtimeMs() - committed,
  };
  log(
    `tail: ${String(transactions)} transactions committed in ${record.commitMs.toFixed(0)} ms and appended ${record.appendMs.toFixed(0)} ms later`,
  );
  return record;
}

/** What calque's re-cut gauges read at once. */
interface RecutReading {
  readonly recuts: number;
  readonly inProgress: number;
  readonly tailEntries: number;
  readonly tailBytes: number;
  readonly thresholdBytes: number;
}

export async function readRecut(
  metricsEndpoint: string,
): Promise<RecutReading> {
  const response = await fetch(`${metricsEndpoint}/metrics`, {
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok)
    throw new Error(`/metrics answered ${String(response.status)}`);
  const text = await response.text();
  const read = (name: string) => {
    const value = parseMetric(text, name);
    if (value === null) throw new Error(`/metrics has no ${name}`);
    return value;
  };
  return {
    recuts: read("calque_artifact_recuts_total"),
    inProgress: read("calque_artifact_recut_in_progress"),
    tailEntries: read("calque_canonical_log_tail_entries"),
    tailBytes: read("calque_canonical_log_tail_bytes"),
    thresholdBytes: read("calque_artifact_recut_threshold_bytes"),
  };
}

/** One sample of calque while a tail is written and settles. */
interface RecutSample {
  readonly atMs: number;
  readonly reading: RecutReading;
  readonly rssBytes: number;
  readonly cpuSeconds: number;
  /** The state directory's size, read every fourth sample. */
  readonly stateBytes: number | null;
}

/** One re-cut, from the first sample that saw it in progress to the first that did not. */
export interface RecutWindow {
  readonly wallMs: number;
  readonly cpuSeconds: number;
  readonly rssBeforeBytes: number;
  readonly peakRssBytes: number;
  readonly stateBeforeBytes: number | null;
  readonly peakStateBytes: number | null;
  readonly stateAfterBytes: number | null;
}

/** What calque re-cut from a tail's first commit until its log was quiet. */
export interface RecutRecord {
  readonly recuts: number;
  /** From the tail's last append to the quiet log. */
  readonly settleMs: number;
  /** The entries and bytes still past the newest cut once quiet. */
  readonly tailEntries: number;
  readonly tailBytes: number;
  readonly thresholdBytes: number;
  readonly windows: readonly RecutWindow[];
}

const RECUT_SAMPLE_MS = 250;

const RECUT_QUIET_MS = 2_000;

const RECUT_SETTLE_TIMEOUT_MS = 15 * 60_000;

/** calque's re-cut gauges, RSS, CPU and state directory every `RECUT_SAMPLE_MS`. */
export class RecutSampler {
  readonly samples: RecutSample[] = [];
  readonly errors: string[] = [];
  #stopped = false;
  #loop: Promise<void> | null = null;

  constructor(
    private readonly server: CalqueServer,
    private readonly stateDirectory: string,
  ) {}

  start(): void {
    this.#loop = (async () => {
      for (let n = 0; !this.#stopped; n += 1) {
        const atMs = hrtimeMs();
        try {
          const [reading, process_, stateBytes] = await Promise.all([
            readRecut(this.server.metricsEndpoint),
            execFileAsync("ps", [
              "-o",
              "rss=,time=",
              "-p",
              String(this.server.pid),
            ]),
            n % 4 === 0
              ? execFileAsync("du", ["-sk", this.stateDirectory]).then(
                  ({ stdout }) => Number(stdout.trim().split(/\s+/u)[0]) * 1024,
                )
              : Promise.resolve(null),
          ]);
          const [rss = "", time = ""] = process_.stdout.trim().split(/\s+/u);
          const cpuSeconds = parseCpuTime(time);
          if (cpuSeconds === null) throw new Error(`ps reported ${time}`);
          this.samples.push({
            atMs,
            reading,
            rssBytes: Number(rss) * 1024,
            cpuSeconds,
            stateBytes,
          });
        } catch (error) {
          this.errors.push(
            error instanceof Error ? error.message : String(error),
          );
        }
        await delay(Math.max(0, atMs + RECUT_SAMPLE_MS - hrtimeMs()));
      }
    })();
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    await this.#loop;
  }
}

/** Each re-cut `samples` saw open and close, with what it cost calque. */
export function recutWindows(
  samples: readonly Pick<
    RecutSample,
    "atMs" | "reading" | "rssBytes" | "cpuSeconds" | "stateBytes"
  >[],
): RecutWindow[] {
  const windows: RecutWindow[] = [];
  let opened: number | null = null;
  for (let index = 0; index < samples.length; index += 1) {
    const sample = samples[index];
    if (sample === undefined) continue;
    const running = sample.reading.inProgress > 0;
    if (running && opened === null) opened = Math.max(0, index - 1);
    if (running || opened === null) continue;
    const span = samples.slice(opened, index + 1);
    const first = span[0];
    if (first !== undefined) {
      const states = span
        .map((entry) => entry.stateBytes)
        .filter((bytes): bytes is number => bytes !== null);
      const after = samples
        .slice(index)
        .find((entry) => entry.stateBytes !== null)?.stateBytes;
      windows.push({
        wallMs: sample.atMs - first.atMs,
        cpuSeconds: sample.cpuSeconds - first.cpuSeconds,
        rssBeforeBytes: first.rssBytes,
        peakRssBytes: Math.max(...span.map((entry) => entry.rssBytes)),
        stateBeforeBytes: states[0] ?? null,
        peakStateBytes: states.length === 0 ? null : Math.max(...states),
        stateAfterBytes: after ?? null,
      });
    }
    opened = null;
  }
  return windows;
}

/**
 * Waits until calque's log is quiet after a tail: no re-cut in progress, the
 * tail past the newest cut under its threshold or re-cutting off, and two
 * readings `RECUT_QUIET_MS` apart that agree, so a maintenance pass has
 * measured the whole tail.
 */
export async function settleRecuts(
  server: CalqueServer,
  recutsBefore: number,
): Promise<Omit<RecutRecord, "windows">> {
  const started = hrtimeMs();
  let quietReading: RecutReading | null = null;
  for (;;) {
    const reading = await readRecut(server.metricsEndpoint);
    const quiet =
      reading.inProgress === 0 &&
      (reading.thresholdBytes === 0 ||
        reading.tailBytes < reading.thresholdBytes);
    if (
      quiet &&
      quietReading !== null &&
      quietReading.recuts === reading.recuts &&
      quietReading.tailEntries === reading.tailEntries
    )
      return {
        recuts: reading.recuts - recutsBefore,
        settleMs: hrtimeMs() - started,
        tailEntries: reading.tailEntries,
        tailBytes: reading.tailBytes,
        thresholdBytes: reading.thresholdBytes,
      };
    quietReading = quiet ? reading : null;
    if (hrtimeMs() - started > RECUT_SETTLE_TIMEOUT_MS)
      throw new Error(
        `calque's log was not quiet ${String(RECUT_SETTLE_TIMEOUT_MS / 60_000)} min after the tail: ${JSON.stringify(reading)}`,
      );
    await delay(RECUT_QUIET_MS);
  }
}
