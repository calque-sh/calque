/// <reference lib="dom" />
/**
 * `--schema-change`'s samples: a live page through one
 * schema change under the write load, judged at the next schema generation.
 */
import {
  closeSync,
  openSync,
  readFileSync,
  readSync,
  renameSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { framesRecord, judgementRecord } from "./bench-capture.ts";
import { CONFIG_PATH } from "./bench-calque.ts";
import {
  loadTransactionsPerSecond,
  type SchemaChange,
  shapingFloorMs,
  judgeShaping,
} from "./bench-conditions.ts";
import { referenceOf, type Run, showPage } from "./bench-context.ts";
import {
  type DroppedFrameSignal,
  frameClockCheck,
  type HashedFrame,
  judgeStablePaint,
  mapEpochSecondsToMonotonicMilliseconds,
  STABLE_PAINT_RULE,
  type StablePaintJudgement,
} from "./bench-frames.ts";
import { hrtimeMs, rereadHostSleep } from "./bench-host.ts";
import {
  chartText,
  type OracleResult,
  pageOracle,
  readSourceState,
} from "./bench-oracle.ts";
import {
  type BenchWindow,
  oraclePage,
  readPageFacts,
  readPageState,
  readPublications,
  reportedError,
} from "./bench-page.ts";
import { requestPlan } from "./bench-page-server.ts";
import { PsqlSession, runSql } from "./bench-postgres.ts";
import { awaitQuietPublications, LoadWriter } from "./bench-realtime.ts";
import { log, writeRecord } from "./bench-record.ts";
import { distribution, secondReading, type Verdict } from "./bench-results.ts";
import {
  type Invalidity,
  probeInvalidity,
  reachedMs,
  type Retaken,
  type Taken,
} from "./bench-samples.ts";
import {
  clockCheck,
  closeSession,
  COLD_TIMEOUT_MS,
  HOLD_MARGIN_MS,
  navigate,
  openSession,
  readWallDriftMs,
  type Session,
  shapingWitness,
} from "./bench-session.ts";
import { delay } from "./demo-process.ts";

/** How long the load runs before the change. */
const LOAD_BEFORE_CHANGE_MS = 10_000;
/** How long rename-suspend leaves the column renamed. */
const RENAMED_FOR_MS = 10_000;
/** How long the page has, from the change, to reach its stable paint. */
const RECOVERY_TIMEOUT_MS = COLD_TIMEOUT_MS;
/** How long the reverse of a change has to serve its generation. */
const REVERSE_TIMEOUT_MS = 300_000;
const LOG_POLL_MS = 25;

const TRIAGE_NOTE = Object.freeze({
  name: "triage_note",
  postgresType: "text",
  nullable: true,
});

/** The configuration calque dev reads in a schema run, in the run directory. */
export function schemaConfigPath(run: Pick<Run, "directory">): string {
  return join(run.directory, "calque.config.json");
}

/** `calque dev`'s extra arguments in a schema run. */
export function schemaDevArguments(
  run: Pick<Run, "directory" | "options">,
): string[] {
  return run.options.schemaChange === undefined
    ? []
    : ["--config", schemaConfigPath(run)];
}

/**
 * Before calque dev enrolls the source: the case's configuration as the page
 * ships it, and the shape the case starts from.
 */
export function prepareSchemaRun(run: Run): void {
  const change = run.options.schemaChange;
  if (change === undefined) return;
  writeFileSync(schemaConfigPath(run), readFileSync(CONFIG_PATH, "utf8"));
  const setup = schemaSetupSql(change);
  if (setup.length > 0) runSql(setup, run.sourceEnvironment);
}

/** The shape each case starts from, set before calque dev enrolls the source. */
export function schemaSetupSql(change: SchemaChange): string {
  switch (change) {
    case "config-add":
      return "ALTER TABLE incidents.issues ADD COLUMN triage_note text;\n";
    case "rewrite-whole":
      return "ALTER TABLE incidents.memberships REPLICA IDENTITY FULL;\n";
    case "rename-suspend":
      return "";
  }
}

/**
 * `config` with `incidents.issues.triage_note` named, in the collection and
 * in each command on it that inserts, since an insert writes every non-key
 * column.
 */
export function withTriageNote(config: string): string {
  const parsed = JSON.parse(config) as {
    collections: { id: string; columns: object[] }[];
    commands?: {
      collection: string;
      operations: string[];
      writableColumns: string[];
    }[];
  };
  const issues = parsed.collections.find(
    (collection) => collection.id === "issues",
  );
  if (!issues) throw new Error("the configuration names no issues collection");
  if (
    issues.columns.some(
      (column) => (column as { name?: unknown }).name === TRIAGE_NOTE.name,
    )
  )
    throw new Error("the configuration already names triage_note");
  issues.columns.push({ ...TRIAGE_NOTE });
  for (const command of parsed.commands ?? [])
    if (
      command.collection === "issues" &&
      command.operations.includes("insert")
    )
      command.writableColumns.push(TRIAGE_NOTE.name);
  return `${JSON.stringify(parsed, null, 2)}\n`;
}

const REWRITE_SQL = (to: "upper" | "lower") =>
  `ALTER TABLE incidents.memberships ALTER COLUMN role TYPE text USING ${to}(role);\n`;
const RENAME_SQL = (from: string, to: string) =>
  `ALTER TABLE incidents.issues RENAME COLUMN ${from} TO ${to};\n`;

/** One line of calque's log, stamped when the bench read it. */
export interface LogLine {
  readonly text: string;
  readonly atMs: number;
}

/** Follows calque's log from where it ends now, stamping each new line. */
export class LogFollower {
  readonly lines: LogLine[] = [];
  #offset: number;
  #partial = "";
  #timer: NodeJS.Timeout | undefined;

  constructor(readonly path: string) {
    this.#offset = statSync(path).size;
  }

  start(): void {
    this.#timer = setInterval(() => this.poll(), LOG_POLL_MS);
  }

  stop(): void {
    if (this.#timer) clearInterval(this.#timer);
    this.poll();
  }

  poll(): void {
    const size = statSync(this.path).size;
    if (size <= this.#offset) return;
    const buffer = Buffer.alloc(size - this.#offset);
    const fd = openSync(this.path, "r");
    try {
      readSync(fd, buffer, 0, buffer.length, this.#offset);
    } finally {
      closeSync(fd);
    }
    this.#offset = size;
    const now = hrtimeMs();
    const parts = (this.#partial + buffer.toString("utf8")).split("\n");
    this.#partial = parts.pop() ?? "";
    for (const text of parts) this.lines.push({ text, atMs: now });
  }

  /** The first line from `fromMs` on that `matches`, waiting up to `timeoutMs`. */
  async waitFor(
    matches: (text: string) => boolean,
    fromMs: number,
    timeoutMs: number,
  ): Promise<LogLine | null> {
    const deadline = hrtimeMs() + timeoutMs;
    for (;;) {
      this.poll();
      const found = this.lines.find(
        (line) => line.atMs >= fromMs && matches(line.text),
      );
      if (found) return found;
      if (hrtimeMs() > deadline) return null;
      await delay(LOG_POLL_MS);
    }
  }
}

export const servingLine = (generation: number) => (text: string) =>
  text.includes(`Schema generation ${String(generation)}: serving`);

/** What calque's log says of one move, from the change on. */
export interface MoveLog {
  readonly boundaries: number;
  readonly serving: number;
  readonly notStarted: number;
  readonly stops: number;
  readonly schemaStops: number;
  readonly otherErrors: readonly string[];
  readonly suspendedMs: number | null;
  readonly movingMs: number | null;
  readonly boundaryMs: number | null;
  readonly firstCutMs: number | null;
  readonly servingMs: number | null;
}

/**
 * Reads `lines`, calque's log from the change on, for the move to
 * `generation`. Times are the bench's clock when it read each line.
 */
export function readMoveLog(
  lines: readonly LogLine[],
  generation: number,
): MoveLog {
  const g = String(generation);
  const first = (test: (text: string) => boolean) =>
    lines.find((line) => test(line.text))?.atMs ?? null;
  const count = (test: (text: string) => boolean) =>
    lines.filter((line) => test(line.text)).length;
  const boundary = (text: string) =>
    text.includes(
      `the stream skips every transaction until the first cut of schema generation ${g}`,
    );
  const firstCut = (text: string) =>
    text.includes(
      `the stream serves schema generation ${g} from its first cut`,
    );
  const stop = (text: string) =>
    text.includes("CDC runtime stopped without reconnecting");
  return {
    boundaries: count(boundary),
    serving: count(servingLine(generation)),
    notStarted: count((text) =>
      text.includes("The next schema generation did not start"),
    ),
    stops: count(stop),
    schemaStops: count((text) =>
      text.startsWith("The source's schema changed ("),
    ),
    otherErrors: lines
      .filter((line) => / ERROR /u.test(line.text) && !stop(line.text))
      .map((line) => line.text),
    suspendedMs: first((text) => text.startsWith("Schema suspended: ")),
    movingMs: first((text) =>
      text.endsWith("moving to the next schema generation"),
    ),
    boundaryMs: first(boundary),
    firstCutMs: first(firstCut),
    servingMs: first(servingLine(generation)),
  };
}

/** One `/v1/bootstrap` answer the page received, on the bench's clock. */
export interface BootstrapAnswer {
  readonly atMs: number;
  readonly status: number;
  readonly code: string | null;
  readonly schemaGeneration: number | null;
  /** Why the body named no code and no generation, when it did not. */
  readonly unread: string | null;
}

/** The write load's commits before the change, against the rate it paces. */
export interface LoadBefore {
  readonly rate: number;
  readonly completed: number;
  readonly ms: number;
  readonly transactionsPerSecond: number;
}

export function loadBeforeChange(
  completed: number,
  ms: number,
  rate: number,
): LoadBefore {
  return {
    rate,
    completed,
    ms,
    transactionsPerSecond: ms > 0 ? (completed * 1_000) / ms : 0,
  };
}

/** The client's status log, on the bench's clock. */
export interface StatusChange {
  readonly state: string;
  readonly atMs: number;
}

/**
 * How the page lived through the move from `fromMs` on: how often it left
 * `ready`, and when it came back to it for the last time.
 */
export function rebuildsAfter(
  log: readonly StatusChange[],
  fromMs: number,
): { readonly rebuilds: number; readonly readyMs: number | null } {
  let state =
    log.filter((change) => change.atMs < fromMs).at(-1)?.state ?? null;
  let rebuilds = 0;
  let readyMs: number | null = null;
  for (const change of log) {
    if (change.atMs < fromMs) continue;
    if (state === "ready" && change.state !== "ready") rebuilds += 1;
    if (change.state === "ready" && state !== "ready") readyMs = change.atMs;
    state = change.state;
  }
  return { rebuilds, readyMs: state === "ready" ? readyMs : null };
}

/** The frames a recovery judges: the one on screen at `readyMs`, restamped then, and every later one. */
function recoveryFrames(
  frames: readonly HashedFrame[],
  readyMs: number,
): HashedFrame[] {
  const current = frames.filter((frame) => frame.timestampMs <= readyMs).at(-1);
  return [
    ...(current ? [{ ...current, timestampMs: readyMs }] : []),
    ...frames.filter((frame) => frame.timestampMs > readyMs),
  ];
}

/**
 * The stable paint of `expected` in the grid from `readyMs`, when the page's
 * state reached the next generation: the frame on screen then, restamped at
 * `readyMs`, or a later one. The end of the recovery is the later of the two.
 */
export function recoveryPaint(
  frames: readonly HashedFrame[],
  readyMs: number,
  expected: string,
  signals: readonly DroppedFrameSignal[],
): StablePaintJudgement {
  return judgeStablePaint(recoveryFrames(frames, readyMs), "grid", expected, {
    signals,
  });
}

/**
 * When the grid first showed `expected` from `readyMs` on: the match, or, when
 * a dropped-frame signal stopped it, the earliest frame showing it, the value
 * a cold sample taken again reports. Null when neither.
 */
export function recoveryReachedMs(
  frames: readonly HashedFrame[],
  readyMs: number,
  expected: string,
  judgement: StablePaintJudgement,
): number | null {
  const reached = reachedMs(
    judgement,
    recoveryFrames(frames, readyMs),
    "grid",
    expected,
    readyMs,
  );
  return reached === null ? null : readyMs + reached;
}

/** The current schema generation calque plans for the run's own request. */
async function plannedGeneration(run: Run): Promise<number> {
  if (!run.connection || !run.server)
    throw new Error("a schema sample needs the serving calque");
  const plan = JSON.parse(
    await requestPlan(run.connection, `${run.server.endpoint}/`),
  ) as { source?: { schemaGeneration?: unknown } };
  const generation = plan.source?.schemaGeneration;
  if (typeof generation !== "number")
    throw new Error("calque's plan names no schema generation");
  return generation;
}

async function readStatusLog(
  session: Session,
): Promise<readonly StatusChange[]> {
  const offset = session.network.monotonicMinusEpochSeconds;
  if (offset === null) return [];
  const { timeOrigin, entries } = await session.page.evaluate(() => ({
    timeOrigin: performance.timeOrigin,
    entries:
      (globalThis as unknown as BenchWindow).__incidents?.statusLog ?? [],
  }));
  return entries.map((entry) => ({
    state: entry.state,
    atMs: mapEpochSecondsToMonotonicMilliseconds(
      (timeOrigin + entry.at) / 1_000,
      offset,
    ),
  }));
}

/** One `/v1/bootstrap` answer as the page's `fetch` received it. */
interface PageBootstrap {
  readonly at: number;
  readonly status: number;
  readonly code: string | null;
  readonly schemaGeneration: number | null;
  readonly unread: string | null;
}

/**
 * Run in the page before its scripts: records each `/v1/bootstrap` answer the
 * page's `fetch` receives, its body read from a clone. Chrome may drop a body
 * before DevTools asks for it, so the page reads it itself.
 */
function recordBootstraps(): void {
  const answers: PageBootstrap[] = [];
  Object.defineProperty(globalThis, "__benchBootstraps", { value: answers });
  const original = globalThis.fetch.bind(globalThis);
  globalThis.fetch = async (input, init) => {
    const response = await original(input, init);
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.href
          : input.url;
    if (!new URL(url, location.href).pathname.endsWith("/v1/bootstrap"))
      return response;
    const at = performance.now();
    const status = response.status;
    void response
      .clone()
      .text()
      .then(
        (text) => {
          let body: {
            code?: unknown;
            source?: { schemaGeneration?: unknown };
          } = {};
          let unread: string | null = null;
          try {
            body = JSON.parse(text) as typeof body;
          } catch {
            unread = `not JSON: ${JSON.stringify(text.slice(0, 80))} of ${String(text.length)} characters`;
          }
          answers.push({
            at,
            status,
            code: typeof body.code === "string" ? body.code : null,
            schemaGeneration:
              typeof body.source?.schemaGeneration === "number"
                ? body.source.schemaGeneration
                : null,
            unread,
          });
        },
        (error: unknown) => {
          answers.push({
            at,
            status,
            code: null,
            schemaGeneration: null,
            unread: error instanceof Error ? error.message : String(error),
          });
        },
      );
    return response;
  };
}

async function readBootstraps(
  session: Session,
): Promise<readonly BootstrapAnswer[]> {
  const offset = session.network.monotonicMinusEpochSeconds;
  if (offset === null) return [];
  const { timeOrigin, entries } = await session.page.evaluate(() => ({
    timeOrigin: performance.timeOrigin,
    entries: ((globalThis as { __benchBootstraps?: PageBootstrap[] })
      .__benchBootstraps ?? []) as PageBootstrap[],
  }));
  return entries.map(({ at, ...answer }) => ({
    ...answer,
    atMs: mapEpochSecondsToMonotonicMilliseconds(
      (timeOrigin + at) / 1_000,
      offset,
    ),
  }));
}

/** Waits for the loaded page to hold still, as the reference pass does. */
async function settleLoaded(session: Session): Promise<string | null> {
  const quietMs = STABLE_PAINT_RULE.holdWindowMilliseconds + HOLD_MARGIN_MS;
  const started = hrtimeMs();
  let lastHash: string | null = null;
  let lastChangeSeenAt = hrtimeMs();
  for (;;) {
    await delay(100);
    if (hrtimeMs() - started > COLD_TIMEOUT_MS)
      return `the page did not settle within ${String(COLD_TIMEOUT_MS)} ms`;
    const hash = session.capture.prefix().at(-1)?.full ?? null;
    if (hash !== lastHash) {
      lastHash = hash;
      lastChangeSeenAt = hrtimeMs();
    }
    const facts = await readPageFacts(session.page);
    const error = reportedError(facts, session.pageErrors);
    if (error) return error;
    if (
      facts.status === "ready" &&
      facts.coldLoad?.chartAt != null &&
      facts.issueShown &&
      hash !== null &&
      session.capture.complete &&
      hrtimeMs() - lastChangeSeenAt >= quietMs
    )
      return null;
  }
}

/** The page's `memberships.role` values that are not upper case. */
async function lowerCaseRoles(session: Session): Promise<number> {
  const page = oraclePage(session.page);
  let lower = 0;
  const window = 10_000;
  for (let start = 0; ; start += window) {
    const rows = await page.slice("memberships", ["role"], start, window);
    for (const [role] of rows)
      if (typeof role !== "string" || role !== role.toUpperCase()) lower += 1;
    if (rows.length < window) return lower;
  }
}

/** Commit-to-publication on the page, PostgreSQL's clock against Chrome's. */
async function publicationLatencies(
  session: Session,
  fromMs: number,
  toMs: number,
): Promise<number[]> {
  const offset = session.network.monotonicMinusEpochSeconds;
  if (offset === null) return [];
  const { timeOrigin, entries } = await session.page.evaluate(() => ({
    timeOrigin: performance.timeOrigin,
    entries:
      (globalThis as unknown as BenchWindow).__incidents?.publicationLog ?? [],
  }));
  const latencies: number[] = [];
  for (const entry of entries) {
    if (entry.commitTimeUnixMicros === null) continue;
    const wallMs = timeOrigin + entry.at;
    const atMs = mapEpochSecondsToMonotonicMilliseconds(wallMs / 1_000, offset);
    if (atMs < fromMs || atMs > toMs) continue;
    latencies.push(wallMs - Number(entry.commitTimeUnixMicros) / 1_000);
  }
  return latencies;
}

export interface SchemaResult {
  readonly name: string;
  readonly kind: "warmup" | "schema" | "traced";
  readonly n: number;
  readonly verdict: Verdict;
  readonly invalid: readonly Invalidity[];
  readonly recoveryMs: number | null;
  /** The reached value, from the change: what a slot taken again counts in the second reading. */
  readonly reachedMs: number | null;
  readonly engagementPassed: boolean;
  readonly record: Record<string, unknown>;
}

/** Writes a sample's record again, marked when it is taken again. */
export function settleSchema(
  run: Run,
  sample: SchemaResult,
  retake: Taken["retake"],
): void {
  writeRecord(run, sample.name, {
    ...sample.record,
    retaken: retake !== null,
  });
  if (retake !== null)
    log(`${sample.name}: taken again (${retake.join("; ")})`);
}

/**
 * One schema sample: a fresh page loaded and settled, the load for 10 s, the
 * case's change under the load, the page's stable paint at the next
 * generation, the final oracle, then the reverse of the change.
 */
export async function schemaSample(
  run: Run,
  kind: SchemaResult["kind"],
  n: number,
): Promise<SchemaResult> {
  const change = run.options.schemaChange;
  const server = run.server;
  if (change === undefined || !server)
    throw new Error("a schema sample needs --schema-change and calque");
  const reference = referenceOf(run);
  if (!reference.grid) throw new Error("the reference pass has no grid hash");
  const name =
    kind === "schema" ? `schema-${String(n)}` : `schema-${kind}-${String(n)}`;
  const pid = server.pid;
  const generation = await plannedGeneration(run);
  const next = generation + 1;
  const follower = new LogFollower(server.log);
  follower.start();
  showPage(run);
  const session = await openSession(run, false);
  let bootstraps: readonly BootstrapAnswer[] = [];
  await session.page.addInitScript(recordBootstraps);
  let failure: string | null = null;
  let changeMs: number | null = null;
  let renamedMs: number | null = null;
  let judgement: StablePaintJudgement | null = null;
  let readyMs: number | null = null;
  let rebuilds = 0;
  let statusLog: readonly StatusChange[] = [];
  let oracle: OracleResult | null = null;
  const problems: string[] = [];
  let lowerRoles: number | null = null;
  let sourceLowerRoles: number | null = null;
  let wallDriftMs = 0;
  let hostSleptMs = 0;
  let clockPassed = false;
  let frameClockPassed = false;
  let shapingProblem: string | null = null;
  let liveLatencies: number[] = [];
  let load: Awaited<ReturnType<LoadWriter["stop"]>> | null = null;
  let loadBefore: LoadBefore | null = null;
  let frames: HashedFrame[] = [];
  try {
    const beforeGotoMs = await navigate(run, session);
    failure = await settleLoaded(session);
    const clock = clockCheck(session, beforeGotoMs, session.capture.prefix());
    clockPassed = clock.passed;
    const settledGrid = session.capture.prefix().at(-1)?.grid ?? null;
    if (failure === null && settledGrid !== reference.grid)
      failure = "the page's grid before the change differs from the reference";
    const wallDriftAtLoadMs = await readWallDriftMs(session.page);
    if (failure === null) {
      const writer = new LoadWriter(
        run.sourceEnvironment,
        loadTransactionsPerSecond(run.options),
      );
      const changer = new PsqlSession(run.sourceEnvironment, "bench-schema");
      const loadStartedMs = hrtimeMs();
      writer.start();
      try {
        await delay(LOAD_BEFORE_CHANGE_MS);
        loadBefore = loadBeforeChange(
          writer.completed,
          hrtimeMs() - loadStartedMs,
          loadTransactionsPerSecond(run.options),
        );
        session.network.wallShiftMs =
          (await readWallDriftMs(session.page)) - wallDriftAtLoadMs;
        rereadHostSleep();
        if (change === "config-add") {
          const path = schemaConfigPath(run);
          writeFileSync(
            `${path}.next`,
            withTriageNote(readFileSync(path, "utf8")),
          );
          renameSync(`${path}.next`, path);
          changeMs = hrtimeMs();
        } else if (change === "rewrite-whole") {
          await changer.run(REWRITE_SQL("upper"), "bench-schema-rewrite");
          changeMs = hrtimeMs();
        } else {
          await changer.run(
            RENAME_SQL("priority", "priority_renamed"),
            "bench-schema-rename",
          );
          renamedMs = hrtimeMs();
          await delay(Math.max(0, renamedMs + RENAMED_FOR_MS - hrtimeMs()));
          await changer.run(
            RENAME_SQL("priority_renamed", "priority"),
            "bench-schema-rename-back",
          );
          changeMs = hrtimeMs();
        }
        liveLatencies = await publicationLatencies(
          session,
          loadStartedMs,
          renamedMs ?? changeMs,
        );
        const movedFromMs = renamedMs ?? changeMs;
        const deadline = changeMs + RECOVERY_TIMEOUT_MS;
        const quietMs =
          STABLE_PAINT_RULE.holdWindowMilliseconds + HOLD_MARGIN_MS;
        for (;;) {
          await delay(100);
          const now = hrtimeMs();
          statusLog = await readStatusLog(session);
          ({ rebuilds, readyMs } = rebuildsAfter(statusLog, movedFromMs));
          if (readyMs !== null && rebuilds > 0) {
            const prefix = session.capture.prefix();
            judgement = recoveryPaint(
              prefix,
              readyMs,
              reference.grid,
              session.capture.signals.map((entry) => entry.signal),
            );
            // A dropped-frame signal before the match leaves none: the sample
            // is then INVALID_RUN once the grid has shown the reference for
            // the hold, not DNC at the deadline.
            const reachedAt = recoveryReachedMs(
              prefix,
              readyMs,
              reference.grid,
              judgement,
            );
            if (
              reachedAt !== null &&
              now - reachedAt >= quietMs &&
              session.capture.complete
            )
              break;
          }
          const error = reportedError(
            await readPageState(session.page),
            session.pageErrors,
          );
          if (error) {
            failure = error;
            break;
          }
          if (now > deadline) {
            failure = `the page did not reach a stable paint at schema generation ${String(next)} within ${String(RECOVERY_TIMEOUT_MS)} ms of the change`;
            break;
          }
        }
        hostSleptMs = rereadHostSleep();
        wallDriftMs =
          (await readWallDriftMs(session.page)) -
          wallDriftAtLoadMs -
          session.network.wallShiftMs;
      } finally {
        load = await writer.stop();
        await changer.close();
      }
      const drain = await awaitQuietPublications(() =>
        readPublications(session.page),
      );
      if (!drain.quiet && failure === null)
        failure = "the page kept publishing after the load stopped";
      try {
        const shaping =
          shapingFloorMs(run.options.network) === null
            ? judgeShaping(run.options.network, [])
            : await shapingWitness(run, session);
        if (shaping.shaped === false)
          shapingProblem = "Chrome's network emulation did not hold";
      } catch (error) {
        shapingProblem = `the shaping witness failed: ${error instanceof Error ? error.message : String(error)}`;
      }
    }
    frames = await session.capture.stop();
    await session.rss.stop();
    frameClockPassed = frameClockCheck(
      frames.filter(
        (frame) => frame.timestampMs >= (renamedMs ?? changeMs ?? 0),
      ),
    ).passed;
    if (failure === null) {
      const state = readSourceState(run.tables, run.sourceEnvironment);
      oracle = await pageOracle(
        oraclePage(session.page),
        run.tables,
        state.reference,
      );
      const facts = await readPageFacts(session.page);
      if (oracle.unsettled !== null)
        failure = `the oracle: ${oracle.unsettled}`;
      else {
        problems.push(...oracle.problems);
        if (facts.gridRows !== state.events)
          problems.push(
            `the grid shows ${String(facts.gridRows)} events and PostgreSQL holds ${String(state.events)}`,
          );
        if (chartText(facts.chart) !== chartText(state.buckets))
          problems.push("the chart differs from GROUP BY status, priority");
      }
      if (change === "rewrite-whole") {
        lowerRoles = await lowerCaseRoles(session);
        sourceLowerRoles = Number(
          runSql(
            "SELECT count(*) FROM incidents.memberships WHERE role <> upper(role);\n",
            run.sourceEnvironment,
          ).trim(),
        );
        if (lowerRoles !== 0 || sourceLowerRoles !== 0)
          problems.push(
            `memberships.role is not upper case in ${String(lowerRoles)} rows of the page and ${String(sourceLowerRoles)} of PostgreSQL`,
          );
      }
    }
  } catch (error) {
    failure ??= error instanceof Error ? error.message : String(error);
  } finally {
    bootstraps = await readBootstraps(session).catch(() => bootstraps);
    await closeSession(session);
  }
  follower.stop();
  const fromMs = renamedMs ?? changeMs;
  const window =
    fromMs === null ? [] : follower.lines.filter((line) => line.atMs >= fromMs);
  const moveLog = readMoveLog(window, next);
  const after =
    fromMs === null ? [] : bootstraps.filter((answer) => answer.atMs >= fromMs);
  const plans = after.filter((answer) => answer.status === 200);
  const suspendedAnswers =
    renamedMs === null || changeMs === null
      ? 0
      : after.filter(
          (answer) =>
            answer.atMs <= changeMs &&
            answer.status === 503 &&
            answer.code === "SCHEMA_SUSPENDED",
        ).length;
  const plannedNext =
    plans.length > 0 &&
    plans.every((answer) => answer.schemaGeneration === next);
  if (failure === null && !plannedNext)
    problems.push(
      `the page's plans after the change named schema generations ${plans.map((answer) => String(answer.schemaGeneration)).join(", ") || "none"}, not ${String(next)}`,
    );
  const calqueAlive = isAlive(pid) && run.server?.pid === pid;
  const invalid = probeInvalidity(judgement, {
    sessionClockPassed: clockPassed,
    frameClockPassed,
    wallDriftMs,
    hostSleptMs,
  });
  if (shapingProblem !== null)
    invalid.push({ reason: shapingProblem, instrument: true });
  // A cold sample's order: a timeout or a page error, a difference, the
  // instrument or a contradiction, then no match.
  const verdict: Verdict =
    failure !== null || judgement === null
      ? "DNC"
      : problems.length > 0
        ? "INCORRECT"
        : invalid.length > 0
          ? "INVALID_RUN"
          : judgement.matchMs === null
            ? "DNC"
            : "PASS";
  const recoveryMs =
    judgement?.matchMs == null || changeMs === null
      ? null
      : judgement.matchMs - changeMs;
  const reachedAt =
    judgement === null || readyMs === null
      ? null
      : recoveryReachedMs(frames, readyMs, reference.grid, judgement);
  const reachedMsFromChange =
    reachedAt === null || changeMs === null ? null : reachedAt - changeMs;
  const suspendedBeforeRenameBack =
    moveLog.suspendedMs !== null &&
    changeMs !== null &&
    moveLog.suspendedMs <= changeMs;
  const engagement = {
    generationBefore: generation,
    plannedGenerations: plans.map((answer) => answer.schemaGeneration),
    rebuilds,
    plannedNextOnce: plannedNext && rebuilds === 1,
    log: moveLog,
    logHolds:
      moveLog.boundaries === 1 &&
      moveLog.serving === 1 &&
      moveLog.notStarted === 0 &&
      moveLog.stops <= moveLog.schemaStops &&
      moveLog.otherErrors.length === 0 &&
      calqueAlive,
    suspendedAnswers,
    suspension:
      change === "rename-suspend"
        ? suspendedBeforeRenameBack && suspendedAnswers > 0
        : null,
    lowerRoles,
    sourceLowerRoles,
    upperRoles:
      change === "rewrite-whole"
        ? lowerRoles === 0 && sourceLowerRoles === 0
        : null,
  };
  const engagementPassed =
    engagement.plannedNextOnce &&
    engagement.logHolds &&
    engagement.suspension !== false &&
    engagement.upperRoles !== false;
  const record: Record<string, unknown> = {
    kind: "schema",
    sampleKind: kind,
    n,
    change,
    verdict,
    failure,
    problems,
    invalid,
    recovery_ms: recoveryMs,
    reached_ms: reachedMsFromChange,
    changeMs,
    renamedMs,
    readyMs,
    // M2: the bench's reading of each line of calque's log, from the change
    // M1 starts at. Under rename-suspend, the move's first line and the
    // suspension are read from the first rename, which starts them.
    server_ms: {
      moving: since(moveLog.movingMs, fromMs),
      boundary: since(moveLog.boundaryMs, changeMs),
      firstCut: since(moveLog.firstCutMs, changeMs),
      serving: since(moveLog.servingMs, changeMs),
      suspended: since(moveLog.suspendedMs, renamedMs),
    },
    // M3, reported: commit to the page's publication over the load before
    // the change, PostgreSQL's clock against Chrome's.
    live_before_ms: distribution(liveLatencies),
    // What PostgreSQL committed of the write load before the change, before
    // calque moved anything.
    load_before: loadBefore,
    load,
    engagement,
    engagementPassed,
    calque: { pid, alive: calqueAlive },
    statusLog,
    bootstraps,
    judgement: judgementRecord(judgement),
    oracle,
    clock: { clockPassed, frameClockPassed, wallDriftMs, hostSleptMs },
    frames: framesRecord(
      frames.filter((frame) => frame.timestampMs >= (fromMs ?? 0)),
    ),
    calqueLog: window.map((line) => line.text),
  };
  writeRecord(run, name, record);
  log(
    `${name}: ${verdict}${recoveryMs === null ? "" : ` ${recoveryMs.toFixed(0)} ms`}${failure === null ? "" : ` (${failure})`}${engagementPassed ? "" : "; an engagement gate failed"}`,
  );
  await reverse(run, change, next, follower.path);
  return {
    name,
    kind,
    n,
    verdict,
    invalid,
    recoveryMs,
    reachedMs: reachedMsFromChange,
    engagementPassed,
    record,
  };
}

function since(ms: number | null, fromMs: number | null): number | null {
  return ms === null || fromMs === null ? null : ms - fromMs;
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Undoes the change and waits for its move, which is not judged. */
async function reverse(
  run: Run,
  change: SchemaChange,
  served: number,
  logPath: string,
): Promise<void> {
  if (change === "rename-suspend") return;
  const follower = new LogFollower(logPath);
  const startedMs = hrtimeMs();
  if (change === "config-add") {
    const path = schemaConfigPath(run);
    writeFileSync(`${path}.next`, readFileSync(CONFIG_PATH, "utf8"));
    renameSync(`${path}.next`, path);
  } else runSql(REWRITE_SQL("lower"), run.sourceEnvironment);
  const line = await follower.waitFor(
    servingLine(served + 1),
    startedMs,
    REVERSE_TIMEOUT_MS,
  );
  if (line === null)
    throw new Error(
      `the reverse of ${change} did not serve schema generation ${String(served + 1)} within ${String(REVERSE_TIMEOUT_MS)} ms`,
    );
  log(
    `reverse of ${change}: schema generation ${String(served + 1)} serving after ${(line.atMs - startedMs).toFixed(0)} ms`,
  );
}

/** The schema section of a run's summary. */
export function schemaSummary(
  change: SchemaChange,
  samples: readonly SchemaResult[],
  retaken: readonly Retaken[],
) {
  const left = new Set(retaken.map((entry) => entry.n));
  const counted = samples.filter(
    (sample) => sample.kind !== "schema" || !left.has(sample.n),
  );
  const judged = counted.filter((sample) => sample.kind === "schema");
  const recoveryMs = judged.map((sample) =>
    sample.verdict === "PASS" ? sample.recoveryMs : null,
  );
  const second = secondReading(recoveryMs, retaken);
  return {
    change,
    verdicts: Object.fromEntries(
      samples.map((sample) => [sample.name, sample.verdict]),
    ),
    recovery_ms: recoveryMs,
    recovery: distribution(recoveryMs),
    ...(second === null
      ? {}
      : {
          secondReading: {
            recovery_ms: second,
            recovery: distribution(second),
          },
        }),
    engagementPassed: judged.every((sample) => sample.engagementPassed),
    allPassed: counted.every((sample) => sample.verdict === "PASS"),
  };
}
