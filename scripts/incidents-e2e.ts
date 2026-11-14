#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * `pnpm incidents:e2e`: the incident-operations page at 100k rows in headless
 * Chromium, checked against PostgreSQL.
 *
 * It runs `scripts/incidents.ts --no-open --fresh`, so calque prepares the
 * working set from PostgreSQL, and opens the page twice, A and B, each in a
 * fresh browser context with an empty HTTP cache. The grid, the chart, the
 * open-issues list and the issue detail must equal their SQL; comment and
 * status writes through A's controls must show optimistic, then canonical at
 * their CDC echo, on A and on B; and a tag inserted and deleted at the source
 * must reach both pages. It times live changes on A idle and under a full
 * re-sort, and reports cold load, submit-to-echo and preparation time, all
 * directional: the source is a container on the same host.
 *
 * Usage:
 *   CALQUE_INCIDENTS_POSTGRES_PORT=55481 pnpm incidents:e2e [-- [--dataset content-v1]]
 *
 * `--dataset` passes through to `pnpm incidents` (default v1).
 * `CALQUE_INCIDENTS_ROUNDS` sets the mutation round count (default 5),
 * `CALQUE_INCIDENTS_LIVE_SAMPLES` the live samples of each kind (default 5),
 * and `CALQUE_INCIDENTS_PORT` and `CALQUE_INCIDENTS_UI_PORT` pin the ports. The
 * report goes to `target/incidents-e2e/`.
 */
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { cpus, loadavg, totalmem } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import type { Browser, Page } from "playwright";

import { gridWindowSql } from "./bench-oracle.ts";
import {
  incidentsComposeEnvironment,
  incidentsDatasetVersion,
  type IncidentsDataset,
} from "./incidents.ts";
import {
  demoBrowserChannel,
  demoPsql,
  appendBounded,
  availablePort,
  delay,
  exitWithin,
  launchDemoBrowser,
  parseSampleCount,
  summarizeSamples,
} from "./demo-process.ts";
import { DEVELOPMENT_SOURCE_ROLE, parsePort } from "./demo.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const output = join(root, "target/incidents-e2e");

/**
 * How `calque dev` prepares a fresh state: the direct-columnar artifact built
 * straight from PostgreSQL binary COPY. It is not detected from the run, so it
 * is reported only for a fresh state; a reused one may come from an earlier
 * release.
 */
const FRESH_PREPARATION_PATH = "direct-copy";
const PROBE_TIMEOUT_MS = 180_000;
const WRITE_TIMEOUT_MS = 30_000;
const GRID_SETTLE_MS = 300;

/** What the page publishes on `globalThis.__incidents`, as far as this reads it. */
interface IncidentsView {
  readonly coldLoad: {
    readonly readyAt: number | null;
    readonly firstWindowAt: number | null;
    readonly gridPaintedAt: number | null;
    readonly chartAt: number | null;
    readonly issueAt: number | null;
  };
  readonly status: string;
  readonly error: string | null;
  readonly grid: {
    readonly view: { readonly level: string; readonly sort: string };
    readonly rows: number;
    readonly top: number;
    readonly ids: readonly string[];
  } | null;
  readonly chart: readonly Bucket[] | null;
  readonly issue: IssueView | null;
  readonly publicationLog: readonly PublicationRecord[];
  readonly mutations: () => readonly TrackedWrite[];
  readonly shown: () => readonly ShownValue[];
  readonly scrollGrid: ((row: number) => void) | null;
  readonly submitted: () => void;
}

interface PublicationRecord {
  readonly commitTimeUnixMicros: string | null;
  readonly at: number;
  readonly changedCollections: readonly string[];
}

interface ShownValue {
  readonly kind: "issue-title" | "issue-status";
  readonly value: string;
  readonly domAt: number;
  readonly paintedAt: number | null;
}

interface Bucket {
  readonly status: string;
  readonly priority: string;
  readonly count: number;
}

interface IssueView {
  readonly key: string;
  readonly detail: {
    readonly title: string;
    readonly status: string;
    readonly priority: string;
    readonly project: string;
    readonly slug: string;
    readonly assignee: string | null;
  } | null;
  readonly comments: readonly {
    readonly id: string;
    readonly body: string;
    readonly author: string;
    readonly createdAt: string;
  }[];
  readonly tags: readonly string[];
  readonly eventCount: number;
  readonly eventIds: readonly string[];
}

export interface TrackedWrite {
  readonly sequence: number;
  readonly collection: string;
  readonly operation: string;
  readonly key: string | null;
  readonly expected: string | null;
  readonly calledAt: number;
  readonly submittedAt: number | null;
  readonly mutationId: string | null;
  readonly state: string;
  readonly canonicalAt: number | null;
  readonly optimisticShownAt: number | null;
  readonly confirmedShownAt: number | null;
  readonly failure: string | null;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  // eslint-disable-next-line no-var
  var __incidents: IncidentsView | undefined;
}

/** The page URL `scripts/incidents.ts` announces once everything is up. */
export function announcedUrl(text: string): string | undefined {
  return /^Open: (http:\/\/127\.0\.0\.1:\d+\/)$/mu.exec(text)?.[1];
}

interface Preparation {
  readonly seconds: number;
  readonly state: "fresh" | "reused";
}

/** The launcher's `calque dev prepared the working set in X s (...)` line. */
export function announcedPreparation(text: string): Preparation | undefined {
  const match =
    /calque dev prepared the working set in (\d+(?:\.\d+)?) s \((fresh|reused) state\)/u.exec(
      text,
    );
  if (!match?.[1] || !match[2]) return undefined;
  return {
    seconds: Number(match[1]),
    state: match[2] === "fresh" ? "fresh" : "reused",
  };
}

/** `Seeded the fixture in X s.`, when this run had to seed. */
export function announcedSeed(text: string): number | undefined {
  const match = /Seeded the fixture in (\d+(?:\.\d+)?) s\./u.exec(text);
  return match?.[1] ? Number(match[1]) : undefined;
}

/** `demoPsql` output: one row per line, `\u0001` between columns. */
export function sqlRows(text: string): string[][] {
  return text
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => line.split("\u0001"));
}

/** Buckets in one order, whatever order either side produced them in. */
export function canonicalBuckets(buckets: readonly Bucket[]): Bucket[] {
  return [...buckets]
    .map(({ status, priority, count }) => ({ status, priority, count }))
    .sort((left, right) =>
      left.status === right.status
        ? Number(left.priority) - Number(right.priority)
        : left.status < right.status
          ? -1
          : 1,
    );
}

/**
 * Why one write did not go optimistic, then canonical, on screen in that
 * order; empty when it did.
 */
export function writeOrderProblems(write: TrackedWrite): string[] {
  const problems: string[] = [];
  const name = `${write.operation} ${write.collection} ${write.key}`;
  if (write.state !== "canonical")
    problems.push(
      `${name} ended ${write.state}${write.failure ? `: ${write.failure}` : ""}`,
    );
  if (write.submittedAt === null) problems.push(`${name} was never submitted`);
  if (write.optimisticShownAt === null)
    problems.push(`${name} was never on screen while optimistic`);
  if (write.canonicalAt === null)
    problems.push(`${name} never became canonical`);
  if (
    write.optimisticShownAt !== null &&
    write.canonicalAt !== null &&
    write.optimisticShownAt >= write.canonicalAt
  )
    problems.push(`${name} was shown optimistic only after its echo`);
  if (write.confirmedShownAt === null)
    problems.push(`${name} was never on screen confirmed`);
  return problems;
}

function uuidLiteral(key: string): string {
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u.test(key)
  )
    throw new Error(`not a uuid: ${key}`);
  return `'${key}'::uuid`;
}

const E2E_USAGE = "usage: pnpm incidents:e2e [-- [--dataset v1|content-v1]]";

/** `[-- [--dataset v1|content-v1]]`: the dataset the run seeds and checks. */
export function parseIncidentsE2eArguments(arguments_: readonly string[]): {
  readonly dataset: IncidentsDataset;
} {
  const rest = arguments_[0] === "--" ? arguments_.slice(1) : arguments_;
  let dataset: IncidentsDataset | undefined;
  for (let index = 0; index < rest.length; index += 1) {
    const argument = rest[index];
    const value = rest[index + 1];
    if (
      argument === "--dataset" &&
      dataset === undefined &&
      (value === "v1" || value === "content-v1")
    ) {
      dataset = value;
      index += 1;
    } else throw new Error(E2E_USAGE);
  }
  return { dataset: dataset ?? "v1" };
}

/** The artifacts a page's bootstrap plan selected. */
interface PlannedRepresentation {
  readonly encoding: string;
  readonly encodedBytes: number;
  readonly decodedBytes: number;
}

function plannedRepresentations(plan: unknown): PlannedRepresentation[] {
  const artifacts = (plan as { artifacts?: unknown } | null)?.artifacts;
  if (!Array.isArray(artifacts)) return [];
  return artifacts.map((artifact: unknown) => {
    const { decodedBytes, selectedRepresentation } = (artifact ?? {}) as {
      decodedBytes?: unknown;
      selectedRepresentation?: { encoding?: unknown; encodedBytes?: unknown };
    };
    return {
      encoding: String(selectedRepresentation?.encoding),
      encodedBytes: Number(selectedRepresentation?.encodedBytes),
      decodedBytes: Number(decodedBytes),
    };
  });
}

interface Launcher {
  readonly url: string;
  readonly preparation: Preparation;
  readonly seededSeconds: number | undefined;
  readonly output: () => string;
  readonly stop: () => Promise<void>;
}

async function startIncidents(
  environment: NodeJS.ProcessEnv,
  dataset: IncidentsDataset,
): Promise<Launcher> {
  const child = spawn(
    "node",
    [
      "--import",
      "./scripts/register.mjs",
      "scripts/incidents.ts",
      "--no-open",
      "--fresh",
      "--dataset",
      dataset,
    ],
    { cwd: root, env: environment, stdio: ["ignore", "pipe", "pipe"] },
  );
  let text = "";
  let preparation: Preparation | undefined;
  let seededSeconds: number | undefined;
  const collect = (chunk: string) => {
    text = appendBounded(text, chunk);
    preparation ??= announcedPreparation(text);
    seededSeconds ??= announcedSeed(text);
  };
  child.stdout.setEncoding("utf8").on("data", collect);
  child.stderr.setEncoding("utf8").on("data", collect);
  const exited = new Promise<{ code: number | null; signal: string | null }>(
    (resolveExit) =>
      child.once("exit", (code, signal) => resolveExit({ code, signal })),
  );
  const stop = async () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGINT");
    const result = await exitWithin(exited, 30_000);
    if (!result) {
      child.kill("SIGKILL");
      await exited;
      throw new Error("pnpm incidents did not stop on SIGINT");
    }
  };
  // Seeding the cut and preparing it fresh takes minutes.
  const deadline = Date.now() + 30 * 60_000;
  while (Date.now() < deadline) {
    const url = announcedUrl(text);
    if (url && preparation)
      return {
        url,
        preparation,
        seededSeconds,
        output: () => text,
        stop,
      };
    const state = await Promise.race([
      exited.then(() => "exited"),
      delay(250).then(() => "waiting"),
    ]);
    if (state === "exited")
      throw new Error(`pnpm incidents exited before it was ready:\n${text}`);
  }
  await stop();
  throw new Error(`pnpm incidents was not ready in 30 minutes:\n${text}`);
}

interface Oracle {
  readonly sql: (statement: string) => string[][];
}

function count(oracle: Oracle, statement: string): number {
  const value = oracle.sql(statement)[0]?.[0];
  assert(value !== undefined, `no count from ${statement}`);
  return Number(value);
}

function sqlIssueView(oracle: Oracle, key: string): IssueView {
  const issue = uuidLiteral(key);
  const detailRow = oracle.sql(`
    SELECT i.title, i.status, i.priority, p.name, p.slug,
           u.id IS NULL, coalesce(u.display_name, '')
    FROM incidents.issues i
    JOIN incidents.projects p ON p.id = i.project_id
    LEFT JOIN incidents.users u ON u.id = i.assignee_id
    WHERE i.id = ${issue}`)[0];
  const comments = oracle.sql(`
    SELECT c.id, c.body, u.display_name,
           to_char(c.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    FROM incidents.comments c
    JOIN incidents.users u ON u.id = c.author_id
    WHERE c.issue_id = ${issue}
    ORDER BY c.created_at, c.id`);
  return {
    key,
    detail: detailRow
      ? {
          title: detailRow[0] ?? "",
          status: detailRow[1] ?? "",
          priority: detailRow[2] ?? "",
          project: detailRow[3] ?? "",
          slug: detailRow[4] ?? "",
          assignee: detailRow[5] === "t" ? null : (detailRow[6] ?? ""),
        }
      : null,
    comments: comments.map(([id, body, author, createdAt]) => ({
      id: id ?? "",
      body: body ?? "",
      author: author ?? "",
      createdAt: createdAt ?? "",
    })),
    tags: oracle
      .sql(
        `SELECT tag FROM incidents.issue_tags WHERE issue_id = ${issue} ORDER BY tag COLLATE "C"`,
      )
      .map(([tag]) => tag ?? ""),
    eventCount: count(
      oracle,
      `SELECT count(*) FROM incidents.events WHERE issue_id = ${issue}`,
    ),
    eventIds: oracle
      .sql(
        `SELECT id FROM incidents.events WHERE issue_id = ${issue} ORDER BY occurred_at DESC, id DESC LIMIT 12`,
      )
      .map(([id]) => id ?? ""),
  };
}

/** The page's view with timestamps in one spelling, for comparison. */
function normalizedIssueView(view: IssueView): IssueView {
  return {
    ...view,
    comments: view.comments.map((comment) => ({
      ...comment,
      createdAt: new Date(comment.createdAt).toISOString(),
    })),
  };
}

function sqlBuckets(oracle: Oracle): Bucket[] {
  return canonicalBuckets(
    oracle
      .sql(
        "SELECT status, priority, count(*) FROM incidents.issues GROUP BY status, priority",
      )
      .map(([status, priority, total]) => ({
        status: status ?? "",
        priority: priority ?? "",
        count: Number(total),
      })),
  );
}

async function view(page: Page): Promise<IncidentsView> {
  return page.evaluate(() => {
    const probe = globalThis.__incidents;
    if (!probe) throw new Error("the page has no __incidents probe");
    return {
      ...probe,
      mutations: undefined,
      shown: undefined,
      scrollGrid: undefined,
      submitted: undefined,
    } as unknown as IncidentsView;
  });
}

async function writes(page: Page): Promise<readonly TrackedWrite[]> {
  return page.evaluate(() => globalThis.__incidents?.mutations() ?? []);
}

/**
 * Tells the page as each `POST /v1/mutations` leaves, which is where
 * submit-to-echo is measured from: the client posts a write once the facade
 * has applied it optimistically. It wraps `fetch` before the page's scripts
 * run, because the client keeps the `fetch` it is created with.
 */
function reportMutationSubmissions(): void {
  const fetch = globalThis.fetch.bind(globalThis);
  globalThis.fetch = (input, init) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.href
          : input.url;
    if (init?.method === "POST" && url.endsWith("/v1/mutations"))
      globalThis.__incidents?.submitted();
    return fetch(input, init);
  };
}

/**
 * Opens the page and waits for every panel's first complete view, and returns
 * it with what each of its bootstrap plans selected.
 */
async function openPage(
  browser: Browser,
  url: string,
  label: string,
  failures: string[],
): Promise<{ page: Page; plans: PlannedRepresentation[][] }> {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
  });
  await context.addInitScript(reportMutationSubmissions);
  const page = await context.newPage();
  page.on("pageerror", (error) => failures.push(`${label}: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error")
      failures.push(`${label} console: ${message.text()}`);
  });
  const plans: Promise<PlannedRepresentation[]>[] = [];
  page.on("response", (response) => {
    if (
      response.request().method() === "POST" &&
      new URL(response.url()).pathname.endsWith("/v1/bootstrap") &&
      response.ok()
    )
      plans.push(
        response.json().then(plannedRepresentations, (error: unknown) => {
          failures.push(`${label}: unreadable plan: ${String(error)}`);
          return [];
        }),
      );
  });
  await page.goto(url);
  await page.waitForFunction(
    () => {
      const probe = globalThis.__incidents;
      if (probe?.error) throw new Error(probe.error);
      return (
        probe?.coldLoad.gridPaintedAt !== null &&
        probe?.coldLoad.chartAt !== null &&
        probe?.coldLoad.issueAt !== null &&
        probe?.grid !== null &&
        probe?.issue !== null
      );
    },
    undefined,
    { timeout: PROBE_TIMEOUT_MS, polling: 50 },
  );
  return { page, plans: await Promise.all(plans) };
}

type Grid = NonNullable<IncidentsView["grid"]>;

/**
 * The grid once `accepted` holds for two equal reads `GRID_SETTLE_MS` apart.
 * The probe reads glide's accessibility table, which glide rebuilds 200 ms
 * after the grid's last change, so one read can still show the view before.
 */
async function settledGrid(
  page: Page,
  accepted: (grid: Grid) => boolean,
): Promise<Grid> {
  const deadline = Date.now() + PROBE_TIMEOUT_MS;
  let last: Grid | undefined;
  while (Date.now() < deadline) {
    const grid = (await view(page)).grid ?? undefined;
    const current = grid && accepted(grid) ? grid : undefined;
    if (current && last && isDeepStrictEqual(current, last)) return current;
    last = current;
    await delay(GRID_SETTLE_MS);
  }
  throw new Error("the grid never settled");
}

async function setGridView(
  page: Page,
  level: string,
  sort: string,
): Promise<Grid> {
  await page.selectOption("select[data-control=level]", level);
  await page.selectOption("select[data-control=sort]", sort);
  return settledGrid(
    page,
    (grid) =>
      grid.view.level === level && grid.view.sort === sort && grid.top === 0,
  );
}

/** Scrolls the grid so row `row` is at its top, as far as the grid can. */
async function scrollGrid(page: Page, row: number): Promise<Grid> {
  await page.evaluate((target) => {
    const scroll = globalThis.__incidents?.scrollGrid;
    if (!scroll) throw new Error("the grid is not mounted");
    scroll(target);
  }, row);
  return settledGrid(
    page,
    (grid) => grid.top <= row && row < grid.top + grid.ids.length,
  );
}

/** Waits for the page's newest tracked write to settle on screen. */
async function settledWrite(
  page: Page,
  sequence: number,
): Promise<TrackedWrite> {
  await page.waitForFunction(
    (wanted) => {
      const write = globalThis.__incidents
        ?.mutations()
        .find((candidate) => candidate.sequence === wanted);
      return (
        write !== undefined &&
        (write.confirmedShownAt !== null ||
          ["failed", "rejected", "conflict"].includes(write.state))
      );
    },
    sequence,
    { timeout: WRITE_TIMEOUT_MS, polling: 5 },
  );
  const write = (await writes(page)).find(
    (candidate) => candidate.sequence === sequence,
  );
  assert(write);
  return write;
}

/** Waits until page B shows what `shown` expects of its issue view. */
async function converged(
  page: Page,
  shown: (issue: IssueView) => boolean,
  description: string,
): Promise<void> {
  const deadline = Date.now() + WRITE_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const issue = (await view(page)).issue;
    if (issue && shown(issue)) return;
    await delay(10);
  }
  throw new Error(`the page never showed ${description}`);
}

/**
 * One psql session held open for the whole run. `demoPsql` starts a new
 * `docker compose exec` per statement, which takes hundreds of milliseconds
 * before the statement even runs; a change timed against a sort that is
 * already in flight has to commit within milliseconds of being asked for.
 */
class HeldPsql {
  readonly #child: ChildProcess;
  readonly #waiting = new Map<string, (error?: Error) => void>();
  #output = "";
  #next = 0;
  #failure: Error | undefined;

  constructor(environment: NodeJS.ProcessEnv) {
    this.#child = spawn(
      "docker",
      [
        "compose",
        "exec",
        "-T",
        "postgres",
        "psql",
        "-X",
        "--quiet",
        "--set",
        "ON_ERROR_STOP=1",
        "--username",
        DEVELOPMENT_SOURCE_ROLE,
        "--dbname",
        "calque_dev",
        "--no-align",
        "--tuples-only",
      ],
      { cwd: root, env: environment, stdio: ["pipe", "pipe", "pipe"] },
    );
    const collect = (chunk: string) => {
      this.#output = appendBounded(this.#output, chunk);
      for (const [marker, settle] of this.#waiting)
        if (this.#output.includes(`${marker}\n`)) {
          this.#waiting.delete(marker);
          settle();
        }
    };
    this.#child.stdout?.setEncoding("utf8").on("data", collect);
    this.#child.stderr?.setEncoding("utf8").on("data", collect);
    this.#child.once("exit", (code) => {
      this.#failure = new Error(
        `the held psql session exited with ${String(code)}:\n${this.#output}`,
      );
      for (const settle of this.#waiting.values()) settle(this.#failure);
      this.#waiting.clear();
    });
  }

  /** Runs one statement in autocommit; resolves once it has committed. */
  run(statement: string): Promise<void> {
    if (this.#failure) return Promise.reject(this.#failure);
    const marker = `incidents-held-psql-${String(this.#next++)}`;
    return new Promise((resolveRun, rejectRun) => {
      this.#waiting.set(marker, (error) =>
        error ? rejectRun(error) : resolveRun(),
      );
      this.#child.stdin?.write(`${statement};\n\\echo ${marker}\n`);
    });
  }

  close(): void {
    this.#child.stdin?.end();
    if (this.#child.exitCode === null) this.#child.kill("SIGTERM");
  }
}

/** One live timing: a change from its cause to A's screen. */
interface LiveSample {
  readonly kind: "source-commit-to-paint" | "click-to-optimistic-paint";
  /** Whether the sample began as the grid re-sorted every event. */
  readonly loaded: boolean;
  readonly ms: number;
  /**
   * Click to the optimistic DOM commit; only for a write, and `null` when the
   * status first reached the screen already confirmed.
   */
  readonly domMs: number | null;
  /** A write whose optimistic status was on screen before its echo landed. */
  readonly optimisticShown: boolean | null;
}

async function pageEpoch(page: Page): Promise<number> {
  return page.evaluate(() => performance.timeOrigin);
}

/**
 * Waits for the first publication of `events` after `after` (page ms): the
 * grid's live window re-runs its sort as it lands.
 */
async function eventsPublishedAfter(page: Page, after: number): Promise<void> {
  await page.waitForFunction(
    (since) =>
      (globalThis.__incidents?.publicationLog ?? []).some(
        (record) =>
          record.at > since && record.changedCollections.includes("events"),
      ),
    after,
    { timeout: WRITE_TIMEOUT_MS, polling: 1 },
  );
}

async function pageNow(page: Page): Promise<number> {
  return page.evaluate(() => performance.now());
}

/** The first paint of `value` as `kind` after `after` (page ms). */
async function paintedAfter(
  page: Page,
  kind: ShownValue["kind"],
  value: string,
  after: number,
): Promise<ShownValue> {
  await page.waitForFunction(
    ([wantedKind, wantedValue, since]) =>
      (globalThis.__incidents?.shown() ?? []).some(
        (record) =>
          record.kind === wantedKind &&
          record.value === wantedValue &&
          record.domAt > since &&
          record.paintedAt !== null,
      ),
    [kind, value, after] as const,
    { timeout: WRITE_TIMEOUT_MS, polling: 2 },
  );
  const shown = await page.evaluate(
    ([wantedKind, wantedValue, since]) =>
      (globalThis.__incidents?.shown() ?? []).find(
        (record) =>
          record.kind === wantedKind &&
          record.value === wantedValue &&
          record.domAt > since,
      ),
    [kind, value, after] as const,
  );
  assert(shown);
  return shown;
}

/**
 * Times live changes on page A, first with nothing else running and then
 * while the grid re-sorts all of `events`. The grid shows every level newest
 * first; an event changed at the source makes it read the view again, and the
 * sample starts as soon as that change is published. Each sample is one kind:
 *
 *  - a source commit to the issue's title, from the PostgreSQL commit time
 *    its publication carried to the second frame after the DOM held it;
 *  - a status change through A's control, from the facade call to the second
 *    frame after the DOM held the optimistic status.
 *
 * The event, the title and the status are put back afterwards.
 */
async function measureLiveUpdates(
  page: Page,
  psql: HeldPsql,
  oracle: Oracle,
  issueKey: string,
  samples: number,
): Promise<LiveSample[]> {
  const issue = uuidLiteral(issueKey);
  const [eventId, eventMessage] =
    oracle.sql(
      "SELECT id, message FROM incidents.events WHERE NOT is_sentinel ORDER BY id LIMIT 1",
    )[0] ?? [];
  assert(eventId && eventMessage !== undefined, "the fixture has no event");
  assert(/^[0-9]+$/u.test(eventId));
  const [title, status] =
    oracle.sql(
      `SELECT title, status FROM incidents.issues WHERE id = ${issue}`,
    )[0] ?? [];
  assert(title !== undefined && status, "the issue has no title or status");
  const otherStatus =
    status === "investigating" ? "unresolved" : "investigating";
  const quote = (text: string) => `'${text.replaceAll("'", "''")}'`;
  const origin = await pageEpoch(page);
  await setGridView(page, "all", "newest");

  const results: LiveSample[] = [];
  let shownStatus = status;
  let eventEdits = 0;
  for (const loaded of [false, true])
    for (let sample = 1; sample <= samples; sample += 1)
      for (const kind of [
        "source-commit-to-paint",
        "click-to-optimistic-paint",
      ] as const) {
        if (loaded) {
          const before = await pageNow(page);
          eventEdits += 1;
          await psql.run(
            `UPDATE incidents.events SET message = ${quote(`incidents live edit ${String(eventEdits)}`)} WHERE id = ${eventId}`,
          );
          await eventsPublishedAfter(page, before);
        }
        const began = await pageNow(page);
        let ms: number;
        let domMs: number | null = null;
        let optimisticShown: boolean | null = null;
        if (kind === "source-commit-to-paint") {
          const marker = `incidents live ${loaded ? "loaded" : "idle"} ${String(sample)} ${String(Date.now())}`;
          await psql.run(
            `UPDATE incidents.issues SET title = ${quote(marker)} WHERE id = ${issue}`,
          );
          const painted = await paintedAfter(
            page,
            "issue-title",
            marker,
            began,
          );
          const publication = [...(await view(page)).publicationLog]
            .reverse()
            .find(
              (record) =>
                record.at <= painted.domAt &&
                record.changedCollections.includes("issues") &&
                record.commitTimeUnixMicros !== null,
            );
          assert(
            publication?.commitTimeUnixMicros,
            "no publication carried the title",
          );
          const commitEpoch =
            Number(BigInt(publication.commitTimeUnixMicros)) / 1000;
          ms = origin + (painted.paintedAt ?? 0) - commitEpoch;
        } else {
          const next = shownStatus === status ? otherStatus : status;
          const sequence = (await writes(page)).length + 1;
          await page.selectOption("select[data-control=issue-status]", next);
          const write = await settledWrite(page, sequence);
          // A write queued behind a long read can first reach the screen
          // confirmed: that is the delay being measured, so under load it
          // is recorded, and every other ordering rule still holds.
          assert.deepEqual(
            writeOrderProblems(write).filter(
              (problem) => !loaded || !OPTIMISTIC_DISPLAY.test(problem),
            ),
            [],
          );
          shownStatus = next;
          const painted = await paintedAfter(
            page,
            "issue-status",
            next,
            write.calledAt,
          );
          ms = (painted.paintedAt ?? 0) - write.calledAt;
          optimisticShown =
            write.optimisticShownAt !== null &&
            write.canonicalAt !== null &&
            write.optimisticShownAt < write.canonicalAt;
          domMs = optimisticShown
            ? (write.optimisticShownAt ?? 0) - write.calledAt
            : null;
        }
        results.push({ kind, loaded, ms, domMs, optimisticShown });
      }

  await psql.run(
    `UPDATE incidents.events SET message = ${quote(eventMessage)} WHERE id = ${eventId}`,
  );
  await psql.run(
    `UPDATE incidents.issues SET title = ${quote(title)} WHERE id = ${issue}`,
  );
  if (shownStatus !== status) {
    const sequence = (await writes(page)).length + 1;
    await page.selectOption("select[data-control=issue-status]", status);
    await settledWrite(page, sequence);
  }
  await converged(
    page,
    (shown) => shown.detail?.title === title && shown.detail.status === status,
    "the issue's own title and status again",
  );
  await setGridView(page, "all", "engine");
  return results;
}

/** The two `writeOrderProblems` about when the optimistic value showed. */
const OPTIMISTIC_DISPLAY =
  /(?:was never on screen while optimistic|was shown optimistic only after its echo)$/u;

/** Idle against loaded, per kind, as the report states them. */
function liveUpdateSummary(samples: readonly LiveSample[]) {
  const pick = (kind: LiveSample["kind"], loaded: boolean) =>
    samples.filter(
      (sample) => sample.kind === kind && sample.loaded === loaded,
    );
  const summary = (kind: LiveSample["kind"], loaded: boolean) => {
    const chosen = pick(kind, loaded);
    const dom = chosen
      .map(({ domMs }) => domMs)
      .filter((value): value is number => value !== null);
    return {
      ms: summarizeSamples(chosen.map(({ ms }) => ms)),
      ...(kind === "click-to-optimistic-paint"
        ? {
            optimisticShown: chosen.filter((sample) => sample.optimisticShown)
              .length,
            ...(dom.length > 0 ? { domMs: summarizeSamples(dom) } : {}),
          }
        : {}),
    };
  };
  return {
    sourceCommitToPaint: {
      idle: summary("source-commit-to-paint", false),
      loaded: summary("source-commit-to-paint", true),
    },
    clickToOptimisticPaint: {
      idle: summary("click-to-optimistic-paint", false),
      loaded: summary("click-to-optimistic-paint", true),
    },
  };
}

async function main(): Promise<void> {
  const { dataset } = parseIncidentsE2eArguments(process.argv.slice(2));
  const rounds = parseSampleCount(
    process.env.CALQUE_INCIDENTS_ROUNDS ?? "5",
    "CALQUE_INCIDENTS_ROUNDS",
  );
  const liveSamples = parseSampleCount(
    process.env.CALQUE_INCIDENTS_LIVE_SAMPLES ?? "5",
    "CALQUE_INCIDENTS_LIVE_SAMPLES",
  );
  const postgresPort = parsePort(
    process.env.CALQUE_INCIDENTS_POSTGRES_PORT ?? "55442",
    "CALQUE_INCIDENTS_POSTGRES_PORT",
  );
  const environment: NodeJS.ProcessEnv = {
    ...process.env,
    CALQUE_INCIDENTS_POSTGRES_PORT: String(postgresPort),
    CALQUE_INCIDENTS_PORT:
      process.env.CALQUE_INCIDENTS_PORT ?? String(await availablePort()),
    CALQUE_INCIDENTS_UI_PORT:
      process.env.CALQUE_INCIDENTS_UI_PORT ?? String(await availablePort()),
  };
  const composeEnvironment = incidentsComposeEnvironment(
    process.env,
    postgresPort,
  );
  const oracle: Oracle = {
    sql: (statement) => sqlRows(demoPsql(statement, composeEnvironment)),
  };
  mkdirSync(output, { recursive: true });
  const loadBefore = loadavg();

  const launcher = await startIncidents(environment, dataset);
  const failures: string[] = [];
  let browser: Browser | undefined;
  let psql: HeldPsql | undefined;
  try {
    const issueKey = oracle.sql(`
      SELECT c.issue_id FROM incidents.comments c
      JOIN incidents.issues i ON i.id = c.issue_id
      WHERE NOT i.is_sentinel
        AND EXISTS (SELECT 1 FROM incidents.issue_tags t WHERE t.issue_id = i.id)
      GROUP BY c.issue_id ORDER BY count(*) DESC, c.issue_id LIMIT 1`)[0]?.[0];
    assert(issueKey, "the fixture has no commented, tagged issue");
    const url = `${launcher.url}?issue=${issueKey}`;
    const originalStatus = oracle.sql(
      `SELECT status FROM incidents.issues WHERE id = ${uuidLiteral(issueKey)}`,
    )[0]?.[0];
    assert(originalStatus);

    browser = await launchDemoBrowser();
    const { page: pageA, plans: plansA } = await openPage(
      browser,
      url,
      "A",
      failures,
    );
    const coldA = (await view(pageA)).coldLoad;
    const artifactA = await pageA.evaluate(() =>
      performance
        .getEntriesByType("resource")
        .filter((entry) => entry.name.includes("/_calque/"))
        .map((entry) => {
          const resource = entry as PerformanceResourceTiming;
          return {
            path: new URL(resource.name).pathname,
            startMs: Math.round(resource.startTime),
            endMs: Math.round(resource.responseEnd),
            transferBytes: resource.transferSize,
            decodedBytes: resource.decodedBodySize,
          };
        })
        .filter((entry) => entry.decodedBytes > 1024 * 1024),
    );
    const { page: pageB, plans: plansB } = await openPage(
      browser,
      url,
      "B",
      failures,
    );
    const coldB = (await view(pageB)).coldLoad;
    // The grid: the whole collection, a filter, and a sort, against SQL.
    const eventTotal = count(oracle, "SELECT count(*) FROM incidents.events");
    const wholeGrid = await settledGrid(pageA, () => true);
    assert.equal(wholeGrid.rows, eventTotal, "grid rows equal count(*)");
    const errors = await setGridView(pageA, "error", "newest");
    assert.equal(
      errors.rows,
      count(
        oracle,
        "SELECT count(*) FROM incidents.events WHERE level = 'error'",
      ),
      "the level filter's rows equal SQL's count",
    );
    assert.deepEqual(
      errors.ids,
      oracle
        .sql(gridWindowSql("error", "newest", 0, errors.ids.length))
        .map(([id]) => id),
      "the newest errors equal SQL's",
    );
    const oldest = await setGridView(pageA, "all", "oldest");
    assert.equal(oldest.rows, eventTotal);
    assert.deepEqual(
      oldest.ids,
      oracle
        .sql(gridWindowSql("all", "oldest", 0, oldest.ids.length))
        .map(([id]) => id),
      "the oldest events equal SQL's",
    );
    // Every sorted row is reachable: a middle row and the last one, over
    // every event and over one level.
    const scrolled: {
      readonly level: string;
      readonly target: number;
      readonly top: number;
      readonly rows: number;
    }[] = [];
    for (const level of ["all", "error"]) {
      const sorted = await setGridView(pageA, level, "newest");
      for (const target of [Math.floor(sorted.rows / 2), sorted.rows - 1]) {
        const shown = await scrollGrid(pageA, target);
        assert.equal(shown.rows, sorted.rows);
        assert.deepEqual(
          shown.ids,
          oracle
            .sql(gridWindowSql(level, "newest", shown.top, shown.ids.length))
            .map(([id]) => id),
          `${level} newest first at row ${String(shown.top)} equal SQL's`,
        );
        if (target === sorted.rows - 1)
          assert.equal(shown.top + shown.ids.length, sorted.rows);
        scrolled.push({ level, target, top: shown.top, rows: shown.rows });
      }
    }
    const restored = await setGridView(pageA, "all", "engine");
    assert.equal(restored.rows, eventTotal);

    // The chart and the relationship view, against GROUP BY and the joins.
    const openIssues = () =>
      oracle
        .sql(
          `SELECT i.id FROM incidents.issues i
           JOIN incidents.projects p ON p.id = i.project_id
           LEFT JOIN incidents.users u ON u.id = i.assignee_id
           WHERE i.status = 'unresolved' AND NOT i.is_sentinel
           ORDER BY i.priority DESC, i.updated_at DESC, i.id LIMIT 40`,
        )
        .map(([id]) => id);
    const listedIssues = (page: Page) =>
      page.$$eval("button[data-issue]", (buttons) =>
        buttons.map((button) => button.getAttribute("data-issue")),
      );
    const checkReadViews = async (page: Page, label: string) => {
      const probe = await view(page);
      assert(probe.chart && probe.issue, `${label} has no chart or issue`);
      assert.deepEqual(
        canonicalBuckets(probe.chart),
        sqlBuckets(oracle),
        `${label}: the chart equals GROUP BY status, priority`,
      );
      assert.deepEqual(
        normalizedIssueView(probe.issue),
        sqlIssueView(oracle, issueKey),
        `${label}: the relationship view equals the SQL joins`,
      );
      assert.equal(probe.grid?.rows, eventTotal, `${label}: grid rows`);
      assert.deepEqual(
        await listedIssues(page),
        openIssues(),
        `${label}: the open-issues list equals the SQL join`,
      );
    };
    // The same check once the chart and the list have caught up with a write
    // the page has already shown in the issue's detail.
    const settledReadViews = async (page: Page, label: string) => {
      const buckets = sqlBuckets(oracle);
      const listed = openIssues();
      const deadline = Date.now() + WRITE_TIMEOUT_MS;
      while (Date.now() < deadline) {
        const chart = (await view(page)).chart;
        if (
          chart &&
          isDeepStrictEqual(canonicalBuckets(chart), buckets) &&
          isDeepStrictEqual(await listedIssues(page), listed)
        )
          break;
        await delay(10);
      }
      await checkReadViews(page, label);
    };
    await checkReadViews(pageA, "A");
    await checkReadViews(pageB, "B");

    // The writes. Each is made through A's controls; B only watches.
    const settled: TrackedWrite[] = [];
    const settle = async (sequence: number): Promise<TrackedWrite> => {
      const write = await settledWrite(pageA, sequence);
      assert.deepEqual(writeOrderProblems(write), []);
      settled.push(write);
      return write;
    };
    let sequence = (await writes(pageA)).length;
    const otherStatus =
      originalStatus === "investigating" ? "unresolved" : "investigating";
    const statuses = [otherStatus, originalStatus] as const;
    for (let round = 1; round <= rounds; round += 1) {
      const body = `incidents e2e round ${round} ${Date.now()}`;
      await pageA.fill("textarea[data-control=new-comment]", body);
      await pageA.click("button[data-action=add-comment]");
      const { key } = await settle(++sequence);
      assert(key, "the inserted comment never showed");
      await converged(
        pageB,
        (issue) => issue.comments.some((c) => c.id === key && c.body === body),
        `the inserted comment ${key}`,
      );
      assert.equal(
        oracle.sql(
          `SELECT body FROM incidents.comments WHERE id = ${uuidLiteral(key)}`,
        )[0]?.[0],
        body,
      );

      const edited = `${body}, edited`;
      const row = `li[data-comment-id="${key}"]`;
      await pageA.click(`${row} button[data-action=edit]`);
      await pageA.fill(`${row} textarea[data-control=comment-edit]`, edited);
      await pageA.click(`${row} button[data-action=save]`);
      await settle(++sequence);
      await converged(
        pageB,
        (issue) =>
          issue.comments.some((c) => c.id === key && c.body === edited),
        `the edited comment ${key}`,
      );

      await pageA.click(`${row} button[data-action=delete]`);
      await settle(++sequence);
      await converged(
        pageB,
        (issue) => !issue.comments.some((c) => c.id === key),
        `the deleted comment ${key} gone`,
      );
      assert.equal(
        count(
          oracle,
          `SELECT count(*) FROM incidents.comments WHERE id = ${uuidLiteral(key)}`,
        ),
        0,
      );

      // The last round puts the status back, so a rerun starts where this did.
      const status =
        round === rounds
          ? originalStatus
          : statuses[(round - 1) % statuses.length];
      assert(status);
      const current = (await view(pageA)).issue?.detail?.status;
      if (status !== current) {
        await pageA.selectOption("select[data-control=issue-status]", status);
        await settle(++sequence);
        await converged(
          pageB,
          (issue) => issue.detail?.status === status,
          `the issue ${status}`,
        );
        await settledReadViews(pageA, `A with the issue ${status}`);
        await settledReadViews(pageB, `B with the issue ${status}`);
      }
    }

    // A tag committed at the source reaches both pages through the change
    // stream, keyed by `(issue_id, tag)`: a composite key with a text part.
    // The tag has a two-byte and a four-byte UTF-8 character.
    const tag = `incidents-e2e-ünï-\u{1d11e}-${Date.now()}`;
    const tagKey = `issue_id = ${uuidLiteral(issueKey)} AND tag = '${tag}'`;
    oracle.sql(
      `INSERT INTO incidents.issue_tags (issue_id, tag, bench_index, is_sentinel)
       SELECT ${uuidLiteral(issueKey)}, '${tag}', max(bench_index) + 1, false
         FROM incidents.issue_tags`,
    );
    for (const page of [pageA, pageB])
      await converged(
        page,
        (issue) => issue.tags.includes(tag),
        `the source-inserted tag ${tag}`,
      );
    oracle.sql(`DELETE FROM incidents.issue_tags WHERE ${tagKey}`);
    for (const page of [pageA, pageB])
      await converged(
        page,
        (issue) => !issue.tags.includes(tag),
        `the source-deleted tag ${tag} gone`,
      );

    psql = new HeldPsql(composeEnvironment);
    const live = await measureLiveUpdates(
      pageA,
      psql,
      oracle,
      issueKey,
      liveSamples,
    );

    // After the run, both pages equal PostgreSQL.
    await checkReadViews(pageA, "A after the writes");
    await checkReadViews(pageB, "B after the writes");
    assert.deepEqual(failures, []);

    const submitToEcho = settled.map(
      (write) => (write.canonicalAt ?? 0) - (write.submittedAt ?? 0),
    );
    const submitToConfirmedDom = settled.map(
      (write) => (write.confirmedShownAt ?? 0) - (write.submittedAt ?? 0),
    );
    const callToOptimisticDom = settled.map(
      (write) => (write.optimisticShownAt ?? 0) - write.calledAt,
    );
    const byCollection = (collection: string, values: readonly number[]) =>
      summarizeSamples(
        values.filter((_, index) => settled[index]?.collection === collection),
      );
    const cores = cpus();
    const report = {
      outcome: "passed",
      label:
        "directional: PostgreSQL in a container on the same host, a host not known to be quiet",
      date: new Date().toISOString(),
      host: {
        cpu: cores[0]?.model ?? "unknown",
        cores: cores.length,
        memoryBytes: totalmem(),
        platform: `${process.platform}-${process.arch}`,
        loadAverageBefore: loadBefore,
        loadAverageAfter: loadavg(),
      },
      checkout: checkoutIdentity(),
      dataset: incidentsDatasetVersion(dataset),
      browser: demoBrowserChannel(),
      rows: {
        events: eventTotal,
        total: count(
          oracle,
          `SELECT (SELECT count(*) FROM incidents.events)
            + (SELECT count(*) FROM incidents.issues)
            + (SELECT count(*) FROM incidents.comments)
            + (SELECT count(*) FROM incidents.users)
            + (SELECT count(*) FROM incidents.projects)
            + (SELECT count(*) FROM incidents.issue_tags)
            + (SELECT count(*) FROM incidents.memberships)`,
        ),
      },
      preparation: {
        seconds: launcher.preparation.seconds,
        state: launcher.preparation.state,
        path:
          launcher.preparation.state === "fresh"
            ? FRESH_PREPARATION_PATH
            : "prepared by an earlier run",
        seededSeconds: launcher.seededSeconds ?? null,
      },
      coldLoadMs: { A: coldA, B: coldB },
      plans: { A: plansA, B: plansB },
      gridScrolls: scrolled,
      artifactsA: artifactA,
      writes: {
        rounds,
        count: settled.length,
        submitToEchoMs: summarizeSamples(submitToEcho),
        submitToConfirmedDomMs: summarizeSamples(submitToConfirmedDom),
        callToOptimisticDomMs: {
          comments: byCollection("comments", callToOptimisticDom),
          issues: byCollection("issues", callToOptimisticDom),
        },
      },
      liveUpdates: {
        scenario:
          "loaded: one event changed at the source, and the sample starts once the grid has asked for its window of every event newest first, which sorts them all",
        ...liveUpdateSummary(live),
      },
      samples: settled,
      liveSamples: live,
    };
    writeFileSync(
      join(output, "report.json"),
      `${JSON.stringify(report, null, 2)}\n`,
    );
    const {
      samples: _samples,
      liveSamples: _liveSamples,
      ...headline
    } = report;
    process.stdout.write(`${JSON.stringify(headline, null, 2)}\n`);
  } catch (error) {
    process.stderr.write(
      `Incidents E2E failed: ${error instanceof Error ? error.message : String(error)}\n` +
        `Page failures: ${JSON.stringify(failures)}\n` +
        `Launcher output (tail):\n${launcher.output().slice(-20_000)}\n`,
    );
    throw error;
  } finally {
    psql?.close();
    await browser?.close();
    await launcher.stop();
  }
}

function checkoutIdentity(): { head: string; dirty: boolean } {
  const git = (...arguments_: string[]) =>
    spawnSync("git", arguments_, { cwd: root, encoding: "utf8" }).stdout.trim();
  return {
    head: git("rev-parse", "HEAD"),
    dirty: git("status", "--porcelain", "--untracked-files=no").length > 0,
  };
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === fileURLToPath(import.meta.url)) {
  await main();
}
