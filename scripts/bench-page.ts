/// <reference lib="dom" />
/** What the runner reads from the incident-operations page: its probe, paint log, publications and change requests. */
import type { Page } from "playwright";
import type { Bucket, OraclePage } from "./bench-oracle.ts";

/** Run in every document before its own scripts, to record its long tasks. */
export const LONG_TASK_SCRIPT = `globalThis.__benchLongTasks = [];
try {
  new PerformanceObserver((list) => {
    for (const entry of list.getEntries())
      globalThis.__benchLongTasks.push({ startTime: entry.startTime, duration: entry.duration });
  }).observe({ type: "longtask", buffered: true });
} catch (error) {
  globalThis.__benchLongTasks = String(error);
}
`;

/** One publication in the page's log (`harness.ts`'s `PublicationRecord`). */
interface PagePublication {
  /** The newest source commit in the batch, as decimal microseconds. */
  readonly commitTimeUnixMicros: string | null;
  /** `performance.now()` when the page's main thread was told of it. */
  readonly at: number;
  readonly changedCollections: readonly string[];
}

/** What the runner reads from the page's `__incidents` probe and globals. */
export interface BenchWindow {
  readonly __incidents?: {
    readonly coldLoad: Record<string, number | null>;
    readonly status: string;
    readonly error: string | null;
    readonly grid: {
      readonly view: { readonly level: string; readonly sort: string };
      readonly rows: number;
      readonly top: number;
      readonly ids: readonly string[];
    } | null;
    readonly chart: readonly Bucket[] | null;
    readonly issue: unknown;
    readonly publications: number;
    /** The page's last 512 publications that moved a collection, oldest first. */
    readonly publicationLog?: readonly PagePublication[];
    readonly shown?: () => readonly unknown[];
    readonly db: Record<
      string,
      {
        slice(start: number, count: number): Promise<Record<string, unknown>[]>;
      }
    > | null;
  };
  readonly __benchLongTasks?:
    { readonly startTime: number; readonly duration: number }[] | string;
}

export interface PageFacts {
  readonly timeOrigin: number;
  readonly coldLoad: Record<string, number | null> | null;
  readonly status: string | null;
  readonly error: string | null;
  readonly gridRows: number | null;
  readonly chart: readonly Bucket[] | null;
  readonly issueShown: boolean;
  readonly publications: number | null;
  readonly longTasks:
    | readonly { readonly startTime: number; readonly duration: number }[]
    | string
    | null;
  readonly resources: readonly {
    readonly name: string;
    readonly initiatorType: string;
    readonly startTime: number;
    readonly responseEnd: number;
    readonly transferSize: number;
    readonly encodedBodySize: number;
    readonly decodedBodySize: number;
  }[];
  /**
   * Every user-timing mark, in the order the page left them, with its time
   * since `timeOrigin`.
   */
  readonly marks: readonly PageMark[];
  readonly gridRect: {
    readonly left: number;
    readonly top: number;
    readonly right: number;
    readonly bottom: number;
  } | null;
}

interface PageMark {
  readonly name: string;
  readonly startTime: number;
}

/**
 * The marks a load leaves when it asked for its artifact again:
 * `calque:artifact:retry:<n>` before the n-th retry, and
 * `calque:artifact:replan` when it went back to the bootstrap for a fresh
 * plan. A sample with any is labelled, not dropped: its phases mix two
 * requests.
 */
export function artifactRetryMarks(marks: readonly PageMark[]): string[] {
  return marks
    .map(({ name }) => name)
    .filter(
      (name) =>
        name.startsWith("calque:artifact:retry:") ||
        name === "calque:artifact:replan",
    );
}

/**
 * The page's paint log: when each value it showed reached the DOM, and the
 * frame after the one that painted it (`paintedAt`, two animation frames
 * later), or null when the page keeps no log.
 */
export function readShown(page: Page): Promise<readonly unknown[] | null> {
  return page.evaluate(
    () => (globalThis as unknown as BenchWindow).__incidents?.shown?.() ?? null,
  );
}

export function readPageFacts(page: Page): Promise<PageFacts> {
  return page.evaluate(() => {
    const view = globalThis as unknown as BenchWindow;
    const probe = view.__incidents;
    const rect = document
      .querySelector("section.grid-panel")
      ?.getBoundingClientRect();
    return {
      timeOrigin: performance.timeOrigin,
      coldLoad: probe ? { ...probe.coldLoad } : null,
      status: probe?.status ?? null,
      error: probe?.error ?? null,
      gridRows: probe?.grid?.rows ?? null,
      chart: probe?.chart ?? null,
      issueShown: Boolean(probe?.issue),
      publications: probe?.publications ?? null,
      longTasks: view.__benchLongTasks ?? null,
      resources: performance.getEntriesByType("resource").map((entry) => {
        const resource = entry as PerformanceResourceTiming;
        return {
          name: resource.name,
          initiatorType: resource.initiatorType,
          startTime: resource.startTime,
          responseEnd: resource.responseEnd,
          transferSize: resource.transferSize,
          encodedBodySize: resource.encodedBodySize,
          decodedBodySize: resource.decodedBodySize,
        };
      }),
      marks: performance
        .getEntriesByType("mark")
        .map(({ name, startTime }) => ({ name, startTime })),
      gridRect: rect
        ? {
            left: rect.left,
            top: rect.top,
            right: rect.right,
            bottom: rect.bottom,
          }
        : null,
    };
  });
}

export function readPageState(page: Page): Promise<{
  readonly status: string | null;
  readonly error: string | null;
} | null> {
  return page.evaluate(() => {
    const probe = (globalThis as unknown as BenchWindow).__incidents;
    return probe ? { status: probe.status, error: probe.error } : null;
  });
}

/** The error a page reported: the client's own, or an uncaught exception. */
export function reportedError(
  state: {
    readonly status: string | null;
    readonly error: string | null;
  } | null,
  pageErrors: readonly string[],
): string | null {
  if (state?.error) return `the client reported: ${state.error}`;
  if (state?.status === "error") return "the client reported an error";
  const thrown = pageErrors[0];
  return thrown === undefined ? null : `the page threw: ${thrown}`;
}

export function longTaskSummary(facts: PageFacts) {
  if (!Array.isArray(facts.longTasks))
    return { observed: false, count: 0, totalMs: 0, longestMs: null };
  const durations = facts.longTasks.map((task) => task.duration);
  return {
    observed: true,
    count: durations.length,
    totalMs: durations.reduce((sum, duration) => sum + duration, 0),
    longestMs: durations.length === 0 ? 0 : Math.max(...durations),
  };
}

export function resourceTotals(facts: PageFacts) {
  return {
    entries: facts.resources.length,
    transferSize: facts.resources.reduce(
      (sum, entry) => sum + entry.transferSize,
      0,
    ),
    encodedBodySize: facts.resources.reduce(
      (sum, entry) => sum + entry.encodedBodySize,
      0,
    ),
    decodedBodySize: facts.resources.reduce(
      (sum, entry) => sum + entry.decodedBodySize,
      0,
    ),
  };
}

/** The page's working set, read through the facade's `slice`. */
export function oraclePage(page: Page): OraclePage {
  return {
    publications: () => readPublications(page),
    slice: (collection, columns, start, count) =>
      page.evaluate(
        async ({ collection, columns, start: from, count }) => {
          const db = (globalThis as unknown as BenchWindow).__incidents?.db;
          const facade = db?.[collection];
          if (!facade) throw new Error(`the page exposes no db.${collection}`);
          const cell = (value: unknown): unknown =>
            typeof value === "bigint" ? value.toString() : value;
          const slice = await facade.slice(from, count);
          return slice.map((row) => columns.map((name) => cell(row[name])));
        },
        { collection, columns: [...columns], start, count },
      ),
  };
}

export function readPublications(page: Page): Promise<number | null> {
  return page.evaluate(
    () =>
      (globalThis as unknown as BenchWindow).__incidents?.publications ?? null,
  );
}

export function readGridRows(page: Page): Promise<number | null> {
  return page.evaluate(
    () =>
      (globalThis as unknown as BenchWindow).__incidents?.grid?.rows ?? null,
  );
}

/** What the events grid shows: its controls' values, its total and the ids in view. */
export interface GridReading {
  readonly view: { readonly level: string; readonly sort: string };
  readonly rows: number;
  readonly top: number;
  readonly ids: readonly string[];
}

/** The grid as the page's probe reads it, or null while it is busy or unmounted. */
export function readGridView(page: Page): Promise<GridReading | null> {
  return page.evaluate(() => {
    const grid = (globalThis as unknown as BenchWindow).__incidents?.grid;
    return grid
      ? {
          view: { ...grid.view },
          rows: grid.rows,
          top: grid.top,
          ids: [...grid.ids],
        }
      : null;
  });
}

/**
 * Resource entries enough for every request a realtime session starts.
 * Chrome's default buffer holds 250 and drops what follows, which a
 * long-poll fills within seconds.
 */
const ENGAGEMENT_RESOURCE_BUFFER = 100_000;

/**
 * The engagement reading: the change requests the page started from the
 * first probe on, against the publications it counted over the same span.
 * An open response that has not ended has no resource entry yet, so the
 * request in flight at the reading is not counted.
 */
export interface Engagement extends ChangeTraffic {
  readonly publications: number | null;
  /** Every resource entry the page held at the reading; below the size, none was dropped. */
  readonly resourceEntries: number;
  readonly resourceBufferSize: number;
  readonly spanMs: number;
}

/**
 * What the page's `/v1/changes` resource entries say about the change
 * requests that started within the span. Between two requests the client
 * applies the previous response and, after a 5xx or a 429, waits out a
 * retry delay that starts at 100 ms and doubles; a refused status beside a
 * long gap is that delay.
 */
interface ChangeTraffic {
  readonly changeRequests: number;
  /** The entries by HTTP status; 0 is a status the page could not read. */
  readonly changeStatuses: Readonly<Record<string, number>>;
  /** The longest request, from its start to its response's end; null for none. */
  readonly longestChangeRequestMs: number | null;
  /**
   * The longest time from one response's end to the next request's start;
   * null for fewer than two requests.
   */
  readonly longestChangeGapMs: number | null;
}

/** The fields of one resource entry that `changeTraffic` reads. */
interface ChangeEntry {
  readonly startTime: number;
  readonly responseEnd: number;
  readonly responseStatus: number;
}

export function changeTraffic(entries: readonly ChangeEntry[]): ChangeTraffic {
  const ordered = [...entries].sort((a, b) => a.startTime - b.startTime);
  const changeStatuses: Record<string, number> = {};
  let longestChangeRequestMs: number | null = null;
  let longestChangeGapMs: number | null = null;
  let previous: ChangeEntry | undefined;
  for (const entry of ordered) {
    const status = String(entry.responseStatus);
    changeStatuses[status] = (changeStatuses[status] ?? 0) + 1;
    const duration = entry.responseEnd - entry.startTime;
    longestChangeRequestMs = Math.max(
      longestChangeRequestMs ?? duration,
      duration,
    );
    if (previous !== undefined) {
      const gap = entry.startTime - previous.responseEnd;
      longestChangeGapMs = Math.max(longestChangeGapMs ?? gap, gap);
    }
    previous = entry;
  }
  return {
    changeRequests: ordered.length,
    changeStatuses,
    longestChangeRequestMs,
    longestChangeGapMs,
  };
}

export interface EngagementStart {
  readonly atMs: number;
  readonly publications: number | null;
}

export function startEngagement(page: Page): Promise<EngagementStart> {
  return page.evaluate((size) => {
    performance.setResourceTimingBufferSize(size);
    return {
      atMs: performance.now(),
      publications:
        (globalThis as unknown as BenchWindow).__incidents?.publications ??
        null,
    };
  }, ENGAGEMENT_RESOURCE_BUFFER);
}

export async function readEngagement(
  page: Page,
  start: EngagementStart,
): Promise<Engagement> {
  const { changes, ...reading } = await page.evaluate(
    ({ atMs, publications, size }) => {
      const entries = performance.getEntriesByType(
        "resource",
      ) as PerformanceResourceTiming[];
      const counted =
        (globalThis as unknown as BenchWindow).__incidents?.publications ??
        null;
      return {
        changes: entries
          .filter(
            (entry) =>
              entry.startTime >= atMs &&
              new URL(entry.name).pathname.endsWith("/v1/changes"),
          )
          .map(({ startTime, responseEnd, responseStatus }) => ({
            startTime,
            responseEnd,
            responseStatus,
          })),
        publications:
          counted === null || publications === null
            ? null
            : counted - publications,
        resourceEntries: entries.length,
        resourceBufferSize: size,
        spanMs: performance.now() - atMs,
      };
    },
    { ...start, size: ENGAGEMENT_RESOURCE_BUFFER },
  );
  return { ...changeTraffic(changes), ...reading };
}

export function engagementLine(engagement: Engagement | null): string {
  if (engagement === null) return "engagement: not read";
  const statuses = Object.entries(engagement.changeStatuses)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([status, count]) => `${status} x${String(count)}`)
    .join(", ");
  const longest = (ms: number | null) =>
    ms === null ? "-" : `${ms.toFixed(1)} ms`;
  return `engagement: ${String(engagement.changeRequests)} change requests (${statuses || "none"}) and ${engagement.publications === null ? "?" : String(engagement.publications)} publications over ${(engagement.spanMs / 1_000).toFixed(1)} s; longest request ${longest(engagement.longestChangeRequestMs)}, longest gap between requests ${longest(engagement.longestChangeGapMs)}; ${String(engagement.resourceEntries)} of ${String(engagement.resourceBufferSize)} resource entries held`;
}
