/// <reference lib="dom" />
/**
 * What a sample's page did with the rows it keeps between visits, read from
 * its marks, its plan, its request ledger and its resource timing.
 */
import type { Page } from "playwright";
import { hrtimeMs } from "./bench-host.ts";
import { delay } from "./demo-process.ts";

/**
 * How long a sample waits, after its stable paint, for a page that is
 * storing its artifact to say whether the copy landed.
 */
export const STORE_OUTCOME_TIMEOUT_MS = 10_000;

export interface StoredRowsRecord {
  /**
   * Whether the plan rendered into the page carried `storedRowsKey`, or null
   * for a page without a rendered plan.
   */
  readonly planKey: boolean | null;
  readonly hit: boolean;
  readonly miss: boolean;
  readonly failed: boolean;
  readonly abandoned: boolean;
  /** `calque:store:written` from T_nav, or null without it. */
  readonly writtenMs: number | null;
  /** From the sample's stable paint to `calque:store:written`. */
  readonly writtenAfterPaintMs: number | null;
  /** How many requests the ledger holds for an artifact object. */
  readonly artifactRequests: number;
  /**
   * Whether Chrome answered every request for the page's scripts from its
   * HTTP cache, or null where the page timed none. A worker's script answers
   * on the worker's own target, outside the page's request ledger, so this
   * reads resource timing, where a body that crossed no wire
   * (`transferSize` 0, `encodedBodySize` above 0) came from the cache.
   */
  readonly scriptsFromCache: boolean | null;
}

export function storedRowsRecord(sample: {
  readonly marks: readonly {
    readonly name: string;
    readonly startTime: number;
  }[];
  readonly coldMs: number | null;
  readonly requests: readonly { readonly url: string }[];
  readonly resources: readonly {
    readonly name: string;
    readonly transferSize: number;
    readonly encodedBodySize: number;
  }[];
  readonly pageOrigin: string;
  readonly planKey: boolean | null;
}): StoredRowsRecord {
  const at = (name: string) =>
    sample.marks.find((mark) => mark.name === `calque:${name}`)?.startTime ??
    null;
  const writtenMs = at("store:written");
  const scripts = sample.resources.filter((resource) => {
    const url = new URL(resource.name);
    return (
      url.origin === sample.pageOrigin &&
      url.pathname.startsWith("/assets/") &&
      url.pathname.endsWith(".js")
    );
  });
  return {
    planKey: sample.planKey,
    hit: at("store:hit") !== null,
    miss: at("store:miss") !== null,
    failed: at("store:failed") !== null,
    abandoned: at("store:abandoned") !== null,
    writtenMs,
    writtenAfterPaintMs:
      writtenMs === null || sample.coldMs === null
        ? null
        : writtenMs - sample.coldMs,
    artifactRequests: sample.requests.filter((request) =>
      new URL(request.url).pathname.includes("/v1/artifact-objects/"),
    ).length,
    scriptsFromCache:
      scripts.length === 0
        ? null
        : scripts.every(
            (script) => script.transferSize === 0 && script.encodedBodySize > 0,
          ),
  };
}

/** The samples, by name, that each engagement reading holds for. */
export function storedRowsSummary(
  samples: readonly {
    readonly name: string;
    readonly storedRows: StoredRowsRecord;
  }[],
) {
  const where = (holds: (record: StoredRowsRecord) => boolean) =>
    samples
      .filter((sample) => holds(sample.storedRows))
      .map(({ name }) => name);
  return {
    samples: samples.map(({ name }) => name),
    planKey: where((record) => record.planKey === true),
    hit: where((record) => record.hit),
    written: where((record) => record.writtenMs !== null),
    artifactRequested: where((record) => record.artifactRequests > 0),
    scriptsFromCache: where((record) => record.scriptsFromCache === true),
  };
}

/** Whether the plan rendered into the page carries `storedRowsKey`. */
export function readPlanKey(page: Page): Promise<boolean | null> {
  return page.evaluate(() => {
    const text = document.getElementById("calque-plan")?.textContent;
    if (!text) return null;
    try {
      const { plan } = JSON.parse(text) as { plan: string };
      const { storedRowsKey } = JSON.parse(plan) as { storedRowsKey?: unknown };
      return typeof storedRowsKey === "string";
    } catch {
      return null;
    }
  });
}

/**
 * Waits until a page that missed its stored copy marks whether the copy it
 * stored landed, for at most `STORE_OUTCOME_TIMEOUT_MS`. A page that marked
 * no miss stores nothing, and is not waited for.
 */
export async function awaitStoreOutcome(page: Page): Promise<void> {
  const started = hrtimeMs();
  for (;;) {
    const settled = await page.evaluate(() => {
      const marked = (name: string) =>
        performance.getEntriesByName(`calque:${name}`, "mark").length > 0;
      return (
        !marked("store:miss") ||
        marked("store:written") ||
        marked("store:abandoned")
      );
    });
    if (settled || hrtimeMs() - started >= STORE_OUTCOME_TIMEOUT_MS) return;
    await delay(50);
  }
}
