/** The browser's memory with no calque in the page, for the page's own peak RSS to be read against. */
import type { Run } from "./bench-context.ts";
import { hrtimeMs } from "./bench-host.ts";
import { log, writeRecord } from "./bench-record.ts";
import { closeSession, openSession, shapeNetwork } from "./bench-session.ts";
import { delay } from "./demo-process.ts";

/**
 * How long a blank page is sampled: as long as a cold sample's page takes to
 * reach a complete stable paint and hold it, rounded up.
 */
const EMPTY_PAGE_MS = 5_000;

/**
 * Peak browser process-tree RSS of `run.plan.emptyPages` sessions that load
 * `about:blank`: the same Chrome for Testing, flags and network profile as a
 * cold sample, sampled by the same `RssSampler`, with no page, client or
 * calque request.
 */
export async function emptyPagePeaks(run: Run): Promise<(number | null)[]> {
  const peaks: (number | null)[] = [];
  for (let n = 0; n < run.plan.emptyPages; n += 1) {
    const session = await openSession(run, false);
    try {
      await shapeNetwork(run, session);
      session.rss.start();
      const started = hrtimeMs();
      await session.page.goto("about:blank");
      await delay(Math.max(0, EMPTY_PAGE_MS - (hrtimeMs() - started)));
      await session.rss.stop();
      const rss = session.rss.summary();
      peaks.push(rss.browserPeakBytes);
      writeRecord(run, `empty-page-${String(n)}`, {
        kind: "empty-page",
        n,
        browser: {
          version: session.browser.version(),
          launches: session.browserLaunches,
        },
        memory: rss,
      });
      log(
        `empty page ${String(n)}: ${rss.browserPeakBytes === null ? "unsampled" : `${(rss.browserPeakBytes / 1_048_576).toFixed(0)} MiB`}`,
      );
    } finally {
      await closeSession(session);
    }
  }
  return peaks;
}
