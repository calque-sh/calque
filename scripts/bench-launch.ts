/** The pinned browser's process and window, and the bounded readings every session starts with. */
import { existsSync, readFileSync } from "node:fs";
import { type Browser, type CDPSession, chromium } from "playwright";
import {
  BROWSER_EXIT_LOOKBACK_MS,
  browserExitCause,
  type BrowserExitCause,
  CHROME_FOR_TESTING,
  type InstrumentFault,
  instrumentFault,
  isBrowserLoss,
  type JetsamKills,
  parseJetsamKills,
  requireBrowserVersion,
  requireChromeForTesting,
  unifiedLogTime,
} from "./bench-browser.ts";
import { decodePng, hashFrame, type Roi, VIEWPORT } from "./bench-frames.ts";
import { output } from "./bench-host.ts";
import { log } from "./bench-record.ts";
import { parseMemoryFreePercent } from "./bench-results.ts";
import { delay } from "./demo-process.ts";

const closingBrowsers = new WeakSet<Browser>();

/**
 * A new browser process, refused unless it reports the pinned version.
 *
 * Playwright's own SIGINT handler closes its browsers and exits 130, at once
 * on a second SIGINT. Once `listenForStop` has attached, that exit would end
 * node partway through the run's stop, so `stopListens` leaves SIGINT to the
 * stop; Playwright still kills the browser on node's 'exit'.
 */
export async function launchBrowser(
  target: Pick<LaunchTarget, "executablePath" | "args">,
  stopListens: boolean,
): Promise<Browser> {
  const browser = await chromium.launch({
    executablePath: target.executablePath,
    args: [...(target.args ?? [])],
    headless: true,
    handleSIGINT: !stopListens,
  });
  browser.on("disconnected", () => {
    if (!closingBrowsers.has(browser)) browserLost("browser-disconnected");
  });
  try {
    // Chromium 149 ignores the screencast's last-frame parameters without an
    // error, so the version is what says they took effect.
    requireBrowserVersion(browser.version());
  } catch (error) {
    await closeBrowser(browser);
    throw error;
  }
  return browser;
}

export async function closeBrowser(browser: Browser): Promise<void> {
  closingBrowsers.add(browser);
  await browser.close().catch(() => undefined);
}

interface BrowserLoss {
  readonly kind: "browser-disconnected" | "page-crash";
  readonly at: string;
}

/** The browsers and pages this process lost without closing them. */
const browserLosses: BrowserLoss[] = [];

export function browserLost(kind: BrowserLoss["kind"]): void {
  browserLosses.push({ kind, at: new Date().toISOString() });
}

/** What the host and Playwright said around a lost browser or page. */
export interface BrowserExit {
  readonly at: string;
  readonly losses: readonly BrowserLoss[];
  readonly freeMemoryPercent: number | null;
  /** Null off macOS or when the unified log could not be read. */
  readonly jetsamKills: JetsamKills | null;
  /**
   * The last lines Playwright's browser logger wrote to `DEBUG_FILE`, with
   * every process exit it saw, when the run was started with
   * `DEBUG=pw:browser`.
   */
  readonly playwrightBrowserLog: readonly string[] | null;
  readonly cause: BrowserExitCause;
}

/**
 * Reads, at once, why the run lost its browser: jetsam's exits in the host's
 * log since shortly before the first loss, the host's free memory, and
 * Playwright's own record of the browser processes. Null when the failure
 * was not a lost browser or page.
 */
export function readBrowserExit(error: unknown): BrowserExit | null {
  if (!isBrowserLoss(error) && browserLosses.length === 0) return null;
  const now = Date.now();
  const firstLoss = browserLosses[0];
  const since =
    (firstLoss === undefined ? now : Date.parse(firstLoss.at)) -
    BROWSER_EXIT_LOOKBACK_MS;
  const log =
    process.platform === "darwin"
      ? output("/usr/bin/log", [
          "show",
          "--start",
          unifiedLogTime(new Date(since)),
          "--style",
          "compact",
          "--predicate",
          'process == "launchd" AND eventMessage CONTAINS "OS_REASON_JETSAM"',
        ])
      : null;
  const jetsamKills = log === null ? null : parseJetsamKills(log);
  const freeMemoryPercent = parseMemoryFreePercent(
    output("memory_pressure", []) ?? "",
  );
  return {
    at: new Date(now).toISOString(),
    losses: [...browserLosses],
    freeMemoryPercent,
    jetsamKills,
    playwrightBrowserLog: playwrightBrowserLog(process.env.DEBUG_FILE),
    cause: browserExitCause(jetsamKills, freeMemoryPercent),
  };
}

const PLAYWRIGHT_LOG_TAIL = 20;

function playwrightBrowserLog(path: string | undefined): string[] | null {
  if (path === undefined || !existsSync(path)) return null;
  const lines = readFileSync(path, "utf8").split("\n").filter(Boolean);
  const exits = lines.filter((line) => line.includes("<process did exit"));
  const tail = lines.slice(-PLAYWRIGHT_LOG_TAIL);
  return [...exits.filter((line) => !tail.includes(line)), ...tail];
}

/** How many browsers a session launches before a refused screenshot ends the run. */
const SESSION_LAUNCHES = 2;

/**
 * Runs `launch` again, up to `launches` times in all, while it fails because
 * Chrome refused every screenshot it asked for. A session takes its
 * screenshots before navigating, so no timed interval includes a relaunch.
 * Any other failure, and the last refusal, is thrown as it came.
 */
export async function relaunchOnRefusal<T>(
  launch: () => Promise<T>,
  launches = SESSION_LAUNCHES,
): Promise<{ readonly value: T; readonly launches: number }> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return { value: await launch(), launches: attempt };
    } catch (error) {
      if (
        attempt >= launches ||
        instrumentFault(error)?.kind !== "screenshot-refused"
      )
        throw error;
      log(
        `browser ${String(attempt)} of ${String(launches)} refused every screenshot before navigating, launching another`,
      );
    }
  }
}

export interface LaunchTarget {
  readonly executablePath: string;
  readonly archiveSha256: string | null;
  readonly bundleSha256: string;
  /** The rows each of the browser's windows grows by: see `windowInset`. */
  readonly windowInsetPx: number;
  /** Chrome's extra arguments: the edge's certificate trust, when it has one. */
  readonly args?: readonly string[];
}

/**
 * The pinned browser's launch target. Its bundle digest is recomputed here,
 * on every run, and a mismatch refuses the run before it starts. The
 * browser's window inset is measured here too.
 */
export async function launchTarget(): Promise<LaunchTarget> {
  const pinned = requireChromeForTesting();
  return {
    executablePath: pinned.executablePath,
    archiveSha256: CHROME_FOR_TESTING.archiveSha256,
    bundleSha256: pinned.bundleSha256,
    windowInsetPx: await windowInset(pinned.executablePath),
  };
}

/** A page with nothing on it, for measuring the window inset. */
const BLANK_PAGE =
  '<!doctype html><html style="height:100%"><body style="margin:0;height:100%;background:rgb(0,0,0)"></body></html>';

/**
 * The rows the headless window keeps out of a page's capture, measured in a
 * browser of its own before the run. Playwright sizes a headless window to
 * the viewport, and Chrome for Testing 154 then captures 1600x813 of a page
 * that still lays out at 1600x900. Each session's window then grows by the
 * same rows in `fitWindow`, which checks it.
 */
export async function windowInset(executablePath: string): Promise<number> {
  const browser = await launchBrowser({ executablePath }, false);
  try {
    const context = await browser.newContext({
      viewport: { width: VIEWPORT.width, height: VIEWPORT.height },
      deviceScaleFactor: 1,
    });
    const page = await context.newPage();
    await page.setContent(BLANK_PAGE);
    return await measureWindowInset(await context.newCDPSession(page));
  } finally {
    await closeBrowser(browser);
  }
}

/** The rows missing from a screenshot of a painted page, checked by `fitWindow`. */
export async function measureWindowInset(
  cdp: Pick<CDPSession, "send">,
): Promise<number> {
  const before = decodePng(Buffer.from(await pageScreenshot(cdp), "base64"));
  const insetPx = VIEWPORT.height - before.height;
  if (before.width !== VIEWPORT.width || insetPx < 0)
    throw new Error(
      `the page captures at ${String(before.width)}x${String(before.height)}, which no window inset explains for the ${String(VIEWPORT.width)}x${String(VIEWPORT.height)} viewport`,
    );
  await fitWindow(cdp, insetPx);
  return insetPx;
}

/**
 * Grows the page's window by `insetPx` rows and refuses unless a screenshot
 * then has the viewport's size. Chrome applies the new bounds after
 * `Browser.setWindowBounds` returns, and the screenshot lets them land before
 * a screencast starts.
 */
export async function fitWindow(
  cdp: Pick<CDPSession, "send">,
  insetPx: number,
): Promise<void> {
  if (insetPx !== 0) {
    const { windowId, bounds } = await cdp.send("Browser.getWindowForTarget");
    if (bounds.height === undefined)
      throw new Error("Chrome disclosed no height for the page's window");
    await cdp.send("Browser.setWindowBounds", {
      windowId,
      bounds: { height: bounds.height + insetPx },
    });
  }
  const after = decodePng(Buffer.from(await pageScreenshot(cdp), "base64"));
  if (after.width !== VIEWPORT.width || after.height !== VIEWPORT.height)
    throw new Error(
      `the page captures at ${String(after.width)}x${String(after.height)} after its window grew by ${String(insetPx)} rows`,
    );
}

/** How many screenshots `pageScreenshot` asks for before it gives up. */
const SCREENSHOT_ATTEMPTS = 3;

/**
 * How long `pageScreenshot` waits after a refusal before it asks again, to
 * give the page a frame to capture.
 */
const SCREENSHOT_RETRY_DELAY_MS = 100;

/**
 * A screenshot of the page as base64 PNG, asked for again when Chrome answers
 * "Unable to capture screenshot". The error after the last refusal carries a
 * `screenshot-refused` instrument fault; any other failure, a missed deadline
 * included, is thrown as it came.
 */
export async function pageScreenshot(
  cdp: Pick<CDPSession, "send">,
): Promise<string> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return (
        await withDeadline(
          cdp.send("Page.captureScreenshot", { format: "png" }),
          "screenshot",
        )
      ).data;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes("Unable to capture screenshot")) throw error;
      if (attempt === SCREENSHOT_ATTEMPTS) {
        const fault: InstrumentFault = {
          kind: "screenshot-refused",
          attempts: SCREENSHOT_ATTEMPTS,
        };
        throw Object.assign(
          new Error(
            `Chrome refused ${String(SCREENSHOT_ATTEMPTS)} screenshots of the page in a row: ${message}`,
          ),
          { fault },
        );
      }
      log(
        `screenshot ${String(attempt)} of ${String(SCREENSHOT_ATTEMPTS)} refused, asking again: ${message}`,
      );
      await delay(SCREENSHOT_RETRY_DELAY_MS);
    }
  }
}

/**
 * How long each reading `withDeadline` guards may take: every screenshot,
 * including those that fit the window and calibrate the reference pass, and
 * every reading at a missed deadline.
 */
const DEADLINE_READ_TIMEOUT_MS = 10_000;

/**
 * The reading, or an error naming it once `milliseconds` pass first. The timer
 * is cleared on every path, so it cannot hold node open once a run finishes.
 */
export async function withDeadline<T>(
  promise: Promise<T>,
  what: string,
  milliseconds = DEADLINE_READ_TIMEOUT_MS,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, rejectDeadline) => {
        timer = setTimeout(() => {
          rejectDeadline(
            new Error(`no ${what} within ${String(milliseconds)} ms`),
          );
        }, milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export interface ScreenshotCalibration {
  readonly matchesFull: boolean | null;
  readonly matchesGrid: boolean | null;
  readonly error: string | null;
}

/**
 * Whether `Page.captureScreenshot` hashes as the screencast's last frame did
 * for the same settled page. Without that, a deadline screenshot that matches
 * neither the reference nor the last frame says nothing about the page.
 */
export async function screenshotCalibration(
  cdp: Pick<CDPSession, "send">,
  grid: Roi,
  expected: { readonly full: string; readonly grid: string | null },
): Promise<ScreenshotCalibration> {
  try {
    const { data } = await withDeadline(
      cdp.send("Page.captureScreenshot", { format: "png" }),
      "screenshot",
    );
    const hashes = hashFrame(Buffer.from(data, "base64"), grid, "screenshot");
    return {
      matchesFull: hashes.full === expected.full,
      matchesGrid:
        expected.grid === null ? null : hashes.grid === expected.grid,
      error: null,
    };
  } catch (error) {
    return {
      matchesFull: null,
      matchesGrid: null,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
