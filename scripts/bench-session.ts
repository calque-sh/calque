/// <reference lib="dom" />
/** One browser session: its launch, its instruments, and the navigation and stable-paint wait every sample shares. */
import type { Browser, BrowserContext, CDPSession, Page } from "playwright";
import { DecoderRequests } from "./bench-brotli.ts";
import { CHROME_FOR_TESTING, SCREENCAST_PARAMETERS } from "./bench-browser.ts";
import { Capture, NetworkLedger } from "./bench-capture.ts";
import {
  emulatedNetwork,
  judgeShaping,
  type KernelShapingWitness,
  type ShapingWitness,
} from "./bench-conditions.ts";
import { type Run, servedBrotli } from "./bench-context.ts";
import {
  frameClockCheck,
  type FrameClockCheck,
  type HashedFrame,
  judgeStablePaint,
  mapEpochSecondsToMonotonicMilliseconds,
  STABLE_PAINT_RULE,
  VIEWPORT,
} from "./bench-frames.ts";
import {
  HOST_SLEEP_TOLERANCE_MS,
  hrtimeMs,
  rereadHostSleep,
} from "./bench-host.ts";
import {
  browserLost,
  closeBrowser,
  fitWindow,
  launchBrowser,
  relaunchOnRefusal,
} from "./bench-launch.ts";
import {
  LONG_TASK_SCRIPT,
  type PageFacts,
  readPageState,
  reportedError,
} from "./bench-page.ts";
import { RssSampler } from "./bench-sampling.ts";
import { delay } from "./demo-process.ts";

export const COLD_TIMEOUT_MS = 120_000;

/** How long a cold sample waits for a match before it reads the page. */
const ERROR_POLL_AFTER_MS = 20_000;

export const HOLD_MARGIN_MS = 250;

/** The failure a cold sample records when its deadline passes. */
export const NO_PRESENT_FAILURE = `no T_present within ${String(COLD_TIMEOUT_MS)} ms of T_nav`;

export interface Session {
  readonly browser: Browser;
  readonly context: BrowserContext;
  readonly page: Page;
  readonly cdp: CDPSession;
  /**
   * The CDP session that emulates `--network`, or null when Chrome emulates
   * nothing (`emulatedNetwork`).
   */
  readonly shaping: CDPSession | null;
  readonly capture: Capture;
  readonly network: NetworkLedger;
  readonly rss: RssSampler;
  readonly pageErrors: string[];
  /**
   * The requests for the client's decoder files where the run's pages load a
   * brotli object (`servedBrotli`), or null in any other run, which listens
   * for none.
   */
  readonly decoderRequests: DecoderRequests | null;
  /** How many browsers `openSession` launched for it, this one included. */
  readonly browserLaunches: number;
}

export async function openSession(
  run: Run,
  keepLastPng: boolean,
): Promise<Session> {
  const { value, launches } = await relaunchOnRefusal(() =>
    launchSession(run, keepLastPng),
  );
  run.launches.push(launches);
  return { ...value, browserLaunches: launches };
}

async function launchSession(
  run: Run,
  keepLastPng: boolean,
): Promise<Omit<Session, "browserLaunches">> {
  if (!run.connection || !run.server)
    throw new Error("the run has no running calque dev");
  const target = run.browser;
  const browser = await launchBrowser(target, true);
  try {
    const context = await browser.newContext({
      viewport: { width: VIEWPORT.width, height: VIEWPORT.height },
      deviceScaleFactor: 1,
    });
    await context.addInitScript({ content: LONG_TASK_SCRIPT });
    const page = await context.newPage();
    const pageErrors: string[] = [];
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("crash", () => browserLost("page-crash"));
    const served = servedBrotli(run);
    const decoderRequests =
      served === null
        ? null
        : new DecoderRequests(page, [served.moduleUrl, served.workerUrl]);
    const cdp = await context.newCDPSession(page);
    const shaping =
      emulatedNetwork(run.options.network) === null
        ? null
        : await context.newCDPSession(page);
    await fitWindow(cdp, target.windowInsetPx);
    const browserCdp = await browser.newBrowserCDPSession();
    const info = await browserCdp.send("SystemInfo.getProcessInfo");
    const browserProcess = info.processInfo.find(
      (entry) => entry.type === "browser",
    );
    if (!browserProcess) throw new Error("Chrome disclosed no browser process");
    if (run.host.browser === null) {
      const system = await browserCdp.send("SystemInfo.getInfo");
      run.host.browser = {
        version: browser.version(),
        expectedVersion: CHROME_FOR_TESTING.version,
        platform: CHROME_FOR_TESTING.platform,
        archiveSha256: target.archiveSha256,
        bundleSha256: target.bundleSha256,
        screencast: SCREENCAST_PARAMETERS,
        windowInsetPx: target.windowInsetPx,
        gpuFeatureStatus: system.gpu.featureStatus,
      };
    }
    await browserCdp.detach();
    const network = new NetworkLedger(cdp, run.pageUrl);
    return {
      browser,
      context,
      page,
      cdp,
      shaping,
      capture: new Capture(
        cdp,
        run.hasher,
        run.reference?.gridRoi ?? null,
        keepLastPng,
        () => network.frameOffsetSeconds(),
      ),
      network,
      rss: new RssSampler(browserProcess.id, run.server.pid),
      pageErrors,
      decoderRequests,
    };
  } catch (error) {
    await closeBrowser(browser);
    throw error;
  }
}

export async function closeSession(session: Session): Promise<void> {
  await session.rss.stop();
  await closeBrowser(session.browser);
}

/**
 * Chrome's document-request timestamp between the host's readings before
 * `goto` and at the event's receipt, since `hrtimeMs()` reads Chrome's
 * clock; every frame's wall-clock stamp, mapped through that request's
 * offset, at or before its arrival on the host; and no host sleep from the
 * read in `navigate` to the check, since one moves the clocks apart.
 */
export interface ClockCheck {
  readonly documentTimestampMs: number | null;
  readonly documentReceivedMs: number | null;
  readonly monotonicMinusEpochSeconds: number | null;
  readonly frames: FrameClockCheck;
  readonly hostSleptMs: number;
  readonly passed: boolean;
}

/**
 * `--network` under the page, on the session's own CDP session.
 * `Network.disable` on another of the page's CDP sessions may clear the
 * conditions, so the realtime session sends them again after it disables the
 * ledger's, and `shapingWitness` checks that they held.
 */
export async function shapeNetwork(run: Run, session: Session): Promise<void> {
  const network = emulatedNetwork(run.options.network);
  if (!network || !session.shaping) return;
  await session.shaping.send("Network.enable");
  const bytesPerSecond = (network.megabitsPerSecond * 1_000_000) / 8;
  await session.shaping.send("Network.emulateNetworkConditions", {
    offline: false,
    latency: network.latencyMs,
    downloadThroughput: bytesPerSecond,
    uploadThroughput: bytesPerSecond,
  });
}

/** How many uncached fetches `shapingWitness` times. */
const SHAPING_WITNESS_FETCHES = 5;

/**
 * Times uncached fetches of the page's document from the page. Taken after
 * the probes, with nothing clearing the conditions after `shapeNetwork`, it
 * witnesses that the probes ran shaped.
 */
export async function shapingWitness(
  run: Run,
  session: Session,
): Promise<ShapingWitness | KernelShapingWitness> {
  const fetchMs = await session.page.evaluate(
    async ({ url, count }) => {
      const times: number[] = [];
      for (let fetched = 0; fetched < count; fetched += 1) {
        const started = performance.now();
        const response = await fetch(url, { cache: "no-store" });
        await response.arrayBuffer();
        times.push(performance.now() - started);
      }
      return times;
    },
    { url: run.pageUrl, count: SHAPING_WITNESS_FETCHES },
  );
  return judgeShaping(run.options.network, fetchMs);
}

/** Start capture, memory sampling and the network ledger, then navigate. */
export async function navigate(run: Run, session: Session): Promise<number> {
  await shapeNetwork(run, session);
  await session.cdp.send("Network.enable");
  await session.capture.start();
  session.rss.start();
  // A sleep since the last session is harmless once read; one from here on
  // fails the clock check.
  rereadHostSleep();
  const beforeGotoMs = hrtimeMs();
  await session.page.goto(run.pageUrl, {
    waitUntil: "commit",
    timeout: COLD_TIMEOUT_MS,
  });
  return beforeGotoMs;
}

export function clockCheck(
  session: Session,
  beforeGotoMs: number,
  frames: readonly HashedFrame[],
): ClockCheck {
  const {
    documentTimestampMs,
    documentReceivedMs,
    monotonicMinusEpochSeconds,
  } = session.network;
  const frameCheck = frameClockCheck(frames);
  const hostSleptMs = rereadHostSleep();
  return {
    documentTimestampMs,
    documentReceivedMs,
    monotonicMinusEpochSeconds,
    frames: frameCheck,
    hostSleptMs,
    passed:
      documentTimestampMs !== null &&
      documentReceivedMs !== null &&
      beforeGotoMs <= documentTimestampMs &&
      documentTimestampMs <= documentReceivedMs &&
      frameCheck.passed &&
      hostSleptMs < HOST_SLEEP_TOLERANCE_MS,
  };
}

/**
 * How far Chrome's wall clock has moved against its monotonic clock since
 * the page's time origin. Frame stamps are wall-clock and map through the
 * offset read at the document request, so a probe late in the session moves
 * that offset by how far this has moved since load, and trusts the mapping
 * only while this holds still during the probe; differences, not readings,
 * are used, since the page's time origin may carry its own rounding.
 */
export function readWallDriftMs(page: Page): Promise<number> {
  return page.evaluate(
    () => Date.now() - (performance.timeOrigin + performance.now()),
  );
}

/**
 * Wait for the first frame at or after the document request whose full
 * viewport hashes to `expected`, then W + 250 ms of wall time. Returns the
 * failure that ended the wait instead, if one did.
 */
export async function awaitStablePaint(
  session: Session,
  expected: string,
): Promise<string | null> {
  const started = hrtimeMs();
  let matchSeenAt: number | null = null;
  let lastErrorPoll = 0;
  for (;;) {
    await delay(20);
    const now = hrtimeMs();
    const from = session.network.documentTimestampMs;
    if (from === null) {
      const error = reportedError(null, session.pageErrors);
      if (error) return error;
      if (now - started > COLD_TIMEOUT_MS)
        return `Chrome reported no document request within ${String(COLD_TIMEOUT_MS)} ms`;
      continue;
    }
    const judgement = judgeStablePaint(
      session.capture.prefix(),
      "full",
      expected,
      { fromMs: from },
    );
    if (judgement.matchMs !== null) {
      matchSeenAt ??= now;
      if (
        now - matchSeenAt >=
          STABLE_PAINT_RULE.holdWindowMilliseconds + HOLD_MARGIN_MS &&
        session.capture.complete
      )
        return null;
      continue;
    }
    if (now - from > COLD_TIMEOUT_MS) return NO_PRESENT_FAILURE;
    if (now - from > ERROR_POLL_AFTER_MS && now - lastErrorPoll > 1_000) {
      lastErrorPoll = now;
      const error = reportedError(
        await readPageState(session.page),
        session.pageErrors,
      );
      if (error) return error;
    }
  }
}

export function tNavMs(session: Session, facts: PageFacts): number | null {
  const offset = session.network.monotonicMinusEpochSeconds;
  return offset === null
    ? null
    : mapEpochSecondsToMonotonicMilliseconds(facts.timeOrigin / 1_000, offset);
}
