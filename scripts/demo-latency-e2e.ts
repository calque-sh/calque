#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Times the demo from PostgreSQL commit to stable paint.
 *
 * For each of N source changes it records the commit time the change stream
 * carried, which the page reads from the public `client.subscribePublications`;
 * the page's own publication and render times; and the swap time of the first
 * compositor frame presented at or after that render, from CDP
 * `Page.screencastFrame` metadata. All three are epoch clocks on one machine.
 * The result is directional: the source is a container on the same host, the
 * change is one two-row transaction, and a screencast frame is presented but
 * is not a display scanout.
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { CDPSession, Page } from "playwright";

import {
  demoBrowserChannel,
  availablePort,
  type PagePublication,
  delay,
  launchDemoBrowser,
  parseSampleCount,
  runSourceChange,
  startDemo,
  stopDemo,
  summarizeSamples,
  type DemoProcess,
} from "./demo-process.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const measuredChanges = parseSampleCount(
  process.env.CALQUE_DEMO_LATENCY_CHANGES ?? "20",
  "CALQUE_DEMO_LATENCY_CHANGES",
);
/** One unmeasured change first, so JIT and cache warm-up are not a sample. */
const WARM_UP_CHANGES = 1;
const OPEN_TASKS_AT_SEED = 4;

interface PresentedFrame {
  readonly sessionId: number;
  readonly epochMs: number;
}

interface Sample {
  readonly change: number;
  readonly warmUp: boolean;
  readonly cursor: string;
  readonly changeStartedEpochMs: number;
  readonly changeFinishedEpochMs: number;
  readonly commitEpochMs: number;
  readonly publishedAtEpochMs: number;
  readonly renderedAtEpochMs: number;
  readonly presentedEpochMs: number;
  readonly frameSessionId: number;
  readonly commitToPublishMs: number;
  readonly publishToRenderMs: number;
  readonly renderToFrameMs: number;
  readonly commitToFrameMs: number;
  /** Commit minus the host's own change start; negative means clock skew. */
  readonly commitAfterChangeStartMs: number;
}

const pageErrors: string[] = [];
const consoleErrors: string[] = [];
const frames: PresentedFrame[] = [];
const ackFailures: string[] = [];
const framesWithoutTimestamp: number[] = [];
const output = join(root, "target/demo-latency");
mkdirSync(output, { recursive: true });

const [uiPort, serverPort] = await Promise.all([
  availablePort(),
  availablePort(),
]);
const browser = await launchDemoBrowser();
let demo: DemoProcess | undefined;
let page: Page | undefined;
let cdp: CDPSession | undefined;
try {
  page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  page.on("pageerror", (error) => pageErrors.push(String(error)));
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  cdp = await page.context().newCDPSession(page);
  await cdp.send("Page.enable");
  cdp.on("Page.screencastFrame", (frame) => {
    const timestamp = frame.metadata.timestamp;
    if (timestamp === undefined) framesWithoutTimestamp.push(frame.sessionId);
    else frames.push({ sessionId: frame.sessionId, epochMs: timestamp * 1000 });
    cdp
      ?.send("Page.screencastFrameAck", { sessionId: frame.sessionId })
      .catch((error: unknown) =>
        ackFailures.push(
          `${frame.sessionId}: ${error instanceof Error ? error.message : String(error)}`,
        ),
      );
  });

  demo = await startDemo(serverPort, uiPort);
  await page.goto(demo.url, { waitUntil: "load", timeout: 30_000 });
  await waitForDashboard(page);
  // Started after the dashboard is up so the mount and its first render are
  // not in the frame stream; only the changes under measurement are.
  await cdp.send("Page.startScreencast", {
    format: "jpeg",
    quality: 40,
    maxWidth: 1280,
    maxHeight: 900,
    everyNthFrame: 1,
  });
  await delay(250);

  const samples: Sample[] = [];
  const totalChanges = WARM_UP_CHANGES + measuredChanges;
  for (let change = 1; change <= totalChanges; change += 1) {
    const publicationsBefore: number = await page.evaluate(
      () => globalThis.__calqueDemoPublications.length,
    );
    const changeStartedEpochMs = Date.now();
    runSourceChange();
    const changeFinishedEpochMs = Date.now();
    await page.waitForFunction(
      (count) => globalThis.__calqueDemoPublications.length > count,
      publicationsBefore,
      { timeout: 30_000 },
    );
    const publication: PagePublication | undefined = await page.evaluate(
      (index) => globalThis.__calqueDemoPublications[index],
      publicationsBefore,
    );
    assert(publication, "the page recorded no publication for the change");
    assert.equal(
      publication.sequence,
      publicationsBefore + 1,
      "publication sequence must advance by exactly one per change",
    );
    assert(
      publication.commitTimeUnixMicros !== null,
      "the publication carried no source commit time",
    );
    // Each change toggles task 1001, so the open-task count alternates; the
    // rendered state is checked so a stale render cannot pass as a fast one.
    const expectedOpen = String(OPEN_TASKS_AT_SEED - (change % 2));
    assert.equal(
      await page.locator("#metric-open").innerText(),
      expectedOpen,
      `change ${change} rendered the wrong open-task count`,
    );
    const presented = await firstFrameAtOrAfter(
      publication.renderedAtEpochMs,
      10_000,
    );
    const commitEpochMs =
      Number(BigInt(publication.commitTimeUnixMicros)) / 1000;
    samples.push({
      change,
      warmUp: change <= WARM_UP_CHANGES,
      cursor: publication.cursor,
      changeStartedEpochMs,
      changeFinishedEpochMs,
      commitEpochMs,
      publishedAtEpochMs: publication.publishedAtEpochMs,
      renderedAtEpochMs: publication.renderedAtEpochMs,
      presentedEpochMs: presented.epochMs,
      frameSessionId: presented.sessionId,
      commitToPublishMs: publication.publishedAtEpochMs - commitEpochMs,
      publishToRenderMs:
        publication.renderedAtEpochMs - publication.publishedAtEpochMs,
      renderToFrameMs: presented.epochMs - publication.renderedAtEpochMs,
      commitToFrameMs: presented.epochMs - commitEpochMs,
      commitAfterChangeStartMs: commitEpochMs - changeStartedEpochMs,
    });
  }
  await cdp.send("Page.stopScreencast");

  assert.deepEqual(pageErrors, []);
  assert.deepEqual(consoleErrors, []);
  assert.deepEqual(ackFailures, []);
  assert.deepEqual(framesWithoutTimestamp, []);
  assert.equal(
    await page.locator("#status").getAttribute("data-state"),
    "ready",
  );
  const measured = samples.filter((sample) => !sample.warmUp);
  assert.equal(measured.length, measuredChanges);
  const report = {
    outcome: "passed",
    scenario: "commit-to-stable-paint-loopback",
    browser: demoBrowserChannel(),
    changes: measuredChanges,
    warmUpChanges: WARM_UP_CHANGES,
    framesObserved: frames.length,
    commitToFrameMs: summarizeSamples(measured.map((s) => s.commitToFrameMs)),
    commitToPublishMs: summarizeSamples(
      measured.map((s) => s.commitToPublishMs),
    ),
    publishToRenderMs: summarizeSamples(
      measured.map((s) => s.publishToRenderMs),
    ),
    renderToFrameMs: summarizeSamples(measured.map((s) => s.renderToFrameMs)),
    // Negative here would mean the container clock runs behind the host's
    // and the commit-to-* figures are correspondingly overstated.
    commitAfterChangeStartMs: summarizeSamples(
      measured.map((s) => s.commitAfterChangeStartMs),
    ),
    samples,
  };
  writeFileSync(
    join(output, "report.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  const { samples: _samples, ...headline } = report;
  process.stdout.write(`${JSON.stringify(headline)}\n`);
} catch (error) {
  process.stderr.write(
    `Demo latency E2E failed: ${JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
      url: page?.url(),
      status: await page
        ?.locator("#status")
        .getAttribute("data-state")
        .catch(() => null),
      notice: await page
        ?.locator("#notice")
        .innerText()
        .catch(() => null),
      pageErrors,
      consoleErrors,
      ackFailures,
      framesObserved: frames.length,
      publications: await page
        ?.evaluate(() => globalThis.__calqueDemoPublications)
        .catch(() => null),
      launcher: demo?.output(),
    })}\n`,
  );
  throw error;
} finally {
  if (demo) await stopDemo(demo);
  await browser.close();
}

async function firstFrameAtOrAfter(
  epochMs: number,
  timeoutMs: number,
): Promise<PresentedFrame> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const frame = frames.find((candidate) => candidate.epochMs >= epochMs);
    if (frame) return frame;
    await delay(5);
  }
  throw new Error(
    `no screencast frame was presented within ${timeoutMs} ms of the render`,
  );
}

async function waitForDashboard(currentPage: Page): Promise<void> {
  await currentPage.waitForFunction(
    () =>
      document.querySelector("#status")?.getAttribute("data-state") ===
        "ready" &&
      document.querySelector("#metric-projects")?.textContent === "3" &&
      document.querySelector("#metric-total")?.textContent ===
        "9 total tasks" &&
      document.querySelectorAll("#task-rows tr").length === 9 &&
      Array.isArray(globalThis.__calqueDemoPublications),
    undefined,
    { timeout: 60_000 },
  );
}
