#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * `pnpm record:incidents`: regenerates `docs/assets/calque-incidents-100k.gif`,
 * the README's recording of the incident-operations page at 100k rows.
 *
 * It runs `scripts/incidents.ts --no-open --fresh --rows 100k --dataset
 * content-v1` on free ports, then drives the page in headless Chromium, in a
 * fresh context with an empty HTTP cache, while Playwright records the
 * viewport: the cold load to a complete paint, a scroll through the events
 * grid, a level filter, a sort, a status write through the page, and a
 * transaction committed in PostgreSQL through `psql` that reaches the page
 * without a reload. ffmpeg turns the video into the GIF.
 *
 * It needs everything `pnpm incidents` needs (Docker and a Rust toolchain for
 * the release build) and `ffmpeg` on `PATH`. The captions on the page are the
 * SQL and the controls the script runs.
 *
 * Usage:
 *   node --import ./scripts/register.mjs scripts/record-incidents.ts
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, renameSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Page } from "playwright";

import {
  appendBounded,
  availablePort,
  delay,
  demoPsql,
  exitWithin,
  launchDemoBrowser,
} from "./demo-process.ts";
import { announcedUrl } from "./incidents-e2e.ts";
import { CUT_ROWS, incidentsComposeEnvironment } from "./incidents.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const OUTPUT = join(root, "docs/assets/calque-incidents-100k.gif");

/**
 * Wider than the page's 1480px minimum, at which the status chart's legend
 * overflows its panel.
 */
const VIEWPORT = { width: 1680, height: 900 } as const;
const GIF_WIDTH = 1120;
const GIF_FPS = 10;

/**
 * One transaction, as a source application would commit it: 40 new `error`
 * events at the head of the grid and 800 unresolved issues resolved, which
 * moves the status chart.
 */
const SOURCE_TRANSACTION = `BEGIN;
INSERT INTO incidents.events
  (id, organization_id, project_id, issue_id, level, message, occurred_at,
   payload, bench_index, is_sentinel)
SELECT 900000000 + e.n, e.organization_id, e.project_id, e.issue_id, 'error',
       'Payment webhook timed out after 3 retries', now(), e.payload,
       900000000 + e.n, false
FROM (SELECT *, row_number() OVER (ORDER BY id) AS n
      FROM incidents.events
      WHERE NOT is_sentinel AND level = 'error'
      ORDER BY id LIMIT 40) e;
UPDATE incidents.issues SET status = 'resolved', updated_at = now()
WHERE id IN (SELECT id FROM incidents.issues
             WHERE status = 'unresolved' AND NOT is_sentinel
             ORDER BY id LIMIT 800);
COMMIT;`;

const [uiPort, serverPort, postgresPort] = await Promise.all([
  availablePort(),
  availablePort(),
  availablePort(),
]);
const environment: NodeJS.ProcessEnv = {
  ...process.env,
  CALQUE_INCIDENTS_POSTGRES_PORT: String(postgresPort),
  CALQUE_INCIDENTS_PORT: String(serverPort),
  CALQUE_INCIDENTS_UI_PORT: String(uiPort),
};

// The recorded transaction commits to the source, so a rerun would find it
// already applied; each run starts from a freshly seeded database instead.
spawnSync("docker", ["compose", "down", "--volumes"], {
  cwd: root,
  env: incidentsComposeEnvironment(environment, postgresPort),
  stdio: "ignore",
});

const launcher = spawn(
  "node",
  [
    "--import",
    "./scripts/register.mjs",
    "scripts/incidents.ts",
    "--no-open",
    "--fresh",
    "--rows",
    "100k",
    "--dataset",
    "content-v1",
  ],
  { cwd: root, env: environment, stdio: ["ignore", "pipe", "pipe"] },
);
let launcherOutput = "";
for (const stream of [launcher.stdout, launcher.stderr]) {
  stream.setEncoding("utf8");
  stream.on("data", (chunk: string) => {
    launcherOutput = appendBounded(launcherOutput, chunk);
  });
}
const launcherExited = new Promise<void>((resolveExit) =>
  launcher.once("exit", () => resolveExit()),
);

const videos = mkdtempSync(join(tmpdir(), "calque-record-"));
try {
  const url = await waitForPage();
  const video = await record(url);
  encode(video);
  report();
} finally {
  if (launcher.exitCode === null && launcher.signalCode === null)
    launcher.kill("SIGINT");
  if (!(await exitWithin(launcherExited, 30_000))) launcher.kill("SIGKILL");
  rmSync(videos, { recursive: true, force: true });
}

async function waitForPage(): Promise<string> {
  const deadline = Date.now() + 30 * 60_000;
  while (Date.now() < deadline) {
    const url = announcedUrl(launcherOutput);
    if (url) return url;
    const state = await Promise.race([
      launcherExited.then(() => "exited"),
      delay(250).then(() => "waiting"),
    ]);
    if (state === "exited")
      throw new Error(`pnpm incidents exited early:\n${launcherOutput}`);
  }
  throw new Error(
    `pnpm incidents was not ready in 30 minutes:\n${launcherOutput}`,
  );
}

function psql(sql: string): string {
  return demoPsql(sql, incidentsComposeEnvironment(process.env, postgresPort));
}

async function record(url: string): Promise<string> {
  const browser = await launchDemoBrowser();
  try {
    // A fresh context is a fresh profile with an empty HTTP cache.
    const context = await browser.newContext({
      viewport: VIEWPORT,
      recordVideo: { dir: videos, size: VIEWPORT },
    });
    await context.addInitScript(() => {
      const mount = () => {
        const caption = document.createElement("div");
        caption.id = "recording-caption";
        caption.style.cssText =
          "position:fixed;left:24px;bottom:20px;z-index:99;max-width:900px;padding:9px 14px;border-radius:8px;background:rgba(23,32,29,.92);color:#fff;font:600 14px/1.4 ui-monospace,Menlo,monospace;white-space:pre-wrap;transition:opacity .15s;opacity:0";
        document.body.append(caption);
      };
      if (document.body) mount();
      else addEventListener("DOMContentLoaded", mount);
    });
    const page = await context.newPage();
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(String(error)));

    await delay(400);
    await page.goto(url, { waitUntil: "commit" });
    await caption(page, "Cold load: empty cache, nothing synced yet");
    await completePaint(page);
    await delay(2200);

    await caption(page, "Scroll the events grid: every row is local");
    await scrollGrid(page);

    await caption(page, "Filter: level = error");
    await page.selectOption("select[data-control=level]", "error");
    await delay(2400);

    await caption(page, "Sort: newest first");
    await page.selectOption("select[data-control=sort]", "newest");
    await delay(2400);

    await caption(page, "Write: resolve the open issue");
    const before = await chartTotals(page);
    await page.selectOption("select[data-control=issue-status]", "resolved");
    await page.waitForFunction(
      () =>
        document.querySelector("select[data-control=issue-status]") !== null,
    );
    await delay(2400);

    const eventsBefore = await eventCount(page);
    await caption(
      page,
      "psql, committed in PostgreSQL:\n40 new error events, 800 issues resolved",
    );
    await delay(900);
    psql(SOURCE_TRANSACTION);
    await page.waitForFunction(
      (previous) => {
        const text = document.querySelector(".grid-panel output")?.textContent;
        return text !== null && text !== undefined && text !== previous;
      },
      eventsBefore,
      { timeout: 30_000 },
    );
    await page.waitForFunction(
      (previous) =>
        [...document.querySelectorAll("svg .chart-count")]
          .map((node) => node.textContent)
          .join("|") !== previous,
      before,
      { timeout: 30_000 },
    );
    await delay(3600);
    await caption(page, undefined);
    assert.deepEqual(errors, [], "the page raised errors while recording");

    const video = page.video();
    assert(video, "Playwright recorded no video");
    await context.close();
    return await video.path();
  } finally {
    await browser.close();
  }
}

async function caption(page: Page, text: string | undefined): Promise<void> {
  await page.evaluate((value) => {
    const element = document.getElementById("recording-caption");
    if (!element) return;
    element.textContent = value ?? "";
    element.style.opacity = value === undefined ? "0" : "1";
  }, text);
}

async function completePaint(page: Page): Promise<void> {
  await page.waitForFunction(
    () =>
      document.querySelector(".sync .dot")?.getAttribute("data-state") ===
        "ready" &&
      /events/u.test(
        document.querySelector(".grid-panel output")?.textContent ?? "",
      ) &&
      document.querySelectorAll("svg .chart-count").length > 0 &&
      document.querySelector(".issue-view [data-field=title]") !== null,
    undefined,
    { timeout: 120_000 },
  );
}

async function scrollGrid(page: Page): Promise<void> {
  const grid = await page.locator(".grid-panel canvas").first().boundingBox();
  assert(grid, "the events grid has no canvas");
  await page.mouse.move(grid.x + grid.width / 2, grid.y + grid.height / 2);
  for (let step = 0; step < 36; step += 1) {
    await page.mouse.wheel(0, 1500);
    await delay(60);
  }
  await delay(500);
}

async function chartTotals(page: Page): Promise<string> {
  return await page.evaluate(() =>
    [...document.querySelectorAll("svg .chart-count")]
      .map((node) => node.textContent)
      .join("|"),
  );
}

async function eventCount(page: Page): Promise<string> {
  return await page.evaluate(
    () => document.querySelector(".grid-panel output")?.textContent ?? "",
  );
}

/** The recorded WebM as a GIF: a two-pass palette, so flat colours stay flat. */
function encode(video: string): void {
  mkdirSync(dirname(OUTPUT), { recursive: true });
  const temporary = `${OUTPUT}.tmp`;
  const filters = `fps=${GIF_FPS},scale=${GIF_WIDTH}:-1:flags=lanczos`;
  const result = spawnSync(
    "ffmpeg",
    [
      "-v",
      "error",
      "-y",
      "-i",
      video,
      "-vf",
      `${filters},split[a][b];[a]palettegen=max_colors=64:stats_mode=diff[p];[b][p]paletteuse=dither=none:diff_mode=rectangle`,
      "-loop",
      "0",
      "-f",
      "gif",
      temporary,
    ],
    { encoding: "utf8" },
  );
  if (result.error || result.status !== 0)
    throw new Error(`ffmpeg failed: ${result.error?.message ?? result.stderr}`);
  renameSync(temporary, OUTPUT);
}

function report(): void {
  const probe = spawnSync(
    "ffprobe",
    [
      "-v",
      "error",
      "-show_entries",
      "format=duration",
      "-of",
      "csv=p=0",
      OUTPUT,
    ],
    { encoding: "utf8" },
  );
  const bytes = statSync(OUTPUT).size;
  process.stdout.write(
    `${JSON.stringify({
      outcome: "recorded",
      path: "docs/assets/calque-incidents-100k.gif",
      bytes,
      seconds: Number.parseFloat(probe.stdout),
      rows: CUT_ROWS,
    })}\n`,
  );
}
