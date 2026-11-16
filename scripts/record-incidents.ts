#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * `pnpm record:incidents`: regenerates `docs/assets/calque-incidents-100k.gif`,
 * the README's recording of the incident console at 100k rows.
 *
 * It runs `scripts/incidents.ts --no-open --fresh --dataset readable` on free
 * ports, then drives the console at `/showcase/` in headless Chromium, in a
 * fresh context with an empty HTTP cache and the benchmark's emulated
 * 100 Mbit/s link with 30 ms of latency, while Playwright records the
 * viewport: the cold load to a complete paint, jumps to the end of the events
 * list, a level filter, an issue resolved through the page, and a
 * transaction committed in PostgreSQL through `psql` that reaches the page
 * without a reload. Chromium's screencast hands over each frame it paints as
 * a PNG, and ffmpeg turns them into the GIF: lossless frames keep the text
 * sharp and let the GIF store only the pixels that changed.
 *
 * The console measures every time it shows: from navigation to the complete
 * paint, from a filter's click to the frame that shows its rows, and from the
 * write's click to PostgreSQL's confirmation. The script adds the captions
 * under the console and the terminal, which shows the statements it commits,
 * abbreviated, and what psql answers.
 *
 * It needs everything `pnpm incidents` needs (Docker and a Rust toolchain for
 * the release build) and `ffmpeg` on `PATH`.
 *
 * Usage:
 *   node --import ./scripts/register.mjs scripts/record-incidents.ts
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
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

/** Recorded at the GIF's own size, so its text is not resampled. */
const VIEWPORT = { width: 1200, height: 750 } as const;
const GIF_FPS = 10;
/** The benchmark's emulated link, `pnpm bench`'s default `--network 100/30`. */
const NETWORK = { megabitsPerSecond: 100, latencyMs: 30 } as const;

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
       'POST /hooks/payments timed out after 30000 ms (attempt 3 of 3)', now(),
       e.payload, 900000000 + e.n, false
FROM (SELECT *, row_number() OVER (ORDER BY id) AS n
      FROM incidents.events
      WHERE NOT is_sentinel AND level = 'error'
      ORDER BY id LIMIT 40) e;
UPDATE incidents.issues SET status = 'resolved', updated_at = now()
WHERE id IN (SELECT id FROM incidents.issues
             WHERE status = 'unresolved' AND NOT is_sentinel
             ORDER BY id LIMIT 800);
COMMIT;`;

/** `SOURCE_TRANSACTION` as the terminal shows it. */
const TERMINAL_LINES = [
  "calque_dev=> BEGIN;",
  "calque_dev=> INSERT INTO incidents.events …;  -- 40 new errors",
  "calque_dev=> UPDATE incidents.issues SET status = 'resolved' …;  -- 800 issues",
  "calque_dev=> COMMIT;",
] as const;

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
    "--dataset",
    "readable",
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

const frames = mkdtempSync(join(tmpdir(), "calque-record-"));
try {
  const url = await waitForPage();
  encode(await record(url));
  report();
} finally {
  if (launcher.exitCode === null && launcher.signalCode === null)
    launcher.kill("SIGINT");
  if (!(await exitWithin(launcherExited, 30_000))) launcher.kill("SIGKILL");
  rmSync(frames, { recursive: true, force: true });
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

/** A painted frame: its PNG under `frames`, and when Chromium painted it. */
interface Frame {
  readonly file: string;
  readonly seconds: number;
}

async function record(url: string): Promise<readonly Frame[]> {
  const browser = await launchDemoBrowser();
  try {
    // A fresh context is a fresh profile with an empty HTTP cache.
    const context = await browser.newContext({ viewport: VIEWPORT });
    await context.addInitScript(mountOverlay);
    const page = await context.newPage();
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(String(error)));
    const shaping = await context.newCDPSession(page);
    const bytesPerSecond = (NETWORK.megabitsPerSecond * 1_000_000) / 8;
    await shaping.send("Network.enable");
    await shaping.send("Network.emulateNetworkConditions", {
      offline: false,
      latency: NETWORK.latencyMs,
      downloadThroughput: bytesPerSecond,
      uploadThroughput: bytesPerSecond,
    });
    const painted: Frame[] = [];
    shaping.on("Page.screencastFrame", (frame) => {
      const file = `${String(painted.length).padStart(5, "0")}.png`;
      writeFileSync(join(frames, file), Buffer.from(frame.data, "base64"));
      painted.push({ file, seconds: frame.metadata.timestamp ?? 0 });
      void shaping.send("Page.screencastFrameAck", {
        sessionId: frame.sessionId,
      });
    });

    // The recording opens on a page of the console's background colour,
    // which holds until the console's first paint.
    await page.setContent('<html style="background:#221d2e"></html>');
    await shaping.send("Page.startScreencast", {
      format: "png",
      everyNthFrame: 3,
    });
    await delay(400);
    await page.goto(new URL("showcase/", url).href, { waitUntil: "commit" });
    await page.waitForFunction(
      () => document.documentElement.dataset.loadMs !== undefined,
      undefined,
      { timeout: 120_000 },
    );
    await delay(2600);

    const total = await eventTotal(page);
    await caption(
      page,
      `All ${total} events are in the page, so the list jumps to the last one`,
    );
    await jumpToEnd(page);
    await delay(900);

    await caption(page, `Filters and sorts run over all ${total} of them`);
    await page.click("button[data-level=error]");
    await page.waitForSelector(".elapsed");
    await delay(2600);

    await caption(page, "A write shows at once, then PostgreSQL confirms it");
    await page.click("button[data-action=resolve]");
    await page.waitForSelector(".confirmed", { timeout: 30_000 });
    await delay(2600);

    const errorsBefore = await eventTotal(page);
    const statusesBefore = await statusCounts(page);
    await caption(page, "A commit from anywhere else reaches the page live");
    await page.evaluate(
      (lines) => window.__recording?.type(lines),
      [...TERMINAL_LINES],
    );
    const tag = psql(SOURCE_TRANSACTION).trim();
    if (tag)
      await page.evaluate((line) => window.__recording?.print(line), tag);
    await page.waitForFunction(
      (previous) =>
        document.querySelector(".count [data-total]")?.textContent !== previous,
      errorsBefore,
      { timeout: 30_000 },
    );
    await page.waitForFunction(
      (previous) =>
        [...document.querySelectorAll(".statuses data")]
          .map((node) => node.textContent)
          .join("|") !== previous,
      statusesBefore,
      { timeout: 30_000 },
    );
    await delay(3800);
    await shaping.send("Page.stopScreencast");
    assert.deepEqual(errors, [], "the page raised errors while recording");
    assert(painted.length > 0, "Chromium painted no frames");
    // The last frame holds until the recording stops.
    return [...painted, { file: "", seconds: Date.now() / 1000 }];
  } finally {
    await browser.close();
  }
}

declare global {
  interface Window {
    __recording?: {
      caption(text: string): void;
      type(lines: readonly string[]): Promise<void>;
      print(line: string): void;
    };
  }
}

/**
 * The recording's captions, in the console's own footer, and its terminal,
 * docked in the events panel below the rows a commit adds. Mounted before
 * the page's scripts run; the footer gets its first caption as the parser
 * adds it, so the layout does not move once the page paints.
 */
function mountOverlay(): void {
  const css = `
#rec-term{position:absolute;right:16px;bottom:52px;left:16px;z-index:9;margin:0;padding:12px 16px 14px;border:1px solid var(--rule);border-radius:10px;background:var(--raise);color:var(--paper);font:13px/1.65 var(--mono);white-space:pre;display:none}
#rec-term::before{content:"psql";display:block;margin-bottom:4px;color:var(--graphite);font:600 12px/1.4 var(--sans)}
#rec-term .out{color:var(--pencil)}
#rec-tick{position:fixed;top:0;left:0;width:1px;height:1px;background:var(--ink);animation:rec-tick .1s steps(2) infinite alternate}
@keyframes rec-tick{to{opacity:.99}}`;
  const first = "First visit: an empty cache and a 100 Mbit/s link";
  const terminal = document.createElement("pre");
  terminal.id = "rec-term";
  const recording: NonNullable<Window["__recording"]> = {
    caption(text) {
      const narration = document.getElementById("narration");
      if (narration) narration.textContent = text;
    },
    async type(lines) {
      document.querySelector("[data-region=events]")?.append(terminal);
      terminal.style.display = "block";
      for (const line of lines) {
        const row = document.createElement("div");
        terminal.append(row);
        for (let end = 0; end <= line.length; end += 3) {
          row.textContent = line.slice(0, end);
          await new Promise((done) => setTimeout(done, 16));
        }
        row.textContent = line;
        await new Promise((done) => setTimeout(done, 120));
      }
    },
    print(line) {
      const row = document.createElement("div");
      row.className = "out";
      row.textContent = line;
      terminal.append(row);
    },
  };
  window.__recording = recording;
  const watch = new MutationObserver(() => {
    if (!document.head || !document.body) return;
    if (!document.getElementById("narration")) return;
    watch.disconnect();
    const style = document.createElement("style");
    style.textContent = css;
    document.head.append(style);
    // Chromium's screencast sends a frame only when the page paints, and
    // drops one painted while it waits for the last to be acknowledged; an
    // invisible animation keeps it painting, so the frame that ends each
    // change is always sent.
    const tick = document.createElement("i");
    tick.id = "rec-tick";
    document.body.append(tick);
    recording.caption(first);
  });
  watch.observe(document, { childList: true, subtree: true });
}

async function caption(page: Page, text: string): Promise<void> {
  await page.evaluate((value) => window.__recording?.caption(value), text);
}

/**
 * Jumps through the events list to its last row in five steps, each held long
 * enough for the rows it lands on to paint. Each step sets the list's scroll
 * position, as dragging its scrollbar does; a wheel turn's smooth scroll
 * would ask for a window at every row it passes.
 */
async function jumpToEnd(page: Page): Promise<void> {
  for (let jump = 1; jump <= 5; jump += 1) {
    await page.locator(".events .scroller").evaluate((list, step) => {
      list.scrollTop = ((list.scrollHeight - list.clientHeight) * step) / 5;
    }, jump);
    await delay(650);
  }
}

async function statusCounts(page: Page): Promise<string> {
  return await page.evaluate(() =>
    [...document.querySelectorAll(".statuses data")]
      .map((node) => node.textContent)
      .join("|"),
  );
}

async function eventTotal(page: Page): Promise<string> {
  return await page.evaluate(
    () => document.querySelector(".count [data-total]")?.textContent ?? "",
  );
}

/**
 * The painted frames as a GIF, each shown until the next was painted: a
 * two-pass palette, so flat colours stay flat.
 */
function encode(painted: readonly Frame[]): void {
  const list = ["ffconcat version 1.0"];
  for (const [index, frame] of painted.slice(0, -1).entries()) {
    const next = painted[index + 1];
    assert(next);
    list.push(
      `file ${frame.file}`,
      `duration ${Math.max(0, next.seconds - frame.seconds).toFixed(4)}`,
    );
  }
  // The concat demuxer ignores the last entry's duration.
  list.push(`file ${painted.at(-2)?.file}`);
  writeFileSync(join(frames, "frames.ffconcat"), `${list.join("\n")}\n`);

  mkdirSync(dirname(OUTPUT), { recursive: true });
  const temporary = `${OUTPUT}.tmp`;
  const filters = `fps=${GIF_FPS}`;
  const result = spawnSync(
    "ffmpeg",
    [
      "-v",
      "error",
      "-y",
      "-f",
      "concat",
      "-i",
      join(frames, "frames.ffconcat"),
      "-vf",
      `${filters},split[a][b];[a]palettegen=max_colors=96:stats_mode=diff[p];[b][p]paletteuse=dither=none:diff_mode=rectangle`,
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
