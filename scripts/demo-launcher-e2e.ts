#!/usr/bin/env node
/// <reference lib="dom" />
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Browser, Page } from "playwright";

import {
  appendBounded,
  availablePort,
  delay,
  exitWithin,
  recordDemo,
  restoreDemo,
  type DemoExit,
} from "./demo-process.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const clientRoot = join(root, "client/packages/client");
const requireFromClient = createRequire(join(clientRoot, "package.json"));
const { chromium } = requireFromClient(
  "playwright",
) as typeof import("playwright");
const target = join(root, "target");
mkdirSync(target, { recursive: true });
const screenshotDirectory = join(target, "demo-launcher");
const screenshotPath = join(screenshotDirectory, "automatic-sync.png");
mkdirSync(screenshotDirectory, { recursive: true });
const profile = mkdtempSync(join(target, "demo-launcher-chrome-"));
const [uiPort, serverPort] = await Promise.all([
  availablePort(),
  availablePort(),
]);

// This harness runs the launcher itself rather than through `startDemo`, so
// it records and restores the source and `.calque-dev` itself.
const found = recordDemo();
const child = spawn(
  "node",
  ["--import", "./scripts/register.mjs", "scripts/demo.ts", "--from-source"],
  {
    cwd: root,
    env: {
      ...process.env,
      CALQUE_LOCAL_PORT: String(serverPort),
      CALQUE_DEMO_UI_PORT: String(uiPort),
      CALQUE_DEMO_CHROME_PROFILE: profile,
      CALQUE_DEMO_CHROME_DEBUG: "1",
    },
    stdio: ["ignore", "pipe", "pipe"],
  },
);
let output = "";
for (const stream of [child.stdout, child.stderr]) {
  stream.setEncoding("utf8");
  stream.on("data", (chunk: string) => {
    output = appendBounded(output, chunk);
  });
}
const exited = new Promise<DemoExit>((resolveExit) => {
  child.once("exit", (code, signal) => resolveExit({ code, signal }));
});

let browser: Browser | undefined;
let page: Page | undefined;
let browserProbe: unknown;
const pageErrors: string[] = [];
const consoleErrors: string[] = [];
const requestFailures: { url: string; error: string }[] = [];
try {
  const url = await waitForLauncherUrl();
  const debuggingPort = await waitForDebuggingPort();
  browser = await chromium.connectOverCDP(`http://127.0.0.1:${debuggingPort}`);
  page = await waitForPage(browser, url);
  page.on("pageerror", (error) => pageErrors.push(String(error)));
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("requestfailed", (request) => {
    requestFailures.push({
      url: request.url(),
      error: request.failure()?.errorText ?? "unknown",
    });
  });
  browserProbe = await page.evaluate(async () => {
    try {
      const response = await fetch("/_calque/readyz", {
        cache: "no-store",
      });
      return { ok: response.ok, status: response.status };
    } catch (error) {
      return {
        ok: false,
        name: error instanceof Error ? error.name : typeof error,
        message: error instanceof Error ? error.message : String(error),
      };
    }
  });
  assert.deepEqual(browserProbe, { ok: true, status: 204 });
  await waitForDashboard(page);
  const initialStatus = await firstProjectStatus(page);
  const initialOpenTasks = await page.locator("#metric-open").innerText();
  const change = spawnSync(
    "node",
    ["--import", "./scripts/register.mjs", "scripts/demo.ts", "change"],
    {
      cwd: root,
      env: process.env,
      encoding: "utf8",
    },
  );
  assert.equal(
    change.status,
    0,
    `demo:change failed:\n${change.stdout}${change.stderr}`,
  );
  await page.waitForFunction(
    ({ previousStatus, previousOpenTasks }) => {
      const status = document
        .querySelector("#project-rows tr td:nth-child(2)")
        ?.textContent?.trim();
      const openTasks = document
        .querySelector("#metric-open")
        ?.textContent?.trim();
      return (
        status !== previousStatus &&
        openTasks !== previousOpenTasks &&
        document.querySelector("#status")?.getAttribute("data-state") ===
          "ready" &&
        document.querySelector("#notice")?.getAttribute("data-kind") !==
          "error" &&
        document.querySelectorAll("#task-rows tr").length === 9
      );
    },
    { previousStatus: initialStatus, previousOpenTasks: initialOpenTasks },
    { timeout: 60_000 },
  );
  const refreshedStatus = await firstProjectStatus(page);
  assert.notEqual(refreshedStatus, initialStatus);
  assert.equal(await page.locator("#metric-open").innerText(), "3");
  const changedTask = page.locator("#task-rows tr", { hasText: "1001" });
  assert.equal(await changedTask.locator("td").first().innerText(), "Done");
  assert.equal(
    await page.locator("#status").getAttribute("data-state"),
    "ready",
  );
  assert.notEqual(
    await page.locator("#notice").getAttribute("data-kind"),
    "error",
  );
  assert.deepEqual(pageErrors, []);
  assert.deepEqual(consoleErrors, []);
  await page.screenshot({ path: screenshotPath, fullPage: true });
  process.stdout.write(
    `${JSON.stringify({
      outcome: "passed",
      scenario: "launcher-owned-visible-chrome-bootstrap",
      uiPort,
      serverPort,
      bootstrapStatus: 200,
      automaticStatusChanged: true,
      completeTaskRows: 9,
      changedTaskState: "Done",
      screenshot: "target/demo-launcher/automatic-sync.png",
      pageErrors: 0,
      consoleErrors: 0,
    })}\n`,
  );
} catch (error) {
  process.stderr.write(
    `Demo launcher browser E2E failed: ${JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
      pageErrors,
      consoleErrors,
      requestFailures,
      browserProbe,
      notice: await page
        ?.locator("#notice")
        .innerText()
        .catch(() => undefined),
      launcher: output,
    })}\n`,
  );
  throw error;
} finally {
  if (child.exitCode === null && child.signalCode === null)
    child.kill("SIGINT");
  await exitWithin(exited, 20_000);
  await browser?.close().catch(() => undefined);
  rmSync(profile, { recursive: true, force: true });
  restoreDemo(found);
}

async function waitForLauncherUrl(): Promise<string> {
  const deadline = Date.now() + 300_000;
  while (Date.now() < deadline) {
    const match = output.match(
      /Open: (http:\/\/127\.0\.0\.1:\d+\/\?run=[A-Za-z0-9_-]+)/u,
    );
    const url = match?.[1];
    if (url) return url;
    const result = await Promise.race([
      exited.then(() => "exited"),
      delay(100).then(() => "waiting"),
    ]);
    if (result === "exited") {
      throw new Error(`launcher exited before readiness:\n${output}`);
    }
  }
  throw new Error(`launcher readiness timed out:\n${output}`);
}

async function waitForDebuggingPort(): Promise<number> {
  const path = join(profile, "DevToolsActivePort");
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (existsSync(path)) {
      const [port] = readFileSync(path, "utf8").split(/\r?\n/u);
      if (port !== undefined && /^[1-9][0-9]*$/u.test(port))
        return Number.parseInt(port, 10);
    }
    await delay(100);
  }
  throw new Error("launcher-owned Chrome did not expose its debugging port");
}

async function waitForPage(
  currentBrowser: Browser,
  url: string,
): Promise<Page> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    for (const context of currentBrowser.contexts()) {
      for (const candidate of context.pages()) {
        if (candidate.url() === url) return candidate;
      }
    }
    await delay(100);
  }
  throw new Error(`launcher-owned Chrome did not open ${url}`);
}

async function waitForDashboard(currentPage: Page): Promise<void> {
  await currentPage.waitForFunction(
    () =>
      document.querySelector("#status")?.getAttribute("data-state") ===
        "ready" &&
      document.querySelector("#metric-projects")?.textContent === "3" &&
      document.querySelector("#metric-total")?.textContent ===
        "9 total tasks" &&
      document.querySelectorAll("#project-rows tr").length === 3 &&
      document.querySelectorAll("#task-rows tr").length === 9 &&
      document.querySelector("#notice")?.getAttribute("data-kind") !== "error",
    undefined,
    { timeout: 60_000 },
  );
}

async function firstProjectStatus(currentPage: Page): Promise<string> {
  const status = (
    await currentPage
      .locator("#project-rows tr")
      .first()
      .locator("td")
      .nth(1)
      .innerText()
  ).trim();
  assert.match(status, /^(At Risk|On Track)$/u);
  return status;
}
