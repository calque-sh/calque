#!/usr/bin/env node
/// <reference lib="dom" />
import assert from "node:assert/strict";
import { mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Page } from "playwright";

import {
  demoBrowserChannel,
  launchDemoBrowser,
  runSourceChange,
} from "./demo-process.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const url = process.env.CALQUE_DEMO_URL ?? "http://127.0.0.1:5173";
if (
  !/^http:\/\/127\.0\.0\.1:\d+\/?(?:\?run=[A-Za-z0-9_-]{16,64})?$/u.test(url)
) {
  throw new Error("CALQUE_DEMO_URL must be an exact numeric-loopback demo URL");
}

interface FetchTraceEntry {
  url: string;
  method: string;
  status?: number;
  error?: string;
}

type NetworkEvent =
  | { event: "failed"; method: string; url: string; error: string }
  | { event: "response"; method: string; url: string; status: number };

// Set by the in-page `addInitScript` below; read back through
// `page.evaluate` for failure diagnostics.
declare global {
  // `var` is required here: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  // eslint-disable-next-line no-var
  var __calqueDemoFetchTrace: FetchTraceEntry[];
}

const pageErrors: string[] = [];
const consoleErrors: string[] = [];
const network: NetworkEvent[] = [];
const output = join(root, "target/demo-browser-smoke");
mkdirSync(output, { recursive: true });
const browser = await launchDemoBrowser();
let page: Page | undefined;
try {
  page = await browser.newPage({
    viewport: { width: 1280, height: 900 },
  });
  await page.addInitScript(() => {
    const nativeFetch = globalThis.fetch.bind(globalThis);
    const trace: FetchTraceEntry[] = [];
    Object.defineProperty(globalThis, "__calqueDemoFetchTrace", {
      value: trace,
    });
    globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const entry: FetchTraceEntry = {
        url: String(input),
        method: init?.method ?? "GET",
      };
      trace.push(entry);
      try {
        const response = await nativeFetch(input, init);
        entry.status = response.status;
        return response;
      } catch (error) {
        entry.error = error instanceof Error ? error.message : String(error);
        throw error;
      }
    };
  });
  page.on("pageerror", (error) => pageErrors.push(String(error)));
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("requestfailed", (request) => {
    network.push({
      event: "failed",
      method: request.method(),
      url: request.url(),
      error: request.failure()?.errorText ?? "unknown",
    });
  });
  page.on("response", (response) => {
    if (response.url().includes("/v1/") || response.url().includes("/readyz")) {
      network.push({
        event: "response",
        method: response.request().method(),
        url: response.url(),
        status: response.status(),
      });
    }
  });
  await page.goto(url, { waitUntil: "load", timeout: 30_000 });
  await waitForDashboard(page);
  const beforeStatus = await firstProjectStatus(page);
  const beforeOpenTasks = await page.locator("#metric-open").innerText();

  runSourceChange();

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
    { previousStatus: beforeStatus, previousOpenTasks: beforeOpenTasks },
    { timeout: 60_000 },
  );
  const afterStatus = await firstProjectStatus(page);
  assert.notEqual(afterStatus, beforeStatus);
  assert.equal(await page.locator("#metric-open").innerText(), "3");
  assert.equal(
    await page
      .locator("#task-rows tr", { hasText: "1001" })
      .locator("td")
      .first()
      .innerText(),
    "Done",
  );
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
  await page.screenshot({
    path: join(output, "automatic-sync.png"),
    fullPage: true,
  });
  process.stdout.write(
    `${JSON.stringify({
      outcome: "passed",
      url,
      browser: demoBrowserChannel(),
      beforeStatus,
      afterStatus,
      completeTaskRows: 9,
      changedTaskState: "Done",
      screenshot: "target/demo-browser-smoke/automatic-sync.png",
      pageErrors: 0,
      consoleErrors: 0,
    })}\n`,
  );
} catch (error) {
  const diagnostics = page
    ? {
        status: await page.locator("#status").getAttribute("data-state"),
        notice: await page.locator("#notice").innerText(),
        pageErrors,
        consoleErrors,
        network,
        fetchTrace: await page.evaluate(
          () => globalThis.__calqueDemoFetchTrace,
        ),
      }
    : { pageErrors, consoleErrors, network };
  await page?.screenshot({
    path: join(output, "failure.png"),
    fullPage: true,
  });
  process.stderr.write(
    `Demo browser smoke failed: ${JSON.stringify(diagnostics)}\n`,
  );
  throw error;
} finally {
  await browser.close();
}

async function waitForDashboard(currentPage: Page): Promise<void> {
  await currentPage.waitForFunction(
    () =>
      document.querySelector("#status")?.getAttribute("data-state") ===
        "ready" &&
      document.querySelector("#metric-projects")?.textContent === "3" &&
      document.querySelector("#metric-total")?.textContent ===
        "9 total tasks" &&
      document.querySelectorAll("#task-rows tr").length === 9,
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
