#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Restarts PostgreSQL under a running `calque dev` and an open tab.
 *
 * `docker compose restart postgres` takes the source down with every
 * connection the server holds and brings it back on the same volume, while the
 * server process stays up. `/readyz` must drop to 503 and return to 204
 * without restarting `calque dev`, and the open dashboard must render the
 * transaction committed during the recovery and the one after it, each exactly
 * once, ending equal to PostgreSQL. Timings are directional: one host, with
 * PostgreSQL in Docker.
 *
 * It restarts the PostgreSQL of whichever compose project it is given; the
 * port and project below keep a run off a developer's own source.
 *
 * Usage:
 *   CALQUE_POSTGRES_PORT=55471 COMPOSE_PROJECT_NAME=calque-cdc-restart \
 *     pnpm demo:e2e:cdc-restart
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import type { Page } from "playwright";

import {
  demoBrowserChannel,
  availablePort,
  delay,
  launchDemoBrowser,
  publicationCount,
  startDemo,
  stopDemo,
  type DemoProcess,
  type PagePublication,
} from "./demo-process.ts";
import {
  DEVELOPMENT_SOURCE_ROLE,
  demoChangeSql,
  sanitizePostgresRoutingEnvironment,
} from "./demo.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const composeEnvironment: NodeJS.ProcessEnv = {
  ...sanitizePostgresRoutingEnvironment(process.env),
  CALQUE_POSTGRES_PORT: process.env.CALQUE_POSTGRES_PORT ?? "55432",
};
/** How long the source may take to come back and the runtime to catch up. */
const RECOVERY_TIMEOUT_MS = 120_000;
const READINESS_POLL_MS = 50;
const SERVICE_UNAVAILABLE_CONSOLE_ERROR =
  "Failed to load resource: the server responded with a status of 503 (Service Unavailable)";

interface ReadinessSample {
  readonly atMs: number;
  readonly status: number | "unreachable";
}

interface ChangeResponse {
  readonly atMs: number;
  readonly status: number;
}

const pageErrors: string[] = [];
const consoleErrors: string[] = [];
const changeResponses: ChangeResponse[] = [];
const changeFailures: string[] = [];
const changeFailureChecks: Promise<void>[] = [];

const [uiPort, serverPort, metricsPort] = await Promise.all([
  availablePort(),
  availablePort(),
  availablePort(),
]);
const endpoint = `http://127.0.0.1:${serverPort}`;
const metricsEndpoint = `http://127.0.0.1:${metricsPort}`;
const browser = await launchDemoBrowser();
let demo: DemoProcess | undefined;
let page: Page | undefined;
let stopSampling = false;
try {
  page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  page.on("pageerror", (error) => pageErrors.push(String(error)));
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("response", (response) => {
    if (new URL(response.url()).pathname === "/_calque/v1/changes") {
      changeResponses.push({
        atMs: performance.now(),
        status: response.status(),
      });
    }
  });
  page.on("requestfailed", (request) => {
    if (new URL(request.url()).pathname !== "/_calque/v1/changes") return;
    const errorText = request.failure()?.errorText ?? "unknown";
    // Chromium ends a streamed response as canceled whenever the page's last
    // read comes after the network's last byte, although the page received
    // every byte. A 200 the page abandoned itself refused a batch, which ends
    // the mount and fails the dashboard checks. Any other failure, or one
    // before a response, counts.
    changeFailureChecks.push(
      request.response().then((response) => {
        if (errorText !== "net::ERR_ABORTED" || response?.status() !== 200)
          changeFailures.push(errorText);
      }),
    );
  });

  demo = await startDemo(serverPort, uiPort, { metricsPort });
  await page.goto(demo.url, { waitUntil: "load", timeout: 30_000 });
  await waitForDashboard(page);
  assert.equal(
    await reconnects(),
    0,
    "the runtime reconnected before the restart",
  );
  const beforeRestart = await commitAndCollect(page, async () => undefined);

  const samples: ReadinessSample[] = [];
  const sampling = sampleReadiness(samples, () => stopSampling);
  const restartStartedAt = performance.now();
  await runAsync(["compose", "restart", "postgres"], { check: true });
  const restartReturnedAt = performance.now();
  await waitForPostgres();
  const postgresAcceptingAt = performance.now();

  // Committed the moment the source accepts connections, before waiting for
  // readiness, so the runtime has usually not re-established its session. A
  // 503 read after the commit returned means it had not when the commit landed.
  let committedWhileNotCurrent = false;
  const duringRecovery = await commitAndCollect(page, async () => {
    const readiness = await fetch(`${endpoint}/readyz`);
    await readiness.arrayBuffer();
    committedWhileNotCurrent = readiness.status !== 204;
    await waitForReadyAfter(samples, restartStartedAt);
  });
  const readyAt = firstReadyAfter(samples, restartStartedAt);
  const afterReady = await commitAndCollect(page, async () => undefined);
  stopSampling = true;
  await sampling;

  const notReady = samples.find(
    (sample) => sample.atMs >= restartStartedAt && sample.status !== 204,
  );
  assert(notReady, "readiness never dropped while PostgreSQL restarted");
  assert(readyAt !== undefined, "readiness did not return");
  assert.equal(
    await reconnects(),
    1,
    "one replication session should have been re-established",
  );

  // Each transaction is rendered once: three commits, three transactions
  // across the publications, and no cursor twice.
  const publications = [...beforeRestart, ...duringRecovery, ...afterReady];
  const cursors = publications.map((publication) => publication.cursor);
  assert.equal(
    new Set(cursors).size,
    cursors.length,
    `a cursor was delivered twice: ${JSON.stringify(cursors)}`,
  );
  for (const [phase, delivered] of [
    ["before the restart", beforeRestart],
    ["during recovery", duringRecovery],
    ["after readiness returned", afterReady],
  ] as const) {
    assert.equal(
      delivered.reduce((sum, entry) => sum + entry.transactionCount, 0),
      1,
      `the publications ${phase} do not account for exactly one transaction`,
    );
  }
  // And the tab that stayed open ends where PostgreSQL is.
  await waitForDashboard(page);
  assert.equal(
    await page.locator("#metric-open").innerText(),
    (await psql("SELECT count(*) FROM tasks WHERE NOT done")).trim(),
    "the open tab disagrees with PostgreSQL about open tasks",
  );

  assert.deepEqual(pageErrors, []);
  await Promise.all(changeFailureChecks);
  assert.deepEqual(changeFailures, []);
  const unexpectedStatuses = changeResponses.filter(
    (response) => response.status !== 200 && response.status !== 503,
  );
  assert.deepEqual(unexpectedStatuses, []);
  const unavailable = changeResponses.filter(
    (response) => response.status === 503,
  );
  assert.deepEqual(
    consoleErrors.filter(
      (message) => message !== SERVICE_UNAVAILABLE_CONSOLE_ERROR,
    ),
    [],
  );
  assert.equal(consoleErrors.length, unavailable.length);

  const elapsed = (from: number, to: number) => Math.round(to - from);
  process.stdout.write(
    `${JSON.stringify({
      outcome: "passed",
      scenario: "postgres-restart-under-open-tab",
      browser: demoBrowserChannel(),
      host: "local Docker; directional",
      restartCommandMs: elapsed(restartStartedAt, restartReturnedAt),
      notReadyAfterMs: elapsed(restartStartedAt, notReady.atMs),
      postgresAcceptingAfterMs: elapsed(restartStartedAt, postgresAcceptingAt),
      readyAfterMs: elapsed(restartStartedAt, readyAt),
      readyAfterPostgresAcceptingMs: elapsed(postgresAcceptingAt, readyAt),
      readinessSamples: samples.length,
      committedWhileNotCurrent,
      publications: publications.length,
      duplicateCursors: 0,
      changeResponses503: unavailable.length,
      cdcReconnects: 1,
    })}\n`,
  );
} catch (error) {
  process.stderr.write(
    `Demo CDC restart E2E failed: ${JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
      status: await page
        ?.locator("#status")
        .getAttribute("data-state")
        .catch(() => null),
      pageErrors,
      consoleErrors,
      changeResponses,
      changeFailures,
      launcher: demo?.output().slice(-12_000),
    })}\n`,
  );
  throw error;
} finally {
  stopSampling = true;
  await page?.close().catch(() => undefined);
  await browser.close().catch(() => undefined);
  if (demo) await stopDemo(demo);
}

/**
 * Commits one source transaction, runs `beforeWaiting`, and returns the
 * publications the page rendered for that transaction.
 */
async function commitAndCollect(
  currentPage: Page,
  beforeWaiting: () => Promise<void>,
): Promise<readonly PagePublication[]> {
  const before = await publicationCount(currentPage);
  await psql(demoChangeSql);
  await beforeWaiting();
  await currentPage.waitForFunction(
    (seen) => globalThis.__calqueDemoPublications.length > seen,
    before,
    { timeout: RECOVERY_TIMEOUT_MS },
  );
  // A settle window, so a second publication of the same transaction is
  // collected rather than missed.
  await delay(250);
  return currentPage.evaluate(
    (seen) => globalThis.__calqueDemoPublications.slice(seen),
    before,
  );
}

async function sampleReadiness(
  samples: ReadinessSample[],
  stopped: () => boolean,
): Promise<void> {
  while (!stopped()) {
    let status: ReadinessSample["status"];
    try {
      const response = await fetch(`${endpoint}/readyz`, {
        signal: AbortSignal.timeout(2_000),
      });
      await response.arrayBuffer();
      status = response.status;
    } catch {
      status = "unreachable";
    }
    samples.push({ atMs: performance.now(), status });
    await delay(READINESS_POLL_MS);
  }
}

function firstReadyAfter(
  samples: readonly ReadinessSample[],
  startedAt: number,
): number | undefined {
  const notReady = samples.findIndex(
    (sample) => sample.atMs >= startedAt && sample.status !== 204,
  );
  if (notReady < 0) return undefined;
  return samples.slice(notReady).find((sample) => sample.status === 204)?.atMs;
}

async function waitForReadyAfter(
  samples: readonly ReadinessSample[],
  startedAt: number,
): Promise<void> {
  const deadline = performance.now() + RECOVERY_TIMEOUT_MS;
  while (firstReadyAfter(samples, startedAt) === undefined) {
    if (performance.now() > deadline)
      throw new Error(
        `readiness did not return: ${JSON.stringify(samples.slice(-20))}`,
      );
    await delay(READINESS_POLL_MS);
  }
}

async function waitForPostgres(): Promise<void> {
  const deadline = performance.now() + RECOVERY_TIMEOUT_MS;
  for (;;) {
    const ready = await runAsync([
      "compose",
      "exec",
      "-T",
      "postgres",
      "pg_isready",
      "--username",
      DEVELOPMENT_SOURCE_ROLE,
      "--dbname",
      "calque_dev",
    ]);
    if (ready.status === 0) return;
    if (performance.now() > deadline)
      throw new Error(`PostgreSQL did not come back: ${ready.stderr}`);
    await delay(100);
  }
}

async function reconnects(): Promise<number> {
  const response = await fetch(`${metricsEndpoint}/metrics`);
  assert.equal(response.status, 200);
  const line = (await response.text())
    .split("\n")
    .find((entry) => entry.startsWith("calque_cdc_reconnects_total "));
  assert(line, "the server does not export calque_cdc_reconnects_total");
  return Number(line.slice("calque_cdc_reconnects_total ".length));
}

async function psql(sql: string): Promise<string> {
  const result = await runAsync(
    [
      "compose",
      "exec",
      "-T",
      "postgres",
      "psql",
      "--set",
      "ON_ERROR_STOP=1",
      "--no-align",
      "--tuples-only",
      "--username",
      DEVELOPMENT_SOURCE_ROLE,
      "--dbname",
      "calque_dev",
    ],
    { check: true, input: sql },
  );
  return result.stdout;
}

/**
 * `docker` without blocking the event loop, so the readiness sampler keeps
 * its cadence while PostgreSQL restarts.
 */
function runAsync(
  arguments_: readonly string[],
  { check = false, input }: { check?: boolean; input?: string } = {},
): Promise<{ status: number | null; stdout: string; stderr: string }> {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn("docker", arguments_, {
      cwd: root,
      env: composeEnvironment,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => (stdout += chunk));
    child.stderr.on("data", (chunk: string) => (stderr += chunk));
    child.once("error", rejectRun);
    child.once("close", (status) => {
      if (check && status !== 0) {
        rejectRun(
          new Error(`docker ${arguments_.join(" ")} failed: ${stderr}`),
        );
        return;
      }
      resolveRun({ status, stdout, stderr });
    });
    child.stdin.end(input ?? "");
  });
}

async function waitForDashboard(currentPage: Page): Promise<void> {
  await currentPage.waitForFunction(
    () => {
      const status = document
        .querySelector("#status")
        ?.getAttribute("data-state");
      const projects = document.querySelector("#metric-projects")?.textContent;
      const tasks = document.querySelector("#metric-total")?.textContent;
      return (
        status === "ready" && projects === "3" && tasks === "9 total tasks"
      );
    },
    undefined,
    { timeout: 60_000 },
  );
}
