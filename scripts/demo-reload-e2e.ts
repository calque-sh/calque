#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Reloads the page with a mutation in flight, in a real browser: the reload
 * must not lose a committed write, duplicate one, or leave an optimistic row
 * alive without its echo.
 *
 * It drives the packaged client in headless Chromium against a disposable
 * PostgreSQL 18 and a real `calque dev`, started by the `pnpm demo`
 * launcher. The page has no editing UI, so the mutation goes through
 * `__calqueDemoMutate`, which calls only the generated facade.
 *
 * Usage:
 *   CALQUE_POSTGRES_PORT=55453 node --import ./scripts/register.mjs \
 *     scripts/demo-reload-e2e.ts
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Browser, Page } from "playwright";

import {
  demoBrowserChannel,
  demoPsql,
  availablePort,
  launchDemoBrowser,
  startDemo,
  stopDemo,
  type DemoProcess,
} from "./demo-process.ts";
import { sanitizePostgresRoutingEnvironment } from "./demo.ts";

/** What `__calqueDemoMutate` in the page reports back. */
interface DemoMutation {
  readonly mutationId: string;
  readonly state: string;
  readonly failure?: string;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  // eslint-disable-next-line no-var
  var __calqueDemoMutate: (id: string, title: string) => Promise<DemoMutation>;
  // eslint-disable-next-line no-var
  var __calqueDemoMutationState: (mutationId: string) => string | undefined;
  // eslint-disable-next-line no-var
  var __calqueDemoTaskTitles: () => Promise<readonly (readonly string[])[]>;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const postgresPort = process.env.CALQUE_POSTGRES_PORT ?? "55432";
const composeEnvironment: NodeJS.ProcessEnv = {
  ...sanitizePostgresRoutingEnvironment(process.env),
  CALQUE_POSTGRES_PORT: postgresPort,
};
const psql = (sql: string): string => demoPsql(sql, composeEnvironment);
/** The task the pending mutation renames; nothing else writes to it. */
const PENDING_TASK_ID = 1001n;
/** The task the source transaction renames while that mutation is pending. */
const COMMITTED_TASK_ID = 1002n;

const pageErrors: string[] = [];
const consoleErrors: string[] = [];
const output = join(root, "target/demo-reload");
mkdirSync(output, { recursive: true });

const [uiPort, serverPort] = await Promise.all([
  availablePort(),
  availablePort(),
]);
/** `chromium` or `chrome` as the other harnesses run it, or `firefox`. */
const browserName = process.env.CALQUE_DEMO_BROWSER ?? demoBrowserChannel();
const browser = await launchBrowser();
let demo: DemoProcess | undefined;
let page: Page | undefined;
try {
  page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  page.on("pageerror", (error) => pageErrors.push(String(error)));
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  demo = await startDemo(serverPort, uiPort);
  await page.goto(demo.url, { waitUntil: "load", timeout: 30_000 });
  await waitForDashboard(page);

  const seededTitle = committedTitleOf(PENDING_TASK_ID);
  assert.equal(
    renderedTitleOf(await renderedTasks(page), PENDING_TASK_ID),
    seededTitle,
    "the page did not render the seeded task before the reload",
  );

  const reload = await reloadWithAPendingMutationAndACommittedWrite(
    page,
    seededTitle,
  );

  assert.deepEqual(pageErrors, []);
  // Chromium reports the mutation POST it was told to abort as the only
  // console error; Firefox reports nothing.
  const expectedConsoleErrors = browserName === "firefox" ? 0 : 1;
  assert.deepEqual(
    consoleErrors.filter(
      (message) => !message.includes("net::ERR_CONNECTION_FAILED"),
    ),
    [],
  );
  assert.equal(
    consoleErrors.length,
    expectedConsoleErrors,
    `the aborted mutation POST should be the only console error: ${JSON.stringify(consoleErrors)}`,
  );
  const report = {
    outcome: "passed",
    scenario: "reload-with-a-mutation-in-flight",
    browser: browserName,
    postgresPort: Number(postgresPort),
    ...reload,
  };
  writeFileSync(
    join(output, "report.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  process.stdout.write(`${JSON.stringify(report)}\n`);
} catch (error) {
  process.stderr.write(
    `Demo reload E2E failed: ${JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
      pageErrors,
      consoleErrors,
      demo: demo?.output().slice(-8000),
    })}\n`,
  );
  throw error;
} finally {
  await page?.close().catch(() => undefined);
  await browser.close().catch(() => undefined);
  if (demo) await stopDemo(demo);
}

/** The demo harness's browser, or Firefox where `CALQUE_DEMO_BROWSER` says so. */
function launchBrowser(): Promise<Browser> {
  if (browserName !== "firefox") return launchDemoBrowser();
  const requireFromClient = createRequire(
    join(root, "client/packages/client/package.json"),
  );
  const { firefox } = requireFromClient(
    "playwright",
  ) as typeof import("playwright");
  return firefox.launch({ headless: true });
}

/**
 * Reloads a page with one mutation pending and one committed write in flight.
 * The pending mutation's POST is aborted before it leaves the browser, so its
 * optimistic row is local-only and must not survive the reload, while a source
 * transaction commits in the same window and must not be lost. Both are
 * checked against PostgreSQL rather than against the page's memory of itself.
 */
async function reloadWithAPendingMutationAndACommittedWrite(
  current: Page,
  seededTitle: string,
): Promise<{
  readonly pendingTitle: string;
  readonly committedTitle: string;
  readonly submissionState: string;
  readonly receiptsWritten: number;
  readonly sawCommittedWriteBeforeReload: boolean;
}> {
  const pendingTitle = `never left the browser ${Date.now()}`;
  const committed = `committed while a mutation was pending ${Date.now()}`;
  const receiptsBefore = receiptCount();

  // The mutation the page will still be holding when it goes away. Aborting
  // the POST is the shape of a network that dropped it: the client cannot
  // know whether it committed, so it keeps the delta and reports ambiguity.
  await current.route("**/v1/mutations", (route) =>
    route.abort("connectionfailed"),
  );
  const settled = await current.evaluate(
    ([id, value]) => globalThis.__calqueDemoMutate(String(id), String(value)),
    [PENDING_TASK_ID.toString(), pendingTitle],
  );
  assert.equal(settled.state, "MUTATION_AMBIGUOUS", JSON.stringify(settled));
  assert.equal(
    await current.evaluate(
      (mutationId) => globalThis.__calqueDemoMutationState(mutationId),
      settled.mutationId,
    ),
    "ambiguous",
    "a submission that got no HTTP response must stay ambiguous",
  );
  // The dashboard renders only on publications, so pending local state is read
  // where an application would read it: through the facade.
  assert.equal(
    await facadeTitle(current, PENDING_TASK_ID),
    pendingTitle,
    "the optimistic rename was not readable through the facade",
  );
  assert.equal(
    committedTitleOf(PENDING_TASK_ID),
    seededTitle,
    "PostgreSQL received a mutation whose POST was aborted",
  );

  // A real source transaction, committed while that delta is still pending.
  psql(
    `UPDATE tasks SET title = '${committed}' WHERE id = ${COMMITTED_TASK_ID}`,
  );
  assert.equal(committedTitleOf(COMMITTED_TASK_ID), committed);
  const sawCommittedWriteBeforeReload =
    (await facadeTitle(current, COMMITTED_TASK_ID)) === committed;

  await current.reload({ waitUntil: "load", timeout: 60_000 });
  await current.unroute("**/v1/mutations");
  await waitForDashboard(current);

  // No lost write.
  await waitForRenderedTitle(
    current,
    COMMITTED_TASK_ID,
    committed,
    "the committed write",
  );
  assert.equal(
    await facadeTitle(current, COMMITTED_TASK_ID),
    committed,
    "the rebuilt client's facade lost the committed write",
  );
  // No duplicate write: nothing was submitted, so nothing was received.
  const receiptsWritten = receiptCount() - receiptsBefore;
  assert.equal(
    receiptsWritten,
    0,
    "a mutation whose POST never left the browser reached the source anyway",
  );
  assert.equal(rowCount(COMMITTED_TASK_ID), 1);
  assert.equal(rowCount(PENDING_TASK_ID), 1);
  // No optimistic row surviving without its echo.
  assert.equal(
    await facadeTitle(current, PENDING_TASK_ID),
    seededTitle,
    "an optimistic row survived the reload without its echo",
  );
  assert.deepEqual(
    await renderedTasks(current),
    committedTasks(),
    "the rebuilt client's rows are not the committed PostgreSQL rows",
  );
  return {
    pendingTitle,
    committedTitle: committed,
    submissionState: settled.state,
    receiptsWritten,
    sawCommittedWriteBeforeReload,
  };
}

/** The title the facade reads for one task now, overlay included. */
async function facadeTitle(
  currentPage: Page,
  id: bigint,
): Promise<string | undefined> {
  const rows = await currentPage.evaluate(() =>
    globalThis.__calqueDemoTaskTitles(),
  );
  return rows.find(([rowId]) => rowId === id.toString())?.[1];
}

async function waitForRenderedTitle(
  currentPage: Page,
  id: bigint,
  title: string,
  what: string,
): Promise<void> {
  try {
    await currentPage.waitForFunction(
      ([rowId, expected]) =>
        Array.from(document.querySelectorAll("#task-rows tr")).some((row) => {
          const cells = row.querySelectorAll("td");
          return (
            cells[3]?.querySelector("strong")?.textContent?.trim() === rowId &&
            cells[1]?.querySelector("strong")?.textContent?.trim() === expected
          );
        }),
      [id.toString(), title],
      { timeout: 30_000 },
    );
  } catch (cause) {
    throw new Error(
      `${what} never reached the page: ${title}; rendered=${JSON.stringify(
        await renderedTasks(currentPage),
      )} facade=${JSON.stringify(
        await facadeTitle(currentPage, id),
      )} committed=${JSON.stringify(
        committedTitleOf(id),
      )} status=${await currentPage.evaluate(() =>
        document.querySelector("#status")?.getAttribute("data-state"),
      )} notice=${await currentPage.evaluate(
        () => document.querySelector("#notice")?.textContent,
      )}`,
      { cause },
    );
  }
}

async function waitForDashboard(currentPage: Page): Promise<void> {
  await currentPage.waitForFunction(
    () =>
      document.querySelector("#status")?.getAttribute("data-state") ===
        "ready" &&
      document.querySelector("#metric-projects")?.textContent === "3" &&
      document.querySelectorAll("#task-rows tr").length === 9 &&
      typeof globalThis.__calqueDemoMutate === "function" &&
      typeof globalThis.__calqueDemoTaskTitles === "function",
    undefined,
    { timeout: 60_000 },
  );
}

/** `id → title`, read out of the rendered table rather than the client. */
async function renderedTasks(
  currentPage: Page,
): Promise<readonly (readonly [string, string])[]> {
  const rows = await currentPage.evaluate(() =>
    Array.from(document.querySelectorAll("#task-rows tr"), (row) => {
      const cells = row.querySelectorAll("td");
      return [
        cells[3]?.querySelector("strong")?.textContent?.trim() ?? "",
        cells[1]?.querySelector("strong")?.textContent?.trim() ?? "",
      ] as [string, string];
    }),
  );
  return rows.toSorted((left, right) => Number(left[0]) - Number(right[0]));
}

function renderedTitleOf(
  rows: readonly (readonly [string, string])[],
  id: bigint,
): string | undefined {
  return rows.find(([rowId]) => rowId === id.toString())?.[1];
}

/** The same shape as `renderedTasks`, straight out of PostgreSQL. */
function committedTasks(): readonly (readonly [string, string])[] {
  return psql("SELECT id, title FROM tasks ORDER BY id")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => {
      const [id, title] = line.split("\u0001");
      assert(
        id !== undefined && title !== undefined,
        `unreadable row: ${line}`,
      );
      return [id, title] as [string, string];
    });
}

function committedTitleOf(id: bigint): string {
  const title = psql(`SELECT title FROM tasks WHERE id = ${id}`);
  return title.replace(/\n$/u, "");
}

function rowCount(id: bigint): number {
  return Number(psql(`SELECT count(*) FROM tasks WHERE id = ${id}`).trim());
}

function receiptCount(): number {
  return Number(
    psql("SELECT count(*) FROM calque_internal.mutation_receipts").trim(),
  );
}
