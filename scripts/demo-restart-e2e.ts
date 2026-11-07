#!/usr/bin/env node
/// <reference lib="dom" />
import assert from "node:assert/strict";
import type { Page, Request } from "playwright";

import {
  demoBrowserChannel,
  availablePort,
  delay,
  launchDemoBrowser,
  type PagePublication,
  recordDemo,
  restoreDemo,
  runSourceChange,
  startDemo,
  stopDemo,
  type DemoProcess,
} from "./demo-process.ts";

const [uiPort, firstServerPort, secondServerPort] = await Promise.all([
  availablePort(),
  availablePort(),
  availablePort(),
]);

const browser = await launchDemoBrowser();
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });

interface FetchTraceEntry {
  url: string;
  method: string;
  status?: number;
  error?: string;
}

type NetworkEvent =
  | {
      event: "failed";
      method: string;
      url: string;
      error: string;
      phase: ScenarioPhase;
    }
  | { event: "response"; method: string; url: string; status: number };

type ScenarioPhase = "before-restart" | "during-restart" | "after-restart";

/**
 * The duplicate-visible-transaction oracle: a restart must not lose or
 * duplicate a visible transaction. Each phase applies its own source
 * transactions and records every publication the page rendered, with the
 * cursor it stood at. A cursor seen twice, a phase whose publications do not
 * account for exactly the transactions it applied, or a post-restart delivery
 * carrying a pre-restart cursor is a duplicate; a phase short of its
 * transactions is a loss. The seed runs again when the replacement starts, so
 * the phases are counted separately rather than summed. The open tab applies
 * its own after the restart, on the plan the first process signed, and the
 * revisit applies the last.
 */
const PRE_RESTART_CHANGES = 2;
const RESUMED_CHANGES = 2;
const POST_RESTART_CHANGES = 2;
const OPEN_TASKS_AT_SEED = 4;

/**
 * The oracle's own reversible fault, the way `pnpm verify:probes` self-tests
 * the gate. With `CALQUE_RESTART_E2E_FAULT=duplicate-delivery` the collector
 * records one publication twice -- what a restart that re-delivered an already
 * visible transaction looks like from here -- and the run must fail on the
 * duplicate assertion rather than pass. Any other value is refused, so a typo
 * cannot quietly turn the self-test off.
 */
const FAULT = process.env.CALQUE_RESTART_E2E_FAULT ?? "none";
if (FAULT !== "none" && FAULT !== "duplicate-delivery")
  throw new Error(
    "CALQUE_RESTART_E2E_FAULT must be unset or duplicate-delivery",
  );

interface PlanIdentity {
  readonly artifactId: string;
  readonly snapshotCursor: string;
  readonly activationCursor: string;
}

const RESOURCE_ERROR_PREFIX = "Failed to load resource: ";
const PERMITTED_RESTART_DISCONNECT_ERRORS: ReadonlySet<string> = new Set([
  // The retry began after shutdown removed the listener.
  "net::ERR_CONNECTION_REFUSED",
  // The long poll connected before shutdown, then lost its peer before a reply.
  "net::ERR_EMPTY_RESPONSE",
  // The established socket was reset while the server process was exiting.
  "net::ERR_CONNECTION_RESET",
  // Chromium aborted an in-flight request as the serving process disappeared.
  "net::ERR_ABORTED",
  // A streamed change response lost its server between two batches.
  "net::ERR_INCOMPLETE_CHUNKED_ENCODING",
]);

declare global {
  // `var` is required here: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  // eslint-disable-next-line no-var
  var __calqueDemoFetchTrace: FetchTraceEntry[];
}

const pageErrors: string[] = [];
const consoleErrors: string[] = [];
const network: NetworkEvent[] = [];
const developmentRuntimeRequests: string[] = [];
const webSockets: string[] = [];
const injectedBootstrapFailures = 3;
let observedInjectedFailures = 0;
let scenarioPhase: ScenarioPhase = "before-restart";
const activeRequests = new Set<Request>();
const requestStartPhases = new WeakMap<Request, ScenarioPhase>();
// A failure may be reported after replacement readiness. Preserve whether its
// request overlapped the outage instead of stamping the callback's later phase.
const restartWindowRequests = new WeakSet<Request>();
// Chromium ends a streamed response as canceled whenever the page's last read
// comes after the network's last byte, although the page received every byte,
// so such an abort of a change or artifact request answered 200 is counted
// apart. A change batch the page abandoned itself ends the mount and fails the
// dashboard checks; an artifact body the network cut is asked for again, which
// the one download per plan checked below refuses.
const STREAMED_BODY_END = "net::ERR_ABORTED";
const answeredStreams = new WeakSet<Request>();
let changeStreamsEndedCanceled = 0;
let artifactBodiesEndedCanceled = 0;
// The revisit navigates away from the open tab's long poll, which Chromium
// ends as aborted without logging it.
const leftByRevisit = new WeakSet<Request>();
let changeStreamsLeftByRevisit = 0;
// The replacement holds the same plan key, so it serves the open tab's plan:
// no change request is refused and no bootstrap follows the restart.
let refusedChangeRequests = 0;
let resumedChangeResponses = 0;
const isChangeRequest = (request: Request): boolean =>
  new URL(request.url()).pathname === "/_calque/v1/changes";
const isArtifactRequest = (request: Request): boolean =>
  new URL(request.url()).pathname.startsWith("/_calque/v1/artifact-objects/");
await page.route("**/v1/bootstrap", async (route) => {
  if (observedInjectedFailures < injectedBootstrapFailures) {
    observedInjectedFailures += 1;
    await route.abort("connectionrefused");
    return;
  }
  await route.continue();
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
  activeRequests.delete(request);
  if (
    request.failure()?.errorText === STREAMED_BODY_END &&
    answeredStreams.has(request)
  ) {
    if (isChangeRequest(request)) changeStreamsEndedCanceled += 1;
    else artifactBodiesEndedCanceled += 1;
    return;
  }
  if (
    request.failure()?.errorText === STREAMED_BODY_END &&
    leftByRevisit.has(request)
  ) {
    changeStreamsLeftByRevisit += 1;
    return;
  }
  network.push({
    event: "failed",
    method: request.method(),
    url: request.url(),
    error: request.failure()?.errorText ?? "unknown",
    phase: restartWindowRequests.has(request)
      ? "during-restart"
      : (requestStartPhases.get(request) ?? scenarioPhase),
  });
});
page.on("request", (request) => {
  activeRequests.add(request);
  requestStartPhases.set(request, scenarioPhase);
  if (scenarioPhase === "during-restart") {
    restartWindowRequests.add(request);
  }
  if (
    request.url().includes("/@vite/") ||
    request.url().includes("vite/client")
  ) {
    developmentRuntimeRequests.push(request.url());
  }
});
page.on("requestfinished", (request) => activeRequests.delete(request));
page.on("websocket", (socket) => webSockets.push(socket.url()));
const servedPlans: PlanIdentity[] = [];
page.on("response", (response) => {
  if (response.url().includes("/v1/") || response.url().includes("/readyz")) {
    network.push({
      event: "response",
      method: response.request().method(),
      url: response.url(),
      status: response.status(),
    });
  }
  if (
    (isChangeRequest(response.request()) ||
      isArtifactRequest(response.request())) &&
    response.status() === 200
  )
    answeredStreams.add(response.request());
  if (
    isChangeRequest(response.request()) &&
    response.status() === 200 &&
    requestStartPhases.get(response.request()) === "after-restart"
  )
    resumedChangeResponses += 1;
  if (isChangeRequest(response.request()) && response.status() !== 200)
    refusedChangeRequests += 1;
  if (
    new URL(response.url()).pathname === "/_calque/v1/bootstrap" &&
    response.status() === 200
  ) {
    // Read on the response's own turn. The plan names the artifact the server
    // is serving, which is how "recovered from durable state" is told apart
    // from "rebuilt the snapshot": a replacement that reuses the artifact did
    // not force a resnapshot on anyone.
    void response
      .json()
      .then((plan: unknown) => servedPlans.push(planIdentity(plan)))
      .catch(() => undefined);
  }
});

// The second process recovers from the source and `.calque-dev` the first
// left, so this run records them once and restores them after both stop.
const found = recordDemo();
let first: DemoProcess | undefined;
let second: DemoProcess | undefined;
try {
  first = await startDemo(firstServerPort, uiPort, { restore: false });
  await page.goto(first.url, { waitUntil: "load", timeout: 30_000 });
  const firstResult = await waitForDashboard(page);
  const beforeRestart = await applyChanges(page, PRE_RESTART_CHANGES);

  scenarioPhase = "during-restart";
  for (const request of activeRequests)
    if (isChangeRequest(request)) restartWindowRequests.add(request);
  await stopDemo(first);
  first = undefined;

  second = await startDemo(secondServerPort, uiPort, { restore: false });
  scenarioPhase = "after-restart";
  assert.notEqual(new URL(firstResult.url).search, new URL(second.url).search);

  // The open tab's change stream retries until the replacement answers, then
  // carries on with the plan it already holds and the rows it already has.
  const resumedBy = Date.now() + 60_000;
  while (resumedChangeResponses === 0) {
    if (Date.now() > resumedBy)
      throw new Error("the open tab's plan was not served by the replacement");
    await delay(100);
  }
  await waitForDashboard(page);
  // The replacement seeded the source again before it served, and the open
  // tab receives that transaction like any other. It commits after every
  // pre-restart change, by the source's own clock, so it is waited for and
  // not counted as one of the open tab's changes.
  const lastBeforeRestart = beforeRestart
    .map((entry) => BigInt(entry.commitTimeUnixMicros ?? "0"))
    .reduce((latest, time) => (time > latest ? time : latest), 0n);
  await page.waitForFunction(
    (after) =>
      globalThis.__calqueDemoPublications.some(
        (publication) =>
          publication.commitTimeUnixMicros !== null &&
          BigInt(publication.commitTimeUnixMicros) > BigInt(after),
      ),
    String(lastBeforeRestart),
    { timeout: 60_000 },
  );
  await delay(250);
  const resumed = await applyChanges(page, RESUMED_CHANGES);
  assert.equal(
    servedPlans.length,
    1,
    "the open tab bootstrapped again after the restart",
  );

  // Then revisit the prior run URL. The static bundle contains no
  // run-specific endpoint or bearer, so it must bind through the current
  // preview proxy and still work.
  for (const request of activeRequests)
    if (isChangeRequest(request)) leftByRevisit.add(request);
  await page.goto(firstResult.url, { waitUntil: "load", timeout: 30_000 });
  await waitForDashboard(page);
  const afterRestart = await applyChanges(page, POST_RESTART_CHANGES);

  // Nothing was delivered twice. Every cursor in the whole run is distinct,
  // and no post-restart delivery carries a cursor that was already visible
  // before the restart.
  const beforeCursors = beforeRestart.map((entry) => entry.cursor);
  const afterCursors = [...resumed, ...afterRestart].map(
    (entry) => entry.cursor,
  );
  const allCursors = [...beforeCursors, ...afterCursors];
  assert.equal(
    new Set(allCursors).size,
    allCursors.length,
    `a cursor was delivered twice: ${JSON.stringify(allCursors)}`,
  );
  const redelivered = afterCursors.filter((cursor) =>
    beforeCursors.includes(cursor),
  );
  assert.deepEqual(
    redelivered,
    [],
    "a transaction visible before the restart was delivered again after it",
  );

  // Nothing was lost either: each phase's publications account for exactly the
  // transactions that phase applied, and the rendered state agrees.
  assert.equal(
    beforeRestart.reduce((sum, entry) => sum + entry.transactionCount, 0),
    PRE_RESTART_CHANGES,
    "the pre-restart publications do not account for the changes applied",
  );
  assert.equal(
    resumed.reduce((sum, entry) => sum + entry.transactionCount, 0),
    RESUMED_CHANGES,
    "the open tab's post-restart publications do not account for the changes applied",
  );
  assert.equal(
    afterRestart.reduce((sum, entry) => sum + entry.transactionCount, 0),
    POST_RESTART_CHANGES,
    "the revisit's publications do not account for the changes applied",
  );
  assert.equal(
    await page.locator("#metric-open").innerText(),
    String(OPEN_TASKS_AT_SEED - ((RESUMED_CHANGES + POST_RESTART_CHANGES) % 2)),
    "the revisited tab rendered the wrong open-task count",
  );

  // And no resnapshot was forced where the durable state made one
  // unnecessary: the revisit's plan names the same published artifact, at
  // the same snapshot cursor, and only its activation cursor moved forward
  // over the transactions committed in between.
  assert.equal(
    servedPlans.length,
    2,
    "expected one plan for the first load and one for the revisit",
  );
  assert.equal(
    refusedChangeRequests,
    0,
    "the replacement refused a change request",
  );
  assert.equal(
    network.filter(
      (event) =>
        event.event === "response" &&
        new URL(event.url).pathname.startsWith("/_calque/v1/artifact-objects/"),
    ).length,
    servedPlans.length,
    "a load downloaded its artifact more than once",
  );
  const [firstPlan, revisitPlan] = servedPlans as [PlanIdentity, PlanIdentity];
  assert.equal(
    revisitPlan.artifactId,
    firstPlan.artifactId,
    "the replacement rebuilt the snapshot instead of recovering it",
  );
  assert.equal(
    revisitPlan.snapshotCursor,
    firstPlan.snapshotCursor,
    "the replacement moved the artifact's snapshot cursor",
  );
  assert.notEqual(
    revisitPlan.activationCursor,
    firstPlan.activationCursor,
    "the replacement did not advance over the transactions it recovered",
  );

  assert.deepEqual(pageErrors, []);
  assert.deepEqual(developmentRuntimeRequests, []);
  assert.deepEqual(webSockets, []);
  const failedRequests = network.filter(
    (event): event is Extract<NetworkEvent, { event: "failed" }> =>
      event.event === "failed",
  );
  const injectedBootstrapRefusals = failedRequests.filter(
    (event) =>
      event.error === "net::ERR_CONNECTION_REFUSED" &&
      new URL(event.url).pathname === "/_calque/v1/bootstrap" &&
      event.phase === "before-restart",
  );
  const restartChangeFailures = failedRequests.filter(
    (event) =>
      new URL(event.url).pathname === "/_calque/v1/changes" &&
      event.phase === "during-restart" &&
      PERMITTED_RESTART_DISCONNECT_ERRORS.has(event.error),
  );
  assert.equal(injectedBootstrapRefusals.length, injectedBootstrapFailures);
  assert.equal(
    failedRequests.length,
    injectedBootstrapRefusals.length + restartChangeFailures.length,
  );
  assert.deepEqual(
    failedRequests.filter((event) => event.phase === "after-restart"),
    [],
  );
  const consoleTransportErrors = consoleErrors.map((message) => {
    assert(
      message.startsWith(RESOURCE_ERROR_PREFIX),
      `unexpected browser console error: ${message}`,
    );
    return message.slice(RESOURCE_ERROR_PREFIX.length);
  });
  assert.deepEqual(
    consoleTransportErrors.toSorted(),
    failedRequests.map((event) => event.error).toSorted(),
  );
  process.stdout.write(
    `${JSON.stringify({
      outcome: "passed",
      scenario: "browser-tab-survives-demo-restart",
      browser: demoBrowserChannel(),
      uiPort,
      firstServerPort,
      secondServerPort,
      runUrlChanged: true,
      injectedBootstrapFailures: observedInjectedFailures,
      restartRetryFailures: restartChangeFailures.length,
      changeStreamsEndedCanceledExempted: changeStreamsEndedCanceled,
      artifactBodiesEndedCanceledExempted: artifactBodiesEndedCanceled,
      changeStreamsLeftByRevisit,
      refusedChangeRequests,
      bootstrapsAfterRestart: servedPlans.length - 1,
      transactionsBeforeRestart: PRE_RESTART_CHANGES,
      transactionsOnTheOpenTabAfterRestart: RESUMED_CHANGES,
      transactionsAfterRevisit: POST_RESTART_CHANGES,
      publicationsBeforeRestart: beforeRestart.length,
      publicationsOnTheOpenTabAfterRestart: resumed.length,
      publicationsAfterRevisit: afterRestart.length,
      duplicateCursors: allCursors.length - new Set(allCursors).size,
      redeliveredAfterRestart: redelivered.length,
      pageErrors: 0,
      unexpectedConsoleErrors: 0,
    })}\n`,
  );
} catch (error) {
  process.stderr.write(
    `Demo restart E2E failed: ${JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
      url: page.url(),
      status: await page
        .locator("#status")
        .getAttribute("data-state")
        .catch(() => null),
      result: await page
        .locator("#result")
        .innerText()
        .catch(() => null),
      pageErrors,
      consoleErrors,
      developmentRuntimeRequests,
      webSockets,
      network,
      servedPlans,
      fetchTrace: await page
        .evaluate(() => globalThis.__calqueDemoFetchTrace)
        .catch(() => null),
      firstLauncher: first?.output(),
      secondLauncher: second?.output(),
    })}\n`,
  );
  throw error;
} finally {
  try {
    if (second) await stopDemo(second);
    if (first) await stopDemo(first);
  } finally {
    restoreDemo(found);
  }
  await browser.close();
}

/**
 * Applies `count` source transactions one at a time and returns the
 * publications the page rendered for them, in order. One at a time, because a
 * batch that carried two transactions and a batch that carried one twice look
 * alike in a total.
 */
async function applyChanges(
  currentPage: Page,
  count: number,
): Promise<readonly PagePublication[]> {
  const collected: PagePublication[] = [];
  for (let applied = 0; applied < count; applied += 1) {
    const before: number = await currentPage.evaluate(
      () => globalThis.__calqueDemoPublications.length,
    );
    runSourceChange();
    await currentPage.waitForFunction(
      (seen) => globalThis.__calqueDemoPublications.length > seen,
      before,
      { timeout: 60_000 },
    );
    // A settle window, so a second publication carrying the same transaction
    // is collected rather than missed.
    await delay(250);
    const next: readonly PagePublication[] = await currentPage.evaluate(
      (seen) => globalThis.__calqueDemoPublications.slice(seen),
      before,
    );
    collected.push(...next);
    const first = next[0];
    if (FAULT === "duplicate-delivery" && first) collected.push(first);
  }
  return collected;
}

function planIdentity(plan: unknown): PlanIdentity {
  const value = plan as {
    activationCursor?: unknown;
    artifacts?: { artifactId?: unknown; snapshotCursor?: unknown }[];
  };
  const artifact = value.artifacts?.[0];
  return {
    artifactId: String(artifact?.artifactId ?? ""),
    snapshotCursor: String(artifact?.snapshotCursor ?? ""),
    activationCursor: String(value.activationCursor ?? ""),
  };
}

async function waitForDashboard(currentPage: Page): Promise<{ url: string }> {
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
  return { url: currentPage.url() };
}
