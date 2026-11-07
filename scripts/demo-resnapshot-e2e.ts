#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Drives a resnapshot end to end in a real browser against the real demo: a
 * client the source can no longer catch up must rebuild from a fresh snapshot
 * without reloading the page.
 *
 * The only fabricated response is the 410 `CURSOR_BEFORE_RETENTION` refusal
 * with `resnapshotRequired`, framed by the built `@calque/protocol` encoder,
 * because driving a real source past its retention floor takes minutes of WAL
 * and a checkpoint. Everything after it comes from the real server. A
 * sentinel in the page's JavaScript context must survive, no main-frame
 * navigation may happen, and an observer on `#status` must record no state but
 * `ready`. `main.ts` writes `error` and `closed` there only after its first
 * go-live, so the list shows what the page displayed, not the client's own
 * `SyncStatus`, which does pass through `initializing`.
 */
import assert from "node:assert/strict";
import { dirname, join, resolve } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type { Page } from "playwright";

import {
  demoBrowserChannel,
  availablePort,
  delay,
  importBuilt,
  launchDemoBrowser,
  publicationCount,
  runSourceChange,
  startDemo,
  stopDemo,
  type DemoProcess,
} from "./demo-process.ts";
import { runChecked } from "./demo.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const protocolRoot = join(root, "client/packages/protocol");

const CHANGE_ROUTE = "**/_calque/v1/changes";

/** Just enough of the two modules this harness loads by path to encode a frame. */
interface WireSchema {
  readonly typeName: string;
}
type WireMessage = Record<string, unknown>;
interface ProtobufRuntime {
  create(schema: WireSchema, init?: WireMessage): WireMessage;
}
interface ProtocolModule {
  readonly change: {
    readonly ChangeStreamFrameSchema: WireSchema;
    readonly StreamErrorSchema: WireSchema;
    readonly StreamErrorCode: { readonly CURSOR_BEFORE_RETENTION: number };
  };
  changeFrameBodySha256(frame: WireMessage): Promise<Uint8Array>;
  encodeFrame(schema: WireSchema, frame: WireMessage): Uint8Array;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  // `__calqueDemoPublications` is declared in `demo-process.ts`.
  // eslint-disable-next-line no-var
  var __calqueResnapshotSentinel: string | undefined;
  // eslint-disable-next-line no-var
  var __calqueResnapshotStates: string[] | undefined;
}

// Built before the import below, so it never reads a stale `dist/`.
runChecked("pnpm", ["--filter", "@calque/client...", "build"]);
/**
 * The wire encoder, loaded from the built `@calque/protocol` so the fabricated
 * frame is the tree's protocol rather than this harness's idea of it.
 */
const protocol = (await importBuilt(
  join(protocolRoot, "dist/index.js"),
  "@calque/protocol",
)) as ProtocolModule;
const { create } = (await importBuilt(
  createRequire(join(protocolRoot, "package.json")).resolve(
    "@bufbuild/protobuf",
  ),
  "@bufbuild/protobuf",
)) as ProtobufRuntime;

const [uiPort, serverPort] = await Promise.all([
  availablePort(),
  availablePort(),
]);
const browser = await launchDemoBrowser();
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });

const pageErrors: string[] = [];
const navigations: string[] = [];
const bootstrapStatuses: number[] = [];
const changeStatuses: number[] = [];
let injected = 0;

page.on("pageerror", (error) => pageErrors.push(String(error)));
page.on("framenavigated", (frame) => {
  if (frame === page.mainFrame()) navigations.push(frame.url());
});
page.on("response", (response) => {
  const path = new URL(response.url()).pathname;
  if (path === "/_calque/v1/bootstrap")
    bootstrapStatuses.push(response.status());
  if (path === "/_calque/v1/changes") changeStatuses.push(response.status());
});

const refusal = await retentionRefusalFrame();
const refuseOnce = async (route: {
  fulfill(options: {
    status: number;
    contentType: string;
    body: Buffer;
  }): Promise<void>;
  continue(): Promise<void>;
}): Promise<void> => {
  // Only the first refusal is fabricated; everything after it is the real
  // server's answer.
  if (injected > 0) {
    await route.continue();
    return;
  }
  injected += 1;
  await route.fulfill({
    status: 410,
    contentType: "application/vnd.calque.change.v1+protobuf",
    body: refusal,
  });
};

let demo: DemoProcess | undefined;
try {
  demo = await startDemo(serverPort, uiPort);
  await page.goto(demo.url, { waitUntil: "load", timeout: 30_000 });
  await waitForDashboard(page);
  const navigationsAtLoad = navigations.length;
  assert.equal(
    bootstrapStatuses.length,
    1,
    "the first dashboard must come from exactly one bootstrap plan",
  );

  // Written into the page's own JavaScript context, not into storage, so only
  // a navigation can remove it.
  const sentinel = `resnapshot-${Date.now()}`;
  await page.evaluate((value) => {
    globalThis.__calqueResnapshotSentinel = value;
  }, sentinel);
  // Every `#status` transition from here on. `main.ts` writes `data-state` on
  // one element it never replaces, so an attribute observer on it sees every
  // write. The records' `oldValue`s are kept too: a callback is delivered once
  // per microtask checkpoint, so an error state written and reverted inside one
  // turn shows only in the records, not in the element.
  await page.evaluate(() => {
    const element = document.querySelector("#status");
    if (!element) throw new Error("the dashboard has no #status element");
    const states: string[] = [];
    globalThis.__calqueResnapshotStates = states;
    const push = (state: string): void => {
      if (states.at(-1) !== state) states.push(state);
    };
    push(element.getAttribute("data-state") ?? "");
    new MutationObserver((records) => {
      for (const record of records) push(record.oldValue ?? "");
      push(element.getAttribute("data-state") ?? "");
    }).observe(element, {
      attributes: true,
      attributeFilter: ["data-state"],
      attributeOldValue: true,
    });
  });
  const publicationsBeforeRefusal = await publicationCount(page);

  // Arm, then make one real source change so the long poll in flight answers
  // now rather than at its timeout. The refusal meets the poll after it.
  await page.route(CHANGE_ROUTE, refuseOnce);
  runSourceChange();
  await page.waitForFunction(
    (count) => globalThis.__calqueDemoPublications.length > count,
    publicationsBeforeRefusal,
    { timeout: 60_000 },
  );

  // The rebuild: a replacement plan, fetched by the page itself, with no
  // navigation between the two.
  const deadline = Date.now() + 120_000;
  while (bootstrapStatuses.length < 2 && Date.now() < deadline)
    await delay(100);
  await page.unroute(CHANGE_ROUTE, refuseOnce);
  assert.equal(injected, 1, "the refusal was never delivered");
  assert.equal(
    changeStatuses.filter((status) => status === 410).length,
    1,
    "exactly one refusal must have reached the client",
  );
  assert.equal(
    bootstrapStatuses.length,
    2,
    "the client did not ask for a replacement plan after the refusal",
  );
  assert.equal(
    bootstrapStatuses.at(-1),
    200,
    "the replacement plan was refused",
  );

  // Recovered in place. The page was never reloaded, and never showed the
  // failure.
  assert.equal(
    await page.evaluate(() => globalThis.__calqueResnapshotSentinel),
    sentinel,
    "the page's JavaScript context did not survive the rebuild",
  );
  assert.equal(
    navigations.length,
    navigationsAtLoad,
    "the page navigated during the rebuild",
  );
  const statusStates = await statusTransitions(page);
  assert.deepEqual(
    statusStates,
    ["ready"],
    `the dashboard's status left ready during the rebuild: ${statusStates.join(" -> ")}`,
  );
  await waitForDashboard(page);

  // And live again against the real server, not merely still rendered: a
  // second source change reaches the rebuilt mount and moves the dashboard.
  const publicationsAfterRebuild = await publicationCount(page);
  runSourceChange();
  await page.waitForFunction(
    (count) => globalThis.__calqueDemoPublications.length > count,
    publicationsAfterRebuild,
    { timeout: 60_000 },
  );
  await waitForDashboard(page);
  assert.deepEqual(pageErrors, []);

  process.stdout.write(
    `${JSON.stringify({
      outcome: "passed",
      scenario: "page-rebuilds-itself-after-a-resnapshot-required-refusal",
      browser: demoBrowserChannel(),
      uiPort,
      serverPort,
      injectedRefusals: injected,
      bootstrapPlans: bootstrapStatuses.length,
      mainFrameNavigations: navigations.length - navigationsAtLoad,
      publicationsBeforeRefusal,
      publicationsAfterRebuild,
      publicationsAtEnd: await publicationCount(page),
      sentinelSurvived: true,
      statusStates: await statusTransitions(page),
      pageErrors: 0,
    })}\n`,
  );
} catch (error) {
  process.stderr.write(
    `Demo resnapshot E2E failed: ${JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
      url: page.url(),
      status: await page
        .locator("#status")
        .getAttribute("data-state")
        .catch(() => null),
      statusStates: await statusTransitions(page).catch(() => null),
      notice: await page
        .locator("#notice")
        .innerText()
        .catch(() => null),
      injected,
      bootstrapStatuses,
      changeStatuses,
      navigations,
      pageErrors,
      launcher: demo?.output(),
    })}\n`,
  );
  throw error;
} finally {
  if (demo) await stopDemo(demo);
  await browser.close();
}

/** The refusal `change-http` sends when a cursor is before the retention floor. */
async function retentionRefusalFrame(): Promise<Buffer> {
  const { change, changeFrameBodySha256, encodeFrame } = protocol;
  const frame = create(change.ChangeStreamFrameSchema, {
    protocolVersion: 1,
    body: {
      case: "error",
      value: create(change.StreamErrorSchema, {
        code: change.StreamErrorCode.CURSOR_BEFORE_RETENTION,
        message: "change cursor is before the retention floor",
        resnapshotRequired: true,
      }),
    },
  });
  frame.bodySha256 = await changeFrameBodySha256(frame);
  return Buffer.from(encodeFrame(change.ChangeStreamFrameSchema, frame));
}

/**
 * The `#status` transitions the observer has recorded so far, oldest first and
 * with repeats collapsed. `["ready"]` means the element was never written a
 * different state, not that nobody looked.
 */
function statusTransitions(current: Page): Promise<string[]> {
  return current.evaluate(() => {
    const states = globalThis.__calqueResnapshotStates;
    if (!states)
      throw new Error("the status recorder was never installed on this page");
    return [...states];
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
    { timeout: 120_000 },
  );
}
