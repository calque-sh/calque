/**
 * Drives one `pnpm demo` process for a browser harness: start it on chosen
 * ports without opening a browser, read the run URL it announces, stop it the
 * way a developer would (SIGINT, then SIGKILL if it will not leave), and leave
 * the checkout's compose PostgreSQL and `.calque-dev` as the run found them.
 * It also carries the browser, source-change and sample helpers the
 * demo end-to-end harnesses share.
 */
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync, readdirSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { Browser, Page } from "playwright";

import { nearestRank } from "./bench-results.ts";
import { DEVELOPMENT_SOURCE_ROLE, runChecked } from "./demo.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * One publication the manual demo page rendered, as
 * `__calqueDemoPublications` records it. Declared once here because several
 * harnesses read the same array, and TypeScript rejects one ambient global
 * declared again under another local name.
 */
export interface PagePublication {
  readonly sequence: number;
  readonly cursor: string;
  readonly commitTimeUnixMicros: string | null;
  readonly changedCollections: readonly string[];
  readonly transactionCount: number;
  readonly publishedAtEpochMs: number;
  readonly renderedAtEpochMs: number;
  readonly frameCallbackAtEpochMs: number | null;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  // eslint-disable-next-line no-var
  var __calqueDemoPublications: PagePublication[];
}

/** How many publications the page has recorded so far. */
export function publicationCount(page: Page): Promise<number> {
  return page.evaluate(() => globalThis.__calqueDemoPublications.length);
}

/** The browser the harnesses drive, `chromium` unless an operator asks. */
export function demoBrowserChannel(): "chromium" | "chrome" {
  const channel = process.env.CALQUE_DEMO_BROWSER ?? "chromium";
  if (channel !== "chromium" && channel !== "chrome") {
    throw new Error("CALQUE_DEMO_BROWSER must be chromium or chrome");
  }
  return channel;
}

/**
 * Headless Chromium for a demo harness.
 *
 * Playwright is required from the client package's own resolution, not
 * imported statically, so it launches the browser that package installed.
 * `typeof import("playwright")` gives it the real static types anyway.
 */
export function launchDemoBrowser(): Promise<Browser> {
  const requireFromClient = createRequire(
    join(root, "client/packages/client/package.json"),
  );
  const { chromium } = requireFromClient(
    "playwright",
  ) as typeof import("playwright");
  const channel = demoBrowserChannel();
  return chromium.launch({
    headless: true,
    ...(channel === "chrome" ? { channel: "chrome" } : {}),
  });
}

/** One real PostgreSQL transaction against `pnpm demo`'s fixture. */
export function runSourceChange(): void {
  const change = spawnSync(
    "node",
    ["--import", "./scripts/register.mjs", "scripts/demo.ts", "change"],
    { cwd: root, encoding: "utf8", maxBuffer: 4 * 1024 * 1024 },
  );
  if (change.error || change.status !== 0) {
    throw new Error(
      `the source change failed: ${change.error?.message ?? change.stderr ?? change.stdout ?? ""}`,
    );
  }
}

/**
 * One query against the source database, as `--no-align --tuples-only` text
 * with `\u0001` between columns. The connection is the container's own socket
 * through `docker compose exec`, so no connection string is ever an argument.
 * `environment` is the caller's compose environment: each harness runs its own
 * disposable source on its own port.
 */
export function demoPsql(sql: string, environment: NodeJS.ProcessEnv): string {
  const result = spawnSync(
    "docker",
    [
      "compose",
      "exec",
      "-T",
      "postgres",
      "psql",
      "--set",
      "ON_ERROR_STOP=1",
      "--username",
      DEVELOPMENT_SOURCE_ROLE,
      "--dbname",
      "calque_dev",
      "--no-align",
      "--tuples-only",
      "--field-separator",
      "\u0001",
      "--command",
      sql,
    ],
    {
      cwd: root,
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
      env: environment,
      stdio: "pipe",
    },
  );
  if (result.error || result.status !== 0) {
    throw new Error(
      `psql failed: ${result.error?.message ?? result.stderr ?? ""}`,
    );
  }
  return result.stdout ?? "";
}

/**
 * Imports a built package by path, naming the build step when it is missing.
 * `scripts/**` is outside every package's dependency graph, so a harness that
 * needs the real encoder loads what `pnpm -r build` produced.
 */
export async function importBuilt(
  path: string,
  name: string,
): Promise<unknown> {
  try {
    return await import(pathToFileURL(path).href);
  } catch (error) {
    throw new Error(
      `${name} is not built at ${path}; run pnpm -r build first (${String(error)})`,
    );
  }
}

export interface DemoExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

export interface DemoProcess {
  readonly child: ChildProcess;
  readonly exited: Promise<DemoExit>;
  readonly output: () => string;
  readonly url: string;
  /** What `stopDemo` restores, or `undefined` when the caller restores it. */
  readonly found: FoundDemo | undefined;
}

/**
 * Starts `pnpm demo` and returns once it announces its run URL. It first
 * records what it finds, and both `stopDemo` and a start that fails here
 * restore that. `restore: false` leaves recording and restoring to a caller
 * that starts more than one process on the same source and state. Without a
 * `metricsPort` the service runs no metrics listener. A harness checks the
 * working tree, so `calque` is built from it unless `fromSource` is false, which
 * runs the image `CALQUE_DEMO_IMAGE` names, or the published one.
 */
export async function startDemo(
  serverPort: number,
  fixedUiPort: number,
  {
    restore = true,
    metricsPort,
    fromSource = true,
  }: {
    readonly restore?: boolean;
    readonly metricsPort?: number;
    readonly fromSource?: boolean;
  } = {},
): Promise<DemoProcess> {
  const found = restore ? recordDemo() : undefined;
  const child = spawn(
    "node",
    [
      "--import",
      "./scripts/register.mjs",
      "scripts/demo.ts",
      "--no-open",
      ...(fromSource ? ["--from-source"] : []),
    ],
    {
      cwd: root,
      env: {
        ...process.env,
        CALQUE_LOCAL_PORT: String(serverPort),
        CALQUE_DEMO_UI_PORT: String(fixedUiPort),
        ...(metricsPort === undefined
          ? {}
          : { CALQUE_LOCAL_METRICS_PORT: String(metricsPort) }),
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let output = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    output = appendBounded(output, chunk);
  });
  child.stderr.on("data", (chunk: string) => {
    output = appendBounded(output, chunk);
  });
  const exited = new Promise<DemoExit>((resolveExit) => {
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
  });
  try {
    const deadline = Date.now() + 300_000;
    while (Date.now() < deadline) {
      const match = output.match(
        /Open: (http:\/\/127\.0\.0\.1:\d+\/\?run=[A-Za-z0-9_-]+)/u,
      );
      const url = match?.[1];
      if (url) return { child, exited, output: () => output, url, found };
      const state = await Promise.race([
        exited.then(() => "exited"),
        delay(100).then(() => "waiting"),
      ]);
      if (state === "exited") {
        throw new Error(`pnpm demo exited before readiness:\n${output}`);
      }
    }
    child.kill("SIGINT");
    await exited;
    throw new Error(`pnpm demo readiness timed out:\n${output}`);
  } catch (error) {
    if (found) restoreDemo(found);
    throw error;
  }
}

/** Stops the process, then restores what its start recorded, if it did. */
export async function stopDemo(owner: DemoProcess): Promise<void> {
  try {
    if (owner.child.exitCode !== null || owner.child.signalCode !== null)
      return;
    owner.child.kill("SIGINT");
    const result = await exitWithin(owner.exited, 20_000);
    if (!result) {
      owner.child.kill("SIGKILL");
      await owner.exited;
      throw new Error(`pnpm demo did not stop gracefully:\n${owner.output()}`);
    }
    assert.equal(result.code, 0, owner.output());
  } finally {
    if (owner.found) restoreDemo(owner.found);
  }
}

/**
 * What a run found of the checkout's compose `postgres` service and of
 * `.calque-dev`, before `pnpm demo` started anything.
 */
export interface FoundDemo {
  readonly postgres: "absent" | "stopped" | "running";
  /** The entries of `.calque-dev`, or `null` when it did not exist. */
  readonly stateEntries: readonly string[] | null;
}

/** One step of putting the checkout back as a run found it. */
type RestoreStep =
  | { readonly kind: "compose-down-volumes" }
  | { readonly kind: "compose-stop" }
  | { readonly kind: "remove-state-directory" }
  | { readonly kind: "remove-state-entry"; readonly entry: string };

/** The launcher's state, which records the identity of the source cluster. */
const stateDirectory = join(root, ".calque-dev");
/** The file `calque dev` writes there; its presence means the state exists. */
const STATE_MARKER = "state.json";

/**
 * Records what `restoreDemo` puts back. Call it before `pnpm demo`
 * starts. Compose reads the same environment `pnpm demo` does, so a
 * `COMPOSE_PROJECT_NAME` selects the same project for both.
 */
export function recordDemo(): FoundDemo {
  const postgres = composePostgresListed("--status", "running")
    ? "running"
    : composePostgresListed("--all")
      ? "stopped"
      : "absent";
  return { postgres, stateEntries: listStateEntries() };
}

/**
 * The steps that put the checkout back as `found` describes it, given what
 * `.calque-dev` holds now.
 *
 * A source the run created goes down with its volume, one it found stopped is
 * stopped again, and one it found running is left alone. State recorded
 * against a source the run created would only be a future identity mismatch,
 * so it is removed unless the run found state there already, and only the
 * entries the run added, because `pnpm incidents` keeps its own state beneath
 * `.calque-dev`. State the run wrote against a source it found is kept: it
 * records the slot and publication `calque dev` created on that source.
 */
export function restorePlan(
  found: FoundDemo,
  stateEntriesNow: readonly string[] | null,
): readonly RestoreStep[] {
  if (found.postgres === "running") return [];
  if (found.postgres === "stopped") return [{ kind: "compose-stop" }];
  const takeDown: RestoreStep = { kind: "compose-down-volumes" };
  if (stateEntriesNow === null || found.stateEntries?.includes(STATE_MARKER))
    return [takeDown];
  if (found.stateEntries === null)
    return [takeDown, { kind: "remove-state-directory" }];
  const before = new Set(found.stateEntries);
  return [
    takeDown,
    ...stateEntriesNow
      .filter((entry) => !before.has(entry))
      .sort()
      .map((entry) => ({ kind: "remove-state-entry" as const, entry })),
  ];
}

/**
 * Puts the checkout back as `found` describes it. A second call changes
 * nothing.
 */
export function restoreDemo(found: FoundDemo): void {
  for (const step of restorePlan(found, listStateEntries())) {
    switch (step.kind) {
      case "compose-down-volumes":
        runChecked("docker", ["compose", "down", "--volumes"]);
        break;
      case "compose-stop":
        runChecked("docker", ["compose", "stop", "postgres"]);
        break;
      case "remove-state-directory":
        rmSync(stateDirectory, { recursive: true, force: true });
        break;
      case "remove-state-entry":
        rmSync(join(stateDirectory, step.entry), {
          recursive: true,
          force: true,
        });
        break;
    }
  }
}

function listStateEntries(): readonly string[] | null {
  return existsSync(stateDirectory) ? readdirSync(stateDirectory) : null;
}

function composePostgresListed(...filters: string[]): boolean {
  return (
    runChecked("docker", [
      "compose",
      "ps",
      ...filters,
      "--quiet",
      "postgres",
    ]).stdout.trim() !== ""
  );
}

export async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen({ host: "127.0.0.1", port: 0 }, resolveListen);
  });
  const address = server.address();
  assert(address && typeof address === "object");
  const port = address.port;
  await new Promise<void>((resolveClose, rejectClose) =>
    server.close((error) => (error ? rejectClose(error) : resolveClose())),
  );
  return port;
}

export function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

/**
 * The child's exit, or `undefined` once `milliseconds` pass first. The timer
 * is cleared on every path: raced as a bare `delay`, it stays armed after the
 * child exits and holds the event loop open for the rest of the period.
 */
export async function exitWithin<T>(
  exited: Promise<T>,
  milliseconds: number,
): Promise<T | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      exited,
      new Promise<undefined>((resolveTimeout) => {
        timer = setTimeout(() => resolveTimeout(undefined), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export function appendBounded(current: string, chunk: string): string {
  const next = current + chunk;
  return next.length <= 1_000_000 ? next : next.slice(-1_000_000);
}

interface Summary {
  readonly samples: number;
  readonly p50: number;
  readonly p95: number;
  readonly min: number;
  readonly max: number;
  readonly mean: number;
}

export function summarizeSamples(values: readonly number[]): Summary {
  assert(values.length > 0, "cannot summarize an empty sample set");
  const round = (value: number) => Math.round(value * 100) / 100;
  return {
    samples: values.length,
    p50: round(nearestRank(values, 0.5)),
    p95: round(nearestRank(values, 0.95)),
    min: round(Math.min(...values)),
    max: round(Math.max(...values)),
    mean: round(values.reduce((sum, value) => sum + value, 0) / values.length),
  };
}

export function parseSampleCount(value: string, name: string): number {
  if (!/^[1-9][0-9]*$/u.test(value) || Number(value) > 10_000)
    throw new Error(`${name} must be an integer from 1 to 10000`);
  return Number(value);
}
