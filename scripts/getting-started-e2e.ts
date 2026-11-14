#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Runs the walk in docs/getting-started.md the way a reader does, on a fresh
 * PostgreSQL 18, then drives the page it ends on in headless Chromium.
 *
 * The reader's database is a stock `postgres:18` container on
 * 127.0.0.1:5432 with an existing `tasks` table that an application role owns.
 * The reader's app is a directory the walk fills from the document's own
 * `title="..."` blocks (`index.html`, `vite.config.ts`, `main.ts`), with the
 * workspace's installed packages standing in for `npm install`, which the
 * walk replaces with a function that records what it was asked to install.
 *
 * Every `sh` block before `## Production` runs in order from the app's root,
 * in one bash under `set -euo pipefail`, with no `CALQUE_*` variable
 * inherited, except the sections for the container and the three blocks that
 * end the document: `calque dev` and `npx vite`, which the walk starts in the
 * background, and the INSERT, which it runs once the page shows the seeded
 * row. The document leaves restarting PostgreSQL to the reader after its
 * `ALTER SYSTEM SET wal_level` block, so the walk restarts the container
 * there. The page has to paint the seeded row and the INSERT without a reload
 * or a failed request.
 *
 * Usage: `pnpm getting-started:e2e [--doc <path>] [--dev-port <port>]
 * [--postgres-port <port>]`. `--dev-port` adds `--port` to the `calque dev`
 * block, and `--postgres-port` replaces 5432 in every block, for a host where
 * something else holds 8787 or 5432. Requires Docker, `psql`, the Rust toolchain
 * and libpq, pnpm with the workspace installed and built, and Playwright's
 * Chromium.
 */
import assert from "node:assert/strict";
import type { ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { delimiter, join, resolve } from "node:path";
import process from "node:process";

import { expectTasks, watchRequests } from "./lib/doc-walk-browser.ts";
import { docFiles, docShellBlocks } from "./lib/doc-walk.ts";
import {
  listening,
  logTail,
  readerEnvironment,
  root,
  run,
  runBlocks,
  startBlock,
  stopGroup,
  succeed,
} from "./lib/doc-walk-run.ts";
import {
  CARGO_TARGET_ARGUMENTS,
  CARGO_TARGET_DIRECTORY,
} from "./lib/cargo-target.ts";

const STOP = "## Production";
const SKIPPED_SECTIONS = ["### With the container"] as const;
const DEV_START = [
  "calque dev --table public.tasks \\",
  "  --public-base-url http://127.0.0.1:5173/_calque/ \\",
  "  --acknowledge-disposable-source",
].join("\n");
const PAGE_START = "npx vite";
const CHANGE_BLOCK = `psql "$CALQUE_DATABASE_URL" -c "INSERT INTO tasks VALUES (2, 'Watch it arrive', false)"`;
const WAL_LEVEL_BLOCK = `psql "$ADMIN_URL" -c "ALTER SYSTEM SET wal_level = 'logical'"`;
const DEFAULT_DEV_PORT = 8787;
const DEFAULT_POSTGRES_PORT = 5432;
const PAGE_ORIGIN = "http://127.0.0.1:5173";
const PAGE_PORT = 5173;
const ROLE_DOC = "docs/postgresql.md";
const WALK_TIMEOUT_MS = 20 * 60_000;
const START_TIMEOUT_MS = 90_000;
const WAIT_FOR_POSTGRES_MS = 60_000;

const runId = randomUUID().replaceAll("-", "").slice(0, 12);
const workDirectory = join(root, "target/getting-started-e2e", runId);
const appDirectory = join(workDirectory, "app");
const binDirectory = join(workDirectory, "bin");
const container = `calque-getting-started-${runId}`;

/** The reader's existing database: a table an application role owns. */
const SEED = `
CREATE ROLE app_owner LOGIN PASSWORD 'app_owner';
GRANT CREATE, USAGE ON SCHEMA public TO app_owner;
SET ROLE app_owner;
CREATE TABLE tasks (id bigint PRIMARY KEY, title text NOT NULL, done boolean NOT NULL DEFAULT false);
INSERT INTO tasks VALUES (1, 'Open the page', false);
`;

interface RunOutcome {
  readonly result: "PASS";
  readonly doc: string;
  readonly blocks: number;
  readonly walkSeconds: number;
  readonly installRequests: readonly string[];
  readonly pageSteps: readonly string[];
  readonly streamedReadsEndedCanceled: number;
}

let walk: ChildProcess | undefined;
let dev: ChildProcess | undefined;
let vite: ChildProcess | undefined;
let browser: import("playwright").Browser | undefined;
let postgresStarted = false;

function docker(...args: string[]): string {
  return succeed("docker", args);
}

function psqlInContainer(sql: string): string {
  return succeed("docker", [
    "exec",
    "-e",
    "PGPASSWORD=postgres",
    container,
    "psql",
    "-h",
    "127.0.0.1",
    "-U",
    "postgres",
    "-d",
    "app",
    "-v",
    "ON_ERROR_STOP=1",
    "-tA",
    "-c",
    sql,
  ]).trim();
}

async function until(
  what: string,
  timeoutMs: number,
  check: () => Promise<boolean> | boolean,
  failure?: () => string | undefined,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const stopped = failure?.();
    if (stopped !== undefined) throw new Error(`${what}: ${stopped}`);
    try {
      if (await check()) return;
    } catch {
      // Not ready yet.
    }
    await new Promise((done) => setTimeout(done, 250));
  }
  throw new Error(`${what} did not happen within ${String(timeoutMs)} ms`);
}

async function startPostgres(port: number): Promise<void> {
  docker(
    "run",
    "--detach",
    "--name",
    container,
    "--publish",
    `127.0.0.1:${String(port)}:5432`,
    "--env",
    "POSTGRES_PASSWORD=postgres",
    "--env",
    "POSTGRES_DB=app",
    "postgres:18",
  );
  postgresStarted = true;
  // The image's init server listens on its socket only; TCP means it is the
  // server a reader's tools reach.
  await until(
    "PostgreSQL accepting TCP connections",
    WAIT_FOR_POSTGRES_MS,
    () => psqlInContainer("SELECT 1") === "1",
  );
  assert.match(
    psqlInContainer("SHOW server_version"),
    /^18\./u,
    "the walk's database is PostgreSQL 18",
  );
  assert.equal(
    psqlInContainer("SHOW wal_level"),
    "replica",
    "a stock server starts below logical, so the guide's fix is exercised",
  );
  psqlInContainer(SEED);
}

/** What a reader does between two blocks: restart PostgreSQL by hand. */
function restartPostgres(): string {
  return [
    "# the walk, not the document: restart PostgreSQL as the guide tells the reader to",
    `docker restart ${container} >/dev/null`,
    `until psql "$ADMIN_URL" -c 'SELECT 1' >/dev/null 2>&1; do sleep 1; done`,
  ].join("\n");
}

/** `npm install` here only records its request: the workspace has the packages. */
function npmShim(record: string): string {
  return `npm() { printf '%s\\n' "$*" >> '${record}'; }`;
}

function writeApp(files: ReadonlyMap<string, string>): void {
  mkdirSync(appDirectory, { recursive: true });
  writeFileSync(
    join(appDirectory, "package.json"),
    `${JSON.stringify({ name: "your-app", private: true, type: "module" })}\n`,
  );
  // The workspace's example already resolves vite, @calque/client and
  // @calque/config; `npm install` would put them in this directory.
  symlinkSync(
    join(root, "client/examples/quickstart/node_modules"),
    join(appDirectory, "node_modules"),
  );
  for (const path of ["index.html", "vite.config.ts", "main.ts"]) {
    const text = files.get(path);
    if (text === undefined)
      throw new Error(`the document has no block titled "${path}"`);
    writeFileSync(join(appDirectory, path), text);
  }
}

function checkInstalled(record: string): string[] {
  const requests = readFileSync(record, "utf8").split("\n").filter(Boolean);
  const packages = requests.flatMap((request) =>
    request.split(" ").filter((word) => word.startsWith("@calque/")),
  );
  assert.ok(packages.length > 0, "the document installs no @calque package");
  for (const name of packages)
    assert.ok(
      existsSync(join(appDirectory, "node_modules", name, "package.json")),
      `the document installs ${name}, which the workspace does not provide`,
    );
  return requests;
}

/** What the document says must match what the packages declare. */
function checkDeclaredRequirements(markdown: string): void {
  const peers = (path: string) =>
    (
      JSON.parse(readFileSync(join(root, path), "utf8")) as {
        peerDependencies: Record<string, string>;
      }
    ).peerDependencies;
  assert.equal(peers("client/packages/client/package.json").vite, "^8.1.4");
  assert.equal(peers("client/packages/react/package.json").react, ">=19.0.0");
  assert.ok(markdown.includes("Vite 8.1.4 or newer"), "the Vite requirement");
  assert.ok(markdown.includes("React 19 or newer"), "the React requirement");
  const grammar = "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$";
  assert.ok(markdown.includes(grammar), "the table-name rule");
  for (const path of [
    "client/packages/config/src/compiler.ts",
    "server/apps/calque/src/init.rs",
  ])
    assert.ok(
      readFileSync(join(root, path), "utf8").includes(grammar),
      `${path} no longer holds the table-name rule the document states`,
    );
}

/** The role SQL is written twice, here and in the reference; keep one text. */
function checkRoleSqlMatchesReference(blocks: readonly string[]): void {
  const role = blocks.find((block) =>
    block.includes("CREATE ROLE calque_sync"),
  );
  assert.ok(role !== undefined, "the document creates no calque_sync role");
  const statements = role
    .split("\n")
    .filter((line) => /^(?:CREATE ROLE|GRANT) /u.test(line));
  const reference = readFileSync(join(root, ROLE_DOC), "utf8");
  for (const statement of statements)
    assert.ok(
      reference.includes(statement),
      `${ROLE_DOC} lacks the statement "${statement}"`,
    );
}

/** The three blocks that end the document, with `calque dev`'s port set. */
function endingBlocks(
  blocks: readonly string[],
  devStart: string,
): { setup: string[]; dev: string; page: string; change: string } {
  const [dev, page, change] = blocks.slice(-3);
  if (dev !== DEV_START || page !== PAGE_START || change !== CHANGE_BLOCK)
    throw new Error(
      `the last three blocks must be\n${DEV_START}\n${PAGE_START}\n${CHANGE_BLOCK}\nthe document ends with\n${[dev, page, change].join("\n")}`,
    );
  return {
    setup: blocks.slice(0, -3),
    dev: devStart,
    page: PAGE_START,
    change: CHANGE_BLOCK,
  };
}

interface Ports {
  readonly dev: number;
  readonly postgres: number;
}

async function main(
  doc: string,
  { dev: devPort, postgres: postgresPort }: Ports,
): Promise<RunOutcome> {
  const markdown = readFileSync(doc, "utf8");
  const blocks = docShellBlocks(markdown, STOP, {
    skipSections: SKIPPED_SECTIONS,
  });
  const files = docFiles(markdown, STOP);
  process.stdout.write(
    `${doc}: ${String(blocks.length)} sh blocks before "${STOP}"\n`,
  );
  for (const [index, block] of blocks.entries())
    process.stdout.write(`--- block ${String(index + 1)}\n${block}\n`);

  const {
    setup,
    dev: devBlock,
    page: pageBlock,
    change: changeBlock,
  } = endingBlocks(
    blocks,
    devPort === DEFAULT_DEV_PORT
      ? DEV_START
      : DEV_START.replace(
          "calque dev ",
          `calque dev --port ${String(devPort)} `,
        ),
  );
  const walled = setup.flatMap((block) =>
    block === WAL_LEVEL_BLOCK
      ? [block, restartPostgres()]
      : [
          block.replaceAll(
            `127.0.0.1:${String(DEFAULT_POSTGRES_PORT)}`,
            `127.0.0.1:${String(postgresPort)}`,
          ),
        ],
  );
  assert.equal(
    walled.length,
    setup.length + 1,
    `the walk needs exactly one block reading ${WAL_LEVEL_BLOCK}`,
  );
  checkDeclaredRequirements(markdown);
  checkRoleSqlMatchesReference(blocks);

  const refusals: string[] = [];
  for (const port of [postgresPort, devPort, PAGE_PORT])
    if (await listening(port))
      refusals.push(`127.0.0.1:${String(port)} is already taken`);
  if (run("psql", ["--version"]).status !== 0)
    refusals.push("psql is not on PATH");
  for (const dist of [
    "client/packages/client/dist/vite.js",
    "client/packages/config/dist/index.js",
  ])
    if (!existsSync(join(root, dist)))
      refusals.push(`${dist} is missing; run pnpm install && pnpm -r build`);
  if (refusals.length > 0)
    throw new Error(`refusing to start:\n  ${refusals.join("\n  ")}`);
  const gitBefore = succeed("git", ["status", "--porcelain"]);

  mkdirSync(workDirectory, { recursive: true });
  succeed("cargo", ["build", "--package", "calque", ...CARGO_TARGET_ARGUMENTS]);
  mkdirSync(binDirectory);
  symlinkSync(
    join(CARGO_TARGET_DIRECTORY, "debug/calque"),
    join(binDirectory, "calque"),
  );
  writeApp(files);
  await startPostgres(postgresPort);

  const environment = {
    ...readerEnvironment(["ADMIN_URL"]),
    PATH: `${binDirectory}${delimiter}${process.env.PATH ?? ""}`,
  };
  const record = join(workDirectory, "npm-requests.txt");
  writeFileSync(record, "");
  const started = Date.now();
  const exported = await runBlocks({
    blocks: walled,
    scriptPath: join(workDirectory, "walk.sh"),
    cwd: appDirectory,
    env: environment,
    timeoutMs: WALK_TIMEOUT_MS,
    preamble: [npmShim(record)],
    exported: (name) => name.startsWith("CALQUE_") || name === "ADMIN_URL",
    track: (child) => {
      walk = child;
    },
  });
  const walkSeconds = Math.round((Date.now() - started) / 1000);
  assert.equal(
    psqlInContainer("SHOW wal_level"),
    "logical",
    "the guide's fix left wal_level below logical",
  );
  const installRequests = checkInstalled(record);

  const served = { ...environment, ...exported };
  dev = startBlock({
    block: devBlock,
    env: served,
    cwd: appDirectory,
    logFile: join(workDirectory, "dev.log"),
  });
  await until(
    "calque dev answering /readyz",
    START_TIMEOUT_MS,
    async () => (await fetch(`http://127.0.0.1:${String(devPort)}/readyz`)).ok,
    () =>
      dev?.exitCode === null
        ? undefined
        : `exited ${String(dev?.exitCode)}:\n${logTail(join(workDirectory, "dev.log"))}`,
  );
  vite = startBlock({
    block: pageBlock,
    env: served,
    cwd: appDirectory,
    logFile: join(workDirectory, "vite.log"),
  });
  await until(
    "the page answering",
    START_TIMEOUT_MS,
    async () => {
      const response = await fetch(`${PAGE_ORIGIN}/`);
      return response.ok && (await response.text()).includes('id="tasks"');
    },
    () =>
      vite?.exitCode === null
        ? undefined
        : `exited ${String(vite?.exitCode)}:\n${logTail(join(workDirectory, "vite.log"))}`,
  );

  const { chromium } = createRequire(
    join(root, "client/packages/client/package.json"),
  )("playwright") as typeof import("playwright");
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const requests = watchRequests(context);
  const opened = await context.newPage();
  const pageErrors: string[] = [];
  opened.on("pageerror", (error) => pageErrors.push(String(error)));
  await opened.goto(PAGE_ORIGIN, { waitUntil: "load", timeout: 60_000 });

  const steps = ["the existing row paints", "an INSERT from psql arrives"];
  await expectTasks(opened, steps[0] ?? "", ["open: Open the page"]);
  await runBlocks({
    blocks: [changeBlock],
    scriptPath: join(workDirectory, "change.sh"),
    cwd: appDirectory,
    env: served,
    timeoutMs: 60_000,
    exported: () => false,
  });
  await expectTasks(opened, steps[1] ?? "", [
    "open: Open the page",
    "open: Watch it arrive",
  ]);
  assert.deepEqual(pageErrors, [], "the page threw");
  assert.deepEqual(requests.failedRequests, [], "the page had failed requests");
  assert.equal(
    requests.bootstrapRequests(),
    1,
    "the page did not ask for its plan exactly once",
  );
  assert.equal(
    succeed("git", ["status", "--porcelain"]),
    gitBefore,
    "the walk changed the checkout",
  );
  return {
    result: "PASS",
    doc,
    blocks: blocks.length,
    walkSeconds,
    installRequests,
    pageSteps: steps,
    streamedReadsEndedCanceled: requests.streamedReadsEndedCanceled(),
  };
}

/** Tears down what the run started and returns what is left behind. */
async function teardown(): Promise<string[]> {
  await browser?.close().catch(() => undefined);
  await stopGroup(vite);
  await stopGroup(dev);
  await stopGroup(walk);
  if (postgresStarted) run("docker", ["rm", "--force", container]);
  const left: string[] = [];
  const containers = run("docker", [
    "ps",
    "--all",
    "--quiet",
    "--filter",
    `name=${container}`,
  ]).stdout.trim();
  if (containers !== "") left.push(`container ${container}`);
  for (const port of [PAGE_PORT])
    if (await listening(port))
      left.push(`a listener on 127.0.0.1:${String(port)}`);
  return left;
}

let finishing: Promise<never> | undefined;

function finish(status: number): Promise<never> {
  finishing ??= (async () => {
    let left: string[];
    try {
      left = await teardown();
    } catch (error) {
      left = [`teardown failed: ${String(error)}`];
    }
    process.stderr.write(
      left.length === 0
        ? "getting-started-e2e: left behind: nothing\n"
        : `getting-started-e2e: left behind:\n  ${left.join("\n  ")}\n`,
    );
    if (status === 0 && left.length === 0)
      rmSync(workDirectory, { recursive: true, force: true });
    else if (existsSync(workDirectory))
      process.stderr.write(`getting-started-e2e: kept ${workDirectory}\n`);
    process.exit(left.length > 0 && status === 0 ? 1 : status);
  })();
  return finishing;
}

for (const [signal, status] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
] as const)
  process.on(signal, () => {
    process.stderr.write(`getting-started-e2e: ${signal}; tearing down\n`);
    void finish(status);
  });

function options(argv: readonly string[]): { doc: string; ports: Ports } {
  const args = argv[0] === "--" ? argv.slice(1) : argv;
  let doc = join(root, "docs", "getting-started.md");
  let dev = DEFAULT_DEV_PORT;
  let postgres = DEFAULT_POSTGRES_PORT;
  for (let index = 0; index < args.length; index += 2) {
    const value = args[index + 1];
    if (args[index] === "--doc" && value !== undefined) doc = resolve(value);
    else if (args[index] === "--dev-port" && value !== undefined)
      dev = Number(value);
    else if (args[index] === "--postgres-port" && value !== undefined)
      postgres = Number(value);
    else
      throw new Error(
        "usage: pnpm getting-started:e2e [--doc <path>] [--dev-port <port>] [--postgres-port <port>]",
      );
  }
  for (const port of [dev, postgres])
    if (!Number.isInteger(port) || port < 1 || port > 65_535)
      throw new Error("a port option must be a port number");
  return { doc, ports: { dev, postgres } };
}

try {
  const { doc, ports } = options(process.argv.slice(2));
  const outcome = await main(doc, ports);
  process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
  await finish(0);
} catch (error) {
  process.stderr.write(`getting-started-e2e: ${String(error)}\n`);
  for (const [name, file] of [
    ["calque dev", "dev.log"],
    ["vite", "vite.log"],
  ] as const) {
    const log = join(workDirectory, file);
    if (existsSync(log))
      process.stderr.write(`--- ${name}\n${logTail(log, 40)}\n`);
  }
  if (postgresStarted)
    process.stderr.write(
      `--- postgres\n${run("docker", ["logs", "--tail", "30", container]).stderr}\n`,
    );
  await finish(1);
}
