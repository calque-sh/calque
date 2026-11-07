#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Runs the walk in docs/self-host.md the way a reader does, then drives the
 * page it ends on in headless Chromium.
 *
 * Every ```sh block before `## Tearing it down` runs in order from the
 * repository root, in one bash under `set -euo pipefail`, with no `CALQUE_*`
 * variable inherited. The last block must read exactly
 * `pnpm --filter @calque/quickstart dev`; it runs in the background. The run
 * then checks that the Docker build context carries none of the walk's files,
 * that the page paints the seeded row and follows an INSERT, an UPDATE and a
 * DELETE without a reload or a failed request, that the rendered bindings and
 * start module equal the committed ones, and that PostgreSQL logged no `FATAL`
 * beyond the startup probe's. Teardown fails
 * the run if anything the walk created is left.
 *
 * Usage: `pnpm quickstart:e2e [--doc <path>]`; `--doc` reads the walk from
 * another copy of the document. Requires Docker, the Rust toolchain and libpq,
 * pnpm, and Playwright's Chromium.
 */
import assert from "node:assert/strict";
import {
  spawn,
  spawnSync,
  type ChildProcess,
  type SpawnSyncReturns,
} from "node:child_process";
import { createHash, randomUUID, X509Certificate } from "node:crypto";
import {
  closeSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { connect as tcpConnect } from "node:net";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import type { Readable } from "node:stream";
import { connect as tlsConnect, type DetailedPeerCertificate } from "node:tls";
import { fileURLToPath } from "node:url";
import type { Browser, Page, Request } from "playwright";

import { docShellBlocks } from "./lib/doc-walk.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const COMPOSE_FILE = "docker-compose.selfhost.yml";
const STOP = "## Tearing it down";
const PAGE_START = "pnpm --filter @calque/quickstart dev";
// Where the page is served and where it looks for the edge: both fixed in
// client/examples/quickstart (vite.config.ts and main.ts).
const PAGE_ORIGIN = "http://127.0.0.1:5173";
const PAGE_PORT = 5173;
const PAGE_TITLE = "<title>Calque quickstart</title>";
const EDGE_HOST = "localhost";
const EDGE_PORT = 8443;
const BINDINGS = "client/examples/quickstart/calque.generated.ts";
const START_MODULE = "client/examples/quickstart/calque.start.generated.ts";
/** What the walk writes to the repository root, all deleted by teardown. */
const WALK_FILES = [
  "grant.json",
  "grant-key.pk8",
  "calque.inspection.json",
  "calque.config.json",
  "caddy-root.crt",
] as const;
// The walk builds `calque` twice, on the host and in the image, and neither
// build has a bound of its own.
const WALK_TIMEOUT_MS = 45 * 60_000;
const PAGE_START_TIMEOUT_MS = 60_000;
const CHANGE_TIMEOUT_MS = 60_000;
const STOP_GRACE_MS = 5_000;

const runId = randomUUID().replaceAll("-", "").slice(0, 12);
const workDirectory = join(root, "target/quickstart-e2e", runId);

interface Composition {
  readonly project: string;
  readonly ports: readonly number[];
  readonly builtImages: readonly string[];
}

interface Baseline {
  readonly composition: Composition;
  readonly volumes: ReadonlySet<string>;
  readonly existingImages: ReadonlySet<string>;
  readonly gitStatus: string;
}

interface RunOutcome {
  readonly result: "PASS";
  readonly doc: string;
  readonly blocks: number;
  readonly walkSeconds: number;
  readonly edgeChainKeys: number;
  readonly pageSteps: readonly string[];
  readonly streamedReadsEndedCanceledExempted: number;
  readonly postgresStartupRefusalsExempted: number;
}

/** The shell a reader starts from: this environment without `CALQUE_*`. */
function readerEnvironment(): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {};
  for (const [name, value] of Object.entries(process.env))
    if (!name.startsWith("CALQUE_") && value !== undefined)
      environment[name] = value;
  return environment;
}

/**
 * Compose interpolates the whole file for every command, `down` and `config`
 * included, and refuses while one of its `${NAME:?}` variables is unset. Their
 * values mean nothing to either command, so every one gets the same
 * placeholder, which is also a path, for the one that names a file.
 */
function composeEnvironment(): NodeJS.ProcessEnv {
  const environment = readerEnvironment();
  const composition = readFileSync(join(root, COMPOSE_FILE), "utf8");
  for (const [, name] of composition.matchAll(/\$\{([A-Z0-9_]+):\?/gu))
    if (name !== undefined) environment[name] = "/dev/null";
  return environment;
}

function run(
  command: string,
  args: readonly string[],
  options: { readonly env?: NodeJS.ProcessEnv; readonly input?: string } = {},
): SpawnSyncReturns<string> {
  return spawnSync(command, [...args], {
    cwd: root,
    encoding: "utf8",
    env: options.env ?? readerEnvironment(),
    ...(options.input === undefined ? {} : { input: options.input }),
    maxBuffer: 64 * 1024 * 1024,
  });
}

function succeed(
  command: string,
  args: readonly string[],
  options: { readonly env?: NodeJS.ProcessEnv; readonly input?: string } = {},
): string {
  const result = run(command, args, options);
  if (result.status !== 0)
    throw new Error(
      `${command} ${args.join(" ")} exited ${String(result.status)}${result.error === undefined ? "" : ` (${result.error.message})`}\n${result.stdout}\n${result.stderr}`,
    );
  return result.stdout;
}

/**
 * The project, published ports and built images the composition resolves to
 * with the walk's defaults.
 */
function composition(): Composition {
  const model = JSON.parse(
    succeed(
      "docker",
      ["compose", "-f", COMPOSE_FILE, "config", "--format", "json"],
      { env: composeEnvironment() },
    ),
  ) as {
    readonly name: string;
    readonly services: Readonly<
      Record<
        string,
        {
          readonly image?: string;
          readonly build?: unknown;
          readonly ports?: readonly { readonly published?: string }[];
        }
      >
    >;
  };
  const services = Object.values(model.services);
  return {
    project: model.name,
    ports: services.flatMap((service) =>
      (service.ports ?? []).map((port) => Number(port.published)),
    ),
    builtImages: services.flatMap((service) =>
      service.build !== undefined && service.image !== undefined
        ? [service.image]
        : [],
    ),
  };
}

/** Whether something accepts connections on this loopback port. */
async function listening(port: number): Promise<boolean> {
  return await new Promise((answer) => {
    const socket = tcpConnect({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      answer(true);
    });
    socket.once("error", () => answer(false));
  });
}

function composeProjects(): string[] {
  const listed = succeed("docker", [
    "compose",
    "ls",
    "--all",
    "--format",
    "json",
  ]);
  return (JSON.parse(listed || "[]") as { readonly Name: string }[]).map(
    (project) => project.Name,
  );
}

function volumes(): Set<string> {
  return new Set(
    succeed("docker", ["volume", "ls", "--quiet"]).split("\n").filter(Boolean),
  );
}

function imageExists(image: string): boolean {
  return run("docker", ["image", "inspect", image]).status === 0;
}

function gitStatus(): string {
  return succeed("git", ["status", "--porcelain"]);
}

async function preflight(): Promise<Baseline> {
  const resolved = composition();
  const refusals: string[] = [];
  for (const file of WALK_FILES)
    if (existsSync(join(root, file)))
      refusals.push(
        `${file} exists in the repository root, and teardown deletes it`,
      );
  if (
    run("git", ["diff", "--quiet", "--", BINDINGS, START_MODULE]).status !== 0
  )
    refusals.push(
      `${BINDINGS} or ${START_MODULE} has unstaged changes, and teardown restores them from the index`,
    );
  if (composeProjects().includes(resolved.project))
    refusals.push(
      `the compose project ${resolved.project} exists, and teardown removes it with its volumes`,
    );
  for (const port of [...resolved.ports, PAGE_PORT])
    if (await listening(port))
      refusals.push(`127.0.0.1:${String(port)} is already taken`);
  if (refusals.length > 0)
    throw new Error(`refusing to start:\n  ${refusals.join("\n  ")}`);
  return {
    composition: resolved,
    volumes: volumes(),
    existingImages: new Set(resolved.builtImages.filter(imageExists)),
    gitStatus: gitStatus(),
  };
}

let walk: ChildProcess | undefined;
let page: ChildProcess | undefined;
let browser: Browser | undefined;

/** Stops a detached child's whole process group: SIGTERM, then SIGKILL. */
async function stopGroup(child: ChildProcess | undefined): Promise<void> {
  if (child?.pid === undefined || child.exitCode !== null) return;
  const exited = new Promise<void>((done) => child.once("exit", () => done()));
  const signal = (name: NodeJS.Signals) => {
    try {
      process.kill(-(child.pid ?? 0), name);
    } catch {
      // The group is already gone.
    }
  };
  signal("SIGTERM");
  const timer = new Promise<"timeout">((done) =>
    setTimeout(() => done("timeout"), STOP_GRACE_MS),
  );
  if ((await Promise.race([exited, timer])) === "timeout") {
    signal("SIGKILL");
    await exited;
  }
}

/**
 * Runs the walk's blocks in one bash and returns the `CALQUE_*` variables the
 * blocks exported, which the checks need to reach the database. The shell
 * hands its environment back over fd 3 after the last block, so nothing the
 * walk exports is written to disk.
 */
async function runWalk(
  blocks: readonly string[],
): Promise<Record<string, string>> {
  const script = [
    "set -euo pipefail",
    ...blocks.flatMap((block, index) => [
      `printf '\\n== block %d of %d\\n' ${String(index + 1)} ${String(blocks.length)}`,
      block,
    ]),
    `node -p 'JSON.stringify(process.env)' >&3`,
    "",
  ].join("\n");
  const scriptPath = join(workDirectory, "walk.sh");
  writeFileSync(scriptPath, script);
  const child = spawn("bash", [scriptPath], {
    cwd: root,
    env: readerEnvironment(),
    detached: true,
    stdio: ["ignore", "inherit", "inherit", "pipe"],
  });
  walk = child;
  let exported = "";
  const environmentPipe = child.stdio[3] as Readable | null;
  environmentPipe?.setEncoding("utf8");
  environmentPipe?.on("data", (chunk: string) => (exported += chunk));
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    void stopGroup(child);
  }, WALK_TIMEOUT_MS);
  const [code, signal] = await new Promise<
    [number | null, NodeJS.Signals | null]
  >((done) => child.once("exit", (...exit) => done(exit)));
  clearTimeout(timer);
  walk = undefined;
  if (timedOut)
    throw new Error(
      `the walk did not finish within ${String(WALK_TIMEOUT_MS / 60_000)} minutes`,
    );
  if (code !== 0)
    throw new Error(
      `the walk exited ${String(code)}${signal === null ? "" : ` on ${signal}`}`,
    );
  const environment = JSON.parse(exported) as Record<string, string>;
  return Object.fromEntries(
    Object.entries(environment).filter(([name]) => name.startsWith("CALQUE_")),
  );
}

/** Which of the walk's files `docker build` would send to the daemon. */
function walkFilesInBuildContext(): string[] {
  const built = spawnSync(
    "docker",
    ["build", "--quiet", "--file", "-", "--output", "type=tar,dest=-", "."],
    {
      cwd: root,
      input: "FROM scratch\nCOPY . /\n",
      env: readerEnvironment(),
      maxBuffer: 512 * 1024 * 1024,
    },
  );
  if (built.status !== 0)
    throw new Error(
      `the build-context listing failed: ${built.stderr.toString("utf8")}`,
    );
  const entries = spawnSync("tar", ["-t"], {
    input: built.stdout,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (entries.status !== 0)
    throw new Error(`tar could not list the build context: ${entries.stderr}`);
  const names = new Set(
    entries.stdout
      .split("\n")
      .map((entry) => entry.replace(/^\.\//u, "").replace(/\/$/u, "")),
  );
  return WALK_FILES.filter((file) => names.has(file));
}

function startPage(block: string, exported: Record<string, string>): void {
  const log = openSync(join(workDirectory, "page.log"), "w");
  page = spawn("bash", ["-c", block], {
    cwd: root,
    env: { ...readerEnvironment(), ...exported },
    detached: true,
    stdio: ["ignore", log, log],
  });
  closeSync(log);
}

function pageLog(): string {
  try {
    return readFileSync(join(workDirectory, "page.log"), "utf8")
      .split("\n")
      .slice(-40)
      .join("\n");
  } catch {
    return "(no page log)";
  }
}

/**
 * Waits for the quickstart page itself, not just for something on 5173: a
 * dev server that died on a taken port leaves whatever held it answering.
 */
async function waitForPage(): Promise<void> {
  const deadline = Date.now() + PAGE_START_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (page?.exitCode !== null)
      throw new Error(
        `the page's dev server exited ${String(page?.exitCode)}:\n${pageLog()}`,
      );
    try {
      const response = await fetch(`${PAGE_ORIGIN}/`);
      if (response.ok && (await response.text()).includes(PAGE_TITLE)) return;
    } catch {
      // Not listening yet.
    }
    await new Promise((done) => setTimeout(done, 250));
  }
  throw new Error(
    `the page did not answer at ${PAGE_ORIGIN} within ${String(PAGE_START_TIMEOUT_MS)} ms:\n${pageLog()}`,
  );
}

/**
 * The SPKI hash, as Chromium's allowlist spells it, of every certificate in
 * the chain the edge presents, once that chain has verified against `caddy`
 * and the edge's name. Caddy never sends its root, so the root's key alone
 * would not match.
 */
async function verifiedEdgeChainKeys(caddy: string): Promise<string[]> {
  const chain = await new Promise<DetailedPeerCertificate>(
    (resolveChain, rejectChain) => {
      const socket = tlsConnect(
        {
          host: "127.0.0.1",
          port: EDGE_PORT,
          servername: EDGE_HOST,
          ca: caddy,
        },
        () => {
          const peer = socket.getPeerCertificate(true);
          socket.end();
          resolveChain(peer);
        },
      );
      socket.on("error", rejectChain);
    },
  );
  const keys = new Set<string>();
  const seen = new Set<string>();
  for (
    let certificate: DetailedPeerCertificate | undefined = chain;
    certificate !== undefined && !seen.has(certificate.fingerprint256);
    certificate = certificate.issuerCertificate
  ) {
    seen.add(certificate.fingerprint256);
    keys.add(
      createHash("sha256")
        .update(
          new X509Certificate(certificate.raw).publicKey.export({
            type: "spki",
            format: "der",
          }),
        )
        .digest("base64"),
    );
  }
  return [...keys];
}

/** Waits for `#tasks` to read `expected`, failing at once on an error status. */
async function expectTasks(
  opened: Page,
  step: string,
  expected: readonly string[],
): Promise<void> {
  const want = JSON.stringify(expected);
  try {
    const settled = await opened.waitForFunction(
      (target) => {
        const status = document.getElementById("status")?.textContent ?? "";
        if (/^(?:error|locked):/u.test(status)) return { failed: status };
        const tasks = [...document.querySelectorAll("#tasks li")].map(
          (item) => item.textContent,
        );
        return JSON.stringify(tasks) === target ? { failed: null } : false;
      },
      want,
      { timeout: CHANGE_TIMEOUT_MS, polling: 100 },
    );
    const { failed } = (await settled.jsonValue()) as {
      readonly failed: string | null;
    };
    if (failed !== null) throw new Error(`the page reports "${failed}"`);
  } catch (error) {
    const seen = await opened.evaluate(() => ({
      status: document.getElementById("status")?.textContent ?? "",
      tasks: [...document.querySelectorAll("#tasks li")].map(
        (item) => item.textContent,
      ),
    }));
    throw new Error(
      `${step}: expected tasks ${want}; the page shows ${JSON.stringify(seen.tasks)} with status "${seen.status}" (${String(error).split("\n")[0] ?? ""})`,
    );
  }
}

// `calque` serves the bootstrap plan and each artifact `no-store, private`,
// and the client reads both through a stream. Chromium ends such a request as
// canceled whenever the page's last read comes after the network's last byte,
// although the page received every byte, so a request answered 200 can end
// this way on a load that worked. The steps say the artifact arrived, and the
// one bootstrap request the run allows says the plan did: the client asks
// again for a plan whose body the network cut.
const STREAMED_BODY_END = "net::ERR_ABORTED";

function isBootstrapRequest(method: string, url: string): boolean {
  return method === "POST" && new URL(url).pathname === "/v1/bootstrap";
}

function isStreamedRequest(method: string, url: string): boolean {
  return (
    isBootstrapRequest(method, url) ||
    (method === "GET" &&
      new URL(url).pathname.startsWith("/v1/artifact-objects/"))
  );
}

async function drivePage(exported: Record<string, string>): Promise<{
  readonly chainKeys: number;
  readonly steps: string[];
  readonly streamedReadsEndedCanceled: number;
}> {
  const keys = await verifiedEdgeChainKeys(
    readFileSync(join(root, "caddy-root.crt"), "utf8"),
  );
  const { chromium } = createRequire(
    join(root, "client/packages/client/package.json"),
  )("playwright") as typeof import("playwright");
  browser = await chromium.launch({
    headless: true,
    args: [`--ignore-certificate-errors-spki-list=${keys.join(",")}`],
  });
  const context = await browser.newContext();
  const failedRequests: string[] = [];
  const answeredStreams = new WeakSet<Request>();
  let streamedReadsEndedCanceled = 0;
  let bootstrapRequests = 0;
  context.on("requestfailed", (request) => {
    const error = request.failure()?.errorText ?? "unknown";
    if (error === STREAMED_BODY_END && answeredStreams.has(request)) {
      streamedReadsEndedCanceled += 1;
      return;
    }
    failedRequests.push(`${request.method()} ${request.url()}: ${error}`);
  });
  context.on("request", (request) => {
    if (isBootstrapRequest(request.method(), request.url()))
      bootstrapRequests += 1;
  });
  context.on("response", (response) => {
    if (
      response.status() === 200 &&
      isStreamedRequest(response.request().method(), response.url())
    )
      answeredStreams.add(response.request());
    if (response.status() >= 400)
      failedRequests.push(
        `${response.request().method()} ${response.url()}: HTTP ${String(response.status())}`,
      );
  });
  const opened = await context.newPage();
  const pageErrors: string[] = [];
  opened.on("pageerror", (error) => pageErrors.push(String(error)));
  await opened.goto(PAGE_ORIGIN, { waitUntil: "load", timeout: 60_000 });

  // The password reaches psql through the environment, never through argv.
  const commit = (sql: string): void => {
    succeed(
      "docker",
      [
        "compose",
        "-f",
        COMPOSE_FILE,
        "exec",
        "-T",
        "-e",
        "PGPASSWORD",
        "postgres",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        "127.0.0.1",
        "-U",
        "calque_sync",
        "-d",
        "calque",
        "-c",
        sql,
      ],
      {
        env: {
          ...readerEnvironment(),
          ...exported,
          PGPASSWORD: exported.CALQUE_SELFHOST_DB_PASSWORD ?? "",
        },
      },
    );
  };
  const steps: readonly (readonly [
    string,
    string | null,
    readonly string[],
  ])[] = [
    ["the seeded task paints", null, ["open: Open the page"]],
    [
      "an INSERT arrives",
      "INSERT INTO tasks VALUES (2, 1, 'Watch it arrive', false)",
      ["open: Open the page", "open: Watch it arrive"],
    ],
    [
      "an UPDATE arrives",
      "UPDATE tasks SET done = true WHERE id = 1",
      ["done: Open the page", "open: Watch it arrive"],
    ],
    [
      "a DELETE arrives",
      "DELETE FROM tasks WHERE id = 2",
      ["done: Open the page"],
    ],
  ];
  for (const [step, sql, expected] of steps) {
    if (sql !== null) commit(sql);
    await expectTasks(opened, step, expected);
  }
  assert.deepEqual(pageErrors, [], "the page threw");
  assert.deepEqual(failedRequests, [], "the page had failed requests");
  assert.equal(
    bootstrapRequests,
    1,
    "the page did not ask for its plan exactly once",
  );
  return {
    chainKeys: keys.length,
    steps: steps.map(([step]) => step),
    streamedReadsEndedCanceled,
  };
}

// The postmaster's answer to a connection that arrives while it is starting,
// after the log prefix's `[pid]`. The healthcheck's pg_isready causes it when
// a probe lands in that window, any pg_isready healthcheck can, and it says
// nothing about configuration.
const STARTUP_REFUSAL = "] FATAL:  the database system is starting up";

interface PostgresLog {
  /** Lines that read as a failure, or why the log cannot be judged. */
  readonly alarming: readonly string[];
  readonly startupRefusals: number;
}

function judgePostgresLog(): PostgresLog {
  const lines = succeed(
    "docker",
    ["compose", "-f", COMPOSE_FILE, "logs", "--no-color", "postgres"],
    { env: composeEnvironment() },
  ).split("\n");
  if (
    !lines.some((line) =>
      line.includes("database system is ready to accept connections"),
    )
  )
    return {
      alarming: [
        `no line says the database system is ready to accept connections, in ${String(lines.length)} lines read`,
      ],
      startupRefusals: 0,
    };
  const startupRefusals = lines.filter((line) =>
    line.endsWith(STARTUP_REFUSAL),
  ).length;
  const alarming = lines.filter(
    (line) =>
      !line.endsWith(STARTUP_REFUSAL) &&
      (line.includes("FATAL") || line.includes("contains errors")),
  );
  return { alarming, startupRefusals };
}

/** The lines where the walk's render of `path` differs from the index's. */
function renderDifferences(path: string): string[] {
  const rendered = readFileSync(join(root, path), "utf8").split("\n");
  const committed = succeed("git", ["show", `:${path}`]).split("\n");
  const differences: string[] = [];
  for (
    let index = 0;
    index < Math.max(rendered.length, committed.length);
    index++
  ) {
    const mine = rendered[index];
    const theirs = committed[index];
    if (mine !== theirs)
      differences.push(
        `line ${String(index + 1)}: committed ${JSON.stringify(theirs ?? null)}, rendered ${JSON.stringify(mine ?? null)}`,
      );
  }
  return differences;
}

let baseline: Baseline | undefined;

async function main(doc: string): Promise<RunOutcome> {
  const blocks = docShellBlocks(readFileSync(doc, "utf8"), STOP);
  process.stdout.write(
    `${doc}: ${String(blocks.length)} sh blocks before "${STOP}"\n`,
  );
  for (const [index, block] of blocks.entries())
    process.stdout.write(`--- block ${String(index + 1)}\n${block}\n`);
  const pageBlock = blocks.at(-1);
  if (pageBlock !== PAGE_START)
    throw new Error(
      `the last block must start the page with exactly "${PAGE_START}"; it reads ${JSON.stringify(pageBlock)}`,
    );
  const walkBlocks = blocks.slice(0, -1);
  if (walkBlocks.includes(PAGE_START))
    throw new Error(`"${PAGE_START}" may appear only as the last block`);

  baseline = await preflight();
  mkdirSync(workDirectory, { recursive: true });
  const started = Date.now();
  const exported = await runWalk(walkBlocks);
  const walkSeconds = Math.round((Date.now() - started) / 1000);
  for (const file of ["calque.inspection.json", "calque.config.json"])
    copyFileSync(join(root, file), join(workDirectory, file));
  copyFileSync(
    join(root, BINDINGS),
    join(workDirectory, "calque.generated.ts"),
  );

  const failures: string[] = [];
  const leaked = walkFilesInBuildContext();
  if (leaked.length > 0)
    failures.push(`the Docker build context carries ${leaked.join(", ")}`);

  startPage(pageBlock, exported);
  let driven: Awaited<ReturnType<typeof drivePage>> | undefined;
  try {
    await waitForPage();
    driven = await drivePage(exported);
  } catch (error) {
    failures.push(`the page: ${String(error)}`);
  }

  for (const path of [BINDINGS, START_MODULE]) {
    const differences = renderDifferences(path);
    if (differences.length > 0)
      failures.push(
        `${path} as the walk rendered it differs from the index:\n  ${differences.join("\n  ")}`,
      );
  }
  const postgresLog = judgePostgresLog();
  if (postgresLog.alarming.length > 0)
    failures.push(
      `the postgres log does not read as a clean start:\n  ${postgresLog.alarming.join("\n  ")}`,
    );
  if (failures.length > 0 || driven === undefined)
    throw new Error(failures.join("\n\n"));
  return {
    result: "PASS",
    doc,
    blocks: blocks.length,
    walkSeconds,
    edgeChainKeys: driven.chainKeys,
    pageSteps: driven.steps,
    streamedReadsEndedCanceledExempted: driven.streamedReadsEndedCanceled,
    postgresStartupRefusalsExempted: postgresLog.startupRefusals,
  };
}

/** Tears down what the run started and returns what is left behind. */
async function teardown(): Promise<string[]> {
  await browser?.close().catch(() => undefined);
  await stopGroup(walk);
  await stopGroup(page);
  // Nothing was started before the preflight passed, and the files it found
  // in the root are not this run's to delete.
  if (baseline === undefined) return [];
  const { composition: resolved } = baseline;
  const environment = composeEnvironment();
  run(
    "docker",
    ["compose", "-f", COMPOSE_FILE, "down", "--volumes", "--remove-orphans"],
    { env: environment },
  );
  for (const file of WALK_FILES) rmSync(join(root, file), { force: true });
  run("git", ["checkout", "--", BINDINGS, START_MODULE]);
  for (const image of resolved.builtImages)
    if (!baseline.existingImages.has(image))
      run("docker", ["image", "rm", "--force", image]);

  const left: string[] = [];
  if (composeProjects().includes(resolved.project))
    left.push(`compose project ${resolved.project}`);
  const containers = succeed("docker", [
    "ps",
    "--all",
    "--quiet",
    "--filter",
    `label=com.docker.compose.project=${resolved.project}`,
  ]).trim();
  if (containers !== "")
    left.push(`containers ${containers.split("\n").join(", ")}`);
  const before = baseline.volumes;
  const newVolumes = [...volumes()].filter((volume) => !before.has(volume));
  if (newVolumes.length > 0) left.push(`volumes ${newVolumes.join(", ")}`);
  for (const image of resolved.builtImages)
    if (!baseline.existingImages.has(image) && imageExists(image))
      left.push(`image ${image}`);
  if (await listening(PAGE_PORT))
    left.push(`a listener on 127.0.0.1:${String(PAGE_PORT)}`);
  for (const file of WALK_FILES)
    if (existsSync(join(root, file))) left.push(`${file} in the root`);
  const status = gitStatus();
  if (status !== baseline.gitStatus)
    left.push(
      `git status changed:\n--- before\n${baseline.gitStatus}--- after\n${status}`,
    );
  return left;
}

let finishing: Promise<never> | undefined;

/** Tears down once, reports what was left, and exits. */
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
        ? "quickstart-e2e: left behind: nothing\n"
        : `quickstart-e2e: left behind:\n  ${left.join("\n  ")}\n`,
    );
    if (status === 0 && left.length === 0)
      rmSync(workDirectory, { recursive: true, force: true });
    else if (existsSync(workDirectory))
      process.stderr.write(`quickstart-e2e: kept ${workDirectory}\n`);
    process.exit(left.length > 0 && status === 0 ? 1 : status);
  })();
  return finishing;
}

for (const [signal, status] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
] as const)
  process.on(signal, () => {
    process.stderr.write(`quickstart-e2e: ${signal}; tearing down\n`);
    void finish(status);
  });

function docArgument(argv: readonly string[]): string {
  const args = argv[0] === "--" ? argv.slice(1) : argv;
  if (args.length === 0) return join(root, "docs", "self-host.md");
  if (args.length === 2 && args[0] === "--doc" && args[1] !== undefined)
    return resolve(args[1]);
  throw new Error("usage: pnpm quickstart:e2e [--doc <path>]");
}

try {
  const outcome = await main(docArgument(process.argv.slice(2)));
  process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
  await finish(0);
} catch (error) {
  process.stderr.write(`quickstart-e2e: ${String(error)}\n`);
  if (baseline !== undefined && finishing === undefined) {
    const logs = run(
      "docker",
      ["compose", "-f", COMPOSE_FILE, "logs", "--no-color", "--tail", "120"],
      { env: composeEnvironment() },
    );
    process.stderr.write(logs.stdout);
  }
  await finish(1);
}
