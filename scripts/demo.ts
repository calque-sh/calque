#!/usr/bin/env node
import {
  spawn,
  spawnSync,
  type ChildProcess,
  type SpawnSyncOptions,
  type SpawnSyncReturns,
} from "node:child_process";
import { randomBytes } from "node:crypto";
import { createRequire } from "node:module";
import { createConnection, createServer } from "node:net";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { Readable } from "node:stream";
import { CARGO_TARGET_ARGUMENTS } from "./lib/cargo-target.ts";
import {
  buildCalqueContainerArguments,
  CONTAINER_STATE_DIRECTORY,
  DEMO_IMAGE_COMPOSE_FILE,
  demoImageReference,
  releaseVersion,
} from "./lib/demo-image.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const stateDirectory = join(root, ".calque-dev");
const clientEnvironmentPath = join(stateDirectory, "client.env");
const clientPackageDirectory = join(root, "client/packages/client");

/**
 * The manual page's pinned bindings: `calque init` of `calque inspect` of the
 * seeded database, and the `calque-bindings` render of that configuration.
 * The launcher does not regenerate them; it checks the committed render is
 * current before it builds the page.
 */
export const DEMO_BINDINGS = Object.freeze({
  config: "client/packages/client/tests/demo/calque.config.json",
  output: "client/packages/client/tests/demo/calque.generated.ts",
});

/** The bindings drift check, as the launcher runs it from the repository root. */
export function buildBindingsCheckArguments(): readonly string[] {
  return [
    "client/packages/config/bin/calque-bindings.js",
    "--config",
    DEMO_BINDINGS.config,
    "--out",
    DEMO_BINDINGS.output,
    "--check",
  ];
}

interface LocalServiceOptions {
  readonly stateDirectory: string;
  readonly tables: readonly string[];
  readonly uiOrigin: string;
  readonly browserEndpoint: string;
  readonly serverPort: number;
  /** The metrics listener's port; without one, `calque dev` serves no scrape. */
  readonly metricsPort?: number | undefined;
  /** The listener's address; `calque dev` binds loopback without one. */
  readonly bindAddress?: string | undefined;
}

/**
 * The build that precedes the local service. It is a step of its own, with no
 * timeout, because a first build compiles the workspace from cold, which on a
 * slow host can outlast the service's readiness window. Diagnostics render to
 * the terminal; standard output carries the JSON messages that name the
 * executable.
 */
export function buildCalqueBuildArguments(): readonly string[] {
  return [
    "build",
    ...CARGO_TARGET_ARGUMENTS,
    "-p",
    "calque",
    "--message-format=json-render-diagnostics",
  ];
}

/** The `calque` executable a `cargo build --message-format=json*` reported. */
export function calqueExecutable(cargoJson: string): string {
  let executable: string | undefined;
  for (const line of cargoJson.split("\n")) {
    if (!line.startsWith("{")) continue;
    const message = JSON.parse(line) as {
      readonly reason?: string;
      readonly target?: { readonly name?: string };
      readonly executable?: string | null;
    };
    if (
      message.reason === "compiler-artifact" &&
      message.target?.name === "calque" &&
      typeof message.executable === "string"
    )
      executable = message.executable;
  }
  if (!executable) throw new Error("cargo built no calque executable");
  return executable;
}

/**
 * `calque dev`'s arguments. The tables are the relations the pinned
 * configuration declares; the page binds them by relation and the service
 * reports the wire table ids in its plan.
 */
export function buildDevArguments(
  options: LocalServiceOptions,
): readonly string[] {
  return [
    "dev",
    "--state-dir",
    options.stateDirectory,
    ...options.tables.flatMap((table) => ["--table", table]),
    "--allow-origin",
    options.uiOrigin,
    "--public-base-url",
    options.browserEndpoint,
    "--port",
    String(options.serverPort),
    ...(options.bindAddress === undefined
      ? []
      : ["--bind-address", options.bindAddress]),
    ...(options.metricsPort === undefined
      ? []
      : ["--metrics-port", String(options.metricsPort)]),
    "--acknowledge-disposable-source",
  ];
}

export interface PageCommand {
  readonly command: string;
  readonly arguments_: readonly string[];
  readonly cwd: string;
}

/**
 * A page's preview server: its package's own vite, run by this node from the
 * package directory, as `calque` is run from the executable cargo built.
 * Run through `pnpm --filter <package> <script>`, vite would be pnpm's
 * grandchild, and the SIGTERM that stops the page would also end pnpm, which
 * reports vite's exit status 143 as `ERR_PNPM_RECURSIVE_RUN_FIRST_FAIL` on
 * every clean stop.
 */
export function buildViteCommand(
  packageDirectory: string,
  viteArguments: readonly string[],
): PageCommand {
  const manifestPath = createRequire(
    join(packageDirectory, "package.json"),
  ).resolve("vite/package.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as {
    readonly bin: { readonly vite: string };
  };
  return {
    command: process.execPath,
    arguments_: [
      join(dirname(manifestPath), manifest.bin.vite),
      ...viteArguments,
    ],
    cwd: packageDirectory,
  };
}

/** The page's preview server, `@calque/client`'s vite. */
export function buildPageCommand(uiPort: number): PageCommand {
  return buildViteCommand(clientPackageDirectory, [
    "preview",
    "tests/demo",
    "--host",
    "127.0.0.1",
    "--strictPort",
    "--port",
    String(uiPort),
  ]);
}

/** The relations the pinned configuration declares, `schema.table` each. */
export function configuredRelations(configJson: string): readonly string[] {
  const config = JSON.parse(configJson) as {
    readonly collections?: readonly {
      readonly relation?: { readonly schema?: string; readonly table?: string };
    }[];
  };
  const relations = (config.collections ?? []).map(({ relation }) => {
    if (!relation?.schema || !relation.table)
      throw new Error(
        "calque.config.json declares a collection without a relation",
      );
    return `${relation.schema}.${relation.table}`;
  });
  if (relations.length === 0)
    throw new Error("calque.config.json declares no collections");
  return relations;
}
const postgresRoutingEnvironment = Object.freeze([
  "PGHOST",
  "PGHOSTADDR",
  "PGPORT",
  "PGDATABASE",
  "PGSERVICE",
  "PGSERVICEFILE",
  "PGSYSCONFDIR",
  "PGTARGETSESSIONATTRS",
  "PGLOADBALANCEHOSTS",
]);
// The non-superuser REPLICATION role `docker/postgres-init` creates.
//
// Enrollment refuses a superuser, so the server cannot use the image's
// bootstrap role. The seed runs as this role too, because CREATE PUBLICATION
// FOR TABLE requires ownership of every table it names and the server creates
// that publication itself.
export const DEVELOPMENT_SOURCE_ROLE = "calque_sync";

/**
 * The PostgreSQL major this flow starts, read from the compose file that
 * starts it, so the banner cannot disagree with the container.
 */
export function composePostgresMajor(composeYaml: string): number {
  const image = /^\s*image:\s*postgres:(\d+)\b/mu.exec(composeYaml);
  const major = image?.[1];
  if (!major)
    throw new Error("docker-compose.yml does not pin a postgres image tag");
  return Number(major);
}

export const demoSeedSql = String.raw`
BEGIN;
CREATE TABLE IF NOT EXISTS projects (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  status text NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
  id bigint PRIMARY KEY,
  project_id bigint NOT NULL REFERENCES projects(id),
  title text NOT NULL,
  done boolean NOT NULL DEFAULT false
);
TRUNCATE TABLE tasks, projects;
INSERT INTO projects (id, name, status) VALUES
  (101, 'Atlas launch', 'at_risk'),
  (102, 'Billing reliability', 'on_track'),
  (103, 'Mobile rollout', 'planning');
INSERT INTO tasks (id, project_id, title, done) VALUES
  (1001, 101, 'Freeze the release candidate', false),
  (1002, 101, 'Validate the data migration', true),
  (1003, 101, 'Review launch telemetry', false),
  (1004, 102, 'Rehearse regional failover', true),
  (1005, 102, 'Tune alert thresholds', true),
  (1006, 102, 'Document the recovery path', true),
  (1007, 103, 'Approve the offline experience', false),
  (1008, 103, 'Run the beta cohort', false),
  (1009, 103, 'Finish the accessibility pass', true);
COMMIT;
`;
export const demoChangeSql = String.raw`
WITH changed_task AS (
  UPDATE tasks
  SET done = NOT done
  WHERE id = 1001
  RETURNING project_id, done
)
UPDATE projects AS project
SET status = CASE WHEN changed_task.done THEN 'on_track' ELSE 'at_risk' END
FROM changed_task
WHERE project.id = changed_task.project_id
RETURNING project.id, project.name, project.status, changed_task.done AS task_done;
`;

type DemoCommand =
  | {
      readonly command: "start";
      readonly open: boolean;
      /** Build `calque` with cargo instead of running its image. */
      readonly fromSource: boolean;
    }
  | { readonly command: "change"; readonly open: false };

const DEMO_USAGE =
  "usage: pnpm demo [-- [--no-open] [--from-source]] | pnpm demo:change";

export function parseDemoArguments(arguments_: readonly string[]): DemoCommand {
  if (arguments_[0] === "--") arguments_ = arguments_.slice(1);
  if (arguments_.length === 1 && arguments_[0] === "change") {
    return { command: "change", open: false };
  }
  if (arguments_[0] === "start") arguments_ = arguments_.slice(1);
  const flags = new Set(arguments_);
  if (
    flags.size !== arguments_.length ||
    arguments_.some((flag) => flag !== "--no-open" && flag !== "--from-source")
  ) {
    throw new Error(DEMO_USAGE);
  }
  return {
    command: "start",
    open: !flags.has("--no-open"),
    fromSource: flags.has("--from-source"),
  };
}

interface PrivateClientEnvironment {
  readonly endpoint: string;
  readonly bearerToken: string;
}

export function parsePrivateClientEnvironment(
  contents: string,
): PrivateClientEnvironment {
  const expected = new Set(["CALQUE_ENDPOINT", "CALQUE_BEARER_TOKEN"]);
  const values = new Map<string, string>();
  for (const line of contents.split(/\r?\n/u)) {
    if (line.length === 0) continue;
    const separator = line.indexOf("=");
    if (separator <= 0) {
      throw new Error("generated client settings are malformed");
    }
    const key = line.slice(0, separator);
    const value = line.slice(separator + 1);
    if (!expected.has(key) || values.has(key) || value.length === 0) {
      throw new Error("generated client settings are malformed");
    }
    values.set(key, value);
  }
  const endpoint = values.get("CALQUE_ENDPOINT");
  const bearerToken = values.get("CALQUE_BEARER_TOKEN");
  if (endpoint === undefined || bearerToken === undefined) {
    throw new Error("generated client settings are incomplete");
  }
  if (
    !/^http:\/\/127\.0\.0\.1:\d+$/u.test(endpoint) ||
    !/^[A-Za-z0-9_-]+$/u.test(bearerToken)
  ) {
    throw new Error("generated client settings violate the demo contract");
  }
  return Object.freeze({ endpoint, bearerToken });
}

export function sanitizePostgresRoutingEnvironment(
  environment: NodeJS.ProcessEnv,
): NodeJS.ProcessEnv {
  const sanitized: NodeJS.ProcessEnv = { ...environment };
  for (const name of postgresRoutingEnvironment) delete sanitized[name];
  return sanitized;
}

export function parsePort(value: string, name: string): number {
  if (!/^[1-9][0-9]*$/u.test(value)) {
    throw new Error(`${name} must be an integer from 1 to 65535`);
  }
  const port = Number.parseInt(value, 10);
  if (port > 65_535) {
    throw new Error(`${name} must be an integer from 1 to 65535`);
  }
  return port;
}

export function buildDemoRunUrl(origin: string, runId: string): string {
  if (
    !/^http:\/\/127\.0\.0\.1:\d+$/u.test(origin) ||
    !/^[A-Za-z0-9_-]{16,64}$/u.test(runId)
  ) {
    throw new Error("demo run URL inputs are invalid");
  }
  const url = new URL("/", origin);
  url.searchParams.set("run", runId);
  return url.toString();
}

export function buildDemoChromeArguments(
  url: string,
  profileDirectory: string,
  debug = false,
): string[] {
  if (
    !/^http:\/\/127\.0\.0\.1:\d+\/?(?:\?run=[A-Za-z0-9_-]{16,64})?$/u.test(
      url,
    ) ||
    !isAbsolute(profileDirectory) ||
    typeof debug !== "boolean"
  ) {
    throw new Error("demo Chrome inputs are invalid");
  }
  return [
    `--user-data-dir=${profileDirectory}`,
    "--disable-extensions",
    "--disable-sync",
    "--no-first-run",
    "--no-default-browser-check",
    "--use-mock-keychain",
    "--new-window",
    ...(debug ? ["--remote-debugging-port=0"] : []),
    url,
  ];
}

async function main(): Promise<void> {
  const options = parseDemoArguments(process.argv.slice(2));
  if (options.command === "change") {
    runDockerPsql(demoChangeSql, { inheritOutput: true });
    process.stdout.write(
      "\nOne PostgreSQL transaction changed a task and its project. The open dashboard will update automatically.\n",
    );
    return;
  }
  await start(options);
}

export interface ChildHandle {
  readonly label: string;
  readonly child: ChildProcess;
  readonly exited: Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>;
  /** The child's last lines of output, all of them once `closed` settles. */
  readonly output?: {
    readonly lines: readonly string[];
    readonly closed: Promise<void>;
  };
  readonly cleanup?: () => void;
}

async function start({
  open,
  fromSource,
}: {
  open: boolean;
  fromSource: boolean;
}): Promise<void> {
  // Decided before anything starts: a host that cannot open the page runs as
  // --no-open instead of building and starting everything and then failing.
  const browserUnavailable = open ? demoBrowserUnavailable() : undefined;
  if (browserUnavailable !== undefined) {
    process.stdout.write(
      `Not opening the page: ${browserUnavailable}. When Calque is ready, open the URL printed below in Chrome.\n`,
    );
  }
  const openPage = open && browserUnavailable === undefined;
  const postgresPort = parsePort(
    process.env.CALQUE_POSTGRES_PORT ?? "55432",
    "CALQUE_POSTGRES_PORT",
  );
  const serverPort = await selectPort(
    process.env.CALQUE_LOCAL_PORT,
    8787,
    "CALQUE_LOCAL_PORT",
    "local service",
  );
  const uiPort = await selectPort(
    process.env.CALQUE_DEMO_UI_PORT,
    5173,
    "CALQUE_DEMO_UI_PORT",
    "demo UI",
  );
  // Unset, the local service runs no metrics listener.
  const metricsValue = process.env.CALQUE_LOCAL_METRICS_PORT;
  const metricsPort =
    metricsValue === undefined
      ? undefined
      : parsePort(metricsValue, "CALQUE_LOCAL_METRICS_PORT");
  if (metricsPort !== undefined) {
    if (!fromSource)
      throw new Error(
        "CALQUE_LOCAL_METRICS_PORT needs --from-source: the image's server is not given a metrics listener",
      );
    await assertPortAvailable(metricsPort, "metrics listener");
  }
  const endpoint = `http://127.0.0.1:${serverPort}`;
  const uiOrigin = `http://127.0.0.1:${uiPort}`;
  const browserEndpoint = `${uiOrigin}/_calque/`;
  const runId = randomBytes(18).toString("base64url");
  const uiUrl = buildDemoRunUrl(uiOrigin, runId);
  const databaseUrl = `postgresql://${DEVELOPMENT_SOURCE_ROLE}:${DEVELOPMENT_SOURCE_ROLE}@127.0.0.1:${postgresPort}/calque_dev`;

  await superviseChildren("Demo", async ({ adopt, stopping }) => {
    // Before PostgreSQL: the server crates need no database to compile, and a
    // cold build or a pull that fails or takes minutes would otherwise hold
    // it up.
    let calque: { executable: string } | { image: string };
    if (fromSource) {
      heading("Building calque");
      calque = { executable: buildCalque() };
    } else {
      const image = demoImageReference(releaseVersion(root), process.env);
      heading(`Fetching ${image}`);
      ensureImage(image);
      calque = { image };
    }
    // The image's server is published from the PostgreSQL container, so
    // every compose command that starts it needs the layer that does.
    const composeFiles = fromSource
      ? []
      : ["-f", "docker-compose.yml", "-f", DEMO_IMAGE_COMPOSE_FILE];

    const postgresMajor = composePostgresMajor(
      readFileSync(join(root, "docker-compose.yml"), "utf8"),
    );
    heading(`Starting disposable PostgreSQL ${postgresMajor}`);
    runChecked(
      "docker",
      [
        "compose",
        ...composeFiles,
        "up",
        "-d",
        "--wait",
        "--wait-timeout",
        "60",
        "postgres",
      ],
      { env: { ...process.env, CALQUE_LOCAL_PORT: String(serverPort) } },
    );
    runDockerPsql(demoSeedSql);

    heading("Starting calque dev");
    const tables = configuredRelations(
      readFileSync(join(root, DEMO_BINDINGS.config), "utf8"),
    );
    const server = adopt(
      "executable" in calque
        ? startChild(
            "calque",
            calque.executable,
            buildDevArguments({
              stateDirectory,
              tables,
              uiOrigin,
              browserEndpoint,
              serverPort,
              metricsPort,
            }),
            {
              ...sanitizePostgresRoutingEnvironment(process.env),
              CALQUE_DATABASE_URL: databaseUrl,
            },
          )
        : startCalqueContainer({
            image: calque.image,
            serverPort,
            calqueArguments: buildDevArguments({
              stateDirectory: CONTAINER_STATE_DIRECTORY,
              tables,
              uiOrigin,
              browserEndpoint,
              serverPort,
              bindAddress: "0.0.0.0",
            }),
          }),
    );
    await waitForHttp(`${endpoint}/readyz`, server, 300_000, (response) =>
      isCalqueReadinessResponse(response),
    ).catch(async (error: unknown) => {
      throw await calqueStartFailure(error, server);
    });
    const clientSettings = parsePrivateClientEnvironment(
      readFileSync(clientEnvironmentPath, "utf8"),
    );
    if (clientSettings.endpoint !== endpoint) {
      throw new Error(
        "generated client endpoint differs from the local service",
      );
    }

    const previewEnvironment: NodeJS.ProcessEnv = {
      ...process.env,
      CALQUE_LOCAL_SERVICE_ENDPOINT: clientSettings.endpoint,
      CALQUE_LOCAL_BEARER_TOKEN: clientSettings.bearerToken,
    };

    heading("Building the demo page");
    runChecked("pnpm", ["--filter", "@calque/client...", "build"]);
    heading("Checking the generated bindings");
    runChecked("pnpm", ["--filter", "@calque/config", "build"]);
    runChecked("node", [...buildBindingsCheckArguments()]);
    runChecked("pnpm", ["--filter", "@calque/client", "manual:build"]);

    heading("Starting the demo page");
    const page = buildPageCommand(uiPort);
    const ui = adopt(
      startChild(
        "browser",
        page.command,
        page.arguments_,
        previewEnvironment,
        page.cwd,
      ),
    );
    await waitForHttp(uiOrigin, ui, 60_000, (response) => response.ok);
    await waitForHttp(
      `${browserEndpoint}readyz`,
      ui,
      60_000,
      isCalqueReadinessResponse,
    );

    process.stdout.write(
      [
        "",
        "Calque is ready.",
        `Open: ${uiUrl}`,
        "The Project Atlas dashboard loads its complete local view automatically.",
        "In another terminal run: pnpm demo:change",
        "Watch the task, project status, and portfolio metrics advance together.",
        "Press Ctrl-C here to stop it; PostgreSQL and local state are preserved.",
        "",
      ].join("\n"),
    );
    if (openPage) adopt(startDemoBrowser(uiUrl));

    const exited = await Promise.race(
      [server, ui].map((child) =>
        child.exited.then(({ code, signal }) => ({ child, code, signal })),
      ),
    );
    if (!stopping()) {
      throw new Error(
        `${exited.child.label} exited unexpectedly (${exited.code ?? exited.signal ?? "unknown"})`,
      );
    }
  });
}

/** What `superviseChildren` gives `run`. */
interface Supervision {
  /**
   * Puts a child under the stop and returns it. A child adopted once the stop
   * has begun is stopped at once, the stop waits for it, and `adopt` throws so
   * that `run` goes no further: calque can answer ready between a Ctrl-C and
   * its exit.
   */
  adopt(child: ChildHandle): ChildHandle;
  /** Whether the stop has begun. */
  stopping(): boolean;
}

/**
 * Runs `run`, and stops every child it adopts on Ctrl-C, on SIGTERM, or when
 * `run` throws, which it reports as `<name> failed`. It returns once that stop
 * has finished. A `StoppedBySignal` from `run` stops it as Ctrl-C does, with
 * exit code 0 and no failure message.
 *
 * The listeners use `process.on` and are never removed: a second Ctrl-C
 * signals node again, and with no listener left it would end node on the
 * default action mid-stop, leaving calque and the page, each leading its own
 * process group, running with no parent.
 *
 * On a second Ctrl-C, pnpm sends SIGTERM to the `sh` it runs the script in.
 * The root `demo` and `incidents` scripts begin with `exec` so that this node
 * is that process. Otherwise dash, the `sh` of Debian 12 and Ubuntu 24.04,
 * stays between pnpm and node, and pnpm exits when the SIGTERM ends it, while
 * this stop is still running.
 */
export async function superviseChildren(
  name: string,
  run: (supervision: Supervision) => Promise<void>,
): Promise<void> {
  const children: ChildHandle[] = [];
  let stopped: Promise<void> | undefined;
  const stop = (exitCode: number): Promise<void> => {
    stopped ??= (async () => {
      await Promise.allSettled(
        [...children].reverse().map((child) => stopChild(child)),
      );
      process.exitCode = exitCode;
    })();
    return stopped;
  };
  const adopt = (child: ChildHandle): ChildHandle => {
    if (stopped === undefined) {
      children.push(child);
      return child;
    }
    stopped = Promise.allSettled([stopped, stopChild(child)]).then(
      () => undefined,
    );
    throw new Error(`${child.label} started after the stop began`);
  };
  const onSignal = () => {
    if (stopped === undefined) heading("Stopping");
    void stop(0);
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
  try {
    await run({ adopt, stopping: () => stopped !== undefined });
  } catch (error) {
    // A Ctrl-C reaches a build step's child and this process together, and a
    // child that exits on it fails the step before this process's listener has
    // run. Two turns of the event loop put a poll phase between them, which is
    // where the listener runs, so that its stop makes this a stop.
    await new Promise((resolveTurn) => setImmediate(resolveTurn));
    await new Promise((resolveTurn) => setImmediate(resolveTurn));
    if (stopped === undefined) {
      if (error instanceof StoppedBySignal) {
        onSignal();
      } else {
        process.stderr.write(`\n${name} failed: ${formatError(error)}\n`);
        await stop(1);
      }
    }
  }
  await stopped;
}

/** Pulls `image` unless this host has it already. */
function ensureImage(image: string): void {
  const present = spawnSync("docker", ["image", "inspect", image], {
    stdio: "ignore",
  });
  if (present.status === 0) return;
  const pull = spawnSync("docker", ["pull", image], {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "inherit", "inherit"],
  });
  throwIfStoppedBySignal("docker", ["pull", image], pull);
  if (pull.status !== 0)
    throw new Error(
      `docker could not pull ${image}; its errors are above. To build calque from source instead, install Rust as docs/development.md says and run: pnpm demo -- --from-source`,
    );
}

/**
 * `calque dev` in its image, in the PostgreSQL container's network namespace.
 * `docker run` hands the SIGINT that stops it on to the container. Cleanup
 * removes a container that outlives a `docker` killed after its grace period.
 */
function startCalqueContainer({
  image,
  serverPort,
  calqueArguments,
}: {
  readonly image: string;
  readonly serverPort: number;
  readonly calqueArguments: readonly string[];
}): ChildHandle {
  // Created here, by this user: Docker would create a missing mount source as
  // root, and the container's user could not write to it.
  mkdirSync(stateDirectory, { recursive: true, mode: 0o700 });
  const postgresContainer = runChecked("docker", [
    "compose",
    "ps",
    "--quiet",
    "postgres",
  ]).stdout.trim();
  if (postgresContainer === "")
    throw new Error("docker compose started no postgres container");
  const name = `calque-demo-${serverPort}-${randomBytes(4).toString("hex")}`;
  const uid = process.getuid?.() ?? 10001;
  const gid = process.getgid?.() ?? 10001;
  const handle = startChild(
    "calque",
    "docker",
    buildCalqueContainerArguments({
      image,
      name,
      postgresContainer,
      stateDirectory,
      user: `${uid}:${gid}`,
      calqueArguments,
    }),
    {
      ...sanitizePostgresRoutingEnvironment(process.env),
      CALQUE_DATABASE_URL: containerDatabaseUrl(),
    },
  );
  return {
    ...handle,
    cleanup: () => {
      spawnSync("docker", ["rm", "--force", name], { stdio: "ignore" });
    },
  };
}

/** The source as the container sees it: PostgreSQL's own loopback address. */
function containerDatabaseUrl(): string {
  return `postgresql://${DEVELOPMENT_SOURCE_ROLE}:${DEVELOPMENT_SOURCE_ROLE}@127.0.0.1:5432/calque_dev`;
}

function buildCalque(): string {
  const arguments_ = buildCalqueBuildArguments();
  const result = spawnSync("cargo", arguments_, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
    stdio: ["ignore", "pipe", "inherit"],
  });
  if (result.error)
    throw new Error(
      `cargo ${arguments_.join(" ")} failed: ${formatError(result.error)}`,
    );
  throwIfStoppedBySignal("cargo", arguments_, result);
  if (result.status !== 0)
    throw new Error(
      `cargo ${arguments_.join(" ")} failed (${result.status ?? result.signal ?? "unknown"}); its errors are above`,
    );
  return calqueExecutable(result.stdout);
}

function runDockerPsql(
  sql: string,
  { inheritOutput = false }: { inheritOutput?: boolean } = {},
): void {
  runChecked(
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
    ],
    {
      input: sql,
      stdio: inheritOutput ? ["pipe", "inherit", "inherit"] : "pipe",
    },
  );
}

/**
 * A step's child ended on SIGINT or SIGTERM: a Ctrl-C or a stop, which
 * `superviseChildren` stops on as it does on its own signal, not a failure.
 */
export class StoppedBySignal extends Error {
  constructor(
    command: string,
    arguments_: readonly string[],
    signal: NodeJS.Signals,
  ) {
    super(`${command} ${arguments_.join(" ")} ended on ${signal}`);
    this.name = "StoppedBySignal";
  }
}

export function throwIfStoppedBySignal(
  command: string,
  arguments_: readonly string[],
  result: SpawnSyncReturns<string>,
): void {
  // A timeout or an overflowing buffer also ends the child with SIGTERM, and
  // sets `error`: that is a failure.
  if (
    !result.error &&
    (result.signal === "SIGINT" || result.signal === "SIGTERM")
  )
    throw new StoppedBySignal(command, arguments_, result.signal);
}

export function runChecked(
  command: string,
  arguments_: readonly string[],
  options: Omit<SpawnSyncOptions, "cwd" | "encoding"> = {},
): SpawnSyncReturns<string> {
  const result = spawnSync(command, arguments_, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
    ...options,
  });
  throwIfStoppedBySignal(command, arguments_, result);
  if (result.error || result.status !== 0) {
    const detail = [result.stderr, result.stdout]
      .filter((value) => typeof value === "string" && value.trim().length > 0)
      .join("\n")
      .trim();
    throw new Error(
      `${command} ${arguments_.join(" ")} failed${detail ? `: ${detail}` : ""}`,
    );
  }
  return result;
}

export function startChild(
  label: string,
  command: string,
  arguments_: readonly string[],
  environment: NodeJS.ProcessEnv,
  cwd: string = root,
): ChildHandle {
  const child = spawn(command, arguments_, {
    cwd,
    env: environment,
    stdio: ["ignore", "pipe", "pipe"],
    // Its own process group, so `signalChild` can signal the whole tree. A
    // grandchild left running holds the write end of these pipes, which keeps
    // this process's event loop alive, so the launcher would never exit on
    // Ctrl-C.
    detached: true,
  });
  const lines: string[] = [];
  pipeLines(child.stdout, label, process.stdout, lines);
  pipeLines(child.stderr, label, process.stderr, lines);
  // `exit` can come before the pipes have delivered the child's last lines.
  const closed = new Promise<void>((resolveClose) => {
    child.once("close", () => resolveClose());
  });
  const exited = new Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>((resolveExit, rejectExit) => {
    child.once("error", rejectExit);
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
  });
  return { label, child, exited, output: { lines, closed } };
}

const START_OVER =
  "To start over, `docker compose down -v && rm -rf .calque-dev` removes the demo database, its volume and the state calque dev kept for it. Then run pnpm demo again.";

/**
 * What `pnpm demo` reports when calque did not become ready. A refusal whose
 * remedy is `calque destroy` names a command the demo's user cannot run as
 * printed: the image's state directory is `/state` inside a container, and a
 * source build puts no `calque` on the PATH.
 */
export async function calqueStartFailure(
  error: unknown,
  server: ChildHandle,
): Promise<unknown> {
  const output = server.output;
  if (output === undefined) return error;
  const grace = countdown(2_000);
  try {
    await Promise.race([output.closed, grace.expired]);
  } finally {
    grace.cancel();
  }
  return output.lines.some((line) => line.includes("Run calque destroy"))
    ? new Error(`${formatError(error)}. ${START_OVER}`)
    : error;
}

export async function stopChild(owner: ChildHandle): Promise<void> {
  try {
    if (owner.child.exitCode !== null || owner.child.signalCode !== null)
      return;
    signalChild(owner.child, owner.label === "calque" ? "SIGINT" : "SIGTERM");
    const grace = countdown(10_000);
    let graceful: boolean;
    try {
      graceful = await Promise.race([
        owner.exited.then(() => true),
        grace.expired,
      ]);
    } finally {
      // The child usually wins this race in well under a second, and the
      // losing timer would otherwise stay armed: an armed `setTimeout` holds
      // the event loop open, so Ctrl-C on `pnpm demo` would cost the full
      // grace period per child after every child had already exited.
      grace.cancel();
    }
    if (!graceful) {
      signalChild(owner.child, "SIGKILL");
      await owner.exited;
    }
  } finally {
    owner.cleanup?.();
  }
}

// `NodeJS.ErrnoException` is Node's type for an `Error` a syscall failure
// attached a `code` to.
function errorCode(error: unknown): string | undefined {
  return error instanceof Error
    ? (error as NodeJS.ErrnoException).code
    : undefined;
}

/**
 * Signals the child's whole process group, falling back to the child alone.
 * A child spawned without `detached`, as `startDemoBrowser` spawns Chrome,
 * leads no group, so signalling its pid as a group is `ESRCH`. A group that has
 * already gone is `ESRCH` too, and then the fallback finds the child gone and
 * signals nothing.
 */
function signalChild(child: ChildProcess, signal: NodeJS.Signals): void {
  const pid = child.pid;
  if (pid !== undefined) {
    try {
      process.kill(-pid, signal);
      return;
    } catch (error) {
      const code = errorCode(error);
      if (code !== "ESRCH" && code !== "EPERM" && code !== "EINVAL")
        throw error;
    }
  }
  try {
    child.kill(signal);
  } catch (error) {
    if (errorCode(error) !== "ESRCH") throw error;
  }
}

const RECENT_LINES = 20;

function pipeLines(
  stream: Readable,
  label: string,
  output: NodeJS.WritableStream,
  recent: string[],
): void {
  let pending = "";
  const emit = (line: string): void => {
    output.write(`[${label}] ${line}\n`);
    recent.push(line);
    if (recent.length > RECENT_LINES) recent.shift();
  };
  stream.setEncoding("utf8");
  stream.on("data", (chunk: string) => {
    pending += chunk;
    const lines = pending.split(/\r?\n/u);
    pending = lines.pop() ?? "";
    for (const line of lines) emit(line);
  });
  stream.on("end", () => {
    if (pending.length > 0) emit(pending);
  });
}

export async function waitForHttp(
  url: string,
  owner: ChildHandle,
  timeoutMs: number,
  acceptsResponse: (response: Response) => boolean,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const state = await Promise.race([
      fetch(url, { signal: AbortSignal.timeout(1_000) })
        .then((response) => (acceptsResponse(response) ? "ready" : "waiting"))
        .catch(() => "waiting"),
      owner.exited.then(() => "exited"),
    ]);
    if (state === "ready") return;
    if (state === "exited") {
      throw new Error(`${owner.label} exited before becoming ready`);
    }
    await delay(250);
  }
  throw new Error(`${owner.label} did not become ready within ${timeoutMs}ms`);
}

export function isCalqueReadinessResponse(response: Response): boolean {
  return (
    response.status === 204 &&
    response.headers.get("x-content-type-options") === "nosniff" &&
    response.headers.get("referrer-policy") === "no-referrer"
  );
}

export async function selectPort(
  environmentValue: string | undefined,
  preferred: number,
  environmentName: string,
  label: string,
): Promise<number> {
  if (environmentValue !== undefined) {
    const port = parsePort(environmentValue, environmentName);
    await assertPortAvailable(port, label);
    return port;
  }
  for (let port = preferred; port < preferred + 100; port += 1) {
    try {
      await assertPortAvailable(port, label);
      return port;
    } catch (error) {
      if (!(error instanceof PortInUseError)) throw error;
    }
  }
  throw new Error(`${label} has no free port near ${preferred}`);
}

class PortInUseError extends Error {
  readonly code = "DEMO_PORT_IN_USE";
}

async function assertPortAvailable(port: number, label: string): Promise<void> {
  if (await acceptsTcpConnection(port)) {
    throw new PortInUseError(`${label} port ${port} is already in use`);
  }
  const server = createServer();
  await new Promise<void>((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen({ host: "127.0.0.1", port, exclusive: true }, resolveListen);
  }).catch((error: unknown) => {
    if (errorCode(error) === "EADDRINUSE") {
      throw new PortInUseError(`${label} port ${port} is already in use`);
    }
    throw error;
  });
  await new Promise<void>((resolveClose, rejectClose) =>
    server.close((error) => (error ? rejectClose(error) : resolveClose())),
  );
}

async function acceptsTcpConnection(port: number): Promise<boolean> {
  return await new Promise<boolean>((resolveConnection) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    const finish = (connected: boolean) => {
      socket.destroy();
      resolveConnection(connected);
    };
    socket.setTimeout(250, () => finish(false));
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
}

const DEMO_CHROME =
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

/**
 * Why this host cannot open the page itself, or `undefined` when it can.
 * `startDemoBrowser` launches only macOS's Google Chrome at `DEMO_CHROME`.
 */
export function demoBrowserUnavailable(
  platform: NodeJS.Platform = process.platform,
  exists: (path: string) => boolean = existsSync,
): string | undefined {
  if (platform !== "darwin")
    return "the launcher opens the page itself only on macOS";
  if (!exists(DEMO_CHROME)) return `Google Chrome is not at ${DEMO_CHROME}`;
  return undefined;
}

export function startDemoBrowser(url: string): ChildHandle {
  const unavailable = demoBrowserUnavailable();
  if (unavailable !== undefined) throw new Error(unavailable);
  const configuredProfile = process.env.CALQUE_DEMO_CHROME_PROFILE;
  if (configuredProfile !== undefined && !isAbsolute(configuredProfile)) {
    throw new Error("CALQUE_DEMO_CHROME_PROFILE must be an absolute path");
  }
  const debugValue = process.env.CALQUE_DEMO_CHROME_DEBUG;
  if (debugValue !== undefined && debugValue !== "1") {
    throw new Error("CALQUE_DEMO_CHROME_DEBUG must be 1 when provided");
  }
  const profileDirectory =
    configuredProfile ?? mkdtempSync(join(tmpdir(), "calque-demo-chrome-"));
  mkdirSync(profileDirectory, { recursive: true, mode: 0o700 });
  const child = spawn(
    DEMO_CHROME,
    buildDemoChromeArguments(url, profileDirectory, debugValue === "1"),
    {
      cwd: root,
      stdio: "ignore",
    },
  );
  const exited = new Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>((resolveExit, rejectExit) => {
    child.once("error", rejectExit);
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
  });
  return {
    label: "chrome",
    child,
    exited,
    cleanup: () => {
      if (configuredProfile === undefined) {
        rmSync(profileDirectory, { recursive: true, force: true });
      }
    },
  };
}

export function heading(message: string): void {
  process.stdout.write(`\n==> ${message}\n`);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

/** A deadline for a `Promise.race` that can be disarmed when it loses. */
function countdown(milliseconds: number): {
  readonly expired: Promise<false>;
  cancel(): void;
} {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<false>((resolveExpiry) => {
    timer = setTimeout(() => resolveExpiry(false), milliseconds);
  });
  return {
    expired,
    cancel(): void {
      if (timer !== undefined) clearTimeout(timer);
    },
  };
}

export function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === fileURLToPath(import.meta.url)) {
  await main();
}
