import assert from "node:assert/strict";
import test from "node:test";

import { spawn, type ChildProcess } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";

import {
  demoBrowserUnavailable,
  buildDemoChromeArguments,
  buildBindingsCheckArguments,
  buildCalqueBuildArguments,
  buildDevArguments,
  calqueExecutable,
  composePostgresMajor,
  configuredRelations,
  buildDemoRunUrl,
  calqueStartFailure,
  type ChildHandle,
  DEMO_BINDINGS,
  formatError,
  parseDemoArguments,
  parsePort,
  parsePrivateClientEnvironment,
  runChecked,
  sanitizePostgresRoutingEnvironment,
  startChild,
  stopChild,
  StoppedBySignal,
  waitForHttp,
} from "./demo.ts";
import {
  buildCalqueContainerArguments,
  DEMO_IMAGE_REPOSITORY,
  demoImageReference,
  releaseVersion,
} from "./lib/demo-image.ts";

test("the browser pnpm demo opens uses an isolated extension-free Chrome profile", () => {
  assert.deepEqual(
    buildDemoChromeArguments(
      "http://127.0.0.1:5173/?run=0123456789abcdef",
      "/tmp/calque-demo-profile",
      true,
    ),
    [
      "--user-data-dir=/tmp/calque-demo-profile",
      "--disable-extensions",
      "--disable-sync",
      "--no-first-run",
      "--no-default-browser-check",
      "--use-mock-keychain",
      "--new-window",
      "--remote-debugging-port=0",
      "http://127.0.0.1:5173/?run=0123456789abcdef",
    ],
  );
  assert.throws(() =>
    buildDemoChromeArguments(
      "https://example.com/",
      "/tmp/calque-demo-profile",
    ),
  );
  assert.throws(() =>
    buildDemoChromeArguments(
      "http://127.0.0.1:5173/?run=0123456789abcdef",
      "relative-profile",
    ),
  );
});

test("each launcher run gets an exact cache-busting browser URL", () => {
  assert.equal(
    buildDemoRunUrl("http://127.0.0.1:5173", "0123456789abcdef"),
    "http://127.0.0.1:5173/?run=0123456789abcdef",
  );
  for (const [origin, runId] of [
    ["http://localhost:5173", "0123456789abcdef"],
    ["http://127.0.0.1:5173/path", "0123456789abcdef"],
    ["http://127.0.0.1:5173", "short"],
    ["http://127.0.0.1:5173", "invalid/value/abcdef"],
  ] as const) {
    assert.throws(() => buildDemoRunUrl(origin, runId));
  }
});

test("pnpm demo has one-command defaults, an explicit no-open mode and a source build", () => {
  const start = (open: boolean, fromSource: boolean) => ({
    command: "start",
    open,
    fromSource,
  });
  assert.deepEqual(parseDemoArguments([]), start(true, false));
  assert.deepEqual(parseDemoArguments(["--no-open"]), start(false, false));
  assert.deepEqual(
    parseDemoArguments(["--", "--no-open"]),
    start(false, false),
  );
  assert.deepEqual(
    parseDemoArguments(["start", "--no-open"]),
    start(false, false),
  );
  assert.deepEqual(
    parseDemoArguments(["--", "--from-source"]),
    start(true, true),
  );
  assert.deepEqual(
    parseDemoArguments(["--no-open", "--from-source"]),
    start(false, true),
  );
  assert.deepEqual(parseDemoArguments(["change"]), {
    command: "change",
    open: false,
  });
  for (const refused of [
    ["destroy"],
    ["--no-open", "--no-open"],
    ["--from-source", "start"],
    ["change", "--from-source"],
  ])
    assert.throws(() => parseDemoArguments(refused), /usage/u, `${refused}`);
});

test("private client settings are parsed narrowly without evaluating shell", () => {
  assert.deepEqual(
    parsePrivateClientEnvironment(
      [
        "CALQUE_ENDPOINT=http://127.0.0.1:8787",
        "CALQUE_BEARER_TOKEN=abc_DEF-123",
        "",
      ].join("\n"),
    ),
    {
      endpoint: "http://127.0.0.1:8787",
      bearerToken: "abc_DEF-123",
    },
  );
  for (const [contents, refusal] of [
    ["CALQUE_ENDPOINT=http://127.0.0.1:8787\n", /incomplete/u],
    [
      "CALQUE_ENDPOINT=http://localhost:8787\nCALQUE_BEARER_TOKEN=x\n",
      /demo contract/u,
    ],
    [
      "CALQUE_ENDPOINT=http://127.0.0.1:8787\nCALQUE_BEARER_TOKEN=x\nCALQUE_SCOPE_KEY=local-dev\n",
      /malformed/u,
    ],
    [
      "CALQUE_ENDPOINT=http://127.0.0.1:8787\nCALQUE_BEARER_TOKEN=x\nEXTRA=value\n",
      /malformed/u,
    ],
    [
      "CALQUE_ENDPOINT=http://127.0.0.1:8787\nCALQUE_ENDPOINT=http://127.0.0.1:8788\nCALQUE_BEARER_TOKEN=x\n",
      /malformed/u,
    ],
  ] as const) {
    assert.throws(() => parsePrivateClientEnvironment(contents), refusal);
  }
});

test("PostgreSQL routing variables are removed without mutating other settings", () => {
  const original = {
    PATH: "/bin",
    HOME: "/tmp/home",
    PGHOST: "elsewhere",
    PGSERVICE: "remote",
    PGPORT: "6543",
  };
  assert.deepEqual(sanitizePostgresRoutingEnvironment(original), {
    PATH: "/bin",
    HOME: "/tmp/home",
  });
  assert.equal(original.PGHOST, "elsewhere");
});

test("pnpm demo's ports are finite nonzero TCP ports", () => {
  assert.equal(parsePort("1", "PORT"), 1);
  assert.equal(parsePort("65535", "PORT"), 65_535);
  for (const value of ["", "0", "65536", "-1", "1.5", "abc"]) {
    assert.throws(() => parsePort(value, "PORT"), /1 to 65535/u);
  }
});

test("the announced PostgreSQL major comes from the compose file", () => {
  // The test reads the real file, because a fixture cannot catch the banner
  // disagreeing with the compose file.
  const major = composePostgresMajor(
    readFileSync("docker-compose.yml", "utf8"),
  );
  assert.equal(typeof major, "number");
  assert.ok(
    major >= 18,
    `compose pins postgres:${major}; enrollment requires 18`,
  );
});

test("a compose file with no pinned postgres image is refused", () => {
  assert.throws(
    () => composePostgresMajor("services:\n  postgres:\n    image: postgres\n"),
    /does not pin a postgres image tag/u,
  );
  assert.throws(() => composePostgresMajor(""), /does not pin/u);
});

test("the compose healthcheck probes PostgreSQL over TCP", () => {
  // Over the Unix socket, pg_isready also reports the entrypoint's temporary
  // initdb server ready, so `up --wait` would return while initialization
  // still runs and the seed would fail with "the database system is shutting
  // down".
  const compose = readFileSync("docker-compose.yml", "utf8");
  const probe =
    /^\s*healthcheck:\s*\n(?:\s*#[^\n]*\n)*\s*test:\s*\[\s*"CMD-SHELL",\s*"([^"]*)"/mu.exec(
      compose,
    )?.[1];
  assert.ok(probe, "docker-compose.yml has no CMD-SHELL healthcheck");
  assert.match(probe, /^pg_isready\s(?:.*\s)?-h 127\.0\.0\.1(?:\s|$)/u);
});

test("pnpm demo's page shows the Calque brand mark", () => {
  const html = readFileSync(
    "client/packages/client/tests/demo/index.html",
    "utf8",
  );
  assert.match(html, /<span class="brand-mark" aria-hidden="true">C<\/span>/u);
});

test("pnpm demo's pinned bindings are complete and its check is a drift check", () => {
  for (const path of Object.values(DEMO_BINDINGS)) {
    assert.ok(readFileSync(path, "utf8").length > 0, path);
  }
  const config = readFileSync(DEMO_BINDINGS.config, "utf8");
  assert.deepEqual(configuredRelations(config), [
    "public.projects",
    "public.tasks",
  ]);
  assert.deepEqual(buildBindingsCheckArguments(), [
    "client/packages/config/bin/calque-bindings.js",
    "--config",
    DEMO_BINDINGS.config,
    "--out",
    DEMO_BINDINGS.output,
    "--check",
  ]);
  const generated = readFileSync(DEMO_BINDINGS.output, "utf8");
  assert.match(generated, /tableId: "public\.projects"/u);
  assert.match(generated, /tableId: "public\.tasks"/u);
  assert.throws(
    () => configuredRelations(JSON.stringify({ collections: [] })),
    /declares no collections/u,
  );
});

test("pnpm demo runs the bindings check before it builds the page", () => {
  // The argument builder above proves the spelling; this proves the launcher
  // runs it first. Deleting the `runChecked` call from `start()` leaves every
  // other test in this file green.
  const source = readFileSync("scripts/demo.ts", "utf8");
  const check = source.search(
    /runChecked\(\s*"node",\s*\[\s*\.\.\.buildBindingsCheckArguments\(\)/u,
  );
  const pageBuild = source.search(/"manual:build"/u);
  assert.ok(check > 0, "start() does not run the bindings check");
  assert.ok(pageBuild > 0, "start() does not build the page");
  assert.ok(
    check < pageBuild,
    "the page is built before its bindings are checked",
  );
});

test("the local service is started for the configured relations, without a schema emitter", () => {
  const arguments_ = buildDevArguments({
    stateDirectory: "/tmp/state",
    tables: ["public.projects", "public.tasks"],
    uiOrigin: "http://127.0.0.1:5173",
    browserEndpoint: "http://127.0.0.1:5173/_calque/",
    serverPort: 8787,
  });
  assert.deepEqual(arguments_, [
    "dev",
    "--state-dir",
    "/tmp/state",
    "--table",
    "public.projects",
    "--table",
    "public.tasks",
    "--allow-origin",
    "http://127.0.0.1:5173",
    "--public-base-url",
    "http://127.0.0.1:5173/_calque/",
    "--port",
    "8787",
    "--acknowledge-disposable-source",
  ]);
  assert.ok(!arguments_.includes("--metrics-port"));
});

test("the local service binds an address only when it is given one", () => {
  const options = {
    stateDirectory: "/state",
    tables: ["public.tasks"],
    uiOrigin: "http://127.0.0.1:5173",
    browserEndpoint: "http://127.0.0.1:5173/_calque/",
    serverPort: 8787,
  };
  assert.ok(!buildDevArguments(options).includes("--bind-address"));
  const arguments_ = buildDevArguments({ ...options, bindAddress: "0.0.0.0" });
  assert.equal(arguments_[arguments_.indexOf("--bind-address") + 1], "0.0.0.0");
});

test("the local service serves metrics only when it is given a metrics port", () => {
  const arguments_ = buildDevArguments({
    stateDirectory: "/tmp/state",
    tables: ["public.tasks"],
    uiOrigin: "http://127.0.0.1:5173",
    browserEndpoint: "http://127.0.0.1:5173/_calque/",
    serverPort: 8787,
    metricsPort: 8788,
  });
  const flag = arguments_.indexOf("--metrics-port");
  assert.ok(flag > 0, "the metrics port is not passed");
  assert.equal(arguments_[flag + 1], "8788");
});

test("calque is built by a cargo build into this checkout's target directory, not compiled by cargo run", () => {
  // A build is a step of its own, outside the readiness window, and its JSON
  // messages name the executable the launcher then starts. A target directory
  // other worktrees share could hand it another worktree's binary.
  assert.deepEqual(buildCalqueBuildArguments(), [
    "build",
    "--target-dir",
    fileURLToPath(new URL("../target", import.meta.url)),
    "-p",
    "calque",
    "--message-format=json-render-diagnostics",
  ]);
});

test("the executable is the calque binary cargo reported", () => {
  const lines = [
    "   Compiling calque v0.1.0",
    JSON.stringify({
      reason: "compiler-artifact",
      target: { name: "calque-engine" },
      executable: null,
    }),
    JSON.stringify({
      reason: "compiler-artifact",
      target: { name: "calque" },
      executable: "/target/debug/calque",
    }),
    JSON.stringify({ reason: "build-finished", success: true }),
  ];
  assert.equal(calqueExecutable(lines.join("\n")), "/target/debug/calque");
  assert.throws(
    () => calqueExecutable(lines.slice(0, 2).join("\n")),
    /no calque executable/u,
  );
});

test("pnpm demo builds or pulls calque before docker starts anything, and gives only the start its readiness window", () => {
  // A cold first build can outlast a fixed window, and a build after
  // `docker compose up` would hold PostgreSQL up while it runs or fails.
  // Moving the build or the pull back after either, or starting `cargo`
  // again, leaves every test of the exported functions above green.
  const source = readFileSync("scripts/demo.ts", "utf8");
  const body = source.slice(source.search(/^async function start\(/mu));
  const build = body.search(/buildCalque\(\)/u);
  const pull = body.search(/ensureImage\(image\)/u);
  const firstDocker = body.search(/runChecked\(\s*"docker"|runDockerPsql\(/u);
  const serverStart = body.search(
    /startChild\(\s*"calque",\s*calque\.executable,/u,
  );
  const containerStart = body.search(/startCalqueContainer\(/u);
  const readiness = body.search(/waitForHttp\(`\$\{endpoint\}\/readyz`/u);
  assert.ok(build > 0, "start() does not build calque");
  assert.ok(pull > 0, "start() does not pull the calque image");
  assert.ok(firstDocker > build, "start() runs docker before building calque");
  assert.ok(firstDocker > pull, "start() runs docker before pulling calque");
  assert.ok(serverStart > build, "start() starts calque before building it");
  assert.ok(containerStart > pull, "start() starts calque before pulling it");
  assert.ok(readiness > serverStart, "start() waits before starting calque");
  assert.ok(readiness > containerStart, "start() waits before starting calque");
  assert.doesNotMatch(body, /startChild\(\s*"calque",\s*"cargo"/u);
});

test("the page opens itself only on macOS with Google Chrome in /Applications", () => {
  const chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
  const checked: string[] = [];
  const present = (path: string) => {
    checked.push(path);
    return path === chrome;
  };
  assert.equal(demoBrowserUnavailable("darwin", present), undefined);
  assert.deepEqual(checked, [chrome]);
  assert.match(
    demoBrowserUnavailable("darwin", () => false) ?? "",
    /Google Chrome is not at \/Applications\/Google Chrome\.app/u,
  );
  for (const platform of ["linux", "win32", "freebsd"] as const)
    assert.match(
      demoBrowserUnavailable(platform, present) ?? "",
      /only on macOS/u,
    );
});

test("pnpm demo decides whether it can open the page before it starts anything", () => {
  // Deciding later, a host that cannot open the page would build and start
  // everything, print that it is ready, and then fail and tear it all
  // down.
  const source = readFileSync("scripts/demo.ts", "utf8");
  const body = source.slice(source.search(/^async function start\(/mu));
  const decision = body.search(/demoBrowserUnavailable\(\)/u);
  const build = body.search(/buildCalque\(\)/u);
  const firstStart = body.search(/runChecked\(\s*"docker"/u);
  assert.ok(decision > 0, "start() does not ask whether it can open the page");
  assert.ok(build > decision, "start() builds calque first");
  assert.ok(firstStart > decision, "start() starts PostgreSQL first");
  assert.match(body, /if \(openPage\) adopt\(startDemoBrowser/u);
});

test("the image is the published one at this checkout's release version unless CALQUE_DEMO_IMAGE names another", () => {
  assert.equal(DEMO_IMAGE_REPOSITORY, "ghcr.io/calque-sh/calque");
  assert.equal(
    demoImageReference("0.1.0", {}),
    "ghcr.io/calque-sh/calque:0.1.0",
  );
  assert.equal(
    demoImageReference("0.1.0", { CALQUE_DEMO_IMAGE: "calque-local:dev" }),
    "calque-local:dev",
  );
  for (const refused of ["", "--privileged", "a b", "Image;rm"])
    assert.throws(
      () => demoImageReference("0.1.0", { CALQUE_DEMO_IMAGE: refused }),
      /image reference/u,
      refused,
    );
  assert.match(releaseVersion(process.cwd()), /^\d+\.\d+\.\d+/u);
});

test("the server's image is run in PostgreSQL's network namespace with the connection string out of its arguments", () => {
  const arguments_ = buildCalqueContainerArguments({
    image: "ghcr.io/calque-sh/calque:0.1.0",
    name: "calque-demo-8787-ab12",
    postgresContainer: "0123abcd",
    stateDirectory: "/work/.calque-dev",
    user: "501:20",
    calqueArguments: ["dev", "--port", "8787"],
  });
  assert.deepEqual(arguments_, [
    "run",
    "--rm",
    "--name",
    "calque-demo-8787-ab12",
    "--network",
    "container:0123abcd",
    "--user",
    "501:20",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--volume",
    "/work/.calque-dev:/state",
    "--env",
    "CALQUE_DATABASE_URL",
    "ghcr.io/calque-sh/calque:0.1.0",
    "dev",
    "--port",
    "8787",
  ]);
  // A value after `--env NAME=` would put the credentials in `ps`.
  assert.ok(!arguments_.some((argument) => argument.includes("postgresql:")));
});

test("the image layer publishes the server's port on loopback only, from the PostgreSQL container", () => {
  const layer = readFileSync("docker-compose.demo-image.yml", "utf8");
  assert.match(
    layer,
    /ports:\s*- "127\.0\.0\.1:\$\{CALQUE_LOCAL_PORT:\?[^}]*\}:\$\{CALQUE_LOCAL_PORT\}"/u,
  );
  assert.match(layer, /^services:\n {2}postgres:/mu);
});

// A node process that does nothing until it is signalled. It installs no
// signal handler, so SIGTERM ends it.
const IDLE = "setInterval(() => {}, 1_000);";

function exitOf(child: ChildProcess): ChildHandle["exited"] {
  return new Promise((resolveExit, rejectExit) => {
    child.once("error", rejectExit);
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
  });
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

async function goneWithin(pid: number, milliseconds: number): Promise<boolean> {
  const deadline = Date.now() + milliseconds;
  while (alive(pid)) {
    if (Date.now() >= deadline) return false;
    await sleep(25);
  }
  return true;
}

// `stopChild`'s grace period is 10 s, and a child that SIGTERM reaches is gone
// in milliseconds. The bound sits under the grace period, so a stop that ends
// the child only with the SIGKILL that follows it fails as well. The timer is
// unref'd so that a stop that wins does not keep the test file alive until the
// timer fires.
function stopWithin(
  owner: ChildHandle,
  milliseconds: number,
): Promise<"stopped" | "still waiting"> {
  return Promise.race([
    stopChild(owner).then(() => "stopped" as const),
    sleep(milliseconds, "still waiting" as const, { ref: false }),
  ]);
}

test(
  "stopChild ends a child that leads no process group, as Chrome is spawned",
  { timeout: 60_000 },
  async () => {
    // `startDemoBrowser` spawns Chrome without `detached`, so Chrome's pid
    // names no process group and signalling it as one is ESRCH.
    const child = spawn(process.execPath, ["-e", IDLE], { stdio: "ignore" });
    const owner: ChildHandle = { label: "idle", child, exited: exitOf(child) };
    try {
      const pid = child.pid;
      assert.ok(pid !== undefined);
      assert.equal(await stopWithin(owner, 9_000), "stopped");
      assert.equal(child.signalCode, "SIGTERM");
      assert.equal(alive(pid), false);
    } finally {
      if (child.exitCode === null && child.signalCode === null)
        child.kill("SIGKILL");
    }
  },
);

test(
  "stopChild ends a detached child together with the grandchild it spawned",
  { timeout: 60_000 },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "calque-stop-child-"));
    const pidFile = join(directory, "grandchild.pid");
    // The grandchild is not detached, so it shares the child's process group,
    // and it outlives a signal sent to the child alone.
    const parent = [
      'const { spawn } = require("node:child_process");',
      'const { renameSync, writeFileSync } = require("node:fs");',
      `const grandchild = spawn(process.execPath, ["-e", ${JSON.stringify(IDLE)}], { stdio: "ignore" });`,
      `writeFileSync(${JSON.stringify(`${pidFile}.partial`)}, String(grandchild.pid));`,
      `renameSync(${JSON.stringify(`${pidFile}.partial`)}, ${JSON.stringify(pidFile)});`,
      IDLE,
    ].join("\n");
    const owner = startChild(
      "tree",
      process.execPath,
      ["-e", parent],
      process.env,
    );
    let grandchild: number | undefined;
    try {
      const deadline = Date.now() + 30_000;
      while (!existsSync(pidFile)) {
        assert.ok(
          Date.now() < deadline,
          "the child never spawned its grandchild",
        );
        await sleep(25);
      }
      grandchild = Number(readFileSync(pidFile, "utf8"));
      assert.ok(Number.isInteger(grandchild) && grandchild > 0);
      assert.ok(alive(grandchild));

      assert.equal(await stopWithin(owner, 9_000), "stopped");
      assert.equal(owner.child.signalCode, "SIGTERM");
      assert.ok(
        await goneWithin(grandchild, 5_000),
        "the grandchild outlived stopChild",
      );
    } finally {
      if (owner.child.exitCode === null && owner.child.signalCode === null)
        owner.child.kill("SIGKILL");
      if (grandchild !== undefined && alive(grandchild))
        process.kill(grandchild, "SIGKILL");
      rmSync(directory, { recursive: true, force: true });
    }
  },
);

/** How `pnpm demo` reports a calque that exits with `stderr` before ready. */
async function demoFailureAfter(stderr: string): Promise<string> {
  const calque = startChild(
    "calque",
    process.execPath,
    ["-e", `process.stderr.write(${JSON.stringify(stderr)}); process.exit(1);`],
    process.env,
  );
  // Nothing listens on port 1, so only the exit ends the wait.
  const error = await waitForHttp(
    "http://127.0.0.1:1/readyz",
    calque,
    20_000,
    () => true,
  ).then(
    () => assert.fail("a calque that exited became ready"),
    (failure: unknown) => failure,
  );
  return formatError(await calqueStartFailure(error, calque));
}

test(
  "a calque refusal whose remedy is calque destroy ends pnpm demo with the command that starts over",
  { timeout: 60_000 },
  async () => {
    // As the image prints it: a state directory inside the container, and no
    // newline before the exit, so the line arrives only as the pipe closes.
    const refusal =
      "calque: this source was enrolled by another state directory at the same path. Run calque destroy --state-dir /state --database-url-env CALQUE_DATABASE_URL --yes, then rerun dev to prepare the working set anew.";
    assert.equal(
      await demoFailureAfter(refusal),
      "calque exited before becoming ready. To start over, `docker compose down -v && rm -rf .calque-dev` removes the demo database, its volume and the state calque dev kept for it. Then run pnpm demo again.",
    );
    assert.equal(
      await demoFailureAfter("calque: CALQUE_DATABASE_URL is not set.\n"),
      "calque exited before becoming ready",
    );
  },
);

interface FixtureRun {
  readonly exit: { code: number | null; signal: NodeJS.Signals | null };
  readonly output: string;
  /** The fixture's exit code, or undefined when a signal ended its node. */
  readonly record: string | undefined;
  /** The fixture's processes still running when `pnpm run` exited. */
  readonly survivors: readonly string[];
}

/** The running fixture, as a test drives it. */
interface Fixture {
  /** SIGINT to pnpm's process group and to the launcher's, as one Ctrl-C. */
  interrupt(): void;
  /** The first line of the output matching `pattern`, once it is printed. */
  line(pattern: RegExp): Promise<RegExpExecArray>;
  /** The pids the fixture has printed so far. */
  pids(): Partial<Record<"launcher" | "calque" | "page", number>>;
  /** Creates `release` or `go` in the fixture's directory. */
  create(name: "release" | "go"): void;
  /** What the fixture wrote to `building`, once it has. */
  written(name: "building"): Promise<string>;
}

function fixturePids(output: string): ReturnType<Fixture["pids"]> {
  const calque = /^calque started (.+)$/mu.exec(output)?.[1];
  const page = /^page started (\d+)$/mu.exec(output)?.[1];
  return {
    ...(calque === undefined
      ? {}
      : (JSON.parse(calque) as { launcher: number; calque: number })),
    ...(page === undefined ? {} : { page: Number(page) }),
  };
}

/**
 * `demo-stop-fixture.ts` run the way a terminal runs `pnpm demo`: as a
 * `pnpm run` script that begins with `exec`, as the root `demo` script does,
 * under the repository's pinned pnpm, in a process group of its own, as a
 * shell runs a foreground job. A terminal's Ctrl-C sends SIGINT to pnpm and to
 * the launcher, and on the second pnpm adds a SIGTERM. With no terminal, pnpm
 * moves the script into a group of its own, so `interrupt` signals both
 * groups, and pnpm also passes the first SIGINT on to the launcher. `drive`
 * signals the run; once it returns, the calque stand-in may finish shutting
 * down, and the run ends when pnpm exits.
 */
async function runFixtureThroughPnpm(
  mode: "page" | "go" | "build",
  drive: (fixture: Fixture) => Promise<void>,
): Promise<FixtureRun> {
  const directory = mkdtempSync(join(tmpdir(), "calque-ctrl-c-"));
  const recordFile = join(directory, "record");
  writeFileSync(
    join(directory, "index.html"),
    "<!doctype html><title>stop</title>\n",
  );
  const quote = (word: string) => `'${word.replaceAll("'", "'\\''")}'`;
  const { packageManager } = JSON.parse(
    readFileSync(new URL("../package.json", import.meta.url), "utf8"),
  ) as { packageManager: string };
  writeFileSync(
    join(directory, "package.json"),
    JSON.stringify({
      name: "calque-ctrl-c-fixture",
      private: true,
      packageManager,
      scripts: {
        launcher: `exec ${[
          process.execPath,
          "--import",
          new URL("register.mjs", import.meta.url).href,
          fileURLToPath(new URL("demo-stop-fixture.ts", import.meta.url)),
          directory,
          ...(mode === "page" ? [] : [mode]),
        ]
          .map(quote)
          .join(" ")}`,
      },
    }),
  );
  const pnpm = spawn("pnpm", ["run", "launcher"], {
    cwd: directory,
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  pnpm.stdout.setEncoding("utf8").on("data", (chunk: string) => {
    output += chunk;
  });
  pnpm.stderr.setEncoding("utf8").on("data", (chunk: string) => {
    output += chunk;
  });
  let survivors: readonly string[] = [];
  const exited = new Promise<FixtureRun["exit"]>((resolveExit, rejectExit) => {
    pnpm.once("error", rejectExit);
    pnpm.once("exit", (code, signal) => {
      survivors = Object.entries(fixturePids(output))
        .filter(([, pid]) => alive(pid))
        .map(([label]) => label);
      resolveExit({ code, signal });
    });
  });
  const group = pnpm.pid;
  const deadline = Date.now() + 60_000;
  try {
    assert.ok(group !== undefined, "pnpm did not start");
    await drive({
      interrupt() {
        const { launcher } = fixturePids(output);
        assert.ok(launcher !== undefined, "the launcher has not started");
        process.kill(-group, "SIGINT");
        process.kill(-launcher, "SIGINT");
      },
      async line(pattern) {
        for (;;) {
          const match = pattern.exec(output);
          if (match !== null) return match;
          assert.ok(
            pnpm.exitCode === null && pnpm.signalCode === null,
            `pnpm run exited before printing ${String(pattern)}:\n${output}`,
          );
          assert.ok(
            Date.now() < deadline,
            `pnpm run printed no ${String(pattern)} in 60 s:\n${output}`,
          );
          await sleep(25);
        }
      },
      pids: () => fixturePids(output),
      create: (name) => writeFileSync(join(directory, name), ""),
      async written(name) {
        const file = join(directory, name);
        for (;;) {
          const contents = existsSync(file) ? readFileSync(file, "utf8") : "";
          if (contents !== "") return contents;
          assert.ok(
            Date.now() < deadline,
            `the fixture wrote no ${name} in 60 s:\n${output}`,
          );
          await sleep(25);
        }
      },
    });
    writeFileSync(join(directory, "release"), "");
    const exit = await Promise.race([
      exited,
      sleep(60_000, undefined, { ref: false }),
    ]);
    assert.ok(exit !== undefined, `pnpm run did not exit in 60 s:\n${output}`);
    return {
      exit,
      output,
      record: existsSync(recordFile)
        ? readFileSync(recordFile, "utf8")
        : undefined,
      survivors,
    };
  } finally {
    if (
      group !== undefined &&
      pnpm.exitCode === null &&
      pnpm.signalCode === null
    )
      process.kill(-group, "SIGKILL");
    for (const pid of Object.values(fixturePids(output)))
      if (alive(pid)) process.kill(pid, "SIGKILL");
    rmSync(directory, { recursive: true, force: true });
  }
}

/** What a clean stop looks like, set beside what the run did. */
function stopSummary(run: FixtureRun) {
  return {
    exit: run.exit,
    record: run.record,
    survivors: run.survivors,
    listenerRan: /^==> Stopping$/mu.test(run.output),
    calqueShutDown: /^\[calque\] shutting down$/mu.test(run.output),
    pageStarted: /^page started \d+$/mu.test(run.output),
    pageServed: /^ready$|^\[browser\].*Local:/mu.test(run.output),
    errorLines:
      run.output.match(/^.*(?:ELIFECYCLE|ERR_PNPM|failed:).*$/gmu) ?? [],
  };
}

const CLEAN_STOP = {
  exit: { code: 0, signal: null },
  record: "0",
  survivors: [],
  listenerRan: true,
  calqueShutDown: true,
  pageStarted: true,
  pageServed: true,
  errorLines: [],
};

test("the root scripts a terminal stops with Ctrl-C begin with exec", () => {
  // Without `exec`, dash, the `sh` of Debian 12 and Ubuntu 24.04, stays
  // between pnpm and node, and pnpm exits on a second Ctrl-C while the stop is
  // still running. The fixtures begin with `exec` themselves, so deleting it
  // from package.json leaves every other test green.
  const { scripts } = JSON.parse(
    readFileSync(new URL("../package.json", import.meta.url), "utf8"),
  ) as { scripts: Record<string, string> };
  for (const name of ["demo", "bench", "incidents"])
    assert.match(scripts[name] ?? "", /^exec node /u, `pnpm ${name}`);
});

test(
  "Ctrl-C through pnpm run stops calque and the page and exits 0 with no error line",
  { timeout: 180_000 },
  async () => {
    const run = await runFixtureThroughPnpm("page", async (fixture) => {
      await fixture.line(/^ready$/mu);
      fixture.interrupt();
    });
    assert.deepEqual(stopSummary(run), CLEAN_STOP, run.output);
  },
);

test(
  "a second Ctrl-C while calque is still shutting down leaves the stop running",
  { timeout: 180_000 },
  async () => {
    const run = await runFixtureThroughPnpm("page", async (fixture) => {
      await fixture.line(/^ready$/mu);
      fixture.interrupt();
      const { calque, page } = fixture.pids();
      assert.ok(calque !== undefined && page !== undefined);
      // The launcher's `run` returns when the page exits, while calque is
      // still shutting down, and the second Ctrl-C below arrives after it.
      assert.ok(await goneWithin(page, 9_000), "the page outlived its stop");
      assert.ok(alive(calque), "calque finished before it was released");
      fixture.interrupt();
    });
    assert.deepEqual(stopSummary(run), CLEAN_STOP, run.output);
  },
);

test(
  "a page started after the stop has finished is stopped at once and never serves",
  { timeout: 180_000 },
  async () => {
    const run = await runFixtureThroughPnpm("go", async (fixture) => {
      await fixture.line(/^calque started /mu);
      const { launcher, calque } = fixture.pids();
      assert.ok(launcher !== undefined && calque !== undefined);
      // The launcher's node alone, so that no child ends by the group's
      // signal and only the stop can end the page.
      process.kill(launcher, "SIGTERM");
      await fixture.line(/^\[calque\] shutting down$/mu);
      fixture.create("release");
      assert.ok(await goneWithin(calque, 9_000), "calque outlived its stop");
      fixture.create("go");
    });
    assert.deepEqual(
      stopSummary(run),
      { ...CLEAN_STOP, pageServed: false },
      run.output,
    );
  },
);

const STOPPED_IN_A_BUILD = {
  ...CLEAN_STOP,
  pageStarted: false,
  pageServed: false,
};

test(
  "SIGTERM to a build step's child alone is a stop, with exit 0 and no failure line",
  { timeout: 180_000 },
  async () => {
    const run = await runFixtureThroughPnpm("build", async (fixture) => {
      const build = Number(await fixture.written("building"));
      // That child alone: the launcher gets no signal, so only the step's
      // result can start the stop.
      process.kill(build, "SIGTERM");
      await fixture.line(/^\[calque\] shutting down$/mu);
    });
    assert.deepEqual(stopSummary(run), STOPPED_IN_A_BUILD, run.output);
  },
);

test(
  "Ctrl-C during a build step whose child exits 130 on it is a stop, not a failure",
  { timeout: 180_000 },
  async () => {
    const run = await runFixtureThroughPnpm("build", async (fixture) => {
      await fixture.written("building");
      fixture.interrupt();
      await fixture.line(/^\[calque\] shutting down$/mu);
    });
    assert.deepEqual(stopSummary(run), STOPPED_IN_A_BUILD, run.output);
  },
);

test("runChecked reports a step whose child SIGINT or SIGTERM ended as a stop, and a step that failed or timed out as a failure", () => {
  for (const signal of ["SIGINT", "SIGTERM"])
    assert.throws(
      () =>
        runChecked(process.execPath, [
          "-e",
          `process.kill(process.pid, "${signal}"); ${IDLE}`,
        ]),
      StoppedBySignal,
    );
  const failure = (error: unknown) =>
    !(error instanceof StoppedBySignal) && /failed/u.test(String(error));
  assert.throws(
    () => runChecked(process.execPath, ["-e", "process.exit(1)"]),
    failure,
  );
  assert.throws(
    () =>
      runChecked(process.execPath, [
        "-e",
        "process.kill(process.pid, 'SIGHUP')",
      ]),
    failure,
  );
  // spawnSync ends a child it timed out with SIGTERM.
  assert.throws(
    () => runChecked(process.execPath, ["-e", IDLE], { timeout: 200 }),
    failure,
  );
});
