#!/usr/bin/env node
/**
 * `pnpm bench`: the benchmark. It starts and seeds its own PostgreSQL
 * 18, has a release `calque dev` prepare the incident-operations tables, times cold loads
 * of the incident-operations page in a pinned Chrome to a complete stable
 * paint and changes from their PostgreSQL commit to the paint that shows them,
 * and compares every sample's working set with PostgreSQL.
 *
 * Run `pnpm bench:fetch-browser` once, then `pnpm bench [-- <options>]`;
 * `USAGE` lists the options and `docs/benchmark.md` describes the method and
 * what each option does.
 */
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { decoderFiles } from "./bench-brotli.ts";
import { BROWSER_EXIT_LOOKBACK_MS } from "./bench-browser.ts";
import { fetchCaddy } from "./bench-caddy.ts";
import { CONFIG_PATH } from "./bench-calque.ts";
import {
  benchPlan,
  emulatedNetwork,
  NETWORK_CAVEAT,
  parseBenchArguments,
  rendersPlan,
} from "./bench-conditions.ts";
import type { Run } from "./bench-context.ts";
import {
  caddyVersion,
  type EdgePlan,
  edgeRecord,
  planEdge,
  waitForListening,
} from "./bench-edge.ts";
import { postgresEnvironmentFromUrl } from "./bench-fixture.ts";
import { FrameHasher, STABLE_PAINT_RULE } from "./bench-frames.ts";
import { type HostRecord, readHost } from "./bench-host.ts";
import {
  type BrowserExit,
  launchTarget,
  readBrowserExit,
} from "./bench-launch.ts";
import {
  availableShapedPort,
  KernelShaper,
  NETSHAPE,
  NETSHAPE_PROBE,
  NETSHAPE_SOURCE,
  SHAPER_CAVEAT,
} from "./bench-netshape.ts";
import { DATASETS, oracleTables } from "./bench-oracle.ts";
import {
  PAGE_ROOT,
  servePageCommand,
  waitForPage,
  writePageControl,
} from "./bench-page-server.ts";
import {
  ADMIN_ROLE,
  type BenchPostgres,
  ContainerPostgres,
  databaseUrl,
  NativePostgres,
  SOURCE_ROLE,
  waitForPostgres,
} from "./bench-postgres.ts";
import { log, scrubRunLogs } from "./bench-record.ts";
import { runBench } from "./bench-run.ts";
import { listenForStop, runStop } from "./bench-stop.ts";
import { availablePort } from "./demo-process.ts";
import {
  calqueExecutable,
  type ChildHandle,
  parsePort,
  runChecked,
  sanitizePostgresRoutingEnvironment,
  startChild,
  stopChild,
} from "./demo.ts";
import { CARGO_TARGET_ARGUMENTS } from "./lib/cargo-target.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const INSPECTION_PATH = join(
  root,
  "client/examples/incidents/calque.inspection.json",
);

/** The client's committed decoder module, which the served build carries. */
const DECODER_MODULE = join(
  root,
  "client/packages/worker/wasm/brotli-decoder.wasm",
);

const DEFAULT_POSTGRES_PORT = "55521";

const HASH_WORKERS = 4;

async function main(arguments_: readonly string[]): Promise<void> {
  if (arguments_[0] === "serve-page") {
    await servePageCommand(arguments_.slice(1));
    return;
  }
  const options = parseBenchArguments(arguments_);
  const shaper =
    options.network !== null && "shaper" in options.network
      ? new KernelShaper(
          options.network,
          join(root, NETSHAPE_SOURCE),
          join(root, NETSHAPE_PROBE),
        )
      : null;
  shaper?.preflight();
  const browser = await launchTarget();
  const plan = benchPlan(options.smoke, options.realtimeOnly);
  const mode = options.smoke ? "smoke" : "full";
  const postgresPort = parsePort(
    process.env.CALQUE_BENCH_POSTGRES_PORT ?? DEFAULT_POSTGRES_PORT,
    "CALQUE_BENCH_POSTGRES_PORT",
  );
  const id = `${new Date().toISOString().replace(/[:.]/gu, "-")}${options.smoke ? "-smoke" : ""}${options.diagnostic ? "-diagnostic" : ""}${options.realtimeOnly ? "-realtime" : ""}${shaper ? "-shaped" : ""}${options.postgresBin === null ? "" : "-native-postgres"}${options.edge ? "" : "-no-edge"}${options.backgroundClients === undefined ? "" : `-background-${String(options.backgroundClients)}`}${options.backgroundQos === undefined ? "" : `-${options.backgroundQos}`}${options.loadRate === undefined ? "" : `-load-${String(options.loadRate)}`}${options.tail === undefined ? "" : `-tail-${String(options.tail)}`}${options.recutTailRatio === undefined ? "" : `-recut-ratio-${String(options.recutTailRatio)}`}${options.recutMinIntervalSeconds === undefined ? "" : `-recut-interval-${String(options.recutMinIntervalSeconds)}`}`;
  const directory = join(root, "target/bench", id);
  mkdirSync(join(directory, "samples"), { recursive: true });
  mkdirSync(join(directory, "logs"), { recursive: true });
  const postgresInit = join(root, "docker/postgres-init");
  const postgres: BenchPostgres =
    options.postgresBin === null
      ? new ContainerPostgres(postgresInit)
      : new NativePostgres(
          options.postgresBin,
          postgresInit,
          join(directory, "logs", "postgres.log"),
        );
  // It holds the bearer while the run lasts, in a private temporary directory
  // rather than the run's, whose files a record copies: a run that dies
  // before its cleanup leaves this copy outside them.
  const pageControlDirectory = mkdtempSync(
    join(tmpdir(), "calque-bench-page-"),
  );
  const pageControl = join(pageControlDirectory, "page-control.json");
  const startedAt = new Date().toISOString();

  const children: ChildHandle[] = [];
  const servers = new Set<ChildHandle>();
  let hasher: FrameHasher | null = null;
  let postgresStarted = false;
  let aborted: string | null = null;
  let result: unknown = null;
  let host: HostRecord | null = null;
  let browserExit: BrowserExit | null = null;
  // Once, after an `up`, on every exit: a run's own path, a signal, or a
  // throw that leaves the process.
  const shaperDown = () => {
    const disabled = shaper?.down() ?? null;
    if (disabled === false)
      log(
        `calque-netshape's status did not read disabled after down: sudo -n ${NETSHAPE} down`,
      );
  };
  if (shaper) process.once("exit", shaperDown);
  const cleanup = async () => {
    shaperDown();
    await Promise.allSettled(
      [...servers, ...children].map((child) => stopChild(child)),
    );
    servers.clear();
    children.length = 0;
    scrubRunLogs(directory);
    await hasher?.close();
    hasher = null;
    if (postgresStarted) postgres.remove();
    postgresStarted = false;
    rmSync(pageControlDirectory, { recursive: true, force: true });
  };
  const emulated = emulatedNetwork(options.network);
  const summary = (reason: string | null) => ({
    runId: id,
    mode,
    diagnostic: options.diagnostic,
    startedAt,
    finishedAt: new Date().toISOString(),
    host,
    rule: STABLE_PAINT_RULE,
    plan: {
      ...plan,
      realtimeOnly: options.realtimeOnly,
      renderedPlan: rendersPlan(options),
      edge: edgeRecord(options.edge),
      dataset: {
        name: options.dataset,
        id: DATASETS[options.dataset],
      },
      network: shaper
        ? { ...shaper.record(), caveat: SHAPER_CAVEAT }
        : emulated === null
          ? null
          : {
              ...emulated,
              emulation: "CDP Network.emulateNetworkConditions",
              caveat: NETWORK_CAVEAT,
            },
    },
    result,
    aborted: reason,
    browserExit,
  });
  const stop = runStop(directory, summary, cleanup);
  listenForStop(stop);

  try {
    log(`run ${id} (${mode})`);
    const calque =
      options.calque ??
      calqueExecutable(
        runChecked(
          "cargo",
          [
            "build",
            "--release",
            ...CARGO_TARGET_ARGUMENTS,
            "-p",
            "calque",
            "--message-format=json",
          ],
          { env: process.env, stdio: ["ignore", "pipe", "inherit"] },
        ).stdout,
      );
    log("building the page");
    // The page's dependencies, then the page with the entry that publishes
    // what this reads on `globalThis.__incidents`.
    runChecked("pnpm", ["--filter", "@calque/incidents^...", "build"]);
    runChecked("pnpm", ["--filter", "@calque/incidents", "build:harness"]);
    const decoder = decoderFiles(PAGE_ROOT, readFileSync(DECODER_MODULE));
    const caddy = options.edge ? await fetchCaddy({ log }) : null;

    log(`starting PostgreSQL on port ${String(postgresPort)}`);
    // Before `start`, which can fail half way: `remove` deletes whatever it
    // made and does nothing when it made nothing.
    postgresStarted = true;
    postgres.start(postgresPort);
    const sourceEnvironment = postgresEnvironmentFromUrl(
      databaseUrl(postgresPort, SOURCE_ROLE, SOURCE_ROLE),
    );
    const adminEnvironment = postgresEnvironmentFromUrl(
      databaseUrl(postgresPort, ADMIN_ROLE, ADMIN_ROLE),
    );
    await waitForPostgres(sourceEnvironment);
    host = readHost(calque, postgres);

    // Behind the edge the browser connects only to the edge's ports, so
    // those are the shaped ones and the page and calque listen unshaped.
    const shapedPort = async (taken: ReadonlySet<number>) =>
      shaper
        ? await availableShapedPort(new Set(taken))
        : await availablePort();
    const pagePort = options.edge
      ? await availablePort()
      : await shapedPort(new Set());
    const calquePort = options.edge
      ? await availablePort()
      : await shapedPort(new Set([pagePort]));
    const calqueMetricsPort = await availablePort();
    let edgePlan: EdgePlan | null = null;
    if (caddy !== null) {
      const pageEdge = await shapedPort(new Set([pagePort, calquePort]));
      const calqueEdge = await shapedPort(
        new Set([pagePort, calquePort, pageEdge]),
      );
      edgePlan = planEdge(join(directory, "edge"), {
        page: pagePort,
        calque: calquePort,
        pageEdge,
        calqueEdge,
      });
      log(`edge: ${caddyVersion(caddy.executablePath)}, TLS with HTTP/2`);
    }
    const pageOrigin =
      edgePlan?.pageOrigin ?? `http://127.0.0.1:${String(pagePort)}`;
    // Until the run has a connection, the page goes as stored.
    writePageControl(pageControl, {
      connection: null,
      renderedPlan: false,
      backendEndpoint: null,
    });
    children.push(
      startChild(
        "page",
        process.execPath,
        [
          "--import",
          "./scripts/register.mjs",
          "scripts/bench.ts",
          "serve-page",
          String(pagePort),
          pageControl,
          PAGE_ROOT,
        ],
        // The page server loads Playwright too, which would truncate the
        // browser log this process writes there.
        Object.fromEntries(
          Object.entries(process.env).filter(([name]) => name !== "DEBUG_FILE"),
        ),
      ),
    );
    await waitForPage(`http://127.0.0.1:${String(pagePort)}/`);
    if (edgePlan !== null && caddy !== null) {
      children.push(
        startChild(
          "edge",
          caddy.executablePath,
          ["run", "--config", edgePlan.caddyfile, "--adapter", "caddyfile"],
          edgePlan.environment,
        ),
      );
      await waitForListening([
        Number(new URL(edgePlan.pageOrigin).port),
        Number(new URL(edgePlan.calqueBaseUrl).port),
      ]);
    }

    hasher = new FrameHasher(HASH_WORKERS);
    const run: Run = {
      id,
      directory,
      options,
      plan,
      calque,
      host,
      pageUrl: `${pageOrigin}/`,
      pageOrigin,
      calquePort,
      calqueMetricsPort,
      calquePublicBaseUrl: edgePlan?.calqueBaseUrl ?? null,
      shapedPorts:
        edgePlan === null
          ? [pagePort, calquePort]
          : [
              Number(new URL(edgePlan.pageOrigin).port),
              Number(new URL(edgePlan.calqueBaseUrl).port),
            ],
      hasher,
      sourceEnvironment,
      adminEnvironment,
      serviceEnvironment: {
        ...sanitizePostgresRoutingEnvironment(process.env),
        CALQUE_DATABASE_URL: databaseUrl(
          postgresPort,
          SOURCE_ROLE,
          SOURCE_ROLE,
        ),
      },
      tables: oracleTables(
        readFileSync(CONFIG_PATH, "utf8"),
        readFileSync(INSPECTION_PATH, "utf8"),
      ),
      servers,
      browser:
        edgePlan === null ? browser : { ...browser, args: edgePlan.chromeArgs },
      stateDirectory: join(directory, "state"),
      readings: [],
      server: null,
      connection: null,
      source: null,
      queryExpectation: null,
      reference: null,
      launches: [],
      shaper,
      postgres,
      pageControl,
      decoder,
      calqueArtifact: null,
    };
    result = await runBench(run);
  } catch (error) {
    aborted =
      error instanceof Error ? (error.stack ?? error.message) : String(error);
    // Before the cleanup, which takes tens of seconds.
    browserExit = readBrowserExit(error);
    if (browserExit !== null)
      log(
        `lost the browser (${browserExit.losses.map((loss) => loss.kind).join(", ") || "no event"}): ${String(browserExit.jetsamKills?.count ?? "unread")} jetsam exits in the host log since ${String(BROWSER_EXIT_LOOKBACK_MS / 1_000)} s before, ${String(browserExit.freeMemoryPercent ?? "unknown")}% memory free, cause ${browserExit.cause}`,
      );
    process.exitCode = 1;
  } finally {
    await stop.finish(aborted);
    if (aborted) process.stderr.write(`[bench] aborted: ${aborted}\n`);
  }
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === fileURLToPath(import.meta.url)) {
  try {
    await main(process.argv.slice(2));
  } catch (error) {
    // Only a refusal before the run starts lands here: a run records its own
    // failure in summary.json.
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 2;
  }
}
