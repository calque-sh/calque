#!/usr/bin/env node
/**
 * `pnpm incidents`: the incident-operations page over the deterministic
 * 1M-row fixture, served by the `calque` binary.
 *
 * It starts a disposable PostgreSQL 18 in its own compose project
 * (`<checkout>-incidents`, so the `pnpm demo` harnesses that recreate the
 * default project's database never drop the seeded rows), seeds the fixture into
 * schema `incidents` unless that dataset is already there, starts a
 * release `calque dev` over the seven business tables, and serves the page
 * from `vite preview` with `/_calque/` proxied to the service. The page
 * reaches nothing else.
 *
 * `--fresh` removes calque's local state first, so the working set is
 * prepared from PostgreSQL again. `--no-open` skips Chrome. `--dataset
 * content-v1` seeds v1's rows with realistic content instead. `--rows 100k`
 * seeds the benchmark's 100k-row cut of either dataset in place of the whole
 * fixture.
 * `CALQUE_INCIDENTS_POSTGRES_PORT`, `CALQUE_INCIDENTS_PORT` and
 * `CALQUE_INCIDENTS_UI_PORT` choose the ports.
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  DATASET_VERSION,
  CORRECTED_PROFILE_SHA256,
  defaultFixtureSeams,
  evaluateValidationReport,
  FIXTURE_DATABASE_ENV,
  psqlArguments,
  seedFixture,
} from "./bench-fixture.ts";
import {
  benchSeedSql,
  benchValidationSql,
  BENCH_ROW_COUNTS,
  BENCH_TABLES,
  SEED_ORDER,
} from "./bench-oracle.ts";
import {
  CONTENT_DATASET_VERSION,
  createContentGenerator,
  schemaMarkerSql,
  seedContentFixture,
  streamToProcess,
} from "./bench-content-fixture.ts";
import {
  buildDevArguments,
  buildViteCommand,
  calqueExecutable,
  composePostgresMajor,
  configuredRelations,
  DEVELOPMENT_SOURCE_ROLE,
  formatError,
  heading,
  isCalqueReadinessResponse,
  parsePort,
  parsePrivateClientEnvironment,
  runChecked,
  sanitizePostgresRoutingEnvironment,
  selectPort,
  startDemoBrowser,
  startChild,
  superviseChildren,
  throwIfStoppedBySignal,
  waitForHttp,
  type PageCommand,
} from "./demo.ts";
import { CARGO_TARGET_ARGUMENTS } from "./lib/cargo-target.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const stateDirectory = join(root, ".calque-dev", "incidents");

/**
 * The page's pinned bindings: `calque init` of `calque inspect` of the seeded
 * `incidents` tables, with the page's commands added, and the
 * `calque-bindings` render of that configuration.
 */
export const INCIDENTS_BINDINGS = Object.freeze({
  config: "client/examples/incidents/calque.config.json",
  output: "client/examples/incidents/src/calque.generated.ts",
});

/** The page's preview server, `@calque/incidents`'s vite. */
function buildIncidentsPageCommand(uiPort: number): PageCommand {
  return buildViteCommand(join(root, "client/examples/incidents"), [
    "preview",
    "--host",
    "127.0.0.1",
    "--port",
    String(uiPort),
    "--strictPort",
  ]);
}

export function buildIncidentsBindingsCheckArguments(): readonly string[] {
  return [
    "client/packages/config/bin/calque-bindings.js",
    "--config",
    INCIDENTS_BINDINGS.config,
    "--out",
    INCIDENTS_BINDINGS.output,
    "--check",
  ];
}

/** A compose project of its own, named after the checkout it runs from. */
export function incidentsComposeProject(checkout: string): string {
  const name = basename(checkout)
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/gu, "-")
    .replace(/^[-_]+/u, "");
  return `${name || "calque"}-incidents`;
}

/** What `docker compose` needs to address the incidents demo's PostgreSQL. */
export function incidentsComposeEnvironment(
  environment: NodeJS.ProcessEnv,
  postgresPort: number,
  checkout: string = root,
): NodeJS.ProcessEnv {
  return {
    ...environment,
    COMPOSE_PROJECT_NAME: incidentsComposeProject(checkout),
    CALQUE_POSTGRES_PORT: String(postgresPort),
  };
}

/** Whether the fixture's `metadata` table exists: `t` or `f`. */
export const metadataPresentSql =
  "SELECT to_regclass('incidents.metadata') IS NOT NULL;";

/** Which dataset the source holds, as `metadata` records it. */
const seededDatasetSql =
  "SELECT dataset_version || ' ' || dataset_profile_sha256 FROM incidents.metadata;";

export function isSeededDataset(output: string): boolean {
  return output.trim() === `${DATASET_VERSION} ${CORRECTED_PROFILE_SHA256}`;
}

/**
 * Whether `issue_tags` carries the `bench_index` key an earlier `pnpm incidents` run
 * gave it, with its own `(issue_id, tag)` key demoted to this unique
 * constraint: `1` or `0`.
 */
export const issueTagsRekeyedSql =
  "SELECT count(*) FROM pg_constraint WHERE conrelid = to_regclass('incidents.issue_tags') AND conname = 'issue_tags_issue_id_tag_key';";

/** The fixture `pnpm incidents` seeds: v1, or v1's rows with realistic content. */
export type IncidentsDataset = "v1" | "content-v1";

export function incidentsDatasetVersion(dataset: IncidentsDataset): string {
  return dataset === "content-v1" ? CONTENT_DATASET_VERSION : DATASET_VERSION;
}

/** How much of the dataset `pnpm incidents` seeds: the whole fixture, or the benchmark's cut. */
export type IncidentsRows = "1m" | "100k";

/** The rows in the benchmark's cut, the one sentinel per table included. */
export const CUT_ROWS: number =
  SEED_ORDER.reduce((sum, table) => sum + BENCH_ROW_COUNTS[table], 0) +
  SEED_ORDER.length;

/** The schema comment that marks a source as holding the cut, not the whole fixture. */
export function cutSchemaMarker(dataset: IncidentsDataset): string {
  return `${incidentsDatasetVersion(dataset)} (100k cut)`;
}

/**
 * Whether the source holds this fixture with its own schema. PostgreSQL
 * resolves every relation in a statement when it parses it, so `metadata` is
 * read only after a statement of its own has found it. A source an earlier run
 * re-keyed is reseeded. Both datasets' metadata names v1, so the schema's
 * comment tells them apart: v1 has none, and the content variant's names it
 * only once its seed passed.
 */
export function holdsSeededDataset(
  sql: (statement: string) => string,
  dataset: IncidentsDataset = "v1",
  rows: IncidentsRows = "1m",
): boolean {
  if (sql(metadataPresentSql).trim() !== "t") return false;
  if (!isSeededDataset(sql(seededDatasetSql))) return false;
  if (sql(issueTagsRekeyedSql).trim() !== "0") return false;
  const marker =
    rows === "100k"
      ? cutSchemaMarker(dataset)
      : dataset === "content-v1"
        ? CONTENT_DATASET_VERSION
        : "";
  return sql(schemaMarkerSql).trim() === marker;
}

/** The PostgreSQL system the source runs on. */
const systemIdentifierSql =
  "SELECT system_identifier FROM pg_control_system();";

/**
 * How `calque destroy` removes the incidents demo's local state. On the system the
 * state was prepared from, destroy drops its slot and publication too. A state
 * that names another system outlived a recreated volume, whose slot and
 * publication went with it, and destroy would refuse to connect to the new
 * system, so only the local files are removed. A state that names no system
 * is left to destroy's own identity check.
 */
export function localStateDestroyArguments(
  directory: string,
  state: string,
  sourceSystem: string,
): string[] {
  const preparedOn = (
    JSON.parse(state) as { sourceIdentity?: { systemIdentifier?: unknown } }
  ).sourceIdentity?.systemIdentifier;
  const destroy = ["destroy", "--state-dir", directory, "--yes"];
  return typeof preparedOn === "string" && preparedOn !== sourceSystem.trim()
    ? [...destroy, "--local-state-only"]
    : destroy;
}

interface IncidentsOptions {
  readonly open: boolean;
  readonly fresh: boolean;
  readonly dataset: IncidentsDataset;
  readonly rows: IncidentsRows;
}

export function parseIncidentsArguments(
  arguments_: readonly string[],
): IncidentsOptions {
  const rest = arguments_[0] === "--" ? arguments_.slice(1) : arguments_;
  const usage = new Error(
    "usage: pnpm incidents [--] [--no-open] [--fresh] [--dataset v1|content-v1] [--rows 1m|100k]",
  );
  let dataset: IncidentsDataset | undefined;
  let rows: IncidentsRows | undefined;
  for (let index = 0; index < rest.length; index += 1) {
    const argument = rest[index];
    if (argument === "--dataset") {
      const value = rest[index + 1];
      if (dataset !== undefined || (value !== "v1" && value !== "content-v1"))
        throw usage;
      dataset = value;
      index += 1;
    } else if (argument === "--rows") {
      const value = rest[index + 1];
      if (rows !== undefined || (value !== "1m" && value !== "100k"))
        throw usage;
      rows = value;
      index += 1;
    } else if (argument !== "--no-open" && argument !== "--fresh") {
      throw usage;
    }
  }
  return {
    open: !rest.includes("--no-open"),
    fresh: rest.includes("--fresh"),
    dataset: dataset ?? "v1",
    rows: rows ?? "1m",
  };
}

export function formatSeconds(milliseconds: number): string {
  return (milliseconds / 1000).toFixed(1);
}

async function main(): Promise<void> {
  const options = parseIncidentsArguments(process.argv.slice(2));
  const postgresPort = parsePort(
    process.env.CALQUE_INCIDENTS_POSTGRES_PORT ?? "55442",
    "CALQUE_INCIDENTS_POSTGRES_PORT",
  );
  const serverPort = await selectPort(
    process.env.CALQUE_INCIDENTS_PORT,
    8797,
    "CALQUE_INCIDENTS_PORT",
    "incidents service",
  );
  const uiPort = await selectPort(
    process.env.CALQUE_INCIDENTS_UI_PORT,
    5183,
    "CALQUE_INCIDENTS_UI_PORT",
    "incidents page",
  );
  const endpoint = `http://127.0.0.1:${serverPort}`;
  const uiOrigin = `http://127.0.0.1:${uiPort}`;
  const browserEndpoint = `${uiOrigin}/_calque/`;
  const databaseUrl = `postgresql://${DEVELOPMENT_SOURCE_ROLE}:${DEVELOPMENT_SOURCE_ROLE}@127.0.0.1:${postgresPort}/calque_dev`;
  const composeEnvironment = incidentsComposeEnvironment(
    process.env,
    postgresPort,
  );
  const serviceEnvironment: NodeJS.ProcessEnv = {
    ...sanitizePostgresRoutingEnvironment(process.env),
    CALQUE_DATABASE_URL: databaseUrl,
  };
  await superviseChildren("Incidents", async ({ adopt, stopping }) => {
    const postgresMajor = composePostgresMajor(
      readFileSync(join(root, "docker-compose.yml"), "utf8"),
    );
    heading(
      `Starting disposable PostgreSQL ${postgresMajor} (${composeEnvironment.COMPOSE_PROJECT_NAME}, port ${postgresPort})`,
    );
    runChecked(
      "docker",
      ["compose", "up", "-d", "--wait", "--wait-timeout", "60", "postgres"],
      { env: composeEnvironment },
    );

    heading("Building calque (release)");
    const calque = calqueExecutable(
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

    const sql = (statement: string) =>
      composePsql(statement, composeEnvironment);
    const datasetVersion = incidentsDatasetVersion(options.dataset);
    const seeded = holdsSeededDataset(sql, options.dataset, options.rows);
    if (!seeded || options.fresh) {
      // Whatever calque prepared belongs to the rows it read; a reseed or a
      // fresh run removes it first, with its slot and publication when they
      // are on this source.
      removeLocalState(calque, serviceEnvironment, sql(systemIdentifierSql));
    }
    if (seeded) {
      process.stdout.write(`Fixture already seeded (${datasetVersion}).\n`);
    } else {
      heading(
        options.rows === "100k"
          ? `Seeding the ${CUT_ROWS.toLocaleString("en-US")}-row cut (${datasetVersion})`
          : `Seeding the 1M-row fixture (${datasetVersion})`,
      );
      const started = performance.now();
      const seedEnvironment = {
        ...process.env,
        [FIXTURE_DATABASE_ENV]: databaseUrl,
      };
      const seams = {
        readProfile: defaultFixtureSeams.readProfile,
        runSql: (statement: string) => sql(statement),
      };
      const contentSeams = {
        ...seams,
        streamSql: (chunks: Iterable<string>) =>
          streamToProcess(
            "docker",
            composePsqlArguments,
            { cwd: root, env: composeEnvironment },
            chunks,
          ),
      };
      if (options.rows === "100k" && options.dataset === "content-v1") {
        await seedContentFixture(
          seedEnvironment,
          contentSeams,
          BENCH_TABLES,
          createContentGenerator(),
          benchValidationSql(),
        );
        sql(
          `COMMENT ON SCHEMA incidents IS '${cutSchemaMarker("content-v1")}';`,
        );
      } else if (options.rows === "100k") seedCut(seams.runSql);
      else if (options.dataset === "content-v1")
        await seedContentFixture(seedEnvironment, contentSeams);
      else seedFixture(seedEnvironment, seams);
      process.stdout.write(
        `Seeded the fixture in ${formatSeconds(performance.now() - started)} s.\n`,
      );
    }
    heading("Starting calque dev");
    const fresh = !existsSync(join(stateDirectory, "catalog.sqlite3"));
    const preparationStarted = performance.now();
    const server = adopt(
      startChild(
        "calque",
        calque,
        buildDevArguments({
          stateDirectory,
          tables: configuredRelations(
            readFileSync(join(root, INCIDENTS_BINDINGS.config), "utf8"),
          ),
          uiOrigin,
          browserEndpoint,
          serverPort,
        }),
        serviceEnvironment,
      ),
    );
    try {
      await waitForHttp(
        `${endpoint}/readyz`,
        server,
        900_000,
        isCalqueReadinessResponse,
      );
    } catch (error) {
      // calque names `calque destroy` when it refuses a state directory
      // another build prepared; for `pnpm incidents`, --fresh runs it.
      if (fresh) throw error;
      throw new Error(
        `${formatError(error)}\nIf calque refused ${stateDirectory}, rerun with --fresh, which destroys it and prepares the working set again.`,
      );
    }
    process.stdout.write(
      `calque dev prepared the working set in ${formatSeconds(performance.now() - preparationStarted)} s (${fresh ? "fresh" : "reused"} state).\n`,
    );
    const clientSettings = parsePrivateClientEnvironment(
      readFileSync(join(stateDirectory, "client.env"), "utf8"),
    );
    if (clientSettings.endpoint !== endpoint)
      throw new Error(
        "generated client endpoint differs from the local service",
      );

    heading("Checking the generated bindings");
    runChecked("pnpm", ["--filter", "@calque/config", "build"]);
    runChecked("node", [...buildIncidentsBindingsCheckArguments()]);
    heading("Building the page");
    // The page's dependencies, then the page with the entry that publishes
    // what this reads on `globalThis.__incidents`.
    runChecked("pnpm", ["--filter", "@calque/incidents^...", "build"]);
    runChecked("pnpm", ["--filter", "@calque/incidents", "build:harness"]);

    heading("Starting the page");
    const page = buildIncidentsPageCommand(uiPort);
    const ui = adopt(
      startChild(
        "page",
        page.command,
        page.arguments_,
        {
          ...process.env,
          CALQUE_LOCAL_SERVICE_ENDPOINT: clientSettings.endpoint,
          CALQUE_LOCAL_BEARER_TOKEN: clientSettings.bearerToken,
        },
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
        "The incident-operations page is ready.",
        `Open: ${uiOrigin}/`,
        "Every panel is a local query over the complete synchronized tables.",
        "Press Ctrl-C here to stop; PostgreSQL and calque's local state are kept.",
        "",
      ].join("\n"),
    );
    if (options.open) adopt(startDemoBrowser(`${uiOrigin}/`));

    const exited = await Promise.race(
      [server, ui].map((child) =>
        child.exited.then(({ code, signal }) => ({ child, code, signal })),
      ),
    );
    if (!stopping())
      throw new Error(
        `${exited.child.label} exited unexpectedly (${exited.code ?? exited.signal ?? "unknown"})`,
      );
  });
}

/**
 * The benchmark's cut in place of the whole fixture: the schema from scratch,
 * each table's first tenth, the sentinels, checked against v1's cut checks,
 * and marked so a source holding the whole fixture is not taken for it.
 */
function seedCut(runSql: (statement: string) => string): void {
  runSql(benchSeedSql());
  const validated = evaluateValidationReport(runSql(benchValidationSql()));
  if (!validated.valid)
    throw new Error(
      `the 100k cut failed v1's checks: ${validated.failedChecks.join(", ")}`,
    );
  runSql(`COMMENT ON SCHEMA incidents IS '${cutSchemaMarker("v1")}';`);
}

/**
 * `docker compose` arguments that run the container's own `psql`, with the
 * arguments the fixture's runner uses, so the seed needs no PostgreSQL client
 * on the host.
 */
const composePsqlArguments: readonly string[] = [
  "compose",
  "exec",
  "-T",
  "postgres",
  "psql",
  ...psqlArguments,
  "--username",
  DEVELOPMENT_SOURCE_ROLE,
  "--dbname",
  "calque_dev",
];

/** SQL through the container's own `psql`. */
function composePsql(sql: string, environment: NodeJS.ProcessEnv): string {
  const result = spawnSync("docker", composePsqlArguments, {
    cwd: root,
    env: environment,
    encoding: "utf8",
    input: sql,
    maxBuffer: 64 * 1024 * 1024,
  });
  throwIfStoppedBySignal("docker", composePsqlArguments, result);
  if (result.error || result.status !== 0)
    throw new Error(
      `psql failed: ${result.error?.message ?? result.stderr.trim()}`,
    );
  return result.stdout.trim();
}

function removeLocalState(
  calque: string,
  environment: NodeJS.ProcessEnv,
  sourceSystem: string,
): void {
  const statePath = join(stateDirectory, "state.json");
  if (!existsSync(statePath)) return;
  heading("Removing calque's local state");
  runChecked(
    calque,
    localStateDestroyArguments(
      stateDirectory,
      readFileSync(statePath, "utf8"),
      sourceSystem,
    ),
    { env: environment },
  );
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === fileURLToPath(import.meta.url)) {
  await main();
}
