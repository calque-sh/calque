#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Drives a page whose generated bindings name fewer tables than its server
 * serves, from headless Chromium, through the packaged client.
 *
 * `calque serve` serves `projects` and `tasks`; the page's bindings read
 * `projects` only. The page must reach `ready` with the complete `projects`
 * working set, show nothing of `tasks`, and stay live: a transaction that
 * changes both tables shows its `projects` part, a transaction that changes
 * only `tasks` leaves the page ready, and a later `projects` change still
 * arrives. Once the server has derived the artifact of `projects` alone, a
 * second visitor on a fresh profile must be planned that artifact, download
 * nothing of `tasks`, and reach the committed rows the same way.
 *
 * Usage: `pnpm serve:e2e:narrow`. Requires Docker and a Rust toolchain.
 * `CALQUE_SERVE_E2E_POSTGRES_PORT` selects the container's host port
 * (default 55491) and `CALQUE_SERVE_E2E_KEEP_CONTAINER=1` leaves the container
 * running for a follow-up run.
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import process from "node:process";
import type { Browser, BrowserContext, Page } from "playwright";
import {
  buildServeBinaries,
  ensurePostgresContainer,
  psqlIn,
  removePostgresContainer,
  repositoryRoot as root,
  run,
  servePage,
  startServe,
  stopServe,
  waitFor,
} from "./lib/serve-e2e.ts";
import { reservedPort } from "./lib/ports.ts";

interface NarrowPageState {
  readonly state: string;
  readonly failure: string | null;
  readonly cursor: string | null;
  readonly projects: readonly (readonly [number, string, string])[];
  readonly publishedCollections: readonly (readonly string[])[];
  readonly collections: readonly string[];
  readonly statuses: readonly string[];
}

interface PlannedBootstrap {
  readonly requiredTables: readonly unknown[];
  readonly artifacts: readonly {
    readonly tables: readonly unknown[];
    readonly decodedBytes: number;
    readonly selectedRepresentation: { readonly encodedBytes: number };
  }[];
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations to `typeof globalThis`.
  var __calqueNarrowState: () => NarrowPageState;
}

const runId = randomUUID().replaceAll("-", "").slice(0, 12);
const workDirectory = join(root, "target/serve-narrow-e2e", runId);
const pageRoot = join(root, "target/client-serve-narrow");
const postgresPort = process.env.CALQUE_SERVE_E2E_POSTGRES_PORT ?? "55491";
const container = "calque-servee2e-pg18";
const keepContainer = process.env.CALQUE_SERVE_E2E_KEEP_CONTAINER === "1";
const runDatabase = `calque_serve_narrow_e2e_${runId}`;
const runSlot = `${runDatabase}_slot`;
const runPublication = `${runDatabase}_pub`;
const DATABASE_ENV = "CALQUE_SERVE_E2E_DATABASE_URL";
const GRANT_KEY_ENV = "CALQUE_SERVE_E2E_GRANT_KEY";
const GRANT_AUDIENCE = "serve-narrow.e2e";
const READY_TIMEOUT_MS = 300_000;
const MOUNT_TIMEOUT_MS = 180_000;
const LIVE_TIMEOUT_MS = 60_000;
/** How long a tasks-only transaction is given to break the page. */
const QUIET_WATCH_MS = 5_000;
const DERIVE_TIMEOUT_MS = 120_000;

const SEED_SQL = `
CREATE TABLE projects (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  status text NOT NULL
);
CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  project_id bigint NOT NULL REFERENCES projects(id),
  title text NOT NULL,
  done boolean NOT NULL DEFAULT false
);
INSERT INTO projects (id, name, status) VALUES
  (101, 'Atlas launch', 'at_risk'),
  (102, 'Billing reliability', 'on_track'),
  (103, 'Mobile rollout', 'planning');
INSERT INTO tasks (id, project_id, title, done) VALUES
  (1001, 101, 'Freeze the release candidate', false),
  (1002, 101, 'Validate the data migration', true),
  (1003, 102, 'Rehearse regional failover', true);
CREATE PUBLICATION ${runPublication} FOR TABLE projects, tasks;
`;

const DROP_RUN_DATABASE_SQL = `
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
  WHERE slot_name = '${runSlot}';
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE datname = '${runDatabase}' AND pid <> pg_backend_pid();
DROP DATABASE IF EXISTS ${runDatabase};
`;

function psql(sql: string, stopOnError = true): string {
  return psqlIn(container, runDatabase, sql, stopOnError);
}

function adminPsql(sql: string, stopOnError = true): string {
  return psqlIn(container, "calque_verify", sql, stopOnError);
}

/** The committed `projects` rows, the oracle the page is compared with. */
function committedProjects(): readonly (readonly [number, string, string])[] {
  return psql("SELECT id, name, status FROM projects ORDER BY id;")
    .trim()
    .split("\n")
    .map((line) => {
      const [id = "", name = "", status = ""] = line.split("|");
      return [Number(id), name, status] as const;
    });
}

/**
 * The plan of the first bootstrap any page of `context` is answered, read as
 * the route passes the response on: Chrome may drop a body before a
 * `response` listener asks DevTools for it.
 */
async function firstPlan(
  context: BrowserContext,
): Promise<{ readonly plan: Promise<PlannedBootstrap> }> {
  const planned = Promise.withResolvers<PlannedBootstrap>();
  await context.route("**/v1/bootstrap", async (route) => {
    if (route.request().method() !== "POST") {
      await route.continue();
      return;
    }
    const response = await route.fetch();
    const body = await response.body();
    if (response.ok())
      planned.resolve(JSON.parse(body.toString("utf8")) as PlannedBootstrap);
    await route.fulfill({ response, body });
  });
  return { plan: planned.promise };
}

/** The tables the plan installs, which must be exactly the ones it needs. */
function installedTables(plan: PlannedBootstrap): string {
  return JSON.stringify(plan.artifacts.flatMap((artifact) => artifact.tables));
}

async function main(): Promise<Record<string, unknown>> {
  mkdirSync(workDirectory, { recursive: true });
  const binaries = buildServeBinaries();
  run("pnpm", ["--filter", "@calque/client...", "build"]);
  run("pnpm", ["--filter", "@calque/client", "serve-narrow:build"]);
  ensurePostgresContainer(container, postgresPort);
  const servePort = await reservedPort();
  const pagePort = await reservedPort();
  const pageOrigin = `http://127.0.0.1:${String(pagePort)}`;
  const endpoint = `http://127.0.0.1:${String(servePort)}/`;
  const databaseUrl = `postgresql://calque_test:calque_test@127.0.0.1:${postgresPort}/${runDatabase}`;
  const serveLog: string[] = [];

  // The deployment's manifest serves both tables; the page's bindings, pinned
  // from `tests/fixtures/config/serve-narrow.config.json`, name `projects` only.
  const configPath = join(workDirectory, "calque.config.json");
  const manifest = JSON.parse(
    readFileSync(
      join(root, "client/packages/client/tests/demo/calque.config.json"),
      "utf8",
    ),
  ) as {
    sources: { databaseUrlEnv: string; slot: string; publication: string }[];
  };
  const [source] = manifest.sources;
  if (!source) throw new Error("the binding fixture declares no source");
  source.databaseUrlEnv = DATABASE_ENV;
  source.slot = runSlot;
  source.publication = runPublication;
  writeFileSync(configPath, `${JSON.stringify(manifest, null, 2)}\n`);

  adminPsql(DROP_RUN_DATABASE_SQL, false);
  adminPsql(`CREATE DATABASE ${runDatabase} OWNER calque_test;`);
  psql(SEED_SQL);

  const grant = JSON.parse(
    run(join(binaries, "calque-dev-grant"), [
      "--key-file",
      join(workDirectory, "grant-key.pk8"),
      "--key-id",
      "serve-narrow-e2e",
      "--audience",
      GRANT_AUDIENCE,
      "--subject",
      "serve-narrow-reader",
      "--generation",
      "1",
      "--lifetime-seconds",
      "900",
    ]),
  ) as { token: string; verificationKeyEnvironment: unknown };

  const serving = await startServe(
    join(binaries, "calque"),
    [
      "--config",
      configPath,
      "--state-dir",
      join(workDirectory, "state"),
      "--grant-audience",
      GRANT_AUDIENCE,
      "--grant-verification-key-env",
      GRANT_KEY_ENV,
      "--allow-origin",
      pageOrigin,
      "--port",
      String(servePort),
    ],
    {
      [DATABASE_ENV]: databaseUrl,
      [GRANT_KEY_ENV]: JSON.stringify(grant.verificationKeyEnvironment),
    },
    serveLog,
    READY_TIMEOUT_MS,
  );
  let browser: Browser | undefined;
  const pageServer = servePage(pagePort, pageRoot);
  try {
    assert.equal(serving.contract.get("ready"), "true");
    const requireFromClient = createRequire(
      join(root, "client/packages/client/package.json"),
    );
    const { chromium } = requireFromClient(
      "playwright",
    ) as typeof import("playwright");
    const pageErrors: string[] = [];
    browser = await chromium.launch({ headless: true });
    const openedBrowser = browser;
    // Each visitor is a fresh profile with an empty HTTP cache.
    const visit = async (): Promise<{
      page: Page;
      plan: Promise<PlannedBootstrap>;
    }> => {
      const context = await openedBrowser.newContext({
        viewport: { width: 1280, height: 900 },
      });
      const { plan } = await firstPlan(context);
      const page = await context.newPage();
      page.on("pageerror", (error) => pageErrors.push(String(error)));
      await page.addInitScript(
        ([injectedEndpoint, injectedGrant, injectedDevice]) => {
          Object.defineProperty(globalThis, "__calqueNarrowConfig", {
            value: {
              endpoint: injectedEndpoint,
              grant: injectedGrant,
              deviceId: injectedDevice,
            },
          });
        },
        [endpoint, grant.token, `serve-narrow-${randomUUID()}`] as const,
      );
      return { page, plan };
    };
    const stateOf = (page: Page): Promise<NarrowPageState> =>
      page.evaluate(() => globalThis.__calqueNarrowState());
    const first = await visit();
    const { page } = first;
    const state = (): Promise<NarrowPageState> => stateOf(page);
    const failIfBroken = (current: NarrowPageState): void => {
      if (current.state === "error")
        throw new Error(
          `the narrow page failed: ${current.failure ?? "unknown"}`,
        );
    };

    await page.goto(pageOrigin, { waitUntil: "load", timeout: 60_000 });
    const readyAfterMs = await waitFor(
      "the narrow page to mount the complete projects working set",
      MOUNT_TIMEOUT_MS,
      async () => {
        const current = await state();
        failIfBroken(current);
        return current.state === "ready";
      },
    );
    const mounted = await state();
    assert.deepEqual(mounted.collections, ["projects"]);
    assert.deepEqual(mounted.projects, committedProjects());
    assert.equal(mounted.projects.length, 3);
    assert.equal(mounted.statuses.at(-1), "ready");
    // Nothing of `projects` alone is published yet: the first visitor
    // installs the artifact of every served table.
    const firstPlanned = await first.plan;
    assert.equal(firstPlanned.artifacts.length, 1);
    assert.equal(firstPlanned.artifacts[0]?.tables.length, 2);

    // One transaction over both tables: its projects part must show, and the
    // tasks part must not break the page.
    psql(`
      BEGIN;
      UPDATE projects SET status = 'shipped' WHERE id = 101;
      UPDATE tasks SET done = true WHERE id = 1001;
      COMMIT;`);
    await waitFor(
      "the mixed transaction's projects change",
      LIVE_TIMEOUT_MS,
      async () => {
        const current = await state();
        failIfBroken(current);
        return current.projects.some(
          ([id, , status]) => id === 101 && status === "shipped",
        );
      },
    );
    const afterMixed = await state();
    assert.deepEqual(afterMixed.projects, committedProjects());
    for (const collections of afterMixed.publishedCollections)
      assert.ok(
        !collections.includes("tasks"),
        `a publication named an unbound collection: ${JSON.stringify(afterMixed.publishedCollections)}`,
      );

    // A transaction that touches only the unbound table, then a bound change:
    // the page is still live if the second one arrives.
    psql("UPDATE tasks SET title = 'unbound change' WHERE id = 1002;");
    await new Promise((done) => setTimeout(done, QUIET_WATCH_MS));
    failIfBroken(await state());
    psql(`
      INSERT INTO tasks (id, project_id, title) VALUES (1004, 103, 'new');
      INSERT INTO projects (id, name, status) VALUES (104, 'Audit', 'planning');`);
    await waitFor(
      "a projects insert after an unbound-only transaction",
      LIVE_TIMEOUT_MS,
      async () => {
        const current = await state();
        failIfBroken(current);
        return current.projects.some(([id]) => id === 104);
      },
    );
    const final = await state();
    assert.equal(final.state, "ready");
    assert.deepEqual(
      final.statuses,
      mounted.statuses,
      "the page rebuilt its session after a change to an unbound table",
    );
    assert.deepEqual(final.projects, committedProjects());

    // The first plan installed more than its page reads, so the server
    // derives the artifact of `projects` alone at the same cut.
    await waitFor(
      "the server to derive the artifact of the page's tables",
      DERIVE_TIMEOUT_MS,
      () =>
        Promise.resolve(
          serveLog.join("").includes("subset derivation finished"),
        ),
    );
    const second = await visit();
    await second.page.goto(pageOrigin, { waitUntil: "load", timeout: 60_000 });
    const secondReadyAfterMs = await waitFor(
      "the second visitor to mount the projects working set",
      MOUNT_TIMEOUT_MS,
      async () => {
        const current = await stateOf(second.page);
        failIfBroken(current);
        return current.state === "ready";
      },
    );
    const secondPlanned = await second.plan;
    assert.equal(secondPlanned.artifacts.length, 1);
    assert.equal(
      installedTables(secondPlanned),
      JSON.stringify(secondPlanned.requiredTables),
      "the second visitor installed tables its page does not read",
    );
    // Caught up from the derived artifact's cut, through every change since.
    assert.deepEqual(
      (await stateOf(second.page)).projects,
      committedProjects(),
    );
    psql("UPDATE projects SET name = 'Audit (signed off)' WHERE id = 104;");
    await waitFor(
      "a projects change on the second visitor",
      LIVE_TIMEOUT_MS,
      async () => {
        const current = await stateOf(second.page);
        failIfBroken(current);
        return current.projects.some(
          ([id, name]) => id === 104 && name === "Audit (signed off)",
        );
      },
    );
    assert.deepEqual(
      (await stateOf(second.page)).projects,
      committedProjects(),
    );
    assert.deepEqual(pageErrors, []);

    const [whole] = firstPlanned.artifacts;
    const [part] = secondPlanned.artifacts;
    return {
      result: "PASS",
      runId,
      postgresPort: Number(postgresPort),
      readyAfterMs,
      projects: final.projects.length,
      publications: final.publishedCollections.length,
      secondReadyAfterMs,
      firstVisitor: {
        tables: whole?.tables.length,
        decodedBytes: whole?.decodedBytes,
        encodedBytes: whole?.selectedRepresentation.encodedBytes,
      },
      secondVisitor: {
        tables: part?.tables.length,
        decodedBytes: part?.decodedBytes,
        encodedBytes: part?.selectedRepresentation.encodedBytes,
      },
    };
  } catch (error) {
    process.stderr.write(
      `calque serve log:\n${serveLog.join("").slice(-8000)}\n`,
    );
    throw error;
  } finally {
    await browser?.close();
    await pageServer.close();
    await stopServe(serving);
  }
}

let status = 0;
try {
  process.stdout.write(`${JSON.stringify(await main(), null, 2)}\n`);
} catch (error) {
  status = 1;
  process.stderr.write(
    `serve-narrow-e2e: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
  );
} finally {
  adminPsql(DROP_RUN_DATABASE_SQL, false);
  rmSync(workDirectory, { recursive: true, force: true });
  if (!keepContainer) removePostgresContainer(container);
}
process.exit(status);
