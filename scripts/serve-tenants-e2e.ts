#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Drives a `calque serve` whose scope is partitioned by `tenant_id` from
 * headless Chromium, through the packaged client and generated bindings.
 *
 * Two tenants hold the same task and project ids and differ only in their
 * tenant column and their text, and both see one shared priorities table.
 * This script plays the application backend: it mints one grant per tenant
 * and warms tenant 2 with a signed tenant order before any page opens. Tenant
 * 1 is cold, and its build is held behind a table lock until its page has
 * reported `preparing`, so the client's retry is the one under test.
 *
 * Each page must paint its own tenant's rows and never the other's, also
 * after a change to each tenant at the source. Then the run hands tenant 1's
 * client tenant 2's bytes: a change response tenant 2's page received; on a
 * fresh page, tenant 2's artifact in place of its own; and on another,
 * tenant 2's artifact entry in place of its own in its bootstrap plan, which
 * matches the artifact in every check but the scope hash. The client must
 * refuse each and paint nothing of tenant 2.
 *
 * Usage: `pnpm serve:e2e:tenants`. Requires Docker and a Rust toolchain.
 * `CALQUE_SERVE_TENANTS_E2E_CONTAINER` and
 * `CALQUE_SERVE_TENANTS_E2E_POSTGRES_PORT` select the container (default
 * `calque-servetenants-pg18` on 55493), and
 * `CALQUE_SERVE_TENANTS_E2E_KEEP_CONTAINER=1` leaves it running.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import process from "node:process";
import type { Browser, Page } from "playwright";
import {
  loadSigningKey,
  mintTenantOrder,
  type TenantAction,
} from "../client/examples/grant-issuer/dataset-grant.mts";
import {
  buildServeBinaries,
  ensurePostgresContainer,
  psqlIn,
  removePostgresContainer,
  repositoryRoot as root,
  reservedPort,
  run,
  servePage,
  startServe,
  stopServe,
  waitFor,
  type Serving,
} from "./lib/serve-e2e.ts";

/** What the page exposes; see `tests/serve-tenants/main.ts`. */
interface StatusRecord {
  readonly atEpochMs: number;
  readonly state: string;
  readonly message: string | null;
}

interface CarriedResponse {
  readonly status: number;
  readonly headers: readonly (readonly [string, string])[];
  readonly base64: string;
}

interface TenantsPageState {
  readonly role: string;
  readonly state: string;
  readonly failure: string | null;
  readonly statuses: readonly StatusRecord[];
  readonly bootstraps: number;
  readonly artifactUrl: string | null;
  readonly planArtifact: unknown;
  readonly cursor: string | null;
  readonly tasks: readonly (readonly [number, number, string])[];
  readonly priorities: readonly (readonly [number, string])[];
  readonly painted: readonly string[];
  readonly foreignReceived: readonly string[];
  readonly substituted: readonly string[];
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations to `typeof globalThis`.
  var __calqueTenantsState: () => TenantsPageState;
  var __calqueTenantsChangeResponseWith: (
    marker: string,
  ) => CarriedResponse | null;
  var __calqueTenantsAnswerChange: (response: CarriedResponse) => void;
}

interface TenantReport {
  readonly tenant: string;
  readonly state: string;
  readonly pinned: boolean;
  readonly cutAgeSeconds: number | null;
  readonly sizeBytes: number | null;
}

const runId = randomUUID().replaceAll("-", "").slice(0, 12);
const workDirectory = join(root, "target/serve-tenants-e2e", runId);
const pageRoot = join(root, "target/client-serve-tenants");
const container =
  process.env.CALQUE_SERVE_TENANTS_E2E_CONTAINER ?? "calque-servetenants-pg18";
const postgresPort =
  process.env.CALQUE_SERVE_TENANTS_E2E_POSTGRES_PORT ?? "55493";
const keepContainer =
  process.env.CALQUE_SERVE_TENANTS_E2E_KEEP_CONTAINER === "1";
/** This run's own database, created and dropped here; see `serve-mutation-e2e`. */
const runDatabase = `calque_serve_tenants_${runId}`;
const runSlot = `${runDatabase}_slot`;
const runPublication = `${runDatabase}_pub`;
const DATABASE_ENV = "CALQUE_SERVE_TENANTS_E2E_DATABASE_URL";
const GRANT_KEY_ENV = "CALQUE_SERVE_TENANTS_E2E_GRANT_KEY";
const GRANT_AUDIENCE = "serve-tenants.e2e";
const KEY_ID = "serve-tenants-e2e";
const READY_TIMEOUT_MS = 300_000;
const MOUNT_TIMEOUT_MS = 180_000;
const CHANGE_TIMEOUT_MS = 60_000;
const REFUSAL_TIMEOUT_MS = 60_000;

const TENANT_ONE = "tenant-1-only";
const TENANT_TWO = "tenant-2-only";
const TASKS_PER_TENANT = 3;
const PRIORITIES = 2;
const CHANGED_ONE = `${TENANT_ONE} changed ${runId}`;
const CHANGED_TWO = `${TENANT_TWO} changed ${runId}`;

const SEED_SQL = `
CREATE TABLE priorities (
  id bigint PRIMARY KEY,
  label text NOT NULL
);
CREATE TABLE projects (
  tenant_id bigint NOT NULL,
  id bigint NOT NULL,
  name text NOT NULL,
  PRIMARY KEY (tenant_id, id)
);
CREATE TABLE tasks (
  tenant_id bigint NOT NULL,
  id bigint NOT NULL,
  project_id bigint NOT NULL,
  title text NOT NULL,
  done boolean NOT NULL DEFAULT false,
  PRIMARY KEY (tenant_id, id)
);
INSERT INTO priorities (id, label) VALUES (1, 'shared high'), (2, 'shared low');
INSERT INTO projects (tenant_id, id, name)
  SELECT tenant, project, 'tenant-' || tenant || '-only project ' || project
  FROM generate_series(1, 2) AS tenant, generate_series(1, 2) AS project;
INSERT INTO tasks (tenant_id, id, project_id, title, done)
  SELECT tenant, task, 1 + task % 2, 'tenant-' || tenant || '-only task ' || task, task % 2 = 0
  FROM generate_series(1, 2) AS tenant, generate_series(1, ${String(TASKS_PER_TENANT)}) AS task;
CREATE PUBLICATION ${runPublication} FOR TABLE priorities, projects, tasks;
`;

const DROP_RUN_DATABASE_SQL = `
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
  WHERE database = '${runDatabase}' AND NOT active;
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE datname = '${runDatabase}' AND pid <> pg_backend_pid();
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
  WHERE database = '${runDatabase}';
DROP DATABASE IF EXISTS ${runDatabase};
`;

function psql(sql: string, stopOnError = true): string {
  return psqlIn(container, runDatabase, sql, stopOnError);
}

function adminPsql(sql: string, stopOnError = true): string {
  return psqlIn(container, "calque_verify", sql, stopOnError);
}

/**
 * A transaction holding `tasks` in ACCESS EXCLUSIVE mode, so a tenant build's
 * copy of it waits until `release` commits.
 */
async function lockTasks(): Promise<{ release: () => Promise<void> }> {
  const session = spawn(
    "docker",
    [
      "exec",
      "-i",
      container,
      "psql",
      "-U",
      "calque_test",
      "-d",
      runDatabase,
      "-X",
      "-q",
      "-v",
      "ON_ERROR_STOP=1",
    ],
    { stdio: ["pipe", "pipe", "inherit"] },
  );
  session.stdin.write(
    "BEGIN; LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n",
  );
  await new Promise<void>((resolve, reject) => {
    let output = "";
    session.stdout.setEncoding("utf8");
    session.stdout.on("data", (chunk: string) => {
      output += chunk;
      if (output.includes("locked")) resolve();
    });
    session.once("exit", (code) => {
      reject(new Error(`the lock session exited ${String(code)}`));
    });
  });
  return {
    release: async () => {
      session.stdin.end("COMMIT;\n");
      await new Promise<void>((resolve) => {
        if (session.exitCode !== null) resolve();
        else session.once("exit", () => resolve());
      });
    },
  };
}

async function postJson(
  url: string,
  body: unknown,
): Promise<{ status: number; body: unknown }> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let parsed: unknown = text;
  try {
    parsed = JSON.parse(text);
  } catch {
    // Kept as text for the failure message.
  }
  return { status: response.status, body: parsed };
}

async function stateOf(page: Page): Promise<TenantsPageState> {
  return await page.evaluate(() => globalThis.__calqueTenantsState());
}

async function openPage(
  browser: Browser,
  pageOrigin: string,
  endpoint: string,
  grant: string,
  role: string,
  foreignMarkers: readonly string[],
  artifactFrom: string | null,
  pageErrors: string[],
  planArtifactFrom: unknown = null,
): Promise<Page> {
  const page = await browser.newPage({
    viewport: { width: 1280, height: 900 },
  });
  page.on("pageerror", (error) => pageErrors.push(`${role}: ${String(error)}`));
  await page.addInitScript(
    ([injected]) => {
      Object.defineProperty(globalThis, "__calqueTenantsConfig", {
        value: injected,
      });
    },
    [
      {
        endpoint,
        grant,
        deviceId: `serve-tenants-${role}-${randomUUID()}`,
        role,
        foreignMarkers,
        artifactFrom,
        planArtifactFrom,
      },
    ] as const,
  );
  await page.goto(pageOrigin, { waitUntil: "load", timeout: 60_000 });
  return page;
}

async function untilMounted(page: Page, role: string): Promise<void> {
  await waitFor(
    `the ${role} page to mount its tenant's working set`,
    MOUNT_TIMEOUT_MS,
    async () => {
      const state = await stateOf(page);
      if (state.state === "error")
        throw new Error(
          `the ${role} page failed: ${state.failure ?? "unknown"}`,
        );
      return (
        state.state === "ready" &&
        state.tasks.length === TASKS_PER_TENANT &&
        state.priorities.length === PRIORITIES
      );
    },
  );
}

/** One page's tenant isolation, asserted on everything it painted and received. */
function assertIsolated(
  state: TenantsPageState,
  own: string,
  foreign: string,
): void {
  assert.ok(
    state.tasks.every(([, , title]) => title.startsWith(own)),
    `${state.role} holds a row that is not its tenant's: ${JSON.stringify(state.tasks)}`,
  );
  assert.deepEqual(
    state.painted.filter((title) => title.includes(foreign)),
    [],
    `${state.role} painted the other tenant's rows`,
  );
  assert.deepEqual(
    state.foreignReceived,
    [],
    `${state.role} was sent the other tenant's rows`,
  );
}

async function main(): Promise<unknown> {
  mkdirSync(workDirectory, { recursive: true });
  const binaries = buildServeBinaries();
  run("pnpm", ["--filter", "@calque/client...", "build"]);
  run("pnpm", ["--filter", "@calque/client", "serve-tenants:build"]);
  ensurePostgresContainer(container, postgresPort);
  const stateDirectory = join(workDirectory, "state");
  const servePort = await reservedPort();
  const pagePort = await reservedPort();
  const pageOrigin = `http://127.0.0.1:${String(pagePort)}`;
  const endpoint = `http://127.0.0.1:${String(servePort)}/`;
  const databaseUrl = `postgresql://calque_test:calque_test@127.0.0.1:${postgresPort}/${runDatabase}`;
  const serveLog: string[] = [];

  // The manifest the page's bindings were generated from, with the fields a
  // deployment owns rewritten.
  const configPath = join(workDirectory, "calque.config.json");
  const manifest = JSON.parse(
    readFileSync(
      join(root, "tests/fixtures/config/serve-tenants.config.json"),
      "utf8",
    ),
  ) as {
    sources: { connectionEnv: string; slot: string; publication: string }[];
  };
  const [source] = manifest.sources;
  if (!source) throw new Error("the tenants fixture declares no source");
  source.connectionEnv = DATABASE_ENV;
  source.slot = runSlot;
  source.publication = runPublication;
  writeFileSync(configPath, `${JSON.stringify(manifest, null, 2)}\n`);

  adminPsql(DROP_RUN_DATABASE_SQL, false);
  adminPsql(`CREATE DATABASE ${runDatabase} OWNER calque_test;`);
  psql(SEED_SQL);

  // `calque-dev-grant` creates the key and mints tenant 1's grant; the Node
  // example mints tenant 2's and signs the tenant orders, so `calque serve`
  // verifies what no Rust code signed.
  const keyFile = join(workDirectory, "grant-key.pk8");
  const mint = (
    issuer: readonly [string, ...string[]],
    tenant: number,
  ): { token: string; verificationKeyEnvironment: unknown } =>
    JSON.parse(
      run(issuer[0], [
        ...issuer.slice(1),
        "--key-file",
        keyFile,
        "--key-id",
        KEY_ID,
        "--audience",
        GRANT_AUDIENCE,
        "--subject",
        `tenant-${String(tenant)}-member`,
        "--generation",
        "1",
        "--lifetime-seconds",
        "900",
        "--attribute",
        `tenant_id=int8:${String(tenant)}`,
      ]),
    ) as { token: string; verificationKeyEnvironment: unknown };
  const grantOne = mint([join(binaries, "calque-dev-grant")], 1);
  const grantTwo = mint(
    [
      process.execPath,
      join(root, "client/examples/grant-issuer/mint-grant.mts"),
    ],
    2,
  );
  const signingKey = loadSigningKey(readFileSync(keyFile));
  const order = (action: TenantAction, tenant: number): string => {
    const now = Math.floor(Date.now() / 1000);
    return mintTenantOrder(
      {
        keyId: KEY_ID,
        audience: GRANT_AUDIENCE,
        action,
        tenant: { int8: String(tenant) },
        issuedAtUnixSeconds: now,
        expiresAtUnixSeconds: now + 120,
      },
      signingKey,
    );
  };

  const serving: Serving = await startServe(
    join(binaries, "calque"),
    [
      "--config",
      configPath,
      "--state-dir",
      stateDirectory,
      "--grant-audience",
      GRANT_AUDIENCE,
      "--grant-verification-key-env",
      GRANT_KEY_ENV,
      "--allow-origin",
      pageOrigin,
      "--port",
      String(servePort),
      // Shorter than the default hold, so the locked build answers preparing
      // within a second.
      "--tenant-hold-open-seconds",
      "1",
    ],
    {
      [DATABASE_ENV]: databaseUrl,
      [GRANT_KEY_ENV]: JSON.stringify(grantOne.verificationKeyEnvironment),
    },
    serveLog,
    READY_TIMEOUT_MS,
  );
  let browser: Browser | undefined;
  const pageServer = servePage(pagePort, pageRoot);
  try {
    const tenants = new URL("v1/tenants", endpoint).href;

    // A browser's grant is not an order.
    const grantAsOrder = await postJson(tenants, { order: grantTwo.token });
    assert.equal(grantAsOrder.status, 401, JSON.stringify(grantAsOrder.body));

    const warmed = await postJson(tenants, { order: order("warm", 2) });
    assert.equal(warmed.status, 200, JSON.stringify(warmed.body));
    await waitFor("tenant 2 to be warm", MOUNT_TIMEOUT_MS, async () => {
      const reported = await postJson(tenants, { order: order("state", 2) });
      assert.equal(reported.status, 200, JSON.stringify(reported.body));
      return (reported.body as TenantReport).state === "warm";
    });
    const coldOne = (await postJson(tenants, { order: order("state", 1) }))
      .body as TenantReport;
    assert.equal(coldOne.state, "cold");

    const requireFromClient = createRequire(
      join(root, "client/packages/client/package.json"),
    );
    const { chromium } = requireFromClient(
      "playwright",
    ) as typeof import("playwright");
    const pageErrors: string[] = [];
    browser = await chromium.launch({ headless: true });

    // Tenant 1 is cold: its first request starts its build, which waits on
    // the lock, so the server answers preparing until the lock is released.
    const lock = await lockTasks();
    let one: Page;
    try {
      one = await openPage(
        browser,
        pageOrigin,
        endpoint,
        grantOne.token,
        "tenant-1",
        [TENANT_TWO],
        null,
        pageErrors,
      );
      await waitFor(
        "the tenant 1 page to report preparing",
        MOUNT_TIMEOUT_MS,
        async () =>
          (await stateOf(one)).statuses.some(
            ({ state }) => state === "preparing",
          ),
      );
    } finally {
      await lock.release();
    }
    await untilMounted(one, "tenant-1");
    const preparedOne = await stateOf(one);
    assert.ok(
      preparedOne.bootstraps >= 2,
      "the client retried the preparing answer",
    );

    const two = await openPage(
      browser,
      pageOrigin,
      endpoint,
      grantTwo.token,
      "tenant-2",
      [TENANT_ONE],
      null,
      pageErrors,
    );
    await untilMounted(two, "tenant-2");
    const warmTwo = await stateOf(two);
    assert.equal(warmTwo.bootstraps, 1, "a warm tenant is served at once");
    assert.ok(
      !warmTwo.statuses.some(({ state }) => state === "preparing"),
      "a warm tenant never answers preparing",
    );
    assert.deepEqual(
      preparedOne.priorities,
      warmTwo.priorities,
      "both tenants see the shared table whole",
    );
    assertIsolated(preparedOne, TENANT_ONE, TENANT_TWO);
    assertIsolated(warmTwo, TENANT_TWO, TENANT_ONE);

    // A change to each tenant reaches that tenant's page and no other.
    psql(`UPDATE tasks SET title = '${CHANGED_TWO}' WHERE tenant_id = 2 AND id = 1;
UPDATE tasks SET title = '${CHANGED_ONE}' WHERE tenant_id = 1 AND id = 2;`);
    for (const [page, title] of [
      [one, CHANGED_ONE],
      [two, CHANGED_TWO],
    ] as const)
      await waitFor(`${title} to reach its page`, CHANGE_TIMEOUT_MS, async () =>
        (await stateOf(page)).tasks.some(([, , painted]) => painted === title),
      );
    const changedOne = await stateOf(one);
    const changedTwo = await stateOf(two);
    assertIsolated(changedOne, TENANT_ONE, TENANT_TWO);
    assertIsolated(changedTwo, TENANT_TWO, TENANT_ONE);

    // Tenant 2's change response, handed to tenant 1's client.
    const carried = await two.evaluate(
      (marker) => globalThis.__calqueTenantsChangeResponseWith(marker),
      CHANGED_TWO,
    );
    assert.ok(carried, "tenant 2's page recorded the change response it got");
    await one.evaluate((response) => {
      globalThis.__calqueTenantsAnswerChange(response);
    }, carried);
    await waitFor(
      "tenant 1's client to refuse tenant 2's changes",
      REFUSAL_TIMEOUT_MS,
      async () => {
        const state = await stateOf(one);
        return (
          state.state === "error" || state.bootstraps > preparedOne.bootstraps
        );
      },
    );
    const refusedChange = await stateOf(one);
    assert.deepEqual(refusedChange.substituted, ["change"]);
    assert.deepEqual(
      refusedChange.painted.filter((title) => title.includes(TENANT_TWO)),
      [],
      "tenant 1 painted a row of tenant 2's change",
    );

    // Tenant 2's artifact, fetched by a fresh tenant 1 page in place of its own.
    assert.ok(warmTwo.artifactUrl, "tenant 2's plan named its artifact");
    const swapped = await openPage(
      browser,
      pageOrigin,
      endpoint,
      grantOne.token,
      "tenant-1-swapped",
      [],
      warmTwo.artifactUrl,
      pageErrors,
    );
    await waitFor(
      "tenant 1's client to refuse tenant 2's artifact",
      REFUSAL_TIMEOUT_MS,
      async () => (await stateOf(swapped)).state === "error",
    );
    const refusedArtifact = await stateOf(swapped);
    assert.ok(refusedArtifact.substituted.includes("artifact"));
    assert.deepEqual(refusedArtifact.tasks, []);
    assert.deepEqual(refusedArtifact.painted, []);

    // Tenant 2's artifact entry in tenant 1's plan: its length and digest
    // match the bytes fetched, so the artifact's scope hash, set against the
    // plan's, is what refuses it.
    assert.ok(warmTwo.planArtifact, "tenant 2's plan held its artifact entry");
    const rebound = await openPage(
      browser,
      pageOrigin,
      endpoint,
      grantOne.token,
      "tenant-1-rebound",
      [],
      null,
      pageErrors,
      warmTwo.planArtifact,
    );
    await waitFor(
      "tenant 1's client to refuse tenant 2's artifact under its own plan",
      REFUSAL_TIMEOUT_MS,
      async () => (await stateOf(rebound)).state === "error",
    );
    const refusedScope = await stateOf(rebound);
    assert.deepEqual(refusedScope.substituted, ["plan artifact"]);
    assert.equal(refusedScope.artifactUrl, warmTwo.artifactUrl);
    assert.match(refusedScope.failure ?? "", /identity binding mismatch/u);
    assert.deepEqual(refusedScope.tasks, []);
    assert.deepEqual(refusedScope.painted, []);
    assert.deepEqual(pageErrors, []);

    const finalOne = (await postJson(tenants, { order: order("state", 1) }))
      .body as TenantReport;
    assert.equal(finalOne.state, "warm");
    return {
      result: "PASS",
      runId,
      tenantOne: {
        statuses: preparedOne.statuses.map(({ state }) => state),
        bootstraps: preparedOne.bootstraps,
        tasks: changedOne.tasks.length,
        sizeBytes: finalOne.sizeBytes,
      },
      tenantTwo: {
        warmedBy: "tenant order",
        bootstraps: warmTwo.bootstraps,
        tasks: changedTwo.tasks.length,
      },
      sharedPriorities: preparedOne.priorities.length,
      refusedChange: {
        state: refusedChange.state,
        failure: refusedChange.failure,
        bootstraps: refusedChange.bootstraps,
      },
      refusedArtifact: {
        state: refusedArtifact.state,
        failure: refusedArtifact.failure,
      },
      refusedScope: {
        state: refusedScope.state,
        failure: refusedScope.failure,
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
  const outcome = await main();
  process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
} catch (error) {
  status = 1;
  process.stderr.write(
    `serve-tenants-e2e: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
  );
} finally {
  adminPsql(DROP_RUN_DATABASE_SQL, false);
  rmSync(workDirectory, { recursive: true, force: true });
  if (!keepContainer) removePostgresContainer(container);
}
process.exit(status);
