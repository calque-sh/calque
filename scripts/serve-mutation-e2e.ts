#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Drives `calque serve`'s mutation route end to end from headless Chromium,
 * through the packaged client and the generated typed facade.
 *
 * One `calque serve` and one disposable PostgreSQL 18 serve two pages that
 * differ only in their grant. The writer's carries `--command tasks=update`:
 * its one `db.tasks.update` call must be answered 200 and show the optimistic
 * read, the committed row and the canonical echo. The reader's carries no
 * command: it sees the writer's change through CDC alone, and then the same
 * facade call gets a 404, ends `rejected` with its delta gone, and is not
 * resubmitted. The page records every `POST /v1/mutations` and its status, so
 * "no row changed" cannot pass on a page that sent nothing.
 *
 * Usage: `pnpm serve:e2e`. Requires Docker and a Rust toolchain.
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
import type { Browser, Page } from "playwright";
import {
  buildServeBinaries,
  ensurePostgresContainer,
  psqlIn,
  removePostgresContainer,
  repositoryRoot as root,
  reservedPort,
  run,
  servePage,
  startServe as spawnServe,
  stopServe,
  waitFor,
  type Serving,
} from "./lib/serve-e2e.ts";

/** One `POST /v1/mutations`, as the page's own `fetch` saw it. */
interface MutationRequestRecord {
  readonly url: string;
  readonly status: number | null;
  readonly failure: string | null;
  readonly atEpochMs: number;
}

/** One facade mutation call on a page, settled either way. */
interface MutationAttempt {
  readonly collection: string;
  readonly key: number;
  readonly title: string;
  readonly settled: "accepted" | "refused";
  readonly state: string | null;
  readonly resultMessage: string | null;
  readonly mutationId: string | null;
  readonly errorName: string | null;
  readonly errorCode: string | null;
  readonly errorMessage: string | null;
  readonly statusAtSettle: string | null;
  readonly calledAtEpochMs: number;
  readonly settledAtEpochMs: number;
}

interface CanonicalEcho {
  readonly mutationId: string;
  readonly observedAtEpochMs: number;
  readonly publicationsAtEcho: number;
  readonly cursorAtEcho: string | null;
  readonly title: string | null;
}

/** What the page under test exposes; see the page's own `main.ts`. */
interface ServeMutationPageState {
  readonly role: string;
  readonly state: string;
  readonly failure: string | null;
  readonly cursor: string | null;
  readonly publications: number;
  readonly projects: readonly (readonly [number, string, string])[];
  readonly tasks: readonly (readonly [number, string, boolean])[];
  readonly facadeWrites: {
    readonly insert: boolean;
    readonly update: boolean;
    readonly delete: boolean;
  };
  readonly mutationRequests: readonly MutationRequestRecord[];
  readonly attempts: readonly MutationAttempt[];
  readonly mutationStatuses: readonly (readonly [string, string])[];
  readonly canonicalEchoes: readonly CanonicalEcho[];
  readonly taskPublications: readonly (readonly [string, number])[];
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations to `typeof globalThis`.
  var __calqueServeMutationState: () => ServeMutationPageState;
  var __calqueServeMutationUpdateTask: (
    id: number,
    title: string,
  ) => Promise<MutationAttempt>;
}

const runId = randomUUID().replaceAll("-", "").slice(0, 12);
const workDirectory = join(root, "target/serve-mutation-e2e", runId);
const pageRoot = join(root, "target/client-serve-mutation");
const postgresPort = process.env.CALQUE_SERVE_E2E_POSTGRES_PORT ?? "55491";
const container = "calque-servee2e-pg18";
const keepContainer = process.env.CALQUE_SERVE_E2E_KEEP_CONTAINER === "1";
/**
 * A database of this run's own, created and dropped here. `calque serve`
 * leaves durable state in the source, and a second run on the same database
 * would inherit a canonical log written under an epoch its own enrollment does
 * not carry, which `serve` refuses at CDC startup.
 */
const runDatabase = `calque_serve_e2e_${runId}`;
const runSlot = `${runDatabase}_slot`;
const runPublication = `${runDatabase}_pub`;
const DATABASE_ENV = "CALQUE_SERVE_E2E_DATABASE_URL";
const GRANT_KEY_ENV = "CALQUE_SERVE_E2E_GRANT_KEY";
const GRANT_AUDIENCE = "serve-mutation.e2e";
const READY_TIMEOUT_MS = 300_000;
const MOUNT_TIMEOUT_MS = 180_000;
const ECHO_TIMEOUT_MS = 60_000;
/**
 * How long the run watches for a retry the page did not ask for. The change
 * poll, the only timer still running after the refusal, cycles well inside
 * this.
 */
const QUIET_WATCH_MS = 15_000;

/** The rows `SEED_SQL` inserts. */
const SEEDED_PROJECTS = 3;
const SEEDED_TASKS = 9;
/** The row the writer page mutates, and the row the reader page tries to. */
const WRITTEN_TASK_ID = 1001;
const REFUSED_TASK_ID = 1002;
const WRITTEN_TITLE = `written from Chromium ${runId}`;
const REFUSED_TITLE = `refused from Chromium ${runId}`;
/** What the seed puts in the refused row, and what must survive the refusal. */
const REFUSED_TASK_SEEDED_TITLE = "Validate the data migration";
/** Written to the refused row from PostgreSQL, after the refusal. */
const SOURCE_TITLE = `changed at the source ${runId}`;

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
  (1002, 101, '${REFUSED_TASK_SEEDED_TITLE}', true),
  (1003, 101, 'Review launch telemetry', false),
  (1004, 102, 'Rehearse regional failover', true),
  (1005, 102, 'Tune alert thresholds', true),
  (1006, 102, 'Document the recovery path', true),
  (1007, 103, 'Approve the offline experience', false),
  (1008, 103, 'Run the beta cohort', false),
  (1009, 103, 'Finish the accessibility pass', true);
CREATE PUBLICATION ${runPublication} FOR TABLE projects, tasks;
`;

/**
 * The run's database, removed whole. The slot goes first because a database
 * with one still attached cannot be dropped, and the backends go next because
 * `calque serve` may not have closed its connections yet.
 */
const DROP_RUN_DATABASE_SQL = `
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
  WHERE slot_name = '${runSlot}';
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE datname = '${runDatabase}' AND pid <> pg_backend_pid();
DROP DATABASE IF EXISTS ${runDatabase};
`;

interface RunOutcome {
  readonly result: "PASS";
  readonly runId: string;
  readonly postgresPort: number;
  readonly serveStartupMillis: number;
  readonly mutationsLine: string;
  readonly mutationCommandsLine: string;
  readonly projects: number;
  readonly tasks: number;
  /** The positive case. */
  readonly write: {
    readonly mutationId: string;
    readonly httpStatus: number;
    readonly acceptedState: string;
    readonly optimisticTitle: string;
    readonly committedTitle: string;
    readonly canonicalEchoTitle: string;
    readonly submitToEchoMs: number;
    readonly taskPublicationsAfterWrite: number;
  };
  /** The CDC echo, observed by a page that applied no optimistic delta. */
  readonly readerEcho: {
    readonly title: string;
    readonly publicationsBefore: number;
    readonly publicationsAfter: number;
    readonly observedAfterMs: number;
  };
  /** The negative case. */
  readonly refusal: {
    readonly facadeExposedUpdate: true;
    readonly httpStatus: number;
    readonly requestsAtSettle: number;
    readonly requestsAfterQuietMs: number;
    readonly quietMs: number;
    readonly errorName: string;
    readonly errorCode: string;
    readonly mutationStatusAfterRefusal: string;
    readonly clientStateAfterRefusal: string;
    readonly optimisticDeltaStillVisible: boolean;
    readonly committedTitleUnchanged: string;
    /** What the page reads for that row after a source change to it. */
    readonly titleAfterSourceChange: string;
    readonly sourceTitle: string;
  };
}

/** This run's own database, where the seeded relations live. */
function psql(sql: string, stopOnError = true): string {
  return psqlIn(container, runDatabase, sql, stopOnError);
}

/**
 * The bootstrap database the live suite created, used only to create and drop
 * this run's database.
 */
function adminPsql(sql: string, stopOnError = true): string {
  return psqlIn(container, "calque_verify", sql, stopOnError);
}

function committedTitle(id: number): string {
  return psql(`SELECT title FROM tasks WHERE id = ${String(id)};`).trim();
}

/**
 * `calque serve`, started the way an operator starts it: the manifest, a state
 * directory, the grant audience, and the name of the environment variable
 * holding the verification key the minting binary wrote.
 */
async function startServe(
  binary: string,
  configPath: string,
  stateDirectory: string,
  port: number,
  pageOrigin: string,
  verificationKeyEnvironment: string,
  databaseUrl: string,
  log: string[],
): Promise<Serving> {
  return await spawnServe(
    binary,
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
      String(port),
    ],
    {
      [DATABASE_ENV]: databaseUrl,
      [GRANT_KEY_ENV]: verificationKeyEnvironment,
    },
    log,
    READY_TIMEOUT_MS,
  );
}

/** One page, mounted and ready, with the grant this role holds injected. */
async function openPage(
  browser: Browser,
  pageOrigin: string,
  endpoint: string,
  grant: string,
  role: string,
  pageErrors: string[],
): Promise<Page> {
  const page = await browser.newPage({
    viewport: { width: 1280, height: 900 },
  });
  page.on("pageerror", (error) => pageErrors.push(`${role}: ${String(error)}`));
  await page.addInitScript(
    ([injectedEndpoint, injectedGrant, injectedRole, injectedDevice]) => {
      Object.defineProperty(globalThis, "__calqueServeMutationConfig", {
        value: {
          endpoint: injectedEndpoint,
          grant: injectedGrant,
          deviceId: injectedDevice,
          role: injectedRole,
        },
      });
    },
    [endpoint, grant, role, `serve-mutation-${role}-${randomUUID()}`] as const,
  );
  await page.goto(pageOrigin, { waitUntil: "load", timeout: 60_000 });
  await waitFor(
    `the ${role} page to mount the complete working set`,
    MOUNT_TIMEOUT_MS,
    async () => {
      const state = await page.evaluate(() =>
        globalThis.__calqueServeMutationState(),
      );
      if (state.state === "error")
        throw new Error(
          `the ${role} page failed: ${state.failure ?? "unknown"}`,
        );
      return (
        state.state === "ready" &&
        state.projects.length === SEEDED_PROJECTS &&
        state.tasks.length === SEEDED_TASKS
      );
    },
  );
  return page;
}

function titleOf(state: ServeMutationPageState, id: number): string | null {
  return state.tasks.find(([taskId]) => taskId === id)?.[1] ?? null;
}

async function main(): Promise<RunOutcome> {
  mkdirSync(workDirectory, { recursive: true });
  const binaries = buildServeBinaries();
  // The page, bundled for Chromium by the same Vite project every other client
  // page uses, after the client packages it imports. Both are built here so a
  // run cannot pass against a stale bundle or fail against a stale `dist/`.
  run("pnpm", ["--filter", "@calque/client...", "build"]);
  run("pnpm", ["--filter", "@calque/client", "serve-mutation:build"]);
  ensurePostgresContainer(container, postgresPort);
  const stateDirectory = join(workDirectory, "state");
  const servePort = await reservedPort();
  const pagePort = await reservedPort();
  const pageOrigin = `http://127.0.0.1:${String(pagePort)}`;
  const endpoint = `http://127.0.0.1:${String(servePort)}/`;
  const databaseUrl = `postgresql://calque_test:calque_test@127.0.0.1:${postgresPort}/${runDatabase}`;
  const serveLog: string[] = [];

  // The deployment's manifest: the shared binding fixture with the three
  // fields a deployment owns rewritten. The commands section is the
  // fixture's, so what `serve` advertises is what the generated facades were
  // generated from.
  const configPath = join(workDirectory, "calque.config.json");
  const manifest = JSON.parse(
    readFileSync(
      join(root, "client/packages/client/tests/demo/calque.config.json"),
      "utf8",
    ),
  ) as {
    sources: { connectionEnv: string; slot: string; publication: string }[];
  };
  const [source] = manifest.sources;
  if (!source) throw new Error("the binding fixture declares no source");
  source.connectionEnv = DATABASE_ENV;
  source.slot = runSlot;
  source.publication = runPublication;
  writeFileSync(configPath, `${JSON.stringify(manifest, null, 2)}\n`);

  // This run's own source, in this run's own database.
  adminPsql(DROP_RUN_DATABASE_SQL, false);
  adminPsql(`CREATE DATABASE ${runDatabase} OWNER calque_test;`);
  psql(SEED_SQL);
  assert.equal(committedTitle(REFUSED_TASK_ID), REFUSED_TASK_SEEDED_TITLE);

  // Two grants from one key file, so one `serve` verifies both.
  // `calque-dev-grant` creates the key file and mints the capable one, naming
  // only `tasks=update`. The Node issuer example mints the other, with no
  // command, so the reader page loading at all shows `calque serve` verifying
  // a token no Rust code signed.
  const keyFile = join(workDirectory, "grant-key.pk8");
  const mint = (
    issuer: readonly [string, ...string[]],
    extra: readonly string[],
    subject: string,
  ) =>
    JSON.parse(
      run(issuer[0], [
        ...issuer.slice(1),
        "--key-file",
        keyFile,
        "--key-id",
        "serve-mutation-e2e",
        "--audience",
        GRANT_AUDIENCE,
        "--subject",
        subject,
        "--generation",
        "1",
        // The ceiling `dataset-grant` enforces (MAX_GRANT_LIFETIME_SECONDS,
        // 15 minutes). This run has to finish inside one grant's window.
        "--lifetime-seconds",
        "900",
        ...extra,
      ]),
    ) as {
      token: string;
      commands: readonly string[];
      verificationKeyEnvironment: unknown;
    };

  const capable = mint(
    [join(binaries, "calque-dev-grant")],
    ["--command", "tasks=update"],
    "serve-mutation-writer",
  );
  const readOnly = mint(
    [
      process.execPath,
      join(root, "client/examples/grant-issuer/mint-grant.mts"),
    ],
    [],
    "serve-mutation-reader",
  );
  assert.deepEqual(
    readOnly.verificationKeyEnvironment,
    capable.verificationKeyEnvironment,
    "the Node issuer must print the verification key calque-dev-grant printed for the same key file",
  );
  assert.deepEqual(
    capable.commands,
    ["tasks:update"],
    "the writer's grant must carry exactly the command capability it was minted with",
  );
  assert.deepEqual(
    readOnly.commands,
    [],
    "the reader's grant must carry no command capability at all",
  );

  const serving = await startServe(
    join(binaries, "calque"),
    configPath,
    stateDirectory,
    servePort,
    pageOrigin,
    JSON.stringify(capable.verificationKeyEnvironment),
    databaseUrl,
    serveLog,
  );
  let outcome: RunOutcome;
  let browser: Browser | undefined;
  const pageServer = servePage(pagePort, pageRoot);
  try {
    const mutationsLine = serving.contract.get("mutations") ?? "";
    const mutationCommandsLine =
      serving.contract.get("mutation_commands") ?? "";
    assert.equal(
      mutationsLine,
      "mounted-per-collection-command-grant",
      `the startup contract must say the write surface is mounted; got ${JSON.stringify(serving.lines)}`,
    );
    assert.equal(
      mutationCommandsLine,
      "projects:insert+update+delete,tasks:insert+update+delete",
    );
    assert.equal(serving.contract.get("ready"), "true");
    // calque writes to a pipe here, as under a service manager. Its log is
    // plain text, with none of the NOTICEs PostgreSQL sends for each object an
    // install's `IF NOT EXISTS` skips.
    const startupLog = serveLog.join("");
    assert.ok(
      !startupLog.includes("\u001b["),
      "calque's redirected log must carry no terminal color escapes",
    );
    assert.ok(
      !startupLog.includes("NOTICE:"),
      "calque's start must log no PostgreSQL NOTICE",
    );
    assert.equal(
      serving.contract.get("expected_host"),
      `127.0.0.1:${String(servePort)}`,
    );
    const serveStartupMillis = Number(serving.contract.get("startup_millis"));
    assert.ok(Number.isFinite(serveStartupMillis));

    const requireFromClient = createRequire(
      join(root, "client/packages/client/package.json"),
    );
    const { chromium } = requireFromClient(
      "playwright",
    ) as typeof import("playwright");
    const pageErrors: string[] = [];
    browser = await chromium.launch({ headless: true });

    // The reader first, so it is mounted and at a cursor before the write. It
    // is the CDC oracle: it never applies an optimistic delta for the writer's
    // mutation, so whatever it shows for that row came from PostgreSQL.
    const reader = await openPage(
      browser,
      pageOrigin,
      endpoint,
      readOnly.token,
      "reader",
      pageErrors,
    );
    const writer = await openPage(
      browser,
      pageOrigin,
      endpoint,
      capable.token,
      "writer",
      pageErrors,
    );

    const readerBefore = await reader.evaluate(() =>
      globalThis.__calqueServeMutationState(),
    );
    assert.equal(
      titleOf(readerBefore, WRITTEN_TASK_ID),
      "Freeze the release candidate",
      "the reader must start at the seeded value of the row the writer changes",
    );
    // The generated facade exposes the same write surface on both pages: it
    // comes from the generated schema, while the capability lives in the
    // grant, so the refusal below has to be answered on the wire.
    assert.deepEqual(readerBefore.facadeWrites, {
      insert: true,
      update: true,
      delete: true,
    });

    // ---- The positive case: one facade mutation, through to the echo. ----
    const written = await writer.evaluate(
      ([id, title]) => globalThis.__calqueServeMutationUpdateTask(id, title),
      [WRITTEN_TASK_ID, WRITTEN_TITLE] as const,
    );
    assert.equal(
      written.settled,
      "accepted",
      `the facade mutation was refused: ${JSON.stringify(written)}`,
    );
    assert.equal(
      written.state,
      "awaiting_canonical_echo",
      `the facade mutation did not reach the optimistic-then-echo path: ${JSON.stringify(written)}`,
    );
    const writtenMutationId = written.mutationId;
    assert.ok(writtenMutationId, "an accepted mutation carries an identifier");

    const afterWrite = await writer.evaluate(() =>
      globalThis.__calqueServeMutationState(),
    );
    // An assertion about the row alone would not distinguish a write from a
    // page that never sent one.
    assert.equal(
      afterWrite.mutationRequests.length,
      1,
      `the writer page sent ${String(afterWrite.mutationRequests.length)} mutation requests, not one`,
    );
    const writeRequest = afterWrite.mutationRequests[0];
    assert.ok(writeRequest);
    assert.equal(writeRequest.status, 200);
    assert.ok(
      writeRequest.url.startsWith(`${endpoint}v1/mutations`),
      `the writer posted to ${writeRequest.url}, not to calque serve`,
    );
    const optimisticTitle = titleOf(afterWrite, WRITTEN_TASK_ID);
    assert.equal(
      optimisticTitle,
      WRITTEN_TITLE,
      "the optimistic value was not readable through the facade before the echo",
    );

    // The row PostgreSQL committed, which is the evidence a status code
    // cannot carry.
    const committed = committedTitle(WRITTEN_TASK_ID);
    assert.equal(
      committed,
      WRITTEN_TITLE,
      "the facade mutation did not reach a committed PostgreSQL row",
    );

    // The canonical echo, on the writer: `canonical` is set only from
    // `acknowledgedMutationIds` on an applied change batch.
    await waitFor("the writer's canonical echo", ECHO_TIMEOUT_MS, async () => {
      const state = await writer.evaluate(() =>
        globalThis.__calqueServeMutationState(),
      );
      if (state.state === "error")
        throw new Error(
          `the writer page failed: ${state.failure ?? "unknown"}`,
        );
      return state.canonicalEchoes.length > 0;
    });
    const echoed = await writer.evaluate(() =>
      globalThis.__calqueServeMutationState(),
    );
    const echo = echoed.canonicalEchoes[0];
    assert.ok(echo);
    assert.equal(echo.mutationId, writtenMutationId);
    assert.equal(
      echo.title,
      WRITTEN_TITLE,
      "the canonical echo did not carry the value that was written",
    );
    assert.ok(
      echoed.taskPublications.length > 0,
      "no publication named tasks, so the echo did not arrive through the change stream",
    );

    // ---- The CDC echo in a browser that wrote nothing. ----
    const readerObservedAfterMs = await waitFor(
      "the reader page to see the written value arrive through CDC",
      ECHO_TIMEOUT_MS,
      async () => {
        const state = await reader.evaluate(() =>
          globalThis.__calqueServeMutationState(),
        );
        if (state.state === "error")
          throw new Error(
            `the reader page failed: ${state.failure ?? "unknown"}`,
          );
        return titleOf(state, WRITTEN_TASK_ID) === WRITTEN_TITLE;
      },
    );
    const readerAfter = await reader.evaluate(() =>
      globalThis.__calqueServeMutationState(),
    );
    assert.equal(
      readerAfter.mutationRequests.length,
      0,
      "the reader must not have written anything before its own negative case",
    );
    assert.ok(
      readerAfter.publications > readerBefore.publications,
      "the reader's value changed without a publication, so not through the change stream",
    );

    // ---- The negative case: the same facade, a grant with no capability. ----
    const refused = await reader.evaluate(
      ([id, title]) => globalThis.__calqueServeMutationUpdateTask(id, title),
      [REFUSED_TASK_ID, REFUSED_TITLE] as const,
    );
    assert.equal(
      refused.settled,
      "refused",
      `a grant with no command capability wrote through the facade: ${JSON.stringify(refused)}`,
    );
    const atSettle = await reader.evaluate(() =>
      globalThis.__calqueServeMutationState(),
    );
    assert.equal(
      atSettle.mutationRequests.length,
      1,
      `the reader page sent ${String(atSettle.mutationRequests.length)} mutation requests, not one`,
    );
    const refusedRequest = atSettle.mutationRequests[0];
    assert.ok(refusedRequest);
    assert.equal(
      refusedRequest.status,
      404,
      "a grant with no command capability must find no write surface at all",
    );
    assert.equal(
      committedTitle(REFUSED_TASK_ID),
      REFUSED_TASK_SEEDED_TITLE,
      "the refused mutation changed a PostgreSQL row",
    );

    // A 404 is definitive, since no command ran, so the delta is dropped, the
    // status is terminal, and the row the page reads is the row PostgreSQL
    // holds.
    assert.equal(refused.errorName, "MutationSubmissionError");
    assert.equal(refused.errorCode, "MUTATION_REJECTED");
    const refusedMutationId = refused.mutationId;
    assert.ok(
      refusedMutationId,
      "a refused submission still names its mutation",
    );
    const statusAfterRefusal =
      atSettle.mutationStatuses.find(([id]) => id === refusedMutationId)?.[1] ??
      "forgotten";
    assert.equal(statusAfterRefusal, "rejected");
    const optimisticDeltaStillVisible =
      titleOf(atSettle, REFUSED_TASK_ID) === REFUSED_TITLE;
    assert.equal(
      optimisticDeltaStillVisible,
      false,
      "the refused mutation left its optimistic delta in the overlay",
    );
    assert.equal(
      titleOf(atSettle, REFUSED_TASK_ID),
      REFUSED_TASK_SEEDED_TITLE,
      "the refused row does not read the value PostgreSQL holds",
    );
    assert.equal(
      atSettle.state,
      "ready",
      "a refused mutation must not take the client out of ready",
    );

    // A source-side change to the same row is delivered to this page through
    // CDC, and the page must show the source value: a delta that can never
    // commit would leave the browser diverged from PostgreSQL for the rest of
    // the session.
    psql(
      `UPDATE tasks SET title = '${SOURCE_TITLE}' WHERE id = ${String(REFUSED_TASK_ID)};`,
    );
    const publicationsBeforeSourceChange = atSettle.publications;
    await waitFor(
      "a publication carrying the source change to the refused row",
      ECHO_TIMEOUT_MS,
      async () => {
        const state = await reader.evaluate(() =>
          globalThis.__calqueServeMutationState(),
        );
        if (state.state === "error")
          throw new Error(
            `the reader page failed: ${state.failure ?? "unknown"}`,
          );
        return state.publications > publicationsBeforeSourceChange;
      },
    );
    const afterSourceChange = await reader.evaluate(() =>
      globalThis.__calqueServeMutationState(),
    );
    const titleAfterSourceChange = titleOf(afterSourceChange, REFUSED_TASK_ID);
    assert.equal(
      committedTitle(REFUSED_TASK_ID),
      SOURCE_TITLE,
      "the source change did not commit",
    );
    assert.equal(
      titleAfterSourceChange,
      SOURCE_TITLE,
      "a refused mutation's delta is masking the canonical value CDC delivered",
    );

    // Nothing retries on its own: the client dropped the refused mutation's
    // envelope, so a second POST here would be a resubmission nobody asked
    // for.
    await new Promise((done) => setTimeout(done, QUIET_WATCH_MS));
    const afterQuiet = await reader.evaluate(() =>
      globalThis.__calqueServeMutationState(),
    );
    assert.equal(
      afterQuiet.mutationRequests.length,
      1,
      `the client resubmitted the refused mutation on its own: ${JSON.stringify(afterQuiet.mutationRequests)}`,
    );
    assert.equal(
      committedTitle(REFUSED_TASK_ID),
      SOURCE_TITLE,
      "a row changed while nothing was supposed to be in flight",
    );
    // And the write the run did make is still the committed one, so the
    // refusal did not roll anything back either.
    assert.equal(committedTitle(WRITTEN_TASK_ID), WRITTEN_TITLE);

    assert.deepEqual(pageErrors, []);

    outcome = {
      result: "PASS",
      runId,
      postgresPort: Number(postgresPort),
      serveStartupMillis,
      mutationsLine,
      mutationCommandsLine,
      projects: readerAfter.projects.length,
      tasks: readerAfter.tasks.length,
      write: {
        mutationId: writtenMutationId,
        httpStatus: 200,
        acceptedState: written.state ?? "",
        optimisticTitle,
        committedTitle: committed,
        canonicalEchoTitle: echo.title ?? "",
        submitToEchoMs: echo.observedAtEpochMs - written.settledAtEpochMs,
        taskPublicationsAfterWrite: echoed.taskPublications.length,
      },
      readerEcho: {
        title: WRITTEN_TITLE,
        publicationsBefore: readerBefore.publications,
        publicationsAfter: readerAfter.publications,
        observedAfterMs: readerObservedAfterMs,
      },
      refusal: {
        facadeExposedUpdate: true,
        httpStatus: 404,
        requestsAtSettle: atSettle.mutationRequests.length,
        requestsAfterQuietMs: afterQuiet.mutationRequests.length,
        quietMs: QUIET_WATCH_MS,
        errorName: refused.errorName ?? "",
        errorCode: refused.errorCode ?? "",
        mutationStatusAfterRefusal: statusAfterRefusal,
        clientStateAfterRefusal: atSettle.state,
        optimisticDeltaStillVisible,
        committedTitleUnchanged: REFUSED_TASK_SEEDED_TITLE,
        titleAfterSourceChange: titleAfterSourceChange ?? "",
        sourceTitle: SOURCE_TITLE,
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
  return outcome;
}

let status = 0;
try {
  const outcome = await main();
  process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
} catch (error) {
  status = 1;
  process.stderr.write(
    `serve-mutation-e2e: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
  );
} finally {
  adminPsql(DROP_RUN_DATABASE_SQL, false);
  rmSync(workDirectory, { recursive: true, force: true });
  if (!keepContainer) {
    // Only the container this suite's own bootstrap created. Nothing else on
    // this host is touched.
    removePostgresContainer(container);
  }
}
process.exit(status);
