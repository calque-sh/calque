#!/usr/bin/env node
/**
 * Drives the demo's mutations end to end against a disposable
 * PostgreSQL 18 in Docker and a real `calque dev`: update submission-to-echo
 * timing; `insert` and `delete` accepted, echoed, committed and refused; a lost
 * `POST /v1/mutations` response recovered by `retryMutation` without writing
 * twice; and seeded randomized batches compared with a model and with
 * PostgreSQL. The packaged client runs in Node with its workers on
 * `node:worker_threads`, so the timings are directional and nothing is painted.
 *
 * Usage:
 *   CALQUE_POSTGRES_PORT=55453 node --import ./scripts/register.mjs \
 *     scripts/demo-mutation-e2e.ts
 *
 * `CALQUE_DEMO_MUTATIONS` sets the measured update count and
 * `CALQUE_DEMO_REPLAY_SEEDS` the randomized phase's seed count.
 */
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Worker as NodeWorker } from "node:worker_threads";

import {
  demoPsql,
  appendBounded,
  availablePort,
  delay,
  exitWithin,
  parseSampleCount,
  summarizeSamples,
} from "./demo-process.ts";
import {
  composePostgresMajor,
  DEVELOPMENT_SOURCE_ROLE,
  DEMO_BINDINGS,
  demoSeedSql,
  parsePrivateClientEnvironment,
  sanitizePostgresRoutingEnvironment,
} from "./demo.ts";
import { CARGO_TARGET_ARGUMENTS } from "./lib/cargo-target.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const bindingsModule = pathToFileURL(join(root, DEMO_BINDINGS.output)).href;
const clientModule = pathToFileURL(
  join(root, "client/packages/client/dist/client.js"),
).href;
const workerBootstrap = pathToFileURL(
  join(root, "scripts/columnar-worker-node-bootstrap.mjs"),
);
const output = join(root, "target/demo-mutation");
const measuredMutations = parseSampleCount(
  process.env.CALQUE_DEMO_MUTATIONS ?? "24",
  "CALQUE_DEMO_MUTATIONS",
);
/** One unmeasured mutation first, so JIT and cache warm-up are not a sample. */
const WARM_UP_MUTATIONS = 1;
const SEEDED_TASK_IDS = [
  1001n,
  1002n,
  1003n,
  1004n,
  1005n,
  1006n,
  1007n,
  1008n,
  1009n,
] as const;
/** A key no seeded row has: the server must refuse the patch as a conflict. */
const MISSING_TASK_ID = 424_242n;
/** The row the insert/delete phase creates and then removes. */
const INSERTED_TASK_ID = 2001n;
/**
 * Where the lost-response phase's keys start. Each attempt inserts a fresh
 * key, so a command that ran twice would collide with itself and be seen.
 */
const LOST_RESPONSE_KEY_BASE = 2100n;
/**
 * The retry races the CDC echo, so the phase loses several responses to reach
 * a retry that lands first without depending on winning that race.
 */
const LOST_RESPONSE_ATTEMPTS = 3;
/**
 * On a fast runner the echo can win every early attempt, so the phase keeps
 * dropping responses until one retry beats it, up to this many.
 */
const LOST_RESPONSE_MAX_ATTEMPTS = 12;
/** A project every generated row references, so no insert fails on the FK. */
const SEEDED_PROJECT_ID = 101n;
/** Seeds for the randomized replay phase, and the batch size per seed. */
const REPLAY_SEEDS = parseSampleCount(
  process.env.CALQUE_DEMO_REPLAY_SEEDS ?? "8",
  "CALQUE_DEMO_REPLAY_SEEDS",
);
const REPLAY_STEPS = 6;
/** Where the randomized phase's fresh keys start, above every other phase. */
const REPLAY_KEY_BASE = 30_000n;

interface Sample {
  readonly mutation: number;
  readonly warmUp: boolean;
  readonly taskId: string;
  readonly mutationId: string;
  readonly calledAtEpochMs: number;
  readonly submittedAtEpochMs: number;
  readonly canonicalAtEpochMs: number;
  readonly optimisticMs: number;
  readonly submitToEchoMs: number;
  readonly callToEchoMs: number;
  /** Whether the row read the optimistic value before its echo arrived. */
  readonly optimisticObserved: boolean;
}

interface MutationOutcome {
  readonly state: string;
  readonly mutationId: string;
}

interface TasksFacade {
  insert(row: Readonly<Record<string, unknown>>): Promise<MutationOutcome>;
  update(
    key: Readonly<Record<string, unknown>>,
    patch: Readonly<Record<string, unknown>>,
  ): Promise<MutationOutcome>;
  delete(key: Readonly<Record<string, unknown>>): Promise<MutationOutcome>;
  query(): {
    select(...columns: readonly string[]): {
      run(): Promise<readonly Readonly<Record<string, unknown>>[]>;
    };
  };
}

interface HarnessClient {
  readonly status: { readonly state: string };
  readonly db: { readonly tasks: TasksFacade };
  mutationStatus(mutationId: string): { readonly state: string } | undefined;
  subscribeMutation(mutationId: string, listener: () => void): () => void;
  /**
   * Resubmits a mutation whose response never arrived, under the identifier
   * it was already submitted with. The durable receipt makes the second
   * submission a replay rather than a second execution.
   */
  retryMutation(mutationId: string): Promise<MutationOutcome>;
  close(): Promise<void>;
}

/** One task row, as both the client and PostgreSQL are read for it. */
interface TaskRow {
  readonly projectId: bigint;
  readonly title: string;
  readonly done: boolean;
}

type TaskRows = ReadonlyMap<bigint, TaskRow>;

mkdirSync(output, { recursive: true });
assertChromiumArtifactProfile();

const postgresPort = process.env.CALQUE_POSTGRES_PORT ?? "55453";
const serverPort = await availablePort();
// Every compose call below runs under this run's own project. Without it
// compose names the project after the checkout's directory, as `pnpm demo`
// does for the developer's database, and this harness's `down --volumes`
// would delete that database.
const composeEnvironment: NodeJS.ProcessEnv = {
  ...sanitizePostgresRoutingEnvironment(process.env),
  COMPOSE_PROJECT_NAME: `calque-mutation-e2e-${process.pid}`,
  CALQUE_POSTGRES_PORT: postgresPort,
};
const psql = (sql: string): string => demoPsql(sql, composeEnvironment);
const databaseUrl = `postgresql://${DEVELOPMENT_SOURCE_ROLE}:${DEVELOPMENT_SOURCE_ROLE}@127.0.0.1:${postgresPort}/calque_dev`;
const endpoint = `http://127.0.0.1:${serverPort}`;

let serverOutput = "";
let submittedAt = 0;
/** Set immediately before the one mutation whose response is to be lost. */
let dropNextMutationResponse = false;
let droppedMutationResponses = 0;
let server: ChildProcess | undefined;
let client: HarnessClient | undefined;
const workers: NodeWorker[] = [];
// `calque dev`'s state lives outside the checkout, so a run leaves
// `.calque-dev` as it found it. The finally removes it.
const stateRoot = mkdtempSync(join(tmpdir(), "calque-mutation-e2e-"));
const stateDirectory = join(stateRoot, "state");
const clientEnvironmentPath = join(stateDirectory, "client.env");
try {
  // The packaged client and its engine worker are imported from `dist/`, so
  // they are built from this tree first.
  process.stdout.write("Building the client packages\n");
  runChecked("pnpm", ["--filter", "@calque/client...", "build"]);
  const postgresMajor = composePostgresMajor(
    readFileSync(join(root, "docker-compose.yml"), "utf8"),
  );
  process.stdout.write(
    `Starting disposable PostgreSQL ${postgresMajor} on ${postgresPort}\n`,
  );
  // A volume left by an earlier run would carry its control schema and its
  // enrolled epoch into this one, so the source starts empty every time.
  runChecked("docker", ["compose", "down", "--volumes"]);
  runChecked("docker", [
    "compose",
    "up",
    "-d",
    "--wait",
    "--wait-timeout",
    "90",
    "postgres",
  ]);
  runPsql(demoSeedSql);

  process.stdout.write("Starting calque dev\n");
  server = startServer();
  await waitForReadiness(`${endpoint}/readyz`, 300_000);
  const settings = parsePrivateClientEnvironment(
    readFileSync(clientEnvironmentPath, "utf8"),
  );
  assert.equal(settings.endpoint, endpoint);

  const { createInternalColumnarClient } = (await import(clientModule)) as {
    createInternalColumnarClient: (
      options: Record<string, unknown>,
      workerFactory: () => unknown,
      hasherFactory: () => unknown,
    ) => HarnessClient;
  };
  const { schema } = (await import(bindingsModule)) as {
    readonly schema: unknown;
  };
  client = createInternalColumnarClient(
    {
      endpoint: settings.endpoint,
      auth: () => ({
        bearerToken: settings.bearerToken,
      }),
      schema,
      memoryBudgetBytes: 1024 * 1024 * 1024,
      clientHeapReserveBytes: 64 * 1024 * 1024,
      deviceId: "demo-mutation-e2e",
      fetch: timedFetch,
    },
    createNodeColumnarWorker,
    createNodeColumnarHasher,
  );

  await waitForReady(client, 300_000);
  assert.equal((await readTitles(client)).size, SEEDED_TASK_IDS.length);

  const samples: Sample[] = [];
  const total = WARM_UP_MUTATIONS + measuredMutations;
  for (let index = 1; index <= total; index += 1) {
    const taskId = SEEDED_TASK_IDS[index % SEEDED_TASK_IDS.length] ?? 1001n;
    const title = `mutated ${index} at ${Date.now()}`;
    const calledAtEpochMs = Date.now();
    submittedAt = 0;
    const result: MutationOutcome = await client.db.tasks.update(
      { id: taskId },
      { title },
    );
    assert.equal(
      result.state,
      "awaiting_canonical_echo",
      `mutation ${index} was not accepted: ${JSON.stringify(result)}`,
    );
    const submittedAtEpochMs = submittedAt;
    assert(
      submittedAtEpochMs >= calledAtEpochMs,
      "no mutation POST was observed for the call",
    );
    // HTTP acceptance is pending; only the echo confirms.
    const accepted = client.mutationStatus(result.mutationId);
    assert(accepted, "the session forgot the mutation it just submitted");
    assert.notEqual(accepted.state, "canonical");
    const optimisticObserved = accepted.state === "awaiting_canonical_echo";
    if (optimisticObserved)
      assert.equal(
        (await readTitles(client)).get(taskId),
        title,
        "the optimistic value was not readable before the echo",
      );
    const canonicalAtEpochMs = await awaitCanonical(
      client,
      result.mutationId,
      30_000,
    );
    assert.equal(
      (await readTitles(client)).get(taskId),
      title,
      "the canonical read does not carry the echoed value",
    );
    samples.push({
      mutation: index,
      warmUp: index <= WARM_UP_MUTATIONS,
      taskId: taskId.toString(),
      mutationId: result.mutationId,
      calledAtEpochMs,
      submittedAtEpochMs,
      canonicalAtEpochMs,
      optimisticMs: submittedAtEpochMs - calledAtEpochMs,
      submitToEchoMs: canonicalAtEpochMs - submittedAtEpochMs,
      callToEchoMs: canonicalAtEpochMs - calledAtEpochMs,
      optimisticObserved,
    });
  }

  // The row is not there, so the command's UPDATE affects nothing and the
  // server answers Conflict. The optimistic delta must be gone and the other
  // rows unchanged.
  const beforeRejection = await readTitles(client);
  const refused = await client.db.tasks.update(
    { id: MISSING_TASK_ID },
    { title: "this row does not exist" },
  );
  assert.equal(refused.state, "conflict", JSON.stringify(refused));
  assert.equal(
    client.mutationStatus(refused.mutationId)?.state,
    "conflict",
    "a refused mutation must not stay pending",
  );
  assert.deepEqual(
    printable(await readTitles(client)),
    printable(beforeRejection),
    "a refused mutation left the mount changed",
  );

  // The facade refuses a patch outside the command's writable columns before
  // anything is applied, so the server never sees it.
  await assert.rejects(
    async () => client?.db.tasks.update({ id: 1001n }, { id: 7n }),
    /not writable/u,
  );

  const insertDelete = await proveInsertAndDelete(client);
  const lostResponse = await proveLostResponseAppliesOnce(client);
  const replay = await proveRandomizedReplay(client);

  const measured = samples.filter((sample) => !sample.warmUp);
  assert.equal(measured.length, measuredMutations);
  assert(
    measured.some((sample) => sample.optimisticObserved),
    "no mutation was observed optimistically before its echo",
  );

  assert.deepEqual(
    printableRows(await readRows(client)),
    printableRows(readCommittedRows()),
    "the client's rows are not the committed PostgreSQL rows",
  );

  const report = {
    outcome: "passed",
    scenario: "mutation-submission-to-canonical-echo-loopback",
    runtime: "node",
    postgresPort: Number(postgresPort),
    mutations: measuredMutations,
    warmUpMutations: WARM_UP_MUTATIONS,
    optimisticallyObserved: measured.filter((s) => s.optimisticObserved).length,
    submitToEchoMs: summarizeSamples(measured.map((s) => s.submitToEchoMs)),
    callToEchoMs: summarizeSamples(measured.map((s) => s.callToEchoMs)),
    optimisticMs: summarizeSamples(measured.map((s) => s.optimisticMs)),
    insertDelete,
    lostResponse,
    replay,
    samples,
  };
  writeFileSync(
    join(output, "report.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  const { samples: _samples, ...headline } = report;
  process.stdout.write(`${JSON.stringify(headline)}\n`);
} catch (error) {
  process.stderr.write(
    `Demo mutation E2E failed: ${JSON.stringify({
      error: error instanceof Error ? error.message : String(error),
      status: client?.status,
      server: serverOutput.slice(-8000),
    })}\n`,
  );
  throw error;
} finally {
  if (client) await client.close().catch(() => undefined);
  await Promise.allSettled(workers.map((worker) => worker.terminate()));
  if (server) await stopServer(server);
  rmSync(stateRoot, { recursive: true, force: true });
  runChecked("docker", ["compose", "down", "--volumes"]);
}

/**
 * The artifact transport profile the packaged client requires. Node has no
 * `navigator.userAgentData`, so the harness declares the profile it is
 * standing in for rather than letting the client refuse to open at all.
 */
function assertChromiumArtifactProfile(): void {
  assert.equal(
    typeof globalThis.ReadableStreamBYOBReader,
    "function",
    "this Node build has no BYOB reader; the artifact path needs one",
  );
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: Object.freeze({
      userAgentData: Object.freeze({
        brands: Object.freeze([Object.freeze({ brand: "Chromium" })]),
      }),
    }),
  });
}

/**
 * The client's `fetch`: it timestamps the mutation POST, loses a mutation
 * response on request, and checks the bootstrap plan's artifact identity on a
 * copy of the response.
 */
async function timedFetch(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const url = String(input instanceof Request ? input.url : input);
  if (url.endsWith("/v1/mutations")) submittedAt = Date.now();
  const response = await globalThis.fetch(input, init);
  // The answer is read to completion before it is thrown away, so the command
  // certainly committed and only the answer is lost. The error is what
  // `undici` raises for a connection reset, which `submitMutationBatch` turns
  // into MUTATION_AMBIGUOUS with the same mutation id.
  if (url.endsWith("/v1/mutations") && dropNextMutationResponse) {
    dropNextMutationResponse = false;
    await response.arrayBuffer();
    droppedMutationResponses += 1;
    throw new TypeError("fetch failed");
  }
  if (url.endsWith("/v1/bootstrap") && response.ok)
    assertPlanIdentity((await response.clone().json()) as PlanShape);
  return response;
}

interface PlanShape {
  readonly source: {
    readonly schemaGeneration: number;
    readonly authorizationGeneration: number;
  };
  readonly artifacts: readonly {
    readonly runtime: {
      readonly format?: string;
      readonly identity?: unknown;
    };
  }[];
}

function assertPlanIdentity(plan: PlanShape): void {
  let checked = 0;
  for (const artifact of plan.artifacts) {
    if (artifact.runtime.format !== "directColumnar") continue;
    checked += 1;
    assert.deepEqual(
      artifact.runtime.identity,
      {
        manifestVersion: String(plan.source.schemaGeneration),
        authorizationScopeVersion: String(plan.source.authorizationGeneration),
      },
      "the plan's artifact identity is not the one its generations derive",
    );
  }
  assert(checked > 0, "the bootstrap plan carries no direct-columnar artifact");
}

/** The engine worker, as a real thread. */
function createNodeColumnarWorker(): NodeColumnarWorker {
  return createNodeWorker("columnar-worker-entry.js");
}

/**
 * One hasher worker, as a real thread. The session spawns them for each load
 * and hands each one end of a `MessageChannel` whose other end goes to the
 * engine worker, so the chunks travel hasher to engine without the page.
 */
function createNodeColumnarHasher(): NodeColumnarWorker {
  return createNodeWorker("columnar-hasher-entry.js");
}

interface NodeColumnarWorker {
  addEventListener(type: string, handler: (event: never) => void): void;
  postMessage(message: unknown, transfer?: readonly unknown[]): void;
  terminate(): void;
}

/**
 * A packaged worker entry on a Node worker thread, shaped like the DOM
 * `Worker` the session drives. Each entry talks to a worker scope —
 * `globalThis.onmessage` and `globalThis.postMessage` — which the bootstrap
 * module bridges onto `parentPort`.
 */
function createNodeWorker(entry: string): NodeColumnarWorker {
  const worker = new NodeWorker(workerBootstrap, { workerData: { entry } });
  worker.unref();
  workers.push(worker);
  return {
    addEventListener(type, handler) {
      if (type === "message")
        worker.on("message", (data: unknown) =>
          (handler as (event: { data: unknown }) => void)({ data }),
        );
      else if (type === "error")
        worker.on("error", (error: Error) =>
          (handler as (event: { message: string }) => void)({
            message: error.message,
          }),
        );
      else if (type === "messageerror")
        worker.on("messageerror", () =>
          (handler as (event: object) => void)({}),
        );
    },
    postMessage(message, transfer) {
      worker.postMessage(message, transfer as readonly ArrayBuffer[]);
    },
    terminate() {
      void worker.terminate();
    },
  };
}

async function readTitles(
  current: HarnessClient,
): Promise<ReadonlyMap<bigint, unknown>> {
  const rows = await current.db.tasks.query().select("id", "title").run();
  return new Map(rows.map((row) => [row.id as bigint, row.title]));
}

function printable(rows: ReadonlyMap<bigint, unknown>): readonly string[] {
  return [...rows.entries()]
    .map(([id, title]) => `${id.toString()}=${String(title)}`)
    .sort();
}

/** Every task the client can read, whole rows rather than titles. */
async function readRows(current: HarnessClient): Promise<TaskRows> {
  const rows = await current.db.tasks
    .query()
    .select("id", "project_id", "title", "done")
    .run();
  return new Map(
    rows.map((row) => [
      row.id as bigint,
      {
        projectId: row.project_id as bigint,
        title: String(row.title),
        done: row.done === true,
      },
    ]),
  );
}

function printableRows(rows: TaskRows): readonly string[] {
  return [...rows.entries()]
    .map(
      ([id, row]) =>
        `${id.toString()}|${row.projectId.toString()}|${row.title}|${row.done}`,
    )
    .sort();
}

/** Every committed task, straight out of PostgreSQL, as the oracle. */
function readCommittedRows(): TaskRows {
  const rows = new Map<bigint, TaskRow>();
  const committed = psql(
    "SELECT id, project_id, title, done FROM tasks ORDER BY id",
  );
  for (const line of committed.split("\n")) {
    if (line.trim().length === 0) continue;
    const [id, projectId, title, done] = line.split("\u0001");
    assert(
      id !== undefined &&
        projectId !== undefined &&
        title !== undefined &&
        done !== undefined,
      `unreadable row: ${line}`,
    );
    rows.set(BigInt(id), {
      projectId: BigInt(projectId),
      title,
      done: done === "t",
    });
  }
  return rows;
}

/**
 * Drives `insert` and `delete` through the facade, the server and the CDC
 * echo: each accepted, echoed, committed, and refused.
 */
async function proveInsertAndDelete(current: HarnessClient): Promise<{
  readonly inserted: string;
  readonly refusedInsert: string;
  readonly deleted: string;
  readonly refusedDelete: string;
}> {
  const before = await readRows(current);
  assert(
    !before.has(INSERTED_TASK_ID),
    "the key the insert claims must not already exist",
  );

  const title = `inserted through the generated facade at ${Date.now()}`;
  const inserted = await current.db.tasks.insert({
    id: INSERTED_TASK_ID,
    project_id: SEEDED_PROJECT_ID,
    title,
    done: false,
  });
  assert.equal(
    inserted.state,
    "awaiting_canonical_echo",
    `the insert was not accepted: ${JSON.stringify(inserted)}`,
  );
  // HTTP acceptance is pending, and the optimistic row is an overlay until the
  // echo confirms it.
  assert.notEqual(
    current.mutationStatus(inserted.mutationId)?.state,
    "canonical",
    "the insert was canonical before its echo",
  );
  assert(
    (await readRows(current)).has(INSERTED_TASK_ID),
    "the optimistic insert was not readable",
  );
  await awaitCanonical(current, inserted.mutationId, 30_000);
  const echoed = (await readRows(current)).get(INSERTED_TASK_ID);
  assert.deepEqual(
    echoed,
    { projectId: SEEDED_PROJECT_ID, title, done: false },
    "the echoed insert does not carry the row the facade sent",
  );
  assert.equal(
    psql(`SELECT count(*) FROM tasks WHERE id = ${INSERTED_TASK_ID}`).trim(),
    "1",
    "the insert did not reach PostgreSQL exactly once",
  );

  // The refused insert: the key is taken, so PostgreSQL raises a unique
  // violation, which `classify_postgres_error` reports as a conflict. The
  // optimistic row must be gone and every other row untouched.
  const beforeDuplicate = printableRows(await readRows(current));
  const duplicate = await current.db.tasks.insert({
    id: INSERTED_TASK_ID,
    project_id: SEEDED_PROJECT_ID,
    title: "this key is already taken",
    done: true,
  });
  assert.equal(duplicate.state, "conflict", JSON.stringify(duplicate));
  assert.equal(
    current.mutationStatus(duplicate.mutationId)?.state,
    "conflict",
    "a refused insert must not stay pending",
  );
  assert.deepEqual(
    printableRows(await readRows(current)),
    beforeDuplicate,
    "a refused insert left the mount changed",
  );

  const deleted = await current.db.tasks.delete({ id: INSERTED_TASK_ID });
  assert.equal(
    deleted.state,
    "awaiting_canonical_echo",
    `the delete was not accepted: ${JSON.stringify(deleted)}`,
  );
  assert.notEqual(
    current.mutationStatus(deleted.mutationId)?.state,
    "canonical",
    "the delete was canonical before its echo",
  );
  assert(
    !(await readRows(current)).has(INSERTED_TASK_ID),
    "the optimistic delete did not remove the row",
  );
  await awaitCanonical(current, deleted.mutationId, 30_000);
  assert(
    !(await readRows(current)).has(INSERTED_TASK_ID),
    "the echoed delete left the row readable",
  );
  assert.equal(
    psql(`SELECT count(*) FROM tasks WHERE id = ${INSERTED_TASK_ID}`).trim(),
    "0",
    "the delete did not reach PostgreSQL",
  );

  // The refused delete: no row matches, so the command affects nothing and
  // the server answers `row-missing`.
  const beforeRefusedDelete = printableRows(await readRows(current));
  const refusedDelete = await current.db.tasks.delete({ id: MISSING_TASK_ID });
  assert.equal(refusedDelete.state, "conflict", JSON.stringify(refusedDelete));
  assert.equal(
    current.mutationStatus(refusedDelete.mutationId)?.state,
    "conflict",
    "a refused delete must not stay pending",
  );
  assert.deepEqual(
    printableRows(await readRows(current)),
    beforeRefusedDelete,
    "a refused delete left the mount changed",
  );

  return {
    inserted: inserted.state,
    refusedInsert: duplicate.state,
    deleted: deleted.state,
    refusedDelete: refusedDelete.state,
  };
}

/**
 * Loses a mutation response after its command committed and checks that
 * `retryMutation` replays the durable receipt instead of writing twice.
 *
 * The mutation is an insert because a second execution is observable: it
 * would hit the primary key and come back `conflict`, where an update would be
 * idempotent by value. The echo may land before the retry, which is also
 * correct, so the phase repeats until at least one retry is answered from the
 * receipt.
 */
async function proveLostResponseAppliesOnce(current: HarnessClient): Promise<{
  readonly attempts: number;
  readonly retriedBeforeEcho: number;
  readonly droppedResponses: number;
}> {
  let retriedBeforeEcho = 0;
  let attempts = 0;
  for (
    let attempt = 1;
    attempt <= LOST_RESPONSE_ATTEMPTS ||
    (retriedBeforeEcho === 0 && attempt <= LOST_RESPONSE_MAX_ATTEMPTS);
    attempt += 1
  ) {
    attempts = attempt;
    const id = LOST_RESPONSE_KEY_BASE + BigInt(attempt);
    const title = `survived a lost response ${attempt}`;
    assert.equal(
      psql(`SELECT count(*) FROM tasks WHERE id = ${id}`).trim(),
      "0",
      "the key the lost insert claims must not already exist",
    );
    let mutationId: string | undefined;
    dropNextMutationResponse = true;
    await assert.rejects(
      async () => {
        await current.db.tasks.insert({
          id,
          project_id: SEEDED_PROJECT_ID,
          title,
          done: false,
        });
      },
      (error: unknown) => {
        const failure = error as {
          readonly code?: string;
          readonly mutationId?: string;
        };
        assert.equal(failure.code, "MUTATION_AMBIGUOUS", describe(error));
        mutationId = failure.mutationId;
        return true;
      },
    );
    assert.equal(
      dropNextMutationResponse,
      false,
      "no mutation POST was intercepted, so no response was lost",
    );
    assert(mutationId, "the ambiguous failure did not name its mutation id");
    assert.match(
      mutationId,
      /^[A-Za-z0-9._-]{1,128}$/u,
      "the mutation id is not a safe SQL literal",
    );
    assert.equal(
      current.mutationStatus(mutationId)?.state,
      "ambiguous",
      "a lost response must leave the mutation ambiguous, not resolved",
    );
    // The delta survives the ambiguity: dropping it would lose a write that
    // may already be canonical.
    assert(
      (await readRows(current)).has(id),
      "the ambiguous insert dropped its optimistic row",
    );

    // The retry, before anything slow enough to lose the race with the echo.
    let retried: MutationOutcome | undefined;
    try {
      retried = await current.retryMutation(mutationId);
    } catch (error) {
      // The echo confirmed the mutation first, so there is nothing left to
      // resubmit. That is the other correct outcome.
      assert.equal(
        current.mutationStatus(mutationId)?.state,
        "canonical",
        `the retry failed while the mutation was not canonical: ${describe(error)}`,
      );
    }
    if (retried?.state === "already_canonical") {
      // The echo won: the page applied it before the retry could resend, or
      // the server answered the resend from the canonical log. Neither is a
      // replayed receipt. The session keeps the delta until the echo replaces
      // it, so the row stays in the mount either way.
      assert.equal(retried.mutationId, mutationId);
      assert(
        (await readRows(current)).has(id),
        "an already_canonical answer took the row out of the mount before its echo",
      );
      await awaitCanonical(current, mutationId, 30_000);
    } else if (retried) {
      retriedBeforeEcho += 1;
      assert.equal(
        retried.state,
        "awaiting_canonical_echo",
        `the replayed receipt did not re-accept the mutation: ${JSON.stringify(retried)}`,
      );
      assert.equal(retried.mutationId, mutationId);
      // `already_accepted_outcome`'s message. A re-executed insert would have
      // hit the primary key and come back a conflict instead.
      assert.equal(
        (retried as { readonly message?: string }).message,
        "mutation was already committed and is awaiting canonical echo",
        "the retry was not answered from the durable receipt",
      );
      await awaitCanonical(current, mutationId, 30_000);
    }

    assert.equal(
      current.mutationStatus(mutationId)?.state,
      "canonical",
      "the recovered mutation never reached its echo",
    );
    assert.deepEqual(
      (await readRows(current)).get(id),
      { projectId: SEEDED_PROJECT_ID, title, done: false },
      "the mount does not carry the write whose response was lost",
    );
    assert.equal(
      psql(
        `SELECT count(*) FROM tasks WHERE id = ${id} AND title = '${title}'`,
      ).trim(),
      "1",
      "the write whose response was lost is not in PostgreSQL exactly once",
    );
    assert.equal(
      psql(
        `SELECT count(*) FROM calque_internal.mutation_receipts WHERE mutation_id = '${mutationId}'`,
      ).trim(),
      "1",
      "the retry wrote a second receipt",
    );
  }
  assert(
    retriedBeforeEcho > 0,
    `no attempt reached the retry before its echo in ${String(attempts)} tries`,
  );
  assert.equal(droppedMutationResponses, attempts);
  return {
    attempts,
    retriedBeforeEcho,
    droppedResponses: droppedMutationResponses,
  };
}

/**
 * Drives seeded batches of inserts, updates and deletes through the real
 * mutation path and CDC echo, and compares the mount with an independent model
 * and with PostgreSQL.
 *
 * Each step of a batch uses its own key, so the model holds whatever order the
 * server commits them in. A batch is submitted whole before any echo is
 * awaited, so a refusal inside it forces a replay of the pending survivors.
 */
async function proveRandomizedReplay(current: HarnessClient): Promise<{
  readonly seeds: number;
  readonly steps: number;
  readonly accepted: number;
  readonly refused: number;
  readonly coverage: Readonly<Record<string, number>>;
}> {
  const model = new Map(await readRows(current));
  assert.deepEqual(
    printableRows(model),
    printableRows(readCommittedRows()),
    "the replay phase started from a mount that already disagreed with PostgreSQL",
  );
  let accepted = 0;
  let refused = 0;
  // What the seeds emitted: a generator that stopped producing a shape would
  // leave every seed green while the phase stopped testing it.
  const coverage: Record<string, number> = {
    insert: 0,
    update: 0,
    delete: 0,
    "insert refused": 0,
    "update refused": 0,
    "delete refused": 0,
  };
  for (let seed = 0; seed < REPLAY_SEEDS; seed += 1) {
    const random = seededRandom(0x5eed_0000 + seed);
    const planned: ReplayStep[] = [];
    const claimed = new Set<bigint>();
    for (let step = 0; step < REPLAY_STEPS; step += 1) {
      planned.push(planReplayStep(random, model, claimed, seed, step));
    }
    const outcomes = await Promise.all(
      planned.map(async (step) => applyReplayStep(current, step)),
    );
    for (const [index, step] of planned.entries()) {
      const outcome = outcomes[index];
      assert(outcome, "a planned step produced no outcome");
      coverage[step.refused ? `${step.kind} refused` : step.kind] =
        (coverage[step.refused ? `${step.kind} refused` : step.kind] ?? 0) + 1;
      if (step.refused) {
        refused += 1;
        assert.equal(
          outcome.state,
          "conflict",
          `seed ${seed} step ${index} (${step.kind}, refused) was not refused: ${JSON.stringify(outcome)}`,
        );
        continue;
      }
      accepted += 1;
      assert.equal(
        outcome.state,
        "awaiting_canonical_echo",
        `seed ${seed} step ${index} (${step.kind}) was not accepted: ${JSON.stringify(outcome)}`,
      );
      await awaitCanonical(current, outcome.mutationId, 30_000);
      applyReplayStepToModel(model, step);
    }
    // The rejected members of the batch must have left nothing behind, and
    // the survivors must have replayed onto canonical state unchanged.
    assert.deepEqual(
      printableRows(await readRows(current)),
      printableRows(model),
      `seed ${seed}: the mount is not the model after the batch settled`,
    );
    assert.deepEqual(
      printableRows(readCommittedRows()),
      printableRows(model),
      `seed ${seed}: PostgreSQL is not the model after the batch settled`,
    );
  }
  for (const [shape, count] of Object.entries(coverage))
    assert(
      count > 0,
      `the seeds never generated a ${shape} step, so the phase did not test it: ${JSON.stringify(coverage)}`,
    );
  return {
    seeds: REPLAY_SEEDS,
    steps: REPLAY_STEPS,
    accepted,
    refused,
    coverage,
  };
}

interface ReplayStep {
  readonly kind: "insert" | "update" | "delete";
  readonly id: bigint;
  readonly projectId: bigint;
  readonly title: string;
  readonly done: boolean;
  /** Whether the server must refuse this step. */
  readonly refused: boolean;
}

/**
 * One step whose outcome the model already knows: a fresh key for an insert,
 * a resident key for an update or a delete, and — one step in four — a key
 * chosen so the server must refuse.
 */
function planReplayStep(
  random: () => number,
  model: TaskRows,
  claimed: Set<bigint>,
  seed: number,
  step: number,
): ReplayStep {
  const resident = [...model.keys()].filter((id) => !claimed.has(id)).sort();
  const draw = random();
  const kind =
    draw < 0.4 || resident.length === 0
      ? "insert"
      : draw < 0.75
        ? "update"
        : "delete";
  const refused = random() < 0.25;
  const fresh = REPLAY_KEY_BASE + BigInt(seed) * 100n + BigInt(step);
  const id =
    kind === "insert"
      ? refused
        ? // A key that is already resident: the insert must hit the primary
          // key and come back a conflict.
          (resident[Math.floor(random() * resident.length)] ?? fresh)
        : fresh
      : refused
        ? // A key nothing holds: the update or delete affects no row.
          MISSING_TASK_ID + BigInt(seed) * 100n + BigInt(step)
        : (resident[Math.floor(random() * resident.length)] ?? fresh);
  claimed.add(id);
  return {
    kind,
    id,
    projectId: SEEDED_PROJECT_ID,
    title: `replay s${seed} n${step} ${kind}${refused ? " refused" : ""}`,
    done: random() < 0.5,
    // A fresh-key insert has nothing to collide with, so it cannot be refused.
    refused: refused && !(kind === "insert" && id === fresh),
  };
}

async function applyReplayStep(
  current: HarnessClient,
  step: ReplayStep,
): Promise<MutationOutcome> {
  if (step.kind === "insert")
    return current.db.tasks.insert({
      id: step.id,
      project_id: step.projectId,
      title: step.title,
      done: step.done,
    });
  if (step.kind === "update")
    return current.db.tasks.update(
      { id: step.id },
      { title: step.title, done: step.done },
    );
  return current.db.tasks.delete({ id: step.id });
}

function applyReplayStepToModel(
  model: Map<bigint, TaskRow>,
  step: ReplayStep,
): void {
  if (step.kind === "delete") {
    model.delete(step.id);
    return;
  }
  const existing = model.get(step.id);
  model.set(step.id, {
    projectId:
      step.kind === "insert"
        ? step.projectId
        : (existing?.projectId ?? step.projectId),
    title: step.title,
    done: step.done,
  });
}

/** A small deterministic generator, so a failing seed replays exactly. */
function seededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x9e37_79b9) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 16), 0x21f0_aaad) >>> 0;
    mixed = Math.imul(mixed ^ (mixed >>> 15), 0x735a_2d97) >>> 0;
    mixed = (mixed ^ (mixed >>> 15)) >>> 0;
    return mixed / 0x1_0000_0000;
  };
}

/** Resolves when the session has moved the mutation to `canonical`. */
async function awaitCanonical(
  current: HarnessClient,
  mutationId: string,
  timeoutMs: number,
): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  return new Promise<number>((resolveEcho, rejectEcho) => {
    const check = (): boolean => {
      const state = current.mutationStatus(mutationId)?.state;
      if (state === "canonical") {
        resolveEcho(Date.now());
        return true;
      }
      if (state !== "awaiting_canonical_echo" && state !== "submitting") {
        rejectEcho(new Error(`mutation ${mutationId} ended as ${state}`));
        return true;
      }
      return false;
    };
    const unsubscribe = current.subscribeMutation(mutationId, () => {
      if (check()) unsubscribe();
    });
    // Subscribing cannot be atomic with the status the session already holds,
    // so the current one is read once after: an echo that landed first is not
    // waited for forever.
    if (check()) {
      unsubscribe();
      return;
    }
    const timer = setInterval(() => {
      if (check()) {
        unsubscribe();
        clearInterval(timer);
      } else if (Date.now() > deadline) {
        unsubscribe();
        clearInterval(timer);
        rejectEcho(new Error(`mutation ${mutationId} was never echoed`));
      }
    }, 5);
    timer.unref();
  });
}

/** A `SyncError`'s message is not enumerable; a failure report needs it. */
function describe(value: unknown): string {
  return JSON.stringify(value, (_key, member: unknown) =>
    member instanceof Error
      ? { ...member, name: member.name, message: member.message }
      : member,
  );
}

async function waitForReady(
  current: HarnessClient,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const status = current.status;
    if (status.state === "ready") return;
    if (status.state === "error")
      throw new Error(`the client failed: ${describe(status)}`);
    await delay(25);
  }
  throw new Error("the client never became ready");
}

function startServer(): ChildProcess {
  const child = spawn(
    "cargo",
    [
      "run",
      ...CARGO_TARGET_ARGUMENTS,
      "-p",
      "calque",
      "--",
      "dev",
      "--state-dir",
      stateDirectory,
      "--table",
      "public.projects",
      "--table",
      "public.tasks",
      "--allow-origin",
      "http://127.0.0.1:5173",
      "--port",
      String(serverPort),
      "--acknowledge-disposable-source",
    ],
    {
      cwd: root,
      env: {
        ...sanitizePostgresRoutingEnvironment(process.env),
        CALQUE_DATABASE_URL: databaseUrl,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  const collect = (chunk: string) => {
    serverOutput = appendBounded(serverOutput, chunk);
  };
  child.stdout.on("data", collect);
  child.stderr.on("data", collect);
  return child;
}

async function stopServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolveExit) =>
    child.once("exit", () => resolveExit()),
  );
  child.kill("SIGINT");
  const stopped = await exitWithin(
    exited.then(() => true),
    20_000,
  );
  if (stopped === undefined) {
    child.kill("SIGKILL");
    await exited;
  }
}

async function waitForReadiness(url: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (server?.exitCode !== null && server?.exitCode !== undefined)
      throw new Error(`calque dev exited early:\n${serverOutput}`);
    try {
      const response = await globalThis.fetch(url);
      if (response.ok) return;
    } catch {
      // The listener is not up yet.
    }
    await delay(250);
  }
  throw new Error(`calque dev was not ready in time:\n${serverOutput}`);
}

/**
 * Runs `sql`, read from standard input, against the source database. The
 * connection is the container's own socket through `docker compose exec`, so
 * no connection string is ever an argument here.
 */
function runPsql(sql: string): void {
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
    { input: sql },
  );
}

function runChecked(
  command: string,
  arguments_: readonly string[],
  options: { input?: string } = {},
): { readonly stdout: string } {
  const result = spawnSync(command, arguments_, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
    env: composeEnvironment,
    ...options,
  });
  if (result.error || result.status !== 0) {
    const detail = [result.stderr, result.stdout]
      .filter((value) => typeof value === "string" && value.trim().length > 0)
      .join("\n");
    throw new Error(
      `${command} ${arguments_.join(" ")} failed: ${result.error?.message ?? detail}`,
    );
  }
  return { stdout: result.stdout ?? "" };
}
