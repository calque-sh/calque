#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Drives revocation and grant expiry on a running `calque serve` from headless
 * Chromium. This script plays the application backend: it signs a fence order
 * with the key `calque-dev-grant` minted the grants with, signs every barrier
 * draft Calque answers with, and posts both.
 *
 * Four pages share one `calque serve` and one database of the run's own.
 * **rebuild** (alice) is revoked and handed a narrower grant, and must rebuild
 * and be refused a write. **lock** (bob) is revoked and signed out, and must
 * lock and paint nothing further. **renew** (carol) is handed a fresh grant at
 * each renewal and must cross three grant expiries without leaving `ready`,
 * downloading again or missing a change, and still write. **expire** (dave)
 * cannot renew, and must lock and discard its rows. Each revoked page holds one write in
 * flight across the barrier, which must end ambiguous and then be refused
 * under the retired grant. Rows committed after the fence are past the barrier
 * cursor, so a paint that lacks them came from a revoked mount.
 *
 * Usage: `pnpm serve:e2e` (after `scripts/serve-mutation-e2e.ts`) or
 * `pnpm serve:e2e:revocation`. Requires Docker and a Rust toolchain.
 * `CALQUE_SERVE_E2E_POSTGRES_PORT` and `CALQUE_SERVE_E2E_KEEP_CONTAINER` mean
 * what they mean to `serve-mutation-e2e`, and the two share one container.
 */
import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { randomUUID, sign as signEd25519, type KeyObject } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { cpus, loadavg } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { loadSigningKey } from "@calque/grant";
import type { Browser, Page } from "playwright";
import { importBuilt } from "./demo-process.ts";
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
} from "./lib/serve-e2e.ts";

/** What the page exposes; see `tests/serve-revocation/main.ts`. */
interface StatusRecord {
  readonly atEpochMs: number;
  readonly state: string;
  readonly reason: string | null;
  readonly code: string | null;
  readonly message: string | null;
}
interface PaintRecord {
  readonly atEpochMs: number;
  readonly sessionState: string;
  readonly mountedGrant: string | null;
  readonly inserted: readonly number[];
  readonly insertedTitles: readonly string[];
  readonly removed: number;
  readonly rows: readonly number[];
}
interface MutationTransition {
  readonly atEpochMs: number;
  readonly mutationId: string;
  readonly state: string;
  readonly code: string | null;
  readonly message: string | null;
}
interface PageState {
  readonly role: string;
  readonly status: StatusRecord;
  readonly statuses: readonly StatusRecord[];
  readonly lockBanner: { readonly visible: boolean; readonly text: string };
  readonly rows: readonly (readonly [number, string])[];
  readonly paints: readonly PaintRecord[];
  readonly auths: readonly {
    readonly atEpochMs: number;
    readonly grant: string | null;
  }[];
  readonly bootstraps: readonly {
    readonly atEpochMs: number;
    readonly grant: string | null;
    readonly status: number | null;
    readonly renewal: boolean;
  }[];
  readonly artifactDownloads: number;
  readonly mutationRequests: readonly {
    readonly mutationIds: readonly string[];
    readonly grant: string | null;
    readonly calledAtEpochMs: number;
    readonly sentAtEpochMs: number;
    readonly status: number | null;
    readonly failure: string | null;
  }[];
  readonly mutationTransitions: readonly MutationTransition[];
  readonly attempts: readonly {
    readonly id: number;
    readonly title: string;
    readonly settled: string;
    readonly mutationId: string | null;
    readonly errorCode: string | null;
    readonly errorMessage: string | null;
  }[];
  readonly heldMutations: number;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations to `typeof globalThis`.
  var __calqueRevocationState: () => PageState;
  var __calqueRevocationSetGrant: (
    token: string | null,
    label: string | null,
  ) => void;
  var __calqueRevocationQueueGrants: (
    grants: readonly (readonly [token: string, label: string])[],
  ) => void;
  var __calqueRevocationHoldMutations: () => void;
  var __calqueRevocationReleaseMutations: () => void;
  var __calqueRevocationStartUpdate: (id: number, title: string) => number;
}

/** The slice of the built `@calque/protocol` this script signs with. */
interface WireSchema {
  readonly typeName: string;
}
type WireMessage = Record<string, unknown>;
interface ProtocolModule {
  readonly change: {
    readonly CursorSchema: WireSchema;
    readonly RevocationBarrierSchema: WireSchema;
  };
  revocationBarrierSigningPreimage(barrier: WireMessage): Uint8Array;
}
interface ProtobufRuntime {
  create(schema: WireSchema, init?: WireMessage): WireMessage;
}

/** One unsigned barrier as `POST /v1/authorization/fence` answers it. */
interface BarrierDraft {
  readonly barrierId: string;
  readonly barrierCursor: string;
  readonly revokedAuthorizationGeneration: number;
  readonly effectiveAuthorizationGeneration: number;
  readonly revokedAuthorizationScopeHash: string;
  readonly issuedAtUnixMicros: number;
}

const runId = randomUUID().replaceAll("-", "").slice(0, 12);
const workDirectory = join(root, "target/serve-revocation-e2e", runId);
const pageRoot = join(root, "target/client-serve-revocation");
const postgresPort = process.env.CALQUE_SERVE_E2E_POSTGRES_PORT ?? "55491";
const container = "calque-servee2e-pg18";
const keepContainer = process.env.CALQUE_SERVE_E2E_KEEP_CONTAINER === "1";
/** One database per run, for the reason `serve-mutation-e2e` gives. */
const runDatabase = `calque_serve_revocation_${runId}`;
const runSlot = `${runDatabase}_slot`;
const runPublication = `${runDatabase}_pub`;
const DATABASE_ENV = "CALQUE_SERVE_REVOCATION_E2E_DATABASE_URL";
const GRANT_KEY_ENV = "CALQUE_SERVE_REVOCATION_E2E_GRANT_KEY";
const GRANT_AUDIENCE = "serve-revocation.e2e";
const KEY_ID = "serve-revocation-e2e";
const READY_TIMEOUT_MS = 300_000;
const MOUNT_TIMEOUT_MS = 180_000;
const DELIVERY_TIMEOUT_MS = 60_000;
/** Covers the new generation's artifact build and the client's retry. */
const REBUILD_TIMEOUT_MS = 150_000;
/**
 * The least time the revoked pages are watched between the fence and the
 * barrier signature, when a revoked plan is parked at the barrier cursor. The
 * watch is ten times the slowest commit-to-paint time the same pages measured
 * just before, when that is longer.
 */
const PARKED_WATCH_FLOOR_MS = 5_000;
/** How long a locked page is watched for a retry nobody asked for. */
const QUIET_WATCH_MS = 5_000;
/**
 * The short grants' lifetime. Long enough for a page to bootstrap and mount
 * before it runs out, which the artifact capability needs: it is bounded by
 * the grant's expiry less one second.
 */
const SHORT_GRANT_SECONDS = 25;
/**
 * How far ahead of its grant a plan ends. The bootstrap bounds the plan by the
 * grant's expiry less one second, on a whole-second clock, so the change
 * stream refuses a plan between one and two seconds before the grant itself
 * expires.
 */
const PLAN_EXPIRY_LEAD_MS = 2_000;

const SEEDED_TITLES = new Map<number, string>([
  [1001, "Freeze the release candidate"],
  [1002, "Validate the data migration"],
  [1003, "Review launch telemetry"],
  [1004, "Rehearse regional failover"],
  [1005, "Tune alert thresholds"],
  [1006, "Document the recovery path"],
  [1007, "Approve the offline experience"],
  [1008, "Run the beta cohort"],
  [1009, "Finish the accessibility pass"],
]);
const SEEDED_TASKS = SEEDED_TITLES.size;
const BEFORE_FENCE_ID = 1003;
const BEFORE_FENCE_TITLE = `changed before the fence ${runId}`;
const AFTER_FENCE_ROW = 1010;
const AFTER_FENCE_ROW_TITLE = `committed after the fence ${runId}`;
const AFTER_FENCE_RENAMED_ID = 1005;
const AFTER_FENCE_RENAMED_TITLE = `renamed after the fence ${runId}`;
const REBUILD_WRITE_ID = 1001;
const LOCK_WRITE_ID = 1002;
const EXPIRE_WRITE_ID = 1004;
const NARROWED_WRITE_ID = 1007;
const RENEWED_LIVE_ID = 1006;
const RENEWED_LIVE_TITLE = `committed after three renewals ${runId}`;
const RENEWED_WRITE_ID = 1008;
const RENEWED_WRITE_TITLE = `written after three renewals ${runId}`;
const REVOKED_MESSAGE =
  "an authenticated revocation barrier retired this mutation's grant before the canonical echo; the mutation may have committed, so read the rebuilt snapshot before reissuing it";
const REBUILD_MESSAGE =
  "the session rebuilt from a fresh snapshot before the canonical echo";

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
${[...SEEDED_TITLES]
  .map(
    ([id, title]) =>
      `  (${String(id)}, ${String(101 + Math.floor((id - 1001) / 3))}, '${title}', false)`,
  )
  .join(",\n")};
ALTER TABLE projects REPLICA IDENTITY FULL;
ALTER TABLE tasks REPLICA IDENTITY FULL;
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

function committedTitle(id: number): string {
  return psql(`SELECT title FROM tasks WHERE id = ${String(id)};`).trim();
}

function fencedGeneration(): string {
  return psql(
    "SELECT generation FROM calque_control.authorization_fence;",
  ).trim();
}

// The client packages, `@calque/protocol` among them, built before this script
// imports the protocol and the page bundles the client, so neither reads a
// stale `dist/`.
run("pnpm", ["--filter", "@calque/client...", "build"]);
const protocolRoot = join(root, "client/packages/protocol");
/**
 * The barrier preimage, taken from the built `@calque/protocol` the client
 * verifies with, so this script signs the same bytes Calque and the browser
 * check.
 */
const protocol = (await importBuilt(
  join(protocolRoot, "dist/index.js"),
  "@calque/protocol",
)) as ProtocolModule;
const { create } = (await importBuilt(
  createRequire(join(protocolRoot, "package.json")).resolve(
    "@bufbuild/protobuf",
  ),
  "@bufbuild/protobuf",
)) as ProtobufRuntime;

interface MintedGrant {
  readonly token: string;
  readonly label: string;
  readonly expiresAtUnixSeconds: number;
  readonly commands: readonly string[];
  readonly verificationKeyEnvironment: { keyId: string; publicKey: string };
}

/**
 * The application's side of the fence route: a fence order is Ed25519 over
 * the domain `calque.authorization-fence.v1\0` and the JSON payload, sent as
 * `base64url(payload).base64url(signature)`.
 */
function signFenceOrder(
  key: KeyObject,
  revokedGeneration: number,
  effectiveGeneration: number,
): string {
  const issuedAtUnixSeconds = Math.floor(Date.now() / 1000);
  const payload = Buffer.from(
    JSON.stringify({
      version: 1,
      keyId: KEY_ID,
      audience: GRANT_AUDIENCE,
      revokedGeneration,
      effectiveGeneration,
      issuedAtUnixSeconds,
      expiresAtUnixSeconds: issuedAtUnixSeconds + 120,
    }),
  );
  const signature = signEd25519(
    null,
    Buffer.concat([Buffer.from("calque.authorization-fence.v1\0"), payload]),
    key,
  );
  return `${payload.toString("base64url")}.${signature.toString("base64url")}`;
}

/** The application's signature over one barrier Calque drafted. */
function signBarrier(key: KeyObject, draft: BarrierDraft): WireMessage {
  const barrier = create(protocol.change.RevocationBarrierSchema, {
    barrierCursor: create(protocol.change.CursorSchema, {
      token: new TextEncoder().encode(draft.barrierCursor),
    }),
    revokedAuthorizationGeneration: BigInt(
      draft.revokedAuthorizationGeneration,
    ),
    effectiveAuthorizationGeneration: BigInt(
      draft.effectiveAuthorizationGeneration,
    ),
    revokedAuthorizationScopeHash: new Uint8Array(
      Buffer.from(draft.revokedAuthorizationScopeHash, "hex"),
    ),
    barrierId: draft.barrierId,
    issuedAtUnixMicros: BigInt(draft.issuedAtUnixMicros),
    signingKeyId: KEY_ID,
  });
  const signature = signEd25519(
    null,
    Buffer.from(protocol.revocationBarrierSigningPreimage(barrier)),
    key,
  );
  return {
    barrierId: draft.barrierId,
    signingKeyId: KEY_ID,
    signature: signature.toString("base64url"),
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

async function stateOf(page: Page): Promise<PageState> {
  return await page.evaluate(() => globalThis.__calqueRevocationState());
}

async function openPage(
  browser: Browser,
  pageOrigin: string,
  endpoint: string,
  grant: MintedGrant,
  grantVerificationKey: MintedGrant["verificationKeyEnvironment"],
  role: string,
  pageErrors: string[],
): Promise<Page> {
  const page = await browser.newPage({
    viewport: { width: 1280, height: 900 },
  });
  page.on("pageerror", (error) => pageErrors.push(`${role}: ${String(error)}`));
  await page.addInitScript(
    ([config]) => {
      Object.defineProperty(globalThis, "__calqueRevocationConfig", {
        value: config,
      });
    },
    [
      {
        endpoint,
        deviceId: `serve-revocation-${role}-${randomUUID()}`,
        role,
        grantVerificationKey,
        grant: { token: grant.token, label: grant.label },
      },
    ] as const,
  );
  await page.goto(pageOrigin, { waitUntil: "load", timeout: 60_000 });
  await waitFor(
    `the ${role} page to paint the complete working set`,
    MOUNT_TIMEOUT_MS,
    async () => {
      const state = await stateOf(page);
      if (state.status.state === "error" || state.status.state === "locked")
        throw new Error(
          `the ${role} page ${state.status.state}: ${state.status.message ?? ""}`,
        );
      return (
        state.status.state === "ready" && state.rows.length >= SEEDED_TASKS
      );
    },
  );
  return page;
}

function titleIn(state: PageState, id: number): string | undefined {
  return state.rows.find(([rowId]) => rowId === id)?.[1];
}

/** The first status the page left `ready` for at or after `sinceEpochMs`. */
function leftReady(state: PageState, sinceEpochMs: number): StatusRecord {
  const record = state.statuses.find(
    (status) => status.atEpochMs >= sinceEpochMs && status.state !== "ready",
  );
  assert.ok(
    record,
    `the ${state.role} page never left ready: ${JSON.stringify(state.statuses)}`,
  );
  return record;
}

/**
 * The first write a page started, and every status it passed through. The page
 * read the id out of the POST body, so it is known while the write is still in
 * flight.
 */
function firstWriteOf(state: PageState): MutationTransition[] {
  const mutationId = state.mutationTransitions[0]?.mutationId;
  assert.ok(mutationId, `the ${state.role} page recorded no write`);
  return state.mutationTransitions.filter(
    (transition) => transition.mutationId === mutationId,
  );
}

/**
 * Every paint at or after `sinceEpochMs` that inserted rows, with the rows it
 * left on screen.
 */
function insertingPaintsSince(
  state: PageState,
  sinceEpochMs: number,
): PaintRecord[] {
  return state.paints.filter(
    (paint) => paint.atEpochMs >= sinceEpochMs && paint.inserted.length > 0,
  );
}

async function main(): Promise<Record<string, unknown>> {
  mkdirSync(workDirectory, { recursive: true });
  const binaries = buildServeBinaries();
  run("pnpm", ["--filter", "@calque/client", "serve-revocation:build"]);
  ensurePostgresContainer(container, postgresPort);
  const stateDirectory = join(workDirectory, "state");
  const servePort = await reservedPort();
  const pagePort = await reservedPort();
  const pageOrigin = `http://127.0.0.1:${String(pagePort)}`;
  const serveOrigin = `http://127.0.0.1:${String(servePort)}`;
  const endpoint = `${serveOrigin}/`;
  const databaseUrl = `postgresql://calque_test:calque_test@127.0.0.1:${postgresPort}/${runDatabase}`;
  const serveLog: string[] = [];

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
  assert.equal(committedTitle(REBUILD_WRITE_ID), SEEDED_TITLES.get(1001));

  // Every grant comes from one key file, whose key also signs the fence order
  // and the barriers, each under its own signing domain.
  const keyFile = join(workDirectory, "grant-key.pk8");
  const mint = (
    label: string,
    subject: string,
    generation: number,
    lifetimeSeconds: number,
    commands: readonly string[],
  ): MintedGrant => {
    const minted = JSON.parse(
      run(join(binaries, "calque-dev-grant"), [
        "--key-file",
        keyFile,
        "--key-id",
        KEY_ID,
        "--audience",
        GRANT_AUDIENCE,
        "--subject",
        subject,
        "--generation",
        String(generation),
        "--lifetime-seconds",
        String(lifetimeSeconds),
        ...commands.flatMap((command) => ["--command", command]),
      ]),
    ) as Omit<MintedGrant, "label">;
    return { ...minted, label };
  };
  const aliceG1 = mint("alice-g1", "alice", 1, 900, ["tasks=update"]);
  const bobG1 = mint("bob-g1", "bob", 1, 900, ["tasks=update"]);
  const applicationKey = loadSigningKey(readFileSync(keyFile));
  // `calque serve` below and each page's client get the same key object: the
  // barrier key and the grant key are the same key.
  const barrierKey = aliceG1.verificationKeyEnvironment;

  const serving = await startServe(
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
    ],
    {
      [DATABASE_ENV]: databaseUrl,
      [GRANT_KEY_ENV]: JSON.stringify(aliceG1.verificationKeyEnvironment),
    },
    serveLog,
    READY_TIMEOUT_MS,
  );
  let browser: Browser | undefined;
  const pageServer = servePage(pagePort, pageRoot);
  try {
    assert.equal(serving.contract.get("ready"), "true");
    assert.equal(serving.contract.get("authorization_generation"), "1");

    const requireFromClient = createRequire(
      join(root, "client/packages/client/package.json"),
    );
    const { chromium } = requireFromClient(
      "playwright",
    ) as typeof import("playwright");
    const pageErrors: string[] = [];
    browser = await chromium.launch({ headless: true });

    const rebuild = await openPage(
      browser,
      pageOrigin,
      endpoint,
      aliceG1,
      barrierKey,
      "rebuild",
      pageErrors,
    );
    const lock = await openPage(
      browser,
      pageOrigin,
      endpoint,
      bobG1,
      barrierKey,
      "lock",
      pageErrors,
    );

    // ---- The control: both pages follow the source before the fence. ----
    const beforeFenceCommittedAt = Date.now();
    psql(
      `UPDATE tasks SET title = '${BEFORE_FENCE_TITLE}' WHERE id = ${String(BEFORE_FENCE_ID)};`,
    );
    const controlPaintMs: Record<string, number> = {};
    for (const [role, page] of [
      ["rebuild", rebuild],
      ["lock", lock],
    ] as const) {
      await waitFor(
        `the ${role} page to paint a change made before the fence`,
        DELIVERY_TIMEOUT_MS,
        async () =>
          titleIn(await stateOf(page), BEFORE_FENCE_ID) === BEFORE_FENCE_TITLE,
        20,
      );
      const paint = (await stateOf(page)).paints.find((candidate) =>
        candidate.insertedTitles.includes(BEFORE_FENCE_TITLE),
      );
      assert.ok(paint);
      controlPaintMs[role] = paint.atEpochMs - beforeFenceCommittedAt;
    }

    // ---- One write in flight on each revoked page. ----
    for (const [page, id, title] of [
      [rebuild, REBUILD_WRITE_ID, `alice pending ${runId}`],
      [lock, LOCK_WRITE_ID, `bob pending ${runId}`],
    ] as const) {
      await page.evaluate(() => globalThis.__calqueRevocationHoldMutations());
      await page.evaluate(
        ([writeId, writeTitle]) =>
          globalThis.__calqueRevocationStartUpdate(writeId, writeTitle),
        [id, title] as const,
      );
      await waitFor(
        "the write's optimistic paint",
        DELIVERY_TIMEOUT_MS,
        async () => {
          const state = await stateOf(page);
          return state.heldMutations === 1 && titleIn(state, id) === title;
        },
      );
    }

    // ---- The fence. ----
    const beforeFence = await Promise.all([stateOf(rebuild), stateOf(lock)]);
    const generationOneMounts = beforeFence
      .flatMap((state) => state.bootstraps)
      .filter((bootstrap) => bootstrap.status === 200).length;
    const fenceStartedAt = Date.now();
    const fenced = await postJson(`${serveOrigin}/v1/authorization/fence`, {
      order: signFenceOrder(applicationKey, 1, 2),
    });
    const fenceAnsweredAt = Date.now();
    assert.equal(fenced.status, 200, JSON.stringify(fenced.body));
    const fenceBody = fenced.body as {
      revokedGeneration: number;
      effectiveGeneration: number;
      drafts: BarrierDraft[];
    };
    assert.equal(fenceBody.revokedGeneration, 1);
    assert.equal(fenceBody.effectiveGeneration, 2);
    assert.equal(
      fencedGeneration(),
      "2",
      "the fence route answered before the fence was durable",
    );
    // One draft for the retired generation's one source-wide scope, which
    // every mount the two pages made holds.
    assert.ok(generationOneMounts >= 2, `${generationOneMounts} mounts`);
    assert.equal(fenceBody.drafts.length, 1, JSON.stringify(fenceBody.drafts));

    // Rows committed after the fence: past the barrier cursor, so the retired
    // plans must never deliver them, whether the barrier is signed or not.
    const afterFenceCommittedAt = Date.now();
    psql(`
INSERT INTO tasks (id, project_id, title, done)
  VALUES (${String(AFTER_FENCE_ROW)}, 101, '${AFTER_FENCE_ROW_TITLE}', false);
UPDATE tasks SET title = '${AFTER_FENCE_RENAMED_TITLE}'
  WHERE id = ${String(AFTER_FENCE_RENAMED_ID)};
`);
    const parkedWatchMs = Math.max(
      PARKED_WATCH_FLOOR_MS,
      10 * Math.max(...Object.values(controlPaintMs)),
    );
    await new Promise((done) => setTimeout(done, parkedWatchMs));
    for (const page of [rebuild, lock]) {
      const state = await stateOf(page);
      assert.equal(
        state.status.state,
        "ready",
        `the ${state.role} page left ready before its barrier was signed`,
      );
      for (const paint of state.paints) {
        assert.ok(
          !paint.inserted.includes(AFTER_FENCE_ROW) &&
            !paint.insertedTitles.includes(AFTER_FENCE_RENAMED_TITLE),
          `the ${state.role} page painted a row committed after the fence under the retired generation`,
        );
      }
    }

    // ---- The application's answer: a narrower grant, and a sign-out. ----
    const aliceG2 = mint("alice-g2", "alice", 2, 900, []);
    await rebuild.evaluate(
      ([token, label]) => globalThis.__calqueRevocationSetGrant(token, label),
      [aliceG2.token, aliceG2.label] as const,
    );
    await lock.evaluate(() =>
      globalThis.__calqueRevocationSetGrant(null, null),
    );

    // ---- The signed barriers. ----
    const attachStartedAt = Date.now();
    const attached = await postJson(
      `${serveOrigin}/v1/authorization/barriers`,
      {
        barriers: fenceBody.drafts.map((draft) =>
          signBarrier(applicationKey, draft),
        ),
      },
    );
    const attachAnsweredAt = Date.now();
    assert.equal(attached.status, 200, JSON.stringify(attached.body));
    assert.deepEqual(attached.body, {
      attached: fenceBody.drafts.length,
      expired: 0,
    });

    await waitFor(
      "the lock page to lock",
      REBUILD_TIMEOUT_MS,
      async () => (await stateOf(lock)).status.state === "locked",
      20,
    );
    await waitFor(
      "the rebuild page to rebuild under the narrower grant",
      REBUILD_TIMEOUT_MS,
      async () => {
        const state = await stateOf(rebuild);
        if (state.status.state === "locked" || state.status.state === "error")
          throw new Error(
            `the rebuild page ${state.status.state}: ${state.status.message ?? ""}`,
          );
        return (
          state.status.state === "ready" &&
          titleIn(state, AFTER_FENCE_ROW) === AFTER_FENCE_ROW_TITLE
        );
      },
      20,
    );

    // ---- The locked page. ----
    const locked = await stateOf(lock);
    const lockBarrier = leftReady(locked, attachStartedAt);
    const lockedRecord = locked.statuses.find(
      (status) => status.state === "locked",
    );
    assert.ok(lockedRecord);
    assert.equal(lockedRecord.reason, "revoked");
    assert.equal(lockedRecord.code, "AUTHORIZATION_REVOKED");
    assert.equal(locked.lockBanner.visible, true, "the lock is not shown");
    assert.match(locked.lockBanner.text, /revoked/u);
    assert.equal(
      await lock.locator("#lock").isVisible(),
      true,
      "the lock banner is not visible in the rendered page",
    );
    assert.deepEqual(locked.rows, [], "a locked page still shows rows");
    assert.deepEqual(
      insertingPaintsSince(locked, lockBarrier.atEpochMs),
      [],
      "the lock page painted rows after its barrier",
    );
    for (const paint of locked.paints)
      assert.ok(
        !paint.inserted.includes(AFTER_FENCE_ROW),
        "the lock page painted a row committed after the fence",
      );
    const lockWrite = firstWriteOf(locked);
    const lockCancelled = lockWrite.find(
      (transition) => transition.state === "ambiguous",
    );
    assert.ok(lockCancelled, JSON.stringify(lockWrite));
    assert.equal(lockCancelled.code, "MUTATION_AMBIGUOUS");
    assert.ok(lockCancelled.message?.includes(REVOKED_MESSAGE));
    assert.ok(lockCancelled.atEpochMs >= lockBarrier.atEpochMs);
    const lockAuthsAfterBarrier = locked.auths.filter(
      (auth) => auth.atEpochMs >= lockBarrier.atEpochMs,
    );
    assert.deepEqual(
      lockAuthsAfterBarrier.map((auth) => auth.grant),
      [null],
      "a revoked page must ask its application exactly once",
    );

    // ---- The rebuilt page. ----
    const rebuilt = await stateOf(rebuild);
    const rebuildBarrier = leftReady(rebuilt, attachStartedAt);
    const rebuildReady = rebuilt.statuses.find(
      (status) =>
        status.atEpochMs > rebuildBarrier.atEpochMs && status.state === "ready",
    );
    assert.ok(rebuildReady);
    const rebuildBootstraps = rebuilt.bootstraps.filter(
      (bootstrap) => bootstrap.atEpochMs >= rebuildBarrier.atEpochMs,
    );
    assert.ok(
      rebuildBootstraps.length > 0 &&
        rebuildBootstraps.every((bootstrap) => bootstrap.grant === "alice-g2"),
      `the rebuild presented something other than the new grant: ${JSON.stringify(rebuildBootstraps)}`,
    );
    assert.equal(rebuildBootstraps.at(-1)?.status, 200);
    // The table emptied on the barrier's turn, before anything was painted
    // again: the revoked rows left the screen, not only the facade.
    const emptied = rebuilt.paints.find(
      (paint) =>
        paint.atEpochMs >= rebuildBarrier.atEpochMs && paint.rows.length === 0,
    );
    assert.ok(emptied, "the rebuild page never cleared the revoked rows");
    // Every row painted after the barrier carries the state past the barrier
    // cursor, which only the new generation's mount can hold.
    const rebuiltPaints = insertingPaintsSince(
      rebuilt,
      rebuildBarrier.atEpochMs,
    );
    assert.ok(rebuiltPaints.length > 0);
    for (const paint of rebuiltPaints) {
      assert.ok(
        paint.atEpochMs >= emptied.atEpochMs,
        "a row was painted between the barrier and the clear",
      );
      assert.equal(paint.mountedGrant, "alice-g2");
      assert.ok(
        paint.inserted.includes(AFTER_FENCE_ROW) &&
          paint.insertedTitles.includes(AFTER_FENCE_RENAMED_TITLE),
        `a paint after the barrier did not come from the new generation: ${JSON.stringify(paint)}`,
      );
    }
    for (const paint of rebuilt.paints.filter(
      (candidate) => candidate.atEpochMs < rebuildBarrier.atEpochMs,
    ))
      assert.ok(!paint.inserted.includes(AFTER_FENCE_ROW));
    assert.equal(rebuilt.rows.length, SEEDED_TASKS + 1);
    assert.equal(
      titleIn(rebuilt, REBUILD_WRITE_ID),
      SEEDED_TITLES.get(REBUILD_WRITE_ID),
      "the cancelled write's optimistic value survived the rebuild",
    );
    const rebuildWrite = firstWriteOf(rebuilt);
    const rebuildCancelled = rebuildWrite.find(
      (transition) => transition.state === "ambiguous",
    );
    assert.ok(rebuildCancelled, JSON.stringify(rebuildWrite));
    assert.ok(rebuildCancelled.message?.includes(REVOKED_MESSAGE));
    assert.ok(rebuildCancelled.atEpochMs >= rebuildBarrier.atEpochMs);

    // ---- The held writes reach the server under the retired grant. ----
    for (const page of [rebuild, lock])
      await page.evaluate(() =>
        globalThis.__calqueRevocationReleaseMutations(),
      );
    for (const page of [rebuild, lock])
      await waitFor(
        "the held write to be answered",
        DELIVERY_TIMEOUT_MS,
        async () =>
          (await stateOf(page)).mutationRequests.some(
            (request) => request.status !== null || request.failure !== null,
          ),
      );
    await new Promise((done) => setTimeout(done, 250));
    const afterRelease = await Promise.all([stateOf(rebuild), stateOf(lock)]);
    for (const state of afterRelease) {
      const [request] = state.mutationRequests;
      assert.ok(request);
      assert.equal(
        request.status,
        401,
        `the ${state.role} page's write under the retired grant was answered ${String(request.status)}`,
      );
      assert.ok(request.grant?.endsWith("-g1"));
      const last = firstWriteOf(state).at(-1);
      assert.ok(last);
      assert.equal(last.state, "ambiguous");
      assert.ok(
        last.message?.includes(REVOKED_MESSAGE),
        `the late refusal replaced the cancellation: ${JSON.stringify(last)}`,
      );
    }
    assert.equal(
      committedTitle(REBUILD_WRITE_ID),
      SEEDED_TITLES.get(REBUILD_WRITE_ID),
    );
    assert.equal(
      committedTitle(LOCK_WRITE_ID),
      SEEDED_TITLES.get(LOCK_WRITE_ID),
    );

    // ---- The narrower grant is what the rebuilt session writes with. ----
    await rebuild.evaluate(
      ([id, title]) => globalThis.__calqueRevocationStartUpdate(id, title),
      [NARROWED_WRITE_ID, `alice after the fence ${runId}`] as const,
    );
    await waitFor(
      "the narrowed write to settle",
      DELIVERY_TIMEOUT_MS,
      async () =>
        (await stateOf(rebuild)).attempts.some(
          (attempt) =>
            attempt.id === NARROWED_WRITE_ID && attempt.settled !== "pending",
        ),
    );
    const narrowed = await stateOf(rebuild);
    const narrowedAttempt = narrowed.attempts.find(
      (attempt) => attempt.id === NARROWED_WRITE_ID,
    );
    assert.ok(narrowedAttempt);
    assert.equal(narrowedAttempt.settled, "refused");
    assert.equal(narrowedAttempt.errorCode, "MUTATION_REJECTED");
    const narrowedRequest = narrowed.mutationRequests.at(-1);
    assert.ok(narrowedRequest);
    assert.equal(narrowedRequest.grant, "alice-g2");
    assert.equal(
      narrowedRequest.status,
      404,
      "a grant with no command capability must find no write surface",
    );
    assert.equal(
      committedTitle(NARROWED_WRITE_ID),
      SEEDED_TITLES.get(NARROWED_WRITE_ID),
    );
    assert.equal(narrowed.status.state, "ready");

    // Nothing retries on the locked page.
    const lockedBefore = await stateOf(lock);
    await new Promise((done) => setTimeout(done, QUIET_WATCH_MS));
    const lockedAfter = await stateOf(lock);
    assert.equal(lockedAfter.auths.length, lockedBefore.auths.length);
    assert.equal(lockedAfter.bootstraps.length, lockedBefore.bootstraps.length);
    assert.equal(lockedAfter.status.state, "locked");

    // ---- Expiry: one application renews, one cannot. ----
    const carolShort = mint("carol-short", "carol", 2, SHORT_GRANT_SECONDS, [
      "tasks=update",
    ]);
    const daveShort = mint("dave-short", "dave", 2, SHORT_GRANT_SECONDS, [
      "tasks=update",
    ]);
    const renew = await openPage(
      browser,
      pageOrigin,
      endpoint,
      carolShort,
      barrierKey,
      "renew",
      pageErrors,
    );
    const expire = await openPage(
      browser,
      pageOrigin,
      endpoint,
      daveShort,
      barrierKey,
      "expire",
      pageErrors,
    );
    // Each renewal takes the next grant, and the first two expire after the
    // grant before them, so the page crosses three expiries on renewals.
    const carolRenewals = [
      mint("carol-r1", "carol", 2, SHORT_GRANT_SECONDS + 10, ["tasks=update"]),
      mint("carol-r2", "carol", 2, SHORT_GRANT_SECONDS + 20, ["tasks=update"]),
      mint("carol-r3", "carol", 2, 900, ["tasks=update"]),
    ];
    await renew.evaluate(
      (grants) => globalThis.__calqueRevocationQueueGrants(grants),
      carolRenewals.map((grant) => [grant.token, grant.label] as const),
    );
    const renewDownloadsBefore = (await stateOf(renew)).artifactDownloads;
    await expire.evaluate(() => globalThis.__calqueRevocationHoldMutations());
    await expire.evaluate(
      ([id, title]) => globalThis.__calqueRevocationStartUpdate(id, title),
      [EXPIRE_WRITE_ID, `dave pending ${runId}`] as const,
    );
    await waitFor(
      "dave's write to be in flight",
      DELIVERY_TIMEOUT_MS,
      async () => (await stateOf(expire)).heldMutations === 1,
    );
    await expire.evaluate(() =>
      globalThis.__calqueRevocationSetGrant(null, null),
    );
    const renewExpiresAt = carolShort.expiresAtUnixSeconds * 1000;
    const expireExpiresAt = daveShort.expiresAtUnixSeconds * 1000;
    assert.ok(
      Date.now() < Math.min(renewExpiresAt, expireExpiresAt),
      "the short grants expired before the pages were set up; raise SHORT_GRANT_SECONDS",
    );
    const expirySetUpAt = Date.now();

    await waitFor(
      "the expire page to lock",
      SHORT_GRANT_SECONDS * 1000 + REBUILD_TIMEOUT_MS,
      async () => (await stateOf(expire)).status.state === "locked",
      20,
    );
    const expired = await stateOf(expire);
    const expiredLock = expired.statuses.find(
      (status) => status.state === "locked",
    );
    assert.ok(expiredLock);
    assert.equal(expiredLock.reason, "expired");
    assert.equal(expiredLock.code, "AUTHORIZATION_EXPIRED");
    assert.ok(
      expiredLock.atEpochMs >= expireExpiresAt - PLAN_EXPIRY_LEAD_MS,
      `the expire page locked ${String(expireExpiresAt - expiredLock.atEpochMs)} ms before its grant expired: ${JSON.stringify({ statuses: expired.statuses, bootstraps: expired.bootstraps, auths: expired.auths })}`,
    );
    assert.equal(await expire.locator("#lock").isVisible(), true);
    assert.match(expired.lockBanner.text, /expired/u);
    assert.deepEqual(expired.rows, []);
    assert.deepEqual(insertingPaintsSince(expired, expiredLock.atEpochMs), []);
    const expireLeftReady = leftReady(expired, expirySetUpAt);
    assert.ok(
      expireLeftReady.atEpochMs >= expireExpiresAt - PLAN_EXPIRY_LEAD_MS,
      "the expire page left ready before its plan could have expired",
    );
    const expireWrite = firstWriteOf(expired);
    const expireCancelled = expireWrite.find(
      (transition) => transition.state === "ambiguous",
    );
    assert.ok(expireCancelled, JSON.stringify(expireWrite));
    assert.ok(expireCancelled.message?.includes(REBUILD_MESSAGE));
    // The page locked when its plan ended, up to two seconds before the grant
    // did. Sent before then, the write would be refused for its expired plan
    // (400) rather than for its expired grant (401); this is the latter.
    const untilGrantExpired = expireExpiresAt + 1_000 - Date.now();
    if (untilGrantExpired > 0)
      await new Promise((done) => setTimeout(done, untilGrantExpired));
    await expire.evaluate(() =>
      globalThis.__calqueRevocationReleaseMutations(),
    );
    await waitFor(
      "dave's write to be answered",
      DELIVERY_TIMEOUT_MS,
      async () =>
        (await stateOf(expire)).mutationRequests.some(
          (request) => request.status !== null || request.failure !== null,
        ),
    );
    const expiredAfterRelease = await stateOf(expire);
    assert.equal(expiredAfterRelease.mutationRequests[0]?.status, 401);
    assert.ok(
      firstWriteOf(expiredAfterRelease)
        .at(-1)
        ?.message?.includes(REBUILD_MESSAGE),
    );
    assert.equal(
      committedTitle(EXPIRE_WRITE_ID),
      SEEDED_TITLES.get(EXPIRE_WRITE_ID),
    );

    // The renew page renews on its own clock. Once the second renewal grant
    // has expired, a change committed now reaches it only under the third
    // renewal's plan, and a write is accepted only under its token.
    const lastExpiredAt = (carolRenewals[1]?.expiresAtUnixSeconds ?? 0) * 1000;
    const untilRenewalsExpired = lastExpiredAt + 1_000 - Date.now();
    if (untilRenewalsExpired > 0)
      await new Promise((done) => setTimeout(done, untilRenewalsExpired));
    const renewedLiveCommittedAt = Date.now();
    psql(`
UPDATE tasks SET title = '${RENEWED_LIVE_TITLE}'
  WHERE id = ${String(RENEWED_LIVE_ID)};
`);
    await waitFor(
      "the renew page to paint a change committed after three renewals",
      DELIVERY_TIMEOUT_MS,
      async () => {
        const state = await stateOf(renew);
        if (state.status.state !== "ready")
          throw new Error(
            `the renew page left ready: ${JSON.stringify(state.statuses)}`,
          );
        return titleIn(state, RENEWED_LIVE_ID) === RENEWED_LIVE_TITLE;
      },
      20,
    );
    const renewedLiveAt = Date.now();
    await renew.evaluate(
      ([id, title]) => globalThis.__calqueRevocationStartUpdate(id, title),
      [RENEWED_WRITE_ID, RENEWED_WRITE_TITLE] as const,
    );
    await waitFor(
      "the renew page's write to settle",
      DELIVERY_TIMEOUT_MS,
      async () =>
        (await stateOf(renew)).attempts.some(
          (attempt) =>
            attempt.id === RENEWED_WRITE_ID && attempt.settled !== "pending",
        ),
    );
    await waitFor(
      "the renew page to paint its own write",
      DELIVERY_TIMEOUT_MS,
      async () =>
        titleIn(await stateOf(renew), RENEWED_WRITE_ID) === RENEWED_WRITE_TITLE,
    );

    const renewed = await stateOf(renew);
    const renewedWrite = renewed.attempts.find(
      (attempt) => attempt.id === RENEWED_WRITE_ID,
    );
    assert.equal(
      renewedWrite?.settled,
      "accepted",
      JSON.stringify(renewedWrite),
    );
    assert.equal(committedTitle(RENEWED_WRITE_ID), RENEWED_WRITE_TITLE);
    assert.deepEqual(
      renewed.mutationRequests.map((request) => [
        request.grant,
        request.status,
      ]),
      [["carol-r3", 200]],
    );
    const renewals = renewed.bootstraps.filter(
      (bootstrap) => bootstrap.renewal,
    );
    assert.deepEqual(
      renewals.map((bootstrap) => [bootstrap.grant, bootstrap.status]),
      [
        ["carol-r1", 200],
        ["carol-r2", 200],
        ["carol-r3", 200],
      ],
      JSON.stringify(renewed.bootstraps),
    );
    assert.equal(
      renewed.bootstraps.length,
      renewals.length + 1,
      "the renew page bootstrapped again",
    );
    assert.equal(
      renewed.artifactDownloads,
      renewDownloadsBefore,
      "the renew page downloaded its artifact again",
    );
    assert.deepEqual(
      renewed.statuses
        .filter((status) => status.atEpochMs >= expirySetUpAt)
        .map((status) => status.state),
      [],
      "the renew page left ready",
    );
    assert.equal(renewed.status.state, "ready");
    // No blank: once the working set was on screen, no paint left the table
    // empty across the three expiries.
    const firstFull = renewed.paints.findIndex(
      (paint) => paint.rows.length >= SEEDED_TASKS,
    );
    assert.ok(firstFull >= 0);
    const blanks = renewed.paints
      .slice(firstFull)
      .filter((paint) => paint.rows.length === 0);
    assert.deepEqual(blanks, [], "the renew page blanked across its renewals");
    assert.equal(renewed.rows.length, SEEDED_TASKS + 1);

    assert.deepEqual(pageErrors, []);

    return {
      result: "PASS",
      runId,
      host: {
        cpus: cpus().length,
        loadAverage: loadavg().map((value) => Number(value.toFixed(2))),
        at: new Date().toISOString(),
      },
      proof: "pnpm serve:e2e (scripts/serve-revocation-e2e.ts)",
      serveStartupMillis: Number(serving.contract.get("startup_millis")),
      control: {
        commitToPaintMs: controlPaintMs,
      },
      fence: {
        fenceRouteMs: fenceAnsweredAt - fenceStartedAt,
        drafts: fenceBody.drafts.length,
        durableGeneration: 2,
        parkedWatchMs,
        afterFenceRowCommittedMsAfterFence:
          afterFenceCommittedAt - fenceAnsweredAt,
      },
      barrier: {
        attachRouteMs: attachAnsweredAt - attachStartedAt,
        lock: {
          attachToBarrierMs: lockBarrier.atEpochMs - attachStartedAt,
          attachToLockedMs: lockedRecord.atEpochMs - attachStartedAt,
          reason: lockedRecord.reason,
          banner: locked.lockBanner.text,
          paintsAfterBarrierWithRows: 0,
          pendingWrite: lockCancelled.message,
          lateRefusalStatus: afterRelease[1]?.mutationRequests[0]?.status,
        },
        rebuild: {
          attachToBarrierMs: rebuildBarrier.atEpochMs - attachStartedAt,
          attachToClearedMs: emptied.atEpochMs - attachStartedAt,
          attachToReadyMs: rebuildReady.atEpochMs - attachStartedAt,
          bootstrapsAfterBarrier: rebuildBootstraps.map(
            (bootstrap) => bootstrap.status,
          ),
          statuses: rebuilt.statuses
            .filter((status) => status.atEpochMs >= rebuildBarrier.atEpochMs)
            .map((status) => status.state),
          paintsAfterBarrierWithRows: rebuiltPaints.length,
          narrowedWriteStatus: narrowedRequest.status,
          pendingWrite: rebuildCancelled.message,
          lateRefusalStatus: afterRelease[0]?.mutationRequests[0]?.status,
        },
      },
      expiry: {
        grantLifetimeSeconds: SHORT_GRANT_SECONDS,
        expire: {
          expiryToLeftReadyMs: expireLeftReady.atEpochMs - expireExpiresAt,
          expiryToLockedMs: expiredLock.atEpochMs - expireExpiresAt,
          reason: expiredLock.reason,
          banner: expired.lockBanner.text,
          pendingWrite: expireCancelled.message,
          lateRefusalStatus: expiredAfterRelease.mutationRequests[0]?.status,
        },
        renew: {
          renewalsBeforeExpiryMs: renewals.map(
            (bootstrap, index) =>
              (index === 0
                ? renewExpiresAt
                : (carolRenewals[index - 1]?.expiresAtUnixSeconds ?? 0) *
                  1000) - bootstrap.atEpochMs,
          ),
          expiriesCrossed: 3,
          artifactDownloads: renewed.artifactDownloads,
          commitToPaintAfterRenewalsMs: renewedLiveAt - renewedLiveCommittedAt,
          blankPaints: blanks.length,
        },
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
    `serve-revocation-e2e: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
  );
} finally {
  adminPsql(DROP_RUN_DATABASE_SQL, false);
  rmSync(workDirectory, { recursive: true, force: true });
  if (!keepContainer) removePostgresContainer(container);
}
process.exit(status);
