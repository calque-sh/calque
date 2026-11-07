#!/usr/bin/env node
/// <reference lib="dom" />
/**
 * Runs the self-hosted composition end to end from a clean state.
 *
 * Brings `docker-compose.selfhost.yml` up on run-scoped credentials, seeds the
 * source, and drives a headless-Chromium page through `createSyncClient`
 * against the Caddy TLS edge: a source change reaches the page, and a write
 * through the generated facade is confirmed by its CDC echo. It then recreates
 * `postgres` under the running server, stops, recreates and kills the server,
 * and backs the deployment up and restores it under the S3 store. Every browser
 * request must go through the edge, the artifact objects must be in the bucket
 * and not the container's filesystem, and the page must read them through the
 * edge. It prints one JSON outcome and exits non-zero on any failure.
 *
 * Usage: `pnpm selfhost:e2e`. Requires Docker and a Rust toolchain, and on
 * Linux NSS's `certutil` (Debian and Ubuntu: `libnss3-tools`).
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  createHash,
  randomBytes,
  randomUUID,
  X509Certificate,
} from "node:crypto";
import { createServer, request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { connect as tlsConnect, type DetailedPeerCertificate } from "node:tls";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, extname, join, normalize, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import type { Browser, Page } from "playwright";
import { MIME, reservedPort, run } from "./lib/serve-e2e.ts";
import { CARGO_TARGET_ARGUMENTS } from "./lib/cargo-target.ts";

/** What the page under test exposes; see the page's own `main.ts`. */
interface SelfhostPageState {
  readonly state: string;
  readonly failure: string | null;
  readonly cursor: string | null;
  readonly publications: number;
  readonly projects: readonly (readonly [number, string, string])[];
  readonly tasks: readonly (readonly [number, string, boolean])[];
  readonly artifactRequests: readonly string[];
  readonly fetchOrigins: readonly string[];
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations to `typeof globalThis`.
  // eslint-disable-next-line no-var
  var __calqueSelfhostState: () => SelfhostPageState;
  // eslint-disable-next-line no-var
  var __calqueSelfhostUpdateTask: (
    id: number,
    title: string,
  ) => Promise<{ readonly mutationId: string; readonly state: string }>;
  // eslint-disable-next-line no-var
  var __calqueSelfhostMutationState: (mutationId: string) => string | null;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const composeFile = join(root, "docker-compose.selfhost.yml");
const pageRoot = join(root, "target/client-selfhost-compose");
const runId = randomUUID().replaceAll("-", "").slice(0, 12);
const workDirectory = join(root, "target/selfhost-compose", runId);
const project = `calque-selfhost-e2e-${runId}`;
const READY_TIMEOUT_MS = 300_000;
const CHANGE_TIMEOUT_MS = 60_000;
/** The name the edge serves; Caddy issues it a certificate from its local CA. */
const PUBLIC_HOST = "localhost";
/** Where Caddy keeps its local CA's root, on the `caddy-data` volume. */
const CADDY_LOCAL_ROOT = "/data/caddy/pki/authorities/local/root.crt";

/** The rows `SEED_SQL` inserts. */
const SEEDED_PROJECTS = 3;
const SEEDED_TASKS = 9;

interface RunOutcome {
  readonly result: "PASS";
  readonly project: string;
  readonly edge: string;
  readonly browserTrust: BrowserTrust["mode"];
  readonly browserOrigins: readonly string[];
  readonly plaintextListenerBrowserHostStatus: number;
  readonly objectStore: "s3";
  readonly bucketObjects: number;
  readonly containerFilesystemObjects: 0;
  readonly startupMillis: number;
  readonly serverStopMs: number;
  readonly readyAfterKillMs: number;
  readonly projects: number;
  readonly tasks: number;
  readonly publicationsAfterSourceChange: number;
  readonly artifactRequests: number;
  readonly write: "canonical";
  readonly mutationRouteStatusWithoutCommand: number;
  readonly unitObjects: number;
  readonly bucketObjectsAfterRestore: number;
  readonly restoredOracleMatchesBackup: true;
  readonly artifactRequestsAfterRestore: number;
  readonly projectsAfterRestore: number;
  readonly restoreRefusedWithoutTheObjectHalf: string;
}

function hex(bytes: number): string {
  return randomBytes(bytes).toString("hex");
}

// The run's own project, from the first compose call to the cleanup. Compose
// prefers an exported `COMPOSE_PROJECT_NAME` to the file's `name:`, whose
// default is a real deployment's, and the cleanup takes its project down with
// its volumes: a project named by anything else could be a developer's own.
const projectEnvironment = {
  CALQUE_SELFHOST_PROJECT: project,
  COMPOSE_PROJECT_NAME: project,
};
let environment: Record<string, string> = { ...projectEnvironment };

function compose(...args: readonly string[]): string {
  return run("docker", ["compose", "-f", composeFile, ...args], {
    env: environment,
  });
}

/**
 * A `docker compose` call that hands secrets to the container without putting
 * them on this host's process listing. Compose's `-e NAME` without a value
 * reads the value from the CLI's own environment, so only the name appears in
 * `ps`; `secrets` are set on this call's child process and nowhere else.
 */
function composeWithSecrets(
  secrets: Record<string, string>,
  ...args: readonly string[]
): string {
  return run("docker", ["compose", "-f", composeFile, ...args], {
    env: { ...environment, ...secrets },
  });
}

function composeQuiet(...args: readonly string[]): void {
  spawnSync("docker", ["compose", "-f", composeFile, ...args], {
    cwd: root,
    encoding: "utf8",
    env: { ...process.env, ...environment },
  });
}

function psql(sql: string): string {
  return composeWithSecrets(
    { PGPASSWORD: environment.CALQUE_SELFHOST_DB_PASSWORD ?? "" },
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
    environment.CALQUE_SELFHOST_DB_ROLE ?? "calque_sync",
    "-d",
    environment.CALQUE_SELFHOST_DATABASE ?? "calque",
    "-c",
    sql,
  );
}

/**
 * One recovery subcommand, in a one-off container with the server's own
 * volumes and environment, reading the connection string from
 * `CALQUE_DATABASE_URL`, never from argv. The unit lands on the
 * `calque-recovery` volume, so destroying the state volume does not destroy the
 * backup. A host bind mount would keep its host owner, which on Linux is not
 * the image's user, so `backup` would fail there with EACCES.
 */
function calqueRecovery(...args: readonly string[]): {
  readonly status: number;
  readonly output: string;
} {
  const result = spawnSync(
    "docker",
    [
      "compose",
      "-f",
      composeFile,
      "run",
      "--rm",
      "--no-deps",
      "calque",
      ...args,
    ],
    {
      cwd: root,
      encoding: "utf8",
      env: { ...process.env, ...environment },
      maxBuffer: 64 * 1024 * 1024,
    },
  );
  return {
    status: result.status ?? -1,
    output: `${result.stdout ?? ""}${result.stderr ?? ""}`,
  };
}

function requireRecovery(what: string, ...args: readonly string[]): string {
  const { status, output } = calqueRecovery(...args);
  if (status !== 0)
    throw new Error(`${what} failed (${String(status)}): ${output}`);
  return output;
}

/**
 * `tar` in a one-off container of the server's image, as its user, on the
 * `calque-recovery` volume: how docs/design/operations.md moves a unit
 * between that volume and the host. `-T` keeps the archive on stdout and
 * stdin byte for byte.
 */
function recoveryTar(args: readonly string[], input?: Buffer): Buffer {
  const result = spawnSync(
    "docker",
    [
      "compose",
      "-f",
      composeFile,
      "run",
      "--rm",
      "--no-deps",
      "-T",
      "--entrypoint",
      "tar",
      "calque",
      ...args,
    ],
    {
      cwd: root,
      env: { ...process.env, ...environment },
      ...(input === undefined ? {} : { input }),
      maxBuffer: 512 * 1024 * 1024,
    },
  );
  if (result.status !== 0)
    throw new Error(
      `tar ${args.join(" ")} in the calque image exited ${String(result.status)}: ${result.error?.message ?? result.stderr.toString("utf8")}`,
    );
  return result.stdout;
}

/**
 * Stops the server and waits until its source lease and logical slot are idle,
 * since every recovery command refuses a live lease or an active slot. The
 * fence lease has a ten-second TTL, so a stopped container is not instantly a
 * stopped source.
 */
async function stopSourceAndWait(): Promise<void> {
  compose("stop", "calque");
  await waitFor(
    "the source lease and logical slot to go idle",
    120_000,
    async () => {
      const busy = psql(
        `SELECT (SELECT count(*) FROM calque_control.source_fence_lease
                WHERE lease_phase <> 'unowned' AND expires_at > clock_timestamp())
            + (SELECT count(*) FROM pg_catalog.pg_replication_slots
                WHERE active_pid IS NOT NULL) AS busy;`,
      );
      return /(^|\s)0(\s|$)/m.test(busy);
    },
  );
}

/**
 * The source lease's phase, whether it is unexpired, and whether a walsender
 * holds the logical slot, as `phase,unexpired|expired,slot-active|slot-idle`.
 */
function sourceHold(): string {
  const output = psql(
    `SELECT 'hold=' || lease.lease_phase
            || CASE WHEN lease.expires_at > clock_timestamp()
                    THEN ',unexpired' ELSE ',expired' END
            || CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
                                  WHERE active_pid IS NOT NULL)
                    THEN ',slot-active' ELSE ',slot-idle' END
       FROM calque_control.source_fence_lease AS lease;`,
  );
  const hold = /hold=(\S+)/.exec(output)?.[1];
  if (hold === undefined) throw new Error(`no lease row in: ${output}`);
  return hold;
}

/** The `<label> oracle: <hex>` line every recovery command prints. */
function oracleOf(output: string): string {
  const line = output
    .split("\n")
    .map((entry) => entry.trim())
    .find((entry) => entry.includes(" oracle: "));
  if (line === undefined) throw new Error(`no oracle line in: ${output}`);
  return line.slice(line.indexOf(" oracle: ") + " oracle: ".length);
}

/** The object keys under the deployment's own prefix, sorted. */
function bucketObjectKeys(): string[] {
  return mc(
    "ls",
    "--recursive",
    `selfhost/${environment.CALQUE_S3_BUCKET ?? ""}`,
  )
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.includes(`${environment.CALQUE_S3_PREFIX}/objects/`))
    .map((line) => line.slice(line.lastIndexOf(" ") + 1))
    .sort();
}

/** One `mc` command inside the composition, against its own object store. */
function mc(...args: readonly string[]): string {
  return composeWithSecrets(
    {
      MC_HOST_selfhost: `http://${environment.CALQUE_S3_ACCESS_KEY ?? ""}:${environment.CALQUE_S3_SECRET_KEY ?? ""}@objectstore:9000`,
    },
    "run",
    "--rm",
    "--no-deps",
    "-e",
    "MC_HOST_selfhost",
    "--entrypoint",
    "mc",
    "objectstore-init",
    ...args,
  );
}

const SEED_SQL = `
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
ALTER TABLE projects REPLICA IDENTITY FULL;
ALTER TABLE tasks REPLICA IDENTITY FULL;
DROP PUBLICATION IF EXISTS calque_demo;
CREATE PUBLICATION calque_demo FOR TABLE projects, tasks;
`;

const CHANGED_PROJECT_NAME = `Atlas launch ${runId}`;

/**
 * The page's own origin, a plain static server rather than a proxy, so every
 * Calque request the page makes is cross-origin to the container, as in a
 * deployment.
 */
function servePage(port: number): { close: () => Promise<void> } {
  const server = createServer((request, response) => {
    const requested = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
    const relative = normalize(requested === "/" ? "/index.html" : requested);
    const file = join(pageRoot, relative);
    if (!file.startsWith(pageRoot)) {
      response.writeHead(403).end();
      return;
    }
    try {
      const body = readFileSync(file);
      // No COOP/COEP: the client needs no cross-origin isolation, and
      // `require-corp` would only add a way for the cross-origin artifact
      // fetch to fail for a reason the deployment does not have.
      response.writeHead(200, {
        "content-type": MIME[extname(file)] ?? "application/octet-stream",
      });
      response.end(body);
    } catch {
      response.writeHead(404).end();
    }
  });
  server.listen(port, "127.0.0.1");
  return {
    close: () =>
      new Promise<void>((done) => {
        server.close(() => done());
      }),
  };
}

async function waitFor(
  what: string,
  timeoutMs: number,
  condition: () => Promise<boolean>,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await condition()) return;
      lastError = undefined;
    } catch (error) {
      lastError = error;
    }
    await new Promise((done) => setTimeout(done, 500));
  }
  throw new Error(
    `timed out after ${String(timeoutMs)} ms waiting for ${what}${lastError === undefined ? "" : `: ${String(lastError)}`}`,
  );
}

interface EdgeResponse {
  readonly status: number;
  readonly body: string;
}

/**
 * One request to the HTTPS edge that verifies what a browser verifies: the
 * chain up to Caddy's local root, which is the only CA this request trusts,
 * and the name `localhost`. It connects to the published loopback port and
 * sends the `Host` a browser would.
 */
async function edge(
  root: string,
  port: number,
  path: string,
  init: {
    readonly method?: string;
    readonly headers?: Record<string, string>;
    readonly body?: string;
  } = {},
): Promise<EdgeResponse> {
  return await new Promise((resolveResponse, rejectResponse) => {
    const request = httpsRequest(
      {
        host: "127.0.0.1",
        port,
        servername: PUBLIC_HOST,
        path,
        method: init.method ?? "GET",
        headers: { host: `${PUBLIC_HOST}:${String(port)}`, ...init.headers },
        ca: root,
        agent: false,
      },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk: string) => (body += chunk));
        response.on("end", () =>
          resolveResponse({ status: response.statusCode ?? 0, body }),
        );
      },
    );
    request.on("error", rejectResponse);
    request.end(init.body);
  });
}

/** One plaintext request to one of the server's own loopback listeners. */
async function plaintextResponse(
  port: number,
  path: string,
  host: string,
): Promise<{ readonly status: number; readonly body: string }> {
  return await new Promise((resolveResponse, rejectResponse) => {
    const request = httpRequest(
      { host: "127.0.0.1", port, path, headers: { host }, agent: false },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk: string) => (body += chunk));
        response.on("end", () =>
          resolveResponse({ status: response.statusCode ?? 0, body }),
        );
      },
    );
    request.on("error", rejectResponse);
    request.end();
  });
}

/** The status of one plaintext request to the server's own listeners. */
async function plaintext(
  port: number,
  path: string,
  host: string,
): Promise<number> {
  return (await plaintextResponse(port, path, host)).status;
}

interface BrowserTrust {
  readonly mode: "nss-trust-anchor" | "spki-allowlist";
  readonly args: readonly string[];
  readonly env: Record<string, string> | undefined;
}

/**
 * The SPKI hash, as Chromium's allowlist spells it, of every certificate in
 * the chain the edge presents, once that chain has verified against `root`
 * and the public host's name.
 */
async function verifiedEdgeChainKeys(
  root: string,
  port: number,
): Promise<string[]> {
  const chain = await new Promise<DetailedPeerCertificate>(
    (resolveChain, rejectChain) => {
      const socket = tlsConnect(
        { host: "127.0.0.1", port, servername: PUBLIC_HOST, ca: root },
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

/**
 * How this run's Chromium comes to accept Caddy's certificate. On Linux the
 * root goes into an NSS database under a private `HOME` and Chromium verifies
 * the chain itself. On macOS Chromium trusts the login keychain, and adding a
 * root there needs an administrator's authorization and changes the whole system's
 * trust, so the harness verifies the chain and allowlists its keys instead.
 * The root's key alone would not match: Caddy sends its leaf and intermediate,
 * never its root.
 */
async function browserTrust(root: string, port: number): Promise<BrowserTrust> {
  if (process.platform === "linux") {
    const rootFile = join(workDirectory, "caddy-local-root.crt");
    writeFileSync(rootFile, root);
    const home = join(workDirectory, "chromium-home");
    const database = join(home, ".pki/nssdb");
    mkdirSync(database, { recursive: true });
    run("certutil", ["-d", `sql:${database}`, "-N", "--empty-password"]);
    run("certutil", [
      "-d",
      `sql:${database}`,
      "-A",
      "-t",
      "C,,",
      "-n",
      "calque-selfhost-caddy-local-root",
      "-i",
      rootFile,
    ]);
    const env: Record<string, string> = {};
    for (const [name, value] of Object.entries(process.env))
      if (value !== undefined) env[name] = value;
    return { mode: "nss-trust-anchor", args: [], env: { ...env, HOME: home } };
  }
  const keys = await verifiedEdgeChainKeys(root, port);
  return {
    mode: "spki-allowlist",
    args: [`--ignore-certificate-errors-spki-list=${keys.join(",")}`],
    env: undefined,
  };
}

async function main(): Promise<RunOutcome> {
  mkdirSync(workDirectory, { recursive: true });

  const httpPort = await reservedPort();
  const metricsPort = await reservedPort();
  const httpsPort = await reservedPort();
  const postgresPort = await reservedPort();
  const s3Port = await reservedPort();
  const pagePort = await reservedPort();
  const pageOrigin = `http://127.0.0.1:${String(pagePort)}`;
  // What the browser addresses: the edge, never the server's own listener.
  const serverBase = `https://${PUBLIC_HOST}:${String(httpsPort)}`;

  // The shared binding fixture, with the one field a deployment owns
  // rewritten: the environment variable that holds the connection string.
  const manifestPath = join(workDirectory, "calque.config.json");
  const manifest = JSON.parse(
    readFileSync(
      join(root, "client/packages/client/tests/demo/calque.config.json"),
      "utf8",
    ),
  ) as { sources: { connectionEnv: string }[] };
  const [source] = manifest.sources;
  if (!source) throw new Error("the binding fixture declares no source");
  source.connectionEnv = "CALQUE_DATABASE_URL";
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

  // Run-scoped credentials. Hexadecimal so they survive being interpolated
  // into the `mc` alias URL the object-store init service reads.
  environment = {
    ...projectEnvironment,
    CALQUE_SELFHOST_HTTP_PORT: String(httpPort),
    CALQUE_SELFHOST_METRICS_PORT: String(metricsPort),
    CALQUE_SELFHOST_HTTPS_PORT: String(httpsPort),
    CALQUE_SELFHOST_PUBLIC_HOST: PUBLIC_HOST,
    CALQUE_SELFHOST_POSTGRES_PORT: String(postgresPort),
    CALQUE_SELFHOST_S3_PORT: String(s3Port),
    CALQUE_SELFHOST_DATABASE: "calque",
    CALQUE_SELFHOST_DB_ROLE: "calque_sync",
    CALQUE_SELFHOST_POSTGRES_SUPERUSER_PASSWORD: hex(16),
    CALQUE_SELFHOST_DB_PASSWORD: hex(16),
    CALQUE_S3_BUCKET: "calque-artifacts",
    CALQUE_S3_PREFIX: "calque",
    CALQUE_S3_ACCESS_KEY: `ak${hex(10)}`,
    CALQUE_S3_SECRET_KEY: hex(24),
    CALQUE_SELFHOST_CONFIG: manifestPath,
    CALQUE_SELFHOST_GRANT_AUDIENCE: "selfhost.e2e",
    CALQUE_SELFHOST_ALLOWED_ORIGIN: pageOrigin,
    CALQUE_SELFHOST_IMAGE: `calque:selfhost-e2e-${runId}`,
    // Replaced below, once the grant key exists. Compose interpolates the
    // whole file for every command, so it has to be set from the first one.
    CALQUE_SELFHOST_GRANT_VERIFICATION_KEY: "{}",
  };

  // `serve` verifies grants and mints none, so the development minter stands
  // in for the application backend. Two grants under one key: the page's,
  // which may rename a task, and one with no command capability, for which the
  // write surface does not exist.
  const grantKeyFile = join(workDirectory, "grant-key.pk8");
  const mint = (...command: readonly string[]) =>
    JSON.parse(
      run("cargo", [
        "run",
        "--quiet",
        ...CARGO_TARGET_ARGUMENTS,
        "-p",
        "dataset-grant-issuer",
        "--bin",
        "calque-dev-grant",
        "--",
        "--key-file",
        grantKeyFile,
        "--key-id",
        "selfhost-e2e",
        "--audience",
        "selfhost.e2e",
        "--generation",
        "1",
        "--subject",
        "selfhost-e2e-member",
        "--lifetime-seconds",
        "900",
        ...command,
      ]),
    ) as { token: string; verificationKeyEnvironment: unknown };
  const minted = mint("--command", "tasks=update");
  const readOnly = mint();
  environment.CALQUE_SELFHOST_GRANT_VERIFICATION_KEY = JSON.stringify(
    minted.verificationKeyEnvironment,
  );

  // From a clean state: no volume, no container, no image layer of a previous
  // run's source.
  composeQuiet("down", "-v", "--remove-orphans");
  compose(
    "up",
    "-d",
    "--build",
    "postgres",
    "objectstore",
    "objectstore-init",
    "caddy",
  );
  await waitFor("PostgreSQL to accept the seed", 120_000, async () => {
    psql("SELECT 1");
    return true;
  });
  psql(SEED_SQL);

  // The edge's local CA exists once Caddy has provisioned the site. Its root
  // is the only certificate every TLS client in this run trusts.
  let caddyRoot = "";
  await waitFor("Caddy's local CA root", 60_000, async () => {
    caddyRoot = compose("exec", "-T", "caddy", "cat", CADDY_LOCAL_ROOT);
    return caddyRoot.includes("BEGIN CERTIFICATE");
  });
  const caddyContainer = compose("ps", "-q", "caddy").trim();
  const probe = async (path: string): Promise<number | "unreachable"> => {
    try {
      return (await edge(caddyRoot, httpsPort, path)).status;
    } catch {
      return "unreachable";
    }
  };

  compose("up", "-d", "calque");
  const startupLines: string[] = [];
  await waitFor(
    "the server to report readiness",
    READY_TIMEOUT_MS,
    async () => {
      const logs = compose("logs", "--no-log-prefix", "calque");
      startupLines.length = 0;
      startupLines.push(...logs.split("\n").map((line) => line.trim()));
      return startupLines.some((line) =>
        line.startsWith("Calque serve is ready"),
      );
    },
  );

  const contract = new Map(
    startupLines
      .filter((line) => line.includes("="))
      .map((line) => {
        const at = line.indexOf("=");
        return [line.slice(0, at), line.slice(at + 1)] as const;
      }),
  );
  assert.equal(
    contract.get("object_store"),
    "s3",
    `the startup contract must report the S3 store; got ${JSON.stringify(startupLines)}`,
  );
  assert.equal(contract.get("artifact"), "published");
  assert.equal(contract.get("ready"), "true");
  assert.equal(contract.get("expected_host"), PUBLIC_HOST);
  const metricsHost = `127.0.0.1:${String(metricsPort)}`;
  assert.equal(contract.get("metrics_listener"), "configured-bind-address");
  assert.equal(contract.get("metrics_expected_host"), metricsHost);
  assert.equal(
    contract.get("mutations"),
    "mounted-per-collection-command-grant",
  );
  assert.equal(
    contract.get("mutation_commands"),
    "projects:insert+update+delete,tasks:insert+update+delete",
  );
  const startupMillis = Number(contract.get("startup_millis"));
  assert.ok(Number.isFinite(startupMillis));
  assert.ok(
    startupLines.includes(`Calque serve is ready at ${serverBase}/`),
    `the server must name the edge as its public address; got ${JSON.stringify(startupLines)}`,
  );

  await waitFor(
    "readiness through the edge",
    60_000,
    async () => (await probe("/readyz")) === 204,
  );
  // Nothing the edge forwards to serves the scrape: the HTTP listener has no
  // `/metrics`, and the metrics listener answers only on its loopback port,
  // to the `Host` that port carries.
  assert.equal((await edge(caddyRoot, httpsPort, "/metrics")).status, 404);
  assert.equal(await plaintext(httpPort, "/metrics", PUBLIC_HOST), 404);
  const scrape = await plaintextResponse(metricsPort, "/metrics", metricsHost);
  assert.equal(scrape.status, 200);
  assert.ok(
    scrape.body.endsWith("# EOF\n"),
    `the scrape must be OpenMetrics; got ${scrape.body.slice(-200)}`,
  );
  assert.equal(await plaintext(metricsPort, "/metrics", PUBLIC_HOST), 421);

  // The server's own plaintext listener answers an operator who names the
  // public host, and refuses the `Host` any browser sent there would carry.
  assert.equal(await plaintext(httpPort, "/readyz", PUBLIC_HOST), 204);
  const plaintextListenerBrowserHostStatus = await plaintext(
    httpPort,
    "/readyz",
    `${PUBLIC_HOST}:${String(httpPort)}`,
  );
  assert.equal(plaintextListenerBrowserHostStatus, 421);
  assert.equal(
    await plaintext(httpPort, "/readyz", `127.0.0.1:${String(httpPort)}`),
    421,
  );

  // The artifact bytes are in the object store, and nowhere else. A run that
  // fell back to the filesystem store would pass every browser assertion
  // below, so this is checked before any of them.
  const listing = mc(
    "ls",
    "--recursive",
    `selfhost/${environment.CALQUE_S3_BUCKET ?? ""}`,
  )
    .split("\n")
    .filter((line) =>
      line.includes(`${environment.CALQUE_S3_PREFIX}/objects/`),
    );
  assert.ok(
    listing.length > 0,
    "the composition's bucket holds no artifact objects; the server fell back to the filesystem store",
  );
  const containerObjects = compose(
    "exec",
    "-T",
    "calque",
    "sh",
    "-c",
    "find /var/lib/calque/state/objects -type f | wc -l",
  ).trim();
  assert.equal(
    Number(containerObjects),
    0,
    `the container's filesystem object tree holds ${containerObjects} files; the server did not use the object store`,
  );

  // The client packages the page imports, then the page, so neither is built
  // against a stale `dist/`.
  run("pnpm", ["--filter", "@calque/client...", "build"]);
  run("pnpm", ["--filter", "@calque/client", "selfhost:build"]);
  const pageServer = servePage(pagePort);
  const requireFromClient = createRequire(
    join(root, "client/packages/client/package.json"),
  );
  const { chromium } = requireFromClient(
    "playwright",
  ) as typeof import("playwright");
  let browser: Browser | undefined;
  let outcome: RunOutcome;
  try {
    const trust = await browserTrust(caddyRoot, httpsPort);
    const launched: Browser = await chromium.launch({
      headless: true,
      args: [...trust.args],
      ...(trust.env === undefined ? {} : { env: trust.env }),
    });
    browser = launched;
    // Each page gets a context of its own, so nothing one page mounted is
    // served to the next out of its storage. The context records the origin
    // of every request sent anywhere but the page's own origin, including
    // the ones a worker sends, which the page's fetch wrapper cannot see.
    const openPage = async (): Promise<{
      readonly page: Page;
      readonly errors: string[];
      readonly origins: Set<string>;
      readonly failedRequests: string[];
    }> => {
      const context = await launched.newContext({
        viewport: { width: 1280, height: 900 },
      });
      const origins = new Set<string>();
      context.on("request", (request) => {
        const url = new URL(request.url());
        if (
          (url.protocol === "http:" || url.protocol === "https:") &&
          url.origin !== pageOrigin
        )
          origins.add(url.origin);
      });
      // What the browser's network stack said about a request the page only
      // sees as a thrown fetch: a certificate or CORS refusal names itself.
      const failedRequests: string[] = [];
      context.on("requestfailed", (request) =>
        failedRequests.push(
          `${request.method()} ${request.url()}: ${request.failure()?.errorText ?? "unknown"}`,
        ),
      );
      const opened = await context.newPage();
      const errors: string[] = [];
      opened.on("pageerror", (error) => errors.push(String(error)));
      await opened.addInitScript(
        ([endpoint, grant]) => {
          Object.defineProperty(globalThis, "__calqueSelfhostConfig", {
            value: { endpoint, grant },
          });
        },
        [`${serverBase}/`, minted.token] as const,
      );
      await opened.goto(pageOrigin, { waitUntil: "load", timeout: 60_000 });
      return { page: opened, errors, origins, failedRequests };
    };
    const first = await openPage();
    const { page } = first;

    await waitFor(
      "the page to mount the complete working set",
      120_000,
      async () => {
        const state = await page.evaluate(() =>
          globalThis.__calqueSelfhostState(),
        );
        if (state.state === "error")
          throw new Error(
            `the page failed: ${state.failure ?? "unknown"}; failed requests: ${JSON.stringify(first.failedRequests)}`,
          );
        return (
          state.state === "ready" &&
          state.projects.length === SEEDED_PROJECTS &&
          state.tasks.length === SEEDED_TASKS
        );
      },
    );
    const mounted = await page.evaluate(() =>
      globalThis.__calqueSelfhostState(),
    );
    assert.deepEqual(first.errors, []);
    assert.ok(
      mounted.artifactRequests.length > 0,
      "the page mounted without fetching an artifact object",
    );
    for (const url of mounted.artifactRequests) {
      assert.ok(
        url.startsWith(`${serverBase}/v1/artifact-objects/`),
        `the page fetched an artifact from ${url}, not the containerized server through the edge`,
      );
    }
    // Bootstrap and the change stream went to the edge too, and nothing the
    // browser sent on the page's behalf went anywhere else.
    assert.deepEqual(mounted.fetchOrigins, [serverBase]);
    assert.deepEqual(
      [...first.origins],
      [serverBase],
      `the browser sent requests outside the HTTPS edge: ${[...first.origins].join(", ")}`,
    );

    // A source change applied to PostgreSQL must reach the page through the
    // containerized server's CDC, with no reload.
    const publicationsBefore = mounted.publications;
    psql(
      `UPDATE projects SET name = '${CHANGED_PROJECT_NAME}' WHERE id = 101;`,
    );
    await waitFor(
      "the page to advance on the source change",
      CHANGE_TIMEOUT_MS,
      async () => {
        const state = await page.evaluate(() =>
          globalThis.__calqueSelfhostState(),
        );
        if (state.state === "error")
          throw new Error(
            `the page failed: ${state.failure ?? "unknown"}; failed requests: ${JSON.stringify(first.failedRequests)}`,
          );
        return state.projects.some(
          ([id, name]) => id === 101 && name === CHANGED_PROJECT_NAME,
        );
      },
    );
    const advanced = await page.evaluate(() =>
      globalThis.__calqueSelfhostState(),
    );
    assert.ok(
      advanced.publications > publicationsBefore,
      "the page changed without a publication, so not through the change stream",
    );

    // A write from the page, through the edge. The facade submits it, the
    // server applies it to PostgreSQL, and the client calls it canonical only
    // once the change stream delivers the echo of that commit.
    const renamedTitle = `Freeze the release candidate ${runId}`;
    const write = await page.evaluate(
      ([id, title]) => globalThis.__calqueSelfhostUpdateTask(id, title),
      [1001, renamedTitle] as const,
    );
    assert.equal(
      write.state,
      "awaiting_canonical_echo",
      `the server did not accept the page's write: ${JSON.stringify(write)}`,
    );
    await waitFor(
      "the page's write to become canonical",
      CHANGE_TIMEOUT_MS,
      async () => {
        const state = await page.evaluate(
          (mutationId) => globalThis.__calqueSelfhostMutationState(mutationId),
          write.mutationId,
        );
        if (state !== "canonical" && state !== "awaiting_canonical_echo")
          throw new Error(`the page's write settled as ${String(state)}`);
        return state === "canonical";
      },
    );
    const written = await page.evaluate(() =>
      globalThis.__calqueSelfhostState(),
    );
    assert.ok(
      written.tasks.some(
        ([id, title]) => id === 1001 && title === renamedTitle,
      ),
      `the canonical write is not on the page: ${JSON.stringify(written.tasks)}`,
    );
    assert.match(
      psql(`SELECT title FROM tasks WHERE id = 1001;`),
      new RegExp(renamedTitle),
      "the page's write never reached PostgreSQL",
    );

    // A grant with no command capability has no write surface at all: the
    // route answers as an unmounted one would.
    const mutationRouteStatusWithoutCommand = (
      await edge(caddyRoot, httpsPort, "/v1/mutations", {
        method: "POST",
        headers: {
          authorization: `Bearer ${readOnly.token}`,
          "content-type": "application/json",
          origin: pageOrigin,
        },
        body: "{}",
      })
    ).status;
    assert.equal(
      mutationRouteStatusWithoutCommand,
      404,
      "a grant with no command capability reached the mutation route",
    );
    assert.deepEqual(
      [...first.origins],
      [serverBase],
      `the write sent requests outside the HTTPS edge: ${[...first.origins].join(", ")}`,
    );

    // ---- The two services are independent ----
    //
    // The two share a Unix socket, not a network namespace. `postgres` is
    // recreated rather than restarted, because a server in PostgreSQL's
    // network namespace would lose its listener with the database container.
    // The `calque` container must stay the same running container and keep
    // answering `/healthz`, which consults no dependency, on its own plaintext
    // port. A shared namespace would refuse the connection, so `ownPort`
    // reports that as a status the assertion can name instead of throwing.
    const ownPort = async (path: string): Promise<number | "unreachable"> => {
      try {
        return await plaintext(httpPort, path, PUBLIC_HOST);
      } catch {
        return "unreachable";
      }
    };

    const calqueContainerBefore = compose("ps", "-q", "calque").trim();
    assert.ok(
      calqueContainerBefore.length > 0,
      "the calque container is not running before the independence check",
    );
    compose("rm", "-sf", "postgres");
    assert.equal(
      await ownPort("/healthz"),
      204,
      "the server stopped answering on its own port when the database container was removed; the two services share a network life after all",
    );
    assert.equal(
      await probe("/healthz"),
      204,
      "the edge stopped reaching the server when the database container was removed",
    );
    assert.equal(
      compose("ps", "-q", "calque").trim(),
      calqueContainerBefore,
      "removing the database container took the server container with it",
    );
    // And the other direction: PostgreSQL comes back while the server
    // container keeps running.
    compose("up", "-d", "postgres");
    await waitFor(
      "PostgreSQL to accept connections again",
      120_000,
      async () => {
        psql("SELECT 1");
        return true;
      },
    );
    assert.equal(
      compose("ps", "-q", "calque").trim(),
      calqueContainerBefore,
      "the server container was replaced while PostgreSQL came back",
    );

    // The CDC runtime rejoins the recreated cluster on its own: the volume
    // kept the cluster, its slot and the fence lease row, so the runtime
    // reacquires its lease and reopens replication at the slot's confirmed
    // position.
    await waitFor(
      "readiness to return without recreating the server",
      120_000,
      async () => (await probe("/readyz")) === 204,
    );
    assert.equal(
      compose("ps", "-q", "calque").trim(),
      calqueContainerBefore,
      "the server container was replaced while it rejoined the cluster",
    );

    // Stopping the server within the composition's grace is an orderly
    // shutdown: the server that rejoined releases the lease it took back, and
    // leaves the slot idle, so its successor has nothing to wait out.
    const stopStarted = Date.now();
    compose("stop", "calque");
    const serverStopMs = Date.now() - stopStarted;
    assert.equal(
      run("docker", [
        "inspect",
        "-f",
        "{{.State.ExitCode}}",
        calqueContainerBefore,
      ]).trim(),
      "0",
      `the server did not exit cleanly within its stop grace (${String(serverStopMs)} ms)`,
    );
    assert.equal(
      sourceHold(),
      "unowned,expired,slot-idle",
      "the server stopped without releasing its lease and slot",
    );

    // Recreating the server alone brings the deployment back on the state
    // that is already there. Nothing else in the composition is touched.
    compose("up", "-d", "--force-recreate", "calque");
    const recreatedContainer = compose("ps", "-q", "calque").trim();
    assert.notEqual(
      recreatedContainer,
      calqueContainerBefore,
      "--force-recreate did not replace the server container",
    );
    await waitFor(
      "readiness after the server alone was recreated",
      READY_TIMEOUT_MS,
      async () => (await probe("/readyz")) === 204,
    );

    // A server killed outright releases nothing: its lease stays held until
    // it expires, one TTL after the last renewal. The next server waits that
    // out instead of refusing the source as someone else's.
    compose("kill", "calque");
    const killedAt = Date.now();
    assert.match(
      sourceHold(),
      /^active,unexpired,/,
      "the killed server's lease was not still held; the restart below would not have to wait for it",
    );
    compose("up", "-d", "--force-recreate", "calque");
    assert.notEqual(
      compose("ps", "-q", "calque").trim(),
      recreatedContainer,
      "--force-recreate did not replace the killed server container",
    );
    await waitFor(
      "readiness after the server was killed and recreated",
      READY_TIMEOUT_MS,
      async () => (await probe("/readyz")) === 204,
    );
    const readyAfterKillMs = Date.now() - killedAt;
    assert.ok(
      compose("logs", "--no-log-prefix", "calque").includes(
        "another process holds the source lease",
      ),
      "the server after the kill became ready without meeting the killed server's lease",
    );

    // ---- A recovery cycle under the S3 store ----
    //
    // Both halves of the deployment's state are destroyed below, the state
    // volume and every object under the bucket's prefix, so only the unit can
    // restore it.
    const artifactUrl = advanced.artifactRequests[0];
    if (artifactUrl === undefined)
      throw new Error("the page fetched no artifact");
    const bucketBeforeLoss = bucketObjectKeys();

    await stopSourceAndWait();
    const backupOracle = oracleOf(
      requireRecovery(
        "backup",
        "backup",
        "--output",
        "/recovery/unit",
        "--state-dir",
        "/var/lib/calque/state",
      ),
    );
    // Off the host's volumes, as an operator takes it: the archive on the host
    // is the only copy once the volumes below are gone.
    const unitArchive = join(workDirectory, "calque-unit.tar");
    writeFileSync(unitArchive, recoveryTar(["-c", "-C", "/recovery", "unit"]));
    const unitObjects = run("tar", ["-tf", unitArchive])
      .split("\n")
      .filter(
        (entry) =>
          entry.startsWith("unit/state/objects/") && !entry.endsWith("/"),
      ).length;
    assert.ok(
      unitObjects > 0,
      "the unit carries no artifact objects; restoring it would publish artifacts nothing can fetch",
    );

    // Lose both halves, and the volume the unit was written to.
    compose("rm", "-sf", "calque");
    run("docker", [
      "volume",
      "rm",
      "-f",
      `${project}_calque-state`,
      `${project}_calque-recovery`,
    ]);
    mc(
      "rm",
      "--recursive",
      "--force",
      `selfhost/${environment.CALQUE_S3_BUCKET ?? ""}/${environment.CALQUE_S3_PREFIX ?? ""}/objects/`,
    );
    assert.deepEqual(
      bucketObjectKeys(),
      [],
      "the bucket still holds objects; the restore below would not have to produce them",
    );

    // Restore from the unit alone, streamed back from the host onto a fresh
    // recovery volume. The control rows for this source are still in
    // PostgreSQL -- only the server's own state was lost -- so this is the
    // stopped-source replacement path.
    recoveryTar(["-x", "-C", "/recovery"], readFileSync(unitArchive));
    const restoredOracle = oracleOf(
      requireRecovery(
        "restore",
        "restore",
        "--backup",
        "/recovery/unit",
        "--state-dir",
        "/var/lib/calque/state",
        "--replace-source-rows",
      ),
    );
    assert.equal(restoredOracle, backupOracle);
    assert.equal(
      oracleOf(
        requireRecovery(
          "verify-restore",
          "verify-restore",
          "--backup",
          "/recovery/unit",
          "--state-dir",
          "/var/lib/calque/state",
        ),
      ),
      backupOracle,
    );
    const bucketAfterRestore = bucketObjectKeys();
    assert.equal(bucketAfterRestore.length, unitObjects);
    for (const key of bucketAfterRestore) {
      assert.ok(
        bucketBeforeLoss.includes(key),
        `the restore produced an object the deployment never had: ${key}`,
      );
    }

    // A second page, in its own context so nothing is served out of the first
    // one's storage, mounts the complete working set from the restored
    // deployment, fetching artifact objects that exist only because the unit
    // carried them.
    compose("up", "-d", "calque");
    await waitFor(
      "readiness after the restore",
      READY_TIMEOUT_MS,
      async () => (await probe("/readyz")) === 204,
    );
    const second = await openPage();
    const restoredPage = second.page;
    await waitFor(
      "the restored deployment to serve a complete working set",
      120_000,
      async () => {
        const state = await restoredPage.evaluate(() =>
          globalThis.__calqueSelfhostState(),
        );
        if (state.state === "error")
          throw new Error(
            `the page failed: ${state.failure ?? "unknown"}; failed requests: ${JSON.stringify(second.failedRequests)}`,
          );
        return (
          state.state === "ready" &&
          state.projects.length === SEEDED_PROJECTS &&
          state.tasks.length === SEEDED_TASKS
        );
      },
    );
    const restored = await restoredPage.evaluate(() =>
      globalThis.__calqueSelfhostState(),
    );
    assert.deepEqual(second.errors, []);
    assert.deepEqual(
      [...second.origins],
      [serverBase],
      `the restored page sent requests outside the HTTPS edge: ${[...second.origins].join(", ")}`,
    );
    assert.ok(
      restored.artifactRequests.length > 0,
      "the restored deployment was mounted without fetching an artifact object",
    );
    for (const url of restored.artifactRequests) {
      assert.ok(
        url.startsWith(`${serverBase}/v1/artifact-objects/`),
        `the page fetched an artifact from ${url}, not the restored server`,
      );
    }
    // The object the first page fetched is the object this one fetched: the
    // capability query differs on every bootstrap, the object id does not.
    const objectOf = (url: string): string =>
      new URL(url).pathname.split("/").at(-1) ?? "";
    assert.ok(
      restored.artifactRequests.some(
        (url) => objectOf(url) === objectOf(artifactUrl),
      ),
      `the restored deployment served no object the original did: ${restored.artifactRequests.join(", ")}`,
    );
    await restoredPage.close();

    // With the object half gone, `verify-restore` must name the missing object
    // rather than report a green oracle over a catalog whose artifacts nothing
    // can fetch.
    await stopSourceAndWait();
    mc(
      "rm",
      "--recursive",
      "--force",
      `selfhost/${environment.CALQUE_S3_BUCKET ?? ""}/${environment.CALQUE_S3_PREFIX ?? ""}/objects/`,
    );
    const refused = calqueRecovery(
      "verify-restore",
      "--backup",
      "/recovery/unit",
      "--state-dir",
      "/var/lib/calque/state",
    );
    assert.notEqual(
      refused.status,
      0,
      `an emptied bucket must not verify: ${refused.output}`,
    );
    const refusalLine = refused.output
      .split("\n")
      .map((line) => line.trim())
      .find((line) => line.includes("absent from the configured object store"));
    assert.ok(
      refusalLine !== undefined,
      `the refusal must name the missing object: ${refused.output}`,
    );

    // The edge was never restarted: every server and database recreation
    // above happened behind the same Caddy container.
    assert.equal(
      compose("ps", "-q", "caddy").trim(),
      caddyContainer,
      "the Caddy container was replaced during the run",
    );

    outcome = {
      result: "PASS",
      project,
      edge: serverBase,
      browserTrust: trust.mode,
      browserOrigins: [...new Set([...first.origins, ...second.origins])],
      plaintextListenerBrowserHostStatus,
      objectStore: "s3",
      bucketObjects: listing.length,
      containerFilesystemObjects: 0,
      startupMillis,
      serverStopMs,
      readyAfterKillMs,
      projects: advanced.projects.length,
      tasks: advanced.tasks.length,
      publicationsAfterSourceChange: advanced.publications,
      artifactRequests: advanced.artifactRequests.length,
      write: "canonical",
      mutationRouteStatusWithoutCommand,
      unitObjects,
      bucketObjectsAfterRestore: bucketAfterRestore.length,
      restoredOracleMatchesBackup: true,
      artifactRequestsAfterRestore: restored.artifactRequests.length,
      projectsAfterRestore: restored.projects.length,
      restoreRefusedWithoutTheObjectHalf: refusalLine,
    };
  } finally {
    await browser?.close();
    await pageServer.close();
  }
  return outcome;
}

let status = 0;
try {
  const outcome = await main();
  process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
} catch (error) {
  status = 1;
  process.stderr.write(`selfhost-compose-e2e: ${String(error)}\n`);
  try {
    process.stderr.write(compose("logs", "--tail", "120"));
  } catch {
    // The composition may never have started; the original error stands.
  }
} finally {
  // Always: no container, no volume, no image left behind.
  composeQuiet("down", "-v", "--remove-orphans");
  spawnSync("docker", [
    "image",
    "rm",
    "-f",
    environment.CALQUE_SELFHOST_IMAGE ?? "",
  ]);
  rmSync(workDirectory, { recursive: true, force: true });
}
process.exit(status);
