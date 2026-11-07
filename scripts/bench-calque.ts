/** The `calque dev` server a run prepares and serves from, and the hold for its brotli artifact. */
import { spawn } from "node:child_process";
import { closeSync, existsSync, openSync, readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { BenchOptions } from "./bench-conditions.ts";
import { hrtimeMs } from "./bench-host.ts";
import { delay } from "./demo-process.ts";
import {
  type ChildHandle,
  configuredRelations,
  isCalqueReadinessResponse,
  runChecked,
} from "./demo.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export const CONFIG_PATH = join(
  root,
  "client/examples/incidents/calque.config.json",
);

const PREPARATION_TIMEOUT_MS = 900_000;

/** The longest a run holds its reference pass for a line of calque's log. */
const LOG_HOLD_TIMEOUT_MS = 300_000;

export interface CalqueServer {
  readonly handle: ChildHandle;
  readonly pid: number;
  /** The loopback address the bench itself reads `/readyz` from. */
  readonly endpoint: string;
  /** The address the browser reaches calque at: the edge's, when there is one. */
  readonly publicEndpoint: string;
  /** The loopback metrics listener the bench scrapes `/metrics` from. */
  readonly metricsEndpoint: string;
  readonly log: string;
}

export function startCalque(
  calque: string,
  stateDirectory: string,
  port: number,
  metricsPort: number,
  pageOrigin: string,
  publicBaseUrl: string | null,
  environment: NodeJS.ProcessEnv,
  log: string,
  recut: readonly string[],
): CalqueServer {
  const fd = openSync(log, "a");
  const child = spawn(
    calque,
    [
      "dev",
      "--state-dir",
      stateDirectory,
      ...configuredRelations(readFileSync(CONFIG_PATH, "utf8")).flatMap(
        (table) => ["--table", table],
      ),
      "--allow-origin",
      pageOrigin,
      "--port",
      String(port),
      "--metrics-port",
      String(metricsPort),
      ...(publicBaseUrl === null ? [] : ["--public-base-url", publicBaseUrl]),
      ...recut,
      "--acknowledge-disposable-source",
    ],
    { cwd: root, env: environment, stdio: ["ignore", fd, fd], detached: true },
  );
  closeSync(fd);
  if (child.pid === undefined) throw new Error("calque dev did not start");
  const exited = new Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>((resolveExit, rejectExit) => {
    child.once("error", rejectExit);
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
  });
  return {
    handle: { label: "calque", child, exited },
    pid: child.pid,
    endpoint: `http://127.0.0.1:${String(port)}`,
    publicEndpoint: publicBaseUrl ?? `http://127.0.0.1:${String(port)}`,
    metricsEndpoint: `http://127.0.0.1:${String(metricsPort)}`,
    log,
  };
}

/** Milliseconds from `startedMs` to the first 204 from `/readyz`, or a failure. */
export async function waitForReady(
  server: CalqueServer,
  startedMs: number,
): Promise<{ readonly ms: number } | { readonly failure: string }> {
  let exited: string | null = null;
  void server.handle.exited.then(({ code, signal }) => {
    exited = `calque dev exited (${String(code ?? signal)})`;
  });
  while (hrtimeMs() - startedMs < PREPARATION_TIMEOUT_MS) {
    if (exited) return { failure: exited };
    const ready = await fetch(`${server.endpoint}/readyz`, {
      signal: AbortSignal.timeout(1_000),
    })
      .then((response) => isCalqueReadinessResponse(response))
      .catch(() => false);
    if (ready) return { ms: hrtimeMs() - startedMs };
    await delay(25);
  }
  return {
    failure: `no 204 from /readyz within ${String(PREPARATION_TIMEOUT_MS)} ms`,
  };
}

interface LogHold {
  readonly text: string;
  /** From the hold's start to the line, the server's exit, or the timeout. */
  readonly heldMs: number;
  readonly logged: boolean;
}

/**
 * Waits until the log at `logPath` has a line containing `text`, for at most
 * `timeoutMs`, or until `exited` settles.
 */
export async function holdUntilLogged(
  logPath: string,
  exited: Promise<unknown>,
  text: string,
  timeoutMs = LOG_HOLD_TIMEOUT_MS,
): Promise<LogHold> {
  let ended = false;
  void exited.finally(() => {
    ended = true;
  });
  const started = hrtimeMs();
  for (;;) {
    const logged = readFileSync(logPath, "utf8").includes(text);
    const heldMs = hrtimeMs() - started;
    if (logged || ended || heldMs >= timeoutMs) return { text, heldMs, logged };
    await delay(100);
  }
}

export function destroyState(
  calque: string,
  stateDirectory: string,
  environment: NodeJS.ProcessEnv,
): void {
  if (existsSync(join(stateDirectory, "state.json")))
    runChecked(calque, ["destroy", "--state-dir", stateDirectory, "--yes"], {
      env: environment,
    });
  // A preparation that failed before writing its state leaves only files.
  rmSync(stateDirectory, { recursive: true, force: true });
}

/**
 * Matches calque's line for a cut's brotli artifact, plain or packed
 * (`packed-brotli-large-window artifact published`).
 */
export const BROTLI_PUBLISHED_LOG = "brotli-large-window artifact published";

/**
 * The log line a run waits for before its first sample: the one it was given,
 * or calque's brotli publication, so a run measures the object calque serves
 * once its encodes are done. A realtime-only run holds too: its page loads the
 * artifact.
 */
export function holdText(
  options: Pick<BenchOptions, "holdUntilLogged">,
): string {
  return options.holdUntilLogged ?? BROTLI_PUBLISHED_LOG;
}

/**
 * Why a run stops after `hold`, or null where it goes on. A run's own hold
 * that ended without its line would measure some other object than calque's
 * brotli one; a hold `--hold-until-logged` named is recorded either way.
 */
export function holdFailure(
  options: Pick<BenchOptions, "holdUntilLogged">,
  hold: LogHold,
): string | null {
  return hold.logged || options.holdUntilLogged !== null
    ? null
    : `calque did not log "${hold.text}" in ${(hold.heldMs / 1000).toFixed(0)} s, so the run has no brotli object of calque's, packed or plain, to measure; --hold-until-logged names another line to wait for`;
}
