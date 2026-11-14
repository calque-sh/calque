// What the doc walks share: the shell a reader starts from, the one bash that
// runs a document's blocks, and the processes they leave running.

import {
  spawn,
  spawnSync,
  type ChildProcess,
  type SpawnSyncReturns,
} from "node:child_process";
import { closeSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { connect as tcpConnect } from "node:net";
import { dirname, resolve } from "node:path";
import process from "node:process";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";

export const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

const STOP_GRACE_MS = 5_000;

/**
 * The shell a reader starts from: this environment without `CALQUE_*` and
 * without the names in `also`, which the document sets itself.
 */
export function readerEnvironment(
  also: readonly string[] = [],
): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {};
  for (const [name, value] of Object.entries(process.env))
    if (
      !name.startsWith("CALQUE_") &&
      !also.includes(name) &&
      value !== undefined
    )
      environment[name] = value;
  return environment;
}

export interface RunOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly input?: string;
  readonly cwd?: string;
}

export function run(
  command: string,
  args: readonly string[],
  options: RunOptions = {},
): SpawnSyncReturns<string> {
  return spawnSync(command, [...args], {
    cwd: options.cwd ?? root,
    encoding: "utf8",
    env: options.env ?? readerEnvironment(),
    ...(options.input === undefined ? {} : { input: options.input }),
    maxBuffer: 64 * 1024 * 1024,
  });
}

export function succeed(
  command: string,
  args: readonly string[],
  options: RunOptions = {},
): string {
  const result = run(command, args, options);
  if (result.status !== 0)
    throw new Error(
      `${command} ${args.join(" ")} exited ${String(result.status)}${result.error === undefined ? "" : ` (${result.error.message})`}\n${result.stdout}\n${result.stderr}`,
    );
  return result.stdout;
}

/** Whether something accepts connections on this loopback port. */
export async function listening(port: number): Promise<boolean> {
  return await new Promise((answer) => {
    const socket = tcpConnect({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      answer(true);
    });
    socket.once("error", () => answer(false));
  });
}

/** Stops a detached child's whole process group: SIGTERM, then SIGKILL. */
export async function stopGroup(
  child: ChildProcess | undefined,
): Promise<void> {
  if (child?.pid === undefined || child.exitCode !== null) return;
  const exited = new Promise<void>((done) => child.once("exit", () => done()));
  const signal = (name: NodeJS.Signals) => {
    try {
      process.kill(-(child.pid ?? 0), name);
    } catch {
      // The group is already gone.
    }
  };
  signal("SIGTERM");
  const timer = new Promise<"timeout">((done) =>
    setTimeout(() => done("timeout"), STOP_GRACE_MS),
  );
  if ((await Promise.race([exited, timer])) === "timeout") {
    signal("SIGKILL");
    await exited;
  }
}

export interface BlocksOptions {
  readonly blocks: readonly string[];
  /** Where the script is written. */
  readonly scriptPath: string;
  readonly cwd?: string;
  readonly env: NodeJS.ProcessEnv;
  readonly timeoutMs: number;
  /** Lines run before the first block, such as a shim for a command. */
  readonly preamble?: readonly string[];
  /** Which variables the blocks exported to hand back to the caller. */
  readonly exported: (name: string) => boolean;
  /** Tells the caller which child to stop on a signal, and when it is gone. */
  readonly track?: (child: ChildProcess | undefined) => void;
}

/**
 * Runs the blocks in one bash under `set -euo pipefail`, each exactly as the
 * document writes it, and returns the variables `exported` selects that the
 * blocks exported. The shell hands its environment back over fd 3 after the
 * last block, so nothing the walk exports is written to disk.
 */
export async function runBlocks(
  options: BlocksOptions,
): Promise<Record<string, string>> {
  const { blocks } = options;
  const script = [
    "set -euo pipefail",
    ...(options.preamble ?? []),
    ...blocks.flatMap((block, index) => [
      `printf '\\n== block %d of %d\\n' ${String(index + 1)} ${String(blocks.length)}`,
      block,
    ]),
    `node -p 'JSON.stringify(process.env)' >&3`,
    "",
  ].join("\n");
  writeFileSync(options.scriptPath, script);
  const child = spawn("bash", [options.scriptPath], {
    cwd: options.cwd ?? root,
    env: options.env,
    detached: true,
    stdio: ["ignore", "inherit", "inherit", "pipe"],
  });
  options.track?.(child);
  let exported = "";
  const environmentPipe = child.stdio[3] as Readable | null;
  environmentPipe?.setEncoding("utf8");
  environmentPipe?.on("data", (chunk: string) => (exported += chunk));
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    void stopGroup(child);
  }, options.timeoutMs);
  const [code, signal] = await new Promise<
    [number | null, NodeJS.Signals | null]
  >((done) => child.once("exit", (...exit) => done(exit)));
  clearTimeout(timer);
  options.track?.(undefined);
  if (timedOut)
    throw new Error(
      `the walk did not finish within ${String(options.timeoutMs / 60_000)} minutes`,
    );
  if (code !== 0)
    throw new Error(
      `the walk exited ${String(code)}${signal === null ? "" : ` on ${signal}`}`,
    );
  const environment = JSON.parse(exported) as Record<string, string>;
  return Object.fromEntries(
    Object.entries(environment).filter(([name]) => options.exported(name)),
  );
}

/** Starts one block in the background in its own process group, logging to `logFile`. */
export function startBlock(options: {
  readonly block: string;
  readonly env: NodeJS.ProcessEnv;
  readonly logFile: string;
  readonly cwd?: string;
}): ChildProcess {
  const log = openSync(options.logFile, "w");
  const child = spawn("bash", ["-c", options.block], {
    cwd: options.cwd ?? root,
    env: options.env,
    detached: true,
    stdio: ["ignore", log, log],
  });
  closeSync(log);
  return child;
}

/** The last lines of a log, or a note that there is none. */
export function logTail(file: string, lines = 40): string {
  try {
    return readFileSync(file, "utf8").split("\n").slice(-lines).join("\n");
  } catch {
    return "(no log)";
  }
}
