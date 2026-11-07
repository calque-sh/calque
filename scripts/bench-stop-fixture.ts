/**
 * A bench run's stop without Docker, PostgreSQL, calque, Chrome or sudo, for
 * the Ctrl-C tests in `bench.test.ts`, which run it through `pnpm run`.
 * It wires the stop as `main` in `bench.ts` does, with stand-ins: a
 * shaper that records its teardown instead of running sudo; a calque child in
 * its own process group that shuts down on SIGINT and finishes only once the
 * release file exists, so a test decides how long the stop lasts; and a shell
 * script for Chrome that never answers Playwright, launched by `launchBrowser`
 * so Playwright installs its process handlers.
 *
 * Usage: bench-stop-fixture.ts <run directory> <release file>
 * <record file>
 *
 * The record file gets `cleanup` each time the cleanup starts, `down` for the
 * shaper's teardown, and `exit <code>` when node exits on its own, never when
 * a signal ended it. The fixture prints `ready <its own pid> <calque pid>
 * <browser pid>` once both stand-ins have started, and the calque stand-in
 * prints `listening` once its SIGINT listener is installed.
 */
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { launchBrowser } from "./bench-launch.ts";
import { listenForStop, runStop } from "./bench-stop.ts";
import { startChild, stopChild, type ChildHandle } from "./demo.ts";

const [directory, releaseFile, recordFile] = process.argv.slice(2);
if (
  directory === undefined ||
  releaseFile === undefined ||
  recordFile === undefined
) {
  throw new Error(
    "usage: bench-stop-fixture.ts <run directory> <release file> <record file>",
  );
}
const record = (line: string) => appendFileSync(recordFile, `${line}\n`);

const CALQUE_STAND_IN = [
  'const { existsSync } = require("node:fs");',
  'process.on("SIGINT", () => {',
  '  console.log("shutting down");',
  "  setInterval(() => {",
  "    if (existsSync(process.argv[1])) process.exit(0);",
  "  }, 25);",
  "});",
  'console.log("listening");',
  "setInterval(() => {}, 1_000);",
].join("\n");

const BROWSER_STAND_IN = '#!/bin/sh\necho $$ > "$0.pid"\nexec sleep 600\n';

/** The pid a stand-in wrote to `path`, once it has written it. */
async function pidIn(path: string): Promise<number> {
  for (;;) {
    const text = existsSync(path) ? readFileSync(path, "utf8") : "";
    if (text.endsWith("\n")) return Number(text);
    await sleep(25);
  }
}

let shaperIsDown = false;
const shaper = {
  down: (): boolean | null => {
    if (shaperIsDown) return null;
    shaperIsDown = true;
    record("down");
    return true;
  },
};

mkdirSync(join(directory, "samples"), { recursive: true });
const children: ChildHandle[] = [];
const shaperDown = () => {
  shaper.down();
};
process.once("exit", shaperDown);
process.on("exit", (code) => record(`exit ${String(code)}`));
const cleanup = async () => {
  record("cleanup");
  shaperDown();
  await Promise.allSettled(children.map((child) => stopChild(child)));
};
const stop = runStop(
  directory,
  (aborted) => ({ runId: "stop-fixture", aborted }),
  cleanup,
);
listenForStop(stop);

let aborted: string | null = null;
try {
  const calque = startChild(
    "calque",
    process.execPath,
    ["-e", CALQUE_STAND_IN, releaseFile],
    process.env,
  );
  children.push(calque);
  const browser = join(directory, "browser-stand-in");
  writeFileSync(browser, BROWSER_STAND_IN, { mode: 0o755 });
  launchBrowser({ executablePath: browser }, true).catch(() => undefined);
  const browserPid = await pidIn(`${browser}.pid`);
  process.stdout.write(
    `ready ${String(process.pid)} ${String(calque.child.pid)} ${String(browserPid)}\n`,
  );
  // A run's body ends when what it waits on fails, as a sample does once the
  // stop has stopped calque.
  await calque.exited;
  throw new Error("calque exited");
} catch (error) {
  aborted = error instanceof Error ? error.message : String(error);
  process.exitCode = 1;
} finally {
  await stop.finish(aborted);
}
