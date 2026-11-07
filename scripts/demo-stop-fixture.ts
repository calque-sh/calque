/**
 * `pnpm demo` without Docker, PostgreSQL or a cargo build, for the Ctrl-C
 * tests in `demo.test.ts`, which run it through `pnpm run`.
 *
 * It runs the launcher's own `superviseChildren` over a stand-in for calque
 * and the page's real preview command, each leading its own process group.
 * The stand-in shuts down on SIGINT, as calque does, and finishes only once
 * `release` exists in the directory, so a test decides how long calque's
 * shutdown lasts.
 *
 * Usage: demo-stop-fixture.ts <directory> [go | build]
 *
 * With `go`, it starts the page only once `go` exists in the directory. With
 * `build`, it first runs a build step through `runChecked`: a node stand-in
 * that writes its pid to `building` once it handles SIGINT, exits 130 on
 * SIGINT, and otherwise runs until a signal ends it.
 *
 * It prints `calque started {"launcher":<pid>,"calque":<pid>}`, then
 * `page started <pid>` and `ready` as the page starts and answers. When node
 * exits on its own it writes its exit code to `record` in the directory.
 */
import { once } from "node:events";
import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import {
  buildPageCommand,
  runChecked,
  startChild,
  superviseChildren,
  waitForHttp,
} from "./demo.ts";
import { availablePort } from "./demo-process.ts";

const [directory, mode] = process.argv.slice(2);
if (
  directory === undefined ||
  (mode !== undefined && mode !== "go" && mode !== "build")
) {
  throw new Error("usage: demo-stop-fixture.ts <directory> [go | build]");
}

const CALQUE_STAND_IN = [
  'const { existsSync } = require("node:fs");',
  'process.on("SIGINT", () => {',
  '  console.log("shutting down");',
  "  setInterval(() => {",
  "    if (existsSync(process.argv[1])) process.exit(0);",
  "  }, 25);",
  "});",
  'console.log("ready");',
  "setInterval(() => {}, 1_000);",
].join("\n");

const BUILD_STAND_IN = [
  'process.on("SIGINT", () => process.exit(130));',
  'require("node:fs").writeFileSync(process.argv[1], String(process.pid));',
  "setInterval(() => {}, 1_000);",
].join("\n");

function fileAppears(path: string): Promise<void> {
  return new Promise((resolveAppearance) => {
    const timer = setInterval(() => {
      if (!existsSync(path)) return;
      clearInterval(timer);
      resolveAppearance();
    }, 25);
  });
}

process.on("exit", (code) =>
  writeFileSync(join(directory, "record"), String(code)),
);

const uiPort = await availablePort();
await superviseChildren("Demo", async ({ adopt, stopping }) => {
  const calque = adopt(
    startChild(
      "calque",
      process.execPath,
      ["-e", CALQUE_STAND_IN, join(directory, "release")],
      process.env,
    ),
  );
  // Its first output, printed once its SIGINT handler is in place.
  const calqueOutput = calque.child.stdout;
  if (calqueOutput === null) throw new Error("calque has no stdout");
  await once(calqueOutput, "data");
  process.stdout.write(
    `calque started ${JSON.stringify({ launcher: process.pid, calque: calque.child.pid })}\n`,
  );
  if (mode === "go") await fileAppears(join(directory, "go"));
  if (mode === "build")
    runChecked(process.execPath, [
      "-e",
      BUILD_STAND_IN,
      join(directory, "building"),
    ]);
  const page = buildPageCommand(uiPort);
  const started = startChild(
    "browser",
    page.command,
    [...page.arguments_, "--outDir", directory],
    process.env,
    page.cwd,
  );
  process.stdout.write(`page started ${String(started.child.pid)}\n`);
  const ui = adopt(started);
  await waitForHttp(
    `http://127.0.0.1:${String(uiPort)}`,
    ui,
    60_000,
    (response) => response.ok,
  );
  process.stdout.write("ready\n");
  const exited = await Promise.race(
    [calque, ui].map((child) => child.exited.then(() => child)),
  );
  if (!stopping()) throw new Error(`${exited.label} exited unexpectedly`);
});
