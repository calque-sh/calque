#!/usr/bin/env node
/**
 * `pnpm demo` as a first-time reader runs it: `calque` from its container
 * image, with no Rust toolchain. It builds this checkout's Dockerfile into a
 * local image, starts the launcher without `--from-source` so it runs that
 * image, drives the page through one source change with `pnpm demo:smoke`, and
 * checks what the image path adds to the from-source one: the server runs in
 * the image, its port is published on loopback only, and stopping the launcher
 * removes the container.
 *
 * Usage:
 *   node --import ./scripts/register.mjs scripts/demo-image-e2e.ts
 *
 * `CALQUE_DEMO_IMAGE` names an image to run instead of building one.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  availablePort,
  startDemo,
  stopDemo,
  type DemoProcess,
} from "./demo-process.ts";
import { runChecked } from "./demo.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

const image = process.env.CALQUE_DEMO_IMAGE ?? "calque-demo-e2e:local";
if (process.env.CALQUE_DEMO_IMAGE === undefined) {
  process.stdout.write(`Building ${image}\n`);
  const build = spawnSync("docker", ["build", "--tag", image, "."], {
    cwd: root,
    stdio: "inherit",
  });
  assert.equal(build.status, 0, "docker build of the Dockerfile failed");
}
// The launcher resolves the image from its environment, as a reader's does.
process.env.CALQUE_DEMO_IMAGE = image;

const containers = (all: boolean, serverPort: number): string[] =>
  runChecked("docker", [
    "ps",
    ...(all ? ["--all"] : []),
    "--filter",
    `name=calque-demo-${serverPort}-`,
    "--format",
    "{{.Image}} {{.Names}}",
  ])
    .stdout.split("\n")
    .filter((line) => line !== "");

const [uiPort, serverPort] = await Promise.all([
  availablePort(),
  availablePort(),
]);
let demo: DemoProcess | undefined;
try {
  demo = await startDemo(serverPort, uiPort, { fromSource: false });
  assert.match(demo.output(), /Fetching /u);
  assert.doesNotMatch(demo.output(), /Building calque/u);

  const running = containers(false, serverPort);
  assert.equal(running.length, 1, `calque runs in one container: ${running}`);
  assert.ok(running[0]?.startsWith(`${image} `), `${running[0]}`);

  // The server's only reachable address is the host's loopback.
  const published = runChecked("docker", [
    "compose",
    "port",
    "postgres",
    String(serverPort),
  ]).stdout.trim();
  assert.equal(published, `127.0.0.1:${serverPort}`);

  const smoke = spawnSync(
    "node",
    ["--import", "./scripts/register.mjs", "scripts/demo-smoke.ts"],
    {
      cwd: root,
      env: { ...process.env, CALQUE_DEMO_URL: demo.url },
      encoding: "utf8",
    },
  );
  assert.equal(
    smoke.status,
    0,
    `demo-smoke failed:\n${smoke.stdout}${smoke.stderr}`,
  );
  process.stdout.write(smoke.stdout);
} finally {
  if (demo) await stopDemo(demo);
}
assert.deepEqual(
  containers(true, serverPort),
  [],
  "stopping the launcher left the calque container behind",
);
process.stdout.write(
  `${JSON.stringify({ outcome: "passed", scenario: "demo-from-image", image })}\n`,
);
