/**
 * `run-node-tests.mjs` in a real subprocess against fixture suites, one per
 * way a run can fail without a failing test to show for it.
 *
 * This file runs under the runner it tests, so after changing the runner, run
 * it with node alone as well:
 *
 *   node --import ./scripts/register.mjs --test scripts/run-node-tests.test.ts
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import process from "node:process";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

const repositoryRoot = realpathSync(
  resolve(dirname(fileURLToPath(import.meta.url)), ".."),
);
// Real paths throughout: macOS's temporary directory sits behind the
// `/var` -> `/private/var` link, and the runner prints paths as node resolved
// them.
const fixtures = realpathSync(
  mkdtempSync(join(tmpdir(), "calque-run-node-tests-")),
);
after(() => {
  rmSync(fixtures, { recursive: true, force: true });
});

/** Writes one fixture suite and returns the glob the runner is given. */
function suite(name: string, files: Record<string, string>): string {
  const directory = join(fixtures, name);
  mkdirSync(directory, { recursive: true });
  for (const [file, source] of Object.entries(files))
    writeFileSync(join(directory, file), source);
  return `${directory}/**/*.test.ts`;
}

/** A fixture file's path as the runner prints it. */
const shown = (name: string, file: string): string =>
  relative(repositoryRoot, join(fixtures, name, file));

interface Run {
  status: number | null;
  stdout: string;
  stderr: string;
  elapsedMs: number;
}

/**
 * The runner, as `pnpm test:node` starts it, over `globs`, from `directory`.
 * A runner that fails to stop gets SIGTERM after 60 s and an "interrupted"
 * line no case expects.
 */
function runner(
  globs: readonly string[],
  environment: Record<string, string> = {},
  nodeArguments: readonly string[] = [],
  directory: string = repositoryRoot,
): Promise<Run> {
  const inherited = { ...process.env };
  // node --test marks every test file's process with NODE_TEST_CONTEXT, and a
  // `node --test` started with it set reports to a parent in v8 serialization
  // instead of running as a test runner of its own.
  delete inherited["NODE_TEST_CONTEXT"];
  // A `node --test` writing to a terminal sets FORCE_COLOR for its test files,
  // which would colour the plan these cases read through a pipe.
  delete inherited["FORCE_COLOR"];
  delete inherited["HARNESS_TEST_TIMEOUT_MS"];
  delete inherited["HARNESS_TEST_WALL_BUDGET_MS"];
  const started = performance.now();
  return new Promise((settle, reject) => {
    const child = spawn(
      process.execPath,
      [
        ...nodeArguments,
        join(repositoryRoot, "scripts", "run-node-tests.mjs"),
        ...globs,
      ],
      {
        cwd: directory,
        env: { ...inherited, ...environment },
        timeout: 60_000,
      },
    );
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (status) => {
      settle({
        status,
        stdout,
        stderr,
        elapsedMs: performance.now() - started,
      });
    });
  });
}

/** Every line the runner wrote about its own verdict, in order. */
const verdict = (run: Run): string[] =>
  run.stderr.split("\n").filter((line) => line.startsWith("run-node-tests: "));

const passing = `import { test } from "node:test";
test("a fixture test that passes", () => {});
`;

// A fixture test that finds its run's report through the command line of its
// parent, the `node --test` process the runner started, and does `damage` to
// it. Nothing else can reach the file: it lives in a fresh temporary directory
// named nowhere but in that command line.
const reportDamage = (
  damage: string,
): string => `import { execFileSync } from "node:child_process";
import { renameSync, unlinkSync, writeFileSync } from "node:fs";
import process from "node:process";
import { test } from "node:test";
test("a fixture test that damages its run's report", () => {
  const command = execFileSync("ps", ["-ww", "-o", "args=", "-p", String(process.ppid)], { encoding: "utf8" });
  const found = /--test-reporter-destination=(\\S+report\\.jsonl)/.exec(command);
  if (found === null) throw new Error("no report destination in " + command);
  const report = found[1];
  ${damage}
});
`;

const missingNode = join(fixtures, "no-node-here", "node");
const hideNode = join(fixtures, "hide-node.mjs");
writeFileSync(hideNode, `process.execPath = ${JSON.stringify(missingNode)};\n`);
const emptyDirectory = join(fixtures, "empty-working-directory");
mkdirSync(emptyDirectory);

// Started together, so the file takes as long as its slowest case.
const runs = {
  clean: runner([suite("clean", { "a.test.ts": passing })]),
  // No blank lines: node locates a test in the code oxc emits, which drops
  // them, not in this text, so a blank line would move the line it reports.
  failing: runner([
    suite("failing", {
      "a.test.ts": `import assert from "node:assert/strict";
import { test } from "node:test";
test("a fixture test that fails", () => { assert.equal(1, 2); });
test("a fixture test that passes beside it", () => {});
`,
    }),
  ]),
  noMatch: runner([`${fixtures}/no-such-directory/**/*.test.ts`]),
  noTestFile: runner([
    suite("no-test-file", {
      "a.test.ts": passing,
      "b.test.ts": "export const declaresNoTest = true;\n",
    }),
  ]),
  emptySuite: runner([
    suite("empty-suite", {
      "a.test.ts": passing,
      "b.test.ts": `import { describe } from "node:test";
describe("a fixture suite with no test in it", () => {});
`,
    }),
  ]),
  awaitsForever: runner(
    [
      suite("awaits-forever", {
        "a.test.ts": `import { test } from "node:test";
test("a fixture test that awaits forever", async () => { await new Promise(() => {}); });
`,
      }),
    ],
    { HARNESS_TEST_TIMEOUT_MS: "1000" },
  ),
  spins: runner(
    [
      suite("spins", {
        "a.test.ts": `import { test } from "node:test";
test("a fixture test that spins", () => { for (;;) {} });
`,
        "b.test.ts": passing,
      }),
    ],
    { HARNESS_TEST_WALL_BUDGET_MS: "2000" },
  ),
  throwsAtImport: runner([
    suite("throws-at-import", {
      "a.test.ts": `import { test } from "node:test";
throw new Error("a fixture file that throws at import");
test("never declared", () => {});
`,
    }),
  ]),
  noNode: runner([suite("no-node", { "a.test.ts": passing })], {}, [
    "--import",
    hideNode,
  ]),
  reportRemoved: runner([
    suite("report-removed", {
      "a.test.ts": reportDamage("unlinkSync(report);"),
    }),
  ]),
  reportEmptied: runner([
    suite("report-emptied", {
      "a.test.ts": reportDamage(
        'renameSync(report, report + ".moved"); writeFileSync(report, "");',
      ),
    }),
  ]),
  allSkipped: runner([
    suite("all-skipped", {
      "a.test.ts": `import { test } from "node:test";
test("a fixture test that is skipped", { skip: "a fixture" }, () => {});
test.todo("a fixture test that is todo");
`,
    }),
  ]),
  // From an empty directory: a regressed runner would hand node the flag and
  // no file, and node would look for tests in its working directory, which
  // from the repository root includes this file.
  flag: runner(["--test-skip-pattern=."], {}, [], emptyDirectory),
};

test("a clean run exits 0 and says what it judged", async () => {
  const run = await runs.clean;
  assert.deepEqual(verdict(run), [
    `run-node-tests: PASS -- 1 tests in 1 files matched by ${fixtures}/clean/**/*.test.ts; node exited 0 and every file declared a test`,
  ]);
  assert.equal(run.status, 0);
  assert.match(run.stdout, /^ℹ tests 1$/m);
  assert.match(run.stdout, /^ℹ pass 1$/m);
});

test("a failing test fails the run and is named with its file and line", async () => {
  const run = await runs.failing;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- node --test exited with code 1; 1 did not pass:",
  ]);
  assert.ok(
    run.stderr.includes(
      `\n    a fixture test that fails (${shown("failing", "a.test.ts")}:3): Expected values to be strictly equal:\n`,
    ),
    run.stderr,
  );
  assert.equal(run.status, 1);
  assert.match(run.stdout, /^ℹ fail 1$/m);
});

test("a glob matching nothing is refused, though node prints a complete plan and exits 0", async () => {
  const run = await runs.noMatch;
  assert.deepEqual(verdict(run), [
    `run-node-tests: FAIL -- no test ran: ${fixtures}/no-such-directory/**/*.test.ts matched no file that declares a test`,
  ]);
  assert.equal(run.status, 1);
  assert.match(run.stdout, /^ℹ tests 0$/m);
  assert.match(run.stdout, /^ℹ fail 0$/m);
});

test("a file that declares no test is refused, though node counts it as a passing test", async () => {
  const run = await runs.noTestFile;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- 1 test file(s) ran and declared no test, which node reports as a pass:",
  ]);
  assert.ok(
    run.stderr.includes(`\n    ${shown("no-test-file", "b.test.ts")}\n`),
    run.stderr,
  );
  assert.equal(run.status, 1);
  // Two tests, both passing: the real one and the file itself.
  assert.match(run.stdout, /^ℹ tests 2$/m);
  assert.match(run.stdout, /^ℹ pass 2$/m);
});

test("a file holding only an empty describe is refused", async () => {
  const run = await runs.emptySuite;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- 1 test file(s) ran and declared no test, which node reports as a pass:",
  ]);
  assert.ok(
    run.stderr.includes(`\n    ${shown("empty-suite", "b.test.ts")}\n`),
    run.stderr,
  );
  assert.equal(run.status, 1);
  assert.match(run.stdout, /^ℹ tests 1$/m);
  assert.match(run.stdout, /^ℹ suites 1$/m);
});

test("a test awaiting forever fails at the per-test timeout and is named, though node counts it cancelled and not failed", async () => {
  const run = await runs.awaitsForever;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- node --test exited with code 1; 1 did not pass:",
  ]);
  assert.ok(
    run.stderr.includes(
      `\n    a fixture test that awaits forever (${shown("awaits-forever", "a.test.ts")}:2): test timed out after 1000ms\n`,
    ),
    run.stderr,
  );
  assert.equal(run.status, 1);
  assert.match(run.stdout, /^ℹ fail 0$/m);
  assert.match(run.stdout, /^ℹ cancelled 1$/m);
});

test("a test spinning synchronously is stopped by the wall-clock budget and fails the run", async () => {
  const run = await runs.spins;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- the run exceeded its 2000 ms wall-clock budget and was killed.",
  ]);
  assert.equal(run.status, 1);
  assert.ok(run.elapsedMs >= 2000, `stopped after ${String(run.elapsedMs)} ms`);
});

test("a file that throws at import fails the run and is named as failing outside any test", async () => {
  const run = await runs.throwsAtImport;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- node --test exited with code 1; 1 did not pass:",
  ]);
  assert.ok(
    run.stderr.includes(
      `\n    ${shown("throws-at-import", "a.test.ts")}: the file failed outside any test -- it threw at import or its process exited non-zero\n`,
    ),
    run.stderr,
  );
  // The file's own stderr, which node relays into the spec report on stdout.
  assert.ok(
    run.stdout.includes("Error: a fixture file that throws at import"),
    run.stdout,
  );
  assert.equal(run.status, 1);
});

test("node failing to start fails the run", async () => {
  const run = await runs.noNode;
  assert.deepEqual(verdict(run), [
    `run-node-tests: FAIL -- node --test could not be started: Error: spawn ${missingNode} ENOENT`,
  ]);
  assert.equal(run.status, 1);
});

test("a report removed during the run fails it, because nothing then says whether a test ran", async () => {
  const run = await runs.reportRemoved;
  const lines = verdict(run);
  assert.equal(lines.length, 1, run.stderr);
  assert.match(
    lines[0] ?? "",
    /^run-node-tests: FAIL -- the machine-readable report could not be read \(Error: ENOENT: no such file or directory, open '[^']+report\.jsonl'\), so nothing says whether a test ran$/,
  );
  assert.equal(run.status, 1);
  // Node itself saw a clean run.
  assert.match(run.stdout, /^ℹ pass 1$/m);
});

test("a report with no summary fails the run", async () => {
  const run = await runs.reportEmptied;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- the machine-readable report has no summary, so node did not finish the run it reported and nothing says whether a test ran",
  ]);
  assert.equal(run.status, 1);
  assert.match(run.stdout, /^ℹ pass 1$/m);
});

test("a run in which every test was skipped or todo is refused", async () => {
  const run = await runs.allSkipped;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- no test ran: all 2 tests were skipped or todo",
  ]);
  assert.equal(run.status, 1);
  assert.match(run.stdout, /^ℹ pass 0$/m);
  assert.match(run.stdout, /^ℹ skipped 1$/m);
  assert.match(run.stdout, /^ℹ todo 1$/m);
});

test("a flag is refused rather than handed to node", async () => {
  const run = await runs.flag;
  assert.deepEqual(verdict(run), [
    "run-node-tests: FAIL -- --test-skip-pattern=. is not a file glob; this runner takes file globs only",
  ]);
  assert.equal(run.status, 1);
  assert.equal(run.stdout, "");
});
