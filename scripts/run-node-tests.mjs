#!/usr/bin/env node
// Runs the harness tests under `node --test` and fails unless node exited 0
// inside its wall-clock budget, at least one test ran, and every file declared
// a test. Plain JavaScript, so it needs none of the toolchain it reports on.
//
// Usage:
//   node scripts/run-node-tests.mjs            # what `pnpm test:node` runs
//   node scripts/run-node-tests.mjs <glob>...  # the same over other files
//
// Arguments are file globs only; a flag such as `--watch` is refused. The
// checks read a second reporter, `run-node-tests-reporter.mjs`, which node
// writes to a temporary file while `spec` prints to stdout.
//
// `--test-timeout` fires only for a test that yields, so a synchronous spin is
// bounded by the wall-clock budget, a SIGKILL from outside the process. Both
// bounds are sized for a heavily loaded host, because a bound that fires on a
// slow but correct run makes `pnpm verify` flaky; HARNESS_TEST_TIMEOUT_MS and
// HARNESS_TEST_WALL_BUDGET_MS override them.
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const number = (name, fallback) => {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0)
    throw new Error(`${name} must be a positive integer of milliseconds`);
  return value;
};

const PER_TEST_MS = number("HARNESS_TEST_TIMEOUT_MS", 600_000);
const WALL_BUDGET_MS = number("HARNESS_TEST_WALL_BUDGET_MS", 1_800_000);

const refuse = (reasons) => {
  for (const reason of reasons)
    process.stderr.write(`run-node-tests: FAIL -- ${reason}\n`);
  process.exit(1);
};

const requested = process.argv.slice(2);
const flag = requested.find((argument) => argument.startsWith("-"));
if (flag !== undefined)
  refuse([`${flag} is not a file glob; this runner takes file globs only`]);
const globs = requested.length > 0 ? requested : ["scripts/**/*.test.ts"];

const reportDirectory = mkdtempSync(join(tmpdir(), "calque-node-tests-"));
const reportPath = join(reportDirectory, "report.jsonl");
const reporterPath = fileURLToPath(
  new URL("run-node-tests-reporter.mjs", import.meta.url),
);

const shown = (path) =>
  path === null ? "an unnamed file" : relative(process.cwd(), path);

const readReport = () => {
  const entries = [];
  let summary;
  for (const line of readFileSync(reportPath, "utf8").split("\n")) {
    if (line === "") continue;
    const entry = JSON.parse(line);
    if (entry.kind === "summary") summary = entry.counts;
    else entries.push(entry);
  }
  return { entries, summary };
};

// Names the tests behind node's non-zero exit.
const unpassed = (report) => {
  const lines = (report?.entries ?? [])
    .filter((entry) => !entry.passed)
    .map((entry) =>
      entry.fileLevel
        ? `    ${shown(entry.entryFile)}: the file failed outside any test -- it threw at import or its process exited non-zero`
        : `    ${entry.name} (${shown(entry.entryFile)}:${String(entry.line)}): ${String(entry.reason)}`,
    );
  return lines.length === 0
    ? "; the report names no test that did not pass"
    : `; ${String(lines.length)} did not pass:\n${lines.join("\n")}`;
};

// What node's exit code cannot say: whether anything was tested at all.
const judgeReport = (report) => {
  const reasons = [];
  const tests = report.summary?.tests;
  if (tests === undefined)
    reasons.push(
      "the machine-readable report has no summary, so node did not finish the run it reported and nothing says whether a test ran",
    );
  else if (tests === 0)
    reasons.push(
      `no test ran: ${globs.join(" ")} matched no file that declares a test`,
    );
  else if (report.summary.skipped + report.summary.todo === tests)
    reasons.push(
      `no test ran: all ${String(tests)} tests were skipped or todo`,
    );
  const files = new Map();
  for (const entry of report.entries) {
    const file = files.get(entry.entryFile) ?? { tests: 0, failed: false };
    if (entry.kind === "test" && !entry.fileLevel) file.tests += 1;
    if (!entry.passed) file.failed = true;
    files.set(entry.entryFile, file);
  }
  // A file that failed is already named by the exit-code line.
  const silent = [...files]
    .filter(([, file]) => file.tests === 0 && !file.failed)
    .map(([path]) => `    ${shown(path)}`);
  if (silent.length > 0)
    reasons.push(
      `${String(silent.length)} test file(s) ran and declared no test, which node reports as a pass:\n${silent.join("\n")}`,
    );
  return {
    reasons,
    passed: `${String(tests)} tests in ${String(files.size)} files matched by ${globs.join(" ")}; node exited 0 and every file declared a test`,
  };
};

let concluded = false;
let killedForTime = false;
let timer;

const conclude = ({ spawnError = null, code = null, signal = null }) => {
  if (concluded) return;
  concluded = true;
  clearTimeout(timer);
  const reasons = [];
  let passed;
  if (spawnError !== null)
    reasons.push(`node --test could not be started: ${String(spawnError)}`);
  else if (killedForTime)
    reasons.push(
      `the run exceeded its ${String(WALL_BUDGET_MS)} ms wall-clock budget and was killed.\n` +
        "  A test is hung. --test-timeout cannot stop a synchronous spin, which is why this bound exists.\n" +
        "  Node reports nothing for a run killed this way, so rerun one file at a time to find it:\n" +
        "    node scripts/run-node-tests.mjs scripts/<file>.test.ts\n" +
        "  Raise HARNESS_TEST_WALL_BUDGET_MS if this was a genuinely slow run rather than a hang.",
    );
  else if (signal !== null) reasons.push(`node --test was killed by ${signal}`);
  else {
    // A killed run's report is cut off wherever the kill landed, so only a
    // finished run's is read.
    let report = null;
    let unreadable = null;
    try {
      report = readReport();
    } catch (cause) {
      unreadable = cause;
    }
    if (code !== 0)
      reasons.push(
        `node --test exited with code ${String(code)}${unpassed(report)}`,
      );
    if (report === null)
      reasons.push(
        `the machine-readable report could not be read (${String(unreadable)}), so nothing says whether a test ran`,
      );
    else {
      const judged = judgeReport(report);
      reasons.push(...judged.reasons);
      passed = judged.passed;
    }
  }
  rmSync(reportDirectory, { recursive: true, force: true });
  if (reasons.length > 0) refuse(reasons);
  process.stderr.write(`run-node-tests: PASS -- ${passed}\n`);
  process.exit(0);
};

// `spawn` throws instead of emitting `error` for a few failures, E2BIG among
// them. That stays uncaught: node exits 1 naming the cause, which fails
// closed.
const child = spawn(
  process.execPath,
  [
    "--import",
    "./scripts/register.mjs",
    `--test-timeout=${String(PER_TEST_MS)}`,
    "--test",
    "--test-reporter=spec",
    "--test-reporter-destination=stdout",
    `--test-reporter=${reporterPath}`,
    `--test-reporter-destination=${reportPath}`,
    ...globs,
  ],
  // Its own process group, so the kill below also reaches the per-file
  // subprocesses the test runner spawns, where a spinning test lives.
  { stdio: "inherit", detached: true },
);

const killGroup = (signal) => {
  try {
    process.kill(-child.pid, signal);
  } catch {
    child.kill(signal);
  }
};

timer = setTimeout(() => {
  killedForTime = true;
  killGroup("SIGKILL");
}, WALL_BUDGET_MS);

// Without this a Ctrl-C leaves the detached group spinning.
for (const signal of ["SIGINT", "SIGTERM"])
  process.on(signal, () => {
    clearTimeout(timer);
    killGroup("SIGKILL");
    rmSync(reportDirectory, { recursive: true, force: true });
    refuse([
      `interrupted by ${signal}; the run was killed and is not a result`,
    ]);
  });

child.on("error", (cause) => conclude({ spawnError: cause }));
child.on("exit", (code, signal) => conclude({ code, signal }));
