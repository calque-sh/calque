// The machine-readable half of `pnpm test:node`'s report. `run-node-tests.mjs`
// passes it to `node --test` beside `spec` and reads its file once node exits,
// because node's exit code cannot tell a run that tested nothing from one that
// passed. One JSON object per line:
//
//   {"kind":"test"|"suite", ...}  every finished test and suite
//   {"kind":"summary", ...}       node's run-wide counts, written last
//
// A run killed before it finished has no summary line.
import { resolve } from "node:path";

export default async function* runNodeTestsReporter(source) {
  for await (const event of source) {
    if (event.type === "test:summary") {
      // Each isolated file's child sends its own summary, carrying `file`.
      // The run-wide one has none.
      if (event.data.file !== undefined) continue;
      yield `${JSON.stringify({ kind: "summary", counts: event.data.counts })}\n`;
      continue;
    }
    if (event.type !== "test:pass" && event.type !== "test:fail") continue;
    const { data } = event;
    yield `${JSON.stringify({
      // Suites are recorded so that a file whose only content is an empty
      // `describe` still shows up as a file that ran, and declared no test.
      kind: data.details?.type === "suite" ? "suite" : "test",
      passed: event.type === "test:pass",
      name: data.name,
      // The test file the test ran under. `file` alone is where `test()` was
      // called, which for a test declared in a shared helper is the helper.
      entryFile: data.entryFile ?? data.file ?? null,
      line: data.line ?? null,
      // A test node reports for a whole file that declared no test or failed
      // outside any test. It is named after the file's path and lacks
      // `entryFile`, because node's parent process emits it itself.
      fileLevel:
        data.entryFile === undefined &&
        data.file !== undefined &&
        data.file === resolve(data.name),
      reason:
        event.type === "test:fail"
          ? (String(data.details?.error?.message ?? "")
              .split("\n")
              .find((line) => line.trim() !== "") ?? "no error message")
          : null,
    })}\n`;
  }
}
