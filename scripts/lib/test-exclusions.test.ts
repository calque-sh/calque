import assert from "node:assert/strict";
import { test } from "node:test";

import { rustIgnored, typeScriptSkipped } from "./test-exclusions.ts";

// Each gated form below is assembled from parts, so that the report this
// module feeds does not list this file's own fixtures as skipped tests.
const SKIP = "skip";
const TODO = "todo";

const reasons = (source: string) =>
  typeScriptSkipped(source, "a.test.ts").map(({ path, name, reason }) => [
    path,
    name,
    reason,
  ]);

test("a skip or todo in node:test's options is reported with its expression as the reason", () => {
  const source = [
    `test("macOS", { ${SKIP}: process.platform !== "darwin" && "macOS clocks only" }, () => {});`,
    "test(",
    '  "encodes",',
    `  { ${SKIP}: noEncoder },`,
    "  async () => {},",
    ");",
    `test("later", { timeout: 5, ${TODO}: "waits on the server" }, () => {});`,
  ].join("\n");
  assert.deepEqual(reasons(source), [
    [
      "a.test.ts:1",
      "options.skip",
      'process.platform !== "darwin" && "macOS clocks only"',
    ],
    ["a.test.ts:4", "options.skip", "noEncoder"],
    ["a.test.ts:7", "options.todo", "waits on the server"],
  ]);
});

test("declaration and context forms are reported in source order", () => {
  const source = [
    `test("gated", (t) => { t.${SKIP}("no brotli"); });`,
    `test.${SKIP}("disabled", () => {});`,
    `t.${SKIP}();`,
  ].join("\n");
  assert.deepEqual(reasons(source), [
    ["a.test.ts:1", "t.skip", "no brotli"],
    [
      "a.test.ts:2",
      "test.skip",
      "source contains an explicit skipped/conditional test declaration",
    ],
    ["a.test.ts:3", "t.skip", "no reason recorded"],
  ]);
});

test("a mention in prose is not a skipped test", () => {
  assert.deepEqual(
    reasons(`// a test may pass ${SKIP} in its options, or call t.${SKIP}`),
    [],
  );
});

test("a Rust ignore is reported with its reason, or none", () => {
  const source = [
    "//! These tests are `#[ignore]`d and run by hand.",
    "#[tokio::test]",
    '#[ignore = "needs PostgreSQL"]',
    "async fn reads_the_wal() {}",
    "#[ignore]",
    "fn slow() {}",
  ].join("\n");
  assert.deepEqual(
    rustIgnored(source, "lib.rs").map(({ path, name, reason }) => [
      path,
      name,
      reason,
    ]),
    [
      ["lib.rs:3", "reads_the_wal", "needs PostgreSQL"],
      ["lib.rs:5", "slow", "no reason recorded"],
    ],
  );
});
