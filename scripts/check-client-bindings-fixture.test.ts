import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";

import {
  checkPinnedBindings,
  PINNED_BINDINGS,
  renderPinnedBindings,
  repositoryRoot,
} from "./lib/client-bindings.ts";

const FIXTURE = {
  config: "tests/fixtures/config/incident-ops.config.json",
  output: "tests/fixtures/config/incident-ops.generated.ts",
} as const;
const START = "tests/fixtures/config/incident-ops.start.generated.ts";

function scratchRoot(): string {
  const root = mkdtempSync(join(tmpdir(), "calque-bindings-"));
  const config = join(root, FIXTURE.config);
  mkdirSync(dirname(config), { recursive: true });
  writeFileSync(config, readFileSync(join(repositoryRoot, FIXTURE.config)));
  return root;
}

test("the checked-in render passes and a drifted render fails", () => {
  const root = scratchRoot();
  for (const { path, bytes } of renderPinnedBindings(FIXTURE, root))
    writeFileSync(path, bytes);
  assert.equal(checkPinnedBindings([FIXTURE], root), 1);

  writeFileSync(join(root, START), "export const requiredTables = [];\n");
  assert.throws(
    () => checkPinnedBindings([FIXTURE], root),
    /^Error: generated client bindings drifted: tests\/fixtures\/config\/incident-ops\.start\.generated\.ts \(regenerate with pnpm exec calque-bindings --config tests\/fixtures\/config\/incident-ops\.config\.json --out tests\/fixtures\/config\/incident-ops\.generated\.ts\)$/u,
  );
});

test("a missing render names both files", () => {
  assert.throws(
    () => checkPinnedBindings([FIXTURE], scratchRoot()),
    /drifted: tests\/fixtures\/config\/incident-ops\.generated\.ts, tests\/fixtures\/config\/incident-ops\.start\.generated\.ts \(regenerate/u,
  );
});

test("every pinned render in the repository is current", () => {
  assert.equal(checkPinnedBindings(), PINNED_BINDINGS.length);
});
