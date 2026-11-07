import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import { emittedJavaScript } from "./verify-no-emitted-javascript.ts";

test("a .js beside its .ts is found, and a shadowed config is named", () => {
  const root = mkdtempSync(join(tmpdir(), "calque-emitted-"));
  try {
    for (const file of [
      "tools/gate.ts",
      "tools/gate.js",
      "app/eslint.config.ts",
      "app/eslint.config.js",
      "app/src/view.tsx",
      "app/src/view.mjs",
      // Not output: a .js with no TypeScript of its name, and a compiled
      // pair under a directory that is not the source tree.
      "tools/register.mjs",
      "app/dist/index.ts",
      "app/dist/index.js",
      "node_modules/pkg/index.ts",
      "node_modules/pkg/index.js",
    ]) {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      writeFileSync(join(root, file), "\n");
    }
    assert.deepEqual(emittedJavaScript(root), [
      {
        emitted: "app/eslint.config.js",
        source: "app/eslint.config.ts",
        shadows: true,
      },
      {
        emitted: "app/src/view.mjs",
        source: "app/src/view.tsx",
        shadows: false,
      },
      {
        emitted: "tools/gate.js",
        source: "tools/gate.ts",
        shadows: false,
      },
    ]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
