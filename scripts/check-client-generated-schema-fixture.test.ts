import assert from "node:assert/strict";
import { cpSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  DESCRIPTOR,
  FIXTURES,
  driftedSchemaFixtures,
} from "./check-client-generated-schema-fixture.ts";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

test("a fixture that differs from the descriptor's render is named", async () => {
  const root = mkdtempSync(join(tmpdir(), "calque-schema-fixture-"));
  try {
    for (const file of [DESCRIPTOR, ...FIXTURES])
      cpSync(join(repositoryRoot, file), join(root, file));
    assert.deepEqual(await driftedSchemaFixtures(root), []);
    const [, drifted] = FIXTURES;
    writeFileSync(join(root, drifted), "export const schema = {};\n");
    assert.deepEqual(await driftedSchemaFixtures(root), [drifted]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
