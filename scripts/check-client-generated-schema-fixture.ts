import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { renderClientSchemaFixture } from "./lib/client-schema-fixture-renderer.ts";

export const DESCRIPTOR =
  "client/packages/client/tests/fixtures/schema-descriptor.json";
export const FIXTURES = [
  "client/packages/client/tests/calque.generated.ts",
  "client/packages/client/tests/browser-consumer/calque.generated.ts",
] as const;

/**
 * The checked-in fixtures under `root` that differ from what the renderer
 * makes of the descriptor. It renders twice, from two parses, and throws if
 * the renders differ.
 */
export async function driftedSchemaFixtures(root: string): Promise<string[]> {
  const descriptorPath = join(root, DESCRIPTOR);
  const first = await renderClientSchemaFixture(
    JSON.parse(readFileSync(descriptorPath, "utf8")),
  );
  const second = await renderClientSchemaFixture(
    JSON.parse(readFileSync(descriptorPath, "utf8")),
  );
  if (first !== second)
    throw new Error("generated client schema renderer is not deterministic");
  return FIXTURES.filter(
    (fixture) => readFileSync(join(root, fixture), "utf8") !== first,
  );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const drifted = await driftedSchemaFixtures(
    resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  );
  if (drifted.length > 0)
    throw new Error(
      `generated client schema fixture drifted: ${drifted.join(", ")}`,
    );
  process.stdout.write(
    "Client generated schema fixture drift: PASS (2 deterministic renders, 2 checked-in fixtures)\n",
  );
}
