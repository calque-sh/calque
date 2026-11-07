import { Buffer } from "node:buffer";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const repositoryRoot = resolve(packageRoot, "../../..");
const fixtureRoot = join(repositoryRoot, "spec/fixtures/protocol-v1");
const temporaryRoot = mkdtempSync(join(packageRoot, ".fixture-check-"));
const producers = ["rust", "typescript"];

function run(command, arguments_, cwd = repositoryRoot) {
  const result = spawnSync(command, arguments_, {
    cwd,
    encoding: "utf8",
    stdio: "pipe",
  });
  if (result.status !== 0) {
    process.stderr.write(result.stdout);
    process.stderr.write(result.stderr);
    throw new Error(`${command} exited with status ${String(result.status)}`);
  }
}

function canonicalHex(path) {
  const text = readFileSync(path, "utf8").trim();
  if (
    text.length === 0 ||
    text.length % 2 !== 0 ||
    !/^[0-9a-f]+$/u.test(text)
  ) {
    throw new Error(`${path} is not canonical lowercase hex`);
  }
  return Buffer.from(text, "hex");
}

function files(path) {
  return readdirSync(path)
    .filter((name) => name.endsWith(".hex"))
    .sort();
}

try {
  const generatedRust = join(temporaryRoot, "rust");
  const generatedTypeScript = join(temporaryRoot, "typescript");
  // This checkout's own target directory: with a `CARGO_TARGET_DIR` that
  // other worktrees share, cargo could run another worktree's generator.
  run("cargo", [
    "run",
    "--target-dir",
    join(repositoryRoot, "target"),
    "-p",
    "sync-protocol",
    "--example",
    "generate_protocol_v1_fixtures",
    "--",
    generatedRust,
  ]);
  run(
    "node",
    ["scripts/generate-protocol-v1-fixtures.mjs", generatedTypeScript],
    packageRoot,
  );

  const expectedNames = files(generatedRust);
  if (
    JSON.stringify(files(generatedTypeScript)) !== JSON.stringify(expectedNames)
  ) {
    throw new Error(
      "Rust and TypeScript fixture generators emitted different file sets",
    );
  }

  const manifest = JSON.parse(
    readFileSync(join(fixtureRoot, "manifest.json"), "utf8"),
  );
  if (manifest.version !== 1 || !Array.isArray(manifest.fixtures)) {
    throw new Error("protocol-v1 fixture manifest has an invalid shape");
  }
  const manifestByName = new Map(
    manifest.fixtures.map((entry) => [entry.file, entry]),
  );
  if (manifestByName.size !== expectedNames.length) {
    throw new Error(
      "protocol-v1 fixture manifest has missing or duplicate entries",
    );
  }

  for (const name of expectedNames) {
    const generated = new Map(
      producers.map((producer) => [
        producer,
        canonicalHex(join(temporaryRoot, producer, name)),
      ]),
    );
    const rust = generated.get("rust");
    const typescript = generated.get("typescript");
    if (!rust.equals(typescript)) {
      throw new Error(
        `${name} differs between independent Rust and TypeScript generation`,
      );
    }
    for (const producer of producers) {
      const committed = canonicalHex(join(fixtureRoot, producer, name));
      if (!committed.equals(generated.get(producer))) {
        throw new Error(`${producer}/${name} is stale`);
      }
    }
    const entry = manifestByName.get(name);
    if (!entry) throw new Error(`manifest is missing ${name}`);
    const digest = createHash("sha256").update(rust).digest("hex");
    if (entry.bytes !== rust.byteLength || entry.sha256 !== digest) {
      throw new Error(`${name} manifest length or SHA-256 is stale`);
    }
    if (!Array.isArray(entry.coverage) || entry.coverage.length === 0) {
      throw new Error(`${name} has no declared semantic coverage`);
    }
  }

  const invalidNames = files(join(generatedRust, "invalid"));
  if (
    JSON.stringify(files(join(generatedTypeScript, "invalid"))) !==
    JSON.stringify(invalidNames)
  ) {
    throw new Error(
      "Rust and TypeScript generators emitted different invalid fixture sets",
    );
  }
  const negativeByName = new Map(
    (manifest.negative_fixtures ?? []).map((entry) => [entry.file, entry]),
  );
  if (negativeByName.size !== invalidNames.length) {
    throw new Error(
      "protocol-v1 manifest has missing or duplicate invalid fixtures",
    );
  }
  for (const name of invalidNames) {
    const generated = new Map(
      producers.map((producer) => [
        producer,
        canonicalHex(join(temporaryRoot, producer, "invalid", name)),
      ]),
    );
    const rust = generated.get("rust");
    if (!rust.equals(generated.get("typescript"))) {
      throw new Error(
        `invalid/${name} differs between independent Rust and TypeScript generation`,
      );
    }
    for (const producer of producers) {
      const committed = canonicalHex(
        join(fixtureRoot, producer, "invalid", name),
      );
      if (!committed.equals(generated.get(producer))) {
        throw new Error(`${producer}/invalid/${name} is stale`);
      }
    }
    const entry = negativeByName.get(name);
    if (!entry) throw new Error(`manifest is missing invalid/${name}`);
    const digest = createHash("sha256").update(rust).digest("hex");
    if (entry.bytes !== rust.byteLength || entry.sha256 !== digest) {
      throw new Error(`invalid/${name} manifest length or SHA-256 is stale`);
    }
    if (
      typeof entry.expectation !== "string" ||
      entry.expectation.length === 0
    ) {
      throw new Error(`invalid/${name} has no executable expectation`);
    }
  }

  const requiredRecipes = new Set([
    "framing",
    "unknown-semantics",
    "integrity",
    "primary-key-canonicality",
    "duplicates",
    "stream-order-fragmentation",
    "exact-public-limits",
  ]);
  for (const recipe of manifest.negative_recipes ?? [])
    requiredRecipes.delete(recipe.id);
  if (requiredRecipes.size > 0) {
    throw new Error(
      `manifest is missing negative recipes: ${[...requiredRecipes].join(", ")}`,
    );
  }
} finally {
  rmSync(temporaryRoot, { recursive: true, force: true });
}
