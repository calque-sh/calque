#!/usr/bin/env node
// Refuses a normal dependency on `dataset-grant-issuer` from any other
// workspace crate. The application backend mints grants and Calque only
// verifies them, so a server binary contains no grant-minting code.
// Dev-dependencies are allowed: the verifier's negative vectors have to be
// validly signed.

import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export const ISSUER_CRATE = "dataset-grant-issuer";

/**
 * The slice of `cargo metadata --no-deps --format-version 1` this gate reads.
 */
export interface CargoDependency {
  readonly name: string;
  readonly kind?: string | null;
}

export interface CargoPackage {
  readonly name: string;
  readonly dependencies: readonly CargoDependency[];
}

export interface CargoMetadata {
  readonly packages: readonly CargoPackage[];
}

export function readWorkspaceMetadata(): CargoMetadata {
  const result = spawnSync(
    "cargo",
    ["metadata", "--no-deps", "--format-version", "1"],
    { cwd: repositoryRoot, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );
  if (result.status !== 0) {
    process.stderr.write(result.stderr ?? "");
    throw new Error("cargo metadata failed");
  }
  return JSON.parse(result.stdout) as CargoMetadata;
}

export function findForbiddenIssuerDependents(
  metadata: CargoMetadata,
): string[] {
  const violations: string[] = [];
  for (const pkg of metadata.packages) {
    if (pkg.name === ISSUER_CRATE) continue;
    for (const dependency of pkg.dependencies) {
      if (dependency.name !== ISSUER_CRATE) continue;
      // `kind` is null for a normal dependency, "dev" or "build" otherwise.
      if (dependency.kind !== null && dependency.kind !== undefined) continue;
      violations.push(pkg.name);
    }
  }
  return violations.sort();
}

function main(): void {
  const metadata = readWorkspaceMetadata();
  if (!metadata.packages.some((pkg) => pkg.name === ISSUER_CRATE)) {
    process.stderr.write(
      `Grant issuer linkage: FAIL\n- ${ISSUER_CRATE} is not a workspace member, so this gate verifies nothing\n`,
    );
    process.exit(1);
  }
  const violations = findForbiddenIssuerDependents(metadata);
  if (violations.length > 0) {
    process.stderr.write(
      `Grant issuer linkage: FAIL\n${violations
        .map(
          (name) =>
            `- ${name} links ${ISSUER_CRATE} as a normal dependency; a server build must not be able to mint grants`,
        )
        .join("\n")}\n`,
    );
    process.exit(1);
  }
  process.stdout.write(
    `Grant issuer linkage: PASS (no normal dependency on ${ISSUER_CRATE})\n`,
  );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) main();
