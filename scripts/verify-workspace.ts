import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

import { requireNodeCapabilities } from "./lib/node-capabilities.ts";
import {
  rustIgnored,
  typeScriptSkipped,
  type TestExclusion,
} from "./lib/test-exclusions.ts";
import { workspaceBuildOrder } from "./lib/workspace-build-order.ts";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

interface WorkspacePackage {
  readonly name: string;
  readonly scripts: Record<string, string | undefined>;
  /** Every dependency name the manifest declares, in any dependency field. */
  readonly dependencies: readonly string[];
}

/** The `pnpm -r list --json` entries the inventory reads. */
interface PnpmListEntry {
  readonly name: string;
  readonly path: string;
}

/** The `cargo metadata --no-deps` fields the inventory reads. */
interface CargoWorkspaceMetadata {
  readonly workspace_members: readonly string[];
  readonly packages: readonly { readonly id: string; readonly name: string }[];
}

// Checked first, so a Node that lacks a capability fails here by name rather
// than at its first symptom.
requireNodeCapabilities();
const requiredPackageScripts = ["typecheck", "build", "lint", "test"];
const arguments_ = process.argv.slice(2);
const sourceOnly = arguments_.includes("--source-only");
const unknownArguments = arguments_.filter(
  (argument) => argument !== "--source-only",
);
if (unknownArguments.length > 0) {
  throw new Error(
    `unsupported workspace verification argument: ${unknownArguments.join(", ")}`,
  );
}

function commandText(command: string, arguments_: readonly string[]): string {
  return [command, ...arguments_].join(" ");
}

function capture(command: string, arguments_: readonly string[]): string {
  const result = spawnSync(command, [...arguments_], {
    cwd: repositoryRoot,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(
      `workspace inventory command failed: ${commandText(command, arguments_)}`,
    );
  }
  return result.stdout;
}

/** Each step's wall time, in the order the steps ran. */
const stepTimes: { readonly label: string; readonly seconds: number }[] = [];

function run(
  label: string,
  command: string,
  arguments_: readonly string[],
): void {
  process.stdout.write(
    `\n==> ${label}\n$ ${commandText(command, arguments_)}\n`,
  );
  const started = performance.now();
  const result = spawnSync(command, [...arguments_], {
    cwd: repositoryRoot,
    stdio: "inherit",
  });
  const seconds = (performance.now() - started) / 1_000;
  stepTimes.push({ label, seconds });
  process.stdout.write(`<== ${label}: ${seconds.toFixed(1)} s\n`);
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

/** The slowest steps and the total, so a slow gate is seen where it ran. */
function stepTimeReport(): void {
  const total = stepTimes.reduce((sum, step) => sum + step.seconds, 0);
  const slowest = [...stepTimes]
    .sort((a, b) => b.seconds - a.seconds)
    .slice(0, 10);
  process.stdout.write(
    `\n==> Step times: ${total.toFixed(1)} s over ${String(stepTimes.length)} steps; the slowest:\n`,
  );
  for (const step of slowest)
    process.stdout.write(
      `${step.seconds.toFixed(1).padStart(8)} s  ${step.label}\n`,
    );
}

function workspaceInventory(): WorkspacePackage[] {
  const pnpmPackages = JSON.parse(
    capture("pnpm", ["-r", "list", "--depth", "-1", "--json"]),
  ) as PnpmListEntry[];
  const typescriptPackages = pnpmPackages
    .filter((entry) => resolve(entry.path) !== repositoryRoot)
    .map((entry): WorkspacePackage => {
      const manifestPath = join(entry.path, "package.json");
      const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as {
        scripts?: Record<string, string | undefined>;
        dependencies?: Record<string, string>;
        devDependencies?: Record<string, string>;
        peerDependencies?: Record<string, string>;
        optionalDependencies?: Record<string, string>;
      };
      for (const script of requiredPackageScripts) {
        if (typeof manifest.scripts?.[script] !== "string") {
          throw new Error(
            `${entry.name} is a pnpm workspace package but has no ${script} script`,
          );
        }
      }
      return {
        name: entry.name,
        scripts: manifest.scripts ?? {},
        dependencies: [
          manifest.dependencies,
          manifest.devDependencies,
          manifest.peerDependencies,
          manifest.optionalDependencies,
        ].flatMap((field) => Object.keys(field ?? {})),
      };
    })
    .sort((left, right) => left.name.localeCompare(right.name));

  const cargoMetadata = JSON.parse(
    capture("cargo", ["metadata", "--no-deps", "--format-version", "1"]),
  ) as CargoWorkspaceMetadata;
  const workspaceMembers = new Set(cargoMetadata.workspace_members);
  const rustPackages = cargoMetadata.packages
    .filter((entry) => workspaceMembers.has(entry.id))
    .map((entry) => entry.name)
    .sort();

  if (typescriptPackages.length === 0 || rustPackages.length === 0) {
    throw new Error(
      "workspace discovery returned an empty Rust or TypeScript workspace",
    );
  }
  const protocol = typescriptPackages.find(
    (entry) => entry.name === "@calque/protocol",
  );
  if (typeof protocol?.scripts["generate:check"] !== "string") {
    throw new Error("@calque/protocol must expose generate:check");
  }

  process.stdout.write(
    [
      "Workspace inventory (no package may be silently omitted):",
      `  Rust (${rustPackages.length}): ${rustPackages.join(", ")}`,
      `  TypeScript (${typescriptPackages.length}): ${typescriptPackages
        .map((entry) => entry.name)
        .join(", ")}`,
      `  Hosted CI: ${
        existsSync(join(repositoryRoot, ".github/workflows/ci.yml"))
          ? ".github/workflows/ci.yml; on every push to main and pull request it runs this gate, the browser end-to-end scripts, the self-hosted composition behind Caddy and blob-store's MinIO conformance, and on pushes to main and by dispatch the ignored live-PostgreSQL suite."
          : "NOT CONFIGURED; this local gate is authoritative until repository hosting is available."
      }`,
      "",
    ].join("\n"),
  );
  return typescriptPackages;
}

function walkFiles(
  directory: string,
  extensions: readonly string[],
  files: string[] = [],
): string[] {
  for (const entry of readdirSync(directory)) {
    if (["node_modules", "dist", "target", ".git"].includes(entry)) continue;
    const path = join(directory, entry);
    const stats = statSync(path);
    if (stats.isDirectory()) walkFiles(path, extensions, files);
    else if (extensions.some((extension) => path.endsWith(extension)))
      files.push(path);
  }
  return files;
}

function skippedTestReport(): void {
  const ignored: TestExclusion[] = [];
  for (const path of walkFiles(join(repositoryRoot, "server"), [".rs"]))
    ignored.push(
      ...rustIgnored(
        readFileSync(path, "utf8"),
        relative(repositoryRoot, path),
      ),
    );
  // `scripts` holds the harness suite, so both trees are walked.
  const skipped: TestExclusion[] = [];
  for (const directory of ["client", "scripts"])
    for (const path of walkFiles(join(repositoryRoot, directory), [
      ".ts",
      ".tsx",
    ]))
      skipped.push(
        ...typeScriptSkipped(
          readFileSync(path, "utf8"),
          relative(repositoryRoot, path),
        ),
      );

  process.stdout.write(
    "\n==> Ignored/skipped test report (separate from passes)\n",
  );
  for (const entry of [...ignored, ...skipped]) {
    process.stdout.write(
      `  ${entry.kind}: ${entry.path} ${entry.name} — ${entry.reason}\n`,
    );
  }
  process.stdout.write(
    `  Totals: Rust ignored=${ignored.length}; TypeScript skipped/conditional=${skipped.length}\n`,
  );
}

/**
 * The `buf` binary `@calque/protocol`'s own scripts get from its
 * devDependency, or undefined when it is not installed.
 */
function protocolBufBinary(): string | undefined {
  const binary = join(
    repositoryRoot,
    "client/packages/protocol/node_modules/.bin/buf",
  );
  if (!existsSync(binary)) return undefined;
  const probe = spawnSync(binary, ["--version"], {
    cwd: repositoryRoot,
    encoding: "utf8",
  });
  return probe.status === 0 ? binary : undefined;
}

/**
 * Runs `@calque/protocol`'s `verify:protocol`, which includes `buf breaking`
 * against the frozen `spec/compat/protocol-v1.binpb`; no other gate runs it.
 * It is called by name, so steps added to that script run here too. Without
 * `buf` the stage says so on stdout and the run continues.
 */
function runProtocolCompatibility(): void {
  const binary = protocolBufBinary();
  if (binary === undefined) {
    process.stdout.write(
      "\n==> Protocol compatibility\n" +
        "SKIP: buf is not installed, so `buf lint` over spec/ and `buf breaking`\n" +
        "against spec/compat/protocol-v1.binpb did not run. Nothing in this run\n" +
        "checked the frozen protocol v1 wire format for a breaking change.\n" +
        "Missing: client/packages/protocol/node_modules/.bin/buf, the\n" +
        "@bufbuild/buf devDependency of @calque/protocol. Install it with\n" +
        "`pnpm install --frozen-lockfile`, then re-run this gate.\n",
    );
    return;
  }
  run("Protocol compatibility", "pnpm", [
    "--filter",
    "@calque/protocol",
    "verify:protocol",
  ]);
}

const typescriptPackages = workspaceInventory();

run("Grant issuer linkage", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-grant-issuer-linkage.ts",
]);
run("Emitted JavaScript", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-no-emitted-javascript.ts",
]);
// No live credential: a grant, an artifact capability, a `calque dev` secret
// or an Ed25519 private key in the tree would be published with it.
run("Credentials", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-no-credentials.ts",
]);
// No machine-local path: a home or temporary directory in captured evidence
// names the person who captured it.
run("Local paths", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-no-local-paths.ts",
]);
run("Stale path references", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-no-stale-path-references.ts",
]);
run("Rust formatting", "cargo", ["fmt", "--all", "--", "--check"]);
// `scripts/**` and the grant issuer example have no package.json, so
// `pnpm -r` never formats them; `format:harness` covers the same globs.
run("Harness formatting", "pnpm", [
  "exec",
  "prettier",
  "--check",
  "scripts/**/*.ts",
  "scripts/**/*.mjs",
  "client/examples/grant-issuer/*.mts",
]);
run("TypeScript generated-binding drift", "pnpm", [
  "--filter",
  "@calque/protocol",
  "generate:check",
]);
run("Client generated-schema fixture drift", "pnpm", [
  "--filter",
  "@calque/client",
  "generate:check",
]);

// Build before typecheck, in dependency order.
for (const package_ of workspaceBuildOrder(typescriptPackages)) {
  run(`TypeScript build: ${package_.name}`, "pnpm", [
    "--filter",
    package_.name,
    "build",
  ]);
}

// `scripts/register.mjs` never typechecks, so the harness does it here, after
// the build loop: harness modules import client packages through `dist/`.
run("Harness typecheck", "pnpm", [
  "exec",
  "tsc",
  "-p",
  "scripts/tsconfig.json",
]);

for (const package_ of typescriptPackages) {
  run(`TypeScript typecheck: ${package_.name}`, "pnpm", [
    "--filter",
    package_.name,
    "typecheck",
  ]);
}

run("Rust warnings/clippy", "cargo", [
  "clippy",
  "--workspace",
  "--all-targets",
  "--",
  "-D",
  "warnings",
]);

for (const package_ of typescriptPackages) {
  if (
    sourceOnly &&
    ["@calque/client", "@calque/worker"].includes(package_.name)
  ) {
    run(`TypeScript source-only unit tests: ${package_.name}`, "pnpm", [
      "--filter",
      package_.name,
      "exec",
      "vitest",
      "run",
      "--project",
      "unit",
    ]);
  } else if (sourceOnly && package_.name === "@calque/react") {
    process.stdout.write(
      "\n==> TypeScript source-only tests: @calque/react\n" +
        "Browser-only suite intentionally excluded by --source-only; typecheck/build/lint remain required.\n",
    );
  } else {
    run(`TypeScript default tests: ${package_.name}`, "pnpm", [
      "--filter",
      package_.name,
      "test",
    ]);
  }
}

// `--all-targets` skips doc-tests: the workspace's doc comments fence text,
// not Rust. An example that should run belongs in a test.
run("Rust workspace tests", "cargo", ["test", "--workspace", "--all-targets"]);

run("Harness tests", "pnpm", ["test:node"]);

for (const package_ of typescriptPackages) {
  run(`TypeScript lint: ${package_.name}`, "pnpm", [
    "--filter",
    package_.name,
    "lint",
  ]);
}

runProtocolCompatibility();
// The pinned `calque.generated.ts` files must match the built
// `@calque/config` emitter byte for byte.
run("Client generated-bindings drift", "pnpm", [
  "--filter",
  "@calque/client",
  "generate:bindings:check",
]);
run("Client public API declarations", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-client-public-api.ts",
]);
run("Client duplicate-package schema isolation", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-client-duplicate-package.ts",
]);
run("Packed packages", "node", [
  "--import",
  "./scripts/register.mjs",
  "scripts/verify-packed-packages.ts",
]);
run(
  sourceOnly
    ? "Client consumer bundles (browser runtime excluded)"
    : "Client browser consumer bundle and runtime",
  "pnpm",
  [
    "--filter",
    "@calque/client",
    sourceOnly ? "consumer:build" : "consumer:check",
  ],
);

skippedTestReport();
stepTimeReport();
process.stdout.write(
  `\nWorkspace verification${sourceOnly ? " (source-only; browser execution excluded)" : ""}: PASS\n`,
);
