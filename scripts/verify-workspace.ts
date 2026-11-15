import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  writeFileSync,
} from "node:fs";
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
// `--all` runs every lane even when its inputs match its last pass; CI always
// does.
const runEverything =
  arguments_.includes("--all") || process.env["CI"] === "true";
const unknownArguments = arguments_.filter(
  (argument) => argument !== "--source-only" && argument !== "--all",
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
const verificationStarted = performance.now();

/**
 * While more than one lane runs, each step's output is held and printed as
 * one block when the step ends, so two lanes never interleave lines. A lone
 * lane streams as before.
 */
let lanesRunning = 1;
/** Set by the first failing step; every other lane's output is dropped. */
let failing = false;
const children = new Set<ChildProcess>();

function stopChildren(): void {
  for (const child of children)
    if (child.pid !== undefined)
      try {
        // Each step leads its own process group, so its browsers and
        // workers stop with it.
        process.kill(-child.pid, "SIGTERM");
      } catch {
        // Already gone.
      }
}

for (const [signal, code] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
] as const)
  process.once(signal, () => {
    stopChildren();
    process.exit(code);
  });

/** Never settles: the lane that called it stops where it is. */
const abandoned = new Promise<never>(() => undefined);

async function run(
  label: string,
  command: string,
  arguments_: readonly string[],
): Promise<void> {
  if (failing) return abandoned;
  const header = `\n==> ${label}\n$ ${commandText(command, arguments_)}\n`;
  const buffered = lanesRunning > 1;
  if (!buffered) process.stdout.write(header);
  const started = performance.now();
  const child = spawn(command, [...arguments_], {
    cwd: repositoryRoot,
    stdio: buffered ? ["ignore", "pipe", "pipe"] : "inherit",
    detached: true,
  });
  children.add(child);
  const output: Buffer[] = [];
  child.stdout?.on("data", (chunk: Buffer) => output.push(chunk));
  child.stderr?.on("data", (chunk: Buffer) => output.push(chunk));
  const status = await new Promise<number>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code) => resolve(code ?? 1));
  });
  children.delete(child);
  if (failing) return abandoned;
  const seconds = (performance.now() - started) / 1_000;
  stepTimes.push({ label, seconds });
  if (buffered)
    process.stdout.write(header + Buffer.concat(output).toString("utf8"));
  process.stdout.write(`<== ${label}: ${seconds.toFixed(1)} s\n`);
  if (status !== 0) {
    // The failing step's banner stays the last one printed:
    // verify-workspace-probes.ts reads the failing phase from it.
    failing = true;
    stopChildren();
    process.exit(status);
  }
}

/**
 * A lane's inputs, fingerprinted from git's blob ids for unchanged tracked
 * files and from the bytes on disk for changed and untracked ones.
 */
function laneFingerprint(
  lane: string,
  includes: (path: string) => boolean,
): string {
  const entries = (args: readonly string[]) =>
    capture("git", [...args, "-z"])
      .split("\0")
      .filter(Boolean);
  const indexed = new Map<string, string>();
  for (const entry of entries(["ls-files", "--stage"])) {
    const tab = entry.indexOf("\t");
    indexed.set(entry.slice(tab + 1), entry.slice(0, tab).split(" ")[1] ?? "");
  }
  const changed = new Set([
    ...entries(["diff", "--name-only", "HEAD"]),
    ...entries(["ls-files", "--others", "--exclude-standard"]),
  ]);
  const hash = createHash("sha256");
  hash.update(
    `${lane}\0${process.version}\0${sourceOnly ? "source-only" : "full"}\0`,
  );
  for (const path of [...new Set([...indexed.keys(), ...changed])].sort()) {
    if (!includes(path)) continue;
    let id = indexed.get(path) ?? "";
    if (changed.has(path)) {
      const file = join(repositoryRoot, path);
      id = existsSync(file)
        ? createHash("sha256").update(readFileSync(file)).digest("hex")
        : "deleted";
    }
    hash.update(`${path}\0${id}\0`);
  }
  return hash.digest("hex");
}

/**
 * A lane whose inputs match the fingerprint of its last passing run on this
 * checkout is skipped. The stamps live in the Cargo target directory, so
 * deleting the build deletes them too. CI and `--all` never skip.
 */
class Lane {
  readonly #stamp: string;
  readonly #fingerprint: string;
  readonly skipped: boolean;

  constructor(
    readonly name: string,
    readonly includes: (path: string) => boolean,
  ) {
    const checkout = createHash("sha256")
      .update(repositoryRoot)
      .digest("hex")
      .slice(0, 12);
    this.#stamp = join(
      process.env["CARGO_TARGET_DIR"] ?? join(repositoryRoot, "target"),
      "verify-stamps",
      `${checkout}-${name}${sourceOnly ? "-source-only" : ""}`,
    );
    this.#fingerprint = laneFingerprint(name, includes);
    this.skipped =
      !runEverything &&
      existsSync(this.#stamp) &&
      readFileSync(this.#stamp, "utf8") === this.#fingerprint;
  }

  /** Records a pass, unless the inputs changed while the lane ran. */
  passed(): void {
    if (laneFingerprint(this.name, this.includes) !== this.#fingerprint) return;
    mkdirSync(dirname(this.#stamp), { recursive: true });
    writeFileSync(this.#stamp, this.#fingerprint);
  }
}

/** The slowest steps and the total, so a slow gate is seen where it ran. */
function stepTimeReport(): void {
  const total = stepTimes.reduce((sum, step) => sum + step.seconds, 0);
  const wall = (performance.now() - verificationStarted) / 1_000;
  const slowest = [...stepTimes]
    .sort((a, b) => b.seconds - a.seconds)
    .slice(0, 10);
  process.stdout.write(
    `\n==> Step times: ${total.toFixed(1)} s over ${String(stepTimes.length)} steps, ${wall.toFixed(1)} s wall-clock; the slowest:\n`,
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
          ? ".github/workflows/ci.yml; on every push to main or dev and every pull request it runs this gate, the browser end-to-end scripts, the self-hosted composition behind Caddy and blob-store's MinIO conformance, and on pushes to main or dev and by dispatch the ignored live-PostgreSQL suite."
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
async function runProtocolCompatibility(): Promise<void> {
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
  await run("Protocol compatibility", "pnpm", [
    "--filter",
    "@calque/protocol",
    "verify:protocol",
  ]);
}

const typescriptPackages = workspaceInventory();
const node = (script: string) => ["--import", "./scripts/register.mjs", script];

// The checks every change gets, Markdown included: a few seconds in all.
await run(
  "Grant issuer linkage",
  "node",
  node("scripts/verify-grant-issuer-linkage.ts"),
);
await run(
  "Emitted JavaScript",
  "node",
  node("scripts/verify-no-emitted-javascript.ts"),
);
// No live credential: a grant, an artifact capability, a `calque dev` secret
// or an Ed25519 private key in the tree would be published with it.
await run("Credentials", "node", node("scripts/verify-no-credentials.ts"));
// No machine-local path: a home or temporary directory in captured evidence
// names the person who captured it.
await run("Local paths", "node", node("scripts/verify-no-local-paths.ts"));
await run(
  "Stale path references",
  "node",
  node("scripts/verify-no-stale-path-references.ts"),
);

// Rust reads nothing outside these paths; `package.json` holds
// `lint:rust-production`.
const rustInputs = (path: string) =>
  /^(server|vendor|spec|\.cargo)\//.test(path) ||
  [
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    "clippy.toml",
    "rustfmt.toml",
    "package.json",
    "scripts/verify-workspace.ts",
  ].includes(path);
// The TypeScript and harness tests call cargo and read spec/ and scripts/
// Markdown, so only the docs' and the root's Markdown are outside them.
const typescriptInputs = (path: string) =>
  !(path.endsWith(".md") && (path.startsWith("docs/") || !path.includes("/")));

const rust = new Lane("rust", rustInputs);
const typescript = new Lane("typescript", typescriptInputs);
for (const lane of [rust, typescript])
  if (lane.skipped)
    process.stdout.write(
      `\n==> ${lane.name} lane: SKIP, its inputs match its last passing run on this checkout (\`pnpm verify --all\` runs it)\n`,
    );
lanesRunning = [rust, typescript].filter((lane) => !lane.skipped).length;

// TypeScript tests shell out to cargo, and cargo holds the build directory's
// lock while it compiles. They start once the Rust lane's compiling is done,
// so no test spends its own timeout waiting on that lock.
let rustCompiled: () => void = () => undefined;
const rustCompiledOrSkipped = new Promise<void>((resolve) => {
  rustCompiled = resolve;
});

async function rustLane(): Promise<void> {
  if (rust.skipped) {
    rustCompiled();
    return;
  }
  await run("Rust formatting", "cargo", ["fmt", "--all", "--", "--check"]);
  await run("Rust warnings/clippy", "cargo", [
    "clippy",
    "--workspace",
    "--all-targets",
    "--",
    "-D",
    "warnings",
  ]);
  // Production code returns an error instead of unwrapping or panicking; an
  // `.expect` that names the invariant it relies on stays allowed. Tests,
  // examples and benches are left out.
  await run("Rust production panics", "pnpm", ["run", "lint:rust-production"]);
  // `--all-targets` skips doc-tests: the workspace's doc comments fence text,
  // not Rust. An example that should run belongs in a test.
  await run("Rust test build", "cargo", [
    "test",
    "--workspace",
    "--all-targets",
    "--no-run",
  ]);
  // @calque/core's artifact test runs this example through cargo.
  await run("Rust fixture example build", "cargo", [
    "build",
    "-p",
    "direct-columnar",
    "--example",
    "emit_fixture",
  ]);
  rustCompiled();
  await run("Rust workspace tests", "cargo", [
    "test",
    "--workspace",
    "--all-targets",
  ]);
  rust.passed();
}

async function typescriptLane(): Promise<void> {
  if (typescript.skipped) return;
  // `scripts/**` has no package.json, so `pnpm -r` never formats it;
  // `format:harness` covers the same globs.
  await run("Harness formatting", "pnpm", [
    "exec",
    "prettier",
    "--check",
    "scripts/**/*.ts",
    "scripts/**/*.mjs",
  ]);
  await run("TypeScript generated-binding drift", "pnpm", [
    "--filter",
    "@calque/protocol",
    "generate:check",
  ]);
  await run("Client generated-schema fixture drift", "pnpm", [
    "--filter",
    "@calque/client",
    "generate:check",
  ]);

  // Build before typecheck, in dependency order.
  for (const package_ of workspaceBuildOrder(typescriptPackages))
    await run(`TypeScript build: ${package_.name}`, "pnpm", [
      "--filter",
      package_.name,
      "build",
    ]);

  // `scripts/register.mjs` never typechecks, so the harness does it here,
  // after the build loop: harness modules import client packages through
  // `dist/`.
  await run("Harness typecheck", "pnpm", [
    "exec",
    "tsc",
    "-p",
    "scripts/tsconfig.json",
  ]);
  for (const package_ of typescriptPackages)
    await run(`TypeScript typecheck: ${package_.name}`, "pnpm", [
      "--filter",
      package_.name,
      "typecheck",
    ]);
  // Lint before the tests: it takes seconds and fails often.
  for (const package_ of typescriptPackages)
    await run(`TypeScript lint: ${package_.name}`, "pnpm", [
      "--filter",
      package_.name,
      "lint",
    ]);

  await rustCompiledOrSkipped;
  for (const package_ of typescriptPackages) {
    if (
      sourceOnly &&
      ["@calque/client", "@calque/worker"].includes(package_.name)
    ) {
      await run(`TypeScript source-only unit tests: ${package_.name}`, "pnpm", [
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
      await run(`TypeScript default tests: ${package_.name}`, "pnpm", [
        "--filter",
        package_.name,
        "test",
      ]);
    }
  }
  await run("Harness tests", "pnpm", ["test:node"]);
  await runProtocolCompatibility();
  // The pinned `calque.generated.ts` files must match the built
  // `@calque/config` emitter byte for byte.
  await run("Client generated-bindings drift", "pnpm", [
    "--filter",
    "@calque/client",
    "generate:bindings:check",
  ]);
  await run(
    "Client public API declarations",
    "node",
    node("scripts/verify-client-public-api.ts"),
  );
  await run(
    "Client duplicate-package schema isolation",
    "node",
    node("scripts/verify-client-duplicate-package.ts"),
  );
  await run(
    "Packed packages",
    "node",
    node("scripts/verify-packed-packages.ts"),
  );
  await run(
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
  typescript.passed();
}

try {
  await Promise.all([rustLane(), typescriptLane()]);
} catch (error) {
  failing = true;
  stopChildren();
  throw error;
}
lanesRunning = 1;

skippedTestReport();
stepTimeReport();
process.stdout.write(
  `\nWorkspace verification${sourceOnly ? " (source-only; browser execution excluded)" : ""}: PASS\n`,
);
