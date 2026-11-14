import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  appendFileSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const loader = join(repositoryRoot, "scripts/register.mjs");
const verifier = join(repositoryRoot, "scripts/verify-workspace.ts");
const evidenceDirectory = join(repositoryRoot, "target/verify-probes");
mkdirSync(evidenceDirectory, { recursive: true });

const probes = [
  {
    id: "generated-drift",
    path: "client/packages/protocol/src/gen/change_stream_pb.ts",
    append: "\n// verify:probes generated drift probe\n",
    phase: "==> TypeScript generated-binding drift",
  },
  // A type error in `src` is caught by the build, which runs before the
  // typecheck.
  {
    id: "typescript-build-type-error",
    path: "client/packages/client/src/index.ts",
    append: "\nconst __verifyProbeType: string = 1;\n",
    phase: "==> TypeScript build: @calque/client",
  },
  // The typecheck's own control: `tsconfig.build.json` includes only `src`,
  // so the build never opens `eslint.config.ts`, and `tsconfig.json` does.
  {
    id: "typescript-typecheck-type-error",
    path: "client/packages/client/eslint.config.ts",
    append: "\nconst __verifyProbeTypecheck: string = 1;\n",
    phase: "==> TypeScript typecheck: @calque/client",
  },
  {
    id: "rust-warning",
    path: "server/crates/sync-protocol/src/lib.rs",
    append:
      "\npub fn __verify_probe_warning() -> bool {\n    return true;\n}\n",
    phase: "==> Rust warnings/clippy",
  },
  {
    id: "rust-production-unwrap",
    path: "server/crates/sync-protocol/src/lib.rs",
    append:
      "\npub fn __verify_probe_unwrap(value: Option<u8>) -> u8 {\n    value.unwrap()\n}\n",
    phase: "==> Rust production panics",
  },
  {
    id: "formatting-drift",
    path: "server/crates/sync-protocol/src/lib.rs",
    append: "\npub fn __verify_probe_format( )->bool {true}\n",
    phase: "==> Rust formatting",
  },
  {
    id: "failing-test",
    path: "client/packages/protocol/src/protocol-v1-fixtures.test.ts",
    append:
      '\nit("verify:probes deliberate failing test probe", () => {\n  expect(true).toBe(false);\n});\n',
    phase: "==> TypeScript default tests: @calque/protocol",
  },
];

let activeRestore: (() => void) | undefined;
for (const [signal, code] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
] as const) {
  process.once(signal, () => {
    activeRestore?.();
    process.exit(code);
  });
}

function digest(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function runVerifier(name: string) {
  const result = spawnSync(process.execPath, ["--import", loader, verifier], {
    cwd: repositoryRoot,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
  const log = join(evidenceDirectory, `${name}.log`);
  writeFileSync(log, output);
  // The verifier prints `==> <label>` when it starts a phase and exits at the
  // first phase that fails, so the last banner names the failing phase. Any
  // earlier banner would also pass for a probe a later phase caught.
  const banners = output
    .split("\n")
    .filter((line) => line.startsWith("==> "))
    .map((line) => line.trimEnd());
  return { ...result, output, log, lastBanner: banners.at(-1) ?? null };
}

process.stdout.write("pnpm verify negative-probe self-test\n");
const baseline = runVerifier("baseline-clean");
if (baseline.error) throw baseline.error;
if (baseline.status !== 0) {
  throw new Error(`baseline verification failed; see ${baseline.log}`);
}
process.stdout.write(`  PASS baseline clean gate — ${baseline.log}\n`);

for (const probe of probes) {
  const path = join(repositoryRoot, probe.path);
  const original = readFileSync(path);
  const originalDigest = digest(original);
  let restored = false;
  const restore = () => {
    if (restored) return;
    writeFileSync(path, original);
    restored = true;
  };
  activeRestore = restore;
  let result;
  try {
    appendFileSync(path, probe.append);
    result = runVerifier(probe.id);
  } finally {
    restore();
    activeRestore = undefined;
  }
  if (digest(readFileSync(path)) !== originalDigest) {
    throw new Error(`${probe.id} did not restore ${probe.path} byte-for-byte`);
  }
  if (result.error) throw result.error;
  if (result.status === 0) {
    throw new Error(`${probe.id} was not rejected; see ${result.log}`);
  }
  if (result.lastBanner !== probe.phase) {
    throw new Error(
      `${probe.id} was rejected at ${result.lastBanner ?? "no phase at all"}, ` +
        `not at ${probe.phase}; see ${result.log}`,
    );
  }
  process.stdout.write(
    `  PASS ${probe.id}: rejected at ${probe.phase.slice(4)} and restored ${probe.path} — ${result.log}\n`,
  );
}

process.stdout.write(
  `pnpm verify negative-probe self-test: PASS (${String(probes.length)}/${String(probes.length)} probes)\n`,
);
