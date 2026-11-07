/**
 * `pnpm publish:packages`: builds every published `@calque` package from a
 * clean `dist/`, runs `pnpm verify:packed` on the result, then hands each
 * package's tarball to `npm publish`, dependencies first, so no package
 * reaches the registry before one it depends on. A version the registry
 * already holds is skipped, so a run that stopped part way can run again.
 *
 * Without `--publish` it is a dry run: npm checks each tarball and uploads
 * nothing. With it, the checkout must be clean, so what is published is a
 * commit. In GitHub Actions npm signs in through the workflow's OIDC
 * identity (trusted publishing) and attaches provenance; anywhere else it
 * uses `npm login` and asks for the second factor.
 */
import { spawnSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { workspaceBuildOrder } from "./lib/workspace-build-order.ts";

interface Manifest {
  readonly name: string;
  readonly version: string;
  readonly private?: boolean;
  readonly dependencies?: Record<string, string>;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagesDirectory = join(root, "client/packages");

const arguments_ = process.argv.slice(2);
const upload = arguments_.includes("--publish");
const unknown = arguments_.filter((argument) => argument !== "--publish");
if (unknown.length > 0)
  throw new Error(`unknown arguments: ${unknown.join(" ")}`);

function run(command: string, args: readonly string[], cwd: string) {
  const result = spawnSync(command, args, { cwd, stdio: "inherit" });
  if (result.status !== 0)
    throw new Error(
      `${[command, ...args].join(" ")} in ${cwd} exited ${String(result.status ?? result.signal)}`,
    );
}

function onRegistry(name: string, version: string): boolean {
  const result = spawnSync("npm", ["view", `${name}@${version}`, "version"], {
    encoding: "utf8",
  });
  if (result.status === 0) return result.stdout.trim() === version;
  if (result.stderr.includes("E404")) return false;
  throw new Error(`npm view ${name}@${version} failed:\n${result.stderr}`);
}

if (upload) {
  const status = spawnSync("git", ["status", "--porcelain"], {
    cwd: root,
    encoding: "utf8",
  });
  if (status.status !== 0 || status.stdout !== "")
    throw new Error(
      "--publish needs a clean checkout, so the tarballs match a commit",
    );
}

const packages = workspaceBuildOrder(
  readdirSync(packagesDirectory)
    .map((entry) => join(packagesDirectory, entry))
    .map((directory) => ({
      directory,
      manifest: JSON.parse(
        readFileSync(join(directory, "package.json"), "utf8"),
      ) as Manifest,
    }))
    .filter(({ manifest }) => manifest.private !== true)
    .map((entry) => ({
      ...entry,
      name: entry.manifest.name,
      dependencies: Object.keys(entry.manifest.dependencies ?? {}),
    })),
);

for (const { directory } of packages) {
  rmSync(join(directory, "dist"), { recursive: true, force: true });
  run("pnpm", ["run", "build"], directory);
}
run("pnpm", ["verify:packed"], root);

const tarballs = mkdtempSync(join(tmpdir(), "calque-publish-"));
const skipped: string[] = [];
for (const { directory, manifest } of packages) {
  const { name, version } = manifest;
  if (onRegistry(name, version)) {
    skipped.push(`${name}@${version}`);
    continue;
  }
  run("pnpm", ["pack", "--pack-destination", tarballs], directory);
  const tarball = join(
    tarballs,
    `${name.slice(1).replace("/", "-")}-${version}.tgz`,
  );
  run(
    "npm",
    [
      "publish",
      tarball,
      "--access",
      "public",
      ...(process.env["GITHUB_ACTIONS"] === "true" ? ["--provenance"] : []),
      ...(upload ? [] : ["--dry-run"]),
    ],
    tarballs,
  );
}
rmSync(tarballs, { recursive: true, force: true });

process.stdout.write(
  `${upload ? "Published" : "Dry run"}: ${packages
    .map(({ manifest }) => `${manifest.name}@${manifest.version}`)
    .filter((entry) => !skipped.includes(entry))
    .join(
      ", ",
    )}${skipped.length > 0 ? `; already on npm: ${skipped.join(", ")}` : ""}\n`,
);
