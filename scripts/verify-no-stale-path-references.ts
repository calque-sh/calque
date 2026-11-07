#!/usr/bin/env node
// Refuses a string literal that names a repository source file which does not
// exist. The harness reads, hashes, spawns and pins files by name, and the
// typechecker cannot see a rename that misses one of those strings. A literal
// counts only when it starts with a source root and ends with a source
// extension.
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const SKIP_DIRECTORIES = new Set([
  "node_modules",
  ".git",
  "dist",
  "target",
  "coverage",
  ".turbo",
]);

/** Files whose own text is searched for path literals. */
const SEARCHED_EXTENSIONS = new Set([".ts", ".tsx", ".mts", ".mjs", ".js"]);

/** Top-level directories whose contents are source. */
const SOURCE_ROOTS = ["scripts/", "client/", "server/", "spec/", "docs/"];

/** Top-level directories whose files are searched. */
const SEARCHED_ROOTS = [...SOURCE_ROOTS, "tests/"];

/** Extensions that make a literal a claim about a source file. */
const SOURCE_EXTENSIONS = [
  ".ts",
  ".tsx",
  ".mts",
  ".mjs",
  ".js",
  ".jsx",
  ".rs",
  ".json",
  ".c",
  ".sql",
  ".proto",
];

/** A double- or single-quoted literal, or a backtick string with no `${}`. */
const LITERAL = /(?<!\\)(?:"([^"\n]*)"|'([^'\n]*)'|`([^`$\n]*)`)/gu;

export interface Finding {
  readonly file: string;
  readonly line: number;
  readonly reference: string;
}

function* walk(directory: string): Generator<string> {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (SKIP_DIRECTORIES.has(entry.name)) continue;
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) yield* walk(full);
    else if (entry.isFile()) yield full;
  }
}

function* sourceFiles(root: string): Generator<string> {
  for (const directory of SEARCHED_ROOTS) {
    const full = path.join(root, directory);
    if (isDirectory(full)) yield* walk(full);
  }
}

function looksLikeSourcePath(value: string): boolean {
  if (!SOURCE_ROOTS.some((prefix) => value.startsWith(prefix))) return false;
  if (!SOURCE_EXTENSIONS.some((extension) => value.endsWith(extension)))
    return false;
  // A literal carrying a glob, a variable marker or whitespace is a pattern or
  // a message, not a path this gate can resolve.
  return !/[*?\s{}]/u.test(value);
}

function isFile(candidate: string): boolean {
  try {
    return statSync(candidate).isFile();
  } catch {
    return false;
  }
}

function isDirectory(candidate: string): boolean {
  try {
    return statSync(candidate).isDirectory();
  } catch {
    return false;
  }
}

/**
 * The nearest ancestor of `from` that owns a `package.json`, or the repository
 * root.
 */
function packageRootOf(root: string, from: string): string {
  let directory = path.dirname(from);
  while (directory.startsWith(root) && directory !== root) {
    if (isFile(path.join(directory, "package.json"))) return directory;
    directory = path.dirname(directory);
  }
  return root;
}

/**
 * A literal resolves from the repository root or from the package that owns
 * the referencing file, since a package spawns its own tooling with `cwd` set
 * to its root.
 */
function exists(root: string, relative: string, from: string): boolean {
  return (
    isFile(path.join(root, relative)) ||
    isFile(path.join(packageRootOf(root, from), relative)) ||
    builtFromSource(root, relative)
  );
}

/**
 * A client package's `dist/` exists only after a build, so a literal naming
 * `client/packages/<package>/dist/<module>.js` resolves when the `src` module
 * it is built from exists.
 */
function builtFromSource(root: string, relative: string): boolean {
  const built = /^(client\/packages\/[^/]+)\/dist\/(.+)\.js$/u.exec(relative);
  if (built === null) return false;
  const [, packageRoot, module] = built;
  return [".ts", ".tsx"].some((extension) =>
    isFile(path.join(root, `${packageRoot}/src/${module}${extension}`)),
  );
}

/** Skipped, since its comments quote example paths. */
const self = fileURLToPath(import.meta.url);

/** Every quoted repository path under `root` that names no file. */
export function stalePathReferences(root: string): Finding[] {
  const findings: Finding[] = [];
  for (const file of sourceFiles(root)) {
    if (file === self) continue;
    if (!SEARCHED_EXTENSIONS.has(path.extname(file))) continue;
    const lines = readFileSync(file, "utf8").split("\n");
    for (const [index, line] of lines.entries()) {
      for (const match of line.matchAll(LITERAL)) {
        const value = match[1] ?? match[2] ?? match[3];
        if (value === undefined) continue;
        if (!looksLikeSourcePath(value)) continue;
        if (exists(root, value, file)) continue;
        findings.push({
          file: path.relative(root, file),
          line: index + 1,
          reference: value,
        });
      }
    }
  }
  return findings;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const findings = stalePathReferences(
    path.resolve(path.dirname(fileURLToPath(import.meta.url)), ".."),
  );
  if (findings.length === 0) {
    console.log(
      "Stale path references: PASS (every quoted repository path resolves)",
    );
    process.exit(0);
  }

  // Grouped by the missing target, since one rename breaks many call sites.
  const byTarget = new Map<string, Finding[]>();
  for (const finding of findings) {
    const bucket = byTarget.get(finding.reference);
    if (bucket) bucket.push(finding);
    else byTarget.set(finding.reference, [finding]);
  }

  console.error(
    `Stale path references: FAIL — ${findings.length} literal(s) name ${byTarget.size} file(s) that do not exist.`,
  );
  console.error(
    "\nThese are strings, so no typechecker reports them. They fail at readFileSync",
    "\nor in a spawned process. Update the literal.\n",
  );
  for (const [target, sites] of [...byTarget].sort(([a], [b]) =>
    a.localeCompare(b),
  )) {
    console.error(`  ${target}  — referenced by ${sites.length} site(s):`);
    for (const site of sites) console.error(`      ${site.file}:${site.line}`);
  }
  process.exit(1);
}
