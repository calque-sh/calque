/**
 * Change notes and the changelog. A commit that changes what a user of the
 * packages, the `calque` binary or its image meets adds one note to
 * `.changes/`; a release turns the notes into a `CHANGELOG.md` section and
 * deletes them. `.changes/README.md` describes a note for its writers.
 *
 * Every published package and the binary share one version, so a release has
 * one section, not one per package.
 */
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export const changeKinds = [
  "breaking",
  "security",
  "added",
  "changed",
  "fixed",
] as const;
export type ChangeKind = (typeof changeKinds)[number];

const headings: Record<ChangeKind, string> = {
  breaking: "Breaking",
  security: "Security",
  added: "Added",
  changed: "Changed",
  fixed: "Fixed",
};

export interface ChangeNote {
  /** The file name under `.changes/`. */
  readonly file: string;
  readonly kind: ChangeKind;
  readonly body: string;
}

export const changesDirectory = ".changes";
export const changelogFile = "CHANGELOG.md";
const cargoManifest = "server/apps/calque/Cargo.toml";
const packagesDirectory = "client/packages";

/**
 * Names the public repository must not carry: the export refuses them, so a
 * note that holds one would stop the next sync.
 */
const privateReferences: readonly [RegExp, string][] = [
  [/\bADRs?\b/, "a decision record"],
  [/\b[A-Z]{2,}-[A-Z0-9]+-[0-9]{3}\b/, "an experiment id"],
  [
    /\b(?:STATUS\.md|AGENTS\.md|docs\/adr|docs\/models|docs\/evidence|experiments\/)/,
    "a private path",
  ],
];

const versionPattern = /^(\d+)\.(\d+)\.(\d+)(?:-[0-9A-Za-z.]+)?$/;

export function parseChangeNote(file: string, text: string): ChangeNote {
  const match = /^---\nkind: *([a-z]+) *\n---\n([\s\S]*)$/.exec(
    text.replaceAll("\r\n", "\n"),
  );
  if (match === null)
    throw new Error(
      `${changesDirectory}/${file}: a note opens with "---", "kind: <kind>" and "---" on lines of their own`,
    );
  const [, kind = "", rawBody = ""] = match;
  if (!(changeKinds as readonly string[]).includes(kind))
    throw new Error(
      `${changesDirectory}/${file}: kind "${kind}" is not one of ${changeKinds.join(", ")}`,
    );
  const body = rawBody.trim();
  if (body === "")
    throw new Error(`${changesDirectory}/${file}: the note says nothing`);
  if (/^#/m.test(body))
    throw new Error(
      `${changesDirectory}/${file}: a note holds no heading; the release adds them`,
    );
  for (const [pattern, what] of privateReferences)
    if (pattern.test(body))
      throw new Error(
        `${changesDirectory}/${file}: names ${what}, which the public repository does not carry`,
      );
  return { file, kind: kind as ChangeKind, body };
}

export function readChangeNotes(root: string): ChangeNote[] {
  let files: string[];
  try {
    files = readdirSync(join(root, changesDirectory));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
  return files
    .filter((file) => file.endsWith(".md") && file !== "README.md")
    .sort()
    .map((file) =>
      parseChangeNote(
        file,
        readFileSync(join(root, changesDirectory, file), "utf8"),
      ),
    );
}

function parseVersion(version: string): [number, number, number] {
  const match = versionPattern.exec(version);
  if (match === null) throw new Error(`${version} is not a version`);
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

/**
 * Before 1.0 a breaking change or an addition moves the minor version and
 * anything else the patch; from 1.0 on, a breaking change moves the major.
 */
export function nextVersion(
  current: string,
  kinds: readonly ChangeKind[],
): string {
  const [major, minor, patch] = parseVersion(current);
  const breaking = kinds.includes("breaking");
  if (major > 0 && breaking) return `${major + 1}.0.0`;
  if (breaking || kinds.includes("added")) return `${major}.${minor + 1}.0`;
  return `${major}.${minor}.${patch + 1}`;
}

function laterThan(candidate: string, current: string): boolean {
  const a = parseVersion(candidate);
  const b = parseVersion(current);
  for (let index = 0; index < 3; index += 1)
    if (a[index] !== b[index]) return (a[index] ?? 0) > (b[index] ?? 0);
  // Same numbers: only a release, or a later pre-release, follows a
  // pre-release.
  if (!current.includes("-")) return false;
  if (!candidate.includes("-")) return true;
  return candidate > current;
}

export function renderRelease(
  version: string,
  date: string,
  notes: readonly ChangeNote[],
): string {
  const lines = [`## ${version} (${date})`];
  for (const kind of changeKinds) {
    const ofKind = notes.filter((note) => note.kind === kind);
    if (ofKind.length === 0) continue;
    lines.push("", `### ${headings[kind]}`, "");
    for (const note of ofKind)
      lines.push(
        note.body
          .split("\n")
          .map((line, index) =>
            index === 0 ? `- ${line}` : line === "" ? "" : `  ${line}`,
          )
          .join("\n"),
      );
  }
  return `${lines.join("\n")}\n`;
}

/** Puts a release's section above the newest one. */
export function insertRelease(changelog: string, section: string): string {
  const first = changelog.search(/^## /m);
  if (first === -1) return `${changelog.trimEnd()}\n\n${section}`;
  return `${changelog.slice(0, first)}${section}\n${changelog.slice(first)}`;
}

/** The body of a release's section, without its `## ` line. */
export function releaseNotes(
  changelog: string,
  version: string,
): string | undefined {
  const lines = changelog.split("\n");
  const start = lines.findIndex(
    (line) => line === `## ${version}` || line.startsWith(`## ${version} (`),
  );
  if (start === -1) return undefined;
  const after = lines.slice(start + 1);
  const end = after.findIndex((line) => line.startsWith("## "));
  return `${(end === -1 ? after : after.slice(0, end)).join("\n").trim()}\n`;
}

/** The newest version the changelog has a section for. */
export function newestRelease(changelog: string): string | undefined {
  return /^## (\S+)/m.exec(changelog)?.[1];
}

interface VersionedFile {
  readonly path: string;
  readonly version: string;
}

function publishedManifests(root: string): string[] {
  return readdirSync(join(root, packagesDirectory))
    .map((entry) => join(packagesDirectory, entry, "package.json"))
    .filter(
      (path) =>
        (
          JSON.parse(readFileSync(join(root, path), "utf8")) as {
            private?: boolean;
          }
        ).private !== true,
    )
    .sort();
}

/** Each published package's version and the binary's. */
export function releaseVersions(root: string): VersionedFile[] {
  const packages = publishedManifests(root).map((path) => ({
    path,
    version: (
      JSON.parse(readFileSync(join(root, path), "utf8")) as { version: string }
    ).version,
  }));
  const cargo = /^version = "([^"]+)"$/m.exec(
    readFileSync(join(root, cargoManifest), "utf8"),
  )?.[1];
  if (cargo === undefined) throw new Error(`${cargoManifest} has no version`);
  return [...packages, { path: cargoManifest, version: cargo }];
}

/** The one version every published package and the binary carry. */
export function currentVersion(root: string): string {
  const versions = releaseVersions(root);
  const distinct = new Set(versions.map(({ version }) => version));
  if (distinct.size !== 1)
    throw new Error(
      `the published packages and the binary carry different versions:\n${versions
        .map(({ path, version }) => `  ${path}: ${version}`)
        .join("\n")}`,
    );
  return [...distinct][0] ?? "";
}

function rewrite(
  root: string,
  path: string,
  pattern: RegExp,
  replacement: string,
): void {
  const text = readFileSync(join(root, path), "utf8");
  if (!pattern.test(text)) throw new Error(`${path}: no version to replace`);
  writeFileSync(join(root, path), text.replace(pattern, replacement));
}

export interface PreparedRelease {
  readonly previous: string;
  readonly version: string;
  readonly section: string;
  readonly notes: readonly ChangeNote[];
}

/**
 * Moves every published package, the binary and `Cargo.lock` to the next
 * version, writes the release's section into the changelog and returns the
 * notes it used. It deletes nothing: the caller removes the notes.
 */
export function prepareRelease(
  root: string,
  options: { readonly version?: string; readonly date: string },
): PreparedRelease {
  const notes = readChangeNotes(root);
  if (notes.length === 0)
    throw new Error(`${changesDirectory}/ holds no change note to release`);
  const previous = currentVersion(root);
  const version =
    options.version ??
    nextVersion(
      previous,
      notes.map(({ kind }) => kind),
    );
  parseVersion(version);
  if (!laterThan(version, previous))
    throw new Error(`${version} does not come after ${previous}`);
  const changelog = readFileSync(join(root, changelogFile), "utf8");
  if (releaseNotes(changelog, version) !== undefined)
    throw new Error(`${changelogFile} already has a section for ${version}`);

  const escaped = previous.replaceAll(".", "\\.");
  for (const path of publishedManifests(root))
    rewrite(
      root,
      path,
      new RegExp(`("version": ")${escaped}(")`),
      `$1${version}$2`,
    );
  rewrite(
    root,
    cargoManifest,
    new RegExp(`^(version = ")${escaped}(")$`, "m"),
    `$1${version}$2`,
  );
  rewrite(
    root,
    "Cargo.lock",
    new RegExp(`(\\nname = "calque"\\nversion = ")${escaped}(")`),
    `$1${version}$2`,
  );
  const section = renderRelease(version, options.date, notes);
  writeFileSync(join(root, changelogFile), insertRelease(changelog, section));
  return { previous, version, section, notes };
}
