#!/usr/bin/env node
// Refuses a machine-local path anywhere in the tree: a home or temporary
// directory in captured evidence, or an encoded form of one, names the person
// who captured it. Write `<checkout>`, `<scratch>`, `<tmp>` or `<home>`
// instead; an evidence writer passes its text through `withoutLocalPaths`.
//
// The gate holds generic shapes only, since a list of names to refuse would
// publish them. A match is printed by place and shape, never by value.
import { lstatSync, readFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

import { treePaths } from "./lib/tree-paths.ts";

type LocalPathShape =
  | "home directory"
  | "temporary directory"
  | "dash-encoded home directory"
  | "Windows home directory";

interface LocalPath {
  readonly shape: LocalPathShape;
  /** Character offset of the match in the text. */
  readonly offset: number;
}

/** A character of one path segment. `<` keeps a placeholder such as `<name>` out. */
const SEGMENT_CHARACTER = String.raw`[^/\s"'<>]`;
const SEGMENT = `${SEGMENT_CHARACTER}+`;
const USERS = "[Uu][Ss][Ee][Rr][Ss]";

/**
 * `path(separator)` from the filesystem root, written plainly or
 * percent-encoded as in a URL. Nothing constrains what precedes it: a dev
 * server's URL carries an absolute path straight after its port or after
 * Vite's `/@fs`.
 */
function absolute(path: (separator: string) => string): string {
  return `(?:/${path("/")}|%2[Ff]${path("%2[Ff]")})`;
}

// Each alternative is one named group, so a match is attributed to the first
// shape that starts there and is not counted again as a shorter one inside it.
const SHAPES: readonly (readonly [string, LocalPathShape, string])[] = [
  [
    "windows",
    "Windows home directory",
    String.raw`[A-Za-z]:(?:\\+|/)${USERS}(?:\\+|/)`,
  ],
  // An encoded home is refused before its user name, as the export refuses it.
  ["encoded", "home directory", `%2[Ff]${USERS}%2[Ff]`],
  // `[/]` keeps the export's scan from refusing this line as a path.
  ["users", "home directory", `[/]Users/${SEGMENT}`],
  // GitHub's hosted runners run as `runner`, which hosted CI evidence names,
  // and the Dockerfile's image runs as `calque`. Neither is a person.
  [
    "linux",
    "home directory",
    absolute(
      (s) =>
        `home${s}(?!runner${s}|calque(?:${s}|(?!${SEGMENT_CHARACTER})))${SEGMENT}`,
    ),
  ],
  // The folders shape needs no private prefix: it finds the per-user
  // directory both ways it is written.
  ["private", "temporary directory", absolute((s) => `private${s}tmp${s}`)],
  ["folders", "temporary directory", absolute((s) => `var${s}folders${s}`)],
  // A per-user temporary directory a tool names after the uid, as `<tool>-501`.
  [
    "uid",
    "temporary directory",
    absolute((s) => `tmp${s}[A-Za-z]+-[0-9]+(?![A-Za-z0-9_.-])`),
  ],
  // A home path with each separator written as `-`, as a tool names a
  // directory after the project it serves.
  ["dashed", "dash-encoded home directory", "-Users-[A-Za-z0-9]"],
];

const LOCAL_PATH = new RegExp(
  SHAPES.map(([group, , pattern]) => `(?<${group}>${pattern})`).join("|"),
  "gu",
);

/** Every machine-local path in `text`, in order of offset. */
export function localPathsIn(text: string): LocalPath[] {
  const found: LocalPath[] = [];
  for (const match of text.matchAll(LOCAL_PATH)) {
    const shape = SHAPES.find(
      ([group]) => match.groups?.[group] !== undefined,
    )?.[1];
    if (shape) found.push({ shape, offset: match.index });
  }
  return found;
}

/** The places on the writer's machine that `withoutLocalPaths` replaces. */
export interface LocalPlaces {
  /** The checkout that writes; a path inside it becomes relative to it. */
  readonly checkout: string;
  /** The repository's other checkouts, each written `<checkout>`. */
  readonly checkouts: readonly string[];
  /** The home directory, written `<home>`. */
  readonly home: string;
  /** The temporary directories, each written `<tmp>`. */
  readonly temporary: readonly string[];
}

const SYNTAX_CHARACTER = /[\\^$.*+?()[\]{}|/]/gu;

/** A place and what it is written as; `null` makes a path relative to it. */
type Replacement = readonly [place: string, placeholder: string | null];

/** `place` as a whole path in a regex: not the start of a longer segment. */
function wholePath(place: string): string {
  return `${place.replace(SYNTAX_CHARACTER, String.raw`\$&`)}(?!${SEGMENT_CHARACTER})`;
}

/**
 * `text` as a record may publish it. A path in the writing checkout becomes
 * relative to it; another checkout, a temporary directory and the home
 * directory become placeholders, in plain paths and in file URLs. Whatever
 * the gate still refuses is written `<withheld>` to the end of its path, so
 * the result always passes the gate.
 */
export function withoutLocalPaths(text: string, places: LocalPlaces): string {
  // A place can sit inside another, a checkout in the home directory or a
  // worktree in a checkout, so the longest goes first and the home directory
  // last. `null` stands for the writing checkout, whose paths become relative.
  const replacements: Replacement[] = [
    [places.checkout, null] as const,
    ...places.checkouts
      .filter((place) => place !== places.checkout)
      .map((place) => [place, "<checkout>"] as const),
    ...places.temporary.map((place) => [place, "<tmp>"] as const),
  ].sort(([left], [right]) => right.length - left.length);
  replacements.push([places.home, "<home>"]);
  let result = text;
  for (const [place, placeholder] of replacements) {
    if (place === "" || place === "/") continue;
    const forms = [
      [pathToFileURL(place).href, "file://"],
      [place, ""],
    ] as const;
    for (const [form, scheme] of forms) {
      const whole = new RegExp(wholePath(form), "gu");
      result =
        placeholder === null
          ? result
              .replace(new RegExp(`${wholePath(form)}/`, "gu"), "")
              .replace(whole, ".")
          : result.replace(whole, `${scheme}${placeholder}`);
    }
  }
  return result.replace(
    new RegExp(`(?:${LOCAL_PATH.source})[^\\s"'<>]*`, "gu"),
    "<withheld>",
  );
}

/** The 1-based line of each offset, which must be ascending. */
function linesOf(text: string, offsets: readonly number[]): number[] {
  const lines: number[] = [];
  let line = 1;
  let at = 0;
  for (const offset of offsets) {
    for (
      let newline = text.indexOf("\n", at);
      newline !== -1 && newline < offset;
      newline = text.indexOf("\n", newline + 1)
    ) {
      line += 1;
      at = newline + 1;
    }
    lines.push(line);
  }
  return lines;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const root = path.resolve(
    process.argv[2] ??
      path.join(path.dirname(fileURLToPath(import.meta.url)), ".."),
  );
  const started = performance.now();
  const refused: string[] = [];
  let matches = 0;
  let withheld = 0;
  let scanned = 0;
  for (const file of treePaths(root)) {
    const absolutePath = path.join(root, file);
    let stat;
    try {
      stat = lstatSync(absolutePath);
    } catch {
      continue; // listed by the index but deleted from the working tree
    }
    // A symlink's target is scanned under its own path; a gitlink is a directory.
    if (!stat.isFile()) continue;
    scanned += 1;
    const text = readFileSync(absolutePath).toString("latin1");
    const found = localPathsIn(text);
    const named = localPathsIn(file).length > 0;
    if (found.length === 0 && !named) continue;
    matches += found.length;
    if (named) {
      withheld += 1;
      continue;
    }
    const lines = linesOf(
      text,
      found.map(({ offset }) => offset),
    );
    const shapes = new Map<LocalPathShape, number>();
    for (const { shape } of found)
      shapes.set(shape, (shapes.get(shape) ?? 0) + 1);
    const counted = [...shapes]
      .map(([shape, count]) => `${String(count)} ${shape}`)
      .join(", ");
    refused.push(
      `${file}:${String(lines[0])} — ${String(found.length)} on ${String(new Set(lines).size)} line(s): ${counted}`,
    );
  }
  const summary = `${String(scanned)} files in ${String(Math.round(performance.now() - started))} ms`;
  if (refused.length === 0 && withheld === 0) {
    process.stdout.write(`Local paths: PASS (${summary})\n`);
    process.exit(0);
  }
  refused.sort();
  process.stderr.write(
    `Local paths: FAIL — ${String(matches)} machine-local path(s) in ${String(refused.length + withheld)} file(s) (${summary}).\n\n` +
      "Each is printed by place and shape, never by value. Replace it with <checkout>, <scratch>,\n" +
      "<tmp> or <home>, and recompute any digest that covers the file:\n\n",
  );
  for (const place of refused) process.stderr.write(`  ${place}\n`);
  if (withheld > 0)
    process.stderr.write(
      `  ${String(withheld)} file(s) whose own path is machine-local, withheld; rename them\n`,
    );
  process.stderr.write("\n");
  process.exit(1);
}
