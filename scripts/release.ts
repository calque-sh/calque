/**
 * Release notes from the change notes in `.changes/`.
 *
 *   pnpm release:prepare [<version>]
 *     Moves every published package, the `calque` binary and Cargo.lock to
 *     the next version, or to <version>, writes the release's section at the
 *     top of CHANGELOG.md from the notes, deletes the notes, and prints the
 *     section. The version follows from the notes' kinds unless given. It
 *     commits nothing.
 *
 *   pnpm release:notes [<version>]
 *     Prints a release's section of CHANGELOG.md, by default the current
 *     version's.
 */
import { readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

import {
  changelogFile,
  changesDirectory,
  currentVersion,
  prepareRelease,
  releaseNotes,
} from "./lib/change-notes.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [command, version, ...rest] = process.argv.slice(2);
if (rest.length > 0) throw new Error(`unexpected arguments: ${rest.join(" ")}`);

if (command === "prepare") {
  const prepared = prepareRelease(root, {
    ...(version === undefined ? {} : { version }),
    date: new Date().toISOString().slice(0, 10),
  });
  for (const { file } of prepared.notes)
    rmSync(join(root, changesDirectory, file));
  process.stdout.write(
    `${prepared.previous} -> ${prepared.version}, from ${prepared.notes.length} change notes\n\n${prepared.section}\n` +
      `Next: commit "Release ${prepared.version}", merge it, sync the public dev branch, and release ${prepared.version} once CI passes there.\n`,
  );
} else if (command === "notes") {
  const wanted = version ?? currentVersion(root);
  const notes = releaseNotes(
    readFileSync(join(root, changelogFile), "utf8"),
    wanted,
  );
  if (notes === undefined)
    throw new Error(`${changelogFile} has no section for ${wanted}`);
  process.stdout.write(notes);
} else {
  throw new Error("usage: release.ts prepare [<version>] | notes [<version>]");
}
