import assert from "node:assert/strict";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  currentVersion,
  insertRelease,
  newestRelease,
  nextVersion,
  parseChangeNote,
  prepareRelease,
  readChangeNotes,
  releaseNotes,
  renderRelease,
} from "./lib/change-notes.ts";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");

test("the repository's change notes parse, and its versions agree with the changelog", () => {
  readChangeNotes(repository);
  const version = currentVersion(repository);
  const changelog = readFileSync(join(repository, "CHANGELOG.md"), "utf8");
  assert.equal(newestRelease(changelog), version);
  assert.notEqual(releaseNotes(changelog, version), undefined);
});

const note = (kind: string, body: string) =>
  `---\nkind: ${kind}\n---\n${body}\n`;

test("a note is refused without its kind, with an unknown kind, empty, or with a heading", () => {
  assert.throws(() => parseChangeNote("a.md", "Fixed it.\n"), /opens with/);
  assert.throws(
    () => parseChangeNote("a.md", note("feature", "x")),
    /not one of/,
  );
  assert.throws(
    () => parseChangeNote("a.md", note("fixed", "  \n")),
    /says nothing/,
  );
  assert.throws(
    () => parseChangeNote("a.md", note("fixed", "## Fix\nx")),
    /no heading/,
  );
});

test("a note that names a private record is refused", () => {
  // Joined at run time: the public export refuses a file that names them.
  for (const body of [
    ["As AD", "R 0111 says."],
    ["Measured in WARM-VISIT", "-002."],
    ["See STAT", "US.md."],
    ["Recorded in docs/a", "dr/0001.md."],
  ].map((parts) => parts.join("")))
    assert.throws(
      () => parseChangeNote("a.md", note("fixed", body)),
      /public repository/,
      body,
    );
  assert.equal(
    parseChangeNote("a.md", note("fixed", "Reads `MUTATION_CONFLICT`.")).kind,
    "fixed",
  );
});

test("the version follows from the kinds", () => {
  assert.equal(nextVersion("0.3.0", ["fixed"]), "0.3.1");
  assert.equal(nextVersion("0.3.2", ["changed", "security"]), "0.3.3");
  assert.equal(nextVersion("0.3.2", ["fixed", "added"]), "0.4.0");
  assert.equal(nextVersion("0.3.2", ["breaking"]), "0.4.0");
  assert.equal(nextVersion("1.3.2", ["added"]), "1.4.0");
  assert.equal(nextVersion("1.3.2", ["added", "breaking"]), "2.0.0");
});

test("a release's section groups its notes by kind, breaking changes first", () => {
  const section = renderRelease("0.4.0", "2026-11-17", [
    { file: "a.md", kind: "fixed", body: "One fix." },
    {
      file: "b.md",
      kind: "added",
      body: "A thing,\nover two lines.\n\nAnd a paragraph.",
    },
    { file: "c.md", kind: "breaking", body: "A rename." },
  ]);
  assert.equal(
    section,
    [
      "## 0.4.0 (2026-11-17)",
      "",
      "### Breaking",
      "",
      "- A rename.",
      "",
      "### Added",
      "",
      "- A thing,",
      "  over two lines.",
      "",
      "  And a paragraph.",
      "",
      "### Fixed",
      "",
      "- One fix.",
      "",
    ].join("\n"),
  );
  const changelog = insertRelease(
    "# Changelog\n\nIntro.\n\n## 0.3.0 (2026-11-14)\n\n- Old.\n",
    section,
  );
  assert.equal(newestRelease(changelog), "0.4.0");
  assert.equal(releaseNotes(changelog, "0.3.0"), "- Old.\n");
  assert.match(
    releaseNotes(changelog, "0.4.0") ?? "",
    /^### Breaking\n\n- A rename\.\n[\s\S]*- One fix\.\n$/,
  );
  assert.equal(releaseNotes(changelog, "0.2.0"), undefined);
});

/** A package manifest's path in a fixture, which the repository does not hold. */
const fixtureManifest = (name: string) =>
  join("client", "packages", name, "package.json");

function fixture(versions: { a: string; cargo: string }): string {
  const root = mkdtempSync(join(tmpdir(), "calque-change-notes-"));
  const write = (path: string, text: string) => {
    mkdirSync(dirname(join(root, path)), { recursive: true });
    writeFileSync(join(root, path), text);
  };
  write(
    fixtureManifest("a"),
    `{\n  "name": "@calque/a",\n  "version": "${versions.a}",\n  "dependencies": { "x": "${versions.a}" }\n}\n`,
  );
  write(
    fixtureManifest("b"),
    `{\n  "name": "@calque/b",\n  "version": "${versions.cargo}"\n}\n`,
  );
  write(
    fixtureManifest("private"),
    `{\n  "name": "private",\n  "private": true,\n  "version": "9.9.9"\n}\n`,
  );
  write(
    "server/apps/calque/Cargo.toml",
    `[package]\nname = "calque"\nversion = "${versions.cargo}"\n\n[dependencies]\nx = "${versions.cargo}"\n`,
  );
  write(
    "Cargo.lock",
    `[[package]]\nname = "bytes"\nversion = "${versions.cargo}"\n\n[[package]]\nname = "calque"\nversion = "${versions.cargo}"\n`,
  );
  write(
    "CHANGELOG.md",
    `# Changelog\n\n## ${versions.cargo} (2026-11-14)\n\n- Old.\n`,
  );
  return root;
}

test("preparing a release moves every published version and writes the section", (t) => {
  const root = fixture({ a: "0.3.0", cargo: "0.3.0" });
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, ".changes"));
  writeFileSync(join(root, ".changes/README.md"), "Not a note.\n");
  writeFileSync(join(root, ".changes/fix.md"), note("fixed", "A fix."));
  writeFileSync(join(root, ".changes/thing.md"), note("added", "A thing."));

  const prepared = prepareRelease(root, { date: "2026-11-17" });
  assert.equal(prepared.previous, "0.3.0");
  assert.equal(prepared.version, "0.4.0");
  assert.deepEqual(
    prepared.notes.map(({ file }) => file),
    ["fix.md", "thing.md"],
  );
  assert.equal(currentVersion(root), "0.4.0");
  const read = (path: string) => readFileSync(join(root, path), "utf8");
  assert.match(
    read(fixtureManifest("a")),
    /"version": "0\.4\.0",\n {2}"dependencies": \{ "x": "0\.3\.0" \}/,
  );
  assert.match(read(fixtureManifest("private")), /"9\.9\.9"/);
  assert.match(
    read("server/apps/calque/Cargo.toml"),
    /^version = "0\.4\.0"\n[\s\S]*x = "0\.3\.0"/m,
  );
  assert.equal(
    read("Cargo.lock"),
    `[[package]]\nname = "bytes"\nversion = "0.3.0"\n\n[[package]]\nname = "calque"\nversion = "0.4.0"\n`,
  );
  assert.equal(newestRelease(read("CHANGELOG.md")), "0.4.0");
  assert.equal(releaseNotes(read("CHANGELOG.md"), "0.3.0"), "- Old.\n");
});

test("preparing a release refuses without notes, across versions, or going back", (t) => {
  const agreeing = fixture({ a: "0.3.0", cargo: "0.3.0" });
  const split = fixture({ a: "0.2.0", cargo: "0.3.0" });
  t.after(() => {
    rmSync(agreeing, { recursive: true, force: true });
    rmSync(split, { recursive: true, force: true });
  });
  assert.throws(
    () => prepareRelease(agreeing, { date: "2026-11-17" }),
    /no change note/,
  );
  for (const root of [agreeing, split]) {
    mkdirSync(join(root, ".changes"));
    writeFileSync(join(root, ".changes/fix.md"), note("fixed", "A fix."));
  }
  assert.throws(
    () => prepareRelease(split, { date: "2026-11-17" }),
    /different versions/,
  );
  for (const version of ["0.3.0", "0.2.9", "0.3.0-rc.1", "next"])
    assert.throws(
      () => prepareRelease(agreeing, { version, date: "2026-11-17" }),
      /after|not a version/,
      version,
    );
  const changelog = join(agreeing, "CHANGELOG.md");
  writeFileSync(
    changelog,
    insertRelease(readFileSync(changelog, "utf8"), "## 0.3.1\n\n- Early.\n"),
  );
  assert.throws(
    () => prepareRelease(agreeing, { date: "2026-11-17" }),
    /already has a section for 0\.3\.1/,
  );
  assert.equal(currentVersion(agreeing), "0.3.0");
  assert.equal(
    prepareRelease(agreeing, { version: "0.3.5", date: "2026-11-17" }).version,
    "0.3.5",
  );
});
