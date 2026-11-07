import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import { stalePathReferences } from "./verify-no-stale-path-references.ts";

// Every repository path below is joined from parts, and every quote is `q`, so
// this file's own source holds no quoted path for the gate to resolve.
const p = (...parts: string[]) => parts.join("/");
const q = '"';
const quoted = (value: string) => `${q}${value}${q}`;

test("a quoted source path that names no file is found, and one that resolves is not", () => {
  const root = mkdtempSync(join(tmpdir(), "calque-stale-paths-"));
  const files: Record<string, string> = {
    [p("scripts", "present.ts")]: "export {};\n",
    [p("client", "packages", "lib", "package.json")]: "{}\n",
    [p("client", "packages", "lib", "src", "built.ts")]: "export {};\n",
    [p("client", "packages", "lib", "scripts", "own.mjs")]: "\n",
    [p("client", "packages", "lib", "scripts", "check.mjs")]: [
      // From the package that owns the referencing file.
      `spawn(${quoted(p("scripts", "own.mjs"))});`,
      "",
    ].join("\n"),
    [p("scripts", "gate.ts")]: [
      `readFileSync(${quoted(p("scripts", "present.ts"))});`,
      `readFileSync(${quoted(p("scripts", "renamed.ts"))});`,
      // A package's dist resolves through the module it is built from.
      `import(${quoted(p("client", "packages", "lib", "dist", "built.js"))});`,
      `import(${quoted(p("client", "packages", "lib", "dist", "gone.js"))});`,
      // Not claims about a file: a glob, an extension outside the list, a
      // top-level directory outside the list.
      `glob(${quoted(p("scripts", "*.test.ts"))});`,
      `read(${quoted(p("docs", "notes.md"))});`,
      `read(${quoted(p("tmp", "scratch.ts"))});`,
      "",
    ].join("\n"),
  };
  try {
    for (const [file, contents] of Object.entries(files)) {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      writeFileSync(join(root, file), contents);
    }
    assert.deepEqual(stalePathReferences(root), [
      {
        file: p("scripts", "gate.ts"),
        line: 2,
        reference: p("scripts", "renamed.ts"),
      },
      {
        file: p("scripts", "gate.ts"),
        line: 4,
        reference: p("client", "packages", "lib", "dist", "gone.js"),
      },
    ]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
