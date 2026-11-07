import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  collectPublicDeclarationGraph,
  type PublicDeclarationGraph,
} from "./lib/public-declaration-graph.ts";
// The client package's TypeScript, which the API gate walks the declarations
// with.
import type * as TypeScriptApi from "../client/packages/client/node_modules/typescript";

const ts = createRequire(
  new URL("../client/packages/client/package.json", import.meta.url),
)("typescript") as typeof TypeScriptApi;

/** A declaration file whose three doc comments are given, in order. */
function declarations(alias: string, constant: string, member: string) {
  return [
    `/** ${alias} */`,
    `export type Recovery = "none" | "retry";`,
    `/** ${constant} */`,
    `export declare const LIMIT: number;`,
    `export interface Options {`,
    `  /** ${member} */`,
    `  readonly recovery?: Recovery;`,
    `}`,
    ``,
  ].join("\n");
}

function graph(source: string): PublicDeclarationGraph {
  const root = mkdtempSync(join(tmpdir(), "calque-declaration-graph-"));
  const entryPath = join(root, "index.d.ts");
  writeFileSync(entryPath, source);
  return collectPublicDeclarationGraph({
    ts,
    entryPath,
    declarationRoots: [{ root, prefix: "dist" }],
    label: "fixture",
  });
}

test("the digest moves with every doc comment a declaration carries", () => {
  const base = graph(declarations("before", "before", "before"));
  assert.deepEqual(
    base.records.map((record) => record.symbol),
    ["LIMIT", "Options", "Recovery", "recovery"],
  );
  assert.equal(
    graph(declarations("before", "before", "before")).sha256,
    base.sha256,
  );
  // The comment above a top-level declaration sits in its leading trivia,
  // outside `getText`, and a variable's sits on its statement; the member's
  // sits inside its interface's text.
  for (const changed of [
    declarations("after", "before", "before"),
    declarations("before", "after", "before"),
    declarations("before", "before", "after"),
  ])
    assert.notEqual(graph(changed).sha256, base.sha256);
});
