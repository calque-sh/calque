#!/usr/bin/env node
// Refuses a `.js` file beside a `.ts` of the same base name: one of them is
// output from a compiler pointed at the source tree. Vitest would run an
// emitted test twice, and an emitted `eslint.config.js` silently shadows the
// real `eslint.config.ts`.
import { readdirSync, statSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const SKIP = new Set([
  "node_modules",
  ".git",
  "dist",
  "target",
  "coverage",
  ".turbo",
]);

const TYPESCRIPT = [".ts", ".tsx", ".mts", ".cts"];
const JAVASCRIPT = [".js", ".jsx", ".mjs", ".cjs"];

/** Config files whose shadowing is silent, and therefore worth naming. */
const SHADOWS = new Set(["eslint.config", "vitest.config", "vite.config"]);

function* walk(directory: string): Generator<string> {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (SKIP.has(entry.name)) continue;
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) yield* walk(full);
    else if (entry.isFile()) yield full;
  }
}

export interface EmittedFinding {
  readonly emitted: string;
  readonly source: string;
  readonly shadows: boolean;
}

/** Every JavaScript file under `root` with a TypeScript file of its name beside it. */
export function emittedJavaScript(root: string): EmittedFinding[] {
  const findings: EmittedFinding[] = [];
  for (const file of walk(root)) {
    const extension = path.extname(file);
    if (!JAVASCRIPT.includes(extension)) continue;
    const stem = file.slice(0, -extension.length);
    const source = TYPESCRIPT.map((candidate) => `${stem}${candidate}`).find(
      (candidate) => {
        try {
          return statSync(candidate).isFile();
        } catch {
          return false;
        }
      },
    );
    if (!source) continue;
    findings.push({
      emitted: path.relative(root, file),
      source: path.relative(root, source),
      shadows: SHADOWS.has(path.basename(stem)),
    });
  }
  return findings.sort((a, b) => a.emitted.localeCompare(b.emitted));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const findings = emittedJavaScript(
    path.resolve(path.dirname(fileURLToPath(import.meta.url)), ".."),
  );
  if (findings.length === 0) {
    console.log(
      "Emitted-JavaScript check: PASS (no .js beside its .ts source)",
    );
    process.exit(0);
  }
  console.error(
    `Emitted-JavaScript check: FAIL — ${findings.length} compiled file(s) in the source tree.`,
  );
  console.error(
    "A .js and a .ts with the same name are never both source. Delete the .js and",
    "\ngive the compiler an --outDir outside the source tree.\n",
  );
  for (const finding of findings)
    console.error(
      `  ${finding.emitted}  (compiled from ${finding.source})${
        finding.shadows ? "  (shadows the real config)" : ""
      }`,
    );
  process.exit(1);
}
