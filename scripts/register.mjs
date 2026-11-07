// The harness's TypeScript loader. Plain JavaScript that imports only node:
// builtins and oxc-transform, so a broken toolchain cannot stop the gate that
// reports it.
//
// It transpiles from source on every run, because a precompiled tree goes
// stale silently and verifies a harness that is not the one in the working
// tree.
//
// Usage:
//   node --import ./scripts/register.mjs scripts/<entry>.ts
//   node --import ./scripts/register.mjs --test "scripts/**/*.test.ts"

import { readFileSync } from "node:fs";
import { registerHooks } from "node:module";
import process from "node:process";
import { fileURLToPath } from "node:url";

// Node ignores the inline source maps the `load` hook emits unless asked.
process.setSourceMapsEnabled(true);

// Resolved eagerly so a missing transpiler is reported here, with the command
// that fixes it.
let transformSync;
try {
  ({ transformSync } = await import("oxc-transform"));
} catch (cause) {
  throw new Error(
    "The harness TypeScript loader could not load oxc-transform, so no scripts/**/*.ts file can run.\n" +
      "  Fix:      pnpm install\n" +
      "  Verify:   node -e \"import('oxc-transform').then(() => console.log('ok'))\"\n" +
      "Every gate under scripts/ loads through this file, so none of them can run until it does.",
    { cause },
  );
}

registerHooks({
  // Client sources name their siblings with NodeNext `.js` specifiers, which
  // exist only after a build, so a relative `.js` import from a `.ts` file
  // that finds no `.js` is retried as the `.ts`.
  resolve(specifier, context, nextResolve) {
    try {
      return nextResolve(specifier, context);
    } catch (error) {
      if (
        error?.code !== "ERR_MODULE_NOT_FOUND" ||
        !context.parentURL?.startsWith("file:") ||
        !context.parentURL.endsWith(".ts") ||
        !/^\.\.?\//.test(specifier) ||
        !specifier.endsWith(".js")
      )
        throw error;
      try {
        return nextResolve(`${specifier.slice(0, -3)}.ts`, context);
      } catch {
        throw error;
      }
    }
  },
  load(url, context, nextLoad) {
    if (!url.startsWith("file:") || !url.endsWith(".ts"))
      return nextLoad(url, context);
    const path = fileURLToPath(url);
    // Erasing types moves lines, so stack traces need the inline map to name
    // the real source line.
    const { code, map, errors } = transformSync(
      path,
      readFileSync(path, "utf8"),
      {
        sourcemap: true,
      },
    );
    // oxc reports syntax errors only and emits through type errors, so
    // `pnpm typecheck:harness` is a separate, required gate step.
    if (errors.length > 0) {
      throw new Error(
        `${path} failed to transpile:\n${errors.map(String).join("\n")}`,
      );
    }
    const inlineMap = map
      ? `\n//# sourceMappingURL=data:application/json;base64,${Buffer.from(JSON.stringify(map)).toString("base64")}\n`
      : "";
    return {
      format: "module",
      shortCircuit: true,
      source: `${code}${inlineMap}`,
    };
  },
});
