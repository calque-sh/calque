import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

/**
 * Node builtins spelled without the `node:` prefix. Both spellings resolve, so
 * banning only the prefixed one lets `import ... from "crypto"` through.
 */
const BARE_NODE_BUILTINS = [
  "buffer",
  "child_process",
  "crypto",
  "fs",
  "http",
  "https",
  "os",
  "path",
  "sqlite",
  "stream",
  "util",
  "worker_threads",
  "zlib",
];

const NODE_MESSAGE =
  "@calque/core is consumed in the browser through its public entry. Node builtins belong in *.fixture.ts, *.test.ts or *.measure.ts, which are not part of the published browser surface.";

const FIXTURE_MESSAGE =
  "Importing a fixture from a browser module would pull its Node builtins into the public entry, where it surfaces downstream as an opaque bundler externalization.";

const NODE_SPECIFIERS = ["node:.*", ...BARE_NODE_BUILTINS].join("|");
const DYNAMIC_NODE_SELECTOR = `ImportExpression > Literal[value=/^(${NODE_SPECIFIERS})$/]`;
const DYNAMIC_FIXTURE_SELECTOR =
  "ImportExpression > Literal[value=/\\.fixture(\\.[jt]s)?$/]";

/**
 * A computed specifier, `import(name)` or a template, has no literal to check
 * against the boundary, so it is refused. Literal dynamic imports are
 * unaffected.
 */
const COMPUTED_IMPORT_SELECTOR =
  'ImportExpression:not([source.type="Literal"])';

export default tseslint.config(
  { ignores: ["dist", "coverage"] },
  eslint.configs.recommended,
  ...tseslint.configs.strict,
  {
    files: ["**/*.ts"],
    languageOptions: {
      parserOptions: { tsconfigRootDir: import.meta.dirname },
    },
  },
  {
    // Everything under `src/` but test scaffolding ships to browsers, also
    // through `@calque/worker`'s chunks, where a Node builtin would fail the
    // consumer's build rather than ours.
    files: ["src/**/*.ts"],
    ignores: ["src/**/*.fixture.ts", "src/**/*.test.ts", "src/**/*.measure.ts"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            { group: ["node:*", ...BARE_NODE_BUILTINS], message: NODE_MESSAGE },
            {
              group: ["**/*.fixture.js", "**/*.fixture.ts"],
              message: FIXTURE_MESSAGE,
            },
          ],
        },
      ],
      "no-restricted-syntax": [
        "error",
        { selector: DYNAMIC_NODE_SELECTOR, message: NODE_MESSAGE },
        { selector: DYNAMIC_FIXTURE_SELECTOR, message: FIXTURE_MESSAGE },
        {
          selector: COMPUTED_IMPORT_SELECTOR,
          message:
            "Dynamic import with a computed specifier cannot be checked against the Node/browser boundary. Use a literal specifier.",
        },
      ],
    },
  },
);
