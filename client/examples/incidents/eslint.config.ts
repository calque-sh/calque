import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

/**
 * Node builtins spelled without the `node:` prefix. Both spellings resolve, so
 * banning only the prefixed one leaves `import ... from "crypto"` through.
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
  "stream",
  "util",
  "worker_threads",
  "zlib",
];

const BROWSER_BOUNDARY_MESSAGE =
  "src/ is bundled for the browser and cannot import Node builtins.";

/**
 * `no-restricted-imports` only inspects static import declarations, so dynamic
 * `import()` needs a syntax selector to cover the same targets.
 */
const DYNAMIC_IMPORT_SELECTOR = `ImportExpression > Literal[value=/^(node:.*|${BARE_NODE_BUILTINS.join("|")})$/]`;

/**
 * A computed specifier -- `import(name)` or `` import(`${base}/x`) `` -- has no
 * literal for the selector above to read, so it would slip the boundary
 * silently. Requiring a literal keeps every import in `src/` checkable; lazy
 * chunks are unaffected, since `import("./thing.js")` is already a literal.
 */
const COMPUTED_IMPORT_SELECTOR =
  'ImportExpression:not([source.type="Literal"])';

export default tseslint.config(
  { ignores: ["dist", "coverage"] },
  eslint.configs.recommended,
  ...tseslint.configs.strict,
  {
    files: ["**/*.ts", "**/*.tsx"],
    languageOptions: {
      parserOptions: { tsconfigRootDir: import.meta.dirname },
    },
  },
  {
    // `src/` is the browser bundle. `vite.config.ts` runs on the Node side of
    // the Vite server; pulling a Node builtin across this line breaks the
    // build with a bundler message about an externalized builtin rather than
    // a useful one.
    files: ["src/**/*.ts", "src/**/*.tsx"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["node:*", ...BARE_NODE_BUILTINS],
              message: BROWSER_BOUNDARY_MESSAGE,
            },
          ],
        },
      ],
      "no-restricted-syntax": [
        "error",
        {
          selector: DYNAMIC_IMPORT_SELECTOR,
          message: BROWSER_BOUNDARY_MESSAGE,
        },
        {
          selector: COMPUTED_IMPORT_SELECTOR,
          message:
            "Dynamic import with a computed specifier cannot be checked against the Node/browser boundary. Use a literal specifier.",
        },
      ],
    },
  },
);
