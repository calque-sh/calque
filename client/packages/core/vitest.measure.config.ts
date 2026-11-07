import { defineConfig } from "vitest/config";

/**
 * Runs the `*.measure.ts` benchmarks, apart from `pnpm test` so tests never
 * depend on timing: `pnpm --filter @calque/core run measure`.
 */
export default defineConfig({
  test: {
    include: ["src/**/*.measure.ts"],
    // Each measurement reports on stdout, which some Vitest reporters drop
    // for a passing test.
    disableConsoleIntercept: true,
    testTimeout: 900_000,
  },
});
