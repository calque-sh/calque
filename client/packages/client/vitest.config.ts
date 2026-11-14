import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

// `CALQUE_BROWSER` runs the browser suite in one engine; unset, it runs in
// Chromium and Firefox.
const engines = ["chromium", "firefox", "webkit"] as const;
const requested = process.env["CALQUE_BROWSER"];
const instances = (
  requested === undefined
    ? (["chromium", "firefox"] as const)
    : engines.filter((name) => name === requested)
).map((browser) => ({ browser }));
if (instances.length === 0)
  throw new Error(`CALQUE_BROWSER must be one of ${engines.join(", ")}`);

export default defineConfig({
  optimizeDeps: {
    include: [
      "@bufbuild/protobuf",
      "@bufbuild/protobuf/codegenv2",
      "@bufbuild/protobuf/wire",
    ],
  },
  test: {
    projects: [
      {
        test: {
          name: "unit",
          environment: "node",
          include: ["src/**/*.test.ts"],
        },
      },
      // `scripts/verify-client-browser-consumer.ts` drives the packaged
      // client in Chromium against a real artifact.
      {
        test: {
          name: "browser",
          include: ["tests/**/*.browser.test.ts"],
          browser: {
            enabled: true,
            headless: true,
            instances,
            provider: playwright(),
          },
        },
      },
    ],
  },
});
