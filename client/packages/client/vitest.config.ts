import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

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
      // Firefox: this project proves only that a browser outside the
      // supported profile is refused before any request or allocation.
      // `scripts/verify-client-browser-consumer.ts` drives the packaged client
      // in Chromium against a real artifact.
      {
        test: {
          name: "unsupported-browser",
          include: ["tests/unsupported-browser.browser.test.ts"],
          browser: {
            enabled: true,
            headless: true,
            instances: [{ browser: "firefox" }],
            provider: playwright(),
          },
        },
      },
    ],
  },
});
