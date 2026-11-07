import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

import { readBrotliFixture } from "./tests/brotli-fixture.js";
import { encodeColumnarFixture } from "./tests/columnar-fixture.js";
import { encodePackedFixture } from "./tests/packed-fixture.js";

export default defineConfig({
  optimizeDeps: {
    include: ["@bufbuild/protobuf", "@bufbuild/protobuf/codegenv2"],
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
      {
        test: {
          name: "browser",
          include: ["tests/**/*.browser.test.ts"],
          browser: {
            enabled: true,
            headless: true,
            instances: [{ browser: "chromium" }],
            provider: playwright(),
            // The direct-columnar suite's artifact is minted on this side:
            // the fixture encoder needs Node. So does reading the decoder
            // suite's bodies from their files, and encoding the packed one.
            commands: {
              encodeColumnarFixture,
              encodePackedFixture,
              readBrotliFixture,
            },
          },
        },
      },
    ],
  },
});
