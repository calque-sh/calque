/**
 * The `brotli-large-window` body the client package's session tests load,
 * read on the Node side for a suite that runs in the browser. The brotli
 * 1.2.0 command-line tool wrote it, under `brotli -q 11 --large_window=25`,
 * and the session tests hold the file to its SHA-256.
 */
import { readFileSync } from "node:fs";

import type { BrowserCommand } from "vitest/node";

declare module "vitest/browser" {
  interface BrowserCommands {
    /** The fixture body, as base64. */
    readBrotliFixture: () => Promise<string>;
  }
}

export const readBrotliFixture: BrowserCommand<[], string> = () =>
  readFileSync(
    new URL(
      "../../client/tests/fixtures/artifact.brotli-large-window",
      import.meta.url,
    ),
  ).toString("base64");
