import { fileURLToPath, URL } from "node:url";

import { calque } from "@calque/client/vite";

// A build-only Vite project, the same shape as `tests/serve-mutation`:
// `scripts/serve-tenants-e2e.ts` builds this page and serves the output from
// its own origin, so every Calque request the page makes is cross-origin to
// `calque serve` and goes through `--allow-origin`.
export default {
  plugins: [calque()],
  build: {
    outDir: fileURLToPath(
      new URL("../../../../../target/client-serve-tenants/", import.meta.url),
    ),
    emptyOutDir: true,
  },
};
