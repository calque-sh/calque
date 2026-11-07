import { fileURLToPath, URL } from "node:url";

import { calque } from "@calque/client/vite";

// A build-only Vite project, the same shape as `tests/serve-mutation`:
// `scripts/serve-revocation-e2e.ts` builds this page and serves the output
// from its own origin, so every Calque request the page makes is cross-origin
// to `calque serve`.
export default {
  plugins: [calque()],
  build: {
    outDir: fileURLToPath(
      new URL(
        "../../../../../target/client-serve-revocation/",
        import.meta.url,
      ),
    ),
    emptyOutDir: true,
  },
};
