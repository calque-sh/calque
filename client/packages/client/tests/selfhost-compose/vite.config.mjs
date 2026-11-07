import { fileURLToPath, URL } from "node:url";

import { calque } from "@calque/client/vite";

// A build-only Vite project: `scripts/selfhost-compose-e2e.ts` builds this page
// and serves the output itself, so the page addresses the containerized server
// directly. `pnpm selfhost:build` typechecks and builds it under
// `consumer:build`, so `pnpm verify` covers its types without Docker.
export default {
  plugins: [calque()],
  build: {
    outDir: fileURLToPath(
      new URL(
        "../../../../../target/client-selfhost-compose/",
        import.meta.url,
      ),
    ),
    emptyOutDir: true,
  },
};
