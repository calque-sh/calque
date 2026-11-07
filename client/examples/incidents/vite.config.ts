import { calque } from "@calque/client/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";
import { requiredTables } from "./src/calque.start.generated.ts";

const PAGE_ENTRY = '<script type="module" src="/src/main.ts"></script>';
const HARNESS_ENTRY = '<script type="module" src="/src/harness.ts"></script>';

/**
 * `vite build --mode harness` loads `src/harness.ts` in place of the page's
 * entry: it runs the page, then publishes what the benchmark and the
 * end-to-end test read.
 */
function harnessEntry(): Plugin {
  return {
    name: "calque-harness-entry",
    transformIndexHtml: {
      order: "pre",
      handler(html) {
        if (!html.includes(PAGE_ENTRY))
          throw new Error("index.html does not load /src/main.ts");
        return html.replace(PAGE_ENTRY, HARNESS_ENTRY);
      },
    },
  };
}

export default defineConfig(({ mode }) => ({
  plugins: [
    react(),
    calque({
      bootstrap: {
        requiredTables,
        // The 1M-row working set decodes to about 350 MB.
        memoryBudgetBytes: 2 * 1024 ** 3,
      },
      bindings: { out: "src/calque.generated.ts" },
    }),
    ...(mode === "harness" ? [harnessEntry()] : []),
  ],
  // `@calque/react` is a linked workspace package with its own `react`
  // devDependency; two copies of React in one bundle break every hook.
  resolve: { dedupe: ["react", "react-dom"] },
}));
