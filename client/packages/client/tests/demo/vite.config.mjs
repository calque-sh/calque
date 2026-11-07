import { fileURLToPath, URL } from "node:url";

import { calque } from "@calque/client/vite";

export default {
  plugins: [calque()],
  build: {
    outDir: fileURLToPath(
      new URL("../../../../../target/client-demo/", import.meta.url),
    ),
    emptyOutDir: true,
  },
};
