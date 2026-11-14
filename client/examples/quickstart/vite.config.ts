import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin, type UserConfig } from "vite";

/**
 * Quickstart only. Serves the `grant.json` that the README's
 * `calque-grant mint` step wrote at `/grant.json`, read again on every request
 * so a grant minted after the old one expired is picked up without a restart.
 * In production the application's backend issues the grant.
 */
function quickstartGrant(grantFile: string): Plugin {
  return {
    name: "calque-quickstart-grant",
    configureServer(server) {
      server.middlewares.use("/grant.json", (_request, response) => {
        readFile(grantFile, "utf8").then(
          (grant) => {
            response.setHeader("content-type", "application/json");
            response.setHeader("cache-control", "no-store");
            response.end(grant);
          },
          () => {
            response.statusCode = 404;
            response.setHeader("content-type", "text/plain; charset=utf-8");
            response.end(
              `${grantFile} is missing; mint a grant with the README's calque-grant mint step`,
            );
          },
        );
      });
    },
  };
}

/** The page's dev server, given where the README's `grant.json` is. */
export function quickstartConfig(grantFile: string): UserConfig {
  return {
    root: fileURLToPath(new URL(".", import.meta.url)),
    plugins: [quickstartGrant(grantFile)],
    server: {
      // The composition's default allowed origin.
      host: "127.0.0.1",
      port: 5173,
      strictPort: true,
      // The repository root holds the grant's signing key beside grant.json,
      // so the dev server reads only this page, the workspace packages and
      // their dependencies, never the root itself.
      fs: {
        allow: [
          fileURLToPath(new URL(".", import.meta.url)),
          fileURLToPath(new URL("../../packages", import.meta.url)),
          fileURLToPath(new URL("../../../node_modules", import.meta.url)),
        ],
      },
    },
    // The engine worker is an ES module so its dynamic imports stay separate
    // chunks instead of being inlined into every mount.
    worker: { format: "es" },
  };
}

export default defineConfig(
  quickstartConfig(
    fileURLToPath(new URL("../../../grant.json", import.meta.url)),
  ),
);
