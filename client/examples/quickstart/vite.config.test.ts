import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer, mergeConfig, type ViteDevServer } from "vite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { quickstartConfig } from "./vite.config.js";

const repositoryRoot = fileURLToPath(new URL("../../../", import.meta.url));

describe("the quickstart dev server", () => {
  let directory = "";
  let server: ViteDevServer | undefined;
  let base = "";

  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), "calque-quickstart-"));
    server = await createServer(
      mergeConfig(quickstartConfig(join(directory, "grant.json")), {
        configFile: false,
        cacheDir: join(directory, "vite"),
        logLevel: "silent",
        server: { port: 0, strictPort: false },
      }),
    );
    await server.listen();
    const url = server.resolvedUrls?.local[0];
    if (!url) throw new Error("the dev server reported no local URL");
    base = url;
  }, 30_000);

  afterAll(async () => {
    await server?.close();
    await rm(directory, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 100,
    });
  });

  it("serves grant.json as it is on disk at each request", async () => {
    const missing = await fetch(new URL("grant.json", base));
    expect(missing.status).toBe(404);
    expect(await missing.text()).toContain("calque-dev-grant");

    for (const token of ["first", "second"]) {
      await writeFile(join(directory, "grant.json"), JSON.stringify({ token }));
      const response = await fetch(new URL("grant.json", base));
      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      expect(await response.json()).toEqual({ token });
    }
  });

  it("refuses files from the repository root, where the signing key sits", async () => {
    const response = await fetch(
      new URL(`@fs${join(repositoryRoot, "package.json")}`, base),
    );
    expect(response.status).toBe(403);
  });

  it("serves the page and the client package it imports", async () => {
    const page = await fetch(new URL("main.ts", base));
    expect(page.status).toBe(200);
    const client = /from "([^"]*\/client\/packages\/client\/[^"]*)"/u.exec(
      await page.text(),
    )?.[1];
    expect(client).toBeDefined();
    const entry = await fetch(new URL(client ?? "", base));
    expect(entry.status).toBe(200);
  }, 30_000);
});
