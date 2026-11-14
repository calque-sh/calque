import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { createServer as createHttpServer, type Server } from "node:http";
import type { AddressInfo, Server as NetServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, relative } from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  build,
  createLogger,
  createServer,
  type HotPayload,
  type InlineConfig,
  type Logger,
  preview,
  type ProxyOptions,
  resolveConfig,
  type Rolldown,
  type ViteDevServer,
} from "vite";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";

import {
  DEFAULT_MEMORY_BUDGET_BYTES,
  startBootstrap,
  takeStartedBootstrap,
} from "./bootstrap-start.js";
import {
  BOOTSTRAP_START_SCRIPT,
  BOOTSTRAP_START_SCRIPT_SHA256,
} from "./bootstrap-start-script.js";
import { calque, type CalquePluginOptions } from "./vite.js";
import { stubBrowserFeatures } from "./browser.fixture.js";

const PACKAGE_ROOT = fileURLToPath(new URL("..", import.meta.url));
const PAGE_ROOT = fileURLToPath(new URL("../tests/vite-page", import.meta.url));
const BOOTSTRAP = {
  requiredTables: ["public.events"],
  memoryBudgetBytes: 64 * 1024 * 1024,
};
const SERVICE_BEARER = "dev-service-grant";
const CONNECTION =
  '<script type="application/json" id="calque-connection">{"endpoint":"/_calque/","bearerToken":"calque-dev-proxy"}</script>';
const INLINE_STARTER = `<script type="module">${BOOTSTRAP_START_SCRIPT}</script>`;

type Output = Rolldown.RolldownOutput["output"];

const temporary: string[] = [];

function temporaryDirectory(): string {
  const directory = mkdtempSync(join(tmpdir(), "calque-vite-"));
  temporary.push(directory);
  return directory;
}

/** A config for the fixture page that leaves no cache and watches nothing. */
function pageConfig(config: InlineConfig): InlineConfig {
  return {
    root: PAGE_ROOT,
    configFile: false,
    logLevel: "silent",
    cacheDir: join(temporaryDirectory(), "cache"),
    optimizeDeps: { noDiscovery: true, include: [] },
    ...config,
  };
}

async function builtPage(config: InlineConfig): Promise<Output> {
  const result = await build(
    pageConfig({ ...config, build: { write: false } }),
  );
  if (!("output" in result)) throw new Error("expected one build output");
  return result.output;
}

function chunksOf(output: Output): Rolldown.OutputChunk[] {
  return output.flatMap((item) => (item.type === "chunk" ? [item] : []));
}

function pageOf(output: Output): string {
  const page = output.find((item) => item.fileName === "index.html");
  if (page?.type !== "asset" || typeof page.source !== "string")
    throw new Error("no index.html");
  return page.source;
}

function jsonElement(html: string, id: string): unknown {
  const opening = `<script type="application/json" id="${id}">`;
  const at = html.indexOf(opening);
  if (at === -1) throw new Error(`no ${id} element`);
  const text = html.slice(at + opening.length, html.indexOf("</script>", at));
  expect(text).not.toContain("<");
  return JSON.parse(text);
}

/** A stand-in for calque dev that records what reaches it. */
async function upstream(): Promise<{
  readonly endpoint: string;
  readonly received: { url: string; authorization: string | undefined }[];
  readonly server: Server;
}> {
  const received: { url: string; authorization: string | undefined }[] = [];
  const server = createHttpServer((request, response) => {
    received.push({
      url: request.url ?? "",
      authorization: request.headers.authorization,
    });
    request.resume();
    response.end("ok");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return { endpoint: `http://127.0.0.1:${String(port)}`, received, server };
}

function origin(server: NetServer | null | undefined): string {
  const address = server?.address() as AddressInfo | null | undefined;
  if (!address) throw new Error("the server is not listening");
  return `http://127.0.0.1:${String(address.port)}`;
}

afterEach(() => {
  vi.unstubAllEnvs();
});

afterAll(() => {
  for (const directory of temporary)
    rmSync(directory, { recursive: true, force: true });
});

function startChunkOf(output: Output): Rolldown.OutputChunk {
  const starters = chunksOf(output).filter((chunk) => chunk.name === "start");
  expect(starters).toHaveLength(1);
  const [starter] = starters;
  if (!starter) throw new Error("no start chunk");
  return starter;
}

/** What a page's `fetch` was called with while `run` ran on a Chromium page. */
async function requestsSentBy(run: () => unknown): Promise<unknown[][]> {
  const fetcher = vi.fn<typeof fetch>(() => new Promise(() => undefined));
  vi.stubGlobal("fetch", fetcher);
  stubBrowserFeatures();
  vi.stubGlobal("document", {
    getElementById: (id: string) =>
      id === "calque-connection"
        ? { textContent: '{"endpoint":"https://sync.test/","bearerToken":"g"}' }
        : null,
  });
  vi.spyOn(globalThis.crypto, "randomUUID").mockReturnValue(
    "00000000-0000-4000-8000-000000000000",
  );
  try {
    await run();
    expect(takeStartedBootstrap()).toBeDefined();
    return fetcher.mock.calls;
  } finally {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  }
}

describe("a page built with calque({ bootstrap })", () => {
  let output: Output;

  beforeAll(async () => {
    output = await builtPage({
      plugins: [calque({ bootstrap: BOOTSTRAP })],
    });
  }, 120_000);

  it("puts its start module and the starter it imports in a chunk that imports nothing and evaluates first", () => {
    const chunks = chunksOf(output);
    const starter = startChunkOf(output);
    expect(
      starter.moduleIds
        .map((id) => (id.startsWith("\0") ? id : relative(PACKAGE_ROOT, id)))
        .sort(),
    ).toEqual([
      "\0calque:start",
      "dist/bootstrap-start-entry.js",
      "dist/bootstrap-start.js",
    ]);
    // A static import here would load and evaluate before the request leaves.
    expect(starter.imports).toEqual([]);
    expect(starter.dynamicImports).toEqual([]);
    // The inline starter is already in the page's head.
    expect(starter.code).not.toContain("calque-bootstrap");

    // A browser evaluates a module's imports in order, depth first, so the
    // start chunk evaluates first only if following each chunk's first import
    // from the entry ends at it.
    const entries = chunks.filter((chunk) => chunk.isEntry);
    expect(entries).toHaveLength(1);
    const byFile = new Map(chunks.map((chunk) => [chunk.fileName, chunk]));
    const firstImports: string[] = [];
    for (let chunk = entries[0]; chunk && chunk.imports.length > 0;) {
      const current = chunk;
      const at = (file: string) => current.code.indexOf(`./${basename(file)}`);
      for (const file of current.imports) expect(at(file)).toBeGreaterThan(-1);
      const first = [...current.imports].sort((a, b) => at(a) - at(b))[0];
      if (first === undefined || firstImports.includes(first)) break;
      firstImports.push(first);
      chunk = byFile.get(first);
    }
    expect(firstImports.at(-1)).toBe(starter.fileName);
  });

  it("has a start chunk that sends, alone, the request startBootstrap sends with the bootstrap values", async () => {
    const { code } = startChunkOf(output);
    const fromChunk = await requestsSentBy(
      () => import(`data:text/javascript,${encodeURIComponent(code)}`),
    );
    expect(fromChunk).toHaveLength(1);
    expect(fromChunk).toEqual(
      await requestsSentBy(() => {
        startBootstrap(BOOTSTRAP);
      }),
    );
  });

  it("writes the inline starter verbatim after the charset and before every module script, with the bootstrap values", () => {
    const html = pageOf(output);
    const opening = '<script type="module">';
    const start = html.indexOf(opening);
    expect(start).toBeGreaterThan(html.indexOf("<meta charset="));
    // The text a browser hashes for the CSP is what lies between the tags.
    const text = html.slice(
      start + opening.length,
      html.indexOf("</script>", start),
    );
    expect(text).toBe(BOOTSTRAP_START_SCRIPT);
    expect(
      `sha256-${createHash("sha256").update(text, "utf8").digest("base64")}`,
    ).toBe(BOOTSTRAP_START_SCRIPT_SHA256);
    expect(html.indexOf(opening, start + 1)).toBe(-1);
    expect(html.indexOf('<script type="module"')).toBe(start);
    expect(jsonElement(html, "calque-bootstrap")).toEqual(BOOTSTRAP);
    expect(html.indexOf('id="calque-bootstrap"')).toBeLessThan(start);
    // A build has no calque dev to name.
    expect(html).not.toContain('id="calque-connection"');
  });

  it("starts the engine worker's entry chunk, and no other file, with V8's compile hint", () => {
    const hinted = output.flatMap((item) => {
      const text = item.type === "chunk" ? item.code : item.source;
      return typeof text === "string" &&
        text.includes("allFunctionsCalledOnLoad")
        ? [[basename(item.fileName), text.slice(0, text.indexOf("\n"))]]
        : [];
    });
    expect(hinted).toEqual([
      [
        expect.stringMatching(/^columnar-browser-worker-entry-[\w-]+\.js$/u),
        "//# allFunctionsCalledOnLoad",
      ],
    ]);
  });
});

describe("a page built with calque({ bootstrap }) and no memory budget", () => {
  it("carries the default memory budget in its head and its start module", async () => {
    const tables = { requiredTables: ["public.events"] };
    const output = await builtPage({
      plugins: [calque({ bootstrap: tables })],
    });
    const html = pageOf(output);
    expect(html).toContain(INLINE_STARTER);
    const request = {
      ...tables,
      memoryBudgetBytes: DEFAULT_MEMORY_BUDGET_BYTES,
    };
    expect(jsonElement(html, "calque-bootstrap")).toEqual(request);
    const { code } = startChunkOf(output);
    expect(
      await requestsSentBy(
        () => import(`data:text/javascript,${encodeURIComponent(code)}`),
      ),
    ).toEqual(
      await requestsSentBy(() => {
        startBootstrap(request);
      }),
    );
  }, 120_000);
});

describe("the dev server", () => {
  let service: Awaited<ReturnType<typeof upstream>>;
  let server: Awaited<ReturnType<typeof createServer>>;
  let page: string;

  beforeAll(async () => {
    service = await upstream();
    vi.stubEnv("CALQUE_LOCAL_SERVICE_ENDPOINT", service.endpoint);
    vi.stubEnv("CALQUE_LOCAL_BEARER_TOKEN", SERVICE_BEARER);
    server = await createServer(
      pageConfig({
        plugins: [calque({ bootstrap: BOOTSTRAP })],
        server: { host: "127.0.0.1", port: 0, watch: null },
      }),
    );
    await server.listen();
    page = await (
      await fetch(`${origin(server.httpServer)}/`, {
        headers: { accept: "text/html" },
      })
    ).text();
  }, 120_000);

  afterAll(async () => {
    await server.close();
    service.server.closeAllConnections();
    service.server.close();
  });

  it("serves the page with the proxy's connection and the starter ahead of every module script, then the start module", () => {
    const firstModule = page.indexOf('<script type="module"');
    expect(page.indexOf(CONNECTION)).toBeGreaterThan(-1);
    expect(page.indexOf(CONNECTION)).toBeLessThan(firstModule);
    expect(page.indexOf(INLINE_STARTER)).toBe(firstModule);
    const start = page.indexOf('<script type="module" src="/@calque/start">');
    expect(start).toBeGreaterThan(firstModule);
    expect(start).toBeLessThan(page.indexOf('src="/main.ts"'));
    expect(page).not.toContain(SERVICE_BEARER);
  });

  it("serves the start module, which calls startBootstrap with the bootstrap values", async () => {
    const response = await fetch(`${origin(server.httpServer)}/@calque/start`);
    expect(response.headers.get("content-type")).toMatch(/javascript/u);
    const code = await response.text();
    expect(code).toMatch(
      /^import \{ startBootstrap \} from "[^"]*\/dist\/bootstrap-start-entry\.js"/u,
    );
    expect(code).toContain(`startBootstrap(${JSON.stringify(BOOTSTRAP)});`);
  });

  it("swaps the page's placeholder for calque dev's bearer on bootstrap, changes and mutations, and drops any other", async () => {
    for (const path of [
      "/v1/bootstrap",
      "/v1/changes?cursor=c-1",
      "/v1/mutations",
    ])
      for (const [sent, forwarded] of [
        ["Bearer calque-dev-proxy", `Bearer ${SERVICE_BEARER}`],
        ["Bearer someone-else", undefined],
        [undefined, undefined],
      ] as const) {
        service.received.length = 0;
        const response = await fetch(
          `${origin(server.httpServer)}/_calque${path}`,
          {
            method: "POST",
            headers: sent === undefined ? {} : { authorization: sent },
          },
        );
        expect(await response.text()).toBe("ok");
        expect(service.received).toEqual([
          { url: path, authorization: forwarded },
        ]);
      }
  });

  it("forwards another path under /_calque/ as the page sent it", async () => {
    service.received.length = 0;
    await (
      await fetch(`${origin(server.httpServer)}/_calque/readyz`, {
        headers: { authorization: "Bearer calque-dev-proxy" },
      })
    ).text();
    expect(service.received).toEqual([
      { url: "/readyz", authorization: "Bearer calque-dev-proxy" },
    ]);
  });
});

describe("the preview server", () => {
  let service: Awaited<ReturnType<typeof upstream>>;
  let server: Awaited<ReturnType<typeof preview>>;

  beforeAll(async () => {
    const outDir = join(temporaryDirectory(), "dist");
    await build(
      pageConfig({ plugins: [calque()], build: { outDir, emptyOutDir: true } }),
    );
    service = await upstream();
    vi.stubEnv("CALQUE_LOCAL_SERVICE_ENDPOINT", service.endpoint);
    vi.stubEnv("CALQUE_LOCAL_BEARER_TOKEN", SERVICE_BEARER);
    server = await preview(
      pageConfig({
        plugins: [calque()],
        build: { outDir },
        preview: { host: "127.0.0.1", port: 0 },
      }),
    );
  }, 120_000);

  afterAll(async () => {
    await server.close();
    service.server.closeAllConnections();
    service.server.close();
  });

  it("serves the built page with the proxy's connection, whether asked for by name or by a route", async () => {
    for (const path of ["/", "/index.html", "/issues/7"]) {
      const response = await fetch(`${origin(server.httpServer)}${path}`, {
        headers: { accept: "text/html" },
      });
      expect(response.headers.get("cache-control")).toBe("no-store");
      const html = await response.text();
      expect(html.indexOf(CONNECTION)).toBeGreaterThan(-1);
      expect(html.indexOf(CONNECTION)).toBeLessThan(
        html.indexOf('<script type="module"'),
      );
      // calque() alone writes no starter.
      expect(html).not.toContain("calque-bootstrap");
      expect(html).not.toContain(SERVICE_BEARER);
    }
  });

  it("swaps the bearer on bootstrap as the dev server does", async () => {
    service.received.length = 0;
    await (
      await fetch(`${origin(server.httpServer)}/_calque/v1/bootstrap`, {
        method: "POST",
        headers: { authorization: "Bearer calque-dev-proxy" },
      })
    ).text();
    expect(service.received).toEqual([
      { url: "/v1/bootstrap", authorization: `Bearer ${SERVICE_BEARER}` },
    ]);
  });
});

describe("the calque dev the plugin proxies to", () => {
  /** A Vite root with `.calque-dev/client.env` two directories above it. */
  function rootUnder(clientEnv: string | undefined): {
    readonly root: string;
    readonly file: string;
  } {
    const project = temporaryDirectory();
    const file = join(project, ".calque-dev", "client.env");
    if (clientEnv !== undefined) {
      mkdirSync(join(project, ".calque-dev"));
      writeFileSync(file, clientEnv);
    }
    const root = join(project, "apps", "web");
    mkdirSync(root, { recursive: true });
    return { root, file };
  }

  async function proxied(
    root: string,
    command: "build" | "serve" = "serve",
  ): Promise<string | undefined> {
    const config = await resolveConfig(
      { root, configFile: false, logLevel: "silent", plugins: [calque()] },
      command,
    );
    const options = config.server.proxy?.["/_calque/"] as
      ProxyOptions | undefined;
    expect(config.preview.proxy?.["/_calque/"]).toEqual(options);
    return options?.target as string | undefined;
  }

  function withoutEnvironment(): void {
    vi.stubEnv("CALQUE_LOCAL_SERVICE_ENDPOINT", undefined);
    vi.stubEnv("CALQUE_LOCAL_BEARER_TOKEN", undefined);
  }

  it("is read from client.env in the nearest .calque-dev at or above Vite's root", async () => {
    withoutEnvironment();
    const { root } = rootUnder(
      "CALQUE_ENDPOINT=http://127.0.0.1:4100\nCALQUE_BEARER_TOKEN=abc_DEF-1\n",
    );
    expect(await proxied(root)).toBe("http://127.0.0.1:4100");
  });

  it("is read from the environment ahead of a client.env", async () => {
    vi.stubEnv("CALQUE_LOCAL_SERVICE_ENDPOINT", "http://127.0.0.1:4200");
    vi.stubEnv("CALQUE_LOCAL_BEARER_TOKEN", "abc");
    const { root } = rootUnder(
      "CALQUE_ENDPOINT=http://127.0.0.1:4100\nCALQUE_BEARER_TOKEN=abc\n",
    );
    expect(await proxied(root)).toBe("http://127.0.0.1:4200");
  });

  it("is none without either, and none for a build, which reads neither", async () => {
    withoutEnvironment();
    expect(await proxied(rootUnder(undefined).root)).toBeUndefined();
    vi.stubEnv("CALQUE_LOCAL_SERVICE_ENDPOINT", "http://localhost:4100");
    expect(
      await proxied(rootUnder("CALQUE_ENDPOINT=x\n").root, "build"),
    ).toBeUndefined();
  });

  it("leaves a preview's page with no module script as it was built", async () => {
    withoutEnvironment();
    const { root } = rootUnder(
      "CALQUE_ENDPOINT=http://127.0.0.1:4100\nCALQUE_BEARER_TOKEN=abc\n",
    );
    const outDir = join(temporaryDirectory(), "dist");
    mkdirSync(outDir);
    const page = "<!doctype html><title>static</title>\n";
    writeFileSync(join(outDir, "index.html"), page);
    const server = await preview({
      root,
      configFile: false,
      logLevel: "silent",
      plugins: [calque()],
      build: { outDir },
      preview: { host: "127.0.0.1", port: 0 },
    });
    try {
      const response = await fetch(`${origin(server.httpServer)}/`, {
        headers: { accept: "text/html" },
      });
      expect(response.status).toBe(200);
      expect(await response.text()).toBe(page);
    } finally {
      await server.close();
    }
  });

  const refused: readonly (readonly [
    string,
    Record<string, string | undefined>,
    string | undefined,
    RegExp,
  ])[] = [
    [
      "an endpoint without a bearer",
      { CALQUE_LOCAL_SERVICE_ENDPOINT: "http://127.0.0.1:4100" },
      undefined,
      /^CALQUE_LOCAL_SERVICE_ENDPOINT and CALQUE_LOCAL_BEARER_TOKEN must be provided together$/u,
    ],
    [
      "a bearer without an endpoint",
      { CALQUE_LOCAL_BEARER_TOKEN: "abc" },
      undefined,
      /^CALQUE_LOCAL_SERVICE_ENDPOINT and CALQUE_LOCAL_BEARER_TOKEN must be provided together$/u,
    ],
    [
      "an endpoint that is not numeric loopback",
      {
        CALQUE_LOCAL_SERVICE_ENDPOINT: "http://localhost:4100",
        CALQUE_LOCAL_BEARER_TOKEN: "abc",
      },
      undefined,
      /^CALQUE_LOCAL_SERVICE_ENDPOINT must be an exact numeric-loopback URL$/u,
    ],
    [
      "a malformed bearer",
      {
        CALQUE_LOCAL_SERVICE_ENDPOINT: "http://127.0.0.1:4100",
        CALQUE_LOCAL_BEARER_TOKEN: "grant secret",
      },
      undefined,
      /^CALQUE_LOCAL_BEARER_TOKEN is malformed$/u,
    ],
    [
      "a client.env with another line",
      {},
      "CALQUE_ENDPOINT=http://127.0.0.1:4100\nCALQUE_BEARER_TOKEN=abc\nOTHER=1\n",
      /client\.env is not a client\.env that calque dev wrote$/u,
    ],
    [
      "a client.env without a bearer",
      {},
      "CALQUE_ENDPOINT=http://127.0.0.1:4100\n",
      /client\.env must name both CALQUE_ENDPOINT and CALQUE_BEARER_TOKEN$/u,
    ],
    [
      "a client.env with a malformed bearer",
      {},
      "CALQUE_ENDPOINT=http://127.0.0.1:4100\nCALQUE_BEARER_TOKEN=grant secret\n",
      /^CALQUE_BEARER_TOKEN in .*client\.env is malformed$/u,
    ],
  ];

  it.each(refused)(
    "refuses %s, without echoing the value",
    async (_case, environment, clientEnv, message) => {
      withoutEnvironment();
      for (const [name, value] of Object.entries(environment))
        vi.stubEnv(name, value);
      const { root } = rootUnder(clientEnv);
      const error = await proxied(root).then(
        () => undefined,
        (reason: unknown) => reason,
      );
      expect(error).toBeInstanceOf(Error);
      const text = (error as Error).message;
      expect(text).toMatch(message);
      expect(text).not.toContain("grant secret");
      expect(text).not.toContain("localhost:4100");
    },
  );
});

/** A configuration and the `calque-bindings` render of it the repository pins. */
interface Render {
  readonly config: string;
  readonly bindings: string;
  readonly start: string;
}

const REPOSITORY_ROOT = fileURLToPath(new URL("../../../..", import.meta.url));
const pinned = (directory: string, name: string): Render => ({
  config: readFileSync(
    join(REPOSITORY_ROOT, directory, `${name}config.json`),
    "utf8",
  ),
  bindings: readFileSync(
    join(REPOSITORY_ROOT, directory, `${name}generated.ts`),
    "utf8",
  ),
  start: readFileSync(
    join(REPOSITORY_ROOT, directory, `${name}start.generated.ts`),
    "utf8",
  ),
});
const INCIDENT = pinned("tests/fixtures/config", "incident-ops.");
const QUICKSTART = pinned("client/examples/quickstart", "calque.");
const UNKNOWN_TYPE = INCIDENT.config.replace(
  '"postgresType": "uuid"',
  '"postgresType": "pg_catalog.uuid"',
);
const REFUSED =
  /calque\.config\.json: \$\.collections\[0\]\.columns\[0\]\.postgresType: pg_catalog\.uuid is not a type calque serve enrolls; it must be one of bool, int2, /u;

describe("the bindings calque() renders", () => {
  // Vite names a built page by its real path relative to the root, which it
  // does not resolve, and macOS puts the temporary directory under a link.
  const scratch = (): string => realpathSync(temporaryDirectory());

  /** A Vite root with a page and, given its text, `calque.config.json`. */
  function pageRoot(config?: string): string {
    const root = scratch();
    writeFileSync(
      join(root, "index.html"),
      '<!doctype html>\n<script type="module" src="/main.ts"></script>\n',
    );
    writeFileSync(join(root, "main.ts"), "export {};\n");
    if (config !== undefined)
      writeFileSync(join(root, "calque.config.json"), config);
    return root;
  }

  /** What the plugins said, one `level message` line each. */
  function recorder(): { readonly logger: Logger; readonly lines: string[] } {
    const lines: string[] = [];
    const record =
      (level: string) =>
      (message: string): void => {
        if (message.startsWith("calque: ") && !message.includes("calque dev"))
          lines.push(`${level} ${message}`);
      };
    return {
      lines,
      logger: {
        ...createLogger("silent"),
        info: record("info"),
        warn: record("warn"),
        warnOnce: record("warn"),
        error: record("error"),
      },
    };
  }

  async function served(
    root: string,
    logger: Logger,
    options: CalquePluginOptions = {},
  ): Promise<ViteDevServer> {
    vi.stubEnv("CALQUE_LOCAL_SERVICE_ENDPOINT", undefined);
    vi.stubEnv("CALQUE_LOCAL_BEARER_TOKEN", undefined);
    return createServer(
      pageConfig({
        root,
        customLogger: logger,
        plugins: [calque(options)],
        server: { host: "127.0.0.1", port: 0 },
      }),
    );
  }

  /** Resolves once the watcher has `path`, so a change to it is seen. */
  async function watching(server: ViteDevServer, path: string): Promise<void> {
    await expect
      .poll(
        () =>
          server.watcher.getWatched()[dirname(path)]?.includes(basename(path)),
        { timeout: 10_000 },
      )
      .toBe(true);
  }

  /** The bindings and start module at `bindings`, or undefined for either missing. */
  function rendered(bindings: string): Omit<Render, "config"> | undefined {
    const start = bindings.replace(
      /(?:\.generated)?\.ts$/u,
      ".start.generated.ts",
    );
    if (!existsSync(bindings) || !existsSync(start)) return undefined;
    return {
      bindings: readFileSync(bindings, "utf8"),
      start: readFileSync(start, "utf8"),
    };
  }

  const renderOf = ({ bindings, start }: Render) => ({ bindings, start });

  /**
   * A page's HMR connection to `server`, and the errors and updates it has
   * received. Vite also sends it reloads, for files it sees written.
   */
  async function hmrClient(server: ViteDevServer): Promise<{
    readonly received: HotPayload[];
    readonly socket: WebSocket;
  }> {
    const received: HotPayload[] = [];
    let connected = false;
    const socket = new WebSocket(
      origin(server.httpServer).replace("http:", "ws:"),
      "vite-hmr",
    );
    socket.addEventListener("message", (event) => {
      const payload = JSON.parse(String(event.data)) as HotPayload;
      if (payload.type === "connected") connected = true;
      if (payload.type === "error" || payload.type === "update")
        received.push(payload);
    });
    await expect.poll(() => connected).toBe(true);
    return { received, socket };
  }

  const CLEARED = { type: "update", updates: [] };

  it("renders calque.config.json beside it when the dev server starts, as calque-bindings does", async () => {
    const root = pageRoot(INCIDENT.config);
    const { logger, lines } = recorder();
    const server = await served(root, logger);
    try {
      expect(rendered(join(root, "calque.generated.ts"))).toEqual(
        renderOf(INCIDENT),
      );
      expect(lines).toEqual([
        "info calque: wrote calque.generated.ts and calque.start.generated.ts",
      ]);
    } finally {
      await server.close();
    }
  });

  it("renders it again when the configuration changes, and writes nothing when the render is unchanged", async () => {
    const root = pageRoot(INCIDENT.config);
    const out = join(root, "calque.generated.ts");
    writeFileSync(out, INCIDENT.bindings);
    writeFileSync(join(root, "calque.start.generated.ts"), INCIDENT.start);
    for (const name of ["calque.generated.ts", "calque.start.generated.ts"])
      utimesSync(join(root, name), 1_000, 1_000);
    const { logger, lines } = recorder();
    const server = await served(root, logger);
    try {
      expect(statSync(out).mtimeMs).toBe(1_000_000);
      expect(lines).toEqual([]);

      const config = join(root, "calque.config.json");
      await watching(server, config);
      // Another layout of the same configuration renders the same bytes.
      writeFileSync(config, JSON.stringify(JSON.parse(INCIDENT.config)));
      writeFileSync(config, QUICKSTART.config);
      await expect
        .poll(() => rendered(out), { timeout: 10_000 })
        .toEqual(renderOf(QUICKSTART));
      expect(lines.filter((line) => line.startsWith("info "))).toEqual([
        "info calque: wrote calque.generated.ts and calque.start.generated.ts",
      ]);
    } finally {
      await server.close();
    }
  });

  it("renders a configuration saved again within 50 ms of the last change, which the watcher does not report", async () => {
    const root = pageRoot(INCIDENT.config);
    const config = join(root, "calque.config.json");
    const out = join(root, "calque.generated.ts");
    const { logger } = recorder();
    const server = await served(root, logger);
    try {
      await watching(server, config);
      writeFileSync(config, QUICKSTART.config);
      await expect
        .poll(() => rendered(out), { interval: 1, timeout: 10_000 })
        .toEqual(renderOf(QUICKSTART));
      writeFileSync(config, INCIDENT.config);
      await expect
        .poll(() => rendered(out), { timeout: 2_000 })
        .toEqual(renderOf(INCIDENT));
    } finally {
      await server.close();
    }
  });

  it("keeps the last bindings and reports a configuration it cannot render on the terminal and in the error overlay, until it renders again", async () => {
    const root = pageRoot("{");
    const config = join(root, "calque.config.json");
    const out = join(root, "calque.generated.ts");
    const { logger, lines } = recorder();
    const server = await served(root, logger);
    try {
      expect(lines).toEqual([
        expect.stringMatching(
          /^error calque: .*calque\.config\.json is not valid JSON: /u,
        ),
      ]);
      expect(rendered(out)).toBeUndefined();
      await server.listen();
      // A page opened after the failure is shown it.
      const page = await hmrClient(server);
      expect(page.received).toEqual([
        {
          type: "error",
          err: {
            message: lines[0]?.slice("error calque: ".length),
            stack: "",
            plugin: "calque:bindings",
            id: config,
          },
        },
      ]);

      page.received.length = 0;
      await watching(server, config);
      writeFileSync(config, INCIDENT.config);
      await expect
        .poll(() => rendered(out), { timeout: 10_000 })
        .toEqual(renderOf(INCIDENT));
      await expect.poll(() => page.received.at(-1)).toEqual(CLEARED);

      page.received.length = 0;
      lines.length = 0;
      writeFileSync(config, UNKNOWN_TYPE);
      await expect
        .poll(() => page.received.at(-1), { timeout: 10_000 })
        .toEqual({
          type: "error",
          err: expect.objectContaining({
            message: expect.stringMatching(REFUSED),
            plugin: "calque:bindings",
          }),
        });
      expect(lines.at(-1)).toMatch(REFUSED);
      expect(rendered(out)).toEqual(renderOf(INCIDENT));

      page.received.length = 0;
      writeFileSync(config, QUICKSTART.config);
      await expect
        .poll(() => rendered(out), { timeout: 10_000 })
        .toEqual(renderOf(QUICKSTART));
      await expect.poll(() => page.received.at(-1)).toEqual(CLEARED);
      page.socket.close();
    } finally {
      await server.close();
    }
  });

  it("is rendered before vite build, which fails on a configuration it cannot render", async () => {
    const root = pageRoot(UNKNOWN_TYPE);
    const out = join(root, "calque.generated.ts");
    const { logger } = recorder();
    const config = pageConfig({
      root,
      customLogger: logger,
      plugins: [calque()],
      build: { write: false },
    });
    await expect(build(config)).rejects.toThrow(REFUSED);
    expect(rendered(out)).toBeUndefined();

    writeFileSync(join(root, "calque.config.json"), QUICKSTART.config);
    await build(config);
    expect(rendered(out)).toEqual(renderOf(QUICKSTART));
  }, 120_000);

  it("is not rendered with bindings: false, or without calque.config.json", async () => {
    for (const [root, options] of [
      [pageRoot(INCIDENT.config), { bindings: false }],
      [pageRoot(), {}],
    ] as const) {
      const { logger, lines } = recorder();
      await (await served(root, logger, options)).close();
      await build(
        pageConfig({
          root,
          customLogger: logger,
          plugins: [calque(options)],
          build: { write: false },
        }),
      );
      expect(rendered(join(root, "calque.generated.ts"))).toBeUndefined();
      expect(lines).toEqual([]);
    }
  }, 120_000);

  it("reads the configuration and writes the bindings that bindings names, and requires the configuration it names", async () => {
    const config = join(scratch(), "shared", "app.json");
    mkdirSync(dirname(config));
    writeFileSync(config, INCIDENT.config);
    const root = pageRoot(QUICKSTART.config);
    mkdirSync(join(root, "src"));
    const out = join(root, "src", "bindings.ts");
    const { logger, lines } = recorder();
    const server = await served(root, logger, {
      bindings: { config: relative(root, config), out: "src/bindings.ts" },
    });
    try {
      expect(rendered(out)).toEqual(renderOf(INCIDENT));
      expect(rendered(join(root, "calque.generated.ts"))).toBeUndefined();
      expect(lines).toEqual([
        "info calque: wrote src/bindings.ts and src/bindings.start.generated.ts",
      ]);
      // Outside Vite's root, which Vite does not watch by itself.
      await watching(server, config);
      writeFileSync(config, QUICKSTART.config);
      await expect
        .poll(() => rendered(out), { timeout: 10_000 })
        .toEqual(renderOf(QUICKSTART));
    } finally {
      await server.close();
    }

    rmSync(config);
    await expect(
      build(
        pageConfig({
          root,
          customLogger: logger,
          plugins: [calque({ bindings: { config: config } })],
          build: { write: false },
        }),
      ),
    ).rejects.toThrow(`${config} does not exist; run calque init to write it`);
  }, 120_000);

  it("reports bindings that are not a .ts file, as it reports a configuration it cannot render", async () => {
    const root = pageRoot(QUICKSTART.config);
    const options = { bindings: { out: "calque.generated.js" } };
    const refused = `the bindings must be a .ts file, not ${join(root, "calque.generated.js")}`;
    const { logger, lines } = recorder();
    await (await served(root, logger, options)).close();
    expect(lines).toEqual([`error calque: ${refused}`]);
    await expect(
      build(
        pageConfig({
          root,
          customLogger: logger,
          plugins: [calque(options)],
          build: { write: false },
        }),
      ),
    ).rejects.toThrow(refused);
  }, 120_000);

  it("says once that @calque/config is not installed, and renders nothing", () => {
    // The built plugin, alone in a directory no node_modules is above.
    const plugin = scratch();
    for (const name of [
      "vite.js",
      "bootstrap-start.js",
      "bootstrap-start-script.js",
    ])
      copyFileSync(join(PACKAGE_ROOT, "dist", name), join(plugin, name));
    const root = pageRoot(INCIDENT.config);
    // In Node itself, which is what fails to find the package; a module
    // vitest loads would fail in vitest's words. Two builds with one plugin
    // are what `vite build --watch` does.
    const script = `
      const { build, createLogger } = await import(${JSON.stringify(pathToFileURL(join(PACKAGE_ROOT, "node_modules/vite/dist/node/index.js")).href)});
      const { calque } = await import(${JSON.stringify(pathToFileURL(join(plugin, "vite.js")).href)});
      const lines = [];
      const record = (level) => (message) => {
        if (message.startsWith("calque: ")) lines.push(level + " " + message);
      };
      const customLogger = {
        ...createLogger("silent"),
        info: record("info"),
        warn: record("warn"),
        warnOnce: record("warn"),
        error: record("error"),
      };
      const plugins = calque();
      for (let run = 0; run < 2; run += 1)
        await build({
          root: ${JSON.stringify(root)},
          configFile: false,
          logLevel: "silent",
          customLogger,
          plugins,
          build: { write: false },
        });
      process.stdout.write(JSON.stringify(lines));
    `;
    const result = spawnSync(
      process.execPath,
      ["--input-type=module", "--eval", script],
      { cwd: root, encoding: "utf8" },
    );
    expect(result.stderr).toBe("");
    expect(JSON.parse(result.stdout)).toEqual([
      "warn calque: calque.config.json is here, but @calque/config is not installed, so calque.generated.ts is not rendered from it. Install it with npm install -D @calque/config.",
    ]);
    expect(rendered(join(root, "calque.generated.ts"))).toBeUndefined();
  }, 120_000);
});
