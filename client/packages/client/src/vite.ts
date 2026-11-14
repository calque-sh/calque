/**
 * The Vite plugin for a page that uses the client: `@calque/client/vite`.
 * It builds the engine worker as an ES module that Chrome compiles while the
 * working set downloads, starts the page's bootstrap request from its head
 * and from a module that evaluates before the client library, renders the
 * typed bindings from `calque.config.json`, and, while Vite serves the page,
 * proxies `/_calque/` to `calque dev`.
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import { dirname, join, relative, resolve, sep } from "node:path";
import process from "node:process";

import type {
  Connect,
  ErrorPayload,
  Logger,
  Plugin,
  ProxyOptions,
  UserConfig,
  ViteDevServer,
} from "vite";

import {
  type BootstrapRequest,
  DEFAULT_MEMORY_BUDGET_BYTES,
} from "./bootstrap-start.js";
import { BOOTSTRAP_START_SCRIPT } from "./bootstrap-start-script.js";

export interface CalquePluginOptions {
  /**
   * The values the page's bootstrap request sends. The page's head carries
   * them as its `calque-bootstrap` element, beside the client's inline
   * starter, which sends the request as soon as the document is parsed. A
   * client created without `memoryBudgetBytes` takes the element's budget
   * and heap reserve. Ahead of its own entry, the page also loads a module
   * that calls `startBootstrap` with them, from a chunk of its own, so the
   * request still leaves before the client library evaluates if the inline
   * starter did not run.
   */
  readonly bootstrap?: BootstrapRequest;
  /**
   * Where the typed bindings come from and go, relative to Vite's root:
   * `config` is the configuration, by default `calque.config.json`, and
   * `out` the `.ts` file, by default `calque.generated.ts`, with the start
   * module beside it. `vite` renders them when it starts and whenever the
   * configuration changes, and `vite build` before it builds, writing a file
   * only when its render differs from it. A configuration that cannot be
   * rendered fails the build; under `vite` it is reported and the last
   * bindings stay. The default configuration is rendered only once it
   * exists. Rendering takes `@calque/config`, a dev dependency of the
   * application. `false` leaves the bindings to `calque-bindings`.
   */
  readonly bindings?:
    false | { readonly config?: string; readonly out?: string };
}

/** The `calque dev` the proxy reaches, and where its settings came from. */
interface DevService {
  readonly endpoint: string;
  readonly bearerToken: string;
  readonly source: string;
}

const PROXY_PATH = "/_calque/";
/** The bearer a page served by Vite sends; the proxy swaps it for calque dev's. */
const PROXY_BEARER = "calque-dev-proxy";
const AUTHORIZED_PATHS = new Set([
  "/v1/bootstrap",
  "/v1/changes",
  "/v1/mutations",
]);
const MODULE_SCRIPT = /<script\b[^>]*\btype=["']?module["']?[\s>]/iu;
const START_URL = "/@calque/start";
const START_ID = "\0calque:start";
const DEFAULT_CONFIG = "calque.config.json";
const DEFAULT_BINDINGS = "calque.generated.ts";

type BindingsRenderer = typeof import("@calque/config");

/**
 * The plugins for a page that uses the client. With no options they build
 * the engine worker, render the bindings from `calque.config.json`, and
 * proxy `/_calque/` while Vite serves the page; `bootstrap` starts the page's
 * bootstrap request early.
 */
export function calque(options: CalquePluginOptions = {}): Plugin[] {
  let service: DevService | undefined;
  const connection = jsonElement("calque-connection", {
    endpoint: PROXY_PATH,
    bearerToken: PROXY_BEARER,
  });
  const request = options.bootstrap && {
    requiredTables: options.bootstrap.requiredTables,
    memoryBudgetBytes:
      options.bootstrap.memoryBudgetBytes ?? DEFAULT_MEMORY_BUDGET_BYTES,
    clientHeapReserveBytes: options.bootstrap.clientHeapReserveBytes,
  };
  return [
    {
      name: "calque",
      config(config, { command }): UserConfig {
        service =
          command === "serve"
            ? devService(resolve(config.root ?? process.cwd()))
            : undefined;
        const proxy = service === undefined ? undefined : proxyTo(service);
        return {
          worker: {
            format: "es",
            rolldownOptions: {
              output: {
                // After minification, which would drop the comment, and
                // before hashing, so the file name still follows the content.
                postBanner: (chunk) =>
                  chunk.name === "columnar-browser-worker-entry"
                    ? "//# allFunctionsCalledOnLoad"
                    : "",
              },
            },
          },
          ...(request === undefined
            ? {}
            : {
                build: {
                  rolldownOptions: {
                    output: {
                      codeSplitting: {
                        groups: [
                          {
                            name: "start",
                            test: (id: string) => id === START_ID,
                          },
                        ],
                      },
                    },
                  },
                },
              }),
          ...(proxy === undefined
            ? {}
            : { server: { proxy }, preview: { proxy } }),
        };
      },
      configureServer(server) {
        announce(server.httpServer, server.config.logger, service);
      },
      configurePreviewServer(server) {
        announce(server.httpServer, server.config.logger, service);
        if (service === undefined) return;
        const outDir = resolve(server.config.root, server.config.build.outDir);
        // Ahead of Vite's static files, which serve a page asked for by name,
        // and again once its fallback has named the page a route serves.
        server.middlewares.use(
          servePage(outDir, server.config.base, connection),
        );
        return () => {
          server.middlewares.use(servePage(outDir, "/", connection));
        };
      },
      resolveId: (id) =>
        request !== undefined && id === START_URL ? START_ID : undefined,
      load: (id) =>
        request !== undefined && id === START_ID
          ? `import { startBootstrap } from "@calque/client/bootstrap-start";\nstartBootstrap(${JSON.stringify(request)});\n`
          : undefined,
      transformIndexHtml: {
        order: "pre",
        handler: (html) =>
          request === undefined
            ? html
            : beforeFirstModuleScript(html, [
                `<script type="module" src="${START_URL}"></script>`,
              ]),
      },
    },
    {
      name: "calque:page",
      // After Vite's own processing of the page, which would bundle an inline
      // module script and change the text its CSP hash covers.
      transformIndexHtml: {
        order: "post",
        handler(html, context) {
          const elements: string[] = [];
          if (context.server && service) elements.push(connection);
          if (request)
            elements.push(
              jsonElement("calque-bootstrap", request),
              `<script type="module">${BOOTSTRAP_START_SCRIPT}</script>`,
            );
          return elements.length === 0
            ? html
            : beforeFirstModuleScript(html, elements);
        },
      },
    },
    ...(options.bindings === false
      ? []
      : [bindingsPlugin(options.bindings ?? {})]),
  ];
}

/**
 * Rewrites the bindings at `out`, and the start module beside them, wherever
 * they differ from the render of the configuration at `config`.
 */
function bindingsPlugin(
  options: Exclude<CalquePluginOptions["bindings"], false | undefined>,
): Plugin {
  let command: "build" | "serve";
  let root: string;
  let config: string;
  let out: string;
  let logger: Logger;
  let renderer: Promise<BindingsRenderer | undefined> | undefined;
  /** The configuration the last render read, undefined when it could not. */
  let rendered: string | undefined;
  let recheck: ReturnType<typeof setTimeout> | undefined;

  function configText(): string | undefined {
    try {
      return readFileSync(config, "utf8");
    } catch {
      return undefined;
    }
  }

  /** `@calque/config`, or undefined when it is not installed, said once. */
  function loadRenderer(): Promise<BindingsRenderer | undefined> {
    renderer ??= import("@calque/config").catch((error: unknown) => {
      if (!notInstalled(error)) throw error;
      logger.warn(
        `calque: ${relative(root, config)} is here, but @calque/config is not installed, so ${relative(root, out)} is not rendered from it. Install it with npm install -D @calque/config.`,
      );
      return undefined;
    });
    return renderer;
  }

  /**
   * Writes each file whose render differs from the file on disk. Returns
   * what is wrong with a configuration that cannot be rendered.
   */
  async function render(): Promise<string | undefined> {
    rendered = configText();
    if (options.config === undefined && !existsSync(config)) return undefined;
    const bindings = await loadRenderer();
    if (bindings === undefined) return undefined;
    let files;
    try {
      files = bindings.renderBindingsFiles(config, out);
    } catch (error) {
      if (error instanceof bindings.BindingsError) return error.message;
      throw error;
    }
    const stale = bindings.staleBindingsFiles(files);
    for (const { path, bytes } of files)
      if (stale.includes(path)) writeFileSync(path, bytes);
    if (stale.length > 0)
      logger.info(
        `calque: wrote ${stale.map((path) => relative(root, path)).join(" and ")}`,
        { timestamp: true },
      );
    return undefined;
  }

  return {
    name: "calque:bindings",
    configResolved(resolved) {
      command = resolved.command;
      root = resolved.root;
      config = resolve(root, options.config ?? DEFAULT_CONFIG);
      out = resolve(root, options.out ?? DEFAULT_BINDINGS);
      logger = resolved.logger;
    },
    async buildStart() {
      if (command !== "build") return;
      const message = await render();
      if (message !== undefined) this.error(message);
    },
    async configureServer(server) {
      let failure: ErrorPayload | undefined;
      const report = (message: string | undefined): void => {
        if (message !== undefined) {
          logger.error(`calque: ${message}`, { timestamp: true });
          failure = {
            type: "error",
            err: { message, stack: "", plugin: "calque:bindings", id: config },
          };
          for (const client of server.ws.clients) client.send(failure);
        } else if (failure !== undefined) {
          failure = undefined;
          // An update is what closes Vite's error overlay. A page that has
          // had none yet reloads instead.
          for (const client of server.ws.clients)
            client.send({ type: "update", updates: [] });
        }
      };
      server.ws.on("vite:client:connect", (_data, client) => {
        if (failure !== undefined) client.send(failure);
      });
      const refresh = (): void => {
        render().then(report, (error: unknown) => {
          report(error instanceof Error ? error.message : String(error));
        });
      };
      const changed = (path: string): void => {
        if (resolve(path) !== config) return;
        refresh();
        // The watcher drops a change that follows another within 50 ms and
        // never sends it later, so the configuration is read again after that.
        clearTimeout(recheck);
        recheck = setTimeout(() => {
          if (configText() !== rendered) refresh();
        }, 100);
      };
      server.watcher.add(config);
      server.watcher.on("add", changed).on("change", changed);
      report(await render());
    },
    closeBundle() {
      clearTimeout(recheck);
    },
  };
}

/** Whether `error` is Node's for an import of `@calque/config` it cannot find. */
function notInstalled(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    error.code === "ERR_MODULE_NOT_FOUND" &&
    error.message.includes("'@calque/config'")
  );
}

/**
 * The `calque dev` to proxy to: the one `CALQUE_LOCAL_SERVICE_ENDPOINT` and
 * `CALQUE_LOCAL_BEARER_TOKEN` name, or else the one whose `client.env` is in
 * the nearest `.calque-dev` at or above `root`.
 */
function devService(root: string): DevService | undefined {
  const endpoint = process.env.CALQUE_LOCAL_SERVICE_ENDPOINT;
  const bearerToken = process.env.CALQUE_LOCAL_BEARER_TOKEN;
  if ((endpoint === undefined) !== (bearerToken === undefined))
    throw new Error(
      "CALQUE_LOCAL_SERVICE_ENDPOINT and CALQUE_LOCAL_BEARER_TOKEN must be provided together",
    );
  if (endpoint !== undefined && bearerToken !== undefined)
    return checked(
      { endpoint, bearerToken, source: "the environment" },
      "CALQUE_LOCAL_SERVICE_ENDPOINT",
      "CALQUE_LOCAL_BEARER_TOKEN",
    );
  for (let directory = root; ; directory = dirname(directory)) {
    const file = join(directory, ".calque-dev", "client.env");
    if (existsSync(file)) return clientEnvironment(file);
    if (dirname(directory) === directory) return undefined;
  }
}

/** The `CALQUE_ENDPOINT` and `CALQUE_BEARER_TOKEN` lines `calque dev` writes. */
function clientEnvironment(file: string): DevService {
  const values = new Map<string, string>();
  for (const line of readFileSync(file, "utf8").split(/\r?\n/u)) {
    if (line.length === 0) continue;
    const separator = line.indexOf("=");
    const key = line.slice(0, separator);
    if (
      separator <= 0 ||
      (key !== "CALQUE_ENDPOINT" && key !== "CALQUE_BEARER_TOKEN") ||
      values.has(key)
    )
      throw new Error(`${file} is not a client.env that calque dev wrote`);
    values.set(key, line.slice(separator + 1));
  }
  const endpoint = values.get("CALQUE_ENDPOINT");
  const bearerToken = values.get("CALQUE_BEARER_TOKEN");
  if (endpoint === undefined || bearerToken === undefined)
    throw new Error(
      `${file} must name both CALQUE_ENDPOINT and CALQUE_BEARER_TOKEN`,
    );
  return checked(
    { endpoint, bearerToken, source: file },
    `CALQUE_ENDPOINT in ${file}`,
    `CALQUE_BEARER_TOKEN in ${file}`,
  );
}

function checked(
  service: DevService,
  endpointName: string,
  bearerName: string,
): DevService {
  if (!/^http:\/\/127\.0\.0\.1:\d+$/u.test(service.endpoint))
    throw new Error(`${endpointName} must be an exact numeric-loopback URL`);
  if (!/^[A-Za-z0-9_-]+$/u.test(service.bearerToken))
    throw new Error(`${bearerName} is malformed`);
  return service;
}

/**
 * `/_calque/` to calque dev. On `/v1/bootstrap`, `/v1/changes` and
 * `/v1/mutations`, the page's placeholder bearer becomes calque dev's and any
 * other is dropped, so the page never holds the service's bearer.
 */
function proxyTo(service: DevService): Record<string, ProxyOptions> {
  return {
    [PROXY_PATH]: {
      target: service.endpoint,
      changeOrigin: true,
      rewrite: (path) => path.slice(PROXY_PATH.length - 1),
      configure(proxy) {
        proxy.on("proxyReq", (proxyRequest, request) => {
          if (!AUTHORIZED_PATHS.has(request.url?.split("?")[0] ?? "")) return;
          if (request.headers.authorization === `Bearer ${PROXY_BEARER}`)
            proxyRequest.setHeader(
              "authorization",
              `Bearer ${service.bearerToken}`,
            );
          else proxyRequest.removeHeader("authorization");
        });
      },
    },
  };
}

function announce(
  httpServer: ViteDevServer["httpServer"],
  logger: Logger,
  service: DevService | undefined,
): void {
  httpServer?.once("listening", () => {
    if (service)
      logger.info(
        `calque: ${PROXY_PATH} proxies to calque dev at ${service.endpoint}, from ${service.source}`,
      );
    else
      logger.warn(
        `calque: no calque dev to proxy ${PROXY_PATH} to. Start calque dev, which writes .calque-dev/client.env, or set CALQUE_LOCAL_SERVICE_ENDPOINT and CALQUE_LOCAL_BEARER_TOKEN, then restart Vite.`,
      );
  });
}

/**
 * Serves the built pages under `outDir` with `connection` in their heads. A
 * page with no module script runs no Calque code, and Vite serves it as built.
 */
function servePage(
  outDir: string,
  base: string,
  connection: string,
): Connect.NextHandleFunction {
  return (request, response, next) => {
    const file = pageFile(request, outDir, base);
    const html = file === undefined ? undefined : readFileSync(file, "utf8");
    if (html === undefined || !MODULE_SCRIPT.test(html)) {
      next();
      return;
    }
    response.setHeader("content-type", "text/html; charset=utf-8");
    response.setHeader("cache-control", "no-store");
    response.end(
      request.method === "HEAD"
        ? undefined
        : beforeFirstModuleScript(html, [connection]),
    );
  };
}

/** The built page a preview request names, if it names one. */
function pageFile(
  request: IncomingMessage,
  outDir: string,
  base: string,
): string | undefined {
  if (request.method !== "GET" && request.method !== "HEAD") return undefined;
  let path: string;
  try {
    path = decodeURIComponent(
      new URL(request.url ?? "/", "http://localhost").pathname,
    );
  } catch {
    return undefined;
  }
  if (!path.startsWith(base)) return undefined;
  path = path.slice(base.length);
  if (path === "" || path.endsWith("/")) path += "index.html";
  if (!path.endsWith(".html")) return undefined;
  const file = resolve(outDir, path);
  return file.startsWith(`${outDir}${sep}`) && existsSync(file)
    ? file
    : undefined;
}

/** `html` with `elements` ahead of its first module script. */
function beforeFirstModuleScript(
  html: string,
  elements: readonly string[],
): string {
  const at = html.search(MODULE_SCRIPT);
  if (at === -1)
    throw new Error("calque: the page has no module script to start before");
  const line = html.slice(html.lastIndexOf("\n", at - 1) + 1, at);
  const indent = /^[ \t]*$/u.test(line) ? line : "";
  return (
    html.slice(0, at) +
    elements.map((element) => `${element}\n${indent}`).join("") +
    html.slice(at)
  );
}

/** JSON that a script element holds as it is: no `<` can end it early. */
function jsonElement(id: string, value: unknown): string {
  return `<script type="application/json" id="${id}">${JSON.stringify(value).replaceAll("<", "\\u003c")}</script>`;
}
