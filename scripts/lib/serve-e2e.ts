/**
 * What every `calque serve` browser end-to-end run needs and none of them
 * owns: a disposable PostgreSQL 18 bootstrapped into the live suite's
 * least-privilege role, the two binaries built into this checkout's own target
 * directory, a static origin for the page, and `calque serve` started and
 * stopped the way an operator does it. `scripts/serve-mutation-e2e.ts` and
 * `scripts/serve-revocation-e2e.ts` both drive their pages through this.
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { createServer, type ServerResponse } from "node:http";
import { createServer as createSocketServer } from "node:net";
import { dirname, extname, join, normalize, resolve, sep } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import {
  brotliCompressSync,
  constants as zlibConstants,
  gzipSync,
} from "node:zlib";
import Negotiator from "negotiator";
import {
  CARGO_TARGET_ARGUMENTS,
  CARGO_TARGET_DIRECTORY,
} from "./cargo-target.ts";

export const repositoryRoot = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);

export function run(
  command: string,
  args: readonly string[],
  options: {
    readonly env?: NodeJS.ProcessEnv;
    readonly input?: string;
    readonly cwd?: string;
  } = {},
): string {
  const result = spawnSync(command, [...args], {
    cwd: options.cwd ?? repositoryRoot,
    encoding: "utf8",
    env: { ...process.env, ...options.env },
    ...(options.input === undefined ? {} : { input: options.input }),
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} exited ${String(result.status)}\n${result.stdout ?? ""}\n${result.stderr ?? ""}`,
    );
  }
  return result.stdout;
}

/**
 * psql inside `container`, as the least-privilege role the live suite
 * bootstraps. Using the container's own client keeps a run from needing libpq
 * on the host, as `scripts/live-postgres-suite.sh` does.
 */
export function psqlIn(
  container: string,
  database: string,
  sql: string,
  stopOnError = true,
): string {
  const result = spawnSync(
    "docker",
    [
      "exec",
      "-i",
      container,
      "psql",
      "-U",
      "calque_test",
      "-d",
      database,
      ...(stopOnError ? ["-v", "ON_ERROR_STOP=1"] : []),
      "-X",
      "-q",
      "-A",
      "-t",
      "-f",
      "-",
    ],
    { encoding: "utf8", input: sql, maxBuffer: 32 * 1024 * 1024 },
  );
  if (stopOnError && result.status !== 0)
    throw new Error(
      `psql exited ${String(result.status)}\n${result.stdout ?? ""}\n${result.stderr ?? ""}`,
    );
  return result.stdout ?? "";
}

/**
 * A PostgreSQL 18 container of the run's own, created if it is not already
 * running and bootstrapped by `scripts/live-postgres-suite.sh` into the
 * least-privilege role the ignored Rust suite uses: enrollment refuses a
 * superuser, so `serve` cannot connect as the image's bootstrap role.
 *
 * Only the named container is ever created or removed, and it is removed with
 * `-v`: `postgres:18` declares a volume, so a removal without it leaves the
 * cluster behind as an anonymous volume.
 */
export function ensurePostgresContainer(
  container: string,
  hostPort: string,
): void {
  const existing = spawnSync(
    "docker",
    ["inspect", "--format", "{{.State.Running}}", container],
    { encoding: "utf8" },
  );
  if (existing.status !== 0 || existing.stdout.trim() !== "true") {
    spawnSync("docker", ["rm", "-f", "-v", container], { encoding: "utf8" });
    run("docker", [
      "run",
      "-d",
      "--name",
      container,
      "-e",
      "POSTGRES_PASSWORD=postgres",
      "-e",
      "POSTGRES_HOST_AUTH_METHOD=trust",
      "-p",
      `127.0.0.1:${hostPort}:5432`,
      "postgres:18",
      "-c",
      "wal_level=logical",
      "-c",
      "max_replication_slots=20",
      "-c",
      "max_wal_senders=20",
      "-c",
      "max_connections=400",
    ]);
  }
  process.stdout.write(
    run("sh", [
      join(repositoryRoot, "scripts/live-postgres-suite.sh"),
      "bootstrap",
      container,
    ]),
  );
}

export function removePostgresContainer(container: string): void {
  spawnSync("docker", ["rm", "-f", "-v", container], { encoding: "utf8" });
}

/**
 * `calque` and `calque-dev-grant`, built into this checkout's own target
 * directory.
 */
export function buildServeBinaries(): string {
  run("cargo", [
    "build",
    "--quiet",
    ...CARGO_TARGET_ARGUMENTS,
    "-p",
    "calque",
    "-p",
    "dataset-grant-issuer",
  ]);
  return join(CARGO_TARGET_DIRECTORY, "debug");
}

/** A free loopback TCP port, released immediately. */
export async function reservedPort(): Promise<number> {
  return await new Promise((resolvePort, rejectPort) => {
    const server = createSocketServer();
    server.on("error", rejectPort);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (typeof address === "string" || address === null) {
        server.close();
        rejectPort(new Error("the reservation socket has no port"));
        return;
      }
      const { port } = address;
      server.close(() => resolvePort(port));
    });
  });
}

/** The content type a static page server sends for each extension it knows. */
export const MIME: Readonly<Record<string, string>> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
};

/** A file's bytes as stored and as a static host sends them precompressed. */
interface StaticFile {
  readonly type: string;
  readonly identity: Buffer;
  readonly br: Buffer | null;
  readonly gzip: Buffer | null;
}

/**
 * A file's bytes, compressed once if its type is in `MIME`, as a build's
 * precompressed `.br` and `.gz` files are: brotli at quality 11 and gzip at
 * level 9.
 */
function staticFile(identity: Buffer, type: string | undefined): StaticFile {
  return {
    type: type ?? "application/octet-stream",
    identity,
    br:
      type === undefined
        ? null
        : brotliCompressSync(identity, {
            params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 11 },
          }),
    gzip: type === undefined ? null : gzipSync(identity, { level: 9 }),
  };
}

/** Every file under `pageRoot`, as `staticFile` holds it. */
function staticFiles(pageRoot: string): Map<string, StaticFile> {
  const files = new Map<string, StaticFile>();
  for (const entry of readdirSync(pageRoot, { recursive: true })) {
    const relative = String(entry);
    const path = join(pageRoot, relative);
    if (!statSync(path).isFile()) continue;
    files.set(
      `/${relative.split(sep).join("/")}`,
      staticFile(readFileSync(path), MIME[extname(path)]),
    );
  }
  return files;
}

/** The endpoint and the bearer a page's server renders into its HTML. */
interface RenderedConnection {
  readonly endpoint: string;
  readonly bearerToken: string;
}

/**
 * `html` with `connection` rendered before its `</head>` as the
 * `calque-connection` element `@calque/client`'s README tells a backend to
 * render, with `<` written as `\u003c` so that no value can close it.
 */
export function renderConnection(
  html: string,
  connection: RenderedConnection,
): string {
  const head = html.indexOf("</head>");
  if (
    head === -1 ||
    html.indexOf("</head>", head + 1) !== -1 ||
    html.includes('id="calque-connection"')
  )
    throw new Error(
      "the page needs exactly one </head> and no calque-connection element",
    );
  const json = JSON.stringify({
    endpoint: connection.endpoint,
    bearerToken: connection.bearerToken,
  }).replaceAll("<", "\\u003c");
  return `${html.slice(0, head)}<script type="application/json" id="calque-connection">${json}</script>${html.slice(head)}`;
}

/**
 * `index` with `connection` rendered into it, compressed as `staticFile`
 * does.
 */
function renderPage(
  index: StaticFile,
  connection: RenderedConnection,
): StaticFile {
  return staticFile(
    Buffer.from(renderConnection(index.identity.toString("utf8"), connection)),
    MIME[".html"],
  );
}

/**
 * `html` with `fragment` inserted before its `</head>`, which
 * `renderConnection` has already checked is the only one.
 */
function beforeHeadEnd(html: string, fragment: string): string {
  const head = html.indexOf("</head>");
  return `${html.slice(0, head)}${fragment}${html.slice(head)}`;
}

/**
 * A page rendered for one request, compressed in the one coding that request
 * takes, at the settings a server uses for a response it cannot compress
 * ahead of time: brotli at quality 5 and gzip at level 6. Quality 11 would
 * spend milliseconds on the document's critical path for a few hundred bytes.
 */
function dynamicPage(html: string, coding: "br" | "gzip" | null): StaticFile {
  const identity = Buffer.from(html);
  return {
    type: MIME[".html"] ?? "text/html",
    identity,
    br:
      coding === "br"
        ? brotliCompressSync(identity, {
            params: { [zlibConstants.BROTLI_PARAM_QUALITY]: 5 },
          })
        : null,
    gzip: coding === "gzip" ? gzipSync(identity, { level: 6 }) : null,
  };
}

/** What the page server renders into the build's `index.html`. */
export interface ServedPage {
  /** The connection to render, or null to send the page as stored. */
  readonly connection: RenderedConnection | null;
  /**
   * The HTML the application's backend renders into the page's head for
   * each request, after the connection, given the request's
   * `Accept-Encoding`, or null for none. It runs on the document's critical
   * path: no byte of the page is sent until it settles.
   */
  readonly head:
    ((acceptEncoding: string | undefined) => Promise<string>) | null;
}

/** A build, and what to render into its page, read as each request arrives. */
interface PageChoice {
  readonly root: string;
  readonly current: () => ServedPage;
}

/** The codings `servePage` holds each compressed file in, brotli first. */
const STATIC_CODINGS: readonly ("br" | "gzip")[] = ["br", "gzip"];

/**
 * The coding to send for an `Accept-Encoding`, picked with negotiator by the
 * request's weights: brotli before gzip at equal weight, never a coding at
 * `q=0`, and the stored bytes for an empty or missing field. `*` matches
 * brotli.
 */
export function staticCoding(
  acceptEncoding: string | undefined,
): "br" | "gzip" | null {
  const negotiator = new Negotiator({
    headers: { "accept-encoding": acceptEncoding },
  });
  // negotiator 1.1.0's `encodings` takes the `preferred` option that
  // @types/negotiator 0.6.5 leaves out of its signature. A function of fewer
  // parameters is assignable to this type of more, so no cast is needed.
  const encodings: (
    available: readonly string[],
    options: { readonly preferred: readonly string[] },
  ) => string[] = negotiator.encodings.bind(negotiator);
  const [chosen] = encodings(STATIC_CODINGS, { preferred: STATIC_CODINGS });
  return STATIC_CODINGS.find((coding) => coding === chosen) ?? null;
}

/**
 * The page's own origin, a plain static server rather than a proxy, so every
 * Calque request the page makes is cross-origin to `calque serve`, as in a
 * deployment.
 *
 * Each file whose type is in `MIME` goes in the coding `staticCoding` picks,
 * or as stored, with `Vary: Accept-Encoding`, compressed once before the
 * server listens; any other file goes as stored. Given a `PageChoice`, it
 * reads what to render into the page as each request arrives. An `index.html`
 * with a connection to render is sent with `Cache-Control: no-store`, as a
 * page that carries a bearer must be. Without a head to render, it is rendered
 * and compressed once per connection; with one, for each request, once the
 * head has settled.
 */
export function servePage(
  port: number,
  pages: string | PageChoice,
): { close: () => Promise<void> } {
  const { root, current }: PageChoice =
    typeof pages === "string"
      ? { root: pages, current: () => ({ connection: null, head: null }) }
      : pages;
  const files = staticFiles(root);
  const rendered = new Map<string, StaticFile>();
  const server = createServer((request, response) => {
    const requested = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
    const relative = normalize(requested === "/" ? "/index.html" : requested);
    let served: ServedPage;
    try {
      served = current();
    } catch (error) {
      process.stderr.write(
        `the page server could not read what to render: ${String(error)}\n`,
      );
      response.writeHead(500).end();
      return;
    }
    const stored = files.get(relative);
    if (stored === undefined) {
      response.writeHead(404).end();
      return;
    }
    const coding = staticCoding(request.headers["accept-encoding"]);
    const { connection, head } = served;
    if (relative !== "/index.html" || connection === null) {
      send(response, stored, coding, false);
      return;
    }
    if (head === null) {
      const key = JSON.stringify(connection);
      let page = rendered.get(key);
      if (page === undefined) {
        page = renderPage(stored, connection);
        rendered.set(key, page);
      }
      send(response, page, coding, true);
      return;
    }
    const html = renderConnection(stored.identity.toString("utf8"), connection);
    head(request.headers["accept-encoding"]).then(
      (fragment) =>
        send(
          response,
          dynamicPage(beforeHeadEnd(html, fragment), coding),
          coding,
          true,
        ),
      (error: unknown) => {
        process.stderr.write(
          `the page server could not render the page's head: ${String(error)}\n`,
        );
        response.writeHead(500).end();
      },
    );
  });
  server.listen(port, "127.0.0.1");
  return {
    close: () =>
      new Promise<void>((done) => {
        server.close(() => done());
      }),
  };
}

/**
 * `file` in `coding` when it holds that coding and as stored otherwise. A
 * page sent `noStore` was rendered for its request, and any other file of a
 * type `staticFile` compresses was negotiated, so either varies on
 * `Accept-Encoding`.
 */
function send(
  response: ServerResponse,
  file: StaticFile,
  coding: "br" | "gzip" | null,
  noStore: boolean,
): void {
  const compressed = coding === null ? null : file[coding];
  const body = compressed ?? file.identity;
  response.writeHead(200, {
    "content-type": file.type,
    "content-length": String(body.byteLength),
    ...(noStore || file.br !== null ? { vary: "Accept-Encoding" } : {}),
    ...(compressed === null || coding === null
      ? {}
      : { "content-encoding": coding }),
    ...(noStore ? { "cache-control": "no-store" } : {}),
  });
  response.end(body);
}

export async function waitFor(
  what: string,
  timeoutMs: number,
  condition: () => Promise<boolean>,
  intervalMs = 250,
): Promise<number> {
  const started = Date.now();
  const deadline = started + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await condition()) return Date.now() - started;
      lastError = undefined;
    } catch (error) {
      lastError = error;
    }
    await new Promise((done) => setTimeout(done, intervalMs));
  }
  throw new Error(
    `timed out after ${String(timeoutMs)} ms waiting for ${what}${lastError === undefined ? "" : `: ${String(lastError)}`}`,
  );
}

export interface Serving {
  readonly child: ChildProcess;
  readonly lines: readonly string[];
  /** Every `key=value` line printed up to readiness. */
  readonly contract: ReadonlyMap<string, string>;
}

/**
 * `calque serve` with `args`, waited on until it prints its readiness line.
 * Everything it writes is appended to `log`, so a failing run can print it.
 */
export async function startServe(
  binary: string,
  args: readonly string[],
  env: NodeJS.ProcessEnv,
  log: string[],
  readyTimeoutMs: number,
): Promise<Serving> {
  const child = spawn(binary, ["serve", ...args], {
    cwd: repositoryRoot,
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const lines: string[] = [];
  let buffered = "";
  child.stdout?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    log.push(chunk);
    buffered += chunk;
    const parts = buffered.split("\n");
    buffered = parts.pop() ?? "";
    for (const part of parts) lines.push(part.trim());
  });
  child.stderr?.setEncoding("utf8");
  child.stderr?.on("data", (chunk: string) => log.push(chunk));
  let exited: number | null = null;
  child.on("exit", (code) => {
    exited = code ?? -1;
  });

  await waitFor(
    "calque serve to report readiness",
    readyTimeoutMs,
    async () => {
      if (exited !== null)
        throw new Error(
          `calque serve exited ${String(exited)} before readiness:\n${log.join("")}`,
        );
      return lines.some((line) => line.startsWith("Calque serve is ready"));
    },
  );

  const contract = new Map(
    lines
      .filter((line) => line.includes("="))
      .map((line) => {
        const at = line.indexOf("=");
        return [line.slice(0, at), line.slice(at + 1)] as const;
      }),
  );
  return { child, lines, contract };
}

export async function stopServe(serving: Serving): Promise<void> {
  if (serving.child.exitCode !== null) return;
  serving.child.kill("SIGTERM");
  await new Promise<void>((done) => {
    const timer = setTimeout(() => {
      serving.child.kill("SIGKILL");
      done();
    }, 20_000);
    serving.child.once("exit", () => {
      clearTimeout(timer);
      done();
    });
  });
}
