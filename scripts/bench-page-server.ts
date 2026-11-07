/** The page server a run serves the built page from, and the bootstrap request it makes to calque. */
import { readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { BROWSER_ACCEPT_ENCODING } from "./bench-brotli.ts";
import { delay } from "./demo-process.ts";
import { parsePort } from "./demo.ts";
import { servePage } from "./lib/serve-e2e.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export const PAGE_ROOT = join(root, "client/examples/incidents/dist");

export interface Connection {
  readonly endpoint: string;
  readonly bearerToken: string;
}

/** What `showPage` writes and the page server reads. */
interface PageControl {
  readonly connection: Connection | null;
  /**
   * Whether the page carries a rendered plan (`rendersPlan`). Such a page is
   * rendered and compressed for its request, with the head `renderBootstrap`
   * returns. Otherwise the page server renders the connection alone, once
   * per connection.
   */
  readonly renderedPlan: boolean;
  /**
   * Where the page server posts its own bootstrap request: calque's loopback
   * address, as a backend beside calque on the edge's internal network
   * reaches it. The connection names the address the browser reaches, which
   * is the edge's when the run has one. Null with no connection.
   */
  readonly backendEndpoint: string | null;
}

export function writePageControl(path: string, control: PageControl): void {
  const next = `${path}.next`;
  writeFileSync(next, JSON.stringify(control), { mode: 0o600 });
  renameSync(next, path);
}

/** The client's `renderBootstrap`, as the page server calls it. */
type RenderBootstrap = (
  start: Connection & BootstrapElement,
  options: {
    readonly acceptEncoding: string | undefined;
    readonly fetch?: typeof fetch;
  },
) => Promise<string>;

/** `renderBootstrap` from the client's own build. */
async function clientRenderBootstrap(): Promise<RenderBootstrap> {
  const { renderBootstrap } = (await import(
    pathToFileURL(
      join(root, "client/packages/client/dist/bootstrap-render-entry.js"),
    ).href
  )) as { renderBootstrap: RenderBootstrap };
  return renderBootstrap;
}

/**
 * The plan calque answers the page server's bootstrap request with: the
 * request `renderBootstrap` posts for the built page's parameters, to
 * calque's loopback address, with Chrome's `Accept-Encoding`.
 */
export async function requestPlan(
  connection: Connection,
  backendEndpoint: string,
): Promise<string> {
  const renderBootstrap = await clientRenderBootstrap();
  const answers: { readonly status: number; readonly plan: string | null }[] =
    [];
  await renderBootstrap(
    {
      ...connection,
      ...bootstrapElement(readFileSync(join(PAGE_ROOT, "index.html"), "utf8")),
      endpoint: backendEndpoint,
    },
    {
      acceptEncoding: BROWSER_ACCEPT_ENCODING,
      fetch: async (input, init) => {
        const response = await fetch(input, init);
        answers.push({
          status: response.status,
          plan: response.status === 200 ? await response.clone().text() : null,
        });
        return response;
      },
    },
  );
  const plan = answers.at(-1)?.plan ?? null;
  if (plan === null)
    throw new Error(
      `calque answered the run's own bootstrap request with ${String(answers.at(-1)?.status ?? "no response")}, not a plan`,
    );
  return plan;
}

/**
 * The page server: the build named on its command line, with what the control
 * file says rendered into its page on every request (`showPage`). For a
 * rendered plan it calls the client's `renderBootstrap` with the parameters
 * the build rendered into its `calque-bootstrap` element, so the page's client
 * and this stand-in for its backend read one value.
 */
export async function servePageCommand(
  arguments_: readonly string[],
): Promise<void> {
  const [port, control, pageRoot, ...rest] = arguments_;
  if (
    port === undefined ||
    control === undefined ||
    pageRoot === undefined ||
    rest.length > 0
  )
    throw new Error(
      "usage: bench.ts serve-page <port> <control file> <page root>",
    );
  const build = resolve(pageRoot);
  const parameters = bootstrapElement(
    readFileSync(join(build, "index.html"), "utf8"),
  );
  const renderBootstrap = await clientRenderBootstrap();
  const server = servePage(parsePort(port, "port"), {
    root: build,
    current: () => {
      const { connection, renderedPlan, backendEndpoint } = JSON.parse(
        readFileSync(control, "utf8"),
      ) as PageControl;
      if (connection === null || !renderedPlan || backendEndpoint === null)
        return { connection, head: null };
      return {
        connection,
        head: (acceptEncoding) =>
          renderBootstrap(
            { ...connection, ...parameters, endpoint: backendEndpoint },
            { acceptEncoding },
          ),
      };
    },
  });
  // `on` for the reason `listenForStop` gives; a later signal finds the close
  // already running.
  let closing: Promise<void> | null = null;
  const stop = () => {
    closing ??= server.close().then(() => process.exit(0));
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  await new Promise(() => undefined);
}

/** The client's parameters a build renders into its page. */
interface BootstrapElement {
  readonly requiredTables: readonly string[];
  readonly memoryBudgetBytes: number;
  readonly clientHeapReserveBytes?: number;
}

/**
 * The `calque-bootstrap` element `vite.config.ts` renders into `html`, which
 * must be there exactly once and well formed.
 */
export function bootstrapElement(html: string): BootstrapElement {
  const elements = [
    ...html.matchAll(
      /<script type="application\/json" id="calque-bootstrap">([^<]*)<\/script>/gu,
    ),
  ];
  const text = elements.length === 1 ? elements[0]?.[1] : undefined;
  const value: unknown = text === undefined ? null : JSON.parse(text);
  const { requiredTables, memoryBudgetBytes, clientHeapReserveBytes } =
    (value ?? {}) as Partial<Record<string, unknown>>;
  if (
    !Array.isArray(requiredTables) ||
    !requiredTables.every((table) => typeof table === "string") ||
    typeof memoryBudgetBytes !== "number" ||
    !["number", "undefined"].includes(typeof clientHeapReserveBytes)
  )
    throw new Error("the page has no well-formed calque-bootstrap element");
  return {
    requiredTables,
    memoryBudgetBytes,
    ...(typeof clientHeapReserveBytes === "number"
      ? { clientHeapReserveBytes }
      : {}),
  };
}

export async function waitForPage(url: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const ok = await fetch(url, { signal: AbortSignal.timeout(1_000) })
      .then((response) => response.ok)
      .catch(() => false);
    if (ok) return;
    await delay(100);
  }
  throw new Error(`the page server did not answer ${url} within 30 s`);
}
