/**
 * The backend's half of a rendered bootstrap. While rendering a page, the
 * backend posts the bootstrap request the page's client would post and writes
 * the plan into the head beside a preload of each artifact, so the browser
 * starts downloading the working set before any of the page's scripts has
 * loaded. It runs in any runtime with `fetch` and `crypto.randomUUID`; the
 * request carries no `Origin`, so CORS does not apply and the bearer token
 * authenticates it.
 */

import {
  allowedEndpoint,
  type BootstrapRequest,
  bootstrapInit,
  bootstrapUrl,
  DECODING_ARTIFACT_ENCODINGS,
  type RenderedConnection,
  type RenderedPlan,
  requestedParameters,
} from "./bootstrap-start.js";

/** The largest plan the session reads, as `readBootstrapPlan` bounds it. */
const MAX_PLAN_BYTES = 2 * 1024 * 1024;

export interface RenderBootstrapOptions {
  /**
   * The `Accept-Encoding` header of the browser's request for the page. The
   * browser, not the backend, downloads the artifact, so the bootstrap
   * request sends the browser's header. Without it nothing is posted or
   * rendered.
   */
  readonly acceptEncoding: string | undefined;
  /** The `fetch` to post with; the runtime's own when absent. */
  readonly fetch?: typeof fetch;
  /** Ends the request, for a page that will not wait longer for its plan. */
  readonly signal?: AbortSignal;
}

/**
 * Posts the bootstrap request for `start` and returns the HTML for the page's
 * `<head>`: a `<link rel="preload" as="fetch" crossorigin="anonymous">` for
 * each artifact the plan names, and the `calque-plan` element the page's
 * starter reads in place of posting a request. It returns the empty string,
 * and the starter posts its own request, when `acceptEncoding` is undefined
 * or the request fails or answers with anything but a plan whose artifacts
 * are all zstd, `brotli-large-window` or `packed-brotli-large-window`.
 *
 * The page carries the bearer token and the artifact URLs, which are
 * capabilities, so it must be served with `Cache-Control: no-store`. Its
 * client must be created with the endpoint, bearer token and collections in
 * `start`, and with no `memoryBudgetBytes` or the one in `start`; if any
 * differs, or the page cannot run the decoder worker a brotli plan needs,
 * the session posts its own request and the browser downloads the working
 * set twice.
 */
export async function renderBootstrap(
  start: BootstrapRequest & RenderedConnection,
  options: RenderBootstrapOptions,
): Promise<string> {
  const endpoint = allowedEndpoint(start.endpoint);
  if (endpoint === undefined || options.acceptEncoding === undefined) return "";
  const request: Omit<RenderedPlan, "plan"> = {
    ...requestedParameters(start, DECODING_ARTIFACT_ENCODINGS, () =>
      globalThis.crypto.randomUUID(),
    ),
    clientInstanceId: globalThis.crypto.randomUUID(),
  };
  const init = bootstrapInit(
    { ...request, bearerToken: start.bearerToken },
    request.clientInstanceId,
    options.signal,
  );
  let plan: string;
  try {
    const response = await (options.fetch ?? globalThis.fetch)(
      bootstrapUrl(endpoint).href,
      {
        ...init,
        headers: { ...init.headers, "accept-encoding": options.acceptEncoding },
      },
    );
    if (response.status !== 200) {
      await response.body?.cancel();
      return "";
    }
    plan = await response.text();
  } catch {
    return "";
  }
  if (new TextEncoder().encode(plan).byteLength > MAX_PLAN_BYTES) return "";
  const urls = artifactUrls(plan);
  if (urls === undefined) return "";
  const element: RenderedPlan = { ...request, plan };
  return (
    urls
      .map(
        (url) =>
          `<link rel="preload" as="fetch" crossorigin="anonymous" href="${attribute(url)}">`,
      )
      .join("") +
    `<script type="application/json" id="calque-plan">${scriptText(element)}</script>`
  );
}

/**
 * Each artifact's download URL, as the session will fetch it, or undefined
 * for text that is not a plan naming a zstd, `brotli-large-window` or
 * `packed-brotli-large-window` representation at an HTTP URL for each
 * artifact. An identity plan, as when a proxy rewrote the browser's
 * `Accept-Encoding`, is not rendered: its preload would download several
 * times the bytes the browser's own request gets.
 */
function artifactUrls(text: string): string[] | undefined {
  let plan: unknown;
  try {
    plan = JSON.parse(text);
  } catch {
    return undefined;
  }
  const artifacts = (plan as { artifacts?: unknown } | null)?.artifacts;
  if (!Array.isArray(artifacts)) return undefined;
  const urls: string[] = [];
  for (const artifact of artifacts) {
    const representation = (
      artifact as {
        selectedRepresentation?: { encoding?: unknown; downloadUrl?: unknown };
      } | null
    )?.selectedRepresentation;
    const url = representation?.downloadUrl;
    const encoding = representation?.encoding;
    if (
      (encoding !== "zstd" &&
        encoding !== "brotli-large-window" &&
        encoding !== "packed-brotli-large-window") ||
      typeof url !== "string"
    )
      return undefined;
    let protocol: string;
    try {
      ({ protocol } = new URL(url));
    } catch {
      return undefined;
    }
    if (protocol !== "https:" && protocol !== "http:") return undefined;
    urls.push(url);
  }
  return urls;
}

/** JSON that a script element holds as it is: no `<` can end it early. */
function scriptText(value: unknown): string {
  return JSON.stringify(value).replaceAll("<", "\\u003c");
}

/** Text for a double-quoted attribute value. */
function attribute(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}
