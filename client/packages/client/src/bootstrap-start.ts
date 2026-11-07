/**
 * A bootstrap request the page starts before the client library has loaded,
 * and the slot the session takes it from. This module imports nothing, so a
 * page can put it in a chunk of its own that evaluates before the client
 * library's.
 *
 * The slot is a `globalThis` property under a registered symbol. A script
 * that can write it can already replace `fetch` or read the page's bearer, so
 * it is not a new way in; the session checks the shape of what it finds.
 *
 * The session adopts a started request only on the first activation of the
 * first client in the page, and only if every parameter it would send, its
 * own `auth()` bearer token included, equals the started request's; the
 * adopted response then goes through the session's own checks. The started
 * request goes through the page's global `fetch`, so a page whose client
 * `fetch` changes the bootstrap request must not start one.
 */

export const BOOTSTRAP_PROTOCOL_VERSION = 1;
export const CHROMIUM_NATIVE_ARTIFACT_PROFILE = "chromium-native-artifact-v1";
/** What a page that can run the decoder worker names, best first. */
export const DECODING_ARTIFACT_ENCODINGS = Object.freeze([
  "packed-brotli-large-window",
  "brotli-large-window",
  "zstd",
  "identity",
] as const);
/** What every other page names: what the browser itself decodes. */
export const NATIVE_ARTIFACT_ENCODINGS = Object.freeze([
  "zstd",
  "identity",
] as const);
export type ArtifactEncodings =
  typeof DECODING_ARTIFACT_ENCODINGS | typeof NATIVE_ARTIFACT_ENCODINGS;

/** Where a started request waits for the session. */
const STARTED_BOOTSTRAP_SLOT = Symbol.for("@calque/client:started-bootstrap");
const MAX_ENDPOINT_CODE_UNITS = 8_192;
const CONTROL_CHARACTER = /\p{Cc}/u;
/** A path on the page's origin, such as `/_calque/`: `//` and `/\` name a host. */
export const PAGE_PATH = /^\/(?![/\\])/u;
const MAX_DEFAULT_CLIENT_HEAP_RESERVE_BYTES = 64 * 1024 * 1024;
const MIN_DEFAULT_CLIENT_HEAP_RESERVE_BYTES = 4 * 1024 * 1024;
export const DEFAULT_MEMORY_BUDGET_BYTES = 1024 * 1024 * 1024;
/** The smallest WebAssembly module: the magic number and the version. */
const EMPTY_WASM_MODULE = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

/** Everything a bootstrap request sends, besides its `clientInstanceId`. */
export interface BootstrapParameters {
  /** The request's URL: the endpoint's `v1/bootstrap`. */
  readonly url: string;
  readonly bearerToken: string;
  readonly transportProfile: string;
  readonly protocolVersion: number;
  /** Sorted, as the session sends them. */
  readonly requiredTables: readonly string[];
  readonly acceptedEncodings: readonly string[];
  readonly memoryBudgetBytes: number;
  readonly clientHeapReserveBytes: number;
  /** The device the plan is bound to beside the client instance. */
  readonly deviceId: string;
}

/**
 * What `auth()` returns: the grant the application backend issued, presented
 * as a bearer token.
 */
export interface SyncAuth {
  readonly bearerToken: string;
}

/** A request the page started, waiting in the slot for the session. */
export interface StartedBootstrap {
  readonly parameters: BootstrapParameters;
  /** The id the request sent; the server's grant binds it. */
  readonly clientInstanceId: string;
  /** The fetch, as it settles; the session reads it only if it adopts it. */
  readonly response: Promise<Response>;
}

/**
 * The values the page creates its client with, which its bootstrap request
 * carries.
 */
export interface BootstrapRequest {
  /** The generated collections' source relations, as `schema.table`. */
  readonly requiredTables: readonly string[];
  /** As the client is created with it; both default to 1 GiB. */
  readonly memoryBudgetBytes?: number;
  /** The client's own default when absent, as `createSyncClient` applies it. */
  readonly clientHeapReserveBytes?: number;
  /**
   * The `deviceId` the client is created with. When absent, the page's
   * starter sends the id the client keeps in `localStorage` and
   * `renderBootstrap` a new one, which a client created without a `deviceId`
   * takes from the plan.
   */
  readonly deviceId?: string;
}

/**
 * The `calque-connection` element's JSON, which the backend that serves the
 * page renders with the page's grant.
 */
export interface RenderedConnection {
  /** The endpoint the client is created with. */
  readonly endpoint: string;
  /** The grant the client's first `auth()` returns. */
  readonly bearerToken: string;
}

/**
 * What `startBootstrap` needs: the values the page will create its client
 * with, so the request it posts matches the client's own.
 */
export interface BootstrapStart extends BootstrapRequest {
  /**
   * The endpoint the client is created with; the rendered connection's when
   * absent.
   */
  readonly endpoint?: string;
  /**
   * The token the client's first `auth()` returns; the rendered connection's
   * when absent.
   */
  readonly bearerToken?: string;
  /**
   * The text of the `calque-plan` element `renderBootstrap` wrote; the page's
   * own element's when absent. With a usable plan in it, nothing is posted.
   */
  readonly renderedPlan?: string;
}

/**
 * The `calque-plan` element's JSON: the request the backend posted, less the
 * URL and bearer token the page's connection already names, and the plan the
 * server answered with, as the text it sent.
 */
export interface RenderedPlan extends Omit<
  BootstrapParameters,
  "url" | "bearerToken"
> {
  /** The id the backend's request sent; the server's grant binds it. */
  readonly clientInstanceId: string;
  readonly plan: string;
}

type Slot = Record<typeof STARTED_BOOTSTRAP_SLOT, unknown>;

/**
 * Posts the bootstrap request the client would post, and leaves it in the
 * slot; given a usable `renderedPlan`, it posts nothing. It does nothing if
 * the slot is full, in a browser the client would refuse, or for an endpoint
 * `createSyncClient` would refuse, so a page can call it unconditionally and
 * a token never goes where the client would not send it.
 */
export function startBootstrap(start: BootstrapStart): void {
  if (
    (globalThis as unknown as Slot)[STARTED_BOOTSTRAP_SLOT] !== undefined ||
    !chromiumNativeArtifactProfile()
  )
    return;
  const page = globalThis.document as Document | undefined;
  const rendered = readConnection(page);
  const endpoint = allowedEndpoint(start.endpoint ?? rendered?.endpoint);
  const bearerToken = start.bearerToken ?? rendered?.bearerToken;
  if (endpoint === undefined || bearerToken === undefined) return;
  const url = bootstrapUrl(endpoint).href;
  const renderedPlan =
    start.renderedPlan ?? page?.getElementById("calque-plan")?.textContent;
  if (
    typeof renderedPlan === "string" &&
    adoptRenderedPlan(url, bearerToken, renderedPlan)
  )
    return;
  postBootstrap({ url, bearerToken, ...requestedParameters(start) });
}

/**
 * The connection the backend rendered into `page` as its `calque-connection`
 * element. It throws if the page has none, or one that does not hold a string
 * `endpoint` and `bearerToken`.
 */
export function renderedConnection(
  page: Pick<Document, "getElementById"> = globalThis.document,
): RenderedConnection {
  const connection = readConnection(page);
  if (connection === undefined)
    throw new Error(
      'the page has no calque-connection element holding a string "endpoint" and "bearerToken"',
    );
  return connection;
}

/**
 * An `auth` for `createSyncClient`: its first call returns the rendered
 * connection's bearer, which the started request sent, and every later call
 * returns `refresh()`. On a page without a rendered connection, the first
 * call returns `refresh()` too.
 */
export function startedAuth(
  refresh: () => SyncAuth | Promise<SyncAuth>,
): () => SyncAuth | Promise<SyncAuth> {
  let first = true;
  return () => {
    const rendered = first
      ? readConnection(globalThis.document as Document | undefined)
      : undefined;
    first = false;
    return rendered ? { bearerToken: rendered.bearerToken } : refresh();
  };
}

/**
 * The parameters the client would send for `start`, less the URL and bearer
 * token, which each path supplies itself.
 */
export function requestedParameters(
  start: BootstrapRequest,
  acceptedEncodings: ArtifactEncodings = acceptedArtifactEncodings(),
  deviceId: () => string = storedDeviceId,
): Omit<BootstrapParameters, "url" | "bearerToken"> {
  const memoryBudgetBytes =
    start.memoryBudgetBytes ?? DEFAULT_MEMORY_BUDGET_BYTES;
  return {
    transportProfile: CHROMIUM_NATIVE_ARTIFACT_PROFILE,
    protocolVersion: BOOTSTRAP_PROTOCOL_VERSION,
    requiredTables: [...start.requiredTables].sort(),
    acceptedEncodings,
    memoryBudgetBytes,
    clientHeapReserveBytes:
      start.clientHeapReserveBytes ??
      defaultClientHeapReserveBytes(memoryBudgetBytes),
    deviceId: start.deviceId ?? deviceId(),
  };
}

const DEVICE_ID_STORAGE_KEY = "calque-device-id";
const UUID_TEXT =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const REGEXP_TEST_VALUE = Function.prototype.call.bind(
  RegExp.prototype.test,
) as (expression: RegExp, value: string) => boolean;

/**
 * The id this browser keeps in `localStorage` for its installation, created
 * on first use; a fresh one where storage is unavailable.
 */
export function storedDeviceId(): string {
  try {
    const stored = globalThis.localStorage.getItem(DEVICE_ID_STORAGE_KEY);
    if (stored !== null && REGEXP_TEST_VALUE(UUID_TEXT, stored)) return stored;
    const created = globalThis.crypto.randomUUID();
    globalThis.localStorage.setItem(DEVICE_ID_STORAGE_KEY, created);
    return created;
  } catch {
    return globalThis.crypto.randomUUID();
  }
}

/**
 * Posts a bootstrap request with exactly these parameters and leaves it in
 * the slot.
 */
function postBootstrap(requested: BootstrapParameters): void {
  const parameters: BootstrapParameters = Object.freeze({
    ...requested,
    requiredTables: Object.freeze([...requested.requiredTables]),
    acceptedEncodings: Object.freeze([...requested.acceptedEncodings]),
  });
  const clientInstanceId = globalThis.crypto.randomUUID();
  globalThis.performance?.mark?.("calque:bootstrap:started");
  let response: Promise<Response>;
  try {
    response = globalThis.fetch(
      parameters.url,
      bootstrapInit(parameters, clientInstanceId),
    );
  } catch (error) {
    response = Promise.reject(error);
  }
  // A request no session adopts is never read; its failure is not an
  // unhandled rejection. The session that adopts it still sees the failure.
  response.catch(() => undefined);
  (globalThis as unknown as Slot)[STARTED_BOOTSTRAP_SLOT] = Object.freeze({
    parameters,
    clientInstanceId,
    response,
  });
}

/**
 * Leaves the plan the backend rendered in the slot, as the response to the
 * request the backend posted to `url` with `bearerToken`, and returns whether
 * it did. It returns false for text that is not the JSON `renderBootstrap`
 * writes and for a plan already expired by this browser's clock, so that the
 * page posts its own request instead.
 */
function adoptRenderedPlan(
  url: string,
  bearerToken: string,
  text: string,
): boolean {
  const rendered = parseJson(text);
  if (!isRenderedPlan(rendered)) return false;
  const plan = parseJson(rendered.plan) as { expiresAt?: unknown } | undefined;
  if (
    typeof plan?.expiresAt !== "string" ||
    !(Date.parse(plan.expiresAt) > Date.now())
  )
    return false;
  const parameters: BootstrapParameters = Object.freeze({
    url,
    bearerToken,
    transportProfile: rendered.transportProfile,
    protocolVersion: rendered.protocolVersion,
    requiredTables: Object.freeze([...rendered.requiredTables]),
    acceptedEncodings: Object.freeze([...rendered.acceptedEncodings]),
    memoryBudgetBytes: rendered.memoryBudgetBytes,
    clientHeapReserveBytes: rendered.clientHeapReserveBytes,
    deviceId: rendered.deviceId,
  });
  globalThis.performance?.mark?.("calque:bootstrap:rendered");
  (globalThis as unknown as Slot)[STARTED_BOOTSTRAP_SLOT] = Object.freeze({
    parameters,
    clientInstanceId: rendered.clientInstanceId,
    response: Promise.resolve(
      new Response(rendered.plan, {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    ),
  });
  return true;
}

/**
 * The started request, once: the slot is empty after the first read. A value
 * in the slot that is not a started request is taken and dropped.
 */
export function takeStartedBootstrap(): StartedBootstrap | undefined {
  const slot = globalThis as unknown as Slot;
  const value = slot[STARTED_BOOTSTRAP_SLOT];
  Reflect.deleteProperty(slot, STARTED_BOOTSTRAP_SLOT);
  return isStartedBootstrap(value) ? value : undefined;
}

/**
 * The first parameter in which a started request differs from the request
 * the session would send itself, or undefined if it sent exactly that one.
 */
export function differingBootstrapParameter(
  started: BootstrapParameters,
  own: BootstrapParameters,
): keyof BootstrapParameters | undefined {
  if (started.url !== own.url) return "url";
  if (started.bearerToken !== own.bearerToken) return "bearerToken";
  if (started.transportProfile !== own.transportProfile)
    return "transportProfile";
  if (started.protocolVersion !== own.protocolVersion) return "protocolVersion";
  if (!sameList(started.requiredTables, own.requiredTables))
    return "requiredTables";
  if (!sameList(started.acceptedEncodings, own.acceptedEncodings))
    return "acceptedEncodings";
  if (started.memoryBudgetBytes !== own.memoryBudgetBytes)
    return "memoryBudgetBytes";
  if (started.clientHeapReserveBytes !== own.clientHeapReserveBytes)
    return "clientHeapReserveBytes";
  if (started.deviceId !== own.deviceId) return "deviceId";
  return undefined;
}

/** Leaves a request no session adopted unread, and lets its body go. */
export function discardStartedBootstrap(started: StartedBootstrap): void {
  started.response.then(
    (response) => response.body?.cancel().catch(() => undefined),
    () => undefined,
  );
}

/** The endpoint's `v1/bootstrap`, the endpoint read as a directory. */
export function bootstrapUrl(endpoint: URL): URL {
  const directory = new URL(endpoint.href);
  if (!directory.pathname.endsWith("/")) directory.pathname += "/";
  return new URL("v1/bootstrap", directory);
}

/** The request both paths send, so the two cannot drift apart. */
export function bootstrapInit(
  parameters: Omit<BootstrapParameters, "url">,
  clientInstanceId: string,
  signal?: AbortSignal,
): RequestInit & { readonly headers: Readonly<Record<string, string>> } {
  return {
    method: "POST",
    headers: {
      authorization: `Bearer ${parameters.bearerToken}`,
      "content-type": "application/json",
      accept: "application/json",
      "x-calque-artifact-profile": parameters.transportProfile,
    },
    body: JSON.stringify({
      protocolVersion: parameters.protocolVersion,
      requiredTables: parameters.requiredTables,
      cachedArtifactHints: [],
      acceptedEncodings: parameters.acceptedEncodings,
      memoryBudgetBytes: parameters.memoryBudgetBytes,
      clientHeapReserveBytes: parameters.clientHeapReserveBytes,
      clientInstanceId,
      deviceId: parameters.deviceId,
    }),
    ...(signal ? { signal } : {}),
  };
}

/** Whether this browser is the tested Chromium native-decode profile. */
export function chromiumNativeArtifactProfile(): boolean {
  const navigatorValue = globalThis.navigator as
    | (Navigator & {
        readonly userAgentData?: {
          readonly brands: readonly { readonly brand: string }[];
        };
      })
    | undefined;
  return (
    navigatorValue?.userAgentData?.brands.some(
      ({ brand }) => brand === "Chromium",
    ) === true &&
    typeof globalThis.ReadableStreamBYOBReader === "function" &&
    typeof globalThis.Response === "function"
  );
}

/**
 * Whether this page can run the brotli decoder: it has workers, and
 * WebAssembly compiles here, which a Content-Security-Policy without
 * `wasm-unsafe-eval` refuses. The page's inline starter makes the same two
 * checks, so both name the same list. A worker under a stricter policy than
 * its page is not detected.
 */
export function brotliDecoderRuns(): boolean {
  if (typeof globalThis.Worker !== "function") return false;
  try {
    return (
      new WebAssembly.Module(new Uint8Array(EMPTY_WASM_MODULE)) instanceof
      WebAssembly.Module
    );
  } catch {
    return false;
  }
}

/** The encodings this page's bootstrap request names. */
export function acceptedArtifactEncodings(): ArtifactEncodings {
  return brotliDecoderRuns()
    ? DECODING_ARTIFACT_ENCODINGS
    : NATIVE_ARTIFACT_ENCODINGS;
}

/** The heap reserve a client takes when its options name none. */
export function defaultClientHeapReserveBytes(
  memoryBudgetBytes: number,
): number {
  const proportional = Math.floor(memoryBudgetBytes / 8);
  return Math.min(
    MAX_DEFAULT_CLIENT_HEAP_RESERVE_BYTES,
    Math.max(
      1,
      Math.min(MIN_DEFAULT_CLIENT_HEAP_RESERVE_BYTES, memoryBudgetBytes),
      proportional,
    ),
    memoryBudgetBytes,
  );
}

/**
 * The endpoint as a URL, under the rules `createSyncClient` applies. A path
 * such as `/_calque/` is read against the page's URL.
 */
export function allowedEndpoint(value: unknown): URL | undefined {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > MAX_ENDPOINT_CODE_UNITS ||
    CONTROL_CHARACTER.test(value)
  )
    return undefined;
  let url: URL;
  try {
    url = PAGE_PATH.test(value)
      ? new URL(value, globalThis.location?.href)
      : new URL(value);
  } catch {
    return undefined;
  }
  const localHttp =
    url.protocol === "http:" &&
    (url.hostname === "localhost" || url.hostname === "127.0.0.1");
  if (
    (url.protocol !== "https:" && !localHttp) ||
    url.username.length > 0 ||
    url.password.length > 0 ||
    url.hash.length > 0
  )
    return undefined;
  return url;
}

function readConnection(
  page: Pick<Document, "getElementById"> | undefined,
): RenderedConnection | undefined {
  const text = page?.getElementById("calque-connection")?.textContent;
  const value = text ? parseJson(text) : undefined;
  if (typeof value !== "object" || value === null) return undefined;
  const { endpoint, bearerToken } = value as Partial<Record<string, unknown>>;
  return typeof endpoint === "string" && typeof bearerToken === "string"
    ? { endpoint, bearerToken }
    : undefined;
}

function isStartedBootstrap(value: unknown): value is StartedBootstrap {
  if (typeof value !== "object" || value === null) return false;
  const { parameters, clientInstanceId, response } =
    value as Partial<StartedBootstrap>;
  return (
    typeof clientInstanceId === "string" &&
    response instanceof Promise &&
    typeof parameters === "object" &&
    parameters !== null &&
    typeof parameters.url === "string" &&
    typeof parameters.bearerToken === "string" &&
    hasRequestFields(parameters)
  );
}

function isRenderedPlan(value: unknown): value is RenderedPlan {
  if (typeof value !== "object" || value === null) return false;
  const rendered = value as Partial<RenderedPlan>;
  return (
    typeof rendered.clientInstanceId === "string" &&
    rendered.clientInstanceId.length > 0 &&
    typeof rendered.plan === "string" &&
    hasRequestFields(rendered)
  );
}

/** Whether `value` holds the fields `requestedParameters` returns. */
function hasRequestFields(
  value: Partial<Omit<BootstrapParameters, "url" | "bearerToken">>,
): boolean {
  return (
    typeof value.transportProfile === "string" &&
    typeof value.protocolVersion === "number" &&
    stringList(value.requiredTables) &&
    stringList(value.acceptedEncodings) &&
    typeof value.memoryBudgetBytes === "number" &&
    typeof value.clientHeapReserveBytes === "number" &&
    typeof value.deviceId === "string" &&
    value.deviceId.length > 0
  );
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
}

function stringList(value: unknown): value is readonly string[] {
  return (
    Array.isArray(value) && value.every((item) => typeof item === "string")
  );
}

function sameList(left: readonly string[], right: readonly string[]): boolean {
  return (
    left.length === right.length &&
    left.every((value, index) => value === right[index])
  );
}
