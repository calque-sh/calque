/**
 * `startBootstrap` as the text of a module script a page inlines in its head,
 * so the bootstrap request leaves while the page's own scripts download. Put
 * it byte for byte between `<script type="module">` and `</script>`, before
 * the page's own module scripts: module scripts run in document order, and
 * one ahead would hold it until that script's whole graph had loaded. Under a
 * Content-Security-Policy, add `BOOTSTRAP_START_SCRIPT_SHA256` to
 * `script-src`; `'unsafe-inline'` is not needed.
 *
 * The script reads three JSON elements anywhere in the document:
 * `calque-connection` (`{ "endpoint", "bearerToken" }`, rendered by the
 * backend with the page's grant), `calque-plan` (from `renderBootstrap`, if
 * used) and `calque-bootstrap` (`{ "requiredTables", "memoryBudgetBytes",
 * "clientHeapReserveBytes"?, "deviceId"? }`). Without a well-formed connection it sends
 * nothing; with one it does what `startBootstrap` does given the plan's text
 * as `renderedPlan`.
 *
 * The text is a plain template literal, its backslashes doubled, so that a
 * bundler can drop it from a page that imports only `startBootstrap`: a
 * tagged template is a call, which a bundler keeps unless it can prove it
 * pure.
 */

export const BOOTSTRAP_START_SCRIPT: string = `const slot = Symbol.for("@calque/client:started-bootstrap");
const rendered = (id) => {
  try {
    return JSON.parse(document.getElementById(id)?.textContent || "null");
  } catch {
    return null;
  }
};
const strings = (value) =>
  Array.isArray(value) && value.every((item) => typeof item === "string");
const device = () => {
  try {
    const stored = localStorage.getItem("calque-device-id");
    if (stored !== null && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u.test(stored))
      return stored;
    const created = crypto.randomUUID();
    localStorage.setItem("calque-device-id", created);
    return created;
  } catch {
    return crypto.randomUUID();
  }
};
const supported = () => {
  let moduleWorkers = false;
  try {
    new Worker("http://[", { get type() { moduleWorkers = true; return "module"; } });
  } catch {}
  return (
    moduleWorkers &&
    typeof Response === "function" &&
    typeof ReadableStreamBYOBReader === "function" &&
    typeof globalThis.crypto?.subtle?.digest === "function" &&
    typeof crypto.randomUUID === "function" &&
    typeof structuredClone === "function" &&
    typeof Promise.withResolvers === "function" &&
    typeof String.prototype.isWellFormed === "function"
  );
};
const decodes = () => {
  try {
    return (
      new WebAssembly.Module(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0])) instanceof
      WebAssembly.Module
    );
  } catch {
    return false;
  }
};
const start = () => {
  const connection = rendered("calque-connection");
  if (
    globalThis[slot] !== undefined ||
    !supported() ||
    typeof connection?.endpoint !== "string" ||
    typeof connection.bearerToken !== "string"
  )
    return;
  const value = connection.endpoint;
  if (value.length === 0 || value.length > 8192 || /\\p{Cc}/u.test(value))
    return;
  let endpoint;
  try {
    endpoint = /^\\/(?![/\\\\])/u.test(value)
      ? new URL(value, location.href)
      : new URL(value);
  } catch {
    return;
  }
  const localHttp =
    endpoint.protocol === "http:" &&
    (endpoint.hostname === "localhost" || endpoint.hostname === "127.0.0.1");
  if (
    (endpoint.protocol !== "https:" && !localHttp) ||
    endpoint.username.length > 0 ||
    endpoint.password.length > 0 ||
    endpoint.hash.length > 0
  )
    return;
  if (!endpoint.pathname.endsWith("/")) endpoint.pathname += "/";
  const url = new URL("v1/bootstrap", endpoint).href;
  const rendering = rendered("calque-plan");
  let expiresAt;
  try {
    expiresAt = JSON.parse(rendering?.plan).expiresAt;
  } catch {}
  if (
    typeof rendering?.clientInstanceId === "string" &&
    rendering.clientInstanceId.length > 0 &&
    typeof rendering.plan === "string" &&
    typeof rendering.transportProfile === "string" &&
    typeof rendering.protocolVersion === "number" &&
    strings(rendering.requiredTables) &&
    strings(rendering.acceptedEncodings) &&
    typeof rendering.memoryBudgetBytes === "number" &&
    typeof rendering.clientHeapReserveBytes === "number" &&
    typeof rendering.deviceId === "string" &&
    rendering.deviceId.length > 0 &&
    typeof expiresAt === "string" &&
    Date.parse(expiresAt) > Date.now()
  ) {
    performance.mark("calque:bootstrap:rendered");
    globalThis[slot] = Object.freeze({
      parameters: Object.freeze({
        url,
        bearerToken: connection.bearerToken,
        transportProfile: rendering.transportProfile,
        protocolVersion: rendering.protocolVersion,
        requiredTables: Object.freeze([...rendering.requiredTables]),
        acceptedEncodings: Object.freeze([...rendering.acceptedEncodings]),
        memoryBudgetBytes: rendering.memoryBudgetBytes,
        clientHeapReserveBytes: rendering.clientHeapReserveBytes,
        deviceId: rendering.deviceId,
      }),
      clientInstanceId: rendering.clientInstanceId,
      response: Promise.resolve(
        new Response(rendering.plan, {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      ),
    });
    return;
  }
  const request = rendered("calque-bootstrap");
  if (
    !strings(request?.requiredTables) ||
    typeof request.memoryBudgetBytes !== "number" ||
    !["number", "undefined"].includes(typeof request.clientHeapReserveBytes) ||
    !["string", "undefined"].includes(typeof request.deviceId)
  )
    return;
  const budget = request.memoryBudgetBytes;
  const parameters = Object.freeze({
    url,
    bearerToken: connection.bearerToken,
    transportProfile: "chromium-native-artifact-v1",
    protocolVersion: 1,
    requiredTables: Object.freeze([...request.requiredTables].sort()),
    acceptedEncodings: Object.freeze(
      decodes() ? ["packed-brotli-large-window", "brotli-large-window", "zstd", "identity"] : ["zstd", "identity"],
    ),
    memoryBudgetBytes: budget,
    clientHeapReserveBytes:
      request.clientHeapReserveBytes ??
      Math.min(
        64 * 1024 * 1024,
        Math.max(1, Math.min(4 * 1024 * 1024, budget), Math.floor(budget / 8)),
        budget,
      ),
    deviceId: request.deviceId ?? device(),
  });
  const clientInstanceId = crypto.randomUUID();
  performance.mark("calque:bootstrap:started");
  let response;
  try {
    response = fetch(parameters.url, {
      method: "POST",
      headers: {
        authorization: "Bearer " + parameters.bearerToken,
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
    });
  } catch (error) {
    response = Promise.reject(error);
  }
  response.catch(() => undefined);
  globalThis[slot] = Object.freeze({ parameters, clientInstanceId, response });
};
start();`;

/** `BOOTSTRAP_START_SCRIPT`'s hash, as a CSP `script-src` source. */
export const BOOTSTRAP_START_SCRIPT_SHA256 =
  "sha256-d6/MX2OAEZzCd0dA4QuwhFCzqL8UwpwFQoZKpQNOmk4=";
