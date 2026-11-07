# @calque/client

Calque's browser client. It syncs complete, authorized PostgreSQL collections
and queries them locally through typed facades. The
[repository README](../../../README.md) introduces Calque and shows the query
surface.

```sh
npm install @calque/client
```

## What a query returns

A filter treats NULL as a PostgreSQL WHERE clause does: a comparison with
NULL is unknown, so `neq("assignee", 7n)` and `not(gt("score", 0.3))` leave
out rows where that column is NULL, and `isNull` and `isNotNull` test for NULL.

An `int2` or `int4` column is a `number` in TypeScript and an `int8` one a
`bigint`. `sum` over any of the three is a `bigint`, as PostgreSQL's `sum`
over `int2` and `int4` is, and over a `float4` or `float8` column a `number`.

`run()` returns the whole result or rejects with `RESULT_TOO_LARGE`, never part
of it. By default it rejects a result over 1,000 rows or 4 MiB;
`run({ maxRows, maxBytes })` raises those limits to at most 10,000 rows and
16 MiB. `window(offset, count)` reads up to 10,000 consecutive rows from
anywhere in a result of any size, together with the result's total row count.

## The grant verification key

In production, create the client with `grantVerificationKey`: the
`{ keyId, publicKey }` of the Ed25519 key your backend signs grants with, the
object `calque serve`'s `--grant-verification-key-env` reads. When the
application revokes or narrows a user's access, each affected page receives a
barrier the backend signed with that key. A client created with the key
verifies it, empties every subscription, ends each pending mutation as
ambiguous, and rebuilds from the next grant `auth()` returns. A client created
without it is refused at its next change request and rebuilds through `auth()`
without emptying its subscriptions first. `createSyncClient` throws on a
malformed key. [Revoking access](../../../docs/design/operations.md#revoking-access)
has the backend's side.

## Starting the load early

`createSyncClient` starts loading as soon as it is called: it asks `auth` for
a grant and sends the bootstrap request without waiting for anything else.
Call it in the page's entry module and load the rest of the page with
`import()`. An entry that imports its UI statically evaluates every one of
those modules before its own body runs, and the request waits for all of
them. The incident-operations page's entry,
[`client/examples/incidents/src/main.ts`](../../examples/incidents/src/main.ts),
is written this way.

The request can leave before the client library has loaded. When the backend
that serves the page's HTML also issues its grant, it renders the connection
into the page as
`<script type="application/json" id="calque-connection">{"endpoint":"…","bearerToken":"…"}</script>`,
with `<` written as `\u003c`, and serves that HTML with
`Cache-Control: no-store`, since it carries a bearer. The endpoint can be a
path on the page's origin, such as `/_calque/`. The page's first module calls
`startBootstrap` from `@calque/client/bootstrap-start`, which imports nothing
else and reads the connection from the page:

```ts
import { startBootstrap } from "@calque/client/bootstrap-start";

startBootstrap({ requiredTables, memoryBudgetBytes });
```

The page creates its client from the same connection:

```ts
import { createSyncClient } from "@calque/client";
import {
  renderedConnection,
  startedAuth,
} from "@calque/client/bootstrap-start";

const client = createSyncClient({
  endpoint: renderedConnection().endpoint,
  auth: startedAuth(fetchGrant),
  schema,
  memoryBudgetBytes,
});
```

`renderedConnection()` throws if the page has no `calque-connection` element
holding a string `endpoint` and `bearerToken`. `startedAuth(refresh)` returns
an `auth` whose first call returns the rendered bearer, the one the started
request sent, and whose every later call returns `refresh()`. The client calls
`auth` again for each mutation and each reconnect, after the rendered grant
may have expired, so `refresh` asks the backend for a current one. On a page
without a rendered connection, the first call returns `refresh()` too.

`requiredTables` are the generated collections' source relations, as
`schema.table`. The bindings' renderer exports them from
`calque.start.generated.ts`, beside the bindings, a module that imports
nothing. The first client the page creates adopts the started request,
on its first activation, only if it would have sent the same request itself:
the same endpoint, the bearer its own `auth()` returns, the same tables,
memory budget and heap reserve, and the same `deviceId` if it was created
with one. If not, it cancels it, warns in the console naming the parameter
that differs (never its value), adds a `calque:bootstrap:refused` performance
mark, and sends its own. A started request names the `deviceId` the page
passes, else the device id the client keeps in `localStorage`. The started
request goes through the page's global `fetch`, so a page whose client
`fetch` changes the bootstrap request must not start one. A module's code runs
only once its whole static graph has loaded, so a starter in a chunk of its
own saves the client library's evaluation, not the scripts' round trip. An
application that fetches its grant with a request of its own after load gains
nothing from it. The Vite plugin's `bootstrap` option, under
[Bundling](#bundling), writes such a starter and gives it that chunk.

A starter inlined in the page's head sends the request as soon as the
document is parsed, while the page's scripts are still downloading.
`BOOTSTRAP_START_SCRIPT`, from the same subpath, is `startBootstrap` as the
text of a module script. It goes, as it is, between `<script type="module">`
and `</script>`, ahead of the page's own module scripts, which would hold it
until their graphs had loaded. Beside it, the page carries the client's
parameters as
`<script type="application/json" id="calque-bootstrap">{"requiredTables":[…],"memoryBudgetBytes":…}</script>`,
with `<` written as `\u003c`; the backend still renders `calque-connection`
with each response. The script sends nothing unless both elements are there
and well formed, and it fills the same slot, so the page's first module can
still call `startBootstrap`: with the slot full, that call sends nothing. A
page with a Content-Security-Policy adds `BOOTSTRAP_START_SCRIPT_SHA256` to
`script-src`. A client created without `memoryBudgetBytes` takes the budget
and heap reserve in `calque-bootstrap`. The Vite plugin's `bootstrap` option writes the script and
that element, after Vite's own processing of the page, which would bundle an
inline module script and change the text the hash covers.

The backend that renders the page can send the request itself, before the
page reaches the browser. `renderBootstrap`, from
`@calque/client/bootstrap-render`, posts the request the page's client would
post and returns HTML for the page's head: a
`<link rel="preload" as="fetch" crossorigin="anonymous">` for each artifact
the plan names, and the plan as
`<script type="application/json" id="calque-plan">`. The browser starts
downloading the working set while it parses the head, and the client's own
download of the same URL is handed that request. The inline starter and
`startBootstrap` both take the plan from that element in place of sending a
request:

```ts
// The backend, for each page it renders:
const head = await renderBootstrap(
  { endpoint, bearerToken, requiredTables, memoryBudgetBytes },
  { acceptEncoding: request.headers["accept-encoding"] },
);
```

A backend has no page to read a path against, so `renderBootstrap` takes the
endpoint as an absolute URL: for a connection that names `/_calque/`, it
passes that path on the page's origin.

The backend must pass the endpoint, bearer token and tables the page creates
its client with, and the budget and `deviceId` if the client names them; a
client created without `memoryBudgetBytes` takes the budget and heap reserve in
`calque-plan`, and one created without `deviceId` takes the plan's device,
which `renderBootstrap` creates when the backend names none. If any differs,
the client refuses the plan, sends its own request, and the browser downloads
the working set twice. It must also pass the `Accept-Encoding` of
the browser's request for the page: the server picks the artifact's coding
from that header, and the browser, not the backend, downloads the artifact.
`renderBootstrap` returns the empty string when its request fails, and also
for a plan that names the uncompressed artifact, which a proxy that rewrote
the header on its way to the backend would cause; the page then sends its
own request. The HTML now also carries the plan's artifact URLs, which are
capabilities, and stays `Cache-Control: no-store`. The backend's request
delays the document's first byte by its own round trip to the Calque server,
so it saves time only when the backend is close to that server.

## Bundling

Bundle the page; every page in this repository uses Vite. The client starts
its workers as `new Worker(new URL(…, import.meta.url), { type: "module" })`,
the form a bundler recognises and emits as a worker chunk of its own. Served
unbundled, the worker's bare `@calque/worker` import does not resolve and the
worker fails to load.

With Vite, add the plugin from `@calque/client/vite`:

```ts
import { calque } from "@calque/client/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [calque({ bootstrap: { requiredTables, memoryBudgetBytes } })],
});
```

With no options, `calque()` builds the engine worker as an ES module, so its
dynamic imports stay separate chunks rather than putting the change-stream
decoder and the protobuf runtime on the cold-load path of every mount. It
starts the worker's entry chunk with V8's compile hint,
`//# allFunctionsCalledOnLoad`, so Chrome compiles the query engine while the
working set downloads instead of at the first query. `bootstrap` writes the
inline starter and its `calque-bootstrap` element into the page's head. It also
loads a module that calls `startBootstrap` with the same values ahead of the
page's own entry, from a chunk that holds it and the starter's modules, which
the entry chunk imports first. A client created without `memoryBudgetBytes`
takes the element's budget and heap reserve, so the page names its tables and
budget once, in its Vite config.

The plugin also keeps the typed bindings current. It renders
`calque.config.json` into `calque.generated.ts` and `calque.start.generated.ts`
beside it when `vite` starts, whenever the configuration changes, and before
`vite build`, and writes a file only when its render differs from it, so an
unchanged render reloads nothing. `bindings: { config, out }` names other
files, relative to Vite's root, and `bindings: false` turns this off. Until
`calque.config.json` exists, it renders nothing. A configuration that cannot
be rendered fails `vite build`; under `vite` it is logged and shown in Vite's
error overlay, and the last bindings stay until it renders again. The renderer
is `@calque/config`, an optional peer dependency that an application installs
as a dev dependency; without it, the plugin says so once and renders nothing.
Outside Vite, run `calque-bindings`, and in CI `calque-bindings --check`.

While Vite serves the page, with `vite` or `vite preview`, the plugin looks
for a `calque dev`: the one `CALQUE_LOCAL_SERVICE_ENDPOINT` and
`CALQUE_LOCAL_BEARER_TOKEN` name, or else the one whose `client.env`, which
`calque dev` writes into its state directory, is in the nearest `.calque-dev`
at or above Vite's root. It refuses an endpoint that is not
`http://127.0.0.1:<port>`, a malformed bearer, and one of the two variables
without the other. With one found, it proxies `/_calque/` to it and renders a
`calque-connection` naming `/_calque/` and a placeholder bearer into each
page. On `/v1/bootstrap`, `/v1/changes` and `/v1/mutations`, the proxy swaps
the placeholder for `calque dev`'s bearer and drops any other, so the page
never holds the service's bearer. Without one, Vite logs a warning when it
starts.

`vite` is an optional peer dependency too: only `@calque/client/vite` imports
it, its types are Vite's, and its options are Vite 8's rolldown options. A
page built with another bundler does not install it, and sets that bundler's
equivalents of the worker format and the start chunk itself.
