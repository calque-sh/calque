# Incident operations

This page synchronizes seven PostgreSQL tables of the 1M-row
incident-operations fixture (schema `incidents`) from the `calque` binary into
the browser, then answers every view with a local query over the complete
working set:

- an events grid over all 800,001 events, virtualized through `window`
  over each view's whole result, with a level filter and a timestamp sort;
- a chart of every issue by status and priority, drawn as SVG from
  `groupBy("status", "priority").count()`;
- open issues joined to their project and assignee, and the selected issue's
  comments joined to their authors, its tags and its latest events;
- comment insert, edit and delete, and issue status and priority changes,
  through the generated facades. Each write is marked pending until its CDC
  echo arrives, and failed if it ends any other way.

The page is also the benchmark's subject. `pnpm build:harness` builds
it with `src/harness.ts` as its entry, which runs the page's own entry, reads
what the page shows from its DOM and the client's public listeners, and
publishes that on `globalThis.__incidents` for the benchmark and the end-to-end
test. `pnpm build` leaves it out. Each table's `bench_index` and `is_sentinel`
columns exist for the benchmark; an application needs neither.

## Running it

```sh
pnpm incidents
```

It starts PostgreSQL 18 in its own compose project (`<checkout>-incidents`),
seeds the fixture unless that exact dataset is already there, builds and
starts a release `calque dev` over the seven tables, builds the page with its
harness entry, serves it with `vite preview` and prints the URL. A second run
reuses the seed and calque's prepared state; `pnpm incidents -- --fresh` prepares again from
PostgreSQL. `pnpm incidents -- --dataset content-v1` seeds
`dataset.incident-ops-1m.content-v1` instead: v1's rows with generated words,
hashed version 4 UUIDs and append-ordered times
(`scripts/bench-content-fixture.ts`); switching datasets reseeds.
`pnpm incidents -- --rows 100k` seeds the benchmark's cut instead of the whole
fixture: each table's first tenth, 100,007 rows with the sentinels, of v1 or,
with `--dataset content-v1`, of the content variant.
`CALQUE_INCIDENTS_POSTGRES_PORT`, `CALQUE_INCIDENTS_PORT` and
`CALQUE_INCIDENTS_UI_PORT` choose the ports.

`pnpm incidents:e2e` drives two headless Chromium pages at 1M rows and checks
the grid, the chart, the joins and every write against PostgreSQL; see
`scripts/incidents-e2e.ts`. It takes the same `--dataset`.

## How the page reads

`src/main.ts` creates the client (`src/client.ts`) and only then imports the
React page (`src/page.tsx`), so the working set downloads while React and the
grid load. `vite.config.ts` adds `calque()` from `@calque/client/vite`, with
the page's tables and memory budget. The plugin puts the client's inline
starter and those values in the page's head, and loads a start module of its
own from a chunk ahead of the page's entry. The starter sends the bootstrap
request as soon as the document is parsed; the start module sends it if the
starter did not, before the client library evaluates; and the client, which
takes its memory budget from the page's head, adopts it. Both starters read
the endpoint and the bearer from the `calque-connection` element the server
rendered into the page. Under `vite` and `vite preview`,
the plugin renders one naming `/_calque/`, which it proxies to `calque dev`,
swapping the placeholder bearer for the one in calque's private `client.env`.
`src/client.ts` creates the client from the same element.

Every panel reads through `useLiveQuery` and `useLiveWindow` from
`@calque/react` and shows its loading state until its query answers. The grid
reads a live `window` over its view's whole result: the 64-row block in view
and one block on each side. A scroll reaches any row of a sorted or filtered
view, and the grid keeps the rows it has while the next window loads.

The grid is `@glideapps/glide-data-grid` 6.0.3. `lodash`, `marked` and
`react-responsive-carousel` are in `package.json` only because glide declares
them as peer dependencies; the page imports none of them. Glide declares React
16 to 18 as its peer, and the page runs it on React 19, which the root
`pnpm-workspace.yaml` accepts in `peerDependencyRules`.

## Where the bindings come from

`calque.inspection.json` is `calque inspect` of the seeded tables, and
`calque.config.json` is `calque init` of that inspection, with commands kept
for `comments` and `issues` only. The Vite plugin renders the configuration
into `src/calque.generated.ts`, and into `src/calque.start.generated.ts`, the
list of source relations `vite.config.ts` passes to the plugin, when `vite`
starts, whenever the configuration changes, and before `vite build`. Vite
reads that list when it loads `vite.config.ts`: the dev server restarts when
the plugin rewrites it, but a build that rewrites it still passes the previous
list, so build again after changing the configuration. `pnpm verify` fails if
the render drifts.
