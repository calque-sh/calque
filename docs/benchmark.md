# The benchmark

`pnpm bench` starts its own PostgreSQL 18 container, seeds 100k rows of
content-v1, a cut of the incident-operations fixture whose text columns hold drawn words
rather than padding, and has a release `calque dev` prepare its seven tables.
Chrome for Testing then loads the incident-operations page under Chrome's
emulation of a 100 Mbit/s link with 30 ms of latency, reaching `calque dev`
through Caddy over TLS and HTTP/2, the edge the self-hosted composition uses.
`calque serve`'s grant verification and S3 store are not in the measured path.
Each cold sample is a new browser process with an empty cache, and ends at the
first presented frame that matches the reference picture of the complete page
and stays uncontradicted for 2 seconds. A realtime session then times each
probe from its PostgreSQL commit to the stable paint that shows it, under a
background write load. Every sample's working set is compared with PostgreSQL
row by row, and a run with a sample that differs fails.

A cold load is read against the time its own bytes need on the link. In the
published run, each of 30 cold loads moved a median 7.7 MB, which takes 618 ms
at 100 Mbit/s, and reached a complete stable paint in a median 747 ms. The
summary's `above_wire_ms` is that difference for each sample: a median 129 ms
and a p95 of 142 ms, spent on request round trips, digest checks, decoding,
indexing and painting. The run was directional, and Chrome's emulation
serializes decode with the wire, so the emulated figure errs pessimistic.

## Running it

Fetch the pinned browser once, then run it:

```sh
pnpm bench:fetch-browser
pnpm bench
```

The run builds `calque` in release mode unless `--calque <path>` names a
binary, and PostgreSQL listens on 127.0.0.1 at `CALQUE_BENCH_POSTGRES_PORT`,
55521 by default. Each run writes `target/bench/<run id>/`: one JSON
record per sample under `samples/`, the run's `summary.json`, calque's logs,
and under `frames/` the pictures of any cold sample whose verdict is `DNC`:
one that did not reach a complete stable paint. The realtime record's
`calqueCpu` is the CPU time `ps` charged calque from the writer's start until
the page stopped publishing.

`pnpm bench -- --smoke` runs every step with fewer samples and judges nothing;
its run id ends in `-smoke`.

The host is read before and after every sample. A run during which another
container was running, or the one-minute load average went above 4, is
labeled directional in its summary, with the reasons.

## What a run checks besides its samples

Every session drives Chrome for Testing 154.0.8037.57. The run checks the
browser bundle's digest before it starts, and a session refuses a browser that
reports another version. The run fetches Caddy 2.11.4 into a shared cache and
checks it against its pins before it starts the edge.

After the realtime probes, the run times uncached fetches from the page and
fails unless the fastest took at least the emulated latency, so a run whose
emulation did not apply cannot pass.

Chrome stamps frames on its wall clock, which can drift against its monotonic
clock during a session. Each probe reads the drift at its start and maps its
frames through it, and is invalid only when the clock moves more than 5 ms
during the probe itself; such a probe is taken again, up to three times a
session. The realtime record's `series` samples calque's re-cut counter and
gauge and the log's entries and bytes after the newest cut once a second.

A run that loses its browser or page records why it may have: the jetsam
exits in macOS's unified log since 30 s before the loss, the host's free
memory, and, when the run was started with `DEBUG=pw:browser` and
`DEBUG_FILE=<path>`, the last lines Playwright's browser logger wrote, which
carry Chrome's exit code and signal. `summary.json` keeps them as
`browserExit`, whose cause reads `host-memory` when a jetsam wave or under 15%
free memory explains the loss, and `unknown` otherwise.

Before the reference pass the run waits up to 300 s for calque to log
`brotli-large-window artifact published`, the end of the line for its packed
and for its plain brotli artifact, and stops if the line does not come. It then
asks calque for one plan of its own and records the artifact it selects as the
summary's `calqueArtifact`. A run whose cold samples did not all ask for that
artifact and no other is not judged.

## What a run reports besides its thresholds

Four readings sit in `summary.json` beside the threshold table and enter no
verdict.

- `post_load_query_ms`: after each judged cold sample has reached its complete
  stable paint, the runner sets the events grid's Level control to `error` and
  its Order control to newest first, and times the first presented frame that
  shows the result and stays uncontradicted for 2 seconds, from the first
  `input` event's own timestamp. Frames are compared inside the events panel,
  12 px in from its edge, because the panel's border antialiases differently
  from one repaint to the next. The grid's
  controls, total and visible ids are then compared with PostgreSQL's, and a
  difference fails the run. The distribution is p50, p95, p99 and the slowest
  over the judged cold samples, with failures ranking above every number.
- `bytes.byPurpose`: each passed cold sample's encoded bytes on the wire,
  headers included, split into the bootstrap request, the artifact, the change
  requests and everything else. The artifact's request ends as canceled once
  the page has read it, and counts at its declared length. `bytes.wire` is the
  ledger's lower bound and reads well below this total. A page with its plan rendered into the document
  asks no bootstrap request.
- `above_wire_ms`: each cold sample's complete stable paint minus the time its
  `bytes.byPurpose.total` need on the run's link, at 8 bits a byte over the
  Mbit/s of the emulation or the kernel shaper. What remains is the request
  round trips, the digest checks, the decode, the index build and the paint.
  The distribution is p50, p95, p99 and the slowest over the cold samples, with
  failures ranking above every number. It is `null` under `--network off`,
  which sets no link rate. It enters no verdict.
- `empty_page_peak_rss_bytes`: peak browser process-tree RSS of five sessions
  that load `about:blank` in the same Chrome with the same flags, edge
  arguments and network profile, for 5 s each, to read beside
  `browser_process_tree_peak_rss_bytes`.

## Options

`--dataset` and `--network` set the conditions, and every record under
`samples/` names its dataset and network profile in `conditions`.

| Option | Effect |
| --- | --- |
| `--dataset <content-v1\|v1>` | Cut the rows from content-v1, the default, or from v1, whose text columns hold padding. |
| `--network <Mbit/s>/<latency ms>` | Chrome's own network emulation under every page, 100/30 by default. |
| `--network off` | No emulation. |
| `--network shaped:<Mbit/s>/<RTT ms>` | Shape loopback in the kernel through `calque-netshape` instead, and emulate nothing in Chrome. The page server then renders no bootstrap plan into the page, since its own request to calque would cross the shaped link; `renderedPlan` in `conditions` and in the summary's `plan` records which it did. |
| `--no-edge` | Reach the page and calque over plain HTTP/1.1, without Caddy. |
| `--diagnostic` | Traced samples also record `blink.user_timing`, which carries the engine worker's marks, and V8's sampling profiler, and save their raw trace as `samples/traced-<n>.trace.json`; the state's `objects/` is copied into the run's directory after the last preparation. |
| `--realtime-only` | One preparation and the realtime session, without cold samples. |
| `--background-clients <n>` | With `--realtime-only`: once the page has painted, n clients each bootstrap a plan and follow the change stream while the load and the probes run, on calque's default bounds. The session's record keeps their report. On macOS the session is void, and the clients stop, if the host denies a request for network memory or has half its mbuf pool in use. |
| `--background-qos utility` | With `--background-clients`: the clients' processes run under `taskpolicy -c utility` (macOS), below calque's, Chrome's and the edge's scheduling priority, with their sockets as they were. The session's record keeps the clamp and each process's priority as `ps` read it once the clients were ready. |
| `--load-rate <tx/s>` | With `--realtime-only`: the writer commits that many transactions a second instead of 220. |
| `--tail <transactions>` | Commit that many transactions after the artifact is cut and before the reference pass; see below. |
| `--recut-tail-ratio <ratio>` | Start calque with that `--recut-tail-ratio`; 0 turns re-cutting off. |
| `--recut-min-interval <seconds>` | Start calque with that `--recut-min-interval`. |
| `--hold-until-logged <text>` | Hold the reference pass and every sample after it until calque logs a line containing `text`, or for 300 s, in place of the wait for the artifact's publication. The run goes on whether or not the line came, and the summary's `hold` records how long it held. |
| `--postgres-bin <dir>` | Run PostgreSQL 18 from the binaries in `dir`, in a temporary cluster with the container's superuser, database, flags and `docker/postgres-init`, for a host whose Docker does not answer. |

A run under `--diagnostic`, `--network shaped:…`, `--realtime-only`,
`--postgres-bin`, `--no-edge`, `--background-clients`, `--background-qos`,
`--load-rate`, `--tail`, `--recut-tail-ratio` or `--recut-min-interval` is
never judged: its `passes` is false whatever its rows say, its summary is
labeled directional with the reasons, and its run id carries a suffix for
each (`-diagnostic`, `-shaped`, `-realtime`, `-native-postgres`, `-no-edge`,
`-background-<n>`, `-utility`, `-load-<tx/s>`, `-tail-<transactions>`,
`-recut-ratio-<ratio>`, `-recut-interval-<seconds>`).

## Replaying a long tail

A cold load replays every transaction committed since the newest artifact was
cut. `--tail <transactions>` commits that many of the load's transactions
after the artifact is cut; such a run is never judged. The run then waits
until calque has stopped re-cutting: no re-cut or encode in progress, a tail
below calque's threshold, and the same re-cut count and tail on two readings
2 s apart. Its summary's `recut` records the re-cuts, the tail left, and for
each re-cut the wall time, calque's CPU and peak RSS, and the state
directory's size from just before it started until its encodes finished. If
calque re-cut, the run asks for a plan again and judges its cold samples
against the newest artifact; otherwise every page catches up through the
whole tail before it is ready. Calque re-cuts under `--recut-tail-ratio`
above 0, 1/32 by default; the arms below that replay the whole tail pass
`--recut-tail-ratio 0`.

With 10,000 transactions to replay, about 45 s of the benchmark's write load
of 220 transactions a second, twelve cold samples from four runs
of `pnpm bench -- --smoke --tail 10000 --recut-tail-ratio 0` reached a
complete stable paint in a median 1,751.1 ms, against 747.0 ms for
`pnpm bench -- --smoke --recut-tail-ratio 0`. With
`--recut-tail-ratio 0.03125 --recut-min-interval 60` calque re-cut once after
the tail, and the median was 750.6 ms. The numbers are directional. A second
set of directional runs repeated the same arms with a re-cut's build and
encodes in the background class: 1,776.5 ms with the tail, 760.7 ms with none, and 755.1 ms when calque re-cut once or twice
after the tail.
Before the catch-up's responses were compressed with zstd, the same tail
took a median 3,459.9 ms, also directional.
