import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import { EventEmitter } from "node:events";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { crc32, deflateSync } from "node:zlib";

import { instrumentFault } from "./bench-browser.ts";
import { holdUntilLogged } from "./bench-calque.ts";
import { NetworkLedger } from "./bench-capture.ts";
import {
  benchPlan,
  loadTransactionsPerSecond,
  parseBenchArguments,
  parseClientCount,
  parseLoadRate,
  parseTail,
  parseRecutMinInterval,
  parseRecutTailRatio,
  recutArguments,
  unjudgedReasons,
} from "./bench-conditions.ts";
import { hrtimeMs } from "./bench-host.ts";
import {
  fitWindow,
  measureWindowInset,
  relaunchOnRefusal,
  screenshotCalibration,
  withDeadline,
} from "./bench-launch.ts";
import {
  artifactRetryMarks,
  changeTraffic,
  engagementLine,
} from "./bench-page.ts";
import { bootstrapElement } from "./bench-page-server.ts";
import { awaitQuietPublications, backgroundProblem } from "./bench-realtime.ts";
import {
  checkoutPath,
  redactedUrl,
  scrubRunLogs,
  writeRecord,
  writeRunSummary,
} from "./bench-record.ts";
import {
  coldInvalidity,
  reachedMs,
  probeInvalidity,
  takeSamples,
  type Taken,
  type TakenSample,
} from "./bench-samples.ts";
import { RealtimeMonitor, RunnerStalls } from "./bench-sampling.ts";
import { runStop } from "./bench-stop.ts";
import { recutWindows } from "./bench-tail.ts";
import {
  decodePng,
  FRAME_TRANSIT_LIMIT_MS,
  frameClockCheck,
  FrameHasher,
  FULL_VIEWPORT_ROI,
  hashDecodedRoi,
  hashFrame,
  judgeStablePaint,
  mapEpochSecondsToMonotonicMilliseconds,
  roiFromRect,
  settle,
  type HashedFrame,
} from "./bench-frames.ts";
import {
  benchCountProblems,
  benchLoadDdl,
  BENCH_ROW_COUNTS,
  benchSeedSql,
  canonicalPageCell,
  cellArray,
  isoTimestampMicros,
  jsonbText,
  LOAD_ROW_CHANGES_PER_TRANSACTION,
  LOAD_TRANSACTIONS_PER_SECOND,
  loadTransactionSql,
  normalizeBuckets,
  oracleTables,
  parseBuckets,
  parseReference,
  probeBenchIndex,
  probeInsertSql,
  referenceSql,
  SEED_ORDER,
  TableComparison,
  tableDigest,
  type OracleTable,
  type OraclePage,
  pageOracle,
} from "./bench-oracle.ts";
import { parseRssTable, selectProcessTree } from "./bench-processes.ts";
import {
  achievedTransactionsPerSecond,
  artifactObjectLine,
  artifactObjectReasons,
  DescendantCpu,
  directionalReasons,
  distribution,
  edgeProtocolLine,
  judgeArtifactObject,
  judgeEdgeProtocol,
  judgePageCoding,
  nearestRank,
  nearestRankWithFailures,
  pageCodingLine,
  parseCgroupCpuSeconds,
  parseCpuTable,
  parseCpuTime,
  parseMemoryFreePercent,
  parseMetric,
  parseOtherContainers,
  parsePowerSource,
  parseSlotRows,
  rankedValue,
  summarizeTrace,
  runPasses,
  secondReading,
  thresholdRows,
  THRESHOLDS,
  type EnvironmentReading,
  type Measurements,
} from "./bench-results.ts";
import { fixtureDdl, sentinelSql } from "./bench-fixture.ts";
import { localPathsIn } from "./verify-no-local-paths.ts";

const root = resolve(import.meta.dirname, "..");
const md5 = (text: string) => createHash("md5").update(text).digest("hex");
const A = "a".repeat(64);
const B = "b".repeat(64);
const C = "c".repeat(64);

function frame(
  seq: number,
  timestampMs: number,
  full: string,
  grid: string | null = full,
): HashedFrame {
  return {
    seq,
    sessionId: 1,
    epochSeconds: timestampMs / 1_000,
    timestampMs,
    receivedMs: timestampMs,
    pngSha256: full,
    full,
    grid,
  };
}

// Arguments and plan ------------------------------------------------------------

/** The benchmark's defaults, which every argument list below keeps. */
const PRODUCTION = {
  dataset: "content-v1",
  network: { megabitsPerSecond: 100, latencyMs: 30 },
} as const;

test("bench arguments take --smoke, --calque, --diagnostic, --hold-until-logged, --realtime-only and --postgres-bin once each", () => {
  assert.deepEqual(parseBenchArguments([]), {
    smoke: false,
    calque: null,
    ...PRODUCTION,
    diagnostic: false,
    holdUntilLogged: null,
    realtimeOnly: false,
    postgresBin: null,
    edge: true,
  });
  assert.deepEqual(
    parseBenchArguments(["--", "--smoke", "--calque", "bin/calque"]),
    {
      smoke: true,
      calque: resolve("bin/calque"),
      ...PRODUCTION,
      diagnostic: false,
      holdUntilLogged: null,
      realtimeOnly: false,
      postgresBin: null,
      edge: true,
    },
  );
  assert.deepEqual(parseBenchArguments(["--", "--smoke", "--diagnostic"]), {
    smoke: true,
    calque: null,
    ...PRODUCTION,
    diagnostic: true,
    holdUntilLogged: null,
    realtimeOnly: false,
    postgresBin: null,
    edge: true,
  });
  assert.deepEqual(parseBenchArguments(["--", "--realtime-only"]), {
    smoke: false,
    calque: null,
    ...PRODUCTION,
    diagnostic: false,
    holdUntilLogged: null,
    realtimeOnly: true,
    postgresBin: null,
    edge: true,
  });
  assert.equal(
    parseBenchArguments(["--hold-until-logged", "artifact published"])
      .holdUntilLogged,
    "artifact published",
  );
  assert.equal(
    parseBenchArguments(["--", "--postgres-bin", "pg/bin"]).postgresBin,
    resolve("pg/bin"),
  );
  for (const bad of [
    ["--calque"],
    ["--calque", "--smoke"],
    ["--smoke", "--smoke"],
    ["--diagnostic", "--diagnostic"],
    ["--realtime-only", "--realtime-only"],
    ["--hold-until-logged"],
    ["--hold-until-logged", " "],
    ["--hold-until-logged", "a", "--hold-until-logged", "b"],
    ["--postgres-bin"],
    ["--postgres-bin", "--smoke"],
    ["--postgres-bin", "a", "--postgres-bin", "b"],
    ["--bogus"],
    ["--keep-postgres"],
    ["--", "--"],
    ["--browser", "bundled"],
    ["--compare-browser", "bundled"],
    ["--diagnose", "1m", "--samples", "40"],
    ["--tier", "100k"],
  ])
    assert.throws(() => parseBenchArguments(bad), /usage: pnpm bench/u);
});

test("--background-clients follows a realtime-only run", () => {
  const options = parseBenchArguments([
    "--realtime-only",
    "--background-clients",
    "1000",
  ]);
  assert.equal(options.backgroundClients, 1000);
  const plain = parseBenchArguments(["--realtime-only"]);
  assert.equal("backgroundClients" in plain, false);
  assert.throws(
    () => parseBenchArguments(["--background-clients", "10"]),
    /needs --realtime-only/u,
  );
  assert.throws(
    () =>
      parseBenchArguments([
        "--realtime-only",
        "--background-clients",
        "10",
        "--background-clients",
        "20",
      ]),
    /usage: pnpm bench/u,
  );
  assert.equal(parseClientCount("1"), 1);
  assert.equal(parseClientCount("100000"), 100_000);
  for (const bad of ["0", "100001", "-5", "2.5", "1e3", "ten", ""])
    assert.throws(() => parseClientCount(bad), /whole number/u, bad);
});

test("--load-rate sets a realtime-only run's writer and leaves the run unjudged", () => {
  const options = parseBenchArguments(["--realtime-only", "--load-rate", "10"]);
  assert.equal(options.loadRate, 10);
  assert.equal(loadTransactionsPerSecond(options), 10);
  assert.ok(
    unjudgedReasons(options).includes(
      `load of 10 transactions/s, not ${String(LOAD_TRANSACTIONS_PER_SECOND)}`,
    ),
  );
  // Absent, the writer keeps `LOAD_TRANSACTIONS_PER_SECOND`.
  const plain = parseBenchArguments(["--realtime-only"]);
  assert.equal("loadRate" in plain, false);
  assert.equal(loadTransactionsPerSecond(plain), LOAD_TRANSACTIONS_PER_SECOND);
  assert.throws(
    () => parseBenchArguments(["--load-rate", "10"]),
    /needs --realtime-only/u,
  );
  assert.throws(
    () =>
      parseBenchArguments([
        "--realtime-only",
        "--load-rate",
        "10",
        "--load-rate",
        "20",
      ]),
    /usage: pnpm bench/u,
  );
  assert.equal(parseLoadRate("1"), 1);
  assert.equal(parseLoadRate("1000"), 1_000);
  for (const bad of ["0", "1001", "-5", "2.5", "1e2", "ten", ""])
    assert.throws(() => parseLoadRate(bad), /whole number/u, bad);
});

test("--tail writes before the reference pass and leaves the run unjudged", () => {
  const options = parseBenchArguments(["--tail", "100000"]);
  assert.equal(options.tail, 100_000);
  assert.ok(
    unjudgedReasons(options).includes(
      "100000 transactions written before the reference pass",
    ),
  );
  assert.equal("tail" in parseBenchArguments([]), false);
  // A realtime-only run takes no cold sample.
  assert.throws(
    () => parseBenchArguments(["--realtime-only", "--tail", "10"]),
    /--tail is caught up by the cold samples, so it takes no --realtime-only/u,
  );
  assert.throws(
    () => parseBenchArguments(["--tail", "1", "--tail", "2"]),
    /usage: pnpm bench/u,
  );
  assert.equal(parseTail("1"), 1);
  assert.equal(parseTail("5000000"), 5_000_000);
  for (const bad of ["0", "5000001", "-5", "2.5", "1e3", "ten", ""])
    assert.throws(() => parseTail(bad), /whole number/u, bad);
});

test("--recut-tail-ratio and --recut-min-interval reach calque dev and leave the run unjudged", () => {
  const options = parseBenchArguments([
    "--recut-tail-ratio",
    "0.03125",
    "--recut-min-interval",
    "0",
  ]);
  assert.deepEqual(recutArguments(options), [
    "--recut-tail-ratio",
    "0.03125",
    "--recut-min-interval",
    "0",
  ]);
  assert.ok(
    unjudgedReasons(options).includes("re-cut at a tail ratio of 0.03125"),
  );
  assert.ok(unjudgedReasons(options).includes("re-cut at most every 0 s"));
  assert.deepEqual(recutArguments(parseBenchArguments([])), []);
  assert.equal(parseRecutTailRatio("0"), 0);
  assert.equal(parseRecutTailRatio("1000"), 1_000);
  for (const bad of ["-1", "1000.5", "1e-6", "ratio", ""])
    assert.throws(() => parseRecutTailRatio(bad), /decimal/u, bad);
  assert.equal(parseRecutMinInterval("86400"), 86_400);
  for (const bad of ["86401", "1.5", "-1", ""])
    assert.throws(() => parseRecutMinInterval(bad), /whole seconds/u, bad);
});

test("a re-cut's window runs from the sample before its gauge rose to the one after it fell", () => {
  const sample = (
    atMs: number,
    inProgress: number,
    rssBytes: number,
    cpuSeconds: number,
    stateBytes: number | null,
  ) => ({
    atMs,
    reading: {
      recuts: 0,
      inProgress,
      tailEntries: 0,
      tailBytes: 0,
      thresholdBytes: 0,
    },
    rssBytes,
    cpuSeconds,
    stateBytes,
  });
  assert.deepEqual(
    recutWindows([
      sample(0, 0, 100, 1, 10),
      sample(250, 1, 300, 2, null),
      sample(500, 1, 500, 4, 40),
      sample(750, 0, 200, 5, null),
      sample(1_000, 0, 150, 5, 12),
      sample(1_250, 1, 400, 6, 20),
    ]),
    [
      {
        wallMs: 750,
        cpuSeconds: 4,
        rssBeforeBytes: 100,
        peakRssBytes: 500,
        stateBeforeBytes: 10,
        peakStateBytes: 40,
        stateAfterBytes: 12,
      },
    ],
  );
});

test("--background-qos places the background clients", () => {
  const options = parseBenchArguments([
    "--realtime-only",
    "--background-clients",
    "10",
    "--background-qos",
    "utility",
  ]);
  assert.equal(options.backgroundQos, "utility");
  assert.ok(
    unjudgedReasons(options).includes("background clients at utility QoS"),
  );
  assert.throws(
    () =>
      parseBenchArguments(["--realtime-only", "--background-qos", "utility"]),
    /needs --background-clients/u,
  );
  for (const clamp of ["background", "maintenance", ""])
    assert.throws(
      () =>
        parseBenchArguments([
          "--realtime-only",
          "--background-clients",
          "10",
          "--background-qos",
          clamp,
        ]),
      /usage: pnpm bench/u,
      clamp,
    );
});

test("background clients count only when every one got a plan, a batch and the last cursor", () => {
  const report = {
    clients: 4,
    processes: 1,
    bootstrapped: 4,
    bootstrapFailures: {},
    bootstrapMs: 10,
    bootstrapLatencyMs: { p50: 1, p95: 2, max: 3 },
    allLiveMs: 20,
    requests: 40,
    refusals: {},
    transportErrors: 0,
    firstTransportError: null,
    batches: 40,
    bytes: 4_000,
    settled: true,
    settleMs: 5,
    distinctFinalCursors: 1,
    observers: {
      count: 4,
      transactions: 10,
      commitToArrivalMs: { p50: 1, p95: 2, p99: 3, max: 4 },
      spreadMs: { p50: 0, p95: 1, max: 1 },
    },
    loadClient: {
      cpuSeconds: 1,
      wallSeconds: 10,
      eventLoopDelayMs: { p50: 10, p99: 12, max: 20 },
    },
  };
  assert.equal(backgroundProblem(4, report), null);
  assert.match(
    backgroundProblem(4, { ...report, bootstrapped: 3 }) ?? "",
    /1 of 4 background clients got no plan/u,
  );
  assert.match(
    backgroundProblem(4, { ...report, allLiveMs: null }) ?? "",
    /received a batch/u,
  );
  assert.match(
    backgroundProblem(4, {
      ...report,
      settled: false,
      distinctFinalCursors: 2,
    }) ?? "",
    /ended on 2 cursors/u,
  );
});

test("the page server reads the client's parameters from the build's calque-bootstrap element", () => {
  const element = (json: string) =>
    `<head><script type="application/json" id="calque-bootstrap">${json}</script></head>`;
  assert.deepEqual(
    bootstrapElement(
      element('{"requiredTables":["s.a","s.b"],"memoryBudgetBytes":64}'),
    ),
    { requiredTables: ["s.a", "s.b"], memoryBudgetBytes: 64 },
  );
  assert.deepEqual(
    bootstrapElement(
      element(
        '{"requiredTables":["s.\\u003ca"],"memoryBudgetBytes":64,"clientHeapReserveBytes":8}',
      ),
    ),
    {
      requiredTables: ["s.<a"],
      memoryBudgetBytes: 64,
      clientHeapReserveBytes: 8,
    },
  );
  for (const html of [
    "<head></head>",
    element('{"requiredTables":[],"memoryBudgetBytes":64}').repeat(2),
    element('{"requiredTables":[1],"memoryBudgetBytes":64}'),
    element('{"requiredTables":["s.a"]}'),
    element(
      '{"requiredTables":["s.a"],"memoryBudgetBytes":64,"clientHeapReserveBytes":"8"}',
    ),
    element("null"),
  ])
    assert.throws(
      () => bootstrapElement(html),
      /no well-formed calque-bootstrap element/u,
      html,
    );
  assert.throws(() => bootstrapElement(element("{")), SyntaxError);
});

test("a hold ends at the logged line, or when the server exits", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bench-hold-"));
  try {
    const logPath = join(directory, "prepare-2.log");
    writeFileSync(logPath, "INFO calque::preparation: ready\n");
    const never = new Promise<never>(() => {});
    const held = holdUntilLogged(logPath, never, "artifact published");
    await sleep(250);
    writeFileSync(
      logPath,
      "INFO calque::preparation: re-encoded artifact published bytes=7\n",
      { flag: "a" },
    );
    const hold = await held;
    assert.equal(hold.logged, true);
    assert.equal(hold.text, "artifact published");
    assert.ok(hold.heldMs >= 200, String(hold.heldMs));

    const exited = await holdUntilLogged(
      logPath,
      Promise.resolve({ code: 1, signal: null }),
      "never logged",
    );
    assert.equal(exited.logged, false);
    assert.ok(exited.heldMs < 1_000, String(exited.heldMs));

    const timedOut = await holdUntilLogged(logPath, never, "never logged", 150);
    assert.equal(timedOut.logged, false);
    assert.ok(timedOut.heldMs >= 150, String(timedOut.heldMs));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("the plan is the benchmark's sample counts in full, fewer in a smoke", () => {
  assert.deepEqual(benchPlan(false), {
    preparations: 3,
    warmups: 2,
    cold: 30,
    traced: 3,
    probeWarmups: 3,
    probes: 30,
    emptyPages: 5,
  });
  assert.deepEqual(benchPlan(true), {
    preparations: 1,
    warmups: 1,
    cold: 3,
    traced: 1,
    probeWarmups: 1,
    probes: 5,
    emptyPages: 1,
  });
  // A realtime-only run prepares once and keeps the probes.
  assert.deepEqual(benchPlan(false, true), {
    preparations: 1,
    warmups: 0,
    cold: 0,
    traced: 0,
    probeWarmups: 3,
    probes: 30,
    emptyPages: 0,
  });
  assert.deepEqual(benchPlan(true, true), {
    ...benchPlan(true),
    warmups: 0,
    cold: 0,
    traced: 0,
    emptyPages: 0,
  });
});

// The cut and its seed ---------------------------------------------------------

test("the cut is a tenth of the fixture, plus one sentinel per table", () => {
  assert.deepEqual(
    { ...BENCH_ROW_COUNTS },
    {
      users: 800,
      projects: 100,
      issues: 10_000,
      comments: 6_000,
      issue_tags: 2_000,
      memberships: 1_100,
      events: 80_000,
    },
  );
  assert.equal(
    SEED_ORDER.reduce((total, name) => total + BENCH_ROW_COUNTS[name] + 1, 0),
    100_007,
  );
});

test("the seed covers each table's prefix exactly once, in seed order", () => {
  const sql = benchSeedSql();
  assert.ok(sql.startsWith(fixtureDdl));
  assert.ok(sql.includes(sentinelSql));
  assert.ok(sql.endsWith(benchLoadDdl));
  const covered = new Map<string, number>();
  const order: string[] = [];
  const batch =
    /INSERT INTO incidents\.(\w+) \([^)]*\)\nSELECT [\s\S]*?\nFROM generate_series\((\d+)::bigint, (\d+)::bigint\) AS g\(bench_index\);/gu;
  for (const match of sql.matchAll(batch)) {
    const [, table = "", first = "", last = ""] = match;
    assert.equal(Number(first), covered.get(table) ?? 0, `${table} has a gap`);
    covered.set(table, Number(last) + 1);
    if (order.at(-1) !== table) order.push(table);
  }
  assert.deepEqual(order, [...SEED_ORDER]);
  assert.deepEqual(Object.fromEntries(covered), { ...BENCH_ROW_COUNTS });
});

test("the seed check names every table whose count is off", () => {
  const exact = new Map(
    SEED_ORDER.map((name) => [name, BENCH_ROW_COUNTS[name] + 1]),
  );
  assert.deepEqual(benchCountProblems(exact), []);
  exact.set("events", 80_000);
  exact.delete("users");
  assert.deepEqual(benchCountProblems(exact), [
    "users holds undefined rows, expected 801",
    "events holds 80000 rows, expected 80001",
  ]);
});

// Probes and load ---------------------------------------------------------------

test("probe events sit past the cut's events and the index after them", () => {
  assert.equal(probeBenchIndex(0), 80_001);
  assert.equal(probeBenchIndex(678), 80_679);
  assert.throws(() => probeBenchIndex(679), /0 to 678/u);
  assert.throws(() => probeBenchIndex(-1), /0 to 678/u);
  assert.throws(() => probeBenchIndex(1.5), /0 to 678/u);
  const sql = probeInsertSql("v1", 3);
  assert.match(sql, /^INSERT INTO incidents\.events \(/u);
  assert.match(sql, /FROM \(SELECT 80004::bigint AS bench_index\) AS g;\n$/u);
});

test("a load transaction changes 50 rows in rotation, 5 of them enrolled", () => {
  assert.equal(LOAD_ROW_CHANGES_PER_TRANSACTION, 50);
  const first = loadTransactionSql(0);
  assert.match(first, /^BEGIN;\n/u);
  assert.match(first, /COMMIT;\n$/u);
  assert.ok(
    first.includes(
      "UPDATE incidents.issues SET title = overlay(title placing 'load-0-' from 1) WHERE bench_index = ANY (ARRAY[0::bigint, 1::bigint, 2::bigint]) AND NOT is_sentinel;",
    ),
  );
  assert.ok(
    first.includes(
      "WHERE bench_index = ANY (ARRAY[0::bigint, 1::bigint]) AND NOT is_sentinel;",
    ),
  );
  const slots = /slot = ANY \(ARRAY\[([^\]]*)\]\)/u.exec(first)?.[1];
  assert.equal(
    slots,
    Array.from({ length: 45 }, (_, index) => index).join(", "),
  );
  // 10,000 issues at 100k: transaction 3,333 wraps to the start.
  assert.ok(
    loadTransactionSql(3_333).includes(
      "ARRAY[9999::bigint, 0::bigint, 1::bigint]",
    ),
  );
  assert.throws(() => loadTransactionSql(-1), /non-negative/u);
});

// The oracle --------------------------------------------------------------------

const PAIR: OracleTable = {
  collection: "pairs",
  schema: "s",
  table: "pairs",
  columns: [
    { name: "id", kind: "int64" },
    { name: "name", kind: "utf8" },
  ],
  primaryKey: [0],
};

test("the oracle's tables are the page's seven collections with their keys", () => {
  const tables = oracleTables(
    readFileSync(
      resolve(root, "client/examples/incidents/calque.config.json"),
      "utf8",
    ),
    readFileSync(
      resolve(root, "client/examples/incidents/calque.inspection.json"),
      "utf8",
    ),
  );
  assert.deepEqual(
    tables.map((table) => table.table).sort(),
    [...SEED_ORDER].sort(),
  );
  for (const table of tables) {
    assert.equal(table.schema, "incidents");
    assert.ok(table.primaryKey.length > 0, table.table);
  }
  const events = tables.find((table) => table.table === "events");
  assert.deepEqual(
    events?.primaryKey.map((index) => events.columns[index]?.name),
    ["id"],
  );
});

test("oracleTables refuses a relation the inspection lacks or a type it cannot read", () => {
  const config = JSON.stringify({
    collections: [{ id: "pairs", relation: { schema: "s", table: "pairs" } }],
  });
  assert.throws(
    () => oracleTables(config, JSON.stringify({ relations: [] })),
    /no relation s\.pairs/u,
  );
  const inspection = (typeName: string) =>
    JSON.stringify({
      relations: [
        {
          schema: "s",
          table: "pairs",
          columns: [
            {
              name: "id",
              typeSchema: "pg_catalog",
              typeName,
              primaryKeyPosition: 1,
            },
          ],
        },
      ],
    });
  assert.throws(() => oracleTables(config, inspection("money")), /money/u);
  assert.deepEqual(oracleTables(config, inspection("int8"))[0]?.columns, [
    { name: "id", kind: "int64" },
  ]);
  assert.deepEqual(oracleTables(config, inspection("int4"))[0]?.columns, [
    { name: "id", kind: "int32" },
  ]);
});

test("the reference query keys each row by its primary key's canonical cells", () => {
  assert.equal(
    referenceSql(PAIR),
    'SELECT json_build_array("id"::text)::text, md5(json_build_array("id"::text, "name"::text)::text) FROM "s"."pairs";\n',
  );
});

test("parseReference reads key and digest, and refuses a repeat or a malformed row", () => {
  const output = `["1"]\t${md5('["1", "alpha"]')}\n["2"]\t${md5('["2", "beta"]')}\n`;
  assert.deepEqual(
    [...parseReference(output)],
    [
      ['["1"]', md5('["1", "alpha"]')],
      ['["2"]', md5('["2", "beta"]')],
    ],
  );
  assert.throws(
    () => parseReference(`["1"]\t${md5("x")}\n["1"]\t${md5("y")}\n`),
    /repeats key/u,
  );
  assert.throws(() => parseReference(`["1"]\tnot-a-digest\n`), /malformed/u);
});

test("a table digest ignores read order and sees any changed row", () => {
  const one = new Map([
    ['["1"]', md5("a")],
    ['["2"]', md5("b")],
  ]);
  const reordered = new Map([
    ['["2"]', md5("b")],
    ['["1"]', md5("a")],
  ]);
  assert.equal(tableDigest(one), tableDigest(reordered));
  assert.notEqual(
    tableDigest(one),
    tableDigest(new Map([...one, ['["2"]', md5("c")]])),
  );
});

test("page timestamps become PostgreSQL's epoch microseconds", () => {
  assert.equal(isoTimestampMicros("1970-01-01T00:00:01.5Z"), "1500000");
  assert.equal(
    isoTimestampMicros("2024-01-02T03:04:05.123456Z"),
    String(Date.parse("2024-01-02T03:04:05Z") * 1_000 + 123_456),
  );
  assert.equal(isoTimestampMicros("1969-12-31T23:59:59.250Z"), "-750000");
  assert.equal(isoTimestampMicros("1970-01-01T00:00:00Z"), "0");
  assert.throws(
    () => isoTimestampMicros("2024-01-02T03:04:05+01:00"),
    /not ISO 8601 UTC/u,
  );
});

test("a page cell has one canonical text per kind, and a wrong shape is refused", () => {
  const uuid = "0123abcd-4567-89ab-cdef-0123456789ab";
  assert.equal(canonicalPageCell("uuid", uuid), uuid);
  assert.throws(() => canonicalPageCell("uuid", uuid.toUpperCase()), /uuid/u);
  assert.equal(canonicalPageCell("int64", "-42"), "-42");
  assert.throws(() => canonicalPageCell("int64", 42), /int64/u);
  assert.throws(() => canonicalPageCell("int64", "042"), /int64/u);
  assert.equal(canonicalPageCell("int32", -42), "-42");
  assert.throws(() => canonicalPageCell("int32", "-42"), /int32/u);
  assert.throws(() => canonicalPageCell("int32", 1.5), /int32/u);
  assert.equal(canonicalPageCell("bool", false), "false");
  assert.equal(canonicalPageCell("utf8", "é"), "é");
  assert.equal(
    canonicalPageCell("timestamp", "1970-01-01T00:00:00.000001Z"),
    "1",
  );
  assert.equal(
    canonicalPageCell("json", { bb: [1, "x"], a: null }),
    '{"a": null, "bb": [1, "x"]}',
  );
  assert.equal(canonicalPageCell("utf8", null), null);
});

test("decoded JSON is printed the way PostgreSQL 18 prints jsonb", () => {
  // Captured from `SELECT '{"b":1,"aa":[1,2],"a":{"z":null}}'::jsonb::text`.
  assert.equal(
    jsonbText({ b: 1, aa: [1, 2], a: { z: null } }),
    '{"a": {"z": null}, "b": 1, "aa": [1, 2]}',
  );
  assert.throws(() => jsonbText({ ratio: 0.5 }), /no exact jsonb text/);
});

test("a row's cells are joined the way json_build_array prints text", () => {
  // Captured from `SELECT json_build_array('a', NULL, 'x"y', true::text)::text`.
  assert.equal(
    cellArray(["a", null, 'x"y', "true"]),
    '["a", null, "x\\"y", "true"]',
  );
});

test("a table comparison names missing, extra, different, repeated and unreadable rows", () => {
  const reference = parseReference(
    [
      `["1"]\t${md5('["1", "alpha"]')}`,
      `["2"]\t${md5('["2", "beta"]')}`,
      `["3"]\t${md5('["3", "gamma"]')}`,
    ].join("\n"),
  );
  const clean = new TableComparison(PAIR, reference);
  clean.add(["3", "gamma"]);
  clean.add(["1", "alpha"]);
  clean.add(["2", "beta"]);
  assert.deepEqual(clean.finish(), {
    collection: "pairs",
    pageRows: 3,
    referenceRows: 3,
    differenceCount: 0,
    differences: [],
  });

  const wrong = new TableComparison(PAIR, reference);
  wrong.add(["1", "alpha"]);
  wrong.add(["2", "BETA"]);
  wrong.add(["2", "beta"]);
  wrong.add(["4", "delta"]);
  wrong.add(["5"]);
  wrong.add([5, "number"]);
  const verdict = wrong.finish();
  assert.equal(verdict.pageRows, 6);
  assert.equal(verdict.differenceCount, 6);
  assert.deepEqual(
    verdict.differences.map((difference) => [difference.key, difference.kind]),
    [
      ['["2"]', "different"],
      ['["2"]', "repeated"],
      ['["4"]', "extra"],
      ["row 5", "unreadable"],
      ["row 6", "unreadable"],
      ['["3"]', "missing"],
    ],
  );
});

const PAIR_REFERENCE = new Map([
  [
    "pairs",
    parseReference(
      [
        `["1"]\t${md5('["1", "alpha"]')}`,
        `["2"]\t${md5('["2", "beta"]')}`,
        `["3"]\t${md5('["3", "gamma"]')}`,
      ].join("\n"),
    ),
  ],
]);

const PAIR_ROWS = [
  ["1", "alpha"],
  ["2", "beta"],
  ["3", "gamma"],
];

/**
 * A page that holds `pairs` and its publication count; `moved` runs after
 * each window it hands out, with the number of windows read so far.
 */
class MovingPage implements OraclePage {
  reads = 0;
  constructor(
    public rows: readonly (readonly unknown[])[],
    public count: number | null,
    private readonly moved: (page: MovingPage, reads: number) => void = () =>
      undefined,
  ) {}

  publications(): Promise<number | null> {
    return Promise.resolve(this.count);
  }

  slice(
    collection: string,
    _columns: readonly string[],
    start: number,
    count: number,
  ): Promise<readonly (readonly unknown[])[]> {
    assert.equal(collection, "pairs");
    const read = this.rows.slice(start, start + count);
    this.reads += 1;
    this.moved(this, this.reads);
    return Promise.resolve(read);
  }
}

const counts = (result: Awaited<ReturnType<typeof pageOracle>>) =>
  result.enumerations.map(({ before, after }) => [before, after]);

test("an enumeration a delete cut short is read again, not judged against PostgreSQL", async () => {
  // The page holds a row PostgreSQL deleted, and applies the delete after
  // the oracle's first window: the second window is short and ends the table
  // with row 2 never read.
  const page = new MovingPage(
    [["1", "alpha"], ["4", "delta"], ...PAIR_ROWS.slice(1)],
    7,
    (moving, reads) => {
      if (reads !== 1) return;
      moving.rows = PAIR_ROWS;
      moving.count = 8;
    },
  );
  const result = await pageOracle(page, [PAIR], PAIR_REFERENCE, { window: 2 });
  assert.deepEqual(counts(result), [
    [7, 8],
    [8, 8],
    [8, 8],
  ]);
  assert.equal(result.unsettled, null);
  assert.deepEqual(result.problems, []);
  assert.equal(result.passed, true);
  assert.equal(result.tables[0]?.pageRows, 3);
});

test("rows that move with no publication counted are read until two enumerations agree", async () => {
  // The engine applies a batch in the middle of the second enumeration and
  // the page never counts it: that enumeration reads half of each state.
  const stale = [
    ["1", "ALPHA"],
    ["2", "beta"],
    ["3", "GAMMA"],
  ];
  const page = new MovingPage(stale, 3, (moving, reads) => {
    if (reads === 3) moving.rows = PAIR_ROWS;
  });
  const result = await pageOracle(page, [PAIR], PAIR_REFERENCE, { window: 2 });
  assert.deepEqual(counts(result), [
    [3, 3],
    [3, 3],
    [3, 3],
    [3, 3],
  ]);
  const digests = result.enumerations.map(({ digest }) => digest);
  assert.equal(new Set(digests).size, 3);
  assert.equal(digests[2], digests[3]);
  assert.equal(result.unsettled, null);
  assert.deepEqual(result.problems, []);
});

test("a page still publishing through every enumeration is unsettled, a DNC with no problems", async () => {
  const page = new MovingPage(PAIR_ROWS.slice(0, 2), 0, (moving) => {
    moving.count = (moving.count ?? 0) + 1;
  });
  const result = await pageOracle(page, [PAIR], PAIR_REFERENCE, {
    window: 2,
    attempts: 3,
  });
  assert.deepEqual(counts(result), [
    [0, 2],
    [2, 4],
    [4, 6],
  ]);
  assert.equal(
    result.unsettled,
    "no two enumerations in a row read the same rows at one publication count in 3 (counts 0 to 2, 2 to 4, 4 to 6)",
  );
  assert.deepEqual(result.problems, []);
  assert.equal(result.passed, false);
  assert.equal(result.tables[0]?.differenceCount, 1);

  const uncounted = new MovingPage(PAIR_ROWS, null);
  const blind = await pageOracle(uncounted, [PAIR], PAIR_REFERENCE);
  assert.equal(
    blind.unsettled,
    "the page reports no publication count to hold an enumeration against",
  );
  assert.equal(uncounted.reads, 0);
  assert.deepEqual(blind.problems, []);
});

test("an enumeration counts only when the page's count holds from before it to after it", async () => {
  // The rows never change; only the count moves, at the end of the first
  // enumeration and then at the end of the second.
  const first = new MovingPage(PAIR_ROWS, 5, (moving, reads) => {
    if (reads === 2) moving.count = 6;
  });
  const movedFirst = await pageOracle(first, [PAIR], PAIR_REFERENCE, {
    window: 2,
  });
  assert.deepEqual(counts(movedFirst), [
    [5, 6],
    [6, 6],
    [6, 6],
  ]);
  assert.equal(movedFirst.unsettled, null);
  const second = new MovingPage(PAIR_ROWS, 5, (moving, reads) => {
    if (reads === 4) moving.count = 6;
  });
  const movedSecond = await pageOracle(second, [PAIR], PAIR_REFERENCE, {
    window: 2,
  });
  assert.deepEqual(counts(movedSecond), [
    [5, 5],
    [5, 6],
    [6, 6],
    [6, 6],
  ]);
  assert.equal(movedSecond.unsettled, null);
});

test("a page that holds still and disagrees with PostgreSQL is judged on its second enumeration", async () => {
  const page = new MovingPage(
    [
      ["1", "alpha"],
      ["2", "BETA"],
      ["3", "gamma"],
    ],
    5,
  );
  const result = await pageOracle(page, [PAIR], PAIR_REFERENCE, { window: 2 });
  assert.deepEqual(counts(result), [
    [5, 5],
    [5, 5],
  ]);
  assert.equal(result.unsettled, null);
  assert.deepEqual(result.problems, [
    "pairs: 1 rows differ (3 on the page, 3 in PostgreSQL)",
  ]);
  assert.equal(result.passed, false);
});

test("the sample and summary writers name no absolute path and pass the local-path gate", () => {
  const directory = mkdtempSync(join(tmpdir(), "bench-paths-"));
  try {
    mkdirSync(join(directory, "samples"));
    const stack = new Error("stopped").stack ?? "";
    assert.ok(stack.includes(root), stack);
    const record = {
      calque: checkoutPath(join(root, "target/release/calque")),
      elsewhere: checkoutPath(resolve(root, "../other-worktree/calque")),
      log: join(root, "target/bench/run/logs/prepare-0.log"),
      module: pathToFileURL(join(root, "scripts/bench.ts")).href,
      browser: join(homedir(), ".cache/calque/chrome-for-testing"),
      failure: `the page threw at ${root}`,
      stack,
      url: "http://127.0.0.1:8/v1/changes?<redacted>",
      profile: join(tmpdir(), "chrome-profile-1"),
      resolved: join(realpathSync(tmpdir()), "chrome-profile-2"),
      // Assembled here, so this file holds no path the gate refuses.
      encoded: `http://127.0.0.1:8/?root=%2F${"Users"}%2Fsomeone%2Fwork`,
    };
    writeRecord(
      { directory, options: parseBenchArguments([]) },
      "cold-0",
      record,
    );
    writeRunSummary(directory, { aborted: stack, host: record });
    for (const file of ["samples/cold-0.json", "summary.json"]) {
      const text = readFileSync(join(directory, file), "utf8");
      assert.doesNotMatch(text, /(?<![\w.~:/>-])\/[\w.-]/u, file);
      assert.ok(!text.includes(homedir()), file);
      assert.deepEqual(localPathsIn(text), [], file);
      const written = JSON.parse(text) as {
        host?: typeof record;
      } & typeof record;
      const host = written.host ?? written;
      assert.deepEqual(
        [
          host.calque,
          host.elsewhere,
          host.log,
          host.module,
          host.browser,
          host.failure,
          host.url,
          host.profile,
          host.resolved,
          host.encoded,
        ],
        [
          "target/release/calque",
          "calque (outside the checkout)",
          "target/bench/run/logs/prepare-0.log",
          "scripts/bench.ts",
          "<home>/.cache/calque/chrome-for-testing",
          "the page threw at .",
          "http://127.0.0.1:8/v1/changes?<redacted>",
          "<tmp>/chrome-profile-1",
          "<tmp>/chrome-profile-2",
          "http://127.0.0.1:8/?root=<withheld>",
        ],
      );
      assert.match(host.stack, /scripts\/bench\.test\.ts:[0-9]+/u);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a run's logs name no absolute path and pass the local-path gate once scrubbed", () => {
  const directory = mkdtempSync(join(tmpdir(), "bench-logs-"));
  try {
    mkdirSync(join(directory, "logs"));
    const log = join(directory, "logs/prepare-0.log");
    writeFileSync(
      log,
      `Client settings: ${root}/target/bench/run/state/client.env\nbrowser ${homedir()}/.cache/calque\nstate ${tmpdir()}/calque-state-1\n`,
    );
    scrubRunLogs(directory);
    const text = readFileSync(log, "utf8");
    assert.doesNotMatch(text, /(?<![\w.~:/>-])\/[\w.-]/u);
    assert.deepEqual(localPathsIn(text), []);
    assert.equal(
      text,
      "Client settings: target/bench/run/state/client.env\nbrowser <home>/.cache/calque\nstate <tmp>/calque-state-1\n",
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("chart buckets compare in one order whatever order either side used", () => {
  const postgres = parseBuckets("open\t2\t5\nclosed\t0\t7\nopen\t10\t1\n");
  const page = [
    { status: "open", priority: "10", count: 1 },
    { status: "closed", priority: "0", count: 7 },
    { status: "open", priority: "2", count: 5 },
  ];
  assert.deepEqual(normalizeBuckets(postgres), normalizeBuckets(page));
  assert.deepEqual(
    normalizeBuckets(postgres).map((bucket) => bucket.priority),
    ["0", "2", "10"],
  );
  assert.throws(() => parseBuckets("open\t2\n"), /malformed bucket/u);
});

// Read-out ----------------------------------------------------------------------

test("nearest rank leaves its input in order", () => {
  const values = [
    30,
    ...Array.from({ length: 29 }, (_unused, index) => index + 1),
  ];
  const original = [...values];
  assert.equal(nearestRank(values, 0.95), 29);
  assert.deepEqual(values, original);
  assert.equal(nearestRank([7], 0.95), 7);
  assert.throws(() => nearestRank([], 0.5), /nearest-rank inputs are invalid/u);
});

test("a failure ranks above every number: one in 30 leaves p95 finite, two do not", () => {
  const passing = Array.from({ length: 29 }, (_, index) => index + 1);
  assert.equal(nearestRankWithFailures([...passing, null], 0.95), 29);
  assert.equal(nearestRankWithFailures([...passing, null], 0.99), null);
  assert.equal(
    nearestRankWithFailures([...passing.slice(1), null, null], 0.95),
    null,
  );
  assert.equal(nearestRankWithFailures([5, 1, 3], 0.5), 3);
  assert.throws(() => nearestRankWithFailures([], 0.5), /invalid/u);
  assert.throws(() => nearestRankWithFailures([1], 0), /invalid/u);
  assert.deepEqual(distribution([3, 1, null]), {
    samples: 3,
    failures: 1,
    min: 1,
    p50: 3,
    p95: null,
    p99: null,
    max: null,
  });
  assert.equal(rankedValue({ verdict: "PASS", valueMs: 12 }), 12);
  assert.equal(rankedValue({ verdict: "INVALID_RUN", valueMs: 12 }), null);
});

const passing: Measurements = {
  preparationsMs: [4_000, 5_000, 6_000],
  coldMs: Array.from({ length: 30 }, () => 1_000),
  coldRetaken: [],
  commitMs: Array.from({ length: 30 }, () => 100),
  commitRetaken: [],
  peakRssBytes: [1e9, 2e9],
  longestTaskMs: [0, 50],
  achievedRowChangesPerSecond: 10_000,
  correctnessFailed: false,
  realtimeVoided: false,
};

test("every threshold row is judged on its own", () => {
  const rows = thresholdRows(passing);
  assert.deepEqual(
    rows.map((row) => [row.metric, row.statistic, row.value, row.verdict]),
    [
      ["server_preparation_ms", "p95 of 3", 6_000, "pass"],
      ["complete_stable_paint_ms", "p95 of 30", 1_000, "pass"],
      ["commit_to_stable_paint_ms", "p95 of 30", 100, "pass"],
      ["commit_to_stable_paint_ms", "p99 of 30", 100, "pass"],
      ["browser_process_tree_peak_rss_bytes", "max", 2e9, "pass"],
      ["main_thread_long_task_ms", "max", 50, "pass"],
      ["achieved source row changes/s", "first to last commit", 10_000, "pass"],
    ],
  );
  assert.equal(runPasses(rows, false, false), true);
  // A diagnostic run with the same rows does not pass.
  assert.equal(runPasses(rows, false, true), false);

  const slow = thresholdRows({
    ...passing,
    coldMs: [...passing.coldMs.slice(2), null, null],
    longestTaskMs: [51],
    peakRssBytes: [1e9, null],
    achievedRowChangesPerSecond: 9_999,
  });
  assert.deepEqual(
    slow.map((row) => row.verdict),
    ["pass", "fail", "pass", "pass", "unmeasured", "fail", "fail"],
  );
  assert.match(slow[6]?.note ?? "", /lighter load/u);
  assert.equal(runPasses(slow, false, false), false);
  // A row with nothing taken again has one reading.
  assert.ok(rows.every((row) => row.secondReading === undefined));
});

test("a row that passes as judged but fails its second reading fails", () => {
  // Two cold samples taken again: one reached nothing (+∞), one 2,500 ms.
  // Judged, p95 of 30 is 1,000 ms; with each counted in the slot it left,
  // the 29th of 30 is 2,500 ms.
  const rows = thresholdRows({
    ...passing,
    coldRetaken: [
      { slot: 5, valueMs: null },
      { slot: 9, valueMs: 2_500 },
    ],
  });
  assert.deepEqual(rows[1], {
    metric: "complete_stable_paint_ms",
    statistic: "p95 of 30",
    threshold: "< 2,000 ms",
    value: 1_000,
    verdict: "fail",
    secondReading: { value: 2_500, verdict: "fail" },
  });
  assert.equal(runPasses(rows, false, false), false);
  // One probe taken again that reached nothing moves p99 of 30 to +∞ and
  // leaves p95 where it was: only the p99 row fails.
  const probes = thresholdRows({
    ...passing,
    commitRetaken: [{ slot: 0, valueMs: null }],
  });
  assert.deepEqual(
    probes
      .slice(2, 4)
      .map((row) => [row.value, row.verdict, row.secondReading]),
    [
      [100, "pass", { value: 100, verdict: "pass" }],
      [100, "fail", { value: null, verdict: "fail" }],
    ],
  );
  // A sample taken again that reached the threshold's side passes both.
  const fast = thresholdRows({
    ...passing,
    commitRetaken: [{ slot: 3, valueMs: 120 }],
  });
  assert.deepEqual(fast[3]?.secondReading, { value: 120, verdict: "pass" });
  assert.equal(fast[3]?.verdict, "pass");
  assert.equal(runPasses(fast, false, false), true);
});

test("the second reading counts the first sample taken for a slot, in that slot", () => {
  assert.equal(secondReading([1, 2, 3], []), null);
  assert.deepEqual(
    secondReading(
      [1, 2, 3],
      [
        { slot: 1, valueMs: 20 },
        { slot: 1, valueMs: null },
        { slot: 2, valueMs: null },
      ],
    ),
    [1, 20, null],
  );
  assert.throws(
    () => secondReading([1, 2, 3], [{ slot: 3, valueMs: 4 }]),
    /slot 3, outside the 3 judged/u,
  );
});

test("the load rate counts the intervals between the first and last commit", () => {
  // A run's monitor series counted 199 commits at 82,794,439.350 ms and 2,999
  // at 82,808,439.136 ms, 200 a second.
  assert.ok(Math.abs((2_999 - 199) / 13_999.786 - 0.2) < 1e-4);
  // Its writer recorded 3,096 commits, the last 15,487.368 ms after it
  // started; one divided by the other fails the load row.
  const completed = 3_096;
  const lastCommitMs = 15_487.367_834;
  const frozen =
    (completed / (lastCommitMs / 1_000)) * LOAD_ROW_CHANGES_PER_TRANSACTION;
  assert.ok(Math.abs(frozen - 9_995.242_68) < 1e-3);
  assert.equal(
    thresholdRows({ ...passing, achievedRowChangesPerSecond: frozen })[6]
      ?.verdict,
    "fail",
  );
  // The writer issues transaction n at the (n + 1)th 5 ms tick, so every
  // commit landed 7.37 ms after its tick, the first at 12.37 ms.
  const latencyMs = lastCommitMs - completed * 5;
  const amended = achievedTransactionsPerSecond(
    completed,
    5 + latencyMs,
    lastCommitMs,
  );
  assert.ok(amended !== null);
  assert.ok(
    Math.abs(amended * LOAD_ROW_CHANGES_PER_TRANSACTION - 10_000) < 1e-6,
  );
  // Commit latency moves both ends alike and cancels.
  const slower = achievedTransactionsPerSecond(completed, 45, 15_520);
  assert.ok(slower !== null && Math.abs(slower - amended) < 1e-9);
  assert.equal(achievedTransactionsPerSecond(0, 0, 0), null);
  assert.equal(achievedTransactionsPerSecond(1, 12, 12), null);
  assert.equal(achievedTransactionsPerSecond(2, 12, 12), null);
  assert.equal(achievedTransactionsPerSecond(2, 12, 17), 200);
});

test("the writer is paced 10% above the load-rate threshold", () => {
  assert.equal(
    LOAD_TRANSACTIONS_PER_SECOND * LOAD_ROW_CHANGES_PER_TRANSACTION,
    (THRESHOLDS.sourceRowChangesPerSecond * 11) / 10,
  );
});

test("a run that failed correctness reports its timings as void", () => {
  const rows = thresholdRows({ ...passing, correctnessFailed: true });
  assert.deepEqual(
    rows.slice(0, 4).map((row) => row.verdict),
    ["void", "void", "void", "void"],
  );
  assert.equal(runPasses(rows, true, false), false);
  assert.equal(
    thresholdRows({ ...passing, coldMs: [] })[1]?.verdict,
    "unmeasured",
  );
});

test("a realtime session the network memory guard voided reports its probes' timings as void", () => {
  const rows = thresholdRows({ ...passing, realtimeVoided: true });
  assert.deepEqual(
    rows.slice(0, 4).map((row) => [row.metric, row.verdict]),
    [
      ["server_preparation_ms", "pass"],
      ["complete_stable_paint_ms", "pass"],
      ["commit_to_stable_paint_ms", "void"],
      ["commit_to_stable_paint_ms", "void"],
    ],
  );
  assert.equal(runPasses(rows, false, false), false);
});

const quiet: EnvironmentReading = {
  loadAverage: [1, 1, 1],
  freeMemoryBytes: 1e10,
  memoryFreePercent: 60,
  powerSource: "AC Power",
  otherContainersRunning: false,
  benchCpu: {
    atMs: 0,
    runnerSeconds: 0,
    descendantsSeconds: null,
    descendantsAlive: null,
    postgresSeconds: null,
  },
};

test("the environment labels numbers directional and never removes them", () => {
  assert.deepEqual(directionalReasons([quiet, quiet]), []);
  assert.deepEqual(
    directionalReasons([
      quiet,
      { ...quiet, loadAverage: [4.5, 1, 1], otherContainersRunning: true },
      { ...quiet, otherContainersRunning: true },
    ]),
    ["another container was running", "one-minute load average above 4"],
  );
  assert.deepEqual(
    directionalReasons([quiet, { ...quiet, otherContainersRunning: true }]),
    ["another container was running"],
  );
  assert.deepEqual(
    directionalReasons([{ ...quiet, loadAverage: [4, 9, 9] }]),
    [],
  );
  // Docker that did not answer, or was not asked, cannot vouch for a quiet host.
  assert.deepEqual(
    directionalReasons([quiet, { ...quiet, otherContainersRunning: null }]),
    ["the running containers were not read"],
  );
});

test("host readings parse memory_pressure, pmset and docker ps output", () => {
  assert.equal(
    parseMemoryFreePercent(
      "The system has 34359738368 (2097152 pages with a page size of 16384).\n...\nSystem-wide memory free percentage: 42%\n",
    ),
    42,
  );
  assert.equal(parseMemoryFreePercent(""), null);
  assert.equal(
    parsePowerSource(
      "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t100%; charged;\n",
    ),
    "AC Power",
  );
  assert.equal(parsePowerSource(""), null);
  assert.deepEqual(
    parseOtherContainers("calque-bench\ttrue\nsomeone-else\t\nother\tbeta\n\n"),
    ["other", "someone-else"],
  );
});

test("the page coding witness reads every page response of every judged cold sample", () => {
  const page = "http://127.0.0.1:4100";
  const ledger = (script: string | null) => [
    { url: `${page}/`, status: 200, contentEncoding: "br" },
    { url: `${page}/assets/main-1.js`, status: 200, contentEncoding: script },
    // A worker's script loads on the worker's target, and the page's ledger
    // shows it with no status; calque's own responses are not the page's.
    { url: `${page}/assets/worker-1.js`, status: null, contentEncoding: null },
    { url: `${page}/favicon.ico`, status: 404, contentEncoding: null },
    {
      url: "http://127.0.0.1:4200/v1/bootstrap",
      status: 200,
      contentEncoding: null,
    },
  ];
  const all = judgePageCoding(page, [
    { name: "cold-0", requests: ledger("br") },
    { name: "cold-1", requests: ledger("BR") },
  ]);
  assert.equal(all.brotli, true);
  assert.deepEqual(
    all.samples.map((sample) => sample.responses),
    [2, 2],
  );
  assert.equal(
    pageCodingLine(all),
    "page coding: all 4 page responses in 2 judged cold samples arrived in brotli",
  );

  const one = judgePageCoding(page, [
    { name: "cold-0", requests: ledger("br") },
    { name: "cold-1", requests: ledger(null) },
    { name: "cold-2", requests: ledger("gzip") },
  ]);
  assert.equal(one.brotli, false);
  assert.deepEqual(
    one.samples.map((sample) => sample.notBrotli),
    [[], ["/assets/main-1.js (none)"], ["/assets/main-1.js (gzip)"]],
  );
  assert.match(
    pageCodingLine(one),
    /^page coding: 2 of 6 page responses in 3 judged cold samples did not arrive in brotli, .*: cold-1 \/assets\/main-1\.js \(none\), cold-2 \/assets\/main-1\.js \(gzip\)$/u,
  );

  const unseen = judgePageCoding(page, [
    { name: "cold-0", requests: ledger("br") },
    { name: "cold-1", requests: ledger("br").slice(2) },
  ]);
  assert.equal(unseen.brotli, null);
  assert.equal(
    unseen.reason,
    "1 of 2 judged cold samples' ledgers show no page response",
  );
  assert.equal(judgePageCoding(page, []).reason, "no judged cold sample");
});

test("the edge protocol witness reads every response of every judged cold sample", () => {
  const ledger = (protocol: string | null) => [
    { url: "https://127.0.0.1:4100/", status: 200, protocol: "h2" },
    { url: "https://127.0.0.1:4200/v1/bootstrap", status: 200, protocol },
    // A worker's script shows no status in the page's ledger.
    {
      url: "https://127.0.0.1:4100/assets/worker-1.js",
      status: null,
      protocol: null,
    },
  ];
  const all = judgeEdgeProtocol(true, [
    { name: "cold-0", requests: ledger("h2") },
    { name: "cold-1", requests: ledger("h2") },
  ]);
  assert.equal(all.h2, true);
  assert.equal(
    edgeProtocolLine(all),
    "edge protocol: all 4 responses in 2 judged cold samples arrived over HTTP/2",
  );

  const one = judgeEdgeProtocol(true, [
    { name: "cold-0", requests: ledger("h2") },
    { name: "cold-1", requests: ledger("http/1.1") },
  ]);
  assert.equal(one.h2, false);
  assert.deepEqual(
    one.samples.map((sample) => sample.notH2),
    [[], ["/v1/bootstrap (http/1.1)"]],
  );
  assert.match(
    edgeProtocolLine(one),
    /^edge protocol: 1 of 4 responses in 2 judged cold samples did not arrive over HTTP\/2, .*: cold-1 \/v1\/bootstrap \(http\/1\.1\)$/u,
  );

  const unseen = judgeEdgeProtocol(true, [
    { name: "cold-0", requests: ledger("h2") },
    { name: "cold-1", requests: ledger("h2").slice(2) },
  ]);
  assert.equal(unseen.h2, null);
  assert.equal(
    unseen.reason,
    "1 of 2 judged cold samples' ledgers show no response",
  );
  assert.deepEqual(
    judgeEdgeProtocol(false, [
      { name: "cold-0", requests: ledger("http/1.1") },
    ]),
    { h2: null, samples: [], reason: "the run has no edge" },
  );
  assert.equal(
    edgeProtocolLine(judgeEdgeProtocol(false, [])),
    "edge protocol: not shown: the run has no edge",
  );
});

test("a run whose judged cold samples did not each load its one artifact object is not judged", () => {
  const route = "https://127.0.0.1:4200/v1/artifact-objects/";
  const brotli = `${route}sha256:b0?<redacted>`;
  const zstd = `${route}sha256:19?<redacted>`;
  const expected = { objectUrl: brotli, artifactRoute: route };
  const ledger = (...artifacts: string[]) =>
    ["https://127.0.0.1:4100/", ...artifacts].map((url) => ({ url }));

  // A sample that asked for the object twice still asked for one object.
  const all = judgeArtifactObject(expected, [
    { name: "cold-0", requests: ledger(brotli) },
    { name: "cold-1", requests: ledger(brotli, brotli) },
  ]);
  assert.deepEqual(all, {
    one: true,
    expected: brotli,
    objects: [{ url: brotli, samples: ["cold-0", "cold-1"] }],
    without: [],
    reason: null,
  });
  assert.equal(
    artifactObjectLine(all),
    `artifact object: all 2 judged cold samples asked for ${brotli} and no other artifact`,
  );
  assert.deepEqual(artifactObjectReasons(all), []);

  // The hold ended before calque published the object the later samples load.
  const mixed = judgeArtifactObject({ objectUrl: zstd, artifactRoute: route }, [
    { name: "cold-0", requests: ledger(zstd) },
    { name: "cold-1", requests: ledger(brotli) },
    { name: "cold-2", requests: ledger() },
  ]);
  assert.equal(mixed.one, false);
  assert.deepEqual(mixed.objects, [
    { url: zstd, samples: ["cold-0"] },
    { url: brotli, samples: ["cold-1"] },
  ]);
  assert.deepEqual(mixed.without, ["cold-2"]);
  assert.equal(
    artifactObjectLine(mixed),
    `artifact object: the judged cold samples did not each ask for ${zstd} alone, so this run's cold distribution is not of that one object: ${zstd} in cold-0; ${brotli} in cold-1; none in cold-2`,
  );
  assert.deepEqual(artifactObjectReasons(mixed), [
    "cold samples did not each load the one artifact object the run expects",
  ]);

  // Every sample loaded one object, and not the one the run planned; one
  // sample loaded the planned object beside another; one loaded none.
  for (const samples of [
    [{ name: "cold-0", requests: ledger(zstd) }],
    [{ name: "cold-0", requests: ledger(brotli, zstd) }],
    [
      { name: "cold-0", requests: ledger(brotli) },
      { name: "cold-1", requests: ledger() },
    ],
  ])
    assert.equal(judgeArtifactObject(expected, samples).one, false);

  const none = judgeArtifactObject(null, [
    { name: "cold-0", requests: ledger(zstd) },
  ]);
  assert.deepEqual(none, {
    one: null,
    expected: null,
    objects: [],
    without: [],
    reason: "the run expects no artifact object",
  });
  assert.equal(
    artifactObjectLine(none),
    "artifact object: not shown: the run expects no artifact object",
  );
  assert.deepEqual(artifactObjectReasons(none), []);
  assert.equal(
    judgeArtifactObject(expected, []).reason,
    "no judged cold sample",
  );
});

test("the browser's memory is the RSS of its process tree", () => {
  const rows = parseRssTable(
    [
      "  100     1  2048 /usr/local/bin/node",
      "  200   100  4096 /Applications/Chromium",
      "  300   200  1024 /Applications/Chromium Helper (Renderer)",
      "  400     1   512 /usr/sbin/cfprefsd",
      "  500   300   abc unreadable",
      "",
    ].join("\n"),
  );
  assert.deepEqual(
    rows.map((row) => [row.pid, row.parentPid, row.rssBytes]),
    [
      [100, 1, 2_097_152],
      [200, 100, 4_194_304],
      [300, 200, 1_048_576],
      [400, 1, 524_288],
    ],
  );
  assert.equal(rows[2]?.command, "/Applications/Chromium Helper (Renderer)");
  assert.deepEqual(
    selectProcessTree(rows, 200).map((row) => row.pid),
    [200, 300],
  );
  assert.deepEqual(selectProcessTree(rows, 999), []);
  assert.deepEqual(selectProcessTree(rows, 0), []);
});

test("the benchmark's CPU time reads ps, cgroup and a pid that exits or is reused", () => {
  assert.equal(parseCpuTime("0:00.03"), 0.03);
  assert.equal(parseCpuTime("612:27.80"), 612 * 60 + 27.8);
  assert.equal(parseCpuTime("01:02:03.50"), 3_723.5);
  assert.equal(parseCpuTime("2-01:00:00"), 176_400);
  for (const bad of ["", "12.5", "1:2:3:4", "a:00.00", "-1:00.00"])
    assert.equal(parseCpuTime(bad), null, bad);
  assert.deepEqual(
    parseCpuTable(
      [
        "  101     1   1:02.50 /usr/local/bin/node",
        "  202   101   0:10.25 /Applications/Chromium Helper (Renderer)",
        "  303   101   garbage /bin/psql",
        "",
      ].join("\n"),
    ),
    [
      {
        pid: 101,
        parentPid: 1,
        cpuSeconds: 62.5,
        command: "/usr/local/bin/node",
      },
      {
        pid: 202,
        parentPid: 101,
        cpuSeconds: 10.25,
        command: "/Applications/Chromium Helper (Renderer)",
      },
    ],
  );
  assert.equal(
    parseCgroupCpuSeconds("usage_usec 2500000\nuser_usec 2000000\n"),
    2.5,
  );
  assert.equal(parseCgroupCpuSeconds("user_usec 1\n"), null);

  const cpu = new DescendantCpu();
  const row = (pid: number, cpuSeconds: number, command = "chrome") => ({
    pid,
    parentPid: 1,
    cpuSeconds,
    command,
  });
  assert.deepEqual(cpu.observe([row(10, 1), row(11, 2)]), {
    seconds: 3,
    alive: 2,
  });
  // 11 exited after using 2 s; 10 went on to 4 s.
  assert.deepEqual(cpu.observe([row(10, 4)]), { seconds: 6, alive: 1 });
  // 10 is reused by a new chrome at 0.5 s: the old one's 4 s stay.
  assert.deepEqual(cpu.observe([row(10, 0.5), row(12, 1, "psql")]), {
    seconds: 7.5,
    alive: 2,
  });
});

test("calque's OpenMetrics text yields its unlabelled samples", () => {
  const text = [
    "# HELP calque_cdc_lag_bytes Latest observed replication WAL end minus durable flushed WAL position in bytes.",
    "# TYPE calque_cdc_lag_bytes gauge",
    "calque_cdc_lag_bytes 4096",
    "calque_canonical_append_duration_seconds_sum 1.25",
    "calque_canonical_append_duration_seconds_count 812",
    'calque_canonical_append_duration_seconds_bucket{le="0.0005"} 3',
    "# TYPE calque_canonical_entries_appended counter",
    "calque_canonical_entries_appended_total 6000",
    "calque_cdc_reconnects_total 0",
    "calque_cdc_lag_bytes_other 7",
    "calque_broken ",
    "# EOF",
  ].join("\n");
  assert.equal(parseMetric(text, "calque_cdc_lag_bytes"), 4_096);
  assert.equal(
    parseMetric(text, "calque_canonical_append_duration_seconds_count"),
    812,
  );
  assert.equal(
    parseMetric(text, "calque_canonical_entries_appended_total"),
    6_000,
  );
  assert.equal(parseMetric(text, "calque_cdc_reconnects_total"), 0);
  assert.equal(parseMetric(text, "calque_broken"), null);
  assert.equal(parseMetric(text, "calque_absent"), null);
});

test("replication slot rows parse, and an unreadable row is refused", () => {
  assert.deepEqual(parseSlotRows(["calque_a\tt\t4096", "calque_b\tf\t", ""]), [
    { name: "calque_a", active: true, lagBytes: 4_096 },
    { name: "calque_b", active: false, lagBytes: null },
  ]);
  assert.deepEqual(parseSlotRows([]), []);
  assert.throws(() => parseSlotRows(["calque_a\tyes\t1"]), /unreadable/u);
  assert.throws(() => parseSlotRows(["calque_a\tt\tlots"]), /unreadable/u);
});

test("change traffic counts statuses and finds the longest request and the longest wait between two", () => {
  const traffic = changeTraffic([
    { startTime: 390, responseEnd: 430, responseStatus: 200 },
    { startTime: 31, responseEnd: 60, responseStatus: 503 },
    { startTime: 0, responseEnd: 30, responseStatus: 200 },
    { startTime: 160, responseEnd: 190, responseStatus: 503 },
  ]);
  assert.deepEqual(traffic, {
    changeRequests: 4,
    changeStatuses: { "200": 2, "503": 2 },
    longestChangeRequestMs: 40,
    longestChangeGapMs: 200,
  });
  assert.deepEqual(
    changeTraffic([{ startTime: 5, responseEnd: 9, responseStatus: 0 }]),
    {
      changeRequests: 1,
      changeStatuses: { "0": 1 },
      longestChangeRequestMs: 4,
      longestChangeGapMs: null,
    },
  );
  assert.deepEqual(changeTraffic([]), {
    changeRequests: 0,
    changeStatuses: {},
    longestChangeRequestMs: null,
    longestChangeGapMs: null,
  });
  assert.equal(
    engagementLine({
      ...traffic,
      changeStatuses: { "503": 2, "0": 1, "200": 2 },
      publications: 2,
      resourceEntries: 9,
      resourceBufferSize: 100_000,
      spanMs: 1_250,
    }),
    "engagement: 4 change requests (0 x1, 200 x2, 503 x2) and 2 publications over 1.3 s; longest request 40.0 ms, longest gap between requests 200.0 ms; 9 of 100000 resource entries held",
  );
  assert.equal(engagementLine(null), "engagement: not read");
});

test("the realtime monitor samples every source each interval and records a failing one", async () => {
  let publications = 10;
  let slotsCalls = 0;
  const monitor = new RealtimeMonitor(
    {
      load: () => ({ issued: publications * 2, completed: publications }),
      publications: async () => {
        publications += 1;
        if (publications === 12) throw new Error("page gone");
        return publications;
      },
      metrics: () =>
        Promise.resolve(
          "calque_cdc_lag_bytes 512\ncalque_canonical_append_duration_seconds_count 9\ncalque_canonical_entries_appended_total 1152\ncalque_cdc_reconnects_total 1\n",
        ),
      slots: () => {
        slotsCalls += 1;
        if (slotsCalls === 1) throw new Error("psql exited");
        return Promise.resolve(["calque_a\tt\t64"]);
      },
    },
    20,
  );
  monitor.start();
  const deadline = Date.now() + 5_000;
  while (monitor.samples.length < 3 && Date.now() < deadline)
    await new Promise((resolveWait) => setTimeout(resolveWait, 5));
  await monitor.stop();
  const [first, second, third] = monitor.samples;
  assert.ok(first && second && third, "three samples within 5 s");
  assert.equal(first.publications, 11);
  assert.equal(first.slots, null);
  assert.deepEqual(first.failures, ["slots: psql exited"]);
  assert.equal(first.cdcLagBytes, 512);
  assert.equal(first.canonicalEntries, 1_152);
  assert.equal(first.canonicalAppendCommits, 9);
  assert.equal(first.cdcReconnects, 1);
  assert.equal(second.publications, null);
  assert.deepEqual(second.failures, ["publications: page gone"]);
  assert.deepEqual(second.slots, [
    { name: "calque_a", active: true, lagBytes: 64 },
  ]);
  assert.equal(third.publications, 13);
  assert.equal(third.loadCompleted, 13);
  assert.equal(third.loadIssued, 26);
  assert.deepEqual(third.failures, []);
  assert.ok(second.atMs - first.atMs >= 15);
  const count = monitor.samples.length;
  await new Promise((resolveWait) => setTimeout(resolveWait, 50));
  assert.equal(monitor.samples.length, count, "no sample after stop");
});

test("the drain reports a page still publishing instead of throwing", async () => {
  let count = 5;
  const settling = await awaitQuietPublications(
    () => Promise.resolve(count < 8 ? ++count : count),
    40,
    2_000,
  );
  assert.equal(settling.quiet, true);
  assert.equal(settling.publicationsAtLoadStop, 6);
  assert.equal(settling.publicationsAtEnd, 8);
  assert.ok(settling.lastChangeMs >= 0);

  let running = 0;
  const busy = await awaitQuietPublications(
    () => Promise.resolve(++running),
    40,
    150,
  );
  assert.equal(busy.quiet, false);
  assert.equal(busy.publicationsAtLoadStop, 1);
  assert.equal(busy.publicationsAtEnd, running);
  assert.ok(busy.lastChangeMs > 100);
});

test("the probes end at the first that times out", async () => {
  const called: number[] = [];
  const ran = await takeSamples(
    { warmups: 1, judged: 6, retakes: 3, valueMs: () => null },
    (k) => {
      called.push(k);
      return Promise.resolve({
        k,
        verdict: k === 2 ? ("DNC" as const) : ("PASS" as const),
        invalid: [],
        timedOut: k === 2,
      });
    },
  );
  assert.deepEqual(called, [0, 1, 2]);
  assert.equal(ran.timedOutAt, 2);
  assert.deepEqual(
    ran.samples.map((sample) => sample.k),
    [0, 1, 2],
  );
  assert.deepEqual(
    ran.judged.map((sample) => sample.k),
    [1, 2],
  );
  const all = await takeSamples(
    { warmups: 1, judged: 3, retakes: 3, valueMs: () => null },
    (k) =>
      Promise.resolve({
        k,
        verdict: "PASS" as const,
        invalid: [],
        timedOut: false,
      }),
  );
  assert.equal(all.timedOutAt, null);
  assert.equal(all.samples.length, 4);
  assert.equal(all.judged.length, 3);
});

// Samples taken again -----------------------------------------------------------

/** A sample that says what it measured, for the list of those taken again. */
type Measured = TakenSample & {
  readonly n: number;
  readonly valueMs: number | null;
};

/** A probe's judgement over two frames of the new state: clean, contradicted, or reordered. */
function holdJudgement(shape: "clean" | "contradicted" | "reordered") {
  const frames =
    shape === "reordered"
      ? // The matching frame is stamped 3.5 ms before the frame ahead of it.
        [frame(75, 1_003.5, A, A), frame(76, 1_000, B, B)]
      : [
          frame(75, 1_000, B, B),
          frame(76, shape === "contradicted" ? 1_500 : 4_000, C, C),
        ];
  return judgeStablePaint(frames, "grid", B, { fromMs: 900 });
}

const CLOCKS_PASS = {
  sessionClockPassed: true,
  frameClockPassed: true,
  wallDriftMs: 0,
  hostSleptMs: 0,
};

test(
  "host readings sit on mach_absolute_time, the clock Chrome stamps its events with",
  { skip: process.platform !== "darwin" && "macOS clocks only" },
  () => {
    const before = hrtimeMs();
    const read = spawnSync(
      "python3",
      [
        "-I",
        "-c",
        `import ctypes
lib = ctypes.CDLL(None)
lib.mach_absolute_time.restype = ctypes.c_uint64
class Timebase(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]
timebase = Timebase()
lib.mach_timebase_info(ctypes.byref(timebase))
print(lib.mach_absolute_time() * timebase.numer // timebase.denom)`,
      ],
      { encoding: "utf8" },
    );
    const after = hrtimeMs();
    assert.equal(read.status, 0, read.stderr);
    const absoluteMs = Number(BigInt(read.stdout.trim())) / 1_000_000;
    assert.ok(
      before <= absoluteMs && absoluteMs <= after,
      `mach_absolute_time read ${String(absoluteMs)} ms, outside the host's ${String(before)} to ${String(after)} ms`,
    );
  },
);

test("only a dropped-frame signal or a failed clock check is the instrument's alone", () => {
  assert.deepEqual(probeInvalidity(holdJudgement("clean"), CLOCKS_PASS), []);
  assert.deepEqual(coldInvalidity(holdJudgement("clean"), true), []);
  assert.deepEqual(probeInvalidity(holdJudgement("reordered"), CLOCKS_PASS), [
    { reason: "a dropped-frame signal fired", instrument: true },
  ]);
  assert.deepEqual(
    probeInvalidity(holdJudgement("contradicted"), CLOCKS_PASS),
    [
      {
        reason: "stable presented frame contradicted within the hold window",
        instrument: false,
      },
    ],
  );
  assert.deepEqual(
    probeInvalidity(holdJudgement("clean"), {
      sessionClockPassed: false,
      frameClockPassed: false,
      wallDriftMs: -5.5,
      hostSleptMs: 61_250.4,
    }),
    [
      {
        reason: "the session's clock check failed",
        instrument: true,
        sessionWide: true,
      },
      { reason: "the probe's frame clock check failed", instrument: true },
      {
        reason:
          "Chrome's wall clock moved -5.5 ms against its monotonic clock during the probe",
        instrument: true,
      },
      {
        reason: "the host slept 61250 ms since the previous probe",
        instrument: true,
      },
    ],
  );
  assert.deepEqual(
    probeInvalidity(holdJudgement("clean"), {
      ...CLOCKS_PASS,
      wallDriftMs: 5,
      hostSleptMs: 0.000_2,
    }),
    [],
  );
  assert.deepEqual(coldInvalidity(holdJudgement("contradicted"), false), [
    {
      reason: "stable presented frame contradicted within the hold window",
      instrument: false,
    },
    { reason: "the clock check failed", instrument: true },
  ]);
  assert.deepEqual(coldInvalidity(holdJudgement("reordered"), true), [
    { reason: "a dropped-frame signal fired", instrument: true },
  ]);
});

test("a sample invalid through the instrument alone is taken again under the next number", async () => {
  const dropped = probeInvalidity(holdJudgement("reordered"), CLOCKS_PASS);
  const clock = coldInvalidity(holdJudgement("clean"), false);
  const contradicted = [
    ...probeInvalidity(holdJudgement("contradicted"), CLOCKS_PASS),
    ...dropped,
  ];
  const outcomes = [
    { verdict: "INVALID_RUN", invalid: dropped, valueMs: 90 }, // 0, a warm-up
    { verdict: "PASS", invalid: [], valueMs: 91 }, // 1, a warm-up
    { verdict: "PASS", invalid: [], valueMs: 92 }, // 2
    { verdict: "INVALID_RUN", invalid: dropped, valueMs: 193 }, // 3, taken again
    { verdict: "INVALID_RUN", invalid: contradicted, valueMs: 94 }, // 4
    { verdict: "INVALID_RUN", invalid: clock, valueMs: null }, // 5, taken again
    { verdict: "INCORRECT", invalid: dropped, valueMs: 96 }, // 6
    { verdict: "DNC", invalid: clock, valueMs: null }, // 7
    { verdict: "PASS", invalid: [], valueMs: 98 }, // 8
  ] as const;
  const slots: (number | null)[] = [];
  const settled: Taken[] = [];
  const ran = await takeSamples(
    {
      warmups: 2,
      judged: 5,
      retakes: 3,
      valueMs: (sample: Measured) => sample.valueMs,
    },
    (n, slot) => {
      slots.push(slot);
      const outcome = outcomes[n];
      if (!outcome) throw new Error(`sample ${String(n)} was not planned`);
      return Promise.resolve({ n, ...outcome });
    },
    (sample, taken) => {
      assert.equal(sample.n, taken.n);
      settled.push(taken);
    },
  );
  assert.deepEqual(slots, [null, null, 0, 1, 1, 2, 2, 3, 4]);
  assert.deepEqual(
    settled.map(({ n, warmup, slot, retake }) => [n, warmup, slot, retake]),
    [
      [0, true, null, null],
      [1, true, null, null],
      [2, false, 0, null],
      [3, false, 1, ["a dropped-frame signal fired"]],
      [4, false, 1, null],
      [5, false, 2, ["the clock check failed"]],
      [6, false, 2, null],
      [7, false, 3, null],
      [8, false, 4, null],
    ],
  );
  // Each sample taken again is listed with what it measured, so a run shows
  // whether the ones it replaced were faster or slower than those judged.
  assert.deepEqual(ran.retaken, [
    { n: 3, slot: 1, reasons: ["a dropped-frame signal fired"], valueMs: 193 },
    { n: 5, slot: 2, reasons: ["the clock check failed"], valueMs: null },
  ]);
  assert.deepEqual(
    ran.judged.map((sample) => sample.n),
    [2, 4, 6, 7, 8],
  );
  assert.equal(ran.samples.length, 9);
  assert.equal(ran.timedOutAt, null);
});

test("past three samples taken again, an instrument's INVALID_RUN is judged and ranks +∞", async () => {
  const dropped = probeInvalidity(holdJudgement("reordered"), CLOCKS_PASS);
  const settled: Taken[] = [];
  const ran = await takeSamples(
    {
      warmups: 0,
      judged: 30,
      retakes: 3,
      valueMs: (sample: Measured) => sample.valueMs,
    },
    (n) =>
      Promise.resolve({
        n,
        verdict: n % 10 === 0 ? ("INVALID_RUN" as const) : ("PASS" as const),
        invalid: n % 10 === 0 ? dropped : [],
        valueMs: n % 10 === 0 ? null : 100 + n,
      }),
    (_sample, taken) => settled.push(taken),
  );
  assert.deepEqual(
    ran.retaken.map(({ n }) => n),
    [0, 10, 20],
  );
  assert.deepEqual(
    settled.filter(({ retake }) => retake !== null).map(({ n }) => n),
    [0, 10, 20],
  );
  assert.equal(ran.samples.length, 33);
  assert.deepEqual(
    ran.judged
      .filter((sample) => sample.verdict === "INVALID_RUN")
      .map((sample) => sample.n),
    [30],
  );
  const commit = distribution(ran.judged.map(rankedValue));
  assert.equal(commit.samples, 30);
  assert.equal(commit.failures, 1);
  assert.equal(commit.p95, 132);
  assert.equal(commit.p99, null);

  const none = await takeSamples(
    { warmups: 0, judged: 2, retakes: 0, valueMs: () => null },
    (n) =>
      Promise.resolve({ n, verdict: "INVALID_RUN" as const, invalid: dropped }),
  );
  assert.deepEqual(none.retaken, []);
  assert.equal(none.samples.length, 2);
});

test("a probe that failed the session's clock check is not taken again, since every probe of the session fails it", async () => {
  const session = probeInvalidity(holdJudgement("clean"), {
    ...CLOCKS_PASS,
    sessionClockPassed: false,
  });
  const alsoDropped = probeInvalidity(holdJudgement("reordered"), {
    ...CLOCKS_PASS,
    sessionClockPassed: false,
  });
  const frameClock = probeInvalidity(holdJudgement("clean"), {
    ...CLOCKS_PASS,
    frameClockPassed: false,
  });
  const invalid = [session, alsoDropped, frameClock, session];
  const ran = await takeSamples(
    { warmups: 0, judged: 3, retakes: 3, valueMs: () => 120 },
    (n) =>
      Promise.resolve({
        n,
        verdict: "INVALID_RUN" as const,
        invalid: invalid[n] ?? [],
      }),
  );
  // The frame clock is read per probe, so probe 2 is taken again; the
  // session's clock is read once at load, so probes 0, 1 and 3 are judged.
  assert.deepEqual(ran.retaken, [
    {
      n: 2,
      slot: 2,
      reasons: ["the probe's frame clock check failed"],
      valueMs: 120,
    },
  ]);
  assert.deepEqual(
    ran.judged.map((sample) => sample.n),
    [0, 1, 3],
  );
});

test("a sample taken again reports the first expected frame when a dropped-frame signal stopped its match", () => {
  // The matching frame is stamped 3.5 ms before the one ahead of it, so the
  // judge has no match to give.
  const reordered = [frame(75, 1_003.5, A, A), frame(76, 1_000, B, B)];
  const stopped = judgeStablePaint(reordered, "grid", B, {
    fromMs: 900,
  });
  assert.equal(stopped.matchMs, null);
  assert.equal(reachedMs(stopped, reordered, "grid", B, 900), 100);
  assert.equal(reachedMs(stopped, reordered, "full", B, 900), 100);
  // A frame before the sample's start does not count.
  assert.equal(reachedMs(stopped, reordered, "grid", B, 1_001), null);
  // With a match, the match is what it reached.
  const clean = holdJudgement("clean");
  assert.equal(clean.matchMs, 1_000);
  assert.equal(reachedMs(clean, reordered, "grid", C, 900), 100);
  // A probe whose grid never settled, and a cold load with no T_nav.
  assert.equal(reachedMs(null, reordered, "grid", null, 900), null);
  assert.equal(reachedMs(clean, reordered, "grid", B, null), null);
  assert.equal(
    reachedMs(stopped, [frame(75, 1_003.5, A, A)], "grid", B, 900),
    null,
  );
});

test("a sample with no match reports a reached state only when a dropped-frame signal stopped the match", () => {
  // A sample that ended with no match and no signal, judged over fewer
  // frames than it recorded: a cold sample's deadline passed, and a frame
  // captured after it shows the expected state. It reached nothing in time.
  const frames = [frame(75, 1_000, B, B), frame(76, 1_500, C, C)];
  const unmatched = { matchMs: null, droppedFrameSignals: [] };
  assert.equal(reachedMs(unmatched, frames, "grid", B, 900), null);
  // The same frames under a dropped-frame signal report the expected state.
  const signalled = {
    matchMs: null,
    droppedFrameSignals: [{ kind: "screencast-hidden" as const }],
  };
  assert.equal(reachedMs(signalled, frames, "grid", B, 900), 100);
});

test("the reached state is the earliest-stamped expected frame, not the first one received", () => {
  const signalled = {
    matchMs: null,
    droppedFrameSignals: [{ kind: "screencast-hidden" as const }],
  };
  const reordered = [
    frame(74, 1_002, B, B),
    frame(75, 1_003.5, A, A),
    frame(76, 1_000, B, B),
  ];
  assert.equal(reachedMs(signalled, reordered, "grid", B, 900), 100);
  // A frame stamped before `fromMs` is not a candidate, wherever it arrived.
  assert.equal(reachedMs(signalled, reordered, "grid", B, 1_001), 1);
});

test("a load that asked for its artifact again is labelled by its marks", () => {
  const marks = (names: readonly string[]) =>
    names.map((name, index) => ({ name, startTime: index }));
  assert.deepEqual(
    artifactRetryMarks(
      marks([
        "calque:activate",
        "calque:artifact:request",
        "calque:artifact:retry:1",
        "calque:artifact:retry:2",
        "calque:artifact:response",
        "calque:artifact:replan",
        "calque:artifact:retry",
        "calque:artifact:replanned",
      ]),
    ),
    [
      "calque:artifact:retry:1",
      "calque:artifact:retry:2",
      "calque:artifact:replan",
    ],
  );
  assert.deepEqual(artifactRetryMarks(marks(["calque:ready"])), []);
});

test("a trace summary counts GC and renderer tasks over 50 ms per thread", () => {
  const summary = summarizeTrace({
    traceEvents: [
      { ph: "M", name: "process_name", pid: 1, args: { name: "Renderer" } },
      { ph: "M", name: "process_name", pid: 2, args: { name: "Browser" } },
      {
        ph: "M",
        name: "thread_name",
        pid: 1,
        tid: 10,
        args: { name: "CrRendererMain" },
      },
      { ph: "X", name: "RunTask", pid: 1, tid: 10, ts: 0, dur: 60_000 },
      { ph: "X", name: "RunTask", pid: 1, tid: 10, ts: 0, dur: 50_000 },
      { ph: "X", name: "RunTask", pid: 2, tid: 5, ts: 0, dur: 90_000 },
      {
        ph: "B",
        name: "ThreadControllerImpl::RunTask",
        pid: 1,
        tid: 11,
        ts: 100,
      },
      {
        ph: "E",
        name: "ThreadControllerImpl::RunTask",
        pid: 1,
        tid: 11,
        ts: 70_100,
      },
      { ph: "X", name: "MinorGC", pid: 1, tid: 10, ts: 0, dur: 1_000 },
      { ph: "X", name: "MajorGC", pid: 1, tid: 10, ts: 0, dur: 3_000 },
    ],
  });
  assert.deepEqual(summary.gc, { minor: 1, major: 1, totalMs: 4 });
  assert.deepEqual(summary.longTaskThreads, [
    {
      process: "Renderer",
      thread: "1:11",
      tasksOver50Ms: 1,
      longestTaskMs: 70,
    },
    {
      process: "Renderer",
      thread: "CrRendererMain",
      tasksOver50Ms: 1,
      longestTaskMs: 60,
    },
  ]);
  assert.equal(summary.events, 10);
  assert.equal(summarizeTrace([]).gc, null);
  assert.throws(() => summarizeTrace({}), /no event array/u);
});

// Frames ------------------------------------------------------------------------

test("an element's rectangle becomes the integer region covering it", () => {
  assert.deepEqual(
    roiFromRect({ left: 10.5, top: 20.2, right: 100.1, bottom: 50.9 }),
    { x: 10, y: 20, width: 91, height: 31 },
  );
  assert.deepEqual(
    roiFromRect({ left: -5, top: 800, right: 2_000, bottom: 1_000 }),
    { x: 0, y: 800, width: 1_600, height: 100 },
  );
  assert.throws(
    () => roiFromRect({ left: 10, top: 10, right: 10, bottom: 20 }),
    /no pixels/u,
  );
  assert.throws(
    () => roiFromRect({ left: 0, top: 950, right: 10, bottom: 990 }),
    /no pixels/u,
  );
});

test("T_present is the first expected frame, and the hold window can contradict it", () => {
  const frames = [
    frame(0, 900, B),
    frame(1, 1_000, A),
    frame(2, 1_100, B),
    frame(3, 1_200, B),
    frame(4, 3_200, C),
  ];
  const clean = judgeStablePaint(frames, "full", B, {
    navigationMs: 950,
  });
  assert.equal(clean.matchMs, 1_100);
  assert.equal(clean.contradiction, null);
  assert.equal(clean.holdWindowFrameCount, 1);
  assert.equal(clean.framesJudged, 5);

  const contradicted = judgeStablePaint(
    [...frames.slice(0, 3), frame(3, 3_100, C)],
    "full",
    B,
    { navigationMs: 950 },
  );
  assert.equal(contradicted.matchMs, 1_100);
  assert.equal(
    contradicted.contradiction?.contradictingTimestampMilliseconds,
    3_100,
  );

  const late = judgeStablePaint(frames, "full", B, { fromMs: 1_150 });
  assert.equal(late.matchMs, 1_200);
  assert.equal(late.framesJudged, 2);

  assert.equal(
    judgeStablePaint(frames, "full", C, { navigationMs: 950 }).matchMs,
    3_200,
  );
  assert.equal(
    judgeStablePaint(frames.slice(0, 2), "full", B, {
      navigationMs: 950,
    }).matchMs,
    null,
  );
});

test("a capture signal keeps the match readable and invalidates the sample", () => {
  const judgement = judgeStablePaint(
    [frame(0, 1_000, B), frame(1, 1_100, B)],
    "full",
    B,
    { signals: [{ kind: "ack-failed", sessionId: 1 }] },
  );
  assert.equal(judgement.matchMs, 1_000);
  assert.equal(judgement.contradiction, null);
  assert.deepEqual(judgement.droppedFrameSignals, [
    { kind: "ack-failed", sessionId: 1 },
  ]);
  const reordered = judgeStablePaint(
    [frame(0, 1_000, A), frame(1, 1_000, B)],
    "full",
    B,
  );
  assert.equal(reordered.matchMs, null);
  assert.equal(
    reordered.droppedFrameSignals[0]?.kind,
    "non-increasing-timestamp",
  );
  assert.throws(
    () => judgeStablePaint([frame(0, 1_000, A, null)], "grid", A),
    /no grid hash/u,
  );
});

test("a probe's region settles only on a new state held for the quiet period", () => {
  const frames = [
    frame(0, 900, A, A),
    frame(1, 1_050, A, C),
    frame(2, 1_080, A, B),
  ];
  assert.deepEqual(settle(frames.slice(0, 1), "grid", A, 1_000, 9_000, 2_250), {
    settled: false,
    changed: false,
  });
  assert.deepEqual(settle(frames, "grid", A, 1_000, 3_000, 2_250), {
    settled: false,
    changed: true,
  });
  assert.deepEqual(settle(frames, "grid", A, 1_000, 3_330, 2_250), {
    settled: true,
    reference: B,
    lastChangeMs: 1_080,
  });
  assert.deepEqual(
    settle([...frames, frame(3, 1_100, A, A)], "grid", A, 1_000, 9_000, 2_250),
    { settled: false, changed: true },
  );
});

test("a screencast frame's wall-clock stamp meets host readings only once mapped", () => {
  // Shaped like a real run: the document request read 79,373.719309 s on
  // Chrome's monotonic clock, and frames were stamped about 1.79e9 s.
  const offset = 79_373.719_309 - 1_790_459_462.24;
  const epochSeconds = 1_790_459_462.253;
  const mapped: HashedFrame = {
    ...frame(0, 0, B),
    epochSeconds,
    timestampMs: mapEpochSecondsToMonotonicMilliseconds(epochSeconds, offset),
    receivedMs: 79_373_745,
  };
  assert.ok(Math.abs(mapped.timestampMs - 79_373_732.309) < 0.01);
  const check = frameClockCheck([mapped]);
  assert.equal(check.passed, true);
  assert.equal(check.frames, 1);
  assert.ok(Math.abs((check.minTransitMs ?? 0) - 12.691) < 0.01);

  const unmapped: HashedFrame = {
    ...mapped,
    timestampMs: epochSeconds * 1_000,
  };
  assert.equal(frameClockCheck([unmapped]).passed, false);
  assert.equal(frameClockCheck([mapped, unmapped]).passed, false);
  assert.equal(
    frameClockCheck([{ ...mapped, receivedMs: mapped.timestampMs - 1 }]).passed,
    false,
  );
  assert.equal(
    frameClockCheck([
      {
        ...mapped,
        receivedMs: mapped.timestampMs + FRAME_TRANSIT_LIMIT_MS + 1,
      },
    ]).passed,
    false,
  );
  assert.deepEqual(frameClockCheck([]), {
    frames: 0,
    minTransitMs: null,
    maxTransitMs: null,
    passed: false,
  });

  const tBefore = 79_373_700;
  assert.deepEqual(
    settle([mapped], "grid", A, tBefore, tBefore + 2_300, 2_250),
    {
      settled: true,
      reference: B,
      lastChangeMs: mapped.timestampMs,
    },
  );
  // The unmapped stamp is decades ahead of the host's clock, so the quiet
  // period never elapses.
  assert.deepEqual(
    settle([unmapped], "grid", A, tBefore, tBefore + 30_000, 2_250),
    { settled: false, changed: true },
  );
});

function solidPng(split: number, height = 900): Buffer {
  const width = 1_600;
  const raw = Buffer.alloc((width * 4 + 1) * height);
  for (let y = 0; y < height; y += 1) {
    const row = y * (width * 4 + 1);
    for (let x = 0; x < width; x += 1) {
      const pixel = row + 1 + x * 4;
      raw[pixel] = x < split ? 255 : 0;
      raw[pixel + 2] = x < split ? 0 : 255;
      raw[pixel + 3] = 255;
    }
  }
  const chunk = (type: string, data: Buffer) => {
    const length = Buffer.alloc(4);
    length.writeUInt32BE(data.length);
    const body = Buffer.concat([Buffer.from(type, "ascii"), data]);
    const crc = Buffer.alloc(4);
    crc.writeUInt32BE(crc32(body));
    return Buffer.concat([length, body, crc]);
  };
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width, 0);
  header.writeUInt32BE(height, 4);
  header[8] = 8;
  header[9] = 6;
  return Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", header),
    chunk("IDAT", deflateSync(raw)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

test("the worker pool hashes a frame exactly as the main thread would", async () => {
  const png = solidPng(800);
  const grid = { x: 700, y: 100, width: 200, height: 50 };
  const direct = hashFrame(png, grid);
  const decoded = decodePng(png);
  assert.equal(direct.full, hashDecodedRoi(decoded, FULL_VIEWPORT_ROI));
  assert.equal(direct.grid, hashDecodedRoi(decoded, grid));
  assert.equal(
    direct.pngSha256,
    createHash("sha256").update(png).digest("hex"),
  );
  // A refused picture names its source, where `hashDecodedRoi` calls every
  // picture a screencast PNG.
  const short = solidPng(800, 813);
  for (const source of [
    "screenshot",
    "restarted screencast frame",
    "screencast frame",
  ] as const)
    assert.throws(
      () => hashFrame(short, null, source),
      new Error(
        `${source} PNG 1600x813 does not scale exactly from the frozen 1600x900 viewport`,
      ),
    );
  assert.throws(() => hashFrame(short, null), /^Error: screencast frame PNG/);
  // The refusal carries its size as data, for the validation's condition.
  assert.throws(
    () => hashFrame(short, null, "screenshot"),
    (error: unknown) => {
      assert.deepEqual(instrumentFault(error), {
        kind: "picture-size",
        source: "screenshot",
        width: 1_600,
        height: 813,
      });
      return true;
    },
  );
  const other = hashFrame(solidPng(801), grid);
  assert.notEqual(other.full, direct.full);
  assert.notEqual(other.grid, direct.grid);
  assert.equal(hashFrame(png, null).grid, null);

  const hasher = new FrameHasher(2);
  try {
    await assert.rejects(
      hasher.hash(short.toString("base64"), null, "screenshot"),
      /^Error: frame hashing failed: screenshot PNG 1600x813/,
    );
    // A worker's error loses its own properties; the reply carries the fault.
    await assert.rejects(
      hasher.hash(short.toString("base64"), null, "screenshot"),
      (error: unknown) => {
        assert.deepEqual(instrumentFault(error), {
          kind: "picture-size",
          source: "screenshot",
          width: 1_600,
          height: 813,
        });
        return true;
      },
    );
    const pooled = await Promise.all([
      hasher.hash(png.toString("base64"), grid),
      hasher.hash(png.toString("base64"), null),
    ]);
    assert.deepEqual(pooled[0], direct);
    assert.equal(pooled[1]?.grid, null);
    await assert.rejects(
      hasher.hash(Buffer.from("not a png").toString("base64"), null),
      (error: unknown) => {
        assert.match(
          String(error),
          /frame hashing failed: screencast payload is not a PNG/u,
        );
        assert.equal(instrumentFault(error), null);
        return true;
      },
    );
    assert.equal(hasher.pending, 0);
  } finally {
    await hasher.close();
  }
});

// Runner diagnostics ----------------------------------------------------------------

test("a record keeps a URL's path and drops its query, which can carry a capability", () => {
  assert.equal(
    redactedUrl("http://127.0.0.1:9/v1/artifacts/a33f?cap=secret&x=1"),
    "http://127.0.0.1:9/v1/artifacts/a33f?<redacted>",
  );
  assert.equal(
    redactedUrl("http://127.0.0.1:9/assets/index.js"),
    "http://127.0.0.1:9/assets/index.js",
  );
  assert.equal(
    redactedUrl("http://127.0.0.1:9/?"),
    "http://127.0.0.1:9/?<redacted>",
  );
});

test("a frame stamped after Chrome's wall clock moved maps through the offset moved with it", () => {
  const cdp = new EventEmitter();
  const page = "http://127.0.0.1:9/";
  const ledger = new NetworkLedger(
    cdp as unknown as ConstructorParameters<typeof NetworkLedger>[0],
    page,
  );
  assert.equal(ledger.frameOffsetSeconds(), null);
  cdp.emit("Network.requestWillBeSent", {
    requestId: "1",
    request: { url: page, method: "GET" },
    type: "Document",
    initiator: { type: "other" },
    timestamp: 100,
    wallTime: 1_700_000_000,
  });
  // 60 s on, Chrome's wall clock runs 12 ms ahead of its monotonic clock.
  const stamped = 1_700_000_060.012;
  const atLoad = ledger.frameOffsetSeconds();
  assert.ok(atLoad !== null);
  assert.ok(
    Math.abs(
      mapEpochSecondsToMonotonicMilliseconds(stamped, atLoad) - 160_012,
    ) < 0.01,
  );
  ledger.wallShiftMs = 12;
  const moved = ledger.frameOffsetSeconds();
  assert.ok(moved !== null);
  assert.ok(
    Math.abs(mapEpochSecondsToMonotonicMilliseconds(stamped, moved) - 160_000) <
      0.01,
  );
  assert.equal(ledger.monotonicMinusEpochSeconds, 100 - 1_700_000_000);
});

test("the network ledger counts each request's wire bytes once, by the route its end leaves", () => {
  const cdp = new EventEmitter();
  const page = "http://127.0.0.1:9/";
  const ledger = new NetworkLedger(
    cdp as unknown as ConstructorParameters<typeof NetworkLedger>[0],
    page,
  );
  const sent = (
    requestId: string,
    url: string,
    type: string,
    initiator: string,
  ) =>
    cdp.emit("Network.requestWillBeSent", {
      requestId,
      request: { url, method: "GET" },
      type,
      initiator: { type: initiator },
      timestamp: 100,
      wallTime: 1_700_000_000,
    });
  sent("1", page, "Document", "other");
  sent(
    "2",
    "http://127.0.0.1:8/v1/artifacts/a33f?cap=secret",
    "Fetch",
    "parser",
  );
  sent("3", "http://127.0.0.1:8/v1/changes?cursor=9", "Fetch", "script");
  sent("4", "http://127.0.0.1:9/assets/index.js", "Script", "parser");
  sent(
    "5",
    "http://127.0.0.1:8/v1/artifacts/b44e?cap=secret",
    "Fetch",
    "script",
  );
  cdp.emit("Network.responseReceived", {
    requestId: "1",
    timestamp: 100.125,
    response: {
      status: 200,
      protocol: "h2",
      headers: { "Content-Length": "1000", "Cache-Control": "no-store" },
    },
  });
  cdp.emit("Network.responseReceived", {
    requestId: "2",
    timestamp: 100.25,
    response: {
      status: 200,
      protocol: "http/1.1",
      headers: { "content-encoding": "zstd", "content-length": " 21000 " },
    },
  });
  cdp.emit("Network.responseReceived", {
    requestId: "3",
    timestamp: 100.25,
    response: { status: 200, headers: {} },
  });
  cdp.emit("Network.responseReceived", {
    requestId: "4",
    timestamp: 100.0625,
    response: { status: 200, headers: { "content-length": "12x" } },
  });
  cdp.emit("Network.responseReceived", {
    requestId: "5",
    timestamp: 100.375,
    response: {
      status: 200,
      headers: { "Content-Encoding": "zstd", "Content-Length": "9000" },
    },
  });
  cdp.emit("Network.dataReceived", {
    requestId: "1",
    timestamp: 100.25,
    dataLength: 1_000,
    encodedDataLength: 0,
  });
  cdp.emit("Network.dataReceived", {
    requestId: "2",
    timestamp: 100.25,
    dataLength: 300_000,
    encodedDataLength: 20_000,
  });
  cdp.emit("Network.dataReceived", {
    requestId: "2",
    timestamp: 100.5,
    dataLength: 18_000,
    encodedDataLength: 1_000,
  });
  cdp.emit("Network.dataReceived", {
    requestId: "3",
    timestamp: 100.25,
    dataLength: 40,
    encodedDataLength: 40,
  });
  // Like the artifact, which the page reads as a stream: the body arrived,
  // but Chrome reported the request canceled and held the tail of its encoded
  // bytes for a finish that never came.
  cdp.emit("Network.dataReceived", {
    requestId: "5",
    timestamp: 100.5,
    dataLength: 31_000,
    encodedDataLength: 8_900,
  });
  cdp.emit("Network.dataReceived", {
    requestId: "unknown",
    timestamp: 100.5,
    dataLength: 5,
    encodedDataLength: 5,
  });
  cdp.emit("Network.loadingFinished", {
    requestId: "1",
    timestamp: 100.375,
    encodedDataLength: 1_200,
  });
  cdp.emit("Network.loadingFinished", {
    requestId: "2",
    timestamp: 100.75,
    encodedDataLength: 21_350,
  });
  cdp.emit("Network.loadingFailed", {
    requestId: "4",
    timestamp: 100.125,
    errorText: "net::ERR_ABORTED",
    canceled: true,
  });
  cdp.emit("Network.loadingFailed", {
    requestId: "5",
    timestamp: 100.625,
    errorText: "net::ERR_ABORTED",
    canceled: true,
  });
  assert.equal(ledger.documentTimestampMs, 100_000);
  assert.equal(ledger.monotonicMinusEpochSeconds, 100 - 1_700_000_000);
  assert.equal(ledger.documentCacheControl, "no-store");
  const totals = ledger.totals();
  assert.deepEqual(
    { ...totals, byRequest: undefined },
    {
      requests: 5,
      unfinished: 3,
      failed: 2,
      wireBytes: 31_490,
      finishedWireBytes: 1_200 + 21_350,
      unfinishedWireBytes: 40 + 8_900,
      declaredBodyBytes: 1_000 + 21_000 + 9_000,
      undeclaredBodies: 2,
      decodedBodyBytes: 1_000 + 318_000 + 40 + 31_000,
      byRequest: undefined,
    },
  );
  assert.deepEqual(totals.byRequest, [
    {
      url: page,
      method: "GET",
      type: "Document",
      initiator: "other",
      status: 200,
      protocol: "h2",
      contentEncoding: null,
      contentLength: 1_000,
      encoded: 1_200,
      chunkEncoded: 0,
      decoded: 1_000,
      state: "finished",
      failure: null,
      sentMs: 100_000,
      respondedMs: 100_125,
      lastDataMs: 100_250,
      endedMs: 100_375,
    },
    {
      url: "http://127.0.0.1:8/v1/artifacts/a33f?<redacted>",
      method: "GET",
      type: "Fetch",
      initiator: "parser",
      status: 200,
      protocol: "http/1.1",
      contentEncoding: "zstd",
      contentLength: 21_000,
      encoded: 21_350,
      chunkEncoded: 21_000,
      decoded: 318_000,
      state: "finished",
      failure: null,
      sentMs: 100_000,
      respondedMs: 100_250,
      lastDataMs: 100_500,
      endedMs: 100_750,
    },
    {
      url: "http://127.0.0.1:8/v1/changes?<redacted>",
      method: "GET",
      type: "Fetch",
      initiator: "script",
      status: 200,
      protocol: null,
      contentEncoding: null,
      contentLength: null,
      encoded: 0,
      chunkEncoded: 40,
      decoded: 40,
      state: "pending",
      failure: null,
      sentMs: 100_000,
      respondedMs: 100_250,
      lastDataMs: 100_250,
      endedMs: null,
    },
    {
      url: "http://127.0.0.1:9/assets/index.js",
      method: "GET",
      type: "Script",
      initiator: "parser",
      status: 200,
      protocol: null,
      contentEncoding: null,
      contentLength: null,
      encoded: 0,
      chunkEncoded: 0,
      decoded: 0,
      state: "failed",
      failure: "canceled: net::ERR_ABORTED",
      sentMs: 100_000,
      respondedMs: 100_062.5,
      lastDataMs: null,
      endedMs: 100_125,
    },
    {
      url: "http://127.0.0.1:8/v1/artifacts/b44e?<redacted>",
      method: "GET",
      type: "Fetch",
      initiator: "script",
      status: 200,
      protocol: null,
      contentEncoding: "zstd",
      contentLength: 9_000,
      encoded: 0,
      chunkEncoded: 8_900,
      decoded: 31_000,
      state: "failed",
      failure: "canceled: net::ERR_ABORTED",
      sentMs: 100_000,
      respondedMs: 100_375,
      lastDataMs: 100_500,
      endedMs: 100_625,
    },
  ]);
});

test("the runner's stall record sees a blocked event loop and when it ended", async () => {
  const stalls = new RunnerStalls();
  stalls.start();
  await new Promise((resolveDelay) => setTimeout(resolveDelay, 40));
  const blockStartedMs = hrtimeMs();
  while (hrtimeMs() - blockStartedMs < 80);
  await new Promise((resolveDelay) => setTimeout(resolveDelay, 40));
  const summary = stalls.stop();
  assert.equal(stalls.stop(), summary);
  const { eventLoopDelay } = summary;
  assert.equal(eventLoopDelay.resolutionMs, 10);
  assert.ok(eventLoopDelay.ticks > 0);
  assert.ok((eventLoopDelay.maxMs ?? 0) >= 80, String(eventLoopDelay.maxMs));
  const listed = eventLoopDelay.stalls.filter(
    (stall) => stall.endedMs >= blockStartedMs + 80,
  );
  assert.ok(
    listed.some((stall) => stall.intervalMs >= 80),
    JSON.stringify(eventLoopDelay.stalls),
  );
  assert.equal(eventLoopDelay.stallCount, eventLoopDelay.stalls.length);
  assert.equal(summary.gc.longCount, summary.gc.long.length);
  assert.ok(summary.gc.count >= summary.gc.long.length);
});

test("a screenshot calibrates against the reference frame by full and grid hash", async () => {
  const grid = { x: 100, y: 100, width: 200, height: 50 };
  const reference = hashFrame(solidPng(800), grid);
  const expected = { full: reference.full, grid: reference.grid };
  const answering = (png: Buffer) => ({
    send: (method: string) => {
      assert.equal(method, "Page.captureScreenshot");
      return Promise.resolve({ data: png.toString("base64") });
    },
  });
  assert.deepEqual(
    await screenshotCalibration(
      answering(solidPng(800)) as never,
      grid,
      expected,
    ),
    { matchesFull: true, matchesGrid: true, error: null },
  );
  // Columns 800 to 899 change colour outside the grid.
  assert.deepEqual(
    await screenshotCalibration(
      answering(solidPng(900)) as never,
      grid,
      expected,
    ),
    { matchesFull: false, matchesGrid: true, error: null },
  );
  assert.deepEqual(
    await screenshotCalibration(answering(solidPng(800)) as never, grid, {
      full: reference.full,
      grid: null,
    }),
    { matchesFull: true, matchesGrid: null, error: null },
  );
  const failing = {
    send: () => Promise.reject(new Error("Target closed")),
  };
  assert.deepEqual(
    await screenshotCalibration(failing as never, grid, expected),
    { matchesFull: null, matchesGrid: null, error: "Target closed" },
  );
});

// Window insets ---------------------------------------------------------------

/**
 * A page whose screenshots come from `shots` in turn, an error as Chrome's
 * refusal, in a window 900 rows high.
 */
function windowedPage(shots: (Buffer | Error)[]) {
  const sent: { method: string; params: unknown }[] = [];
  return {
    sent,
    send: (method: string, params?: unknown) => {
      sent.push({ method, params });
      if (method === "Page.captureScreenshot") {
        const shot = shots.shift();
        if (shot === undefined)
          return Promise.reject(new Error("no screenshot left"));
        return shot instanceof Error
          ? Promise.reject(shot)
          : Promise.resolve({ data: shot.toString("base64") });
      }
      if (method === "Browser.getWindowForTarget")
        return Promise.resolve({
          windowId: 7,
          bounds: { left: 22, top: 22, width: 1_600, height: 900 },
        });
      return Promise.resolve({});
    },
  };
}

test("a window that keeps rows out of the capture grows by them, and a second screenshot checks it", async () => {
  const page = windowedPage([solidPng(800, 813), solidPng(800)]);
  assert.equal(await measureWindowInset(page as never), 87);
  assert.deepEqual(page.sent, [
    { method: "Page.captureScreenshot", params: { format: "png" } },
    { method: "Browser.getWindowForTarget", params: undefined },
    {
      method: "Browser.setWindowBounds",
      params: { windowId: 7, bounds: { height: 987 } },
    },
    { method: "Page.captureScreenshot", params: { format: "png" } },
  ]);
});

test("a window whose capture has the viewport's size is not resized, and is still checked", async () => {
  const page = windowedPage([solidPng(800), solidPng(800)]);
  assert.equal(await measureWindowInset(page as never), 0);
  assert.deepEqual(
    page.sent.map((entry) => entry.method),
    ["Page.captureScreenshot", "Page.captureScreenshot"],
  );
  const unmoved = windowedPage([solidPng(800)]);
  await fitWindow(unmoved as never, 0);
  assert.deepEqual(
    unmoved.sent.map((entry) => entry.method),
    ["Page.captureScreenshot"],
  );
  const grown = windowedPage([solidPng(800)]);
  await fitWindow(grown as never, 87);
  assert.deepEqual(grown.sent, [
    { method: "Browser.getWindowForTarget", params: undefined },
    {
      method: "Browser.setWindowBounds",
      params: { windowId: 7, bounds: { height: 987 } },
    },
    { method: "Page.captureScreenshot", params: { format: "png" } },
  ]);
});

test("a session's window that still captures short after growing is refused before its navigation", async () => {
  await assert.rejects(
    fitWindow(windowedPage([solidPng(800, 813)]) as never, 87),
    /captures at 1600x813 after its window grew by 87 rows/,
  );
  await assert.rejects(
    fitWindow(windowedPage([solidPng(800, 813)]) as never, 0),
    /captures at 1600x813 after its window grew by 0 rows/,
  );
});

test("a screenshot Chrome refuses is asked for again, three times at most", async () => {
  const refusal = () =>
    new Error(
      "cdpSession.send: Protocol error (Page.captureScreenshot): Unable to capture screenshot",
    );
  const twice = windowedPage([refusal(), refusal(), solidPng(800)]);
  await fitWindow(twice as never, 0);
  assert.equal(twice.sent.length, 3);
  const thrice = windowedPage([refusal(), refusal(), refusal(), solidPng(800)]);
  await assert.rejects(fitWindow(thrice as never, 0), (error: unknown) => {
    assert.match(
      String(error),
      /Chrome refused 3 screenshots of the page in a row: .*Unable to capture screenshot/,
    );
    assert.deepEqual(instrumentFault(error), {
      kind: "screenshot-refused",
      attempts: 3,
    });
    return true;
  });
  assert.equal(thrice.sent.length, 3);
  const measured = windowedPage([refusal(), solidPng(800, 813), solidPng(800)]);
  assert.equal(await measureWindowInset(measured as never), 87);
  // Any other failure is thrown as it came, with no fault on it.
  const targetClosed = new Error("Target closed");
  const closed = windowedPage([targetClosed, solidPng(800)]);
  await assert.rejects(fitWindow(closed as never, 0), (error: unknown) => {
    assert.equal(error, targetClosed);
    assert.equal("fault" in targetClosed, false);
    return true;
  });
  assert.equal(closed.sent.length, 1);
});

test("a refused screenshot is asked for again only after a wait", async () => {
  const refusal = () =>
    new Error(
      "cdpSession.send: Protocol error (Page.captureScreenshot): Unable to capture screenshot",
    );
  const started = performance.now();
  await fitWindow(
    windowedPage([refusal(), refusal(), solidPng(800)]) as never,
    0,
  );
  // Two waits of 100 ms; a timer may fire up to a millisecond early.
  assert.ok(performance.now() - started >= 198);
});

test("a session whose browser refuses every screenshot gets one fresh browser, and nothing else is retried", async () => {
  const refusal = () =>
    new Error(
      "cdpSession.send: Protocol error (Page.captureScreenshot): Unable to capture screenshot",
    );
  const refusing = () => windowedPage([refusal(), refusal(), refusal()]);
  const launcher = (pages: ReturnType<typeof windowedPage>[]) => {
    const launched: ReturnType<typeof windowedPage>[] = [];
    return {
      launched,
      launch: async () => {
        const page = pages.shift();
        if (page === undefined) throw new Error("no browser left");
        launched.push(page);
        await fitWindow(page as never, 0);
        return page;
      },
    };
  };

  const first = launcher([windowedPage([solidPng(800)]), refusing()]);
  const kept = await relaunchOnRefusal(first.launch);
  assert.equal(kept.launches, 1);
  assert.equal(first.launched.length, 1);

  const second = launcher([refusing(), windowedPage([solidPng(800)])]);
  const relaunched = await relaunchOnRefusal(second.launch);
  assert.equal(relaunched.launches, 2);
  assert.equal(relaunched.value, second.launched[1]);
  assert.equal(second.launched[0]?.sent.length, 3);

  const neither = launcher([
    refusing(),
    refusing(),
    windowedPage([solidPng(800)]),
  ]);
  await assert.rejects(relaunchOnRefusal(neither.launch), (error: unknown) => {
    assert.deepEqual(instrumentFault(error), {
      kind: "screenshot-refused",
      attempts: 3,
    });
    return true;
  });
  assert.equal(neither.launched.length, 2);

  // A wrong size, or any error but a refusal, ends the session at once.
  const short = launcher([
    windowedPage([solidPng(800, 813)]),
    windowedPage([solidPng(800)]),
  ]);
  await assert.rejects(
    relaunchOnRefusal(short.launch),
    /captures at 1600x813 after its window grew by 0 rows/,
  );
  assert.equal(short.launched.length, 1);
  const targetClosed = new Error("Target closed");
  const closed = launcher([
    windowedPage([targetClosed]),
    windowedPage([solidPng(800)]),
  ]);
  await assert.rejects(relaunchOnRefusal(closed.launch), (error: unknown) => {
    assert.equal(error, targetClosed);
    return true;
  });
  assert.equal(closed.launched.length, 1);
});

test("a capture that growing the window does not fix, or no inset explains, is refused", async () => {
  await assert.rejects(
    measureWindowInset(
      windowedPage([solidPng(800, 813), solidPng(800, 813)]) as never,
    ),
    /captures at 1600x813 after its window grew by 87 rows/,
  );
  await assert.rejects(
    measureWindowInset(windowedPage([solidPng(800, 901)]) as never),
    /captures at 1600x901, which no window inset explains/,
  );
});

test("an instrument fault is read from the error's fault field, never from its message", () => {
  const sizeMessage =
    "screenshot PNG 1600x813 does not scale exactly from the frozen 1600x900 viewport";
  assert.equal(instrumentFault(new Error(sizeMessage)), null);
  assert.equal(
    instrumentFault(
      new Error(
        "Chrome refused 3 screenshots of the page in a row: Unable to capture screenshot",
      ),
    ),
    null,
  );
  assert.equal(
    instrumentFault(
      Object.assign(new Error("x"), { fault: { kind: "other" } }),
    ),
    null,
  );
  assert.equal(
    instrumentFault(Object.assign(new Error("x"), { fault: "picture-size" })),
    null,
  );
  assert.equal(instrumentFault(sizeMessage), null);
  assert.equal(instrumentFault(null), null);
  assert.deepEqual(
    instrumentFault(
      Object.assign(new Error("x"), {
        fault: { kind: "screenshot-refused", attempts: 3 },
      }),
    ),
    { kind: "screenshot-refused", attempts: 3 },
  );
});

// A stopped run ---------------------------------------------------------------------

test("a signalled run stops what it started, then writes its summary with no verdict, and exits 130", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bench-stopped-"));
  try {
    mkdirSync(join(directory, "samples"));
    for (const name of [
      "prepare-0",
      "reference-0",
      "warmup-0",
      "cold-0",
      "cold-1",
      "cold-2",
      "traced-0",
    ])
      writeFileSync(join(directory, "samples", `${name}.json`), "{}\n");
    for (const signal of ["SIGINT", "SIGTERM"] as const) {
      const steps: string[] = [];
      const exited = new Promise<number>((resolveExit) => {
        runStop(
          directory,
          (aborted) => ({
            runId: "stopped",
            result: null,
            aborted,
          }),
          () => {
            steps.push(
              `cleanup, summary ${existsSync(join(directory, "summary.json")) ? "present" : "absent"}`,
            );
            return Promise.resolve();
          },
          (code) => {
            steps.push("exit");
            resolveExit(code);
          },
        ).onSignal(signal);
      });
      assert.equal(await exited, 130);
      assert.deepEqual(steps, ["cleanup, summary absent", "exit"]);
      const written = readFileSync(join(directory, "summary.json"), "utf8");
      assert.deepEqual(JSON.parse(written), {
        runId: "stopped",
        result: null,
        aborted: signal,
        coldSamplesWritten: 3,
      });
      // Nothing judged the samples on disk.
      assert.doesNotMatch(written, /verdict/u);
      rmSync(join(directory, "summary.json"));
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

/** A cleanup that runs until the test releases it, and counts its starts. */
function heldCleanup(): {
  readonly cleanup: () => Promise<void>;
  readonly starts: () => number;
  readonly release: () => void;
} {
  let starts = 0;
  let release = () => {};
  const held = new Promise<void>((resolveHeld) => {
    release = resolveHeld;
  });
  return {
    cleanup: () => {
      starts += 1;
      return held;
    },
    starts: () => starts,
    release: () => release(),
  };
}

test("a signal while the stop runs neither restarts it nor exits, and the run's own end waits for it", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bench-stopping-"));
  try {
    mkdirSync(join(directory, "samples"));
    const held = heldCleanup();
    const exits: number[] = [];
    const stop = runStop(
      directory,
      (aborted) => ({ runId: "stopping", aborted }),
      held.cleanup,
      (code) => exits.push(code),
    );
    stop.onSignal("SIGINT");
    stop.onSignal("SIGINT");
    stop.onSignal("SIGTERM");
    const finished = stop.finish("the page exited");
    await sleep(10);
    assert.equal(held.starts(), 1);
    assert.deepEqual(exits, []);
    assert.equal(existsSync(join(directory, "summary.json")), false);
    held.release();
    await finished;
    assert.equal(held.starts(), 1);
    assert.deepEqual(exits, [130]);
    assert.deepEqual(
      JSON.parse(readFileSync(join(directory, "summary.json"), "utf8")),
      { runId: "stopping", aborted: "SIGINT", coldSamplesWritten: 0 },
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a run that ends by itself writes what aborted it, and a signal during its stop changes nothing", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bench-finishing-"));
  try {
    mkdirSync(join(directory, "samples"));
    const held = heldCleanup();
    const exits: number[] = [];
    const stop = runStop(
      directory,
      (aborted) => ({ runId: "finishing", aborted }),
      held.cleanup,
      (code) => exits.push(code),
    );
    const finished = stop.finish("the page exited");
    stop.onSignal("SIGINT");
    held.release();
    await finished;
    await sleep(10);
    assert.equal(held.starts(), 1);
    assert.deepEqual(exits, []);
    assert.deepEqual(
      JSON.parse(readFileSync(join(directory, "summary.json"), "utf8")),
      { runId: "finishing", aborted: "the page exited" },
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

/** Polls `condition` every 25 ms for up to `ms`; whether it came true. */
async function within(ms: number, condition: () => boolean): Promise<boolean> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() >= deadline) return false;
    await sleep(25);
  }
  return true;
}

interface InterruptedBench {
  readonly output: string;
  readonly pnpm: { code: number | null; signal: NodeJS.Signals | null };
  /** The fixture's record file, one entry per line. */
  readonly record: readonly string[];
  readonly summary: unknown;
  /**
   * Whether the calque or browser stand-in still ran 2 s after `pnpm run`
   * exited.
   */
  readonly survivor: boolean;
}

/**
 * Ctrl-C on `bench-stop-fixture.ts`, sent the way a terminal sends it.
 * The fixture runs as a `pnpm run` script that begins with `exec`, as the
 * root `bench` script does, under the pnpm the repository pins, in a process
 * group of its own, as a shell runs a foreground job. A terminal's Ctrl-C
 * sends SIGINT to pnpm and to node, and on the second pnpm adds a SIGTERM.
 * With no terminal, pnpm moves the script into a group of its own, so the
 * test signals both groups. pnpm without a terminal also passes the first
 * SIGINT on to node, so node can get that one twice. `whileStopping` runs
 * after the first Ctrl-C and before the calque stand-in may finish shutting
 * down.
 */
async function interruptBenchThroughPnpm(
  whileStopping: (
    interrupt: () => void,
    output: () => string,
  ) => Promise<void> = async () => {},
): Promise<InterruptedBench> {
  const directory = mkdtempSync(join(tmpdir(), "bench-ctrl-c-"));
  const runDirectory = join(directory, "run");
  const releaseFile = join(directory, "release");
  const recordFile = join(directory, "record");
  const quote = (word: string) => `'${word.replaceAll("'", "'\\''")}'`;
  const { packageManager } = JSON.parse(
    readFileSync(join(root, "package.json"), "utf8"),
  ) as { packageManager: string };
  writeFileSync(
    join(directory, "package.json"),
    JSON.stringify({
      name: "bench-ctrl-c-fixture",
      private: true,
      packageManager,
      scripts: {
        bench: `exec ${[
          process.execPath,
          "--import",
          new URL("register.mjs", import.meta.url).href,
          fileURLToPath(new URL("bench-stop-fixture.ts", import.meta.url)),
          runDirectory,
          releaseFile,
          recordFile,
        ]
          .map(quote)
          .join(" ")}`,
      },
    }),
  );
  const pnpm = spawn("pnpm", ["run", "bench"], {
    cwd: directory,
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  pnpm.stdout.setEncoding("utf8").on("data", (chunk: string) => {
    output += chunk;
  });
  pnpm.stderr.setEncoding("utf8").on("data", (chunk: string) => {
    output += chunk;
  });
  let standIns: number[] = [];
  const exited = new Promise<InterruptedBench["pnpm"]>(
    (resolveExit, rejectExit) => {
      pnpm.once("error", rejectExit);
      pnpm.once("exit", (code, signal) => {
        resolveExit({ code, signal });
      });
    },
  );
  const group = pnpm.pid;
  try {
    assert.ok(group !== undefined, "pnpm did not start");
    assert.ok(
      await within(
        60_000,
        () =>
          (/^\[calque\] listening$/mu.test(output) &&
            /^ready \d+ \d+ \d+$/mu.test(output)) ||
          pnpm.exitCode !== null ||
          pnpm.signalCode !== null,
      ),
      `the fixture was not ready in 60 s:\n${output}`,
    );
    const ready = /^ready (\d+) (\d+) (\d+)$/mu.exec(output);
    assert.ok(
      ready?.[1] !== undefined &&
        ready[2] !== undefined &&
        ready[3] !== undefined,
      `the fixture never started:\n${output}`,
    );
    const fixture = Number(ready[1]);
    standIns = [Number(ready[2]), Number(ready[3])];
    const interrupt = () => {
      process.kill(-group, "SIGINT");
      process.kill(-fixture, "SIGINT");
    };
    interrupt();
    await whileStopping(interrupt, () => output);
    writeFileSync(releaseFile, "");
    const pnpmExit = await Promise.race([
      exited,
      sleep(60_000, undefined, { ref: false }),
    ]);
    assert.ok(
      pnpmExit !== undefined,
      `pnpm run did not exit in 60 s:\n${output}`,
    );
    // A stand-in killed as node exits can take a moment to be reaped.
    const survivor = !(await within(2_000, () => !standIns.some(alive)));
    const summaryPath = join(runDirectory, "summary.json");
    return {
      output,
      pnpm: pnpmExit,
      record: existsSync(recordFile)
        ? readFileSync(recordFile, "utf8").split("\n").filter(Boolean)
        : [],
      summary: existsSync(summaryPath)
        ? (JSON.parse(readFileSync(summaryPath, "utf8")) as unknown)
        : undefined,
      survivor,
    };
  } finally {
    if (
      group !== undefined &&
      pnpm.exitCode === null &&
      pnpm.signalCode === null
    )
      process.kill(-group, "SIGKILL");
    for (const pid of standIns) if (alive(pid)) process.kill(pid, "SIGKILL");
    rmSync(directory, { recursive: true, force: true });
  }
}

/** What the run did, set beside what a clean stop does. */
function benchStopSummary(run: InterruptedBench) {
  return {
    record: run.record,
    summary: run.summary,
    survivor: run.survivor,
    calqueShutDown: /^\[calque\] shutting down$/mu.test(run.output),
  };
}

const BENCH_STOPPED = {
  // The cleanup ran once and took the shaper down once, and node then exited
  // 130 by itself: a node that a signal ended writes no exit line.
  record: ["cleanup", "down", "exit 130"],
  summary: { runId: "stop-fixture", aborted: "SIGINT", coldSamplesWritten: 0 },
  survivor: false,
  calqueShutDown: true,
};

test(
  "Ctrl-C through pnpm run takes the shaper down once, stops calque, writes the aborted summary and exits 130",
  { timeout: 180_000 },
  async () => {
    const run = await interruptBenchThroughPnpm();
    assert.deepEqual(benchStopSummary(run), BENCH_STOPPED, run.output);
  },
);

test(
  "a second Ctrl-C while the stop is still running leaves it running",
  { timeout: 180_000 },
  async () => {
    const heard = (output: string) =>
      output.match(/^\[bench\] SIG(?:INT|TERM): already stopping$/gmu)
        ?.length ?? 0;
    let secondHeard = false;
    const run = await interruptBenchThroughPnpm(async (interrupt, output) => {
      // The stand-in prints this once the stop has signalled it, and stays up
      // until the test releases it.
      await within(10_000, () => /^\[calque\] shutting down$/mu.test(output()));
      const before = heard(output());
      interrupt();
      secondHeard = await within(10_000, () => heard(output()) > before);
    });
    assert.deepEqual(
      { ...benchStopSummary(run), secondHeard },
      { ...BENCH_STOPPED, secondHeard: true },
      run.output,
    );
  },
);

function armedTimers(): number {
  return process
    .getActiveResourcesInfo()
    .filter((resource) => resource === "Timeout").length;
}

test("a reading that settles before its deadline leaves no timer armed", async () => {
  const before = armedTimers();
  // The instrument sees an armed timer, so its silence below means something.
  const control = setTimeout(() => undefined, 60_000);
  assert.equal(armedTimers(), before + 1);
  clearTimeout(control);

  assert.equal(await withDeadline(Promise.resolve(7), "reading"), 7);
  await assert.rejects(
    withDeadline(Promise.reject(new Error("refused")), "reading"),
    /refused/u,
  );
  assert.equal(armedTimers(), before);
});

test("a reading past its deadline is refused by name and leaves no timer armed", async () => {
  const before = armedTimers();
  await assert.rejects(
    withDeadline(new Promise(() => {}), "screenshot", 10),
    /^Error: no screenshot within 10 ms$/u,
  );
  assert.equal(armedTimers(), before);
});
