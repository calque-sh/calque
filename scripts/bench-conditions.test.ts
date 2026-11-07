import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_NETWORK,
  judgeShaping,
  parseBenchArguments,
  parseNetwork,
  rendersPlan,
  shapingLine,
  withConditions,
} from "./bench-conditions.ts";
import { realtimePasses } from "./bench-realtime.ts";
import { CADDY } from "./bench-caddy.ts";
import {
  BENCH_ROW_COUNTS,
  BENCH_TABLES,
  benchValidationChecks,
  benchValidationSql,
  DATASETS,
  loadTransactionSql,
  probeInsertSql,
  probeSessionSql,
  SEED_ORDER,
  tailSql,
} from "./bench-oracle.ts";
import {
  CONTENT_COLUMNS,
  CONTENT_DATASET_VERSION,
  contentUuid,
  contentUuidFunctionSql,
  contentVerificationSql,
  createContentGenerator,
  seedContentFixture,
  v1LengthsSql,
  type ContentFixtureSeams,
} from "./bench-content-fixture.ts";
import {
  DATASET_VERSION,
  defaultFixtureSeams,
  FIXTURE_TABLES,
  fixtureValidationChecks,
  FIXTURE_DATABASE_ENV,
  preflightSql,
  sentinelSql,
} from "./bench-fixture.ts";

// Arguments -----------------------------------------------------------------------

test("a run defaults to content-v1 over an emulated 100/30 network", () => {
  const options = parseBenchArguments(["--smoke"]);
  assert.equal(options.dataset, "content-v1");
  assert.deepEqual(options.network, { megabitsPerSecond: 100, latencyMs: 30 });
  assert.deepEqual(DEFAULT_NETWORK, options.network);
  assert.deepEqual(DATASETS, {
    "content-v1": CONTENT_DATASET_VERSION,
    v1: DATASET_VERSION,
  });
});

test("--dataset v1 --network off is v1 over loopback, and --network takes a profile", () => {
  const options = parseBenchArguments([
    "--",
    "--smoke",
    "--dataset",
    "v1",
    "--network",
    "off",
  ]);
  assert.equal(options.dataset, "v1");
  assert.equal(options.network, null);
  assert.deepEqual(
    parseBenchArguments(["--network", "1000/2", "--dataset", "content-v1"])
      .network,
    { megabitsPerSecond: 1_000, latencyMs: 2 },
  );
  assert.deepEqual(parseNetwork("12.5/0"), {
    megabitsPerSecond: 12.5,
    latencyMs: 0,
  });
  for (const bad of [
    ["--dataset"],
    ["--dataset", "v2"],
    ["--dataset", "v1", "--dataset", "v1"],
    ["--network"],
    ["--network", "off", "--network", "off"],
  ])
    assert.throws(() => parseBenchArguments(bad), /usage: pnpm bench/u);
  for (const bad of ["fast", "0/30", "100", "100/", "/30", "-1/30", "100/-2"])
    assert.throws(
      () => parseBenchArguments(["--network", bad]),
      /--network takes off or <Mbit\/s>\/<latency ms>/u,
    );
});

test("every sample record names its dataset, network profile and page", () => {
  const record = { kind: "probe", k: 3 };
  assert.deepEqual(withConditions(parseBenchArguments(["--smoke"]), record), {
    kind: "probe",
    k: 3,
    conditions: {
      dataset: "content-v1",
      network: { megabitsPerSecond: 100, latencyMs: 30 },
      renderedPlan: true,
      edge: {
        caddy: "2.11.4",
        binarySha256: CADDY.binarySha256,
        protocols: ["h1", "h2"],
      },
    },
  });
  assert.deepEqual(
    withConditions(
      parseBenchArguments(["--dataset", "v1", "--network", "off", "--no-edge"]),
      record,
    ),
    {
      kind: "probe",
      k: 3,
      conditions: {
        dataset: "v1",
        network: null,
        renderedPlan: true,
        edge: null,
      },
    },
  );
});

test("the page carries a rendered plan unless the kernel shapes its backend's request", () => {
  const emulated = parseBenchArguments(["--smoke"]);
  const unshaped = parseBenchArguments(["--network", "off"]);
  const shaped = parseBenchArguments(["--network", "shaped:500/20"]);
  assert.deepEqual(
    [emulated, unshaped, shaped].map((options) => rendersPlan(options)),
    [true, true, false],
  );
});

test("a cold sample's own network ledger survives its conditions", () => {
  const ledger = { requests: 4, wireBytes: 9_014_097, byRequest: [] };
  const record = { kind: "cold", n: 0, network: ledger };
  assert.deepEqual(withConditions(parseBenchArguments(["--smoke"]), record), {
    kind: "cold",
    n: 0,
    network: ledger,
    conditions: {
      dataset: "content-v1",
      network: { megabitsPerSecond: 100, latencyMs: 30 },
      renderedPlan: true,
      edge: {
        caddy: "2.11.4",
        binarySha256: CADDY.binarySha256,
        protocols: ["h1", "h2"],
      },
    },
  });
});

test("an unshaped link fails the run apart from its final state", () => {
  assert.equal(
    realtimePasses({ finalFailure: null, shapingFailure: null }),
    true,
  );
  assert.equal(
    realtimePasses({
      finalFailure: null,
      shapingFailure: "the probes ran unshaped",
    }),
    false,
  );
  assert.equal(
    realtimePasses({
      finalFailure: "the page did not load",
      shapingFailure: null,
    }),
    false,
  );
});

test("the probes ran shaped only if no fetch after them beat the latency", () => {
  const network = { megabitsPerSecond: 100, latencyMs: 30 };
  assert.deepEqual(judgeShaping(network, [41.2, 30, 33.5]), {
    latencyMs: 30,
    fetchMs: [41.2, 30, 33.5],
    fastestMs: 30,
    shaped: true,
  });
  const loopback = judgeShaping(network, [31.4, 2.1, 1.8, 40]);
  assert.equal(loopback.fastestMs, 1.8);
  assert.equal(loopback.shaped, false);
  assert.equal(judgeShaping(network, [29.99]).shaped, false);
  assert.deepEqual(judgeShaping(network, []), {
    latencyMs: 30,
    fetchMs: [],
    fastestMs: null,
    shaped: false,
  });
});

test("with no emulation, or none of its latency, the shaping witness does not apply rather than passing", () => {
  const off = judgeShaping(null, []);
  assert.deepEqual(off, {
    latencyMs: null,
    fetchMs: [],
    fastestMs: null,
    shaped: null,
  });
  assert.equal(
    shapingLine(off),
    "network: not emulated, so the shaping witness does not apply",
  );
  // Loopback's 2 ms fetches would pass a 0 ms floor: that is no witness.
  const unlatent = judgeShaping(
    { megabitsPerSecond: 1_000, latencyMs: 0 },
    [2.1, 1.8],
  );
  assert.equal(unlatent.shaped, null);
  assert.equal(
    shapingLine(unlatent),
    "network: 0 ms of emulated latency, so the shaping witness does not apply",
  );
  assert.equal(
    shapingLine(
      judgeShaping({ megabitsPerSecond: 100, latencyMs: 30 }, [33.1]),
    ),
    "network: the fastest of 1 uncached fetches after the probes took 33.1 ms, against 30 ms of emulated latency",
  );
});

// The content seed at the cut -----------------------------------------------------

test("v1's checks at the cut keep v1's own text, except the cut's counts", () => {
  const v1 = new Map(fixtureValidationChecks());
  const checks = benchValidationChecks();
  for (const [name, expression] of checks)
    if (!/_(?:count|bench_index)$/u.test(name))
      assert.equal(expression, v1.get(name), `${name} is not v1's text`);
  const names = checks.map(([name]) => name);
  // These fix the 1M profile's counts, cardinalities, null counts and skews,
  // and the tag skew holds at the cut only by the hash's draw.
  const leftOut = [
    "events_cardinality",
    "issues_cardinality",
    "comments_cardinality",
    "issue_tags_cardinality",
    "users_cardinality",
    "projects_cardinality",
    "memberships_cardinality",
    "null_fractions",
    "organization_skew",
    "comment_issue_skew",
    "tag_skew",
  ];
  assert.deepEqual(
    [...names, ...leftOut].sort(),
    [...v1.keys()].sort(),
    "every v1 check is asked at the cut or left out by name",
  );
  assert.deepEqual(
    names.filter((name) => !/_(?:count|bench_index)$/u.test(name)),
    [
      "role",
      "non_superuser",
      "postgres_18",
      "utf8",
      "utc",
      "logical_wal",
      "replication_slots",
      "wal_senders",
      "metadata",
      "events_fk",
      "issues_fk",
      "comments_fk",
      "issue_tags_fk",
      "memberships_fk",
      "events_widths",
      "issues_widths",
      "comments_widths",
      "issue_tags_widths",
      "users_widths",
      "projects_widths",
      "memberships_widths",
      "event_last_seven_days",
      "issue_last_seven_days",
      "issue_event_count_summary",
      "comment_body_median",
      "payload_shapes",
      "tier_100k_closure",
    ],
  );
  for (const table of FIXTURE_TABLES) {
    assert.ok(names.includes(`${table.name}_count`));
    assert.ok(names.includes(`${table.name}_bench_index`));
  }
  const events = new Map(benchValidationChecks()).get("events_count");
  assert.match(
    events ?? "",
    /= 80000 AND count\(\*\) FILTER \(WHERE is_sentinel\) = 1/u,
  );
  const sql = benchValidationSql();
  assert.match(sql, /^SELECT jsonb_build_object\(\n {2}'environment', /u);
  assert.ok(sql.includes("('events_bench_index', ("));
});

test("the content seed cuts the tables to 100k rows in all and checks the cut", async () => {
  const tables = BENCH_TABLES;
  const counts = BENCH_ROW_COUNTS;
  const statements: string[] = [];
  let session = "";
  const seams: ContentFixtureSeams = {
    readProfile: defaultFixtureSeams.readProfile,
    runSql: (sql) => {
      statements.push(sql);
      if (sql === preflightSql)
        return JSON.stringify({ role: "calque_sync", superuser: false });
      const lengths = tables.find((table) =>
        (CONTENT_COLUMNS[table.name] ?? []).some(
          (content) => sql === v1LengthsSql(table, content.column),
        ),
      );
      if (lengths)
        return Array.from(
          { length: lengths.profileRows },
          (_, index) => 24 + (index % 200),
        ).join(",");
      if (sql === contentVerificationSql(tables)) {
        const report: Record<string, unknown> = {};
        for (const match of sql.matchAll(/'([a-z_]+\.[a-z_]+\.[a-z0-9]+)'/gu))
          if (match[1]) report[match[1]] = 0;
        report["users.first.id"] = contentUuid(1_073_741_824, 0);
        return JSON.stringify(report);
      }
      if (sql === benchValidationSql())
        return JSON.stringify({
          environment: {},
          checks: Object.fromEntries(
            benchValidationChecks().map(([name]) => [name, true]),
          ),
        });
      return "";
    },
    streamSql: async (chunks) => {
      session = [...chunks].join("");
      return "";
    },
  };
  await seedContentFixture(
    {
      [FIXTURE_DATABASE_ENV]:
        "postgresql://calque_sync:calque_sync@127.0.0.1:55521/calque_dev",
    },
    seams,
    tables,
    createContentGenerator(),
    benchValidationSql(),
  );
  assert.equal(statements[0], preflightSql);
  // Each text's v1 lengths are read at the cut's rows, never the profile's.
  for (const table of tables)
    for (const content of CONTENT_COLUMNS[table.name] ?? [])
      if (content.kind === "message" || content.kind === "prose")
        assert.ok(
          statements.includes(v1LengthsSql(table, content.column)),
          `${table.name}.${content.column}`,
        );
  assert.ok(statements.includes(sentinelSql));
  assert.ok(statements.includes(benchValidationSql()));
  assert.ok(!statements.some((sql) => sql.includes("'events_cardinality'")));

  // Every table's rows 0 to its cut count - 1, once, in seed order.
  const covered = new Map<string, number>();
  const order: string[] = [];
  const batch =
    /INSERT INTO incidents\.(\w+) \([^)]*\)\nSELECT [\s\S]*?\n(?:FROM generate_series\((\d+)::bigint, (\d+)::bigint\) AS g\(bench_index\)|FROM content_stage AS g\nWHERE g\.bench_index BETWEEN (\d+) AND (\d+)\nORDER BY g\.bench_index);/gu;
  for (const match of session.matchAll(batch)) {
    const table = match[1] ?? "";
    const first = Number(match[2] ?? match[4]);
    const last = Number(match[3] ?? match[5]);
    assert.equal(first, covered.get(table) ?? 0, `${table} has a gap`);
    covered.set(table, last + 1);
    if (order.at(-1) !== table) order.push(table);
  }
  assert.deepEqual(order, [...SEED_ORDER]);
  assert.deepEqual(Object.fromEntries(covered), { ...counts });
  assert.ok(!session.includes("incidents.fixture_uuid("));
  // The staged events are the cut's 80,000, not the profile's 800,000.
  const staged = session.split("COPY content_stage").length - 1;
  assert.equal(staged, Object.keys(CONTENT_COLUMNS).length);
  assert.match(session, /\n79999\t[^\n]*\n\\\.\n/u);
  assert.ok(!session.includes("\n80000\t"));
});

// Probes and load -----------------------------------------------------------------

test("a content-v1 probe keys its row by content_uuid and draws the seed's content", () => {
  assert.equal(probeSessionSql("content-v1"), contentUuidFunctionSql);
  assert.equal(probeSessionSql("v1"), "");
  const v1 = probeInsertSql("v1", 0);
  assert.equal((v1.match(/incidents\.fixture_uuid\(/gu) ?? []).length, 3);
  assert.match(v1, /FROM \(SELECT 80001::bigint AS bench_index\) AS g;\n$/u);

  const sql = probeInsertSql("content-v1", 0);
  assert.ok(!sql.includes("fixture_uuid("));
  assert.equal((sql.match(/pg_temp\.content_uuid\(/gu) ?? []).length, 3);
  assert.match(sql, /^INSERT INTO incidents\.events \(id, organization_id, /u);
  assert.match(
    sql,
    /\nSELECT g\.bench_index \+ 1,\n {2}pg_temp\.content_uuid\(268435456, /u,
  );
  const generator = createContentGenerator();
  const message = generator.message(80_001, 320);
  assert.ok(
    sql.includes(
      `left('${message}', octet_length(incidents.fixture_text('Captured incident', g.bench_index, 24, 320))) AS message`,
    ),
  );
  // The seed's message at any width is a prefix of the widest one.
  assert.equal(message.slice(0, 57), generator.message(80_001, 57));
  const payload = generator.payload(80_001);
  assert.ok(
    sql.includes(
      `'${payload.trace}'::text AS payload_trace, ${String(payload.latencyMs)} AS payload_latency_ms, ${String(payload.sampled)} AS payload_sampled FROM (SELECT 80001::bigint AS bench_index) AS g) AS g;\n`,
    ),
  );
  // Past the cut's events, a probe lands at the newest instant v1 allows.
  assert.ok(
    sql.includes("timestamptz '2026-08-08T23:59:59.999999+00' AS occurred_at"),
  );
  assert.ok(sql.includes("'trace', g.payload_trace"));
});

test("the tail's n-th transaction rewrites the rows the load's n-th does, and commits on its own", () => {
  const sql = tailSql(5);
  assert.match(sql, /FOR n IN 0\.\.4 LOOP/u);
  assert.match(sql, /COMMIT;\n {2}END LOOP;/u);
  const assigned = [...sql.matchAll(/ SET (\w+) = /gu)].map(
    (match) => match[1],
  );
  assert.deepEqual(assigned, ["title", "body", "touched"]);
  assert.ok(!/INSERT|DELETE/u.test(sql));
  const arrays = [...sql.matchAll(/ANY \((ARRAY\[[^\]]+\])\)/gu)].map(
    (match) => match[1] ?? "",
  );
  assert.equal(arrays.length, 3);
  for (const n of [0, 1, 4, 3_333]) {
    const tailRows = arrays.map((array) =>
      [...array.matchAll(/\(\(n \* (\d+) \+ (\d+)\) % (\d+)\)/gu)].map(
        ([, count, offset, modulus]) =>
          (n * Number(count) + Number(offset)) % Number(modulus),
      ),
    );
    const loadRows = [
      ...loadTransactionSql(n).matchAll(/ANY \(ARRAY\[([^\]]+)\]\)/gu),
    ].map((match) =>
      (match[1] ?? "").split(", ").map((value) => Number.parseInt(value, 10)),
    );
    assert.deepEqual(tailRows, loadRows, `transaction ${String(n)}`);
  }
  assert.throws(() => tailSql(0), /at least one transaction/u);
});

test("the load changes no key, so it runs on either dataset", () => {
  for (const transaction of [0, 1, 3_333]) {
    const sql = loadTransactionSql(transaction);
    const assigned = [...sql.matchAll(/ SET (\w+) = /gu)].map(
      (match) => match[1],
    );
    assert.deepEqual(assigned, ["title", "body", "touched"]);
    assert.ok(!/INSERT|DELETE|fixture_uuid|content_uuid/u.test(sql));
    assert.match(
      sql,
      /SET title = overlay\(title placing 'load-\d+-' from 1\)/u,
    );
    assert.match(sql, /SET body = overlay\(body placing 'load-\d+-' from 1\)/u);
  }
});
