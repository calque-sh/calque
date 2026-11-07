import assert from "node:assert/strict";
import test from "node:test";

import {
  commentTimeMicros,
  CONTENT_COLUMNS,
  CONTENT_DATASET_VERSION,
  CONTENT_SEEDING_MARKER,
  contentBatches,
  contentPayloadExpression,
  contentSelectExpressions,
  contentUuid,
  contentVerificationSql,
  createContentGenerator,
  eventTimeMicros,
  MESSAGE_TEMPLATES,
  PAYLOAD_LATENCY_MAX_MS,
  PAYLOAD_LATENCY_MEDIAN_MS,
  PAYLOAD_LATENCY_SIGMA,
  PAYLOAD_SAMPLED_PROBABILITY,
  seedContentFixture,
  SYLLABLES,
  timestampLiteral,
  v1LengthsSql,
  VOCABULARY_WORDS,
  zipfSampler,
  type ContentFixtureSeams,
} from "./bench-content-fixture.ts";
import {
  defaultFixtureSeams,
  FIXTURE_TABLES,
  fixtureDdl,
  FIXTURE_DATABASE_ENV,
  preflightSql,
  seedBatches,
  sentinelSql,
  validationSql,
  type FixtureTable,
} from "./bench-fixture.ts";

const generator = createContentGenerator();
const table = (name: string): FixtureTable => {
  const found = FIXTURE_TABLES.find((candidate) => candidate.name === name);
  if (!found) throw new Error(`no table ${name}`);
  return found;
};
const ANCHOR = Date.UTC(2026, 7, 9) * 1_000;
const DAY = 86_400_000_000;

test("the generator is a function of its seed and each row alone", () => {
  const again = createContentGenerator();
  const other = createContentGenerator(1);
  assert.deepEqual(again.vocabulary, generator.vocabulary);
  assert.deepEqual(again.templates, generator.templates);
  for (const benchIndex of [0, 1, 2, 799_999]) {
    assert.equal(
      again.message(benchIndex, 320),
      generator.message(benchIndex, 320),
    );
    assert.equal(
      again.prose(6, benchIndex, 2_048),
      generator.prose(6, benchIndex, 2_048),
    );
    assert.equal(again.unit(4, benchIndex), generator.unit(4, benchIndex));
    assert.deepEqual(again.payload(benchIndex), generator.payload(benchIndex));
  }
  // A row's text depends on nothing generated before it.
  const fresh = createContentGenerator();
  assert.equal(fresh.message(123_456, 200), generator.message(123_456, 200));
  assert.notDeepEqual(other.vocabulary, generator.vocabulary);
  assert.notEqual(other.message(5, 320), generator.message(5, 320));
  assert.notDeepEqual(other.payload(5), generator.payload(5));
  assert.notEqual(generator.prose(5, 7, 200), generator.prose(6, 7, 200));
  assert.notEqual(generator.prose(5, 7, 200), generator.prose(5, 8, 200));
});

test("every text is exactly the octet length asked for, in printable ASCII", () => {
  for (const bytes of [
    0, 1, 2, 3, 4, 7, 24, 128, 180, 181, 256, 320, 2_047, 2_048,
  ])
    for (const benchIndex of [0, 1, 2, 3, 59_999, 799_999]) {
      for (const text of [
        generator.message(benchIndex, bytes),
        generator.prose(6, benchIndex, bytes),
      ]) {
        assert.equal(Buffer.byteLength(text, "utf8"), bytes);
        assert.match(text, /^[\x20-\x7e]*$/u);
      }
    }
  assert.throws(() => generator.prose(6, 0, -1), /-1 bytes/u);
  assert.throws(() => generator.message(0, 1.5), /1.5 bytes/u);
});

test("the vocabulary is 5,000 distinct words of one to four syllables, shortest first", () => {
  assert.equal(generator.vocabulary.length, VOCABULARY_WORDS);
  assert.equal(new Set(generator.vocabulary).size, VOCABULARY_WORDS);
  const syllable = `(?:${[...SYLLABLES].sort((a, b) => b.length - a.length).join("|")})`;
  const word = new RegExp(`^${syllable}{1,4}$`, "u");
  const longest = Math.max(...SYLLABLES.map((entry) => entry.length));
  for (const entry of generator.vocabulary) {
    assert.match(entry, word);
    assert.ok(entry.length >= 1 && entry.length <= 4 * longest, entry);
  }
  for (let rank = 1; rank < VOCABULARY_WORDS; rank += 1)
    assert.ok(
      (generator.vocabulary[rank - 1]?.length ?? 0) <=
        (generator.vocabulary[rank]?.length ?? 0),
    );
});

test("the Zipf sampler stays inside its ranks and weighs rank k as 1/k", () => {
  const rank = zipfSampler(VOCABULARY_WORDS, 1);
  assert.equal(rank(0), 0);
  assert.equal(rank(1 - 2 ** -53), VOCABULARY_WORDS - 1);
  const harmonic = Array.from(
    { length: VOCABULARY_WORDS },
    (_, index) => 1 / (index + 1),
  ).reduce((sum, weight) => sum + weight, 0);
  assert.equal(rank(0.999_999 / harmonic), 0);
  assert.equal(rank(1.000_001 / harmonic), 1);
  assert.equal(rank(1.500_001 / harmonic), 2);
  assert.equal(
    zipfSampler(MESSAGE_TEMPLATES, 1)(1 - 2 ** -53),
    MESSAGE_TEMPLATES - 1,
  );

  // Every word a row draws is in the vocabulary, and the most frequent word
  // takes about 1/H(5000) = 11.0% of them.
  const vocabulary = new Set(generator.vocabulary);
  const counts = new Map<string, number>();
  let words = 0;
  for (let benchIndex = 0; benchIndex < 2_000; benchIndex += 1) {
    const text = generator.prose(6, benchIndex, 2_048);
    const drawn = text.split(" ").slice(0, -1);
    for (const entry of drawn) {
      assert.ok(vocabulary.has(entry), entry);
      counts.set(entry, (counts.get(entry) ?? 0) + 1);
      words += 1;
    }
  }
  const share = (counts.get(generator.vocabulary[0] ?? "") ?? 0) / words;
  assert.ok(Math.abs(share - 1 / harmonic) < 0.01, String(share));
});

test("messages start from 200 distinct templates whose fields are filled per row", () => {
  assert.equal(generator.templates.length, MESSAGE_TEMPLATES);
  assert.equal(new Set(generator.templates).size, MESSAGE_TEMPLATES);
  for (const template of generator.templates)
    assert.match(template, /^[a-z ]+: [a-z]+=\{(?:hex|int|path)\}/u);
  const heads = new Set<string>();
  for (let benchIndex = 0; benchIndex < 500; benchIndex += 1) {
    const message = generator.message(benchIndex, 320);
    assert.doesNotMatch(message, /[{}]/u);
    const head = message.slice(0, message.indexOf(":") + 1);
    assert.ok(
      generator.templates.some((template) => template.startsWith(head)),
      head,
    );
    heads.add(head);
  }
  assert.ok(heads.size > 20, String(heads.size));
});

test("a content UUID is version 4, variant 10, and a function of its pair", () => {
  // The value PostgreSQL's pg_temp.content_uuid must also produce; the seed
  // compares the two on the first user.
  assert.equal(
    contentUuid(1_073_741_824, 0),
    "9f4b32f4-4da6-4a63-96dc-043cfc6b775b",
  );
  const seen = new Set<string>();
  for (const namespace of [
    268_435_456, 536_870_912, 805_306_368, 1_073_741_824,
  ])
    for (let value = 0; value < 1_000; value += 1) {
      const uuid = contentUuid(namespace, value);
      assert.match(
        uuid,
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
      );
      assert.equal(uuid, contentUuid(namespace, value));
      seen.add(uuid);
    }
  assert.equal(seen.size, 4_000);
});

test("event times rise with bench_index and keep v1's 55% in the last seven days", () => {
  const rows = table("events").profileRows;
  let previous = -Infinity;
  let recent = 0;
  for (let benchIndex = 0; benchIndex < rows; benchIndex += 1) {
    const time = eventTimeMicros(
      benchIndex,
      rows,
      generator.unit(4, benchIndex),
    );
    assert.ok(time > previous);
    assert.ok(time >= ANCHOR - 90 * DAY && time < ANCHOR);
    if (time >= ANCHOR - 7 * DAY) recent += 1;
    previous = time;
  }
  assert.equal(recent / rows, 0.55);
  assert.equal(eventTimeMicros(rows - 1, rows, 1 - 2 ** -32) < ANCHOR, true);
  assert.equal(
    eventTimeMicros(rows * 0.45 - 1, rows, 1 - 2 ** -32) < ANCHOR - 7 * DAY,
    true,
  );
  assert.throws(() => eventTimeMicros(0, 150, 0), /divisible by 100/u);
});

test("comment times rise with bench_index through the year before the anchor", () => {
  const rows = table("comments").profileRows;
  let previous = -Infinity;
  for (let benchIndex = 0; benchIndex < rows; benchIndex += 1) {
    const time = commentTimeMicros(
      benchIndex,
      rows,
      generator.unit(7, benchIndex),
    );
    assert.ok(time > previous && time >= ANCHOR - 365 * DAY && time < ANCHOR);
    previous = time;
  }
  assert.equal(timestampLiteral(ANCHOR - 1), "2026-08-08T23:59:59.999999+00");
  assert.equal(timestampLiteral(ANCHOR + 42), "2026-08-09T00:00:00.000042+00");
});

test("payload values are drawn per row: 8 hex digits, a log-normal latency and a 1/7 sample", () => {
  assert.equal(PAYLOAD_LATENCY_MEDIAN_MS, 40);
  assert.equal(PAYLOAD_LATENCY_SIGMA, 1);
  assert.equal(PAYLOAD_LATENCY_MAX_MS, 4_999);
  assert.equal(PAYLOAD_SAMPLED_PROBABILITY, 1 / 7);
  const rows = 200_000;
  const latencies: number[] = [];
  const traces = new Set<string>();
  let sampled = 0;
  let logSquares = 0;
  for (let benchIndex = 0; benchIndex < rows; benchIndex += 1) {
    const payload = generator.payload(benchIndex);
    assert.match(payload.trace, /^[0-9a-f]{8}$/u);
    assert.ok(
      Number.isInteger(payload.latencyMs) &&
        payload.latencyMs >= 0 &&
        payload.latencyMs <= PAYLOAD_LATENCY_MAX_MS,
      String(payload.latencyMs),
    );
    latencies.push(payload.latencyMs);
    traces.add(payload.trace);
    if (payload.sampled) sampled += 1;
    if (payload.latencyMs > 0)
      logSquares +=
        Math.log(payload.latencyMs / PAYLOAD_LATENCY_MEDIAN_MS) ** 2;
  }
  latencies.sort((left, right) => left - right);
  assert.equal(latencies[rows / 2], PAYLOAD_LATENCY_MEDIAN_MS);
  // ln(latency / 40) is normal with sigma 1, apart from rounding near 0.
  assert.ok(Math.abs(Math.sqrt(logSquares / rows) - 1) < 0.02);
  assert.ok(Math.abs(sampled / rows - 1 / 7) < 0.005, String(sampled / rows));
  // 32 random bits per trace: a few collisions among 200,000, not thousands.
  assert.ok(traces.size > rows - 20, String(traces.size));
});

test("the payload expression is v1's with its three drawn values staged", () => {
  const events = table("events");
  const v1 = events.selectExpressions[events.columns.indexOf("payload")] ?? "";
  const variant = contentPayloadExpression(v1);
  assert.equal(
    variant
      .replace("g.payload_trace", "lpad(to_hex(g.bench_index), 8, '0')")
      .replace("g.payload_latency_ms", "(g.bench_index * 37) % 5000")
      .replace("g.payload_sampled", "(g.bench_index % 7) = 0"),
    v1,
  );
  for (const staged of [
    "g.payload_trace",
    "g.payload_latency_ms",
    "g.payload_sampled",
  ])
    assert.equal(variant.split(staged).length, 2, staged);
  // The arms that set v1's width bounds, and the shape, stay v1's.
  assert.match(
    variant,
    /WHEN g\.bench_index = 0 THEN jsonb_build_object\('blob', repeat\('x', 1013\)\) WHEN g\.bench_index = 1 THEN/u,
  );
  assert.match(variant, /'shape', g\.bench_index % 24/u);
  assert.throws(
    () => contentPayloadExpression(v1.replace("% 5000", "% 4000")),
    /does not hold \(g\.bench_index \* 37\) % 5000 once/u,
  );
});

test("the variant differs from v1's expressions only in its generated columns and UUIDs", () => {
  let replaced = 0;
  for (const fixture of FIXTURE_TABLES) {
    const generated = new Set(
      (CONTENT_COLUMNS[fixture.name] ?? []).map((content) => content.column),
    );
    const expressions = contentSelectExpressions(fixture);
    assert.equal(expressions.length, fixture.columns.length);
    fixture.columns.forEach((column, index) => {
      const v1 = fixture.selectExpressions[index] ?? "";
      const variant = expressions[index] ?? "";
      assert.doesNotMatch(variant, /incidents\.fixture_uuid\(/u);
      if (column === "payload")
        assert.equal(variant, contentPayloadExpression(v1));
      else if (generated.has(column)) assert.equal(variant, `g.${column}`);
      else {
        assert.equal(
          variant,
          v1.replaceAll("incidents.fixture_uuid(", "pg_temp.content_uuid("),
        );
        if (variant !== v1) replaced += 1;
      }
    });
  }
  // Every UUID column but the sentinels': events 3, issues 4, comments 3,
  // issue_tags 1, users 1, projects 2 and memberships 2.
  assert.equal(replaced, 16);
  assert.deepEqual(
    Object.entries(CONTENT_COLUMNS).flatMap(([name, columns]) =>
      (columns ?? []).map((content) => `${name}.${content.column}`),
    ),
    [
      "events.message",
      "events.occurred_at",
      "events.payload",
      "issues.title",
      "comments.body",
      "comments.created_at",
      "users.display_name",
      "projects.name",
    ],
  );
});

test("the variant inserts in v1's transactions", () => {
  const v1 = seedBatches().map(
    (batch) =>
      `${batch.table} ${batch.firstBenchIndex}-${batch.firstBenchIndex + batch.rowCount - 1}`,
  );
  const order = [...new Set(seedBatches().map((batch) => batch.table))];
  const variant = order.flatMap((name) =>
    contentBatches(table(name)).map(
      ([first, last]) => `${name} ${first}-${last}`,
    ),
  );
  assert.deepEqual(variant, v1);
});

/**
 * A source that answers what the seed asks, with `verification` and
 * `validation` for its two reports, and records every statement and streamed
 * session.
 */
function fakeSource(
  tables: readonly FixtureTable[],
  verification: (sql: string) => Record<string, unknown>,
  validation: Record<string, boolean>,
) {
  const statements: string[] = [];
  const sessions: string[] = [];
  const seams: ContentFixtureSeams = {
    readProfile: defaultFixtureSeams.readProfile,
    runSql: (sql) => {
      statements.push(sql);
      if (sql === preflightSql)
        return JSON.stringify({ role: "calque_sync", superuser: false });
      const lengths = tables.find((candidate) =>
        (CONTENT_COLUMNS[candidate.name] ?? []).some(
          (content) => sql === v1LengthsSql(candidate, content.column),
        ),
      );
      if (lengths)
        return Array.from(
          { length: lengths.profileRows },
          (_, index) => 1 + ((index * 37) % 300),
        ).join(",");
      if (sql === contentVerificationSql(tables))
        return JSON.stringify(verification(sql));
      if (sql === validationSql())
        return JSON.stringify({ environment: {}, checks: validation });
      return "";
    },
    streamSql: async (chunks) => {
      sessions.push([...chunks].join(""));
      return "";
    },
  };
  return { seams, statements, sessions };
}

const small = FIXTURE_TABLES.map((fixture) => ({
  ...fixture,
  profileRows: fixture.name === "events" ? 200 : 30,
}));
const passing = (sql: string): Record<string, unknown> => {
  const report: Record<string, unknown> = {};
  for (const match of sql.matchAll(/'([a-z_]+\.[a-z_]+\.[a-z0-9]+)'/gu))
    if (match[1]) report[match[1]] = 0;
  report["users.first.id"] = contentUuid(1_073_741_824, 0);
  return report;
};
const environment = {
  [FIXTURE_DATABASE_ENV]:
    "postgresql://calque_sync:calque_sync@127.0.0.1:55442/calque_dev",
};

test("a seed that passes both check lists replaces v1's content and names the variant last", async () => {
  const source = fakeSource(small, passing, {
    metadata: true,
    events_widths: true,
  });
  await seedContentFixture(environment, source.seams, small);
  assert.equal(source.statements[0], preflightSql);
  assert.equal(
    source.statements[1],
    `${fixtureDdl}\nCOMMENT ON SCHEMA incidents IS '${CONTENT_SEEDING_MARKER}';\n`,
  );
  assert.ok(source.statements.includes(sentinelSql));
  assert.equal(
    source.statements.at(-1),
    `COMMENT ON SCHEMA incidents IS '${CONTENT_DATASET_VERSION}';\n`,
  );
  assert.equal(source.sessions.length, 1);
  const session = source.sessions[0] ?? "";
  assert.match(session, /CREATE FUNCTION pg_temp\.content_uuid/u);
  const verification = contentVerificationSql(small);
  for (const name of ["kept", "values", "v1"])
    assert.ok(verification.includes(`'events.payload.${name}'`), name);
  assert.doesNotMatch(session, /incidents\.fixture_uuid\(/u);
  // The third staged event: bench_index, its message at v1's length, its
  // time and its payload's draws.
  const staged =
    session.split(
      "COPY content_stage (bench_index, message, occurred_at, payload_trace, payload_latency_ms, payload_sampled) FROM STDIN;\n",
    )[1] ?? "";
  const payload = generator.payload(2);
  assert.deepEqual((staged.split("\n")[2] ?? "").split("\t"), [
    "2",
    generator.message(2, 75),
    timestampLiteral(eventTimeMicros(2, 200, generator.unit(4, 2))),
    payload.trace,
    String(payload.latencyMs),
    payload.sampled ? "t" : "f",
  ]);
  assert.match(
    session,
    /CREATE TEMP TABLE content_stage \(bench_index bigint PRIMARY KEY, message text NOT NULL, occurred_at timestamptz NOT NULL, payload_trace text NOT NULL, payload_latency_ms integer NOT NULL, payload_sampled boolean NOT NULL\);/u,
  );
  assert.equal(session.match(/^\\\.$/gmu)?.length, 5);
});

test("a row whose text misses v1's octet length refuses the seed and never names the variant", async () => {
  const source = fakeSource(
    small,
    (sql) => ({ ...passing(sql), "comments.body.width": 3 }),
    { metadata: true },
  );
  await assert.rejects(
    seedContentFixture(environment, source.seams, small),
    /content fixture refused: comments\.body\.width: 3 rows/u,
  );
  assert.ok(
    !source.statements.some((sql) =>
      sql.includes(`IS '${CONTENT_DATASET_VERSION}'`),
    ),
  );
});

test("PostgreSQL's content UUID disagreeing with contentUuid refuses the seed", async () => {
  const source = fakeSource(
    small,
    (sql) => ({
      ...passing(sql),
      "users.first.id": "40000000-0000-4000-8000-000000000000",
    }),
    { metadata: true },
  );
  await assert.rejects(
    seedContentFixture(environment, source.seams, small),
    /first user's id is 40000000-0000-4000-8000-000000000000 in PostgreSQL/u,
  );
});

test("a failing check of v1's refuses the seed by name", async () => {
  const source = fakeSource(small, passing, {
    metadata: true,
    event_last_seven_days: false,
  });
  await assert.rejects(
    seedContentFixture(environment, source.seams, small),
    /failed v1's checks: event_last_seven_days/u,
  );
  assert.ok(
    !source.statements.some((sql) =>
      sql.includes(`IS '${CONTENT_DATASET_VERSION}'`),
    ),
  );
});

test("the seed refuses a superuser before it drops anything", async () => {
  const source = fakeSource(small, passing, { metadata: true });
  const seams: ContentFixtureSeams = {
    ...source.seams,
    runSql: (sql) =>
      sql === preflightSql
        ? JSON.stringify({ role: "calque_sync", superuser: true })
        : source.seams.runSql(sql, {}),
  };
  await assert.rejects(
    seedContentFixture(environment, seams, small),
    /not a superuser/u,
  );
  assert.deepEqual(source.statements, []);
});
