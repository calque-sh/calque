import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import type { FixtureSeams } from "./bench-fixture.ts";
import {
  CORRECTED_PROFILE_SHA256,
  FIXTURE_SOURCE_ROLE,
  FIXTURE_TABLES,
  MAX_SEED_BATCH_BYTES,
  MAX_SEED_BATCH_ROWS,
  evaluateValidationReport,
  fixtureDdl,
  fixtureSeedSql,
  fixtureValidationChecks,
  postgresEnvironmentFromUrl,
  preflightExpression,
  preflightSql,
  profileSha256,
  psqlArguments,
  seedBatches,
  seedFixture,
  validateFixture,
  validationSql,
} from "./bench-fixture.ts";

test("fixture table counts are the corrected one-million-row incident-operations mix", () => {
  assert.deepEqual(
    Object.fromEntries(
      FIXTURE_TABLES.map((table) => [table.name, table.profileRows]),
    ),
    {
      events: 800_000,
      issues: 100_000,
      comments: 60_000,
      issue_tags: 20_000,
      users: 8_000,
      projects: 1_000,
      memberships: 11_000,
    },
  );
  assert.equal(
    FIXTURE_TABLES.reduce((total, table) => total + table.profileRows, 0),
    1_000_000,
  );
});

test("every generated seed transaction respects both frozen bounds", () => {
  const batches = seedBatches();
  assert.equal(
    batches.reduce((total, batch) => total + batch.rowCount, 0),
    1_000_000,
  );
  assert.equal(batches.length, 1_000);
  for (const batch of batches) {
    assert.ok(batch.rowCount > 0 && batch.rowCount <= MAX_SEED_BATCH_ROWS);
    assert.ok(batch.maximumEncodedBytes < MAX_SEED_BATCH_BYTES);
    assert.match(batch.sql, /^BEGIN;\nINSERT INTO incidents\./u);
    assert.match(batch.sql, /\nCOMMIT;\n$/u);
    assert.doesNotMatch(batch.sql, /COPY|VALUES\s*\(/u);
  }
});

test("DDL exposes only the exact fixture schema and explicit sentinel metadata", () => {
  assert.deepEqual(fixtureDdl.match(/DROP SCHEMA [^;]*/gu), [
    "DROP SCHEMA IF EXISTS incidents CASCADE",
  ]);
  assert.doesNotMatch(fixtureDdl, /DROP DATABASE|DROP SCHEMA public/u);
  for (const table of FIXTURE_TABLES) {
    assert.match(
      fixtureDdl,
      new RegExp(`CREATE TABLE incidents\\.${table.name} \\(`, "u"),
    );
  }
  assert.match(fixtureDdl, /CREATE TABLE incidents\.metadata/u);
  assert.match(fixtureDdl, /bench_index bigint NOT NULL UNIQUE/u);
  assert.match(fixtureDdl, /is_sentinel boolean NOT NULL/u);
  const sql = fixtureSeedSql();
  assert.match(sql, /800000, true\);/u);
  assert.match(sql, /100000, true\);/u);
  assert.match(sql, /60000, true\);/u);
  assert.match(sql, /20000, true\);/u);
  assert.match(sql, /8000, true\);/u);
  assert.match(sql, /1000, true\);/u);
  assert.match(sql, /11000, true\);/u);
});

test("the fixture binds the incident-operations profile's hash", () => {
  const profile = readFileSync(
    "harness/profiles/datasets/incident-ops-1m.v1.json",
  );
  assert.equal(profileSha256(profile), CORRECTED_PROFILE_SHA256);
  assert.match(fixtureDdl, new RegExp(CORRECTED_PROFILE_SHA256, "u"));
});

test("PostgreSQL URL becomes libpq environment and never a psql argument", () => {
  const input = {
    PATH: "/bin",
    PGHOST: "wrong-host",
    PGPORT: "9999",
    PGSERVICE: "wrong-service",
  };
  const environment = postgresEnvironmentFromUrl(
    "postgresql://calque_sync:s%40fe@127.0.0.1:55432/calque_dev?sslmode=disable",
    input,
  );
  assert.deepEqual(environment, {
    PATH: "/bin",
    PGHOST: "127.0.0.1",
    PGPORT: "55432",
    PGDATABASE: "calque_dev",
    PGUSER: "calque_sync",
    PGPASSWORD: "s@fe",
    PGSSLMODE: "disable",
  });
  assert.deepEqual(psqlArguments, [
    "--no-psqlrc",
    "--set",
    "ON_ERROR_STOP=1",
    "--quiet",
    "--tuples-only",
    "--no-align",
  ]);
  assert.ok(psqlArguments.every((argument) => !argument.includes("postgres")));
  for (const value of [
    "",
    "https://127.0.0.1/db",
    "postgresql:///db",
    "postgresql://user@host/db/extra",
    "postgresql://user@host/db?application_name=leak",
  ]) {
    assert.throws(() => postgresEnvironmentFromUrl(value, {}));
  }
});

test("validation refuses a wrong state and names every failed oracle", () => {
  const valid = evaluateValidationReport(
    JSON.stringify({
      environment: { role: "calque_sync", serverVersionNum: 180_006 },
      checks: { counts: true, digest_source: true, sentinels: true },
    }),
  );
  assert.equal(valid.valid, true);
  assert.deepEqual(valid.failedChecks, []);

  const deliberatelyWrong = evaluateValidationReport(
    JSON.stringify({
      environment: { role: "calque_sync", serverVersionNum: 180_006 },
      checks: { counts: false, digest_source: true, sentinels: false },
    }),
  );
  assert.equal(deliberatelyWrong.valid, false);
  assert.deepEqual(deliberatelyWrong.failedChecks, ["counts", "sentinels"]);
  assert.throws(
    () => evaluateValidationReport('{"environment":{},"checks":{"x":1}}'),
    /not boolean/u,
  );
});

/**
 * The seed and the validation driven through their seams. Each pair asserts
 * both halves: a refusing input stops the command before any statement reaches
 * `runSql`, and a passing input reaches the work beyond the check.
 */
const DATABASE_URL =
  "postgresql://calque_sync:pw@127.0.0.1:55432/calque_dev?sslmode=disable";
const sourceEnvironment: NodeJS.ProcessEnv = Object.freeze({
  PATH: "/bin",
  CALQUE_FIXTURE_DATABASE_URL: DATABASE_URL,
});

const preflightReport = JSON.stringify({
  role: "calque_sync",
  superuser: false,
  serverVersionNum: 180_006,
  serverEncoding: "UTF8",
  timeZone: "UTC",
  walLevel: "logical",
  maxReplicationSlots: 10,
  maxWalSenders: 10,
});

const passingValidation = JSON.stringify({
  environment: { role: "calque_sync", serverVersionNum: 180_006 },
  checks: { counts: true, digest_source: true, sentinels: true },
});

/**
 * A PostgreSQL that records what it was asked rather than answering from a
 * database. The discrimination is on the statement's own text, so a test reads
 * the same three questions the command asks in the order it asks them.
 */
function recordingPsql(
  replies: { preflight?: string; validation?: string } = {},
): {
  readonly statements: readonly string[];
  readonly runSql: FixtureSeams["runSql"];
} {
  const statements: string[] = [];
  return {
    statements,
    runSql: (sql: string): string => {
      statements.push(sql);
      // The validation query embeds the preflight expression, so it is tested
      // for first; matching on the preflight's fields would answer both with
      // the preflight's report.
      if (sql.includes("'checks'"))
        return replies.validation ?? passingValidation;
      if (sql.includes("'serverVersionNum'"))
        return replies.preflight ?? preflightReport;
      return "";
    },
  };
}

const realProfile = (): Buffer =>
  readFileSync("harness/profiles/datasets/incident-ops-1m.v1.json");
const wrongProfile = (): string => '{"not":"the incident-operations profile"}';

test("a wrong incident-operations profile stops the seed before a statement is issued", () => {
  const psql = recordingPsql();
  assert.throws(
    () =>
      seedFixture(sourceEnvironment, {
        readProfile: wrongProfile,
        runSql: psql.runSql,
      }),
    /corrected incident-operations profile hash/u,
  );
  assert.deepEqual(psql.statements, []);
});

test("the corrected profile reaches the preflight, the seed and the validation", () => {
  const psql = recordingPsql();
  seedFixture(sourceEnvironment, {
    readProfile: realProfile,
    runSql: psql.runSql,
  });
  assert.equal(psql.statements.length, 3);
  assert.match(psql.statements[0] ?? "", /'serverVersionNum'/u);
  assert.match(psql.statements[1] ?? "", /CREATE TABLE incidents\.events/u);
  assert.match(psql.statements[2] ?? "", /incidents\.metadata/u);
});

test("a wrong incident-operations profile stops validation before a statement is issued", () => {
  const psql = recordingPsql();
  assert.throws(
    () =>
      validateFixture(sourceEnvironment, {
        readProfile: wrongProfile,
        runSql: psql.runSql,
      }),
    /corrected incident-operations profile hash/u,
  );
  assert.deepEqual(psql.statements, []);
});

test("validation reaches the oracle and returns the report's own verdict", () => {
  const psql = recordingPsql({
    validation: JSON.stringify({
      environment: { role: "calque_sync", serverVersionNum: 180_006 },
      checks: { counts: false, digest_source: true, sentinels: false },
    }),
  });
  const result = validateFixture(sourceEnvironment, {
    readProfile: realProfile,
    runSql: psql.runSql,
  });
  assert.equal(psql.statements.length, 1);
  assert.match(psql.statements[0] ?? "", /incidents\.metadata/u);
  assert.equal(result.valid, false);
  assert.deepEqual(result.failedChecks, ["counts", "sentinels"]);
});

test("a seeded fixture that fails its oracle refuses, after the seed ran", () => {
  const psql = recordingPsql({
    validation: JSON.stringify({
      environment: {},
      checks: { events_count: false },
    }),
  });
  assert.throws(
    () =>
      seedFixture(sourceEnvironment, {
        readProfile: realProfile,
        runSql: psql.runSql,
      }),
    /seeded fixture failed validation: events_count/u,
  );
  // The refusal is about the state that was written, so the write had to
  // happen first: preflight, seed, validation, then the throw.
  assert.equal(psql.statements.length, 3);
});

test("a source that is not PostgreSQL 18 stops the seed before the DDL", () => {
  const psql = recordingPsql({
    preflight: JSON.stringify({
      role: "calque_sync",
      superuser: false,
      serverVersionNum: 170_004,
      serverEncoding: "UTF8",
      timeZone: "UTC",
      walLevel: "logical",
      maxReplicationSlots: 10,
      maxWalSenders: 10,
    }),
  });
  assert.throws(
    () =>
      seedFixture(sourceEnvironment, {
        readProfile: realProfile,
        runSql: psql.runSql,
      }),
    /PostgreSQL 18 is required/u,
  );
  assert.equal(psql.statements.length, 1);
});

/**
 * The oracle's SQL, pinned by digest so that an edit to a check must also
 * update the digest. The `metadata` check binds a seeded schema to the
 * profile hash.
 */
const VALIDATION_SQL_DIGEST =
  "7b3806397304d5e85443bf55520ad2d34518743da1a8e9357ee267fc9552cc26";
const PREFLIGHT_SQL_DIGEST =
  "a9d2b4b5720775a483d9ef609b7a7222f4bd181c504907b42e266e7bff4d3ec2";

test("the fixture oracle asks exactly these fifty-two named checks", () => {
  const names = fixtureValidationChecks().map(([name]) => name);
  assert.equal(new Set(names).size, names.length, "check names are unique");
  assert.deepEqual(names, [
    "role",
    "non_superuser",
    "postgres_18",
    "utf8",
    "utc",
    "logical_wal",
    "replication_slots",
    "wal_senders",
    "metadata",
    "events_count",
    "events_bench_index",
    "issues_count",
    "issues_bench_index",
    "comments_count",
    "comments_bench_index",
    "issue_tags_count",
    "issue_tags_bench_index",
    "users_count",
    "users_bench_index",
    "projects_count",
    "projects_bench_index",
    "memberships_count",
    "memberships_bench_index",
    "events_fk",
    "issues_fk",
    "comments_fk",
    "issue_tags_fk",
    "memberships_fk",
    "events_cardinality",
    "issues_cardinality",
    "comments_cardinality",
    "issue_tags_cardinality",
    "users_cardinality",
    "projects_cardinality",
    "memberships_cardinality",
    "null_fractions",
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
    "organization_skew",
    "comment_issue_skew",
    "tag_skew",
    "payload_shapes",
    "tier_100k_closure",
  ]);
  // Every table the profile declares is counted and range-checked, so a table
  // added to FIXTURE_TABLES cannot arrive unvalidated.
  for (const table of FIXTURE_TABLES) {
    assert.ok(names.includes(`${table.name}_count`), `${table.name}_count`);
    assert.ok(
      names.includes(`${table.name}_bench_index`),
      `${table.name}_bench_index`,
    );
  }
});

test("the oracle's generated SQL is the text these checks were derived from", () => {
  assert.equal(profileSha256(validationSql()), VALIDATION_SQL_DIGEST);
  assert.equal(profileSha256(preflightSql), PREFLIGHT_SQL_DIGEST);
  assert.match(validationSql(), new RegExp(CORRECTED_PROFILE_SHA256, "u"));
});

test("every field the preflight reports is one the preflight judge refuses without", () => {
  const fields = [...preflightExpression.matchAll(/'([A-Za-z]+)',/gu)].map(
    (match) => match[1] ?? "",
  );
  assert.deepEqual(fields, [
    "role",
    "superuser",
    "serverVersionNum",
    "serverEncoding",
    "timeZone",
    "walLevel",
    "maxReplicationSlots",
    "maxWalSenders",
  ]);
  const complete: Record<string, unknown> = {
    role: "calque_sync",
    superuser: false,
    serverVersionNum: 180_006,
    serverEncoding: "UTF8",
    timeZone: "UTC",
    walLevel: "logical",
    maxReplicationSlots: 10,
    maxWalSenders: 10,
  };
  // Dropping any one field from the report must stop the seed: a field removed
  // from the expression above becomes a field nothing reports, and the judge
  // has to fail closed on it.
  for (const field of fields) {
    const { [field]: _dropped, ...missing } = complete;
    const statements: string[] = [];
    const runSql: FixtureSeams["runSql"] = (sql: string): string => {
      statements.push(sql);
      if (sql.includes("'checks'")) return passingValidation;
      if (sql.includes("'serverVersionNum'")) return JSON.stringify(missing);
      return "";
    };
    assert.throws(
      () =>
        seedFixture(sourceEnvironment, { readProfile: realProfile, runSql }),
      new RegExp(`PostgreSQL|${FIXTURE_SOURCE_ROLE}`, "u"),
      `a preflight missing ${field} must refuse`,
    );
    assert.equal(
      statements.length,
      1,
      `refused before the DDL, missing ${field}`,
    );
  }
});
