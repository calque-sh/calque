/**
 * The incident-operations dataset, `dataset.incident-ops-1m.v1`: the schema, the batched
 * seed, the sentinel rows every cut must withhold, and the oracle that
 * validates a seeded schema. The benchmark cuts its 100k rows from it,
 * and `pnpm incidents` seeds it whole.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const FIXTURE_SCHEMA = "incidents";
export const FIXTURE_DATABASE_ENV = "CALQUE_FIXTURE_DATABASE_URL";
export const FIXTURE_SOURCE_ROLE = "calque_sync";
export const DATASET_VERSION = "dataset.incident-ops-1m.v1";
const FIXTURE_SCHEMA_VERSION = "incidents.schema.v1";
const AUTHORIZATION_SCOPE_VERSION = "incidents-subject.v1";
const DATASET_SEED = 20_260_809;
export const CORRECTED_PROFILE_SHA256 =
  "63e6d576919dc93efa8ad02bf27ca2cd5a7c6e27c1d59fcb4ca34bb55341f601";
export const MAX_SEED_BATCH_ROWS = 1_000;
export const MAX_SEED_BATCH_BYTES = 4 * 1_024 * 1_024;
/**
 * The benchmark's share of every table's profile rows, as a divisor: the
 * first tenth by `bench_index`, 100k rows of the 1M profile.
 */
export const BENCH_PROFILE_DIVISOR = 10;

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const profilePath = resolve(
  root,
  "harness/profiles/datasets/incident-ops-1m.v1.json",
);

export interface FixtureTable {
  readonly name: FixtureTableName;
  readonly profileRows: number;
  readonly maximumSeedRowBytes: number;
  readonly columns: readonly string[];
  readonly selectExpressions: readonly string[];
}

export type FixtureTableName =
  | "events"
  | "issues"
  | "comments"
  | "issue_tags"
  | "users"
  | "projects"
  | "memberships";

interface SeedBatch {
  readonly table: FixtureTableName;
  readonly firstBenchIndex: number;
  readonly rowCount: number;
  readonly maximumEncodedBytes: number;
  readonly sql: string;
}

const events: FixtureTable = Object.freeze({
  name: "events",
  profileRows: 800_000,
  maximumSeedRowBytes: 1_600,
  columns: [
    "id",
    "organization_id",
    "project_id",
    "issue_id",
    "level",
    "message",
    "occurred_at",
    "payload",
    "bench_index",
    "is_sentinel",
  ],
  selectExpressions: [
    "g.bench_index + 1",
    "incidents.fixture_uuid(268435456, CASE WHEN (CASE WHEN (g.bench_index % 800) < 680 THEN (((g.bench_index / 800) * 85 + ((g.bench_index % 800) * 85 / 680)) / 100) ELSE (g.bench_index / 800) END) < 100 THEN CASE WHEN (g.bench_index % 800) < 680 THEN (((g.bench_index / 800) * 85 + ((g.bench_index % 800) * 85 / 680)) / 100) ELSE (g.bench_index / 800) END ELSE incidents.fixture_skew_rank(CASE WHEN (g.bench_index % 800) < 680 THEN (((g.bench_index / 800) * 85 + ((g.bench_index % 800) * 85 / 680)) / 100) ELSE (g.bench_index / 800) END, 100, 11) END)",
    "incidents.fixture_uuid(536870912, CASE WHEN (g.bench_index % 800) < 680 THEN (((g.bench_index / 800) * 85 + ((g.bench_index % 800) * 85 / 680)) / 100) ELSE (g.bench_index / 800) END)",
    "CASE WHEN (g.bench_index % 800) >= 680 THEN NULL ELSE incidents.fixture_uuid(805306368, (g.bench_index / 800) * 85 + ((g.bench_index % 800) * 85 / 680)) END",
    "(ARRAY['debug', 'info', 'warning', 'error', 'fatal'])[CASE WHEN (g.bench_index % 20) < 1 THEN 1 WHEN (g.bench_index % 20) < 8 THEN 2 WHEN (g.bench_index % 20) < 13 THEN 3 WHEN (g.bench_index % 20) < 19 THEN 4 ELSE 5 END]",
    "incidents.fixture_text('Captured incident', g.bench_index, 24, 320)",
    "timestamptz '2026-08-09 00:00:00+00' - (CASE WHEN (g.bench_index % 100) < 55 THEN incidents.fixture_hash(g.bench_index, 21) % (7 * 86400) ELSE 7 * 86400 + 1 + incidents.fixture_hash(g.bench_index, 22) % (83 * 86400 - 1) END) * interval '1 second'",
    "CASE WHEN g.bench_index = 0 THEN jsonb_build_object('blob', repeat('x', 1013)) WHEN g.bench_index = 1 THEN '{\"x\":\"12345678\"}'::jsonb ELSE jsonb_build_object('shape', g.bench_index % 24, 'trace', lpad(to_hex(g.bench_index), 8, '0'), 'latency_ms', (g.bench_index * 37) % 5000, 'sampled', (g.bench_index % 7) = 0) END",
    "g.bench_index",
    "false",
  ],
});

const issues: FixtureTable = Object.freeze({
  name: "issues",
  profileRows: 100_000,
  maximumSeedRowBytes: 700,
  columns: [
    "id",
    "organization_id",
    "project_id",
    "assignee_id",
    "status",
    "priority",
    "title",
    "event_count",
    "updated_at",
    "bench_index",
    "is_sentinel",
  ],
  selectExpressions: [
    "incidents.fixture_uuid(805306368, g.bench_index)",
    "incidents.fixture_uuid(268435456, CASE WHEN (g.bench_index / 100) < 100 THEN g.bench_index / 100 ELSE incidents.fixture_skew_rank(g.bench_index / 100, 100, 11) END)",
    "incidents.fixture_uuid(536870912, g.bench_index / 100)",
    "CASE WHEN (g.bench_index % 1000) >= 700 THEN NULL ELSE incidents.fixture_uuid(1073741824, (g.bench_index / 1000) * 65 + ((g.bench_index % 1000) * 65 / 700)) END",
    "(ARRAY['unresolved', 'investigating', 'resolved', 'ignored', 'archived'])[CASE WHEN (g.bench_index % 100) < 18 THEN 1 WHEN (g.bench_index % 100) < 25 THEN 2 WHEN (g.bench_index % 100) < 75 THEN 3 WHEN (g.bench_index % 100) < 85 THEN 4 ELSE 5 END]",
    "1 + (g.bench_index % 5)::integer",
    "incidents.fixture_text('Incident', g.bench_index, 24, 256)",
    "CASE WHEN (g.bench_index % 1000) < 500 THEN 1 + (g.bench_index % 5)::integer WHEN (g.bench_index % 1000) < 989 THEN 6 + (((g.bench_index % 1000) - 500) * 1993 / 489)::integer ELSE 2000 END",
    "timestamptz '2026-08-09 00:00:00+00' - (CASE WHEN (g.bench_index % 100) < 40 THEN incidents.fixture_hash(g.bench_index, 31) % (7 * 86400) ELSE 7 * 86400 + 1 + incidents.fixture_hash(g.bench_index, 32) % (358 * 86400 - 1) END) * interval '1 second'",
    "g.bench_index",
    "false",
  ],
});

const comments: FixtureTable = Object.freeze({
  name: "comments",
  profileRows: 60_000,
  maximumSeedRowBytes: 2_300,
  columns: [
    "id",
    "issue_id",
    "author_id",
    "body",
    "created_at",
    "bench_index",
    "is_sentinel",
  ],
  selectExpressions: [
    "incidents.fixture_uuid(1342177280, g.bench_index)",
    "incidents.fixture_uuid(805306368, CASE WHEN g.bench_index < 30000 THEN g.bench_index ELSE incidents.fixture_skew_rank(g.bench_index, 30000, 41) END)",
    "incidents.fixture_uuid(1073741824, g.bench_index / 10)",
    "CASE WHEN g.bench_index = 0 THEN repeat('x', 2048) WHEN g.bench_index = 1 THEN repeat('x', 4) WHEN (g.bench_index % 1000) <= 500 THEN repeat('x', 180) ELSE repeat('x', 181 + (incidents.fixture_hash(g.bench_index, 42) % 1867)::integer) END",
    "timestamptz '2026-08-09 00:00:00+00' - ((g.bench_index * 4513) % (365 * 86400)) * interval '1 second'",
    "g.bench_index",
    "false",
  ],
});

const issueTags: FixtureTable = Object.freeze({
  name: "issue_tags",
  profileRows: 20_000,
  maximumSeedRowBytes: 200,
  columns: ["issue_id", "tag", "bench_index", "is_sentinel"],
  selectExpressions: [
    "incidents.fixture_uuid(805306368, g.bench_index * 3 / 5)",
    "incidents.fixture_tag(CASE WHEN g.bench_index < 2 THEN g.bench_index::integer ELSE ((incidents.fixture_skew_rank(g.bench_index * 3 / 5, 200, 51) + CASE WHEN g.bench_index > 0 AND ((g.bench_index - 1) * 3 / 5) = (g.bench_index * 3 / 5) THEN 1 + (g.bench_index * 3 / 5) % 17 ELSE 0 END) % 200)::integer END)",
    "g.bench_index",
    "false",
  ],
});

const users: FixtureTable = Object.freeze({
  name: "users",
  profileRows: 8_000,
  maximumSeedRowBytes: 500,
  columns: [
    "id",
    "display_name",
    "email",
    "active",
    "bench_index",
    "is_sentinel",
  ],
  selectExpressions: [
    "incidents.fixture_uuid(1073741824, g.bench_index)",
    "incidents.fixture_text('Operator', g.bench_index, 1, 128)",
    "CASE WHEN g.bench_index = 0 THEN repeat('x', 247) || '@a.test' WHEN g.bench_index = 1 THEN 'a@b.co' ELSE 'operator.' || g.bench_index::text || '@example.test' END",
    "(g.bench_index % 100) >= 6",
    "g.bench_index",
    "false",
  ],
});

const projects: FixtureTable = Object.freeze({
  name: "projects",
  profileRows: 1_000,
  maximumSeedRowBytes: 400,
  columns: [
    "id",
    "organization_id",
    "slug",
    "name",
    "bench_index",
    "is_sentinel",
  ],
  selectExpressions: [
    "incidents.fixture_uuid(536870912, g.bench_index)",
    "incidents.fixture_uuid(268435456, CASE WHEN g.bench_index < 100 THEN g.bench_index ELSE incidents.fixture_skew_rank(g.bench_index, 100, 11) END)",
    "incidents.fixture_text('project', g.bench_index, 1, 63)",
    "incidents.fixture_text('Operations Project', g.bench_index, 1, 128)",
    "g.bench_index",
    "false",
  ],
});

const memberships: FixtureTable = Object.freeze({
  name: "memberships",
  profileRows: 11_000,
  maximumSeedRowBytes: 120,
  columns: ["organization_id", "user_id", "role", "bench_index", "is_sentinel"],
  selectExpressions: [
    "incidents.fixture_uuid(268435456, g.bench_index % 100)",
    "incidents.fixture_uuid(1073741824, g.bench_index * 8 / 11)",
    "(ARRAY['viewer', 'member', 'admin', 'owner'])[CASE WHEN (g.bench_index % 100) < 25 THEN 1 WHEN (g.bench_index % 100) < 87 THEN 2 WHEN (g.bench_index % 100) < 98 THEN 3 ELSE 4 END]",
    "g.bench_index",
    "false",
  ],
});

export const FIXTURE_TABLES: readonly FixtureTable[] = Object.freeze([
  events,
  issues,
  comments,
  issueTags,
  users,
  projects,
  memberships,
]);

const seedOrder: readonly FixtureTable[] = Object.freeze([
  users,
  projects,
  issues,
  comments,
  issueTags,
  memberships,
  events,
]);

export const fixtureDdl = String.raw`
DROP SCHEMA IF EXISTS incidents CASCADE;
CREATE SCHEMA incidents;

CREATE FUNCTION incidents.fixture_uuid(namespace integer, value bigint)
RETURNS uuid LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fixture$
  SELECT (lpad(to_hex(namespace), 8, '0') || '-0000-4000-8000-' || lpad(to_hex(value), 12, '0'))::uuid
$fixture$;

CREATE FUNCTION incidents.fixture_text(label text, bench_index bigint, minimum_bytes integer, maximum_bytes integer)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fixture$
  SELECT rpad(
    label || '-' || bench_index::text || '-',
    CASE WHEN bench_index = 0 THEN maximum_bytes WHEN bench_index = 1 THEN minimum_bytes
      ELSE minimum_bytes + ((bench_index * 1103515245 + 20260809) % (maximum_bytes - minimum_bytes + 1))::integer END,
    'x'
  )
$fixture$;

CREATE FUNCTION incidents.fixture_hash(bench_index bigint, salt bigint)
RETURNS bigint LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fixture$
  SELECT hashtextextended(bench_index::text, salt) & 4294967295
$fixture$;

CREATE FUNCTION incidents.fixture_skew_rank(bench_index bigint, domain integer, salt bigint)
RETURNS bigint LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fixture$
  SELECT floor(domain * power(incidents.fixture_hash(bench_index, salt)::double precision / 4294967296.0, 2.2))::bigint
$fixture$;

CREATE FUNCTION incidents.fixture_tag(rank integer)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $fixture$
  SELECT CASE rank WHEN 0 THEN 'x' WHEN 1 THEN repeat('x', 64)
    ELSE 'tag-' || lpad(rank::text, 3, '0') END
$fixture$;

CREATE TABLE incidents.users (
  id uuid PRIMARY KEY, display_name text NOT NULL, email text NOT NULL,
  active boolean NOT NULL, bench_index bigint NOT NULL UNIQUE, is_sentinel boolean NOT NULL,
  CHECK (bench_index >= 0)
);
CREATE TABLE incidents.projects (
  id uuid PRIMARY KEY, organization_id uuid NOT NULL, slug text NOT NULL, name text NOT NULL,
  bench_index bigint NOT NULL UNIQUE, is_sentinel boolean NOT NULL, CHECK (bench_index >= 0)
);
CREATE TABLE incidents.issues (
  id uuid PRIMARY KEY, organization_id uuid NOT NULL,
  project_id uuid NOT NULL REFERENCES incidents.projects(id),
  assignee_id uuid REFERENCES incidents.users(id), status text NOT NULL,
  priority integer NOT NULL, title text NOT NULL, event_count integer NOT NULL,
  updated_at timestamptz NOT NULL, bench_index bigint NOT NULL UNIQUE,
  is_sentinel boolean NOT NULL, CHECK (bench_index >= 0)
);
CREATE TABLE incidents.comments (
  id uuid PRIMARY KEY, issue_id uuid NOT NULL REFERENCES incidents.issues(id),
  author_id uuid NOT NULL REFERENCES incidents.users(id), body text NOT NULL,
  created_at timestamptz NOT NULL, bench_index bigint NOT NULL UNIQUE,
  is_sentinel boolean NOT NULL, CHECK (bench_index >= 0)
);
CREATE TABLE incidents.issue_tags (
  issue_id uuid NOT NULL REFERENCES incidents.issues(id), tag text NOT NULL,
  bench_index bigint NOT NULL UNIQUE, is_sentinel boolean NOT NULL,
  PRIMARY KEY (issue_id, tag), CHECK (bench_index >= 0)
);
CREATE TABLE incidents.memberships (
  organization_id uuid NOT NULL, user_id uuid NOT NULL REFERENCES incidents.users(id),
  role text NOT NULL, bench_index bigint NOT NULL UNIQUE, is_sentinel boolean NOT NULL,
  PRIMARY KEY (organization_id, user_id), CHECK (bench_index >= 0)
);
CREATE TABLE incidents.events (
  id bigint PRIMARY KEY, organization_id uuid NOT NULL,
  project_id uuid NOT NULL REFERENCES incidents.projects(id),
  issue_id uuid REFERENCES incidents.issues(id), level text NOT NULL,
  message text NOT NULL, occurred_at timestamptz NOT NULL, payload jsonb NOT NULL,
  bench_index bigint NOT NULL UNIQUE, is_sentinel boolean NOT NULL,
  CHECK (bench_index >= 0)
);
-- The canonical change log. One row is one complete committed transaction, and
-- it is appended in that same transaction, so a reader that observes the row
-- observes exactly the data the row describes. The sequence is the fence: the
-- change endpoint may serve a cursor only after reading the log contiguously up
-- to it, and metadata.terminal_cursor advances in the same transaction, so a
-- fresh full build and a tail can never name different positions.
CREATE TABLE incidents.change_log (
  sequence bigint PRIMARY KEY CHECK (sequence > 0),
  cursor text NOT NULL UNIQUE, parent_cursor text NOT NULL,
  postgres_xid bigint NOT NULL CHECK (postgres_xid > 0),
  begin_lsn text NOT NULL CHECK (begin_lsn <> ''),
  commit_lsn text NOT NULL CHECK (commit_lsn <> ''),
  commit_time_micros bigint NOT NULL, operations jsonb NOT NULL
);
CREATE TABLE incidents.metadata (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  dataset_version text NOT NULL, schema_version text NOT NULL,
  authorization_scope_version text NOT NULL, dataset_profile_sha256 text NOT NULL,
  frozen_record_profile_sha256 text NOT NULL, seed bigint NOT NULL,
  profile_source_rows bigint NOT NULL, maximum_batch_rows integer NOT NULL,
  maximum_batch_bytes integer NOT NULL, terminal_cursor text NOT NULL
);
INSERT INTO incidents.metadata (
  dataset_version, schema_version, authorization_scope_version,
  dataset_profile_sha256, frozen_record_profile_sha256, seed,
  profile_source_rows, maximum_batch_rows, maximum_batch_bytes, terminal_cursor
) VALUES (
  'dataset.incident-ops-1m.v1', 'incidents.schema.v1', 'incidents-subject.v1',
  '63e6d576919dc93efa8ad02bf27ca2cd5a7c6e27c1d59fcb4ca34bb55341f601',
  '9cb392043d2a4bbe810b72eec9935534b2c968b5d6157400be201a5bdddbe972',
  20260809, 1000000, 1000, 4194304, 'C0'
);
`;

/** The one out-of-scope row per table that every cut must withhold. */
export const sentinelSql = String.raw`
BEGIN;
INSERT INTO incidents.users VALUES ('f0000000-0000-4000-8000-000000000001', 'sentinel', 'sentinel@unauthorized.test', true, 8000, true);
INSERT INTO incidents.projects VALUES ('f0000000-0000-4000-8000-000000000002', 'ffffffff-0000-4000-8000-000000000001', 'sentinel', 'Unauthorized sentinel', 1000, true);
INSERT INTO incidents.issues VALUES ('f0000000-0000-4000-8000-000000000003', 'ffffffff-0000-4000-8000-000000000001', 'f0000000-0000-4000-8000-000000000002', 'f0000000-0000-4000-8000-000000000001', 'unresolved', 5, 'Unauthorized sentinel issue', 1, timestamptz '2026-08-09 00:00:00+00', 100000, true);
INSERT INTO incidents.comments VALUES ('f0000000-0000-4000-8000-000000000004', 'f0000000-0000-4000-8000-000000000003', 'f0000000-0000-4000-8000-000000000001', 'sentinel', timestamptz '2026-08-09 00:00:00+00', 60000, true);
INSERT INTO incidents.issue_tags VALUES ('f0000000-0000-4000-8000-000000000003', 'sentinel', 20000, true);
INSERT INTO incidents.memberships VALUES ('ffffffff-0000-4000-8000-000000000001', 'f0000000-0000-4000-8000-000000000001', 'owner', 11000, true);
INSERT INTO incidents.events VALUES (-1, 'ffffffff-0000-4000-8000-000000000001', 'f0000000-0000-4000-8000-000000000002', 'f0000000-0000-4000-8000-000000000003', 'fatal', 'sentinel', timestamptz '2026-08-09 00:00:00+00', '{"sentinel":true}'::jsonb, 800000, true);
COMMIT;
`;

export function profileSha256(contents: string | Buffer): string {
  return createHash("sha256").update(contents).digest("hex");
}

/**
 * What the seed and the validation need from outside this process: the
 * profile the fixture's hashes are bound to, and a PostgreSQL to run SQL
 * against. A test injects both.
 */
export interface FixtureSeams {
  /** Reads the incident-operations dataset profile the fixture's hashes are bound to. */
  readonly readProfile: () => string | Buffer;
  /** Runs SQL against the source PostgreSQL and returns psql's stdout. */
  readonly runSql: (sql: string, environment: NodeJS.ProcessEnv) => string;
}

export const defaultFixtureSeams: FixtureSeams = Object.freeze({
  readProfile: (): Buffer => readFileSync(profilePath),
  runSql: runPsql,
});

export function assertCorrectedProfile(
  readProfile: FixtureSeams["readProfile"] = defaultFixtureSeams.readProfile,
): void {
  const actual = profileSha256(readProfile());
  if (actual !== CORRECTED_PROFILE_SHA256) {
    throw new Error(
      `corrected incident-operations profile hash is ${actual}, expected ${CORRECTED_PROFILE_SHA256}`,
    );
  }
}

export function seedBatches(): readonly SeedBatch[] {
  return seedOrder.flatMap((table) => {
    const rowsPerBatch = Math.min(
      MAX_SEED_BATCH_ROWS,
      Math.floor(MAX_SEED_BATCH_BYTES / table.maximumSeedRowBytes),
    );
    const batches: SeedBatch[] = [];
    for (let first = 0; first < table.profileRows; first += rowsPerBatch) {
      const rowCount = Math.min(rowsPerBatch, table.profileRows - first);
      const last = first + rowCount - 1;
      const sql = `BEGIN;\nINSERT INTO incidents.${table.name} (${table.columns.join(", ")})\nSELECT ${table.selectExpressions.join(",\n  ")}\nFROM generate_series(${first}::bigint, ${last}::bigint) AS g(bench_index);\nCOMMIT;\n`;
      batches.push(
        Object.freeze({
          table: table.name,
          firstBenchIndex: first,
          rowCount,
          maximumEncodedBytes: rowCount * table.maximumSeedRowBytes,
          sql,
        }),
      );
    }
    return batches;
  });
}

export function fixtureSeedSql(): string {
  return `${fixtureDdl}\n${seedBatches()
    .map((batch) => batch.sql)
    .join("\n")}\n${sentinelSql}`;
}

const postgresVariables = [
  "PGHOST",
  "PGHOSTADDR",
  "PGPORT",
  "PGDATABASE",
  "PGUSER",
  "PGPASSWORD",
  "PGSERVICE",
  "PGSERVICEFILE",
  "PGSYSCONFDIR",
  "PGTARGETSESSIONATTRS",
  "PGLOADBALANCEHOSTS",
  "PGSSLMODE",
] as const;

export function postgresEnvironmentFromUrl(
  value: string,
  baseEnvironment: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${FIXTURE_DATABASE_ENV} must be a PostgreSQL URL`);
  }
  if (
    (url.protocol !== "postgresql:" && url.protocol !== "postgres:") ||
    url.hostname.length === 0 ||
    url.username.length === 0 ||
    url.pathname.length <= 1 ||
    url.pathname.slice(1).includes("/") ||
    url.hash.length !== 0
  ) {
    throw new Error(`${FIXTURE_DATABASE_ENV} must be a PostgreSQL TCP URL`);
  }
  const allowedParameters = new Set(["sslmode"]);
  for (const key of url.searchParams.keys()) {
    if (!allowedParameters.has(key)) {
      throw new Error(
        `${FIXTURE_DATABASE_ENV} has unsupported connection parameter ${key}`,
      );
    }
  }
  const environment: NodeJS.ProcessEnv = { ...baseEnvironment };
  for (const variable of postgresVariables) delete environment[variable];
  environment.PGHOST = url.hostname;
  environment.PGPORT = url.port || "5432";
  environment.PGDATABASE = decodeURIComponent(url.pathname.slice(1));
  environment.PGUSER = decodeURIComponent(url.username);
  environment.PGPASSWORD = decodeURIComponent(url.password);
  const sslmode = url.searchParams.get("sslmode");
  if (sslmode !== null) environment.PGSSLMODE = sslmode;
  return environment;
}

export const psqlArguments = Object.freeze([
  "--no-psqlrc",
  "--set",
  "ON_ERROR_STOP=1",
  "--quiet",
  "--tuples-only",
  "--no-align",
]);

function runPsql(sql: string, environment: NodeJS.ProcessEnv): string {
  const result = spawnSync("psql", psqlArguments, {
    env: environment,
    encoding: "utf8",
    input: sql,
    maxBuffer: 64 * 1_024 * 1_024,
  });
  if (result.error) {
    const diagnostic = result.stderr.trim();
    throw new Error(
      diagnostic.length === 0
        ? `psql could not run: ${result.error.message}`
        : `psql could not run: ${result.error.message}; ${diagnostic}`,
    );
  }
  if (result.status !== 0) {
    const diagnostic = result.stderr.trim();
    throw new Error(
      diagnostic.length === 0
        ? `psql exited with status ${String(result.status)}`
        : `psql failed: ${diagnostic}`,
    );
  }
  return result.stdout.trim();
}

function requiredDatabaseEnvironment(
  environment: NodeJS.ProcessEnv,
): NodeJS.ProcessEnv {
  const value = environment[FIXTURE_DATABASE_ENV];
  if (!value) throw new Error(`${FIXTURE_DATABASE_ENV} is required`);
  return postgresEnvironmentFromUrl(value, environment);
}

/**
 * The environment fields the preflight reports; `assertPreflight` refuses a
 * report missing any one of them.
 */
export const preflightExpression = String.raw`jsonb_build_object(
  'role', current_user,
  'superuser', (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
  'serverVersionNum', current_setting('server_version_num')::integer,
  'serverEncoding', current_setting('server_encoding'),
  'timeZone', current_setting('TimeZone'),
  'walLevel', current_setting('wal_level'),
  'maxReplicationSlots', current_setting('max_replication_slots')::integer,
  'maxWalSenders', current_setting('max_wal_senders')::integer
)`;

export const preflightSql = `SELECT ${preflightExpression}::text;\n`;

/**
 * The fixture oracle's named checks, each a SQL boolean expression.
 * `validationSql` asks all of them; the benchmark reuses the ones that
 * hold at its 100k cut.
 */
export function fixtureValidationChecks(): readonly (readonly [
  string,
  string,
])[] {
  return [
    ["role", `current_user = '${FIXTURE_SOURCE_ROLE}'`],
    [
      "non_superuser",
      "NOT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)",
    ],
    [
      "postgres_18",
      "current_setting('server_version_num')::integer BETWEEN 180000 AND 189999",
    ],
    ["utf8", "current_setting('server_encoding') = 'UTF8'"],
    ["utc", "current_setting('TimeZone') = 'UTC'"],
    ["logical_wal", "current_setting('wal_level') = 'logical'"],
    [
      "replication_slots",
      "current_setting('max_replication_slots')::integer > 0",
    ],
    ["wal_senders", "current_setting('max_wal_senders')::integer > 0"],
    [
      "metadata",
      `(SELECT count(*) = 1 AND bool_and(dataset_version = '${DATASET_VERSION}' AND schema_version = '${FIXTURE_SCHEMA_VERSION}' AND authorization_scope_version = '${AUTHORIZATION_SCOPE_VERSION}' AND dataset_profile_sha256 = '${CORRECTED_PROFILE_SHA256}' AND seed = ${DATASET_SEED} AND profile_source_rows = 1000000 AND maximum_batch_rows = ${MAX_SEED_BATCH_ROWS} AND maximum_batch_bytes = ${MAX_SEED_BATCH_BYTES}) FROM incidents.metadata)`,
    ],
    ...FIXTURE_TABLES.flatMap(
      (table) =>
        [
          [
            `${table.name}_count`,
            `(SELECT count(*) FILTER (WHERE NOT is_sentinel) = ${table.profileRows} AND count(*) FILTER (WHERE is_sentinel) = 1 FROM incidents.${table.name})`,
          ],
          [
            `${table.name}_bench_index`,
            `(SELECT min(bench_index) FILTER (WHERE NOT is_sentinel) = 0 AND max(bench_index) FILTER (WHERE NOT is_sentinel) = ${table.profileRows - 1} AND count(DISTINCT bench_index) = ${table.profileRows + 1} AND bool_and((bench_index < ${table.profileRows}) <> is_sentinel) FROM incidents.${table.name})`,
          ],
        ] as const,
    ),
    [
      "events_fk",
      "NOT EXISTS (SELECT 1 FROM incidents.events e LEFT JOIN incidents.projects p ON p.id=e.project_id LEFT JOIN incidents.issues i ON i.id=e.issue_id WHERE p.id IS NULL OR (e.issue_id IS NOT NULL AND i.id IS NULL))",
    ],
    [
      "issues_fk",
      "NOT EXISTS (SELECT 1 FROM incidents.issues i LEFT JOIN incidents.projects p ON p.id=i.project_id LEFT JOIN incidents.users u ON u.id=i.assignee_id WHERE p.id IS NULL OR (i.assignee_id IS NOT NULL AND u.id IS NULL))",
    ],
    [
      "comments_fk",
      "NOT EXISTS (SELECT 1 FROM incidents.comments c LEFT JOIN incidents.issues i ON i.id=c.issue_id LEFT JOIN incidents.users u ON u.id=c.author_id WHERE i.id IS NULL OR u.id IS NULL)",
    ],
    [
      "issue_tags_fk",
      "NOT EXISTS (SELECT 1 FROM incidents.issue_tags t LEFT JOIN incidents.issues i ON i.id=t.issue_id WHERE i.id IS NULL)",
    ],
    [
      "memberships_fk",
      "NOT EXISTS (SELECT 1 FROM incidents.memberships m LEFT JOIN incidents.users u ON u.id=m.user_id WHERE u.id IS NULL)",
    ],
    [
      "events_cardinality",
      "(SELECT count(DISTINCT organization_id)=101 AND count(DISTINCT project_id) FILTER (WHERE NOT is_sentinel)=1000 AND count(DISTINCT issue_id) FILTER (WHERE NOT is_sentinel)=85000 AND count(DISTINCT level) FILTER (WHERE NOT is_sentinel)=5 FROM incidents.events)",
    ],
    [
      "issues_cardinality",
      "(SELECT count(DISTINCT organization_id)=101 AND count(DISTINCT project_id) FILTER (WHERE NOT is_sentinel)=1000 AND count(DISTINCT assignee_id) FILTER (WHERE NOT is_sentinel)=6500 AND count(DISTINCT status) FILTER (WHERE NOT is_sentinel)=5 FROM incidents.issues)",
    ],
    [
      "comments_cardinality",
      "(SELECT count(DISTINCT issue_id) FILTER (WHERE NOT is_sentinel)=30000 AND count(DISTINCT author_id) FILTER (WHERE NOT is_sentinel)=6000 FROM incidents.comments)",
    ],
    [
      "issue_tags_cardinality",
      "(SELECT count(DISTINCT issue_id) FILTER (WHERE NOT is_sentinel)=12000 AND count(DISTINCT tag) FILTER (WHERE NOT is_sentinel)=200 FROM incidents.issue_tags)",
    ],
    [
      "users_cardinality",
      "(SELECT count(DISTINCT id) FILTER (WHERE NOT is_sentinel)=8000 AND count(DISTINCT active) FILTER (WHERE NOT is_sentinel)=2 FROM incidents.users)",
    ],
    [
      "projects_cardinality",
      "(SELECT count(DISTINCT id) FILTER (WHERE NOT is_sentinel)=1000 AND count(DISTINCT organization_id) FILTER (WHERE NOT is_sentinel)=100 FROM incidents.projects)",
    ],
    [
      "memberships_cardinality",
      "(SELECT count(DISTINCT organization_id) FILTER (WHERE NOT is_sentinel)=100 AND count(DISTINCT user_id) FILTER (WHERE NOT is_sentinel)=8000 AND count(DISTINCT role) FILTER (WHERE NOT is_sentinel)=4 FROM incidents.memberships)",
    ],
    [
      "null_fractions",
      "(SELECT count(*) FILTER (WHERE issue_id IS NULL AND NOT is_sentinel)=120000 FROM incidents.events) AND (SELECT count(*) FILTER (WHERE assignee_id IS NULL AND NOT is_sentinel)=30000 FROM incidents.issues)",
    ],
    [
      "events_widths",
      "(SELECT min(octet_length(message)) FILTER (WHERE NOT is_sentinel)=24 AND max(octet_length(message)) FILTER (WHERE NOT is_sentinel)=320 AND min(octet_length(replace(replace(payload::text, ': ', ':'), ', ', ','))) FILTER (WHERE NOT is_sentinel)=16 AND max(octet_length(replace(replace(payload::text, ': ', ':'), ', ', ','))) FILTER (WHERE NOT is_sentinel)=1024 FROM incidents.events)",
    ],
    [
      "issues_widths",
      "(SELECT min(octet_length(title)) FILTER (WHERE NOT is_sentinel)=24 AND max(octet_length(title)) FILTER (WHERE NOT is_sentinel)=256 AND min(octet_length(status)) FILTER (WHERE NOT is_sentinel)=7 AND max(octet_length(status)) FILTER (WHERE NOT is_sentinel)=13 FROM incidents.issues)",
    ],
    [
      "comments_widths",
      "(SELECT min(octet_length(body)) FILTER (WHERE NOT is_sentinel)=4 AND max(octet_length(body)) FILTER (WHERE NOT is_sentinel)=2048 FROM incidents.comments)",
    ],
    [
      "issue_tags_widths",
      "(SELECT min(octet_length(tag)) FILTER (WHERE NOT is_sentinel)=1 AND max(octet_length(tag)) FILTER (WHERE NOT is_sentinel)=64 FROM incidents.issue_tags)",
    ],
    [
      "users_widths",
      "(SELECT min(octet_length(display_name)) FILTER (WHERE NOT is_sentinel)=1 AND max(octet_length(display_name)) FILTER (WHERE NOT is_sentinel)=128 AND min(octet_length(email)) FILTER (WHERE NOT is_sentinel)=6 AND max(octet_length(email)) FILTER (WHERE NOT is_sentinel)=254 FROM incidents.users)",
    ],
    [
      "projects_widths",
      "(SELECT min(octet_length(slug)) FILTER (WHERE NOT is_sentinel)=1 AND max(octet_length(slug)) FILTER (WHERE NOT is_sentinel)=63 AND min(octet_length(name)) FILTER (WHERE NOT is_sentinel)=1 AND max(octet_length(name)) FILTER (WHERE NOT is_sentinel)=128 FROM incidents.projects)",
    ],
    [
      "memberships_widths",
      "(SELECT min(octet_length(role)) FILTER (WHERE NOT is_sentinel)=5 AND max(octet_length(role)) FILTER (WHERE NOT is_sentinel)=6 FROM incidents.memberships)",
    ],
    [
      "event_last_seven_days",
      "(SELECT avg((occurred_at >= timestamptz '2026-08-02 00:00:00+00')::integer) FILTER (WHERE NOT is_sentinel) = 0.55 FROM incidents.events)",
    ],
    [
      "issue_last_seven_days",
      "(SELECT avg((updated_at >= timestamptz '2026-08-02 00:00:00+00')::integer) FILTER (WHERE NOT is_sentinel) = 0.40 FROM incidents.issues)",
    ],
    [
      "issue_event_count_summary",
      "(SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY event_count) FILTER (WHERE NOT is_sentinel) = 5 AND percentile_disc(0.99) WITHIN GROUP (ORDER BY event_count) FILTER (WHERE NOT is_sentinel) = 2000 FROM incidents.issues)",
    ],
    [
      "comment_body_median",
      "(SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY octet_length(body)) FILTER (WHERE NOT is_sentinel) = 180 FROM incidents.comments)",
    ],
    [
      "organization_skew",
      "(WITH frequencies AS (SELECT organization_id, count(*)::numeric AS frequency FROM incidents.projects WHERE NOT is_sentinel GROUP BY organization_id) SELECT max(frequency) / sum(frequency) BETWEEN 0.08 AND 0.17 AND max(frequency) >= 5 * min(frequency) FROM frequencies)",
    ],
    [
      "comment_issue_skew",
      "(WITH frequencies AS (SELECT issue_id, count(*) AS frequency FROM incidents.comments WHERE NOT is_sentinel GROUP BY issue_id) SELECT max(frequency) >= 20 * min(frequency) FROM frequencies)",
    ],
    [
      "tag_skew",
      "(WITH frequencies AS (SELECT tag, count(*) AS frequency FROM incidents.issue_tags WHERE NOT is_sentinel GROUP BY tag) SELECT max(frequency) >= 5 * min(frequency) FROM frequencies)",
    ],
    [
      "payload_shapes",
      "(SELECT count(DISTINCT payload ->> 'shape') FILTER (WHERE NOT is_sentinel AND payload ? 'shape') = 24 FROM incidents.events)",
    ],
    ["tier_100k_closure", benchClosureExpression()],
  ];
}

export function validationSql(): string {
  const checks = fixtureValidationChecks();
  const checkRows = checks
    .map(([name, expression]) => `('${name}', (${expression}))`)
    .join(",\n      ");
  const summaries = String.raw`jsonb_build_object(
    'eventLastSevenDaysFraction', (SELECT avg((occurred_at >= timestamptz '2026-08-02 00:00:00+00')::integer) FILTER (WHERE NOT is_sentinel) FROM incidents.events),
    'issueLastSevenDaysFraction', (SELECT avg((updated_at >= timestamptz '2026-08-02 00:00:00+00')::integer) FILTER (WHERE NOT is_sentinel) FROM incidents.issues),
    'issueEventCountMedian', (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY event_count) FILTER (WHERE NOT is_sentinel) FROM incidents.issues),
    'issueEventCountP99', (SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY event_count) FILTER (WHERE NOT is_sentinel) FROM incidents.issues),
    'commentBodyMedianBytes', (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY octet_length(body)) FILTER (WHERE NOT is_sentinel) FROM incidents.comments),
    'largestProjectOrganizationFraction', (WITH frequencies AS (SELECT organization_id, count(*)::numeric AS frequency FROM incidents.projects WHERE NOT is_sentinel GROUP BY organization_id) SELECT max(frequency) / sum(frequency) FROM frequencies),
    'commentIssueMaxMinRatio', (WITH frequencies AS (SELECT issue_id, count(*)::numeric AS frequency FROM incidents.comments WHERE NOT is_sentinel GROUP BY issue_id) SELECT max(frequency) / min(frequency) FROM frequencies),
    'tagMaxMinRatio', (WITH frequencies AS (SELECT tag, count(*)::numeric AS frequency FROM incidents.issue_tags WHERE NOT is_sentinel GROUP BY tag) SELECT max(frequency) / min(frequency) FROM frequencies)
  )`;
  return `SELECT jsonb_build_object(\n  'environment', ${preflightExpression},\n  'summaries', ${summaries},\n  'checks', (SELECT jsonb_object_agg(check_name, passed) FROM (VALUES\n      ${checkRows}\n    ) AS fixture_checks(check_name, passed))\n)::text;\n`;
}

/** Whether the benchmark's cut holds every row its foreign keys name. */
function benchClosureExpression(): string {
  const limit = (name: FixtureTableName): number => {
    const table = FIXTURE_TABLES.find((candidate) => candidate.name === name);
    if (!table) throw new Error(`missing table ${name}`);
    return table.profileRows / BENCH_PROFILE_DIVISOR;
  };
  return `NOT EXISTS (
    SELECT 1 FROM incidents.events e
    WHERE e.bench_index < ${limit("events")} AND NOT e.is_sentinel
      AND (NOT EXISTS (SELECT 1 FROM incidents.projects p WHERE p.id=e.project_id AND p.bench_index < ${limit("projects")} AND NOT p.is_sentinel)
        OR (e.issue_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM incidents.issues i WHERE i.id=e.issue_id AND i.bench_index < ${limit("issues")} AND NOT i.is_sentinel)))
    UNION ALL SELECT 1 FROM incidents.issues i
    WHERE i.bench_index < ${limit("issues")} AND NOT i.is_sentinel
      AND (NOT EXISTS (SELECT 1 FROM incidents.projects p WHERE p.id=i.project_id AND p.bench_index < ${limit("projects")} AND NOT p.is_sentinel)
        OR (i.assignee_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM incidents.users u WHERE u.id=i.assignee_id AND u.bench_index < ${limit("users")} AND NOT u.is_sentinel)))
    UNION ALL SELECT 1 FROM incidents.comments c
    WHERE c.bench_index < ${limit("comments")} AND NOT c.is_sentinel
      AND (NOT EXISTS (SELECT 1 FROM incidents.issues i WHERE i.id=c.issue_id AND i.bench_index < ${limit("issues")} AND NOT i.is_sentinel)
        OR NOT EXISTS (SELECT 1 FROM incidents.users u WHERE u.id=c.author_id AND u.bench_index < ${limit("users")} AND NOT u.is_sentinel))
    UNION ALL SELECT 1 FROM incidents.issue_tags t
    WHERE t.bench_index < ${limit("issue_tags")} AND NOT t.is_sentinel
      AND NOT EXISTS (SELECT 1 FROM incidents.issues i WHERE i.id=t.issue_id AND i.bench_index < ${limit("issues")} AND NOT i.is_sentinel)
    UNION ALL SELECT 1 FROM incidents.memberships m
    WHERE m.bench_index < ${limit("memberships")} AND NOT m.is_sentinel
      AND NOT EXISTS (SELECT 1 FROM incidents.users u WHERE u.id=m.user_id AND u.bench_index < ${limit("users")} AND NOT u.is_sentinel)
  )`;
}

interface JsonObject {
  readonly [key: string]: unknown;
}

function objectValue(value: unknown, context: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${context} is not a JSON object`);
  }
  return value as JsonObject;
}

function parseJsonObject(value: string, context: string): JsonObject {
  try {
    return objectValue(JSON.parse(value), context);
  } catch (error) {
    if (error instanceof Error && error.message.endsWith("JSON object"))
      throw error;
    throw new Error(`${context} is not valid JSON`);
  }
}

interface ValidationResult {
  readonly valid: boolean;
  readonly failedChecks: readonly string[];
  readonly environment: JsonObject;
  readonly summaries: JsonObject;
  readonly checks: Readonly<Record<string, boolean>>;
}

export function evaluateValidationReport(value: string): ValidationResult {
  const report = parseJsonObject(value, "fixture validation report");
  const environment = objectValue(report.environment, "validation environment");
  const summaries =
    report.summaries === undefined
      ? Object.freeze({})
      : objectValue(report.summaries, "validation summaries");
  const rawChecks = objectValue(report.checks, "validation checks");
  const checks: Record<string, boolean> = {};
  for (const [name, passed] of Object.entries(rawChecks)) {
    if (typeof passed !== "boolean") {
      throw new Error(`validation check ${name} is not boolean`);
    }
    checks[name] = passed;
  }
  const failedChecks = Object.entries(checks)
    .filter(([, passed]) => !passed)
    .map(([name]) => name)
    .sort();
  return Object.freeze({
    valid: failedChecks.length === 0,
    failedChecks: Object.freeze(failedChecks),
    environment: Object.freeze({ ...environment }),
    summaries: Object.freeze({ ...summaries }),
    checks: Object.freeze(checks),
  });
}

function assertPreflight(value: string): void {
  const preflight = parseJsonObject(value, "PostgreSQL preflight");
  const role = preflight.role;
  const version = preflight.serverVersionNum;
  if (role !== FIXTURE_SOURCE_ROLE) {
    throw new Error(
      `PostgreSQL role is ${String(role)}, expected ${FIXTURE_SOURCE_ROLE}`,
    );
  }
  if (preflight.superuser !== false) {
    throw new Error(`${FIXTURE_SOURCE_ROLE} must not be a superuser`);
  }
  if (typeof version !== "number" || version < 180_000 || version >= 190_000) {
    throw new Error(
      `PostgreSQL 18 is required; server_version_num=${String(version)}`,
    );
  }
  for (const [name, expected] of [
    ["serverEncoding", "UTF8"],
    ["timeZone", "UTC"],
    ["walLevel", "logical"],
  ] as const) {
    if (preflight[name] !== expected)
      throw new Error(
        `PostgreSQL ${name} is ${String(preflight[name])}, expected ${expected}`,
      );
  }
  if (
    typeof preflight.maxReplicationSlots !== "number" ||
    preflight.maxReplicationSlots <= 0 ||
    typeof preflight.maxWalSenders !== "number" ||
    preflight.maxWalSenders <= 0
  ) {
    throw new Error("PostgreSQL logical replication capacity is disabled");
  }
}

export function seedFixture(
  environment: NodeJS.ProcessEnv = process.env,
  seams: FixtureSeams = defaultFixtureSeams,
): void {
  assertCorrectedProfile(seams.readProfile);
  const postgresEnvironment = requiredDatabaseEnvironment(environment);
  assertPreflight(seams.runSql(preflightSql, postgresEnvironment));
  seams.runSql(fixtureSeedSql(), postgresEnvironment);
  const validation = validateFixture(environment, seams);
  if (!validation.valid) {
    throw new Error(
      `seeded fixture failed validation: ${validation.failedChecks.join(", ")}`,
    );
  }
}

export function validateFixture(
  environment: NodeJS.ProcessEnv = process.env,
  seams: FixtureSeams = defaultFixtureSeams,
): ValidationResult {
  assertCorrectedProfile(seams.readProfile);
  const postgresEnvironment = requiredDatabaseEnvironment(environment);
  return evaluateValidationReport(
    seams.runSql(validationSql(), postgresEnvironment),
  );
}
