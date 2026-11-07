/**
 * The benchmark's dataset and its PostgreSQL oracle.
 *
 * The dataset is the incident-operations fixture cut to a prefix: every table's first
 * tenth of its 1M profile rows by `bench_index`, plus the fixture's sentinel
 * rows, cut from v1, whose text is padding, or from content-v1
 * (`bench-content-fixture.ts`). The cut, the probes and the load are the
 * same rows in both. The oracle reads every row of the seven tables from
 * PostgreSQL and compares it with the rows the page's local engine returns
 * through the generated facade. Both sides write a row in the same canonical
 * text: `json_build_array` of one text cell per column, hashed with MD5 and
 * keyed by the primary key's cells.
 */
import { createHash } from "node:crypto";

import {
  CONTENT_COLUMNS,
  CONTENT_DATASET_VERSION,
  contentSelectExpressions,
  contentUuidFunctionSql,
  createContentGenerator,
  eventTimeMicros,
  timestampLiteral,
  type ContentGenerator,
} from "./bench-content-fixture.ts";
import {
  BENCH_PROFILE_DIVISOR,
  DATASET_VERSION,
  FIXTURE_TABLES,
  fixtureDdl,
  fixtureValidationChecks,
  MAX_SEED_BATCH_BYTES,
  MAX_SEED_BATCH_ROWS,
  preflightExpression,
  sentinelSql,
  type FixtureTable,
  type FixtureTableName,
} from "./bench-fixture.ts";
import { runSql } from "./bench-postgres.ts";

/** Each `--dataset` name's dataset id. */
export const DATASETS = Object.freeze({
  "content-v1": CONTENT_DATASET_VERSION,
  v1: DATASET_VERSION,
} as const);
export type DatasetName = keyof typeof DATASETS;

/** The fixture's seed order: every table after the tables it references. */
export const SEED_ORDER: readonly FixtureTableName[] = Object.freeze([
  "users",
  "projects",
  "issues",
  "comments",
  "issue_tags",
  "memberships",
  "events",
]);

/** One sentinel row per table. */
const SENTINEL_ROWS_PER_TABLE = 1;

function fixtureTable(name: FixtureTableName): FixtureTable {
  const table = FIXTURE_TABLES.find((candidate) => candidate.name === name);
  if (!table) throw new Error(`the fixture has no table ${name}`);
  return table;
}

/** Rows each table holds in the benchmark's cut, sentinels excluded. */
export const BENCH_ROW_COUNTS: Readonly<Record<FixtureTableName, number>> =
  (() => {
    const counts = {} as Record<FixtureTableName, number>;
    for (const name of SEED_ORDER) {
      const rows = fixtureTable(name).profileRows / BENCH_PROFILE_DIVISOR;
      if (!Number.isInteger(rows))
        throw new Error(`${name} does not divide into the benchmark's cut`);
      counts[name] = rows;
    }
    return Object.freeze(counts);
  })();

/** The fixture's tables with each one's profile rows cut to the benchmark's. */
export const BENCH_TABLES: readonly FixtureTable[] = Object.freeze(
  FIXTURE_TABLES.map((table) =>
    Object.freeze({ ...table, profileRows: BENCH_ROW_COUNTS[table.name] }),
  ),
);

/**
 * v1's checks that hold at the cut as v1 writes them: the environment, the
 * metadata row, every foreign key and the cut's closure, and the widths,
 * payload shapes, recency fractions and medians the generator fixes. The
 * width bounds are rows 0 and 1 of each table, and the rest repeat every 24,
 * 100 or 1,000 rows, which divide the cut's counts. The other v1 checks fix
 * the 1M profile's counts, cardinalities, null counts and skews; a prefix
 * keeps none of them, and its tag skew holds only by the hash's draw.
 */
const CUT_CHECKS = Object.freeze([
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
]);

/**
 * v1's checks at the benchmark's cut, as `[name, expression]`: the ones that
 * hold there in v1's own text, and each table's count and bench_index checks
 * at the cut's rows.
 */
export function benchValidationChecks(): readonly (readonly [
  string,
  string,
])[] {
  const v1 = new Map(fixtureValidationChecks());
  return [
    ...CUT_CHECKS.map((name) => {
      const expression = v1.get(name);
      if (expression === undefined) throw new Error(`v1 has no check ${name}`);
      return [name, expression] as const;
    }),
    ...BENCH_TABLES.flatMap(
      (table) =>
        [
          [
            `${table.name}_count`,
            `(SELECT count(*) FILTER (WHERE NOT is_sentinel) = ${String(table.profileRows)} AND count(*) FILTER (WHERE is_sentinel) = 1 FROM incidents.${table.name})`,
          ],
          [
            `${table.name}_bench_index`,
            `(SELECT min(bench_index) FILTER (WHERE NOT is_sentinel) = 0 AND max(bench_index) FILTER (WHERE NOT is_sentinel) = ${String(table.profileRows - 1)} AND count(DISTINCT bench_index) = ${String(table.profileRows + 1)} AND bool_and((bench_index < ${String(table.profileRows)}) <> is_sentinel) FROM incidents.${table.name})`,
          ],
        ] as const,
    ),
  ];
}

/** `benchValidationChecks` as a report in `validationSql`'s shape. */
export function benchValidationSql(): string {
  const rows = benchValidationChecks()
    .map(([name, expression]) => `('${name}', (${expression}))`)
    .join(",\n      ");
  return `SELECT jsonb_build_object(\n  'environment', ${preflightExpression},\n  'checks', (SELECT jsonb_object_agg(check_name, passed) FROM (VALUES\n      ${rows}\n    ) AS fixture_checks(check_name, passed))\n)::text;\n`;
}

const BENCH_LOAD_ROWS = 9_000;
/**
 * The unenrolled table the write load's out-of-scope row changes land in. It
 * lives in the fixture's schema so a reseed drops it with everything else,
 * and no `--table` names it, so `calque dev` neither publishes nor serves it.
 */
export const benchLoadDdl = `CREATE TABLE incidents.bench_load (
  slot integer PRIMARY KEY,
  touched bigint NOT NULL
);
INSERT INTO incidents.bench_load (slot, touched)
SELECT slot, 0 FROM generate_series(0, ${String(BENCH_LOAD_ROWS - 1)}) AS s(slot);
`;

/** One batch of a table's prefix, in the fixture's own batch shape. */
function seedBatchSql(
  table: FixtureTable,
  first: number,
  last: number,
): string {
  return `BEGIN;\nINSERT INTO incidents.${table.name} (${table.columns.join(", ")})\nSELECT ${table.selectExpressions.join(",\n  ")}\nFROM generate_series(${String(first)}::bigint, ${String(last)}::bigint) AS g(bench_index);\nCOMMIT;\n`;
}

/**
 * The whole cut: the fixture's schema from scratch, each table's prefix in
 * seed order under the schema's foreign keys, the sentinels, and the load
 * table. Batches follow `seedBatches`, so the cut is seeded the way the
 * fixture is.
 */
export function benchSeedSql(): string {
  const counts = BENCH_ROW_COUNTS;
  const batches: string[] = [];
  for (const name of SEED_ORDER) {
    const table = fixtureTable(name);
    const rowsPerBatch = Math.min(
      MAX_SEED_BATCH_ROWS,
      Math.floor(MAX_SEED_BATCH_BYTES / table.maximumSeedRowBytes),
    );
    for (let first = 0; first < counts[name]; first += rowsPerBatch) {
      const last = Math.min(first + rowsPerBatch, counts[name]) - 1;
      batches.push(seedBatchSql(table, first, last));
    }
  }
  return `${fixtureDdl}\n${batches.join("\n")}\n${sentinelSql}\n${benchLoadDdl}`;
}

/** Counts per table, for the seed's own check: `table<TAB>count` lines. */
export const tableCountsSql = `${SEED_ORDER.map(
  (name) => `SELECT '${name}', count(*) FROM incidents.${name}`,
).join("\nUNION ALL\n")};\n`;

/** Whether PostgreSQL holds exactly the cut's rows, sentinels included. */
export function benchCountProblems(
  counted: ReadonlyMap<string, number>,
): string[] {
  const counts = BENCH_ROW_COUNTS;
  const problems: string[] = [];
  for (const name of SEED_ORDER) {
    const expected = counts[name] + SENTINEL_ROWS_PER_TABLE;
    const actual = counted.get(name);
    if (actual !== expected)
      problems.push(
        `${name} holds ${String(actual)} rows, expected ${String(expected)}`,
      );
  }
  return problems;
}

// The events a realtime probe inserts -----------------------------------------

/**
 * The `bench_index` of the k-th probe event, just past the cut's events. The
 * + 1 keeps clear of the index past them, which the 1M fixture's sentinel
 * holds.
 *
 * The fixture gives the event at index b an issue only while b % 800 < 680,
 * and that issue, (b / 800) * 85 + (b % 800) * 85 / 680, and its project stay
 * inside the cut when b / 800 is the cut's event count / 800. The cut's event
 * count is a multiple of 800, so probes 0 to 678 keep b % 800 in 1..679 and
 * insert under the schema's foreign keys.
 */
export function probeBenchIndex(probe: number): number {
  if (!Number.isInteger(probe) || probe < 0 || probe >= 679)
    throw new Error("a probe index must be an integer from 0 to 678");
  return BENCH_ROW_COUNTS.events + 1 + probe;
}

let generator: ContentGenerator | undefined;

/**
 * One probe's insert, built by the dataset's own `events` expressions. A
 * content-v1 probe keys its row by `pg_temp.content_uuid`, which
 * `probeSessionSql` creates, and draws its content as the seed would for its
 * bench_index: the message cut to v1's octet length for the row, the time,
 * which a probe past the cut's events places at the newest instant, and the
 * payload's values.
 */
export function probeInsertSql(dataset: DatasetName, probe: number): string {
  const events = fixtureTable("events");
  const benchIndex = probeBenchIndex(probe);
  const insert = `INSERT INTO incidents.events (${events.columns.join(", ")})\n`;
  const row = `(SELECT ${String(benchIndex)}::bigint AS bench_index) AS g`;
  if (dataset === "v1")
    return `${insert}SELECT ${events.selectExpressions.join(",\n  ")}\nFROM ${row};\n`;
  generator ??= createContentGenerator();
  const salt = (column: string): number => {
    const content = CONTENT_COLUMNS.events?.find(
      (candidate) => candidate.column === column,
    );
    if (!content) throw new Error(`content-v1 does not generate ${column}`);
    return content.salt;
  };
  const message = generator.message(benchIndex, MESSAGE_MAX_BYTES);
  const occurredAt = timestampLiteral(
    eventTimeMicros(
      benchIndex,
      BENCH_ROW_COUNTS.events,
      generator.unit(salt("occurred_at"), benchIndex),
    ),
  );
  const payload = generator.payload(benchIndex);
  const v1Message = events.selectExpressions[events.columns.indexOf("message")];
  if (/['\\]/u.test(message) || v1Message === undefined)
    throw new Error("a probe's message cannot be quoted");
  return `${insert}SELECT ${contentSelectExpressions(events).join(",\n  ")}\nFROM (SELECT g.bench_index, left('${message}', octet_length(${v1Message})) AS message, timestamptz '${occurredAt}' AS occurred_at, '${payload.trace}'::text AS payload_trace, ${String(payload.latencyMs)} AS payload_latency_ms, ${String(payload.sampled)} AS payload_sampled FROM ${row}) AS g;\n`;
}

/** v1's widest `events.message`; a content message is cut from one this long. */
const MESSAGE_MAX_BYTES = 320;

/** What a probe session runs once, before its first probe. */
export function probeSessionSql(dataset: DatasetName): string {
  return dataset === "v1" ? "" : contentUuidFunctionSql;
}

// The write load --------------------------------------------------------------

const LOAD_ISSUES_PER_TRANSACTION = 3;
const LOAD_COMMENTS_PER_TRANSACTION = 2;
const LOAD_OUT_OF_SCOPE_PER_TRANSACTION = 45;
/** 10% above the threshold's 10,000 row changes/s. */
export const LOAD_TRANSACTIONS_PER_SECOND = 220;
export const LOAD_ROW_CHANGES_PER_TRANSACTION =
  LOAD_ISSUES_PER_TRANSACTION +
  LOAD_COMMENTS_PER_TRANSACTION +
  LOAD_OUT_OF_SCOPE_PER_TRANSACTION;

function rotating(
  transaction: number,
  count: number,
  modulus: number,
): number[] {
  return Array.from(
    { length: count },
    (_, offset) => (transaction * count + offset) % modulus,
  );
}

const bigintArray = (values: readonly number[]): string =>
  `ARRAY[${values.map((value) => `${String(value)}::bigint`).join(", ")}]`;

/**
 * The load's n-th transaction. The two enrolled updates rewrite a text prefix
 * in place, so a title or body keeps its length; `bench_load` takes the
 * out-of-scope changes. Targets rotate so the load does not keep rewriting one
 * page of each table.
 */
export function loadTransactionSql(transaction: number): string {
  if (!Number.isSafeInteger(transaction) || transaction < 0)
    throw new Error("a load transaction index must be a non-negative integer");
  const counts = BENCH_ROW_COUNTS;
  const marker = `load-${String(transaction)}-`;
  const issues = rotating(
    transaction,
    LOAD_ISSUES_PER_TRANSACTION,
    counts.issues,
  );
  const comments = rotating(
    transaction,
    LOAD_COMMENTS_PER_TRANSACTION,
    counts.comments,
  );
  const slots = rotating(
    transaction,
    LOAD_OUT_OF_SCOPE_PER_TRANSACTION,
    BENCH_LOAD_ROWS,
  );
  return `BEGIN;
UPDATE incidents.issues SET title = overlay(title placing '${marker}' from 1) WHERE bench_index = ANY (${bigintArray(issues)}) AND NOT is_sentinel;
UPDATE incidents.comments SET body = overlay(body placing '${marker}' from 1) WHERE bench_index = ANY (${bigintArray(comments)}) AND NOT is_sentinel;
UPDATE incidents.bench_load SET touched = touched + 1 WHERE slot = ANY (ARRAY[${slots.join(", ")}]);
COMMIT;
`;
}

/**
 * `--tail`'s writes: the load's first `transactions` transactions, each
 * committed on its own, from one loop in the server so they commit as fast as
 * PostgreSQL takes them. Each rewrites the rows `loadTransactionSql` would,
 * with `tail-<n>-` as its marker. Commits do not wait for the WAL flush,
 * which changes when the writer goes on and not what a replica decodes.
 */
export function tailSql(transactions: number): string {
  if (!Number.isSafeInteger(transactions) || transactions < 1)
    throw new Error("a tail must hold at least one transaction");
  const rotatingSql = (count: number, modulus: number, type: string): string =>
    `ARRAY[${Array.from({ length: count }, (_, offset) => `((n * ${String(count)} + ${String(offset)}) % ${String(modulus)})::${type}`).join(", ")}]`;
  const counts = BENCH_ROW_COUNTS;
  return `SET synchronous_commit = off;
DO $tail$
BEGIN
  FOR n IN 0..${String(transactions - 1)} LOOP
    UPDATE incidents.issues SET title = overlay(title placing 'tail-' || n || '-' from 1) WHERE bench_index = ANY (${rotatingSql(LOAD_ISSUES_PER_TRANSACTION, counts.issues, "bigint")}) AND NOT is_sentinel;
    UPDATE incidents.comments SET body = overlay(body placing 'tail-' || n || '-' from 1) WHERE bench_index = ANY (${rotatingSql(LOAD_COMMENTS_PER_TRANSACTION, counts.comments, "bigint")}) AND NOT is_sentinel;
    UPDATE incidents.bench_load SET touched = touched + 1 WHERE slot = ANY (${rotatingSql(LOAD_OUT_OF_SCOPE_PER_TRANSACTION, BENCH_LOAD_ROWS, "integer")});
    COMMIT;
  END LOOP;
END
$tail$;
`;
}

// Canonical rows --------------------------------------------------------------

/**
 * `DirectColumnarColumnKind`'s names for the kinds the fixture uses, with
 * `int32` for the `int2` and `int4` columns the page reads as numbers.
 */
type OracleKind =
  "uuid" | "utf8" | "int32" | "int64" | "bool" | "timestamp" | "json";

interface OracleColumn {
  readonly name: string;
  readonly kind: OracleKind;
}

export interface OracleTable {
  /** The facade collection, `db.<collection>`. */
  readonly collection: string;
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly OracleColumn[];
  /** Indexes into `columns`, in primary-key order. */
  readonly primaryKey: readonly number[];
}

const KIND_BY_POSTGRES_TYPE: Readonly<Record<string, OracleKind>> =
  Object.freeze({
    "pg_catalog.uuid": "uuid",
    "pg_catalog.text": "utf8",
    "pg_catalog.varchar": "utf8",
    "pg_catalog.int2": "int32",
    "pg_catalog.int4": "int32",
    "pg_catalog.int8": "int64",
    "pg_catalog.bool": "bool",
    "pg_catalog.timestamptz": "timestamp",
    "pg_catalog.jsonb": "json",
    "pg_catalog.json": "json",
  });

interface InspectionColumn {
  readonly name: string;
  readonly typeSchema: string;
  readonly typeName: string;
  readonly primaryKeyPosition?: number;
}

/**
 * The oracle's tables from the page's pinned configuration and inspection:
 * the collections the page mounts, and each column's PostgreSQL type as the
 * bindings were generated from it.
 */
export function oracleTables(
  configJson: string,
  inspectionJson: string,
): readonly OracleTable[] {
  const config = JSON.parse(configJson) as {
    readonly collections?: readonly {
      readonly id?: string;
      readonly relation?: { readonly schema?: string; readonly table?: string };
    }[];
  };
  const inspection = JSON.parse(inspectionJson) as {
    readonly relations?: readonly {
      readonly schema: string;
      readonly table: string;
      readonly columns: readonly InspectionColumn[];
    }[];
  };
  const tables = (config.collections ?? []).map((collection) => {
    const schema = collection.relation?.schema;
    const table = collection.relation?.table;
    if (!collection.id || !schema || !table)
      throw new Error("calque.config.json declares an incomplete collection");
    const relation = inspection.relations?.find(
      (candidate) => candidate.schema === schema && candidate.table === table,
    );
    if (!relation)
      throw new Error(`the inspection has no relation ${schema}.${table}`);
    const columns = relation.columns.map((column) => {
      const kind =
        KIND_BY_POSTGRES_TYPE[`${column.typeSchema}.${column.typeName}`];
      if (!kind)
        throw new Error(
          `the oracle has no canonical text for ${schema}.${table}.${column.name} (${column.typeName})`,
        );
      return Object.freeze({ name: column.name, kind });
    });
    const primaryKey = relation.columns
      .map((column, index) => ({ index, position: column.primaryKeyPosition }))
      .filter(
        (entry): entry is { index: number; position: number } =>
          typeof entry.position === "number",
      )
      .sort((left, right) => left.position - right.position)
      .map((entry) => entry.index);
    if (primaryKey.length === 0)
      throw new Error(`${schema}.${table} has no primary key`);
    return Object.freeze({
      collection: collection.id,
      schema,
      table,
      columns: Object.freeze(columns),
      primaryKey: Object.freeze(primaryKey),
    });
  });
  if (tables.length === 0)
    throw new Error("calque.config.json declares no collections");
  return Object.freeze(tables);
}

function quoteIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

/** The SQL expression whose text equals `canonicalPageCell` for the same value. */
function postgresCell(kind: OracleKind, column: string): string {
  const c = quoteIdentifier(column);
  switch (kind) {
    case "uuid":
    case "utf8":
    case "int32":
    case "int64":
      return `${c}::text`;
    case "bool":
      return `CASE ${c} WHEN true THEN 'true' WHEN false THEN 'false' END`;
    case "timestamp":
      return `(extract(epoch FROM ${c}) * 1000000)::bigint::text`;
    case "json":
      return `${c}::jsonb::text`;
  }
}

/** PostgreSQL's `jsonb` output: keys by byte length then bytes, `", "` and `": "`. */
export function jsonbText(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || Object.is(value, -0))
      throw new Error(`JSON number ${String(value)} has no exact jsonb text`);
    return String(value);
  }
  if (Array.isArray(value))
    return `[${value.map((item) => jsonbText(item)).join(", ")}]`;
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort((left, right) => {
      const a = Buffer.from(left);
      const b = Buffer.from(right);
      return a.length - b.length || Buffer.compare(a, b);
    });
    return `{${keys.map((key) => `${JSON.stringify(key)}: ${jsonbText(record[key])}`).join(", ")}}`;
  }
  throw new Error(`unexpected decoded JSON value ${typeof value}`);
}

/** A row's cells as `json_build_array` prints them. */
export function cellArray(cells: readonly (string | null)[]): string {
  return `[${cells.map((cell) => (cell === null ? "null" : JSON.stringify(cell))).join(", ")}]`;
}

/** Every row of `table` as `key<TAB>md5(row)`, in PostgreSQL's canonical text. */
export function referenceSql(table: OracleTable): string {
  const cells = table.columns.map((column) =>
    postgresCell(column.kind, column.name),
  );
  const key = table.primaryKey.map((index) => cells[index]);
  return `SELECT json_build_array(${key.join(", ")})::text, md5(json_build_array(${cells.join(", ")})::text) FROM "${table.schema}"."${table.table}";\n`;
}

/** `referenceSql`'s output as a key-to-digest map; a repeated key is refused. */
export function parseReference(output: string): Map<string, string> {
  const rows = new Map<string, string>();
  for (const line of output.split("\n")) {
    if (line.length === 0) continue;
    const separator = line.lastIndexOf("\t");
    const key = line.slice(0, separator);
    const digest = line.slice(separator + 1);
    if (separator <= 0 || !/^[0-9a-f]{32}$/u.test(digest))
      throw new Error(`malformed reference row ${line.slice(0, 200)}`);
    if (rows.has(key)) throw new Error(`the reference repeats key ${key}`);
    rows.set(key, digest);
  }
  return rows;
}

/** One digest over a whole table's rows, whatever order they were read in. */
export function tableDigest(rows: ReadonlyMap<string, string>): string {
  const hash = createHash("sha256");
  for (const key of [...rows.keys()].sort())
    hash.update(`${key}\t${rows.get(key) ?? ""}\n`);
  return hash.digest("hex");
}

/**
 * PostgreSQL's `extract(epoch FROM t) * 1000000` for a facade timestamp,
 * which is ISO 8601 in UTC with a millisecond or microsecond fraction.
 */
export function isoTimestampMicros(text: string): string {
  const match =
    /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z$/u.exec(text);
  const whole = match?.[1];
  if (!whole) throw new Error(`timestamp ${text} is not ISO 8601 UTC`);
  const wholeMs = Date.parse(`${whole}Z`);
  if (!Number.isSafeInteger(wholeMs))
    throw new Error(`timestamp ${text} is out of range`);
  const fraction = BigInt((match[2] ?? "").padEnd(6, "0"));
  return (BigInt(wholeMs) * 1000n + fraction).toString();
}

/**
 * The canonical text of one cell as the page sends it: the client hands a
 * UUID over as its text and an `int2` or `int4` as a number, and the page has
 * already turned an `int64` into decimal, because a `bigint` does not survive
 * `JSON.stringify`.
 */
export function canonicalPageCell(
  kind: OracleKind,
  value: unknown,
): string | null {
  if (value === null) return null;
  switch (kind) {
    case "uuid":
      if (
        typeof value !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u.test(
          value,
        )
      )
        throw new Error(`a uuid cell arrived as ${JSON.stringify(value)}`);
      return value;
    case "utf8":
      if (typeof value !== "string")
        throw new Error(`a text cell arrived as ${typeof value}`);
      return value;
    case "int32":
      if (!Number.isSafeInteger(value) || Object.is(value, -0))
        throw new Error(`an int32 cell arrived as ${JSON.stringify(value)}`);
      return String(value);
    case "int64":
      if (typeof value !== "string" || !/^-?(0|[1-9][0-9]*)$/u.test(value))
        throw new Error(`an int64 cell arrived as ${JSON.stringify(value)}`);
      return value;
    case "bool":
      if (typeof value !== "boolean")
        throw new Error(`a bool cell arrived as ${typeof value}`);
      return value ? "true" : "false";
    case "timestamp":
      if (typeof value !== "string")
        throw new Error(`a timestamp cell arrived as ${typeof value}`);
      return isoTimestampMicros(value);
    case "json":
      return jsonbText(value);
  }
}

const MAX_REPORTED_DIFFERENCES = 10;

export interface TableVerdict {
  readonly collection: string;
  readonly pageRows: number;
  readonly referenceRows: number;
  readonly differenceCount: number;
  readonly differences: readonly {
    readonly key: string;
    readonly kind:
      "missing" | "extra" | "different" | "repeated" | "unreadable";
    readonly detail?: string;
  }[];
}

/**
 * The page's rows of one table against PostgreSQL's, fed one row at a time so
 * a million rows never sit in memory as text.
 */
export class TableComparison {
  readonly #table: OracleTable;
  readonly #reference: ReadonlyMap<string, string>;
  readonly #seen = new Set<string>();
  readonly #differences: TableVerdict["differences"][number][] = [];
  #differenceCount = 0;
  #pageRows = 0;

  constructor(table: OracleTable, reference: ReadonlyMap<string, string>) {
    this.#table = table;
    this.#reference = reference;
  }

  #note(entry: TableVerdict["differences"][number]): void {
    this.#differenceCount += 1;
    if (this.#differences.length < MAX_REPORTED_DIFFERENCES)
      this.#differences.push(entry);
  }

  add(values: readonly unknown[]): void {
    this.#pageRows += 1;
    if (values.length !== this.#table.columns.length) {
      this.#note({
        key: `row ${String(this.#pageRows)}`,
        kind: "unreadable",
        detail: `${String(values.length)} cells for ${String(this.#table.columns.length)} columns`,
      });
      return;
    }
    let cells: (string | null)[];
    try {
      cells = this.#table.columns.map((column, index) =>
        canonicalPageCell(column.kind, values[index]),
      );
    } catch (error) {
      this.#note({
        key: `row ${String(this.#pageRows)}`,
        kind: "unreadable",
        detail: error instanceof Error ? error.message : String(error),
      });
      return;
    }
    const key = cellArray(
      this.#table.primaryKey.map((index) => cells[index] ?? null),
    );
    if (this.#seen.has(key)) {
      this.#note({ key, kind: "repeated" });
      return;
    }
    this.#seen.add(key);
    const expected = this.#reference.get(key);
    if (expected === undefined) {
      this.#note({ key, kind: "extra" });
      return;
    }
    const digest = createHash("md5")
      .update(cellArray(cells), "utf8")
      .digest("hex");
    if (digest !== expected)
      this.#note({ key, kind: "different", detail: cellArray(cells) });
  }

  finish(): TableVerdict {
    for (const key of this.#reference.keys())
      if (!this.#seen.has(key)) this.#note({ key, kind: "missing" });
    return Object.freeze({
      collection: this.#table.collection,
      pageRows: this.#pageRows,
      referenceRows: this.#reference.size,
      differenceCount: this.#differenceCount,
      differences: Object.freeze([...this.#differences]),
    });
  }
}

// The chart ---------------------------------------------------------------------

export interface Bucket {
  readonly status: string;
  readonly priority: string;
  readonly count: number;
}

/** `GROUP BY status, priority` as `status<TAB>priority<TAB>count` lines. */
export const bucketsSql =
  "SELECT status, priority::text, count(*) FROM incidents.issues GROUP BY status, priority;\n";

export function parseBuckets(output: string): Bucket[] {
  return output
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => {
      const [status, priority, count] = line.split("\t");
      if (status === undefined || priority === undefined || !count)
        throw new Error(`malformed bucket row ${line}`);
      return { status, priority, count: Number(count) };
    });
}

/** Buckets in one order, whichever order either side produced them in. */
export function normalizeBuckets(buckets: readonly Bucket[]): Bucket[] {
  return buckets
    .map(({ status, priority, count }) => ({
      status,
      priority: String(priority),
      count,
    }))
    .sort((left, right) =>
      left.status === right.status
        ? Number(left.priority) - Number(right.priority)
        : left.status < right.status
          ? -1
          : 1,
    );
}

const ENUMERATION_WINDOW = 10_000;

/** How many times the oracle reads the page's working set before its DNC. */
const ORACLE_ENUMERATIONS = 4;

type Reference = ReadonlyMap<string, ReadonlyMap<string, string>>;

export interface SourceState {
  readonly reference: Reference;
  readonly digests: Readonly<Record<string, string>>;
  readonly events: number;
  readonly buckets: readonly Bucket[];
}

export function readSourceState(
  tables: readonly OracleTable[],
  environment: NodeJS.ProcessEnv,
): SourceState {
  const reference = new Map<string, ReadonlyMap<string, string>>();
  const digests: Record<string, string> = {};
  for (const table of tables) {
    const rows = parseReference(runSql(referenceSql(table), environment));
    reference.set(table.collection, rows);
    digests[table.collection] = tableDigest(rows);
  }
  return {
    reference,
    digests,
    events: Number(
      runSql("SELECT count(*) FROM incidents.events;\n", environment).trim(),
    ),
    buckets: normalizeBuckets(parseBuckets(runSql(bucketsSql, environment))),
  };
}

export interface OracleResult {
  readonly passed: boolean;
  /**
   * Why no enumeration of the page held still, or null. It is the oracle's
   * `DNC`: `tables` are then the last enumeration's, and `problems` is empty.
   */
  readonly unsettled: string | null;
  /** Each enumeration's publication count before and after it, in order. */
  readonly enumerations: readonly Enumeration[];
  readonly tables: readonly TableVerdict[];
  readonly problems: readonly string[];
}

interface Enumeration {
  readonly before: number | null;
  readonly after: number | null;
  /** SHA-256 over the rows the enumeration read, in the order it read them. */
  readonly digest: string;
}

/** What the oracle reads from the page. */
export interface OraclePage {
  /** The page's count of publications that moved a collection. */
  publications(): Promise<number | null>;
  /** Rows `start` to `start + count` of `collection`, each as `columns`' cells. */
  slice(
    collection: string,
    columns: readonly string[],
    start: number,
    count: number,
  ): Promise<readonly (readonly unknown[])[]>;
}

/**
 * The page's whole working set against PostgreSQL. A batch applied mid-read
 * moves rows between windows, and one can reach the engine before the page
 * counts it, so an enumeration counts only when the publication count holds
 * across it and the one before it read the same rows at that count. The second
 * is judged. With no such pair within `attempts`, the result is unsettled: a
 * `DNC`, never `INCORRECT`.
 */
export async function pageOracle(
  page: OraclePage,
  tables: readonly OracleTable[],
  reference: Reference,
  {
    window = ENUMERATION_WINDOW,
    attempts = ORACLE_ENUMERATIONS,
  }: { readonly window?: number; readonly attempts?: number } = {},
): Promise<OracleResult> {
  const enumerations: Enumeration[] = [];
  let verdicts: TableVerdict[] = [];
  let settled = false;
  for (let attempt = 0; attempt < attempts && !settled; attempt += 1) {
    const before = await page.publications();
    if (before === null) break;
    const digest = createHash("sha256");
    verdicts = [];
    for (const table of tables) {
      const rows = reference.get(table.collection);
      if (!rows) throw new Error(`no reference for ${table.collection}`);
      const comparison = new TableComparison(table, rows);
      const columns = table.columns.map((column) => column.name);
      digest.update(`${JSON.stringify(table.collection)}\n`);
      for (let start = 0; ; start += window) {
        const read = await page.slice(table.collection, columns, start, window);
        for (const row of read) {
          comparison.add(row);
          digest.update(`${JSON.stringify(row)}\n`);
        }
        if (read.length < window) break;
      }
      verdicts.push(comparison.finish());
    }
    const after = await page.publications();
    const previous = enumerations.at(-1);
    const enumeration = { before, after, digest: digest.digest("hex") };
    enumerations.push(enumeration);
    // The count only grows, so the two enumerations held one count from the
    // first one's start to the second one's end.
    settled =
      previous !== undefined &&
      previous.before === before &&
      after === before &&
      previous.digest === enumeration.digest;
  }
  if (!settled) {
    const counts = enumerations
      .map(
        (enumeration) =>
          `${String(enumeration.before)} to ${String(enumeration.after)}`,
      )
      .join(", ");
    return {
      passed: false,
      unsettled:
        enumerations.length === 0
          ? "the page reports no publication count to hold an enumeration against"
          : `no two enumerations in a row read the same rows at one publication count in ${String(enumerations.length)} (counts ${counts})`,
      enumerations,
      tables: verdicts,
      problems: [],
    };
  }
  const problems = verdicts
    .filter((verdict) => verdict.differenceCount > 0)
    .map(
      (verdict) =>
        `${verdict.collection}: ${String(verdict.differenceCount)} rows differ (${String(verdict.pageRows)} on the page, ${String(verdict.referenceRows)} in PostgreSQL)`,
    );
  return {
    passed: problems.length === 0,
    unsettled: null,
    enumerations,
    tables: verdicts,
    problems,
  };
}

/** The rows the events grid shows in `ids`, as PostgreSQL orders the same view. */
export function gridWindowSql(
  level: string,
  sort: "newest" | "oldest",
  offset: number,
  limit: number,
): string {
  if (!/^[a-z]+$/u.test(level)) throw new Error(`unexpected level ${level}`);
  const direction = sort === "newest" ? "DESC" : "ASC";
  const where = level === "all" ? "" : ` WHERE level = '${level}'`;
  return (
    `SELECT id FROM incidents.events${where}` +
    ` ORDER BY occurred_at ${direction}, id ${direction}` +
    ` OFFSET ${String(offset)} LIMIT ${String(limit)}`
  );
}

/**
 * The post-load query: the events grid's Level control set to `error` and its
 * Order control to newest first, which filters the 100k events and sorts the
 * matches. The page has no control that groups on demand; its grouped chart
 * is part of the cold load.
 */
export const QUERY_VIEW = Object.freeze({
  level: "error",
  sort: "newest",
} as const);

/** How many of the view's first ids PostgreSQL is asked for; the grid shows fewer. */
const QUERY_WINDOW = 200;

/** What PostgreSQL says the grid shows once the query has run. */
export interface QueryExpectation {
  /** The events matching the filter. */
  readonly rows: number;
  /** The first ids of the sorted matches. */
  readonly ids: readonly string[];
}

export function readQueryExpectation(
  environment: NodeJS.ProcessEnv,
): QueryExpectation {
  const rows = Number(
    runSql(
      `SELECT count(*) FROM incidents.events WHERE level = '${QUERY_VIEW.level}';\n`,
      environment,
    ).trim(),
  );
  const ids = runSql(
    `${gridWindowSql(QUERY_VIEW.level, QUERY_VIEW.sort, 0, QUERY_WINDOW)};\n`,
    environment,
  )
    .split("\n")
    .filter((line) => line.length > 0);
  return { rows, ids };
}

export function chartText(buckets: readonly Bucket[] | null): string {
  return JSON.stringify(buckets === null ? null : normalizeBuckets(buckets));
}
