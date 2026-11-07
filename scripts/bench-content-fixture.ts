/**
 * `dataset.incident-ops-1m.content-v1`: the incident-operations 1M fixture with
 * realistic content.
 *
 * v1 (`bench-fixture.ts`) pads its text with `x`, zero-fills most of
 * every UUID and scatters event times by hash, so its artifact compresses and
 * decodes unlike a real workspace's. This variant keeps everything v1's checks
 * fix, down to the octet length of every text value, and replaces only the
 * content: Zipf-ranked words and log templates for text, hashed version 4
 * UUIDs, times that rise with `bench_index`, and drawn payload values. It is
 * deterministic in `CONTENT_SEED` and each row's `bench_index`, and its
 * parameters are never tuned toward a compression ratio. v1's validation
 * passes unchanged, so the metadata row still names v1; the schema's comment
 * names the variant.
 *
 * `pnpm incidents --dataset content-v1` seeds the incident-operations source with it,
 * and `pnpm bench --dataset content-v1` cuts the benchmark's rows from it.
 */
import { spawn, type SpawnOptions } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";

import {
  assertCorrectedProfile,
  defaultFixtureSeams,
  FIXTURE_TABLES,
  fixtureDdl,
  MAX_SEED_BATCH_BYTES,
  MAX_SEED_BATCH_ROWS,
  FIXTURE_DATABASE_ENV,
  FIXTURE_SCHEMA,
  FIXTURE_SOURCE_ROLE,
  postgresEnvironmentFromUrl,
  preflightSql,
  psqlArguments,
  evaluateValidationReport,
  seedBatches,
  sentinelSql,
  validationSql,
  type FixtureSeams,
  type FixtureTable,
  type FixtureTableName,
} from "./bench-fixture.ts";

export const CONTENT_DATASET_VERSION = "dataset.incident-ops-1m.content-v1";
const CONTENT_SEED = 20_260_927;
export const VOCABULARY_WORDS = 5_000;
export const MESSAGE_TEMPLATES = 200;
const ZIPF_EXPONENT = 1;

/** v1's time anchor: every generated time lies before it. */
const ANCHOR_MICROS = Date.UTC(2026, 7, 9) * 1_000;
const DAY_MICROS = 86_400 * 1_000_000;

// A syllable is an onset, a nucleus and a coda; a word is one to four of them.
const ONSETS = [
  "",
  ..."b c d f g h j k l m n p r s t v w y z ch sh th st tr".split(" "),
];
const NUCLEI = ["a", "e", "i", "o", "u", "ea", "ou"];
const CODAS = ["", "n", "r", "s", "t", "l", "nd", "st"];
export const SYLLABLES: readonly string[] = Object.freeze(
  ONSETS.flatMap((onset) =>
    NUCLEI.flatMap((nucleus) => CODAS.map((coda) => onset + nucleus + coda)),
  ),
);

const PLACEHOLDER = /\{(hex|int|path)\}/u;

// Each random stream is seeded by its purpose, so adding a column never moves
// another column's content.
const SALT = Object.freeze({
  vocabulary: 1,
  templates: 2,
  eventsMessage: 3,
  eventsOccurredAt: 4,
  issuesTitle: 5,
  commentsBody: 6,
  commentsCreatedAt: 7,
  usersDisplayName: 8,
  projectsName: 9,
  eventsPayload: 10,
});

export const PAYLOAD_LATENCY_MEDIAN_MS = 40;
export const PAYLOAD_LATENCY_SIGMA = 1;
export const PAYLOAD_LATENCY_MAX_MS = 4_999;
export const PAYLOAD_SAMPLED_PROBABILITY = 1 / 7;

type ContentKind =
  "message" | "prose" | "event-time" | "comment-time" | "payload";

interface ContentColumn {
  readonly column: string;
  readonly kind: ContentKind;
  readonly salt: number;
}

/** The columns whose values this variant generates, by table. */
export const CONTENT_COLUMNS: Readonly<
  Partial<Record<FixtureTableName, readonly ContentColumn[]>>
> = Object.freeze({
  events: [
    { column: "message", kind: "message", salt: SALT.eventsMessage },
    { column: "occurred_at", kind: "event-time", salt: SALT.eventsOccurredAt },
    { column: "payload", kind: "payload", salt: SALT.eventsPayload },
  ],
  issues: [{ column: "title", kind: "prose", salt: SALT.issuesTitle }],
  comments: [
    { column: "body", kind: "prose", salt: SALT.commentsBody },
    {
      column: "created_at",
      kind: "comment-time",
      salt: SALT.commentsCreatedAt,
    },
  ],
  users: [
    { column: "display_name", kind: "prose", salt: SALT.usersDisplayName },
  ],
  projects: [{ column: "name", kind: "prose", salt: SALT.projectsName }],
});

/**
 * sfc32, seeded by four words and run past its first outputs. Distinct seeds
 * are distinct states, so no two rows share a stream.
 */
function sfc32(seed: number, salt: number, index: number): () => number {
  let a = seed >>> 0;
  let b = salt >>> 0;
  let c = index >>> 0;
  let d = 0x9e3779b9;
  const next = (): number => {
    const t = (((a + b) | 0) + d) | 0;
    d = (d + 1) | 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) | 0;
    c = (c << 21) | (c >>> 11);
    c = (c + t) | 0;
    return (t >>> 0) / 4_294_967_296;
  };
  for (let round = 0; round < 15; round += 1) next();
  return next;
}

/**
 * The index of a Zipf-distributed rank over `size` ranks: rank `k` (from 1)
 * has weight `k^-exponent`. It maps a uniform `unit` in [0, 1) to [0, size).
 */
export function zipfSampler(
  size: number,
  exponent: number,
): (unit: number) => number {
  const cumulative = new Float64Array(size);
  let total = 0;
  for (let rank = 1; rank <= size; rank += 1) {
    total += rank ** -exponent;
    cumulative[rank - 1] = total;
  }
  return (unit) => {
    const target = unit * total;
    let low = 0;
    let high = size - 1;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if ((cumulative[middle] ?? total) > target) high = middle;
      else low = middle + 1;
    }
    return low;
  };
}

export interface ContentGenerator {
  readonly seed: number;
  /** The vocabulary in rank order, most frequent first. */
  readonly vocabulary: readonly string[];
  /** The message templates in rank order, with `{hex}`, `{int}` and `{path}`. */
  readonly templates: readonly string[];
  /** Words, space-separated, cut to exactly `bytes`. */
  prose(salt: number, benchIndex: number, bytes: number): string;
  /** A filled template, continued with words and cut to exactly `bytes`. */
  message(benchIndex: number, bytes: number): string;
  /** One uniform draw in [0, 1) for this row and purpose. */
  unit(salt: number, benchIndex: number): number;
  /** The drawn values of the event payload at `benchIndex`. */
  payload(benchIndex: number): ContentPayload;
}

interface ContentPayload {
  readonly trace: string;
  readonly latencyMs: number;
  readonly sampled: boolean;
}

export function createContentGenerator(
  seed: number = CONTENT_SEED,
): ContentGenerator {
  const wordRank = zipfSampler(VOCABULARY_WORDS, ZIPF_EXPONENT);
  const templateRank = zipfSampler(MESSAGE_TEMPLATES, ZIPF_EXPONENT);

  const vocabularyNext = sfc32(seed, SALT.vocabulary, 0);
  const distinct = new Set<string>();
  while (distinct.size < VOCABULARY_WORDS) {
    const syllables = 1 + Math.floor(vocabularyNext() * 4);
    let word = "";
    for (let count = 0; count < syllables; count += 1)
      word += SYLLABLES[Math.floor(vocabularyNext() * SYLLABLES.length)];
    distinct.add(word);
  }
  // Shorter words take the frequent ranks, as in natural language (Zipf's law
  // of abbreviation); the sort is stable, so ties keep the order drawn.
  const vocabulary = Object.freeze(
    [...distinct].sort((left, right) => left.length - right.length),
  );
  const word = (next: () => number): string =>
    vocabulary[wordRank(next())] ?? "";

  const templateNext = sfc32(seed, SALT.templates, 0);
  const drawn = new Set<string>();
  while (drawn.size < MESSAGE_TEMPLATES) {
    const count = (minimum: number, maximum: number) =>
      minimum + Math.floor(templateNext() * (maximum - minimum + 1));
    const head = Array.from({ length: count(1, 3) }, () => word(templateNext));
    const fields = Array.from(
      { length: count(1, 4) },
      () =>
        `${word(templateNext)}={${(["hex", "int", "path"] as const)[count(0, 2)]}}`,
    );
    const tail = Array.from({ length: count(0, 3) }, () => word(templateNext));
    drawn.add([`${head.join(" ")}:`, ...fields, ...tail].join(" "));
  }
  const templates = Object.freeze([...drawn]);
  const templateParts = templates.map((template) =>
    template.split(PLACEHOLDER),
  );

  const extend = (text: string, next: () => number, bytes: number): string => {
    let result = text;
    while (result.length < bytes) result += ` ${word(next)}`;
    return result.slice(0, bytes);
  };
  const fill = (kind: string, next: () => number): string => {
    if (kind === "hex") {
      let hex = "";
      const length = 8 + Math.floor(next() * 9);
      for (let digit = 0; digit < length; digit += 1)
        hex += Math.floor(next() * 16).toString(16);
      return hex;
    }
    if (kind === "int") return String(Math.floor(10 ** (next() * 5)));
    const segments = 2 + Math.floor(next() * 3);
    return `/${Array.from({ length: segments }, () => word(next)).join("/")}`;
  };
  const requireBytes = (bytes: number): void => {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new Error(`cannot generate text of ${String(bytes)} bytes`);
  };

  return Object.freeze({
    seed,
    vocabulary,
    templates,
    prose(salt: number, benchIndex: number, bytes: number): string {
      requireBytes(bytes);
      const next = sfc32(seed, salt, benchIndex);
      return extend(word(next), next, bytes);
    },
    message(benchIndex: number, bytes: number): string {
      requireBytes(bytes);
      const next = sfc32(seed, SALT.eventsMessage, benchIndex);
      const parts = templateParts[templateRank(next())] ?? [];
      const text = parts
        .map((part, index) => (index % 2 === 0 ? part : fill(part, next)))
        .join("");
      return extend(text, next, bytes);
    },
    unit(salt: number, benchIndex: number): number {
      return sfc32(seed, salt, benchIndex)();
    },
    payload(benchIndex: number): ContentPayload {
      const next = sfc32(seed, SALT.eventsPayload, benchIndex);
      // One 32-bit draw is exactly 8 hex digits.
      const trace = (next() * 2 ** 32).toString(16).padStart(8, "0");
      // Box-Muller; 1 - u keeps the logarithm's argument in (0, 1].
      const normal =
        Math.sqrt(-2 * Math.log(1 - next())) * Math.cos(2 * Math.PI * next());
      const latencyMs = Math.min(
        PAYLOAD_LATENCY_MAX_MS,
        Math.max(
          0,
          Math.round(
            PAYLOAD_LATENCY_MEDIAN_MS *
              Math.exp(PAYLOAD_LATENCY_SIGMA * normal),
          ),
        ),
      );
      return {
        trace,
        latencyMs,
        sampled: next() < PAYLOAD_SAMPLED_PROBABILITY,
      };
    },
  });
}

/**
 * The version 4 UUID that replaces `fixture_uuid(namespace, value)`: the first
 * 16 bytes of the SHA-256 of `<CONTENT_DATASET_VERSION>:<namespace>:<value>`
 * in decimal, with the version nibble set to 4 and the variant bits to 10
 * (RFC 9562), leaving 122 bits of the digest. `contentUuidFunctionSql` is the
 * same function in PostgreSQL.
 */
export function contentUuid(namespace: number, value: number): string {
  const bytes = createHash("sha256")
    .update(`${CONTENT_DATASET_VERSION}:${namespace}:${value}`)
    .digest()
    .subarray(0, 16);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x40;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export const contentUuidFunctionSql = String.raw`
CREATE FUNCTION pg_temp.content_uuid(namespace integer, value bigint)
RETURNS uuid LANGUAGE plpgsql IMMUTABLE STRICT AS $content$
DECLARE
  digest bytea := substring(sha256(convert_to('${CONTENT_DATASET_VERSION}:' || namespace || ':' || value, 'UTF8')) FROM 1 FOR 16);
BEGIN
  RETURN encode(set_byte(set_byte(digest, 6, (get_byte(digest, 6) & 15) | 64), 8, (get_byte(digest, 8) & 63) | 128), 'hex')::uuid;
END
$content$;
`;

/**
 * When the event at `benchIndex` occurred, in microseconds since the epoch.
 * Events keep v1's split, 55% in the seven days before the anchor and the rest
 * in the 83 days before those, but rise with `benchIndex`: each row owns an
 * equal slot of its span, and `unit` places it inside the slot.
 */
export function eventTimeMicros(
  benchIndex: number,
  rows: number,
  unit: number,
): number {
  if (rows % 100 !== 0)
    throw new Error(`events need a row count divisible by 100, not ${rows}`);
  const recent = (rows / 100) * 55;
  const older = rows - recent;
  return benchIndex < older
    ? slotTime(
        ANCHOR_MICROS - 90 * DAY_MICROS,
        83 * DAY_MICROS,
        older,
        benchIndex,
        unit,
      )
    : slotTime(
        ANCHOR_MICROS - 7 * DAY_MICROS,
        7 * DAY_MICROS,
        recent,
        benchIndex - older,
        unit,
      );
}

/** When the comment at `benchIndex` was written: the year before the anchor. */
export function commentTimeMicros(
  benchIndex: number,
  rows: number,
  unit: number,
): number {
  return slotTime(
    ANCHOR_MICROS - 365 * DAY_MICROS,
    365 * DAY_MICROS,
    rows,
    benchIndex,
    unit,
  );
}

function slotTime(
  start: number,
  span: number,
  slots: number,
  slot: number,
  unit: number,
): number {
  // Clamped so that float rounding can never carry a row into the next span.
  return Math.min(
    start + span - 1,
    start + Math.floor(((slot + unit) * span) / slots),
  );
}

/** A `timestamptz` literal for microseconds since the epoch, in UTC. */
export function timestampLiteral(micros: number): string {
  const seconds = Math.floor(micros / 1_000_000);
  const fraction = String(micros - seconds * 1_000_000).padStart(6, "0");
  return `${new Date(seconds * 1_000).toISOString().slice(0, 19)}.${fraction}+00`;
}

/**
 * The three row-derived values in v1's `events.payload` expression, each with
 * the staged draw that replaces it and that draw's type.
 */
const PAYLOAD_DRAWS: readonly (readonly [
  v1: string,
  staged: string,
  type: string,
])[] = Object.freeze([
  ["lpad(to_hex(g.bench_index), 8, '0')", "payload_trace", "text"],
  ["(g.bench_index * 37) % 5000", "payload_latency_ms", "integer"],
  ["(g.bench_index % 7) = 0", "payload_sampled", "boolean"],
] as const);

/**
 * v1's `events.payload` expression with its drawn values read from the staged
 * row. The keys, `shape` and the arms for bench_index 0 and 1 stay v1's text.
 */
export function contentPayloadExpression(v1: string): string {
  let expression = v1;
  for (const [value, staged] of PAYLOAD_DRAWS) {
    if (expression.split(value).length !== 2)
      throw new Error(`v1's events.payload does not hold ${value} once`);
    expression = expression.replace(value, `g.${staged}`);
  }
  return expression;
}

/**
 * v1's select expressions with the content replaced: a generated column reads
 * the staged value `g.<column>` (the payload, its staged draws), and every
 * `fixture_uuid` call becomes `pg_temp.content_uuid` with the same arguments.
 * Everything else is v1's text.
 */
export function contentSelectExpressions(table: FixtureTable): string[] {
  const generated = new Map(
    (CONTENT_COLUMNS[table.name] ?? []).map((content) => [
      content.column,
      content,
    ]),
  );
  for (const column of generated.keys())
    if (!table.columns.includes(column))
      throw new Error(`${table.name} has no column ${column}`);
  return table.columns.map((column, index) => {
    const content = generated.get(column);
    if (content?.kind === "payload")
      return contentPayloadExpression(v1Expression(table, index));
    if (content) return `g.${column}`;
    return v1Expression(table, index).replaceAll(
      "incidents.fixture_uuid(",
      "pg_temp.content_uuid(",
    );
  });
}

function v1Expression(table: FixtureTable, index: number): string {
  const expression = table.selectExpressions[index];
  if (expression === undefined)
    throw new Error(`${table.name} has no expression ${index}`);
  return expression;
}

function v1ExpressionOf(table: FixtureTable, column: string): string {
  return v1Expression(table, table.columns.indexOf(column));
}

/**
 * The inclusive bench_index ranges v1 inserts each table in, one transaction
 * each, by the rule `seedBatches` applies.
 */
export function contentBatches(
  table: FixtureTable,
): (readonly [number, number])[] {
  const rowsPerBatch = Math.min(
    MAX_SEED_BATCH_ROWS,
    Math.floor(MAX_SEED_BATCH_BYTES / table.maximumSeedRowBytes),
  );
  const batches: (readonly [number, number])[] = [];
  for (let first = 0; first < table.profileRows; first += rowsPerBatch)
    batches.push([
      first,
      Math.min(first + rowsPerBatch, table.profileRows) - 1,
    ] as const);
  return batches;
}

/** v1's octet length of `column` for every row, in bench_index order. */
export function v1LengthsSql(table: FixtureTable, column: string): string {
  return `SELECT string_agg(octet_length(${v1ExpressionOf(table, column)})::text, ',' ORDER BY g.bench_index) FROM generate_series(0::bigint, ${table.profileRows - 1}::bigint) AS g(bench_index);\n`;
}

function parseLengths(output: string, rows: number, label: string): number[] {
  const lengths = output.trim().split(",").map(Number);
  if (
    lengths.length !== rows ||
    lengths.some((length) => !Number.isSafeInteger(length) || length < 0)
  )
    throw new Error(`v1's octet lengths for ${label} are not ${rows} integers`);
  return lengths;
}

const COPY_UNSAFE = /[\t\n\r\\]/u;

/** The temporary table's columns that hold `content`'s staged values. */
function stagedColumns(
  content: ContentColumn,
): readonly (readonly [name: string, type: string])[] {
  if (content.kind === "payload")
    return PAYLOAD_DRAWS.map(([, staged, type]) => [staged, type] as const);
  return [
    [content.column, content.kind.endsWith("time") ? "timestamptz" : "text"],
  ];
}

/**
 * The seeding session after `fixtureDdl`: per table in v1's seed order, the
 * generated values are copied into a temporary table, then inserted with v1's
 * other expressions in v1's transactions.
 */
function* contentSession(
  tables: readonly FixtureTable[],
  lengths: ReadonlyMap<string, readonly number[]>,
  generator: ContentGenerator,
): Generator<string> {
  yield contentUuidFunctionSql;
  for (const table of tables) {
    const contents = CONTENT_COLUMNS[table.name] ?? [];
    const insert = `INSERT INTO incidents.${table.name} (${table.columns.join(", ")})\nSELECT ${contentSelectExpressions(table).join(",\n  ")}\n`;
    if (contents.length === 0) {
      for (const [first, last] of contentBatches(table))
        yield `BEGIN;\n${insert}FROM generate_series(${first}::bigint, ${last}::bigint) AS g(bench_index);\nCOMMIT;\n`;
      continue;
    }
    const value = (content: ContentColumn, benchIndex: number): string => {
      if (content.kind === "payload") {
        const payload = generator.payload(benchIndex);
        return `${payload.trace}\t${payload.latencyMs}\t${payload.sampled ? "t" : "f"}`;
      }
      if (content.kind === "event-time" || content.kind === "comment-time") {
        const unit = generator.unit(content.salt, benchIndex);
        return timestampLiteral(
          content.kind === "event-time"
            ? eventTimeMicros(benchIndex, table.profileRows, unit)
            : commentTimeMicros(benchIndex, table.profileRows, unit),
        );
      }
      const bytes =
        lengths.get(`${table.name}.${content.column}`)?.[benchIndex] ?? -1;
      const text =
        content.kind === "message"
          ? generator.message(benchIndex, bytes)
          : generator.prose(content.salt, benchIndex, bytes);
      if (COPY_UNSAFE.test(text))
        throw new Error(
          `generated ${table.name}.${content.column} needs escaping`,
        );
      return text;
    };
    const staged = contents.flatMap(stagedColumns);
    const columns = staged.map(([name, type]) => `${name} ${type} NOT NULL`);
    yield `CREATE TEMP TABLE content_stage (bench_index bigint PRIMARY KEY, ${columns.join(", ")});\nCOPY content_stage (bench_index, ${staged.map(([name]) => name).join(", ")}) FROM STDIN;\n`;
    let rows: string[] = [];
    for (let benchIndex = 0; benchIndex < table.profileRows; benchIndex += 1) {
      rows.push(
        `${benchIndex}\t${contents.map((content) => value(content, benchIndex)).join("\t")}\n`,
      );
      if (rows.length === 4_096) {
        yield rows.join("");
        rows = [];
      }
    }
    yield `${rows.join("")}\\.\n`;
    for (const [first, last] of contentBatches(table))
      yield `BEGIN;\n${insert}FROM content_stage AS g\nWHERE g.bench_index BETWEEN ${first} AND ${last}\nORDER BY g.bench_index;\nCOMMIT;\n`;
    yield "DROP TABLE content_stage;\n";
  }
}

/**
 * The checks the variant adds to v1's, one count each, every one of which
 * must be 0: a generated text whose octet length differs from v1's for its
 * row, a generated text, payload or UUID that still equals v1's, a generated
 * time earlier than the row before it, a payload whose keys or `shape` differ
 * from v1's (or, at bench_index 0 and 1, any of it), and a drawn payload value
 * outside its type and range. `users.first.id` is the first user's id, for
 * comparison with `contentUuid`. Each table is read once for its counts.
 */
export function contentVerificationSql(
  tables: readonly FixtureTable[],
): string {
  const objects: string[] = [];
  for (const table of tables) {
    const counts: string[] = [];
    const count = (name: string, condition: string) =>
      counts.push(
        `'${table.name}.${name}', count(*) FILTER (WHERE ${condition})`,
      );
    for (const { column, kind } of CONTENT_COLUMNS[table.name] ?? []) {
      if (kind === "event-time" || kind === "comment-time") {
        objects.push(
          `(SELECT jsonb_build_object('${table.name}.${column}.descending', count(*) FILTER (WHERE ordered.earlier)) FROM (SELECT g.${column} < lag(g.${column}) OVER (ORDER BY g.bench_index) AS earlier FROM incidents.${table.name} AS g WHERE NOT g.is_sentinel) AS ordered)`,
        );
        continue;
      }
      const v1 = v1ExpressionOf(table, column);
      if (kind === "payload") {
        const keys = (json: string) =>
          `ARRAY(SELECT jsonb_object_keys(${json}) ORDER BY 1)`;
        const drawn = `g.bench_index >= 2`;
        count(
          `${column}.kept`,
          `CASE WHEN ${drawn} THEN g.${column} -> 'shape' IS DISTINCT FROM (${v1}) -> 'shape' OR ${keys(`g.${column}`)} <> ${keys(`(${v1})`)} ELSE g.${column} IS DISTINCT FROM (${v1}) END`,
        );
        count(
          `${column}.values`,
          `${drawn} AND NOT (jsonb_typeof(g.${column} -> 'trace') = 'string' AND g.${column} ->> 'trace' ~ '^[0-9a-f]{8}$' AND jsonb_typeof(g.${column} -> 'latency_ms') = 'number' AND g.${column} ->> 'latency_ms' ~ '^([0-9]{1,3}|[0-4][0-9]{3})$' AND jsonb_typeof(g.${column} -> 'sampled') = 'boolean')`,
        );
        count(`${column}.v1`, `${drawn} AND g.${column} = (${v1})`);
        continue;
      }
      count(
        `${column}.width`,
        `octet_length(g.${column}) <> octet_length(${v1})`,
      );
      count(`${column}.v1`, `g.${column} = ${v1}`);
    }
    table.columns.forEach((column, index) => {
      const v1 = v1Expression(table, index);
      if (v1.includes("incidents.fixture_uuid("))
        count(`${column}.v1`, `g.${column} = ${v1}`);
    });
    if (counts.length > 0)
      objects.push(
        `(SELECT jsonb_build_object(${counts.join(", ")}) FROM incidents.${table.name} AS g WHERE NOT g.is_sentinel)`,
      );
  }
  objects.push(
    "jsonb_build_object('users.first.id', (SELECT id::text FROM incidents.users WHERE bench_index = 0 AND NOT is_sentinel))",
  );
  return `SELECT (\n  ${objects.join("\n  || ")}\n)::text;\n`;
}

/** The namespace v1's `users.id` expression passes to `fixture_uuid`. */
function usersNamespace(tables: readonly FixtureTable[]): number {
  const users = tables.find((table) => table.name === "users");
  const match = users
    ? /^incidents\.fixture_uuid\((\d+), g\.bench_index\)$/u.exec(
        v1ExpressionOf(users, "id"),
      )
    : null;
  if (!match?.[1])
    throw new Error("v1's users.id is not fixture_uuid(n, g.bench_index)");
  return Number(match[1]);
}

/** Why the variant's own checks refuse the seed; empty when they pass. */
function contentVerificationProblems(
  report: string,
  tables: readonly FixtureTable[],
): string[] {
  const values = JSON.parse(report) as Record<string, unknown>;
  const problems: string[] = [];
  const expectedId = contentUuid(usersNamespace(tables), 0);
  for (const [name, value] of Object.entries(values)) {
    if (name === "users.first.id") {
      if (value !== expectedId)
        problems.push(
          `the first user's id is ${String(value)} in PostgreSQL and ${expectedId} in contentUuid`,
        );
    } else if (value !== 0) {
      problems.push(`${name}: ${String(value)} rows`);
    }
  }
  if (!("users.first.id" in values))
    problems.push("the report has no users.first.id");
  return problems;
}

const markerSql = (marker: string) =>
  `COMMENT ON SCHEMA ${FIXTURE_SCHEMA} IS '${marker}';\n`;

/** The schema comment while a seed is under way; a finished seed replaces it. */
export const CONTENT_SEEDING_MARKER = `${CONTENT_DATASET_VERSION} (seeding)`;

/**
 * The schema's comment, or '' without one: `CONTENT_DATASET_VERSION` once a
 * seed passed, `CONTENT_SEEDING_MARKER` while one runs or after one failed.
 */
export const schemaMarkerSql = `SELECT coalesce(obj_description('${FIXTURE_SCHEMA}'::regnamespace, 'pg_namespace'), '');`;

export interface ContentFixtureSeams extends FixtureSeams {
  /**
   * Runs one psql session fed by `chunks`, which holds SQL and inline `COPY`
   * data, and returns psql's stdout.
   */
  readonly streamSql: (
    chunks: Iterable<string>,
    environment: NodeJS.ProcessEnv,
  ) => Promise<string>;
}

export const defaultContentFixtureSeams: ContentFixtureSeams = Object.freeze({
  ...defaultFixtureSeams,
  streamSql: (chunks: Iterable<string>, environment: NodeJS.ProcessEnv) =>
    streamToProcess("psql", psqlArguments, { env: environment }, chunks),
});

/**
 * Writes `chunks` to a child's stdin as fast as it reads them, and returns its
 * stdout once it exits 0.
 */
export async function streamToProcess(
  command: string,
  arguments_: readonly string[],
  options: SpawnOptions,
  chunks: Iterable<string>,
): Promise<string> {
  const child = spawn(command, arguments_, {
    ...options,
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout?.setEncoding("utf8").on("data", (chunk: string) => {
    stdout += chunk;
  });
  child.stderr?.setEncoding("utf8").on("data", (chunk: string) => {
    stderr = (stderr + chunk).slice(-65_536);
  });
  const exited = new Promise<number | null>((resolve_, reject) => {
    child.once("error", reject);
    child.once("close", (code) => resolve_(code));
  });
  const stdin = child.stdin;
  if (!stdin) throw new Error(`${command} has no stdin`);
  let broken = false;
  stdin.on("error", () => {
    broken = true;
  });
  for (const chunk of chunks) {
    if (broken) break;
    if (!stdin.write(chunk))
      await Promise.race([
        once(stdin, "drain").catch(() => {
          broken = true;
        }),
        exited,
      ]);
  }
  stdin.end();
  const code = await exited;
  if (code !== 0)
    throw new Error(
      `${command} failed: ${stderr.trim() || `exit ${String(code)}`}`,
    );
  return stdout.trim();
}

/**
 * Seeds the variant into schema `incidents`, then refuses it unless the
 * variant's checks and every one of v1's pass. The dataset is
 * `FIXTURE_TABLES`; `tables` cuts it to fewer rows per table, as the
 * benchmark does, and `validation` is then the report of v1's checks that
 * such a cut can pass, in `validationSql`'s shape.
 */
export async function seedContentFixture(
  environment: NodeJS.ProcessEnv = process.env,
  seams: ContentFixtureSeams = defaultContentFixtureSeams,
  tables: readonly FixtureTable[] = FIXTURE_TABLES,
  generator: ContentGenerator = createContentGenerator(),
  validation: string = validationSql(),
): Promise<void> {
  assertCorrectedProfile(seams.readProfile);
  const url = environment[FIXTURE_DATABASE_ENV];
  if (!url) throw new Error(`${FIXTURE_DATABASE_ENV} is required`);
  const postgres = postgresEnvironmentFromUrl(url, environment);
  const preflight = JSON.parse(seams.runSql(preflightSql, postgres)) as {
    role?: unknown;
    superuser?: unknown;
  };
  if (preflight.role !== FIXTURE_SOURCE_ROLE || preflight.superuser !== false)
    throw new Error(
      `the content fixture seeds as ${FIXTURE_SOURCE_ROLE}, not a superuser; the source role is ${String(preflight.role)}`,
    );

  seams.runSql(`${fixtureDdl}\n${markerSql(CONTENT_SEEDING_MARKER)}`, postgres);
  const lengths = new Map<string, readonly number[]>();
  for (const table of tables)
    for (const { column, kind } of CONTENT_COLUMNS[table.name] ?? [])
      if (kind === "message" || kind === "prose")
        lengths.set(
          `${table.name}.${column}`,
          parseLengths(
            seams.runSql(v1LengthsSql(table, column), postgres),
            table.profileRows,
            `${table.name}.${column}`,
          ),
        );
  const order = [...new Set(seedBatches().map((batch) => batch.table))];
  const ordered = order.flatMap((name) =>
    tables.filter((table) => table.name === name),
  );
  await seams.streamSql(contentSession(ordered, lengths, generator), postgres);
  seams.runSql(sentinelSql, postgres);

  const problems = contentVerificationProblems(
    seams.runSql(contentVerificationSql(tables), postgres),
    tables,
  );
  if (problems.length > 0)
    throw new Error(`content fixture refused: ${problems.join("; ")}`);
  const validated = evaluateValidationReport(
    seams.runSql(validation, postgres),
  );
  if (!validated.valid)
    throw new Error(
      `content fixture failed v1's checks: ${validated.failedChecks.join(", ")}`,
    );
  seams.runSql(markerSql(CONTENT_DATASET_VERSION), postgres);
}
