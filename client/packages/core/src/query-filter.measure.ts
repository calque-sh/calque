/**
 * The issue panel's two scans of the example app's events, at its 1M tier's
 * 800,000 rows: `issue_id = X` counted, and its latest twelve. Run with
 * `pnpm --filter @calque/core measure --testNamePattern "issue scans"`.
 * Timings are query-kernel diagnostics on the host that ran them, never
 * browser, cold-load or stable-paint evidence.
 */
import { expect, test } from "vitest";

import {
  openDirectColumnarArtifact,
  type DirectColumnarSchema,
} from "./artifact.js";
import { encodeArtifact } from "./artifact.fixture.js";
import { OverlayEngine, artifactBase } from "./overlay.js";
import {
  ColumnarQueryEngine,
  MAX_QUERY_RESULT_BYTES,
  MAX_QUERY_RESULT_ROWS,
  configureQueryMeasurement,
  inspectQueryExecution,
  type QuerySpecification,
} from "./query.js";
import { referenceQuery } from "./query-reference.fixture.js";

const ROWS = 800_000;
/** The example app's distinct `events.issue_id` values at the 1M tier. */
const ISSUES = 85_000;
const SAMPLES = 30;
const WARMUPS = 5;

const SCHEMA: DirectColumnarSchema = {
  format: "calque-direct-columnar",
  version: 4,
  tables: [
    {
      name: "events",
      primaryKey: ["id"],
      columns: [
        { name: "id", type: "int64", nullable: false },
        { name: "issue_id", type: "uuid", nullable: true },
        { name: "level", type: "dict", nullable: false },
        { name: "message", type: "utf8", nullable: false },
        { name: "occurred_at", type: "timestamp", nullable: false },
      ],
    },
  ],
};
const COLUMNS = SCHEMA.tables[0]?.columns.map(({ name }) => name) ?? [];
const LEVELS = ["debug", "info", "warning", "error", "fatal"];

/** mulberry32, so the fixture replays. */
function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 2 ** 32;
  };
}

const OPTIONS = {
  maxRows: MAX_QUERY_RESULT_ROWS,
  maxBytes: MAX_QUERY_RESULT_BYTES,
};

function median(values: readonly number[]): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)] ?? Number.NaN;
}

test("issue scans over 800,000 events", async () => {
  const random = generator(0x155e);
  const issues = Array.from({ length: ISSUES }, () =>
    Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
  );
  const started = Date.parse("2026-01-01T00:00:00Z");
  const events = Array.from({ length: ROWS }, (_unused, index) => ({
    id: BigInt(index + 1),
    issue_id:
      random() < 0.1 ? null : (issues[Math.floor(random() * ISSUES)] ?? null),
    level: LEVELS[index % LEVELS.length],
    message: `event ${String(index)}`,
    occurred_at: new Date(started + index * 1_000).toISOString(),
  }));
  const needle = events.find((event) => event.issue_id !== null)?.issue_id;
  if (!needle) throw new Error("no event names an issue");
  const encoded = encodeArtifact(SCHEMA, new Map([["events", events]]));
  const overlayOf = async () => {
    const workspace = await openDirectColumnarArtifact(
      encoded.bytes.slice().buffer,
      {
        byteLength: encoded.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema: SCHEMA,
      },
    );
    return new OverlayEngine([
      {
        name: "events",
        columns: COLUMNS,
        base: artifactBase(workspace.table("events"), COLUMNS),
      },
    ]);
  };
  // Chunk statistics are built once per base column, by whichever query asks
  // first, and counted there: each arm gets a base of its own.
  const overlay = await overlayOf();
  const rowOverlay = await overlayOf();
  const where = {
    kind: "compare",
    operator: "eq",
    left: { kind: "field", field: { source: "event", column: "issue_id" } },
    right: { kind: "literal", value: needle },
  } as const;
  const count: QuerySpecification = {
    version: 1,
    from: { collection: "events", as: "event" },
    where,
    select: [{ kind: "aggregate", operator: "count", as: "count" }],
  };
  const latest: QuerySpecification = {
    version: 1,
    from: { collection: "events", as: "event" },
    where,
    select: ["id", "level", "message", "occurred_at"].map((column) => ({
      kind: "field",
      field: { source: "event", column },
      as: column,
    })),
    orderBy: [
      { by: "occurred_at", direction: "desc" },
      { by: "id", direction: "desc" },
    ],
    limit: 12,
  };
  const arms = {
    block: new ColumnarQueryEngine(overlay, { capacityBytes: 1 << 30 }),
    row: new ColumnarQueryEngine(rowOverlay, { capacityBytes: 1 << 30 }),
  };
  configureQueryMeasurement(arms.row, { blockFilter: false });
  const schemas = [{ name: "events", columns: COLUMNS }];
  for (const query of [count, latest]) {
    const expected = referenceQuery(overlay, schemas, query);
    for (const engine of Object.values(arms))
      expect(engine.query(query, OPTIONS)).toEqual(expected);
    expect(inspectQueryExecution(arms.block)).toEqual(
      inspectQueryExecution(arms.row),
    );
  }
  const timings: Record<string, readonly number[]> = {};
  for (const [name, query] of Object.entries({ count, latest }))
    for (const [arm, engine] of Object.entries(arms)) {
      for (let index = 0; index < WARMUPS; index++)
        engine.query(query, OPTIONS);
      timings[`${name}.${arm}`] = Array.from({ length: SAMPLES }, () => {
        const before = performance.now();
        engine.query(query, OPTIONS);
        return performance.now() - before;
      });
    }
  const result = {
    schema: "calque.issue-scans.host-diagnostic.v1",
    rows: ROWS,
    issues: ISSUES,
    matches: Number(arms.block.query(count, OPTIONS).rows[0]?.[0]),
    inspection: inspectQueryExecution(arms.block),
    samples: SAMPLES,
    warmups: WARMUPS,
    medianMs: Object.fromEntries(
      Object.entries(timings).map(([name, values]) => [name, median(values)]),
    ),
    rawSamplesMs: timings,
  };
  console.log(`ISSUE_SCANS_RESULT=${JSON.stringify(result)}`);
}, 120_000);
