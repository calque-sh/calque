/**
 * What resident chunk statistics and encoded vectors cost and save the query
 * kernel over a 65,536-row events table: a predicate over encoded vectors
 * against the same predicate over materialized values, and a scan that skips
 * chunks by their statistics against a full encoded scan, each checked first
 * against the reference query. Run with
 * `pnpm --filter @calque/core measure --testNamePattern "chunk statistics"`.
 * Timings are query-kernel diagnostics, never browser interaction, cold-load,
 * or stable-paint evidence.
 */
import { test } from "vitest";

import { openDirectColumnarArtifact, type ArtifactTable } from "./artifact.js";
import { encodeWorkspace, WORKSPACE_CONTRACT } from "./artifact.fixture.js";
import {
  OverlayEngine,
  artifactBase,
  type OverlayBase,
  type SqlValue,
} from "./overlay.js";
import {
  ColumnarQueryEngine,
  configureQueryMeasurement,
  inspectQueryExecution,
  type QuerySpecification,
} from "./query.js";
import {
  referenceQuery,
  type QueryReferenceSchema,
} from "./query-reference.fixture.js";

const CHUNK_ROWS = 4_096;
const ROWS = CHUNK_ROWS * 16;
const SAMPLES = 30;
const WARMUPS = 3;

type FixtureRow = Readonly<Record<string, unknown>>;

const EVENT_COLUMNS = (() => {
  const table = WORKSPACE_CONTRACT.tables.find(({ name }) => name === "events");
  if (!table) throw new Error("events schema is unavailable");
  return table.columns.map(({ name }) => name);
})();

function rows(): readonly FixtureRow[] {
  return Array.from({ length: ROWS }, (_unused, index) => ({
    id: BigInt(index),
    organization_id: "organization-1",
    project_id: `project-${index % 128}`,
    issue_id: index % 7 === 0 ? null : `issue-${index % 8_192}`,
    level: index < CHUNK_ROWS ? "needle" : `level-${index % 7}`,
    message: `event-${index}`,
    occurred_at: "2026-08-31T00:00:00.000Z",
    payload: { index },
  }));
}

function fixtureTables(
  events: readonly FixtureRow[],
): ReadonlyMap<string, readonly FixtureRow[]> {
  return new Map([
    ["events", events],
    ["issues", []],
    ["comments", []],
    ["issue_tags", []],
    ["users", []],
    ["projects", []],
    ["memberships", []],
  ]);
}

function plainBase(table: ArtifactTable): OverlayBase {
  const columns = EVENT_COLUMNS.map((name) => table.column(name));
  return {
    rows: table.rows,
    lookupKey: (key) => table.lookupKey(key),
    valueAt: (column, row) => columns[column]?.valueAt(row) as SqlValue,
  };
}

function engine(table: ArtifactTable, encoded: boolean): ColumnarQueryEngine {
  const base = encoded ? artifactBase(table, EVENT_COLUMNS) : plainBase(table);
  return new ColumnarQueryEngine(
    new OverlayEngine([{ name: "events", columns: EVENT_COLUMNS, base }]),
    { capacityBytes: 8 * 1024 * 1024 * 1024 },
  );
}

const needle = {
  version: 1,
  from: { collection: "events", as: "event" },
  where: {
    kind: "compare",
    operator: "eq",
    left: {
      kind: "field",
      field: { source: "event", column: "level" },
    },
    right: { kind: "literal", value: "needle" },
  },
  select: [{ kind: "aggregate", operator: "count", as: "count" }],
} as const satisfies QuerySpecification;

const allEncoded = {
  ...needle,
  where: {
    ...needle.where,
    operator: "neq",
    right: { kind: "literal", value: "absent" },
  },
} as const satisfies QuerySpecification;

function samples(operation: () => void): readonly number[] {
  for (let index = 0; index < WARMUPS; index++) operation();
  return Array.from({ length: SAMPLES }, () => {
    const started = performance.now();
    operation();
    return performance.now() - started;
  });
}

function median(values: readonly number[]): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)] ?? Number.NaN;
}

test("resident chunk statistics and encoded vectors", async () => {
  const sourceRows = rows();
  const artifact = encodeWorkspace(fixtureTables(sourceRows));
  const workspace = await openDirectColumnarArtifact(
    artifact.bytes.slice().buffer,
    {
      byteLength: artifact.byteLength,
      digest: artifact.digest,
      identity: artifact.identity,
      schema: WORKSPACE_CONTRACT,
    },
  );
  const table = workspace.table("events");
  const encoded = engine(table, true);
  const materialized = engine(table, false);
  const oracleOverlay = new OverlayEngine([
    { name: "events", columns: EVENT_COLUMNS, base: plainBase(table) },
  ]);
  const schema: readonly QueryReferenceSchema[] = [
    { name: "events", columns: EVENT_COLUMNS },
  ];

  const needleExpected = referenceQuery(oracleOverlay, schema, needle);
  const statisticsStarted = performance.now();
  const coldPrunedResult = encoded.query(needle);
  const statisticsBuildAndPrunedScanMs = performance.now() - statisticsStarted;
  const coldPruningInspection = inspectQueryExecution(encoded);
  if (JSON.stringify(coldPrunedResult) !== JSON.stringify(needleExpected))
    throw new Error("the cold statistics candidate disagrees with the oracle");

  for (const query of [needle, allEncoded]) {
    const expected = referenceQuery(oracleOverlay, schema, query);
    if (JSON.stringify(encoded.query(query)) !== JSON.stringify(expected))
      throw new Error("the encoded candidate disagrees with the oracle");
    if (JSON.stringify(materialized.query(query)) !== JSON.stringify(expected))
      throw new Error("the materialized baseline disagrees with the oracle");
  }

  configureQueryMeasurement(encoded, { chunkPruning: false });
  const encodedSamples = samples(() => void encoded.query(allEncoded));
  const materializedSamples = samples(
    () => void materialized.query(allEncoded),
  );

  configureQueryMeasurement(encoded, { chunkPruning: false });
  const fullScanSamples = samples(() => void encoded.query(needle));
  configureQueryMeasurement(encoded, { chunkPruning: true });
  const prunedSamples = samples(() => void encoded.query(needle));
  const pruningInspection = inspectQueryExecution(encoded);

  const result = {
    schema: "calque.core.chunk-statistics.host-diagnostic.v1",
    rows: ROWS,
    chunkRows: CHUNK_ROWS,
    samples: SAMPLES,
    warmups: WARMUPS,
    artifactBytes: artifact.byteLength,
    shippedStatisticsBytes: 0,
    residentStatisticsBytes: pruningInspection.residentStatisticsBytes,
    statisticsBuildAndPrunedScanMs,
    statisticsBuildRows: coldPruningInspection.statisticsRowsScanned,
    chunksExamined: pruningInspection.chunksExamined,
    chunksSkipped: pruningInspection.chunksSkipped,
    medianMs: {
      materializedPredicate: median(materializedSamples),
      encodedPredicate: median(encodedSamples),
      fullEncodedScan: median(fullScanSamples),
      statisticsPrunedScan: median(prunedSamples),
    },
    rawSamplesMs: {
      materializedPredicate: materializedSamples,
      encodedPredicate: encodedSamples,
      fullEncodedScan: fullScanSamples,
      statisticsPrunedScan: prunedSamples,
    },
  };
  console.log(`CHUNK_STATISTICS_RESULT=${JSON.stringify(result)}`);
}, 900_000);
