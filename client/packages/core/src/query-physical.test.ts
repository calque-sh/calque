import { describe, expect, test } from "vitest";

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
  type QueryComparisonOperator,
  type QuerySpecification,
} from "./query.js";
import {
  referenceQuery,
  type QueryReferenceSchema,
} from "./query-reference.fixture.js";

const CHUNK_ROWS = 4_096;
const ROW_COUNT = CHUNK_ROWS * 4;
function fixtureColumns(tableName: string): readonly string[] {
  const table = WORKSPACE_CONTRACT.tables.find(
    (candidate) => candidate.name === tableName,
  );
  if (!table) throw new Error(`${tableName} fixture schema is unavailable`);
  return table.columns.map((column) => column.name);
}

const EVENT_COLUMNS = fixtureColumns("events");
const ISSUE_COLUMNS = fixtureColumns("issues");
const USER_COLUMNS = fixtureColumns("users");

type FixtureRow = Readonly<Record<string, unknown>>;

interface OpenedEngines {
  readonly physical: OverlayEngine;
  readonly reference: OverlayEngine;
  readonly query: ColumnarQueryEngine;
}

function eventRows(): readonly FixtureRow[] {
  return Array.from({ length: ROW_COUNT }, (_unused, index) => ({
    id: BigInt(index),
    organization_id: "organization-1",
    project_id: "project-1",
    issue_id: index < CHUNK_ROWS || index >= CHUNK_ROWS * 2 ? null : "issue-1",
    level:
      index < CHUNK_ROWS
        ? "alpha"
        : index < CHUNK_ROWS * 2
          ? "middle"
          : index < CHUNK_ROWS * 3
            ? index === CHUNK_ROWS * 2
              ? "needle"
              : "zulu"
            : "omega",
    message: `event-${index}`,
    occurred_at: "2026-08-31T00:00:00.000Z",
    payload: { index },
  }));
}

function fixtureTables(
  events: readonly FixtureRow[],
  issues: readonly FixtureRow[] = [],
  users: readonly FixtureRow[] = [],
): ReadonlyMap<string, readonly FixtureRow[]> {
  return new Map([
    ["events", events],
    ["issues", issues],
    ["comments", []],
    ["issue_tags", []],
    ["users", users],
    ["projects", []],
    ["memberships", []],
  ]);
}

function objectReferenceBase(
  rows: readonly FixtureRow[],
  columns: readonly string[],
): OverlayBase {
  return {
    rows: rows.length,
    lookupKey(key) {
      if (key.length !== 1) return -1;
      return rows.findIndex((row) => Object.is(row[columns[0] ?? ""], key[0]));
    },
    valueAt(column, row) {
      return (rows[row]?.[columns[column] ?? ""] ?? null) as SqlValue;
    },
  };
}

function rowValues(row: FixtureRow): readonly unknown[] {
  return EVENT_COLUMNS.map((column) => row[column]);
}

function referenceBase(rows: readonly FixtureRow[]): OverlayBase {
  const byId = new Map<bigint, number>();
  rows.forEach((row, index) => byId.set(row.id as bigint, index));
  return {
    rows: rows.length,
    lookupKey(key) {
      return key.length === 1 && typeof key[0] === "bigint"
        ? (byId.get(key[0]) ?? -1)
        : -1;
    },
    valueAt(column, row) {
      return rowValues(rows[row] ?? {})[column] as SqlValue;
    },
  };
}

function overlay(table: ArtifactTable): OverlayEngine {
  return new OverlayEngine([
    {
      name: "events",
      columns: EVENT_COLUMNS,
      base: artifactBase(table, EVENT_COLUMNS),
    },
  ]);
}

async function engines(): Promise<OpenedEngines> {
  const rows = eventRows();
  const encoded = encodeWorkspace(fixtureTables(rows));
  const input = encoded.bytes.slice().buffer;
  const workspace = await openDirectColumnarArtifact(input, {
    byteLength: encoded.byteLength,
    digest: encoded.digest,
    identity: encoded.identity,
    schema: WORKSPACE_CONTRACT,
  });
  const physical = overlay(workspace.table("events"));
  const reference = new OverlayEngine([
    {
      name: "events",
      columns: EVENT_COLUMNS,
      base: referenceBase(rows),
    },
  ]);
  return {
    physical,
    reference,
    query: new ColumnarQueryEngine(physical, { capacityBytes: 1 << 30 }),
  };
}

function primaryKey(id: bigint): Uint8Array {
  return new TextEncoder().encode(String(id));
}

function insertedValues(id: bigint, message: string): readonly SqlValue[] {
  return [
    id,
    "organization-1",
    "project-1",
    null,
    "needle",
    message,
    "2026-08-31T00:00:00.000Z",
    null,
  ];
}

function applyNeedleOverlay(target: OverlayEngine): void {
  target.applyCanonical({
    transactions: [
      {
        mutationIds: [],
        operations: [
          {
            kind: "update",
            collection: "events",
            primaryKey: primaryKey(17n),
            key: [17n],
            changed: [{ column: 4, value: "needle" }],
          },
          {
            kind: "insert",
            collection: "events",
            primaryKey: primaryKey(20_000n),
            key: [20_000n],
            values: insertedValues(20_000n, "canonical-insert"),
          },
        ],
      },
    ],
  });
  target.applyOptimistic({
    mutationId: "physical-query-pending",
    actions: [
      {
        kind: "patch",
        collection: "events",
        primaryKey: primaryKey(5_000n),
        key: [5_000n],
        changes: [{ column: 4, value: "needle", mode: "set" }],
      },
      {
        kind: "insert",
        collection: "events",
        primaryKey: primaryKey(20_001n),
        key: [20_001n],
        values: insertedValues(20_001n, "pending-insert"),
      },
    ],
  });
}

const REFERENCE_SCHEMA: readonly QueryReferenceSchema[] = [
  { name: "events", columns: EVENT_COLUMNS },
];

const needleQuery = {
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
  select: [
    {
      kind: "field",
      field: { source: "event", column: "id" },
      as: "id",
    },
    {
      kind: "field",
      field: { source: "event", column: "message" },
      as: "message",
    },
  ],
  orderBy: [{ by: "id", direction: "asc" }],
} as const satisfies QuerySpecification;

const nullIssueQuery = {
  version: 1,
  from: { collection: "events", as: "event" },
  where: {
    kind: "is-null",
    expression: {
      kind: "field",
      field: { source: "event", column: "issue_id" },
    },
  },
  select: [
    {
      kind: "field",
      field: { source: "event", column: "id" },
      as: "id",
    },
  ],
  limit: 16,
} as const satisfies QuerySpecification;

describe("direct-columnar physical query execution", () => {
  test("lazily builds resident chunk statistics and safely prunes around CDC and pending rows", async () => {
    const state = await engines();
    applyNeedleOverlay(state.physical);
    applyNeedleOverlay(state.reference);

    const expected = referenceQuery(
      state.reference,
      REFERENCE_SCHEMA,
      needleQuery,
    );
    const cold = state.query.query(needleQuery, {
      maxRows: 10_000,
      maxBytes: 16 * 1024 * 1024,
    });
    expect(cold).toEqual(expected);
    expect(cold.rows.map((row) => row[0])).toEqual([
      17n,
      5_000n,
      BigInt(CHUNK_ROWS * 2),
      20_000n,
      20_001n,
    ]);

    const coldCounters = inspectQueryExecution(state.query);
    expect(coldCounters).toMatchObject({
      chunksExamined: 4,
      chunksSkipped: 1,
      materializedComparisons: 4,
      statisticsRowsScanned: ROW_COUNT,
    });
    expect(coldCounters.rowsExamined).toBeLessThanOrEqual(CHUNK_ROWS * 3 + 4);
    expect(coldCounters.encodedComparisons).toBeGreaterThan(0);
    expect(coldCounters.encodedComparisons).toBeLessThanOrEqual(CHUNK_ROWS * 3);
    expect(
      coldCounters.encodedComparisons + coldCounters.materializedComparisons,
    ).toBe(coldCounters.rowsExamined);
    expect(coldCounters.residentStatisticsBytes).toBeGreaterThan(0);

    const warm = state.query.query(needleQuery, {
      maxRows: 10_000,
      maxBytes: 16 * 1024 * 1024,
    });
    expect(warm).toEqual(expected);
    const warmCounters = inspectQueryExecution(state.query);
    expect(warmCounters).toMatchObject({
      chunksExamined: 4,
      chunksSkipped: 1,
      materializedComparisons: 4,
      statisticsRowsScanned: 0,
      residentStatisticsBytes: coldCounters.residentStatisticsBytes,
    });
    expect(warmCounters.rowsExamined).toBeLessThanOrEqual(CHUNK_ROWS * 3 + 4);
    expect(warmCounters.encodedComparisons).toBeGreaterThan(0);
    expect(warmCounters.encodedComparisons).toBeLessThanOrEqual(CHUNK_ROWS * 3);
    expect(
      warmCounters.encodedComparisons + warmCounters.materializedComparisons,
    ).toBe(warmCounters.rowsExamined);
  });

  test("prunes no chunk a write dirtied after an earlier query found it clean", async () => {
    const state = await engines();
    const needles = () => {
      const result = state.query.query(needleQuery, {
        maxRows: 10_000,
        maxBytes: 16 * 1024 * 1024,
      });
      expect(result).toEqual(
        referenceQuery(state.reference, REFERENCE_SCHEMA, needleQuery),
      );
      return result.rows.map((row) => row[0]);
    };
    const both = (write: (target: OverlayEngine) => void) => {
      write(state.physical);
      write(state.reference);
    };
    const update = (id: bigint, changed: Readonly<Record<number, SqlValue>>) =>
      both((target) =>
        target.applyCanonical({
          transactions: [
            {
              mutationIds: [],
              operations: [
                {
                  kind: "update",
                  collection: "events",
                  primaryKey: primaryKey(id),
                  key: [id],
                  changed: Object.entries(changed).map(([column, value]) => ({
                    column: Number(column),
                    value,
                  })),
                },
              ],
            },
          ],
        }),
      );
    const third = BigInt(CHUNK_ROWS * 2);

    // Every chunk but the third is pruned on its statistics.
    expect(needles()).toEqual([third]);
    update(17n, { 4: "needle" });
    expect(needles()).toEqual([17n, third]);
    both((target) =>
      target.applyOptimistic({
        mutationId: "dirt-pending",
        actions: [
          {
            kind: "patch",
            collection: "events",
            primaryKey: primaryKey(5_000n),
            key: [5_000n],
            changes: [{ column: 4, value: "needle", mode: "set" }],
          },
        ],
      }),
    );
    expect(needles()).toEqual([17n, 5_000n, third]);
    // Five of eight columns make the row's run dense, three of its cells
    // unwritten.
    update(13_000n, {
      1: "organization-2",
      2: "project-2",
      4: "needle",
      5: "event-dense",
      6: "2026-09-01T00:00:00.000Z",
    });
    expect(needles()).toEqual([17n, 5_000n, third, 13_000n]);
    both((target) => target.dropMutation("dirt-pending"));
    expect(needles()).toEqual([17n, third, 13_000n]);
  });

  test("evaluates nullable dictionary predicates from codes and the packed validity bitmap", async () => {
    const state = await engines();
    const expected = referenceQuery(
      state.reference,
      REFERENCE_SCHEMA,
      nullIssueQuery,
    );

    expect(state.query.query(nullIssueQuery)).toEqual(expected);
    const coldCounters = inspectQueryExecution(state.query);
    expect(coldCounters).toMatchObject({
      chunksExamined: 4,
      chunksSkipped: 1,
      materializedComparisons: 0,
      statisticsRowsScanned: ROW_COUNT,
    });
    expect(coldCounters.rowsExamined).toBeLessThanOrEqual(CHUNK_ROWS * 3);
    expect(coldCounters.encodedComparisons).toBeGreaterThan(0);
    expect(coldCounters.encodedComparisons).toBeLessThanOrEqual(CHUNK_ROWS * 3);
    expect(coldCounters.encodedComparisons).toBe(coldCounters.rowsExamined);
    expect(coldCounters.residentStatisticsBytes).toBeGreaterThan(0);

    expect(state.query.query(nullIssueQuery)).toEqual(expected);
    const warmCounters = inspectQueryExecution(state.query);
    expect(warmCounters).toMatchObject({
      chunksExamined: 4,
      chunksSkipped: 1,
      materializedComparisons: 0,
      statisticsRowsScanned: 0,
      residentStatisticsBytes: coldCounters.residentStatisticsBytes,
    });
    expect(warmCounters.rowsExamined).toBeLessThanOrEqual(CHUNK_ROWS * 3);
    expect(warmCounters.encodedComparisons).toBeGreaterThan(0);
    expect(warmCounters.encodedComparisons).toBeLessThanOrEqual(CHUNK_ROWS * 3);
    expect(warmCounters.encodedComparisons).toBe(warmCounters.rowsExamined);
  });

  test("charges lazy resident statistics once across repeated bounded queries", async () => {
    const state = await engines();
    const capacityBytes = state.query.estimateReservationBytes(needleQuery, {
      maxRows: 10_000,
      maxBytes: 16 * 1024 * 1024,
    });
    const bounded = new ColumnarQueryEngine(state.physical, { capacityBytes });
    const expected = referenceQuery(
      state.reference,
      REFERENCE_SCHEMA,
      needleQuery,
    );

    for (let attempt = 0; attempt < 3; attempt++)
      expect(
        bounded.query(needleQuery, {
          maxRows: 10_000,
          maxBytes: 16 * 1024 * 1024,
        }),
      ).toEqual(expected);
  });

  test("evaluates every fixed-vector comparison operator without materializing values", async () => {
    const state = await engines();
    configureQueryMeasurement(state.query, { chunkPruning: false });
    const comparisons = [
      ["eq", 8_192n],
      ["neq", 8_192n],
      ["lt", 8_192n],
      ["lte", 8_192n],
      ["gt", 8_192n],
      ["gte", 8_192n],
    ] as const satisfies readonly (readonly [
      QueryComparisonOperator,
      bigint,
    ])[];

    for (const [operator, value] of comparisons) {
      const specification: QuerySpecification = {
        version: 1,
        from: { collection: "events", as: "event" },
        where: {
          kind: "compare",
          operator,
          left: {
            kind: "field",
            field: { source: "event", column: "id" },
          },
          right: { kind: "literal", value },
        },
        select: [{ kind: "aggregate", operator: "count", as: "count" }],
      };
      expect(state.query.query(specification)).toEqual(
        referenceQuery(state.reference, REFERENCE_SCHEMA, specification),
      );
      expect(inspectQueryExecution(state.query)).toMatchObject({
        rowsExamined: ROW_COUNT,
        encodedComparisons: ROW_COUNT,
        materializedComparisons: 0,
      });
    }

    for (const [operator, value] of [
      ["eq", "2026-08-31T00:00:00.000Z"],
      ["neq", "2026-08-31T00:00:00.001Z"],
    ] as const) {
      const specification: QuerySpecification = {
        version: 1,
        from: { collection: "events", as: "event" },
        where: {
          kind: "compare",
          operator,
          left: {
            kind: "field",
            field: { source: "event", column: "occurred_at" },
          },
          right: { kind: "literal", value },
        },
        select: [{ kind: "aggregate", operator: "count", as: "count" }],
      };
      expect(state.query.query(specification)).toEqual(
        referenceQuery(state.reference, REFERENCE_SCHEMA, specification),
      );
      expect(inspectQueryExecution(state.query)).toMatchObject({
        rowsExamined: ROW_COUNT,
        encodedComparisons: ROW_COUNT,
        materializedComparisons: 0,
      });
    }
  });

  test("matches the oracle for f64 edge values and packed booleans", async () => {
    const issues: readonly FixtureRow[] = [-2, -0, 0, 5, 9].map(
      (priority, index) => ({
        id: `issue-${index}`,
        organization_id: "organization-1",
        project_id: "project-1",
        assignee_id: null,
        status: "open",
        priority,
        title: `issue ${index}`,
        event_count: index,
        updated_at: "2026-08-31T00:00:00.000Z",
      }),
    );
    const users: readonly FixtureRow[] = [false, true].map((active, index) => ({
      id: `user-${index}`,
      display_name: `User ${index}`,
      email: `user-${index}@example.test`,
      active,
    }));
    const artifact = encodeWorkspace(fixtureTables([], issues, users));
    const workspace = await openDirectColumnarArtifact(
      artifact.bytes.slice().buffer,
      {
        byteLength: artifact.byteLength,
        digest: artifact.digest,
        identity: artifact.identity,
        schema: WORKSPACE_CONTRACT,
      },
    );
    const physical = new OverlayEngine([
      {
        name: "issues",
        columns: ISSUE_COLUMNS,
        base: artifactBase(workspace.table("issues"), ISSUE_COLUMNS),
      },
      {
        name: "users",
        columns: USER_COLUMNS,
        base: artifactBase(workspace.table("users"), USER_COLUMNS),
      },
    ]);
    const reference = new OverlayEngine([
      {
        name: "issues",
        columns: ISSUE_COLUMNS,
        base: objectReferenceBase(issues, ISSUE_COLUMNS),
      },
      {
        name: "users",
        columns: USER_COLUMNS,
        base: objectReferenceBase(users, USER_COLUMNS),
      },
    ]);
    const schemas: readonly QueryReferenceSchema[] = [
      { name: "issues", columns: ISSUE_COLUMNS },
      { name: "users", columns: USER_COLUMNS },
    ];
    const query = new ColumnarQueryEngine(physical, {
      capacityBytes: 1 << 30,
    });
    configureQueryMeasurement(query, { chunkPruning: false });

    const cases = [
      ...(["eq", "neq", "lt", "lte", "gt", "gte"] as const).map(
        (operator) => ["issues", "priority", operator, 0] as const,
      ),
      ...(["eq", "neq", "lt", "lte", "gt", "gte"] as const).map(
        (operator) => ["issues", "priority", operator, Number.NaN] as const,
      ),
      ...(["eq", "neq", "lt", "lte", "gt", "gte"] as const).map(
        (operator) => ["users", "active", operator, false] as const,
      ),
    ];

    for (const [collection, column, operator, value] of cases) {
      const specification: QuerySpecification = {
        version: 1,
        from: { collection, as: "row" },
        where: {
          kind: "compare",
          operator,
          left: { kind: "field", field: { source: "row", column } },
          right: { kind: "literal", value },
        },
        select: [{ kind: "aggregate", operator: "count", as: "count" }],
      };
      expect(query.query(specification)).toEqual(
        referenceQuery(reference, schemas, specification),
      );
      const expectedRows =
        collection === "issues" ? issues.length : users.length;
      expect(inspectQueryExecution(query)).toMatchObject({
        rowsExamined: expectedRows,
        encodedComparisons: expectedRows,
        materializedComparisons: 0,
      });
    }
  });
});
