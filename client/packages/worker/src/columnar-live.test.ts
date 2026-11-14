/**
 * The direct-columnar live tail, checked against a reference state: a mount
 * that started at cursor `C0` and applied the tail to `C1` holds the same rows
 * as a mount built fresh at `C1`. The reference is a second artifact, encoded
 * from an independently maintained source state by the same builder and opened
 * by the same reader, so a bug in the decode, the value conversion or the
 * overlay shows up as a row that differs from a full build.
 *
 * Every batch here is real wire: framed `ChangeStreamFrame`s carrying real
 * `body_sha256`, decoded by `decodeColumnarChangeBatch` through the same
 * `ChangeStreamState` the client drives.
 */
import { readFileSync } from "node:fs";

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  OverlayEngine,
  artifactBase,
  openDirectColumnarArtifact,
  type DirectColumnarWorkspace,
  type SqlValue,
} from "@calque/core";
import {
  WORKSPACE_CONTRACT,
  FIXTURE_ARTIFACT_IDENTITY,
  encodeWorkspace,
} from "../../core/src/artifact.fixture.js";
import {
  BatchFramesDigest,
  change,
  changeFrameBodySha256,
  encodeFrame,
} from "@calque/protocol";
import { describe, expect, test, vi } from "vitest";

import {
  compileColumnarChangeBinding,
  decodeColumnarChangeBatch,
  decodeColumnarChangeBatches,
  decodeOptimisticMutation,
  type ColumnarChangeContract,
} from "./columnar-change.js";
import {
  ColumnarLiveSession,
  type ColumnarLiveMount,
  type ColumnarLiveSessionOptions,
} from "./columnar-live.js";
import type {
  ApplyChangeOptions,
  ColumnarChangeFence,
} from "./columnar-worker-client.js";
import { RuntimeError } from "./runtime-error.js";

type Row = Record<string, unknown>;
type SourceState = Map<string, Row[]>;

const text = new TextEncoder();
const utf8 = new TextDecoder();

const GENERATION = {
  sourceEpoch: "fixture-source-epoch-1",
  publicationGeneration: 1n,
  schemaGeneration: 1n,
  authorizationGeneration: 1n,
  authorizationScopeHash: new Uint8Array(32).fill(0x33),
} as const;

const PLAN_ID = "plan-columnar-live";

/**
 * Wire ids are assigned from the artifact schema, so a column added to the
 * fixture cannot go unbound.
 */
const CONTRACT: ColumnarChangeContract = {
  planId: PLAN_ID,
  generation: GENERATION,
  tables: WORKSPACE_CONTRACT.tables.map((table) => ({
    tableId: `public.${table.name}`,
    collection: table.name,
    columns: table.columns.map((column, ordinal) => ({
      columnId: ordinal + 1,
      name: column.name,
    })),
    primaryKey: table.primaryKey.map(
      (name) => table.columns.findIndex((column) => column.name === name) + 1,
    ),
  })),
};

function schemaOf(collection: string) {
  const table = WORKSPACE_CONTRACT.tables.find(
    (candidate) => candidate.name === collection,
  );
  if (!table) throw new Error(`unknown collection ${collection}`);
  return table;
}

/**
 * Timestamps are millisecond-aligned: the fixture artifact builder parses its
 * source values with `Date.parse`, so a fresh build cannot carry a
 * sub-millisecond digit. This oracle does not cover microsecond-resolution
 * timestamps.
 */
const stamp = (day: number, millisecond: number): string =>
  textForMicros(BigInt(Date.UTC(2024, 0, day, 0, 0, 0, millisecond)) * 1_000n);

function textForMicros(micros: bigint): string {
  const millis = micros / 1_000n;
  const remainder = micros % 1_000n;
  const base = new Date(Number(millis)).toISOString();
  return remainder === 0n
    ? base
    : `${base.slice(0, -1)}${remainder.toString().padStart(3, "0")}Z`;
}

const microsOf = (value: string): bigint => {
  const millis = Date.parse(value);
  if (!Number.isFinite(millis)) throw new Error(`invalid timestamp ${value}`);
  return BigInt(millis) * 1_000n;
};

/** A small but complete seven-table source state. */
function seedState(): SourceState {
  const org = "org-1";
  const users = Array.from({ length: 4 }, (_unused, index) => ({
    id: `user-${index}`,
    display_name: `User ${index}`,
    email: `user${index}@example.test`,
    active: index % 2 === 0,
  }));
  const projects = Array.from({ length: 2 }, (_unused, index) => ({
    id: `project-${index}`,
    organization_id: org,
    slug: `p${index}`,
    name: `Project ${index}`,
  }));
  const issues = Array.from({ length: 6 }, (_unused, index) => ({
    id: `issue-${index}`,
    organization_id: org,
    project_id: `project-${index % 2}`,
    assignee_id: index % 3 === 0 ? null : `user-${index % 4}`,
    status: index % 2 === 0 ? "open" : "resolved",
    priority: index,
    title: `Issue ${index}`,
    event_count: index * 3,
    updated_at: stamp(1 + index, index + 1),
  }));
  const events = Array.from({ length: 10 }, (_unused, index) => ({
    id: BigInt(index + 1),
    organization_id: org,
    project_id: `project-${index % 2}`,
    issue_id: index % 5 === 0 ? null : `issue-${index % 6}`,
    level: index % 2 === 0 ? "error" : "warning",
    message: `event ${index}`,
    occurred_at: stamp(1 + (index % 20), index + 7),
    payload: {
      index,
      tags: [`t${index % 3}`],
      nested: { ok: index % 2 === 0 },
    },
  }));
  const comments = Array.from({ length: 5 }, (_unused, index) => ({
    id: `comment-${index}`,
    issue_id: `issue-${index % 6}`,
    author_id: `user-${index % 4}`,
    body: `comment ${index}`,
    created_at: stamp(2 + index, index + 11),
  }));
  const issueTags = issues.flatMap((issue, index) =>
    index % 2 === 0
      ? [{ issue_id: issue.id, tag: "urgent" }]
      : [
          { issue_id: issue.id, tag: "backlog" },
          { issue_id: issue.id, tag: "triage" },
        ],
  );
  const memberships = users.map((user, index) => ({
    organization_id: org,
    user_id: user.id,
    role: index === 0 ? "owner" : "member",
  }));
  return new Map<string, Row[]>([
    ["events", events],
    ["issues", issues],
    ["comments", comments],
    ["issue_tags", issueTags],
    ["users", users],
    ["projects", projects],
    ["memberships", memberships],
  ]);
}

function cloneState(state: SourceState): SourceState {
  return new Map(
    [...state].map(([name, rows]) => [
      name,
      rows.map((row) => structuredClone(row)),
    ]),
  );
}

// ------------------------------------------------------------------ the wire

type Op =
  | { readonly kind: "insert"; readonly collection: string; readonly row: Row }
  | {
      readonly kind: "update";
      readonly collection: string;
      readonly key: Row;
      readonly changed: Row;
    }
  | { readonly kind: "delete"; readonly collection: string; readonly key: Row };

function cellFor(collection: string, column: string, value: unknown) {
  const schema = schemaOf(collection).columns.find(
    (candidate) => candidate.name === column,
  );
  if (!schema) throw new Error(`unknown column ${collection}.${column}`);
  if (value === null)
    return create(change.CellValueSchema, {
      kind: { case: "nullValue", value: 0 },
    });
  switch (schema.type) {
    case "int64":
      return create(change.CellValueSchema, {
        kind: { case: "int64Value", value: value as bigint },
      });
    case "f64":
      return create(change.CellValueSchema, {
        kind: { case: "float64Value", value: value as number },
      });
    case "bool":
      return create(change.CellValueSchema, {
        kind: { case: "boolValue", value: value as boolean },
      });
    case "timestamp":
      return create(change.CellValueSchema, {
        kind: {
          case: "timestampUnixMicros",
          value: microsOf(value as string),
        },
      });
    case "json":
      return create(change.CellValueSchema, {
        kind: { case: "jsonText", value: JSON.stringify(value) },
      });
    default:
      return create(change.CellValueSchema, {
        kind: { case: "textValue", value: value as string },
      });
  }
}

/** The v1 key encoding: text key parts travel as `text_value`. */
function keyFor(collection: string, key: Row): Uint8Array {
  const table = schemaOf(collection);
  return toBinary(
    change.PrimaryKeySchema,
    create(change.PrimaryKeySchema, {
      codecVersion: 1,
      parts: table.primaryKey.map((name) => {
        const ordinal = table.columns.findIndex(
          (column) => column.name === name,
        );
        const column = table.columns[ordinal];
        if (!column) throw new Error(`unknown key column ${name}`);
        const value = key[name];
        return create(change.ColumnValueSchema, {
          columnId: ordinal + 1,
          value: create(change.CellValueSchema, {
            kind:
              column.type === "int64"
                ? { case: "int64Value", value: value as bigint }
                : { case: "textValue", value: value as string },
          }),
        });
      }),
    }),
  );
}

function columnValues(collection: string, row: Row) {
  const table = schemaOf(collection);
  return table.columns.map((column, ordinal) =>
    create(change.ColumnValueSchema, {
      columnId: ordinal + 1,
      value: cellFor(collection, column.name, row[column.name]),
    }),
  );
}

function wireOperation(operation: Op) {
  const tableId = `public.${operation.collection}`;
  if (operation.kind === "insert")
    return create(change.OperationSchema, {
      tableId,
      kind: change.OperationKind.INSERT_FULL_ROW,
      primaryKey: keyFor(operation.collection, operation.row),
      payload: {
        case: "insert",
        value: create(change.FullRowSchema, {
          columns: columnValues(operation.collection, operation.row),
        }),
      },
    });
  if (operation.kind === "delete")
    return create(change.OperationSchema, {
      tableId,
      kind: change.OperationKind.DELETE_BY_KEY,
      primaryKey: keyFor(operation.collection, operation.key),
      payload: { case: "delete", value: create(change.DeleteSchema, {}) },
    });
  const table = schemaOf(operation.collection);
  return create(change.OperationSchema, {
    tableId,
    kind: change.OperationKind.UPDATE_PATCH,
    primaryKey: keyFor(operation.collection, operation.key),
    payload: {
      case: "update",
      value: create(change.RowPatchSchema, {
        changedColumns: Object.entries(operation.changed).map(
          ([name, value]) => {
            const ordinal = table.columns.findIndex(
              (column) => column.name === name,
            );
            if (ordinal < 0)
              throw new Error(`unknown column ${operation.collection}.${name}`);
            return create(change.ColumnValueSchema, {
              columnId: ordinal + 1,
              value: cellFor(operation.collection, name, value),
            });
          },
        ),
      }),
    },
  });
}

interface WireTransaction {
  readonly cursor: string;
  readonly operations: readonly Op[];
  /** Splits the transaction across this many fragments; 1 sends one frame. */
  readonly fragments?: number;
}

interface BatchOptions {
  /** Drops the closing `batchEnd` frame, ending the response mid-batch. */
  readonly truncateAfterFragments?: boolean;
  /** Marks the last fragment non-final, ending inside a transaction. */
  readonly leaveTransactionOpen?: boolean;
  /** Echoes a fenced request: `batchStart.requestedFence` and the end flag. */
  readonly requestedFence?: string;
  /** Per-table shard snapshot cursor echoed in `batchStart.tableStarts`. */
  readonly tableStartCursor?: string;
  /** `commitTimeUnixMicros` per transaction, in order; defaults to 2. */
  readonly commitTimes?: readonly bigint[];
  /** Mutation ids the batch end acknowledges. */
  readonly acknowledgedMutationIds?: readonly string[];
  /** The plan the batch answers; the session's own unless given. */
  readonly planId?: string;
}

async function encodeBatch(
  fromCursor: string,
  throughCursor: string,
  transactions: readonly WireTransaction[],
  options: BatchOptions = {},
): Promise<Uint8Array> {
  const cursor = (token: string) =>
    create(change.CursorSchema, { token: text.encode(token) });
  const frames = [
    create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: {
        case: "batchStart",
        value: create(change.BatchStartSchema, {
          generation: create(change.SourceGenerationSchema, GENERATION),
          fromCursorExclusive: cursor(fromCursor),
          ...(options.requestedFence === undefined
            ? {}
            : { requestedFence: cursor(options.requestedFence) }),
          tableIds: CONTRACT.tables.map((table) => table.tableId).sort(),
          serverTimeUnixMicros: 1n,
          planId: options.planId ?? PLAN_ID,
          tableStarts: CONTRACT.tables
            .map((table) => ({
              tableId: table.tableId,
              ...(options.tableStartCursor === undefined
                ? {}
                : { shardSnapshotCursor: cursor(options.tableStartCursor) }),
            }))
            .sort((left, right) => left.tableId.localeCompare(right.tableId)),
          streamId: text.encode(`stream-${fromCursor}`),
        }),
      },
    }),
  ];
  let operationCount = 0;
  for (const [position, transaction] of transactions.entries()) {
    const parts = transaction.fragments ?? 1;
    const size = Math.ceil(transaction.operations.length / parts);
    for (let index = 0; index < parts; index += 1) {
      const slice = transaction.operations.slice(
        index * size,
        (index + 1) * size,
      );
      operationCount += slice.length;
      const last = index === parts - 1;
      frames.push(
        create(change.ChangeStreamFrameSchema, {
          protocolVersion: 1,
          body: {
            case: "transactionFragment",
            value: create(change.TransactionFragmentSchema, {
              transactionId: text.encode(transaction.cursor),
              transactionCursor: cursor(transaction.cursor),
              fragmentIndex: index,
              firstFragment: index === 0,
              finalFragment: last && options.leaveTransactionOpen !== true,
              postgresXid: 100n,
              beginLsn: "0/10",
              commitLsn: "0/20",
              commitTimeUnixMicros: options.commitTimes?.[position] ?? 2n,
              operations: slice.map(wireOperation),
            }),
          },
        }),
      );
    }
  }
  const digest = new BatchFramesDigest();
  for (const frame of frames) {
    frame.bodySha256 = await changeFrameBodySha256(frame);
    digest.push(frame.bodySha256);
  }
  if (options.truncateAfterFragments !== true) {
    const end = create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: {
        case: "batchEnd",
        value: create(change.BatchEndSchema, {
          throughCursorInclusive: cursor(throughCursor),
          reachedRequestedFence: options.requestedFence === throughCursor,
          currentlyUpToDate: true,
          relevantTransactionCount: BigInt(transactions.length),
          relevantOperationCount: BigInt(operationCount),
          serverTimeUnixMicros: 3n,
          acknowledgedMutationIds: [...(options.acknowledgedMutationIds ?? [])],
          frameCount: BigInt(digest.frames),
          framesSha256: await digest.finish(),
        }),
      },
    });
    end.bodySha256 = await changeFrameBodySha256(end);
    frames.push(end);
  }
  const encoded = frames.map((frame) =>
    encodeFrame(change.ChangeStreamFrameSchema, frame),
  );
  const bytes = new Uint8Array(
    encoded.reduce((total, frame) => total + frame.byteLength, 0),
  );
  let offset = 0;
  for (const frame of encoded) {
    bytes.set(frame, offset);
    offset += frame.byteLength;
  }
  return bytes;
}

// ------------------------------------------------- the independent reference

/**
 * Applies one operation to the source state the reference build is encoded
 * from.
 */
function applyToSource(state: SourceState, operation: Op): void {
  const rows = state.get(operation.collection);
  if (!rows) throw new Error(`unknown collection ${operation.collection}`);
  const table = schemaOf(operation.collection);
  const matches = (row: Row, key: Row) =>
    table.primaryKey.every((name) => row[name] === key[name]);
  if (operation.kind === "insert") {
    if (rows.some((row) => matches(row, operation.row)))
      throw new Error("reference insert hit a live row");
    rows.push(structuredClone(operation.row));
    return;
  }
  const index = rows.findIndex((row) => matches(row, operation.key));
  if (index < 0) throw new Error("reference update or delete hit no row");
  if (operation.kind === "delete") {
    rows.splice(index, 1);
    return;
  }
  const row = rows[index];
  if (!row) throw new Error("reference row vanished");
  Object.assign(row, structuredClone(operation.changed));
}

function mount(bytes: Uint8Array, digest: `sha256:${string}`, cursor: string) {
  return openDirectColumnarArtifact(
    bytes.slice().buffer,
    {
      byteLength: bytes.byteLength,
      digest,
      identity: { ...FIXTURE_ARTIFACT_IDENTITY, cursor },
      schema: WORKSPACE_CONTRACT,
    },
    undefined,
  );
}

function overlayOver(workspace: DirectColumnarWorkspace): OverlayEngine {
  return new OverlayEngine(
    WORKSPACE_CONTRACT.tables.map((table) => {
      const columns = table.columns.map((column) => column.name);
      return {
        name: table.name,
        columns,
        base: artifactBase(workspace.table(table.name), columns),
      };
    }),
  );
}

async function encodeAt(state: SourceState, cursor: string) {
  const encoded = encodeWorkspace(state, {
    identity: { ...FIXTURE_ARTIFACT_IDENTITY, cursor },
  });
  return {
    encoded,
    workspace: await mount(encoded.bytes, encoded.digest, cursor),
  };
}

/**
 * One collection's rows keyed by primary key, independent of physical order: a
 * full build lays rows out in source order, and a tail appends its inserts
 * after the base. The comparison is exact on every value: a `bigint` does not
 * equal the same number, and `Uint8Array` bytes are compared as bytes.
 */
function renderRow(values: readonly SqlValue[]): string {
  return JSON.stringify(
    values.map((value) => {
      if (typeof value === "bigint") return { bigint: value.toString() };
      if (value instanceof Uint8Array)
        return { bytes: [...value].map((byte) => byte.toString(16)).join("") };
      return value;
    }),
  );
}

function renderOverlay(
  engine: OverlayEngine,
): Map<string, Map<string, string>> {
  const result = new Map<string, Map<string, string>>();
  for (const table of WORKSPACE_CONTRACT.tables) {
    const keyOrdinals = table.primaryKey.map((name) =>
      table.columns.findIndex((column) => column.name === name),
    );
    const rows = engine.rows(table.name);
    const byKey = new Map<string, string>();
    for (const row of rows) {
      const key = renderRow(keyOrdinals.map((ordinal) => row[ordinal] ?? null));
      if (byKey.has(key))
        throw new Error(`${table.name} has a duplicate key ${key}`);
      byKey.set(key, renderRow(row));
    }
    result.set(table.name, byKey);
  }
  return result;
}

function renderWorkspace(
  workspace: DirectColumnarWorkspace,
): Map<string, Map<string, string>> {
  const result = new Map<string, Map<string, string>>();
  for (const table of WORKSPACE_CONTRACT.tables) {
    const physical = workspace.table(table.name);
    const accessors = table.columns.map((column) =>
      physical.column(column.name),
    );
    const keyOrdinals = table.primaryKey.map((name) =>
      table.columns.findIndex((column) => column.name === name),
    );
    const byKey = new Map<string, string>();
    for (let row = 0; row < physical.rows; row += 1) {
      const values = accessors.map((column) => column.valueAt(row) as SqlValue);
      byKey.set(
        renderRow(keyOrdinals.map((ordinal) => values[ordinal] ?? null)),
        renderRow(values),
      );
    }
    result.set(table.name, byKey);
  }
  return result;
}

function diff(
  applied: Map<string, Map<string, string>>,
  reference: Map<string, Map<string, string>>,
): string[] {
  const problems: string[] = [];
  for (const [table, expected] of reference) {
    const actual = applied.get(table);
    if (!actual) {
      problems.push(`${table}: missing from the tail-applied mount`);
      continue;
    }
    if (actual.size !== expected.size)
      problems.push(
        `${table}: ${actual.size} rows applied, ${expected.size} rows in the fresh build`,
      );
    for (const [key, row] of expected) {
      const found = actual.get(key);
      if (found === undefined) problems.push(`${table}: ${key} is absent`);
      else if (found !== row)
        problems.push(
          `${table}: ${key}\n  applied   ${found}\n  reference ${row}`,
        );
    }
    for (const key of actual.keys())
      if (!expected.has(key)) problems.push(`${table}: ${key} is extra`);
  }
  return problems;
}

// ------------------------------------------------------------------- the tail

const TAIL: readonly WireTransaction[] = [
  {
    cursor: "cursor-1",
    operations: [
      {
        kind: "insert",
        collection: "issues",
        row: {
          id: "issue-100",
          organization_id: "org-1",
          project_id: "project-0",
          assignee_id: "user-1",
          status: "open",
          priority: 9,
          title: "Tail issue",
          event_count: 0,
          updated_at: stamp(20, 101),
        },
      },
      {
        kind: "insert",
        collection: "issue_tags",
        row: { issue_id: "issue-100", tag: "tail" },
      },
      {
        kind: "update",
        collection: "issues",
        key: { id: "issue-2" },
        changed: { status: "resolved", event_count: 42, assignee_id: null },
      },
    ],
  },
  {
    cursor: "cursor-2",
    operations: [
      {
        kind: "insert",
        collection: "events",
        row: {
          id: 11n,
          organization_id: "org-1",
          project_id: "project-1",
          issue_id: "issue-100",
          level: "error",
          message: "tail event",
          occurred_at: stamp(21, 202),
          payload: { source: "tail", depth: [1, 2, 3] },
        },
      },
      {
        kind: "update",
        collection: "events",
        key: { id: 3n },
        changed: {
          message: "rewritten",
          payload: { rewritten: true },
          occurred_at: stamp(22, 303),
          issue_id: null,
        },
      },
      { kind: "delete", collection: "comments", key: { id: "comment-1" } },
      {
        kind: "delete",
        collection: "issue_tags",
        key: { issue_id: "issue-1", tag: "triage" },
      },
      {
        kind: "update",
        collection: "users",
        key: { id: "user-3" },
        changed: { active: true, display_name: "Renamed" },
      },
    ],
    // Split across frames so the decoder has to reassemble one transaction
    // from several fragments before it may be applied.
    fragments: 3,
  },
];

async function applyTail(
  transactions: readonly WireTransaction[],
  options: BatchOptions = {},
) {
  const source = seedState();
  const base = await encodeAt(source, "cursor-0");
  const engine = overlayOver(base.workspace);
  const binding = compileColumnarChangeBinding(WORKSPACE_CONTRACT, CONTRACT);
  const bytes = await encodeBatch(
    "cursor-0",
    transactions.at(-1)?.cursor ?? "cursor-0",
    transactions,
    options,
  );
  return { source, engine, binding, bytes };
}

describe("the direct-columnar live tail", () => {
  test("a tail-applied mount matches a fresh full build at the same cursor", async () => {
    const { source, engine, binding, bytes } = await applyTail(TAIL);
    const decoded = await decodeColumnarChangeBatch(
      bytes,
      binding,
      text.encode("cursor-0"),
    );
    expect(decoded.transactionCount).toBe(TAIL.length);
    engine.applyCanonical(decoded.batch);
    expect(new TextDecoder().decode(decoded.throughCursorInclusive)).toBe(
      "cursor-2",
    );

    const reference = cloneState(source);
    for (const transaction of TAIL)
      for (const operation of transaction.operations)
        applyToSource(reference, operation);
    const fresh = await encodeAt(reference, "cursor-2");

    const problems = diff(
      renderOverlay(engine),
      renderWorkspace(fresh.workspace),
    );
    expect(problems.join("\n")).toBe("");
  });

  test("the reference build is a real check, not a restatement of the tail", async () => {
    // One value is changed in the tail and not in the reference; the
    // comparison must find that row.
    const { source, engine, binding, bytes } = await applyTail(TAIL);
    engine.applyCanonical(
      (await decodeColumnarChangeBatch(bytes, binding, text.encode("cursor-0")))
        .batch,
    );
    const reference = cloneState(source);
    for (const transaction of TAIL)
      for (const operation of transaction.operations)
        applyToSource(reference, operation);
    const drifted = reference.get("users")?.find((row) => row.id === "user-3");
    if (!drifted) throw new Error("reference row is missing");
    drifted.display_name = "Not what the tail said";
    const fresh = await encodeAt(reference, "cursor-2");
    const problems = diff(
      renderOverlay(engine),
      renderWorkspace(fresh.workspace),
    );
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain("users");
    expect(problems[0]).toContain("Not what the tail said");
  });

  test("a response that stops inside a transaction is refused whole", async () => {
    const { engine, binding, bytes } = await applyTail(TAIL, {
      leaveTransactionOpen: true,
    });
    const before = renderOverlay(engine);
    await expect(
      decodeColumnarChangeBatch(bytes, binding, text.encode("cursor-0")),
    ).rejects.toMatchObject({ code: "CHANGE_STREAM_INVALID" });
    // Nothing reached the engine, so the mount is still at `cursor-0`.
    expect(diff(renderOverlay(engine), before)).toEqual([]);
  });

  test("a response that stops before its batch end is refused whole", async () => {
    const { engine, binding, bytes } = await applyTail(TAIL, {
      truncateAfterFragments: true,
    });
    const before = renderOverlay(engine);
    await expect(
      decodeColumnarChangeBatch(bytes, binding, text.encode("cursor-0")),
    ).rejects.toMatchObject({ code: "CHANGE_STREAM_INVALID" });
    expect(diff(renderOverlay(engine), before)).toEqual([]);
  });

  test("a body changed after its digest was taken is refused", async () => {
    const { binding, bytes } = await applyTail(TAIL);
    // `issue-100` made `issue-200`: still well-formed protobuf, but no longer
    // the bytes the frame's `body_sha256` was taken over.
    const id = text.encode("issue-100");
    const at = bytes.findIndex((_, index) =>
      id.every((byte, offset) => bytes[index + offset] === byte),
    );
    expect(at).toBeGreaterThan(0);
    const tampered = bytes.slice();
    tampered[at + 6] = "2".charCodeAt(0);
    await expect(
      decodeColumnarChangeBatch(tampered, binding, text.encode("cursor-0")),
    ).rejects.toMatchObject({
      code: "CHANGE_STREAM_INVALID",
      message: expect.stringContaining("bodySha256"),
    });
  });

  test("a batch whose lineage does not continue this mount is refused", async () => {
    const { binding } = await applyTail(TAIL);
    const bytes = await encodeBatch("cursor-9", "cursor-10", TAIL);
    await expect(
      decodeColumnarChangeBatch(bytes, binding, text.encode("cursor-0")),
    ).rejects.toMatchObject({ code: "CHANGE_STREAM_INVALID" });
  });
});

describe("the change binding is compiled against the artifact, not assumed", () => {
  test("a table bound to fewer columns than it has is refused", () => {
    const partial: ColumnarChangeContract = {
      ...CONTRACT,
      tables: CONTRACT.tables.map((table) =>
        table.collection === "users"
          ? { ...table, columns: table.columns.slice(0, 2) }
          : table,
      ),
    };
    expect(() =>
      compileColumnarChangeBinding(WORKSPACE_CONTRACT, partial),
    ).toThrow(/could not carry a complete row image/u);
  });

  test("a key bound in the wrong order is refused", () => {
    const permuted: ColumnarChangeContract = {
      ...CONTRACT,
      tables: CONTRACT.tables.map((table) =>
        table.collection === "issue_tags"
          ? { ...table, primaryKey: [...table.primaryKey].reverse() }
          : table,
      ),
    };
    expect(() =>
      compileColumnarChangeBinding(WORKSPACE_CONTRACT, permuted),
    ).toThrow(/key part 0/u);
  });
});

// ------------------------------------------------------------- the delivery

/**
 * A mount that runs the real decode and the real overlay behind the surface
 * `EngineWorkerClient` exposes.
 */
function liveMount(
  engine: OverlayEngine,
  cursor: string,
): ColumnarLiveMount & {
  readonly applied: string[];
} {
  const binding = compileColumnarChangeBinding(WORKSPACE_CONTRACT, CONTRACT);
  let current: Uint8Array<ArrayBuffer> = text.encode(cursor);
  const applied: string[] = [];
  return {
    applied,
    async applyChangeBatches(
      batches: readonly Uint8Array[],
      options?: ApplyChangeOptions,
    ) {
      const decoded = await decodeColumnarChangeBatches(
        batches,
        options?.planId === undefined
          ? binding
          : { ...binding, planId: options.planId },
        current,
        options?.fence,
      );
      const result = engine.applyCanonical(decoded.batch);
      current = Uint8Array.from(decoded.throughCursorInclusive);
      applied.push(utf8.decode(current));
      return {
        state: {
          cursor: current.slice(),
          live: true,
          collectionRows: {},
          collectionGenerations: {},
          pendingMutationIds: [...engine.pendingMutationIds()],
        },
        changedCollections: result.changedCollections,
        acknowledgedMutationIds: result.acknowledgedMutationIds,
        transactionCount: decoded.transactionCount,
        operationCount: decoded.operationCount,
        currentlyUpToDate: decoded.currentlyUpToDate,
        latestCommitTimeUnixMicros: decoded.latestCommitTimeUnixMicros,
      };
    },
  };
}

/** One observed change request: what the session asked the endpoint for. */
interface ObservedRequest {
  readonly from: string;
  readonly through: string | undefined;
  readonly shardCursors: readonly (string | undefined)[];
  readonly pendingMutationIds: readonly string[];
  readonly stream: boolean;
  readonly wait: number;
  readonly transactions: number;
}

/** The longest wait the protocol allows, which a fenced catch-up asks for. */
const MAX_WAIT_MS = 120_000;
/** The most transactions the page asks for in one batch. */
const BATCH_TRANSACTIONS = 250;

const requestKey = (from: string, through: string | undefined): string =>
  `${from}|${through ?? "live"}`;

function sessionOver(
  mount: ColumnarLiveMount,
  responses: ReadonlyMap<string, Uint8Array>,
  onFailed?: (error: unknown) => void,
  extra: Partial<ColumnarLiveSessionOptions> & {
    readonly observed?: ObservedRequest[];
  } = {},
): ColumnarLiveSession {
  const { observed, ...options } = extra;
  return new ColumnarLiveSession(mount, {
    endpoint: "https://change.test/v1/change",
    planId: PLAN_ID,
    authorizationScopeToken: "scope-token",
    tableIds: CONTRACT.tables.map((table) => table.tableId),
    clientInstanceId: "instance-1",
    deviceId: "device-1",
    cursor: text.encode("cursor-0"),
    maxBatchBytes: 1_048_576,
    longPollTimeoutMs: 10,
    ...(onFailed ? { onFailed } : {}),
    fetch: async (_input, init) => {
      const body = init?.body;
      if (!(body instanceof Uint8Array)) throw new Error("unexpected body");
      const request = fromBinary(change.ChangeRequestSchema, body);
      const from = utf8.decode(
        request.fromCursorExclusive?.token ?? new Uint8Array(),
      );
      const through = request.throughCursorInclusive
        ? utf8.decode(request.throughCursorInclusive.token)
        : undefined;
      observed?.push({
        from,
        through,
        shardCursors: request.tables.map((table) =>
          table.shardSnapshotCursor
            ? utf8.decode(table.shardSnapshotCursor.token)
            : undefined,
        ),
        pendingMutationIds: [...request.pendingMutationIds],
        stream: request.stream,
        wait: request.longPollTimeoutMs,
        transactions: request.maxTransactions,
      });
      // A live response is keyed by its cursor alone; a fenced one by
      // `from|through`.
      const served =
        responses.get(requestKey(from, through)) ??
        (through === undefined ? responses.get(from) : undefined);
      // Nothing left for this cursor: hold the poll open the way a real
      // endpoint does rather than answering with a batch that repeats.
      if (!served) return new Response(null, { status: 503 });
      return new Response(served.slice() as unknown as BodyInit, {
        status: 200,
      });
    },
    ...options,
  });
}

describe("the live session delivers whole batches and follows the mount", () => {
  test("each poll asks from the cursor the mount last published", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const responses = new Map([
      [
        "cursor-0",
        await encodeBatch("cursor-0", "cursor-1", [
          { cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] },
        ]),
      ],
      [
        "cursor-1",
        await encodeBatch("cursor-1", "cursor-2", [
          { cursor: "cursor-2", operations: TAIL[1]?.operations ?? [] },
        ]),
      ],
    ]);
    const session = sessionOver(mount, responses);
    session.start();
    await vi.waitFor(() =>
      expect(mount.applied).toEqual(["cursor-1", "cursor-2"]),
    );
    await session.stopped();
    expect(session.status.publishedBatches).toBe(2);
    expect(new TextDecoder().decode(session.status.cursor)).toBe("cursor-2");

    const reference = cloneState(source);
    for (const transaction of TAIL)
      for (const operation of transaction.operations)
        applyToSource(reference, operation);
    const fresh = await encodeAt(reference, "cursor-2");
    expect(
      diff(renderOverlay(engine), renderWorkspace(fresh.workspace)).join("\n"),
    ).toBe("");
  });

  test("a torn response stops the session instead of advancing it", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const before = renderOverlay(engine);
    const failures: unknown[] = [];
    const session = sessionOver(
      mount,
      new Map([
        [
          "cursor-0",
          await encodeBatch("cursor-0", "cursor-2", TAIL, {
            leaveTransactionOpen: true,
          }),
        ],
      ]),
      (error) => failures.push(error),
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    await session.stopped();
    expect(session.status.publishedBatches).toBe(0);
    expect(new TextDecoder().decode(session.status.cursor)).toBe("cursor-0");
    expect(diff(renderOverlay(engine), before)).toEqual([]);
  });
});

/**
 * A response body the test writes as a server writes an open response: one
 * chunk at a time, ended, cut by the transport, or aborted with the request.
 */
function openResponse(signal: AbortSignal | null | undefined) {
  let body!: ReadableStreamDefaultController<Uint8Array>;
  const response = new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        body = controller;
      },
    }),
  );
  signal?.addEventListener(
    "abort",
    () => body.error(new DOMException("aborted", "AbortError")),
    { once: true },
  );
  return {
    response,
    write: (bytes: Uint8Array) => body.enqueue(bytes.slice()),
    end: () => body.close(),
    cut: () => body.error(new TypeError("network error")),
  };
}

/** A mount whose applies can be held, recording how many batches each took. */
function heldMount(inner: ReturnType<typeof liveMount>) {
  const groups: number[] = [];
  let gate: Promise<void> | undefined;
  const mount: ColumnarLiveMount = {
    async applyChangeBatches(batches, options) {
      groups.push(batches.length);
      await gate;
      return inner.applyChangeBatches(batches, options);
    },
  };
  return {
    mount,
    groups,
    hold(): () => void {
      let release!: () => void;
      gate = new Promise((resolve) => (release = resolve));
      return () => {
        gate = undefined;
        release();
      };
    },
  };
}

/** The `from` cursor of a change request body. */
function requestedFrom(init: RequestInit | undefined): string {
  const body = init?.body;
  if (!(body instanceof Uint8Array)) throw new Error("unexpected body");
  const request = fromBinary(change.ChangeRequestSchema, body);
  return utf8.decode(request.fromCursorExclusive?.token ?? new Uint8Array());
}

/** A request with nothing to say: held open until it is aborted. */
function heldOpen(init: RequestInit | undefined): Promise<Response> {
  return new Promise((_resolve, reject) =>
    init?.signal?.addEventListener(
      "abort",
      () => reject(new DOMException("aborted", "AbortError")),
      { once: true },
    ),
  );
}

function liveSession(
  mount: ColumnarLiveMount,
  fetch: typeof globalThis.fetch,
  extra: Partial<ColumnarLiveSessionOptions> = {},
): ColumnarLiveSession {
  return new ColumnarLiveSession(mount, {
    endpoint: "https://change.test/v1/change",
    planId: PLAN_ID,
    authorizationScopeToken: "scope-token",
    tableIds: CONTRACT.tables.map((table) => table.tableId),
    clientInstanceId: "instance-1",
    deviceId: "device-1",
    cursor: text.encode("cursor-0"),
    maxBatchBytes: 1_048_576,
    longPollTimeoutMs: 10,
    fetch,
    ...extra,
  });
}

describe("a live request reads its batches from one open response", () => {
  const tail = async (): Promise<
    readonly [Uint8Array, Uint8Array, Uint8Array]
  > => [
    await encodeBatch("cursor-0", "cursor-1", [
      { cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] },
    ]),
    await encodeBatch("cursor-1", "cursor-2", [
      { cursor: "cursor-2", operations: TAIL[1]?.operations ?? [] },
    ]),
    // A batch that moves the cursor over transactions no table here holds.
    await encodeBatch("cursor-2", "cursor-3", []),
  ];

  test("batches that arrive while the mount applies are published together", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const inner = liveMount(engine, "cursor-0");
    const held = heldMount(inner);
    const [first, second, third] = await tail();
    const froms: string[] = [];
    let body: ReturnType<typeof openResponse> | undefined;
    const published: string[] = [];
    const session = liveSession(
      held.mount,
      (_input, init) => {
        froms.push(requestedFrom(init));
        if (froms.length > 1) return heldOpen(init);
        body = openResponse(init?.signal);
        return Promise.resolve(body.response);
      },
      {
        onPublished: (applied) =>
          published.push(utf8.decode(applied.state.cursor ?? new Uint8Array())),
      },
    );
    session.start();
    await vi.waitFor(() => expect(body).toBeDefined());
    const release = held.hold();
    body?.write(first);
    await vi.waitFor(() => expect(held.groups).toEqual([1]));
    body?.write(second);
    body?.write(third);
    release();
    await vi.waitFor(() => expect(published).toEqual(["cursor-1", "cursor-3"]));
    expect(held.groups).toEqual([1, 2]);
    expect(inner.applied).toEqual(["cursor-1", "cursor-3"]);

    // The response ends after a complete batch; the next one is asked for at
    // once, from where the mount stands.
    body?.end();
    await vi.waitFor(() => expect(froms).toEqual(["cursor-0", "cursor-3"]));
    await session.stopped();
    expect(session.status).toMatchObject({
      failure: null,
      publishedBatches: 3,
    });

    const reference = cloneState(source);
    for (const transaction of TAIL)
      for (const operation of transaction.operations)
        applyToSource(reference, operation);
    const fresh = await encodeAt(reference, "cursor-3");
    expect(
      diff(renderOverlay(engine), renderWorkspace(fresh.workspace)).join("\n"),
    ).toBe("");
  });

  test("a body cut inside a batch applies the batches before it and asks again from the mount", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const [first, second] = await tail();
    const froms: string[] = [];
    const failures: unknown[] = [];
    let body: ReturnType<typeof openResponse> | undefined;
    const session = liveSession(
      mount,
      (_input, init) => {
        const from = requestedFrom(init);
        froms.push(from);
        if (froms.length === 1) {
          body = openResponse(init?.signal);
          body.write(first);
          body.write(second.subarray(0, 40));
          return Promise.resolve(body.response);
        }
        if (from === "cursor-1")
          return Promise.resolve(
            new Response(second.slice() as unknown as BodyInit),
          );
        return heldOpen(init);
      },
      { onFailed: (error) => failures.push(error) },
    );
    session.start();
    await vi.waitFor(() => expect(mount.applied).toEqual(["cursor-1"]));
    // A cut stream discards what its reader had not taken; either way the
    // part of the second batch never reaches the mount.
    body?.cut();
    await vi.waitFor(() =>
      expect(mount.applied).toEqual(["cursor-1", "cursor-2"]),
    );
    await vi.waitFor(() =>
      expect(froms).toEqual(["cursor-0", "cursor-1", "cursor-2"]),
    );
    await session.stopped();
    expect(failures).toEqual([]);
  });

  test("a body the server ends inside a batch stops the session after the batches before it", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const [first, second] = await tail();
    const failures: unknown[] = [];
    const session = liveSession(
      mount,
      (_input, init) => {
        const body = openResponse(init?.signal);
        body.write(first);
        body.write(second.subarray(0, 40));
        body.end();
        return Promise.resolve(body.response);
      },
      { onFailed: (error) => failures.push(error) },
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    expect(failures[0]).toMatchObject({
      code: "CHANGE_STREAM_INVALID",
      message: "change response ended inside a batch",
    });
    expect(mount.applied).toEqual(["cursor-1"]);
    expect(utf8.decode(session.status.cursor)).toBe("cursor-1");
  });

  test("a batch that does not continue the one before it refuses its whole group", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const before = renderOverlay(engine);
    const [first, , third] = await tail();
    const failures: unknown[] = [];
    const session = liveSession(
      mount,
      (_input, init) => {
        const body = openResponse(init?.signal);
        // One chunk, so both batches are held when the first apply starts.
        const both = new Uint8Array(first.byteLength + third.byteLength);
        both.set(first);
        both.set(third, first.byteLength);
        body.write(both);
        body.end();
        return Promise.resolve(body.response);
      },
      { onFailed: (error) => failures.push(error) },
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    expect(failures[0]).toMatchObject({ code: "CHANGE_STREAM_INVALID" });
    expect(mount.applied).toEqual([]);
    expect(utf8.decode(session.status.cursor)).toBe("cursor-0");
    expect(diff(renderOverlay(engine), before)).toEqual([]);
  });

  test("a nudge ends the response, applies what had arrived and asks again at once", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const inner = liveMount(engine, "cursor-0");
    const held = heldMount(inner);
    const [first, second] = await tail();
    let pending: readonly string[] = [];
    const requests: { from: string; ids: readonly string[] }[] = [];
    let body: ReturnType<typeof openResponse> | undefined;
    const session = liveSession(
      held.mount,
      (_input, init) => {
        const request = fromBinary(
          change.ChangeRequestSchema,
          init?.body as Uint8Array,
        );
        requests.push({
          from: requestedFrom(init),
          ids: [...request.pendingMutationIds],
        });
        if (requests.length > 1) return heldOpen(init);
        body = openResponse(init?.signal);
        return Promise.resolve(body.response);
      },
      { pendingMutationIds: () => pending },
    );
    session.start();
    await vi.waitFor(() => expect(body).toBeDefined());
    const release = held.hold();
    body?.write(first);
    await vi.waitFor(() => expect(held.groups).toEqual([1]));
    body?.write(second);
    // Let the second batch arrive whole before the nudge aborts the body.
    await new Promise((resolve) => setTimeout(resolve, 0));
    pending = ["m-1"];
    session.nudge();
    release();
    await vi.waitFor(() =>
      expect(inner.applied).toEqual(["cursor-1", "cursor-2"]),
    );
    await vi.waitFor(() =>
      expect(requests).toEqual([
        { from: "cursor-0", ids: [] },
        { from: "cursor-2", ids: ["m-1"] },
      ]),
    );
    expect(session.status).toMatchObject({ running: true, failure: null });
    await session.stopped();
  });

  test("the reader stops while it holds more unapplied batches than the bound", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const inner = liveMount(engine, "cursor-0");
    const held = heldMount(inner);
    const batches = await Promise.all(
      [0, 1, 2, 3].map((index) =>
        encodeBatch(
          `cursor-${String(index)}`,
          `cursor-${String(index + 1)}`,
          [],
        ),
      ),
    );
    const size = Math.max(...batches.map((batch) => batch.byteLength));
    let pulls = 0;
    const release = held.hold();
    const session = liveSession(
      held.mount,
      (_input, init) => {
        if (requestedFrom(init) !== "cursor-0") return heldOpen(init);
        return Promise.resolve(
          new Response(
            new ReadableStream<Uint8Array>(
              {
                pull(controller) {
                  const next = batches[pulls];
                  pulls += 1;
                  if (next) controller.enqueue(next.slice());
                  else controller.close();
                },
              },
              { highWaterMark: 0 },
            ),
          ),
        );
      },
      // Two held batches exceed the bound; one does not.
      { maxBatchBytes: 2 * size - 1 },
    );
    session.start();
    // The first batch is being applied; the second and third are read and
    // held, and then the reader waits rather than read the fourth.
    await vi.waitFor(() => expect(pulls).toBe(3));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(pulls).toBe(3);
    expect(held.groups).toEqual([1]);
    release();
    await vi.waitFor(() =>
      expect(inner.applied).toEqual(["cursor-1", "cursor-3", "cursor-4"]),
    );
    expect(held.groups).toEqual([1, 2, 1]);
    await session.stopped();
  });

  test("a stopped session applies none of the batches it read and had not applied", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const inner = liveMount(overlayOver(base.workspace), "cursor-0");
    const held = heldMount(inner);
    const [first, second] = await tail();
    let body: ReturnType<typeof openResponse> | undefined;
    const session = liveSession(held.mount, (_input, init) => {
      if (body) return heldOpen(init);
      body = openResponse(init?.signal);
      return Promise.resolve(body.response);
    });
    session.start();
    await vi.waitFor(() => expect(body).toBeDefined());
    const release = held.hold();
    body?.write(first);
    await vi.waitFor(() => expect(held.groups).toEqual([1]));
    body?.write(second);
    await new Promise((resolve) => setTimeout(resolve, 0));
    session.stop();
    release();
    await session.stopped();
    expect(held.groups).toEqual([1]);
    expect(inner.applied).toEqual(["cursor-1"]);
  });

  test("a batch the mount refuses ends its request and cancels its body while the reader waits for room", async () => {
    const batches = await Promise.all(
      [0, 1, 2, 3].map((index) =>
        encodeBatch(
          `cursor-${String(index)}`,
          `cursor-${String(index + 1)}`,
          [],
        ),
      ),
    );
    const size = Math.max(...batches.map((batch) => batch.byteLength));
    const refusal = new RuntimeError("ARTIFACT_INVALID", "the mount refused");
    const applying: PromiseWithResolvers<void> = Promise.withResolvers();
    const mount: ColumnarLiveMount = {
      async applyChangeBatches() {
        await applying.promise;
        throw refusal;
      },
    };
    let pulls = 0;
    let cancelled = false;
    let signal: AbortSignal | undefined;
    const failures: unknown[] = [];
    const session = liveSession(
      mount,
      (_input, init) => {
        if (signal) return heldOpen(init);
        signal = init?.signal ?? undefined;
        // A body that keeps no watch on the request's signal: only the session
        // cancelling it ends it.
        return Promise.resolve(
          new Response(
            new ReadableStream<Uint8Array>(
              {
                pull(controller) {
                  const next = batches[pulls];
                  pulls += 1;
                  if (next) controller.enqueue(next.slice());
                },
                cancel() {
                  cancelled = true;
                },
              },
              { highWaterMark: 0 },
            ),
          ),
        );
      },
      {
        maxBatchBytes: 2 * size - 1,
        onFailed: (error) => failures.push(error),
      },
    );
    session.start();
    // The first batch is being applied and two more are held: the reader
    // waits for room.
    await vi.waitFor(() => expect(pulls).toBe(3));
    applying.resolve();
    await vi.waitFor(() => expect(failures).toEqual([refusal]));
    expect(signal?.aborted).toBe(true);
    expect(cancelled).toBe(true);
    await session.stopped();
  });

  test("an empty response stops the session", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const mount = liveMount(overlayOver(base.workspace), "cursor-0");
    let requests = 0;
    const failures: unknown[] = [];
    const session = liveSession(
      mount,
      (_input, init) => {
        requests += 1;
        if (requests > 1) return heldOpen(init);
        return Promise.resolve(new Response(new Uint8Array()));
      },
      { onFailed: (error) => failures.push(error) },
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    expect(failures[0]).toMatchObject({
      code: "CHANGE_STREAM_INVALID",
      message: "change endpoint returned an empty response",
    });
    expect(requests).toBe(1);
    expect(mount.applied).toEqual([]);
  });
});

describe("the live session catches up to the activation fence before going live", () => {
  const fenceAt = (cursor: string) => ({
    cursor: text.encode(cursor),
    tableStarts: CONTRACT.tables.map((table) => ({
      tableId: table.tableId,
      snapshotCursor: text.encode("cursor-0"),
    })),
  });

  test("a fenced catch-up runs to the fence, then the session polls live", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const responses = new Map([
      [
        requestKey("cursor-0", "cursor-1"),
        await encodeBatch(
          "cursor-0",
          "cursor-1",
          [{ cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] }],
          { requestedFence: "cursor-1", tableStartCursor: "cursor-0" },
        ),
      ],
      [
        requestKey("cursor-1", undefined),
        await encodeBatch("cursor-1", "cursor-2", [
          { cursor: "cursor-2", operations: TAIL[1]?.operations ?? [] },
        ]),
      ],
    ]);
    const observed: ObservedRequest[] = [];
    const failures: unknown[] = [];
    const session = sessionOver(
      mount,
      responses,
      (error) => failures.push(error),
      { activationFence: fenceAt("cursor-1"), observed },
    );
    session.start();
    await vi.waitFor(() =>
      expect(mount.applied).toEqual(["cursor-1", "cursor-2"]),
    );
    await vi.waitFor(() => expect(observed.length).toBeGreaterThanOrEqual(3));
    await session.stopped();
    expect(failures).toEqual([]);
    expect(session.status.publishedBatches).toBe(2);
    expect(utf8.decode(session.status.cursor)).toBe("cursor-2");
    // The first request is fenced at the activation cursor and names the
    // artifact's snapshot cursor for every table; the live requests carry
    // neither, as the change endpoint requires. Both ask for an open
    // response: the fenced one for the longest wait, which only bounds how
    // long it may run, and the live ones for the plan's.
    expect(observed[0]).toEqual({
      from: "cursor-0",
      through: "cursor-1",
      shardCursors: CONTRACT.tables.map(() => "cursor-0"),
      pendingMutationIds: [],
      stream: true,
      wait: MAX_WAIT_MS,
      transactions: BATCH_TRANSACTIONS,
    });
    expect(observed[1]).toEqual({
      from: "cursor-1",
      through: undefined,
      shardCursors: CONTRACT.tables.map(() => undefined),
      pendingMutationIds: [],
      stream: true,
      wait: 10,
      transactions: BATCH_TRANSACTIONS,
    });
    // The fence is spent once the mount has stood at it. A mount that has
    // since moved past it is not "behind the fence" again: every later poll is
    // live, from wherever the mount now is.
    expect(
      observed.slice(2).map((request) => [request.from, request.through]),
    ).toEqual(observed.slice(2).map(() => ["cursor-2", undefined]));

    const reference = cloneState(source);
    for (const transaction of TAIL)
      for (const operation of transaction.operations)
        applyToSource(reference, operation);
    const fresh = await encodeAt(reference, "cursor-2");
    expect(
      diff(renderOverlay(engine), renderWorkspace(fresh.workspace)).join("\n"),
    ).toBe("");
  });

  test("the session reports going live once, after the batch that lands on the fence", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    // The fence is two batches away: the first fenced answer stops short of
    // it, the second lands on it, and the poll after that is live.
    const responses = new Map([
      [
        requestKey("cursor-0", "cursor-2"),
        await encodeBatch(
          "cursor-0",
          "cursor-1",
          [{ cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] }],
          { requestedFence: "cursor-2", tableStartCursor: "cursor-0" },
        ),
      ],
      [
        requestKey("cursor-1", "cursor-2"),
        await encodeBatch(
          "cursor-1",
          "cursor-2",
          [{ cursor: "cursor-2", operations: TAIL[1]?.operations ?? [] }],
          { requestedFence: "cursor-2", tableStartCursor: "cursor-0" },
        ),
      ],
    ]);
    const events: string[] = [];
    const observed: ObservedRequest[] = [];
    const session = sessionOver(
      mount,
      responses,
      (error) => events.push(`failed:${String(error)}`),
      {
        activationFence: fenceAt("cursor-2"),
        observed,
        onPublished: (applied) =>
          events.push(
            `published:${utf8.decode(applied.state.cursor ?? new Uint8Array())}:${session.status.phase}`,
          ),
      },
    );
    void session.live.then(() =>
      events.push(`live:${utf8.decode(session.status.cursor)}`),
    );
    expect(session.status.phase).toBe("catch-up");
    session.start();
    await vi.waitFor(() => expect(observed.length).toBeGreaterThanOrEqual(3));
    await session.stopped();
    // A page that renders on `live` and ignores publications until then
    // shows the activation cursor's state first, never a catch-up cursor's:
    // the landing batch is published (the mount moved) before the session
    // says it is live, and nothing earlier is live.
    expect(events).toEqual([
      "published:cursor-1:catch-up",
      "published:cursor-2:live",
      "live:cursor-2",
    ]);
    expect(observed[2]).toEqual({
      from: "cursor-2",
      through: undefined,
      shardCursors: CONTRACT.tables.map(() => undefined),
      pendingMutationIds: [],
      stream: true,
      wait: 10,
      transactions: BATCH_TRANSACTIONS,
    });
  });

  test("a catch-up whose batches arrive in one response lands on the fence and goes live", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    // The fence is two batches away, and one open response carries both.
    const first = await encodeBatch(
      "cursor-0",
      "cursor-1",
      [{ cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] }],
      { requestedFence: "cursor-2", tableStartCursor: "cursor-0" },
    );
    const second = await encodeBatch(
      "cursor-1",
      "cursor-2",
      [{ cursor: "cursor-2", operations: TAIL[1]?.operations ?? [] }],
      { requestedFence: "cursor-2", tableStartCursor: "cursor-0" },
    );
    const body = new Uint8Array(first.byteLength + second.byteLength);
    body.set(first);
    body.set(second, first.byteLength);
    const events: string[] = [];
    const observed: ObservedRequest[] = [];
    const session = sessionOver(
      mount,
      new Map([[requestKey("cursor-0", "cursor-2"), body]]),
      (error) => events.push(`failed:${String(error)}`),
      {
        activationFence: fenceAt("cursor-2"),
        observed,
        onPublished: (applied) =>
          events.push(
            `published:${utf8.decode(applied.state.cursor ?? new Uint8Array())}:${session.status.phase}`,
          ),
      },
    );
    void session.live.then(() =>
      events.push(`live:${utf8.decode(session.status.cursor)}`),
    );
    session.start();
    await vi.waitFor(() => expect(observed.length).toBeGreaterThanOrEqual(2));
    await session.stopped();
    // Both batches were applied from the one response, and the next request
    // is live: the catch-up asked the endpoint once.
    expect(events.at(-1)).toBe("live:cursor-2");
    expect(events).not.toContain("published:cursor-1:live");
    expect(events.filter((event) => event.startsWith("failed:"))).toEqual([]);
    expect(session.status.publishedBatches).toBe(2);
    expect(observed[0]).toEqual({
      from: "cursor-0",
      through: "cursor-2",
      shardCursors: CONTRACT.tables.map(() => "cursor-0"),
      pendingMutationIds: [],
      stream: true,
      wait: MAX_WAIT_MS,
      transactions: BATCH_TRANSACTIONS,
    });
    expect(
      observed.slice(1).map((request) => [request.from, request.through]),
    ).toEqual(observed.slice(1).map(() => ["cursor-2", undefined]));

    const reference = cloneState(source);
    for (const transaction of TAIL)
      for (const operation of transaction.operations)
        applyToSource(reference, operation);
    const fresh = await encodeAt(reference, "cursor-2");
    expect(
      diff(renderOverlay(engine), renderWorkspace(fresh.workspace)).join("\n"),
    ).toBe("");
  });

  test("a mount already at the fence skips catch-up and polls live at once", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const observed: ObservedRequest[] = [];
    const session = sessionOver(
      mount,
      new Map([
        [
          "cursor-0",
          await encodeBatch("cursor-0", "cursor-1", [
            { cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] },
          ]),
        ],
      ]),
      undefined,
      {
        activationFence: fenceAt("cursor-0"),
        observed,
      },
    );
    // Live from construction, before any request.
    expect(session.status.phase).toBe("live");
    await session.live;
    session.start();
    await vi.waitFor(() => expect(mount.applied).toEqual(["cursor-1"]));
    await session.stopped();
    expect(observed[0]?.through).toBeUndefined();
    expect(observed[0]?.shardCursors).toEqual(
      CONTRACT.tables.map(() => undefined),
    );
  });

  test("a fenced response that makes no cursor progress stops the session", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const before = renderOverlay(engine);
    const failures: unknown[] = [];
    const session = sessionOver(
      mount,
      new Map([
        [
          requestKey("cursor-0", "cursor-1"),
          // The endpoint answers the fenced request with an empty batch that
          // ends where it began and does not reach the fence.
          await encodeBatch("cursor-0", "cursor-0", [], {
            requestedFence: "cursor-1",
            tableStartCursor: "cursor-0",
          }),
        ],
      ]),
      (error) => failures.push(error),
      { activationFence: fenceAt("cursor-1") },
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    await session.stopped();
    expect(failures[0]).toMatchObject({ code: "CHANGE_STREAM_INVALID" });
    await expect(session.live).rejects.toBe(failures[0]);
    expect(session.status.publishedBatches).toBe(0);
    expect(utf8.decode(session.status.cursor)).toBe("cursor-0");
    expect(diff(renderOverlay(engine), before)).toEqual([]);
  });

  test("a session stopped before it reaches the fence never goes live", async () => {
    const mount = liveMount(
      overlayOver((await encodeAt(seedState(), "cursor-0")).workspace),
      "cursor-0",
    );
    const observed: ObservedRequest[] = [];
    // Nothing answers the fenced request, so the catch-up never lands.
    const session = sessionOver(mount, new Map(), undefined, {
      activationFence: fenceAt("cursor-1"),
      observed,
    });
    session.start();
    await vi.waitFor(() => expect(observed.length).toBeGreaterThanOrEqual(1));
    await session.stopped();
    await expect(session.live).rejects.toMatchObject({
      code: "INVALID_LIFECYCLE",
    });
    expect(session.status.phase).toBe("catch-up");
    expect(mount.applied).toEqual([]);
  });

  test("a fence with no cursor, or one that does not name exactly the subscribed tables, is refused at construction", async () => {
    const mount = liveMount(
      overlayOver((await encodeAt(seedState(), "cursor-0")).workspace),
      "cursor-0",
    );
    const fence = fenceAt("cursor-1");
    const [first, ...others] = fence.tableStarts;
    if (!first) throw new Error("the contract has no tables");
    const construct = (activationFence: ColumnarChangeFence) => () =>
      sessionOver(mount, new Map(), undefined, { activationFence });
    expect(construct({ ...fence, cursor: new Uint8Array() })).toThrow(
      expect.objectContaining({
        code: "INVALID_LIFECYCLE",
        message: "the activation fence has no cursor",
      }),
    );
    for (const tableStarts of [
      others,
      [first, ...fence.tableStarts],
      [...fence.tableStarts, { ...first, tableId: "public.elsewhere" }],
      [...others, { ...first, tableId: "public.elsewhere" }],
    ])
      expect(construct({ ...fence, tableStarts })).toThrow(
        expect.objectContaining({
          code: "INVALID_LIFECYCLE",
          message:
            "the activation fence must name exactly the subscribed tables",
        }),
      );
    expect(construct(fence)).not.toThrow();
  });

  test("a mount that publishes a batch without a cursor stops the session", async () => {
    const real = liveMount(
      overlayOver((await encodeAt(seedState(), "cursor-0")).workspace),
      "cursor-0",
    );
    const mount: ColumnarLiveMount = {
      async applyChangeBatches(batches, options) {
        const applied = await real.applyChangeBatches(batches, options);
        return { ...applied, state: { ...applied.state, cursor: undefined } };
      },
    };
    const failures: unknown[] = [];
    const published: unknown[] = [];
    const session = sessionOver(
      mount,
      new Map([
        [
          "cursor-0",
          await encodeBatch("cursor-0", "cursor-1", [
            { cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] },
          ]),
        ],
      ]),
      (error) => failures.push(error),
      { onPublished: (applied) => published.push(applied) },
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    await session.stopped();
    expect(failures[0]).toMatchObject({
      code: "INVALID_LIFECYCLE",
      message: "the mount published a batch without a cursor",
    });
    expect(published).toEqual([]);
    expect(session.status.publishedBatches).toBe(0);
    expect(utf8.decode(session.status.cursor)).toBe("cursor-0");
  });

  describe("a catch-up is bounded at 16,384 batches", () => {
    const BOUND = 16_384;
    /**
     * A mount that moves one cursor step per batch without decoding it, and
     * one response carrying `count` lone `BatchEnd` frames, so a catch-up of
     * the bound's length costs no encoding.
     */
    function counted(count: number, fence: string) {
      let applied = 0;
      const mount: ColumnarLiveMount = {
        async applyChangeBatches(batches) {
          applied += batches.length;
          return {
            state: {
              cursor: text.encode(`cursor-${applied}`),
              live: true,
              collectionRows: {},
              collectionGenerations: {},
              pendingMutationIds: [],
            },
            changedCollections: [],
            acknowledgedMutationIds: [],
            transactionCount: batches.length,
            operationCount: 0,
            currentlyUpToDate: false,
          };
        },
      };
      const frame = encodeFrame(
        change.ChangeStreamFrameSchema,
        create(change.ChangeStreamFrameSchema, {
          protocolVersion: 1,
          body: { case: "batchEnd", value: create(change.BatchEndSchema) },
        }),
      );
      const body = new Uint8Array(frame.byteLength * count);
      for (let index = 0; index < count; index += 1)
        body.set(frame, index * frame.byteLength);
      const events: string[] = [];
      const session = sessionOver(
        mount,
        new Map([[requestKey("cursor-0", fence), body]]),
        (error) =>
          events.push(
            `failed:${(error as { code?: string }).code}:${(error as Error).message}`,
          ),
        { activationFence: fenceAt(fence) },
      );
      session.live.then(
        () => events.push(`live:${applied}`),
        () => undefined,
      );
      return { session, events };
    }

    test("a catch-up that lands on the fence with its 16,384th batch goes live", async () => {
      const { session, events } = counted(BOUND, `cursor-${BOUND}`);
      session.start();
      await vi.waitFor(() => expect(events).toEqual([`live:${BOUND}`]));
      await session.stopped();
      expect(session.status.publishedBatches).toBe(BOUND);
    });

    test("a catch-up still short of the fence after 16,384 batches stops the session", async () => {
      const { session, events } = counted(BOUND, `cursor-${BOUND + 1}`);
      session.start();
      await vi.waitFor(() =>
        expect(events).toEqual([
          `failed:CHANGE_STREAM_INVALID:the fenced catch-up did not reach the activation cursor within ${BOUND} batches`,
        ]),
      );
      await session.stopped();
      expect(session.status.phase).toBe("catch-up");
      expect(session.status.publishedBatches).toBeLessThan(BOUND);
    });
  });

  test("a fenced batch that names other table starts is refused", async () => {
    const { engine, binding } = await applyTail([]);
    const before = renderOverlay(engine);
    const bytes = await encodeBatch(
      "cursor-0",
      "cursor-1",
      [{ cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] }],
      { requestedFence: "cursor-1", tableStartCursor: "cursor-7" },
    );
    await expect(
      decodeColumnarChangeBatch(bytes, binding, text.encode("cursor-0"), {
        cursor: text.encode("cursor-1"),
        tableStarts: CONTRACT.tables.map((table) => ({
          tableId: table.tableId,
          snapshotCursor: text.encode("cursor-0"),
        })),
      }),
    ).rejects.toMatchObject({ code: "CHANGE_STREAM_INVALID" });
    expect(diff(renderOverlay(engine), before)).toEqual([]);
  });
});

describe("the change request", () => {
  test("posts exactly the method, headers, body and signal, with no cache mode", async () => {
    // Chromium skips its preflight cache for a no-store request, so a `cache`
    // mode here would preflight every long poll. The response's own
    // Cache-Control keeps it out of the HTTP cache.
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const mount = liveMount(overlayOver(base.workspace), "cursor-0");
    const requests: (readonly [unknown, RequestInit | undefined])[] = [];
    const failures: unknown[] = [];
    const session = sessionOver(
      mount,
      new Map(),
      (error) => failures.push(error),
      {
        fetch: async (input, init) => {
          requests.push([input, init]);
          return new Response("refused", { status: 400 });
        },
      },
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    await session.stopped();
    expect(requests).toHaveLength(1);
    const [input, init] = requests[0] ?? [];
    expect(input).toBe("https://change.test/v1/change");
    expect(init).toStrictEqual({
      method: "POST",
      headers: {
        "content-type": "application/vnd.calque.change.v1+protobuf",
        accept: "application/vnd.calque.change.v1+protobuf",
      },
      body: expect.any(Uint8Array),
      signal: expect.any(AbortSignal),
    });
  });
});

describe("a refused change request names the server's refusal", () => {
  test("the stream error code and message reach the failure", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const frame = create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: {
        case: "error",
        value: create(change.StreamErrorSchema, {
          code: change.StreamErrorCode.REQUEST_INVALID,
          message: "change request is invalid",
        }),
      },
    });
    frame.bodySha256 = await changeFrameBodySha256(frame);
    const body = encodeFrame(change.ChangeStreamFrameSchema, frame);
    const failures: unknown[] = [];
    const session = sessionOver(
      mount,
      new Map(),
      (error) => failures.push(error),
      {
        fetch: async () =>
          new Response(body.slice() as unknown as BodyInit, { status: 400 }),
      },
    );
    session.start();
    await vi.waitFor(() => expect(failures).toHaveLength(1));
    await session.stopped();
    expect(failures[0]).toMatchObject({
      code: "CHANGE_STREAM_INVALID",
      message:
        "change request failed with HTTP 400: REQUEST_INVALID change request is invalid",
    });
  });

  /**
   * A live session whose every poll is answered with one framed `StreamError`
   * at `status`. Returns the failures it reported and how many times it asked,
   * once the caller's `until` has been reached.
   */
  const refusedSession = async (
    status: number,
    error: {
      code: change.StreamErrorCode;
      message: string;
      resnapshotRequired: boolean;
    },
    until: (state: {
      failures: unknown[];
      polls: () => number;
    }) => Promise<void>,
  ): Promise<{ failures: unknown[]; polls: number }> => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const mount = liveMount(overlayOver(base.workspace), "cursor-0");
    const frame = create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: { case: "error", value: create(change.StreamErrorSchema, error) },
    });
    frame.bodySha256 = await changeFrameBodySha256(frame);
    const body = encodeFrame(change.ChangeStreamFrameSchema, frame);
    const failures: unknown[] = [];
    let polls = 0;
    const session = sessionOver(
      mount,
      new Map(),
      (failure) => failures.push(failure),
      {
        fetch: async () => {
          polls += 1;
          return new Response(body.slice() as unknown as BodyInit, { status });
        },
      },
    );
    session.start();
    await until({ failures, polls: () => polls });
    session.stop();
    await session.stopped();
    return { failures, polls };
  };

  test("a refusal that requires a resnapshot says so, whatever its status", async () => {
    // Every refusal the server raises `resnapshotRequired` on is sent as 401,
    // 409 or 410, and only the flag separates "this mount fell behind" from
    // "this request was malformed". An authorization refusal carries the flag
    // too, and is named apart: it needs a fresh grant, and one the
    // application cannot supply locks the session rather than looping.
    for (const { status, code, expected } of [
      {
        status: 410,
        code: change.StreamErrorCode.CURSOR_BEFORE_RETENTION,
        expected: "RESNAPSHOT_REQUIRED",
      },
      {
        status: 409,
        code: change.StreamErrorCode.CURSOR_EPOCH_MISMATCH,
        expected: "RESNAPSHOT_REQUIRED",
      },
      {
        status: 401,
        code: change.StreamErrorCode.AUTHORIZATION_CHANGED,
        expected: "AUTHORIZATION_EXPIRED",
      },
    ] as const) {
      const { failures, polls } = await refusedSession(
        status,
        {
          code,
          message: "change cursor is before the retention floor",
          resnapshotRequired: true,
        },
        async ({ failures }) => {
          await vi.waitFor(() => expect(failures).toHaveLength(1));
        },
      );
      expect(failures[0], String(status)).toMatchObject({ code: expected });
      // Terminal, not retried: asking again for a range the source no longer
      // holds would only be refused again.
      expect(polls, String(status)).toBe(1);
    }
  });

  test("a retryable refusal that requires no resnapshot is still retried", async () => {
    // The other side of the same branch. A 503 the server asked to be retried
    // carries `resnapshotRequired: false`, and must keep being polled rather
    // than crossing the rebuild boundary.
    const { failures } = await refusedSession(
      503,
      {
        code: change.StreamErrorCode.INTERNAL_RETRYABLE,
        message: "change source is temporarily unavailable",
        resnapshotRequired: false,
      },
      async ({ polls }) => {
        await vi.waitFor(() => expect(polls()).toBeGreaterThan(1));
      },
    );
    expect(failures).toEqual([]);
  });
});

describe("the applied batch carries the newest wire commit time", () => {
  test("the decoder reports the latest fragment commit time, and none for an empty batch", async () => {
    const { binding, bytes } = await applyTail(TAIL, {
      commitTimes: [5_000_000n, 9_000_000n],
    });
    const decoded = await decodeColumnarChangeBatch(
      bytes,
      binding,
      text.encode("cursor-0"),
    );
    expect(decoded.latestCommitTimeUnixMicros).toBe(9_000_000n);
    const empty = await decodeColumnarChangeBatch(
      await encodeBatch("cursor-0", "cursor-0", []),
      binding,
      text.encode("cursor-0"),
    );
    expect(empty.latestCommitTimeUnixMicros).toBeNull();
  });

  test("the session hands the commit time to the publication callback", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const published: (bigint | null | undefined)[] = [];
    const session = sessionOver(
      mount,
      new Map([
        [
          "cursor-0",
          await encodeBatch(
            "cursor-0",
            "cursor-1",
            [{ cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] }],
            { commitTimes: [1_700_000_000_000_123n] },
          ),
        ],
      ]),
      undefined,
      {
        onPublished: (applied) =>
          published.push(applied.latestCommitTimeUnixMicros),
      },
    );
    session.start();
    await vi.waitFor(() => expect(published).toHaveLength(1));
    await session.stopped();
    expect(published).toEqual([1_700_000_000_000_123n]);
  });
});

// --------------------------------------------------------------- mutations

const operationBytes = (operation: Op): Uint8Array =>
  toBinary(change.OperationSchema, wireOperation(operation));

/** The three operation kinds a crud command may carry, on three collections. */
const OPTIMISTIC: readonly Op[] = [
  {
    kind: "update",
    collection: "issues",
    key: { id: "issue-2" },
    changed: { status: "resolved", event_count: 42, assignee_id: null },
  },
  {
    kind: "insert",
    collection: "issue_tags",
    row: { issue_id: "issue-2", tag: "optimistic" },
  },
  { kind: "delete", collection: "comments", key: { id: "comment-1" } },
];

/** Indexed fixture access that fails loudly; these fixtures are fixed. */
function at<T>(list: readonly T[], index: number): T {
  const value = list[index];
  if (value === undefined) throw new Error(`fixture index ${index} is missing`);
  return value;
}

describe("a mutation applies optimistically through the wire operation grammar", () => {
  test("its deltas equal a fresh build of the same change, drop refolds, and the echo confirms", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const binding = compileColumnarChangeBinding(WORKSPACE_CONTRACT, CONTRACT);
    const before = renderOverlay(engine);

    engine.applyOptimistic(
      decodeOptimisticMutation("m-1", OPTIMISTIC.map(operationBytes), binding),
    );
    expect(engine.pendingMutationIds()).toEqual(["m-1"]);
    // The optimistic state is the state a full build of the same source
    // change would hold.
    const reference = cloneState(source);
    for (const operation of OPTIMISTIC) applyToSource(reference, operation);
    const fresh = await encodeAt(reference, "cursor-1");
    expect(
      diff(renderOverlay(engine), renderWorkspace(fresh.workspace)).join("\n"),
    ).toBe("");

    // Unwind: the base was never touched, so the drop restores it exactly.
    engine.dropMutation("m-1");
    expect(engine.pendingMutationIds()).toEqual([]);
    expect(diff(renderOverlay(engine), before)).toEqual([]);

    // Resubmit, then the server's echo names the id: the delta leaves in the
    // same step the canonical rows land.
    engine.applyOptimistic(
      decodeOptimisticMutation("m-1", OPTIMISTIC.map(operationBytes), binding),
    );
    const echo = await decodeColumnarChangeBatch(
      await encodeBatch(
        "cursor-0",
        "cursor-1",
        [{ cursor: "cursor-1", operations: OPTIMISTIC }],
        { acknowledgedMutationIds: ["m-1"] },
      ),
      binding,
      text.encode("cursor-0"),
    );
    const applied = engine.applyCanonical(echo.batch);
    expect(applied.acknowledgedMutationIds).toEqual(["m-1"]);
    expect(engine.pendingMutationIds()).toEqual([]);
    expect(
      diff(renderOverlay(engine), renderWorkspace(fresh.workspace)).join("\n"),
    ).toBe("");
  });

  test("an echo naming no pending id applies as canonical change and leaves the pending delta", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const binding = compileColumnarChangeBinding(WORKSPACE_CONTRACT, CONTRACT);
    engine.applyOptimistic(
      decodeOptimisticMutation(
        "m-1",
        [operationBytes(at(OPTIMISTIC, 0))],
        binding,
      ),
    );
    const foreign = await decodeColumnarChangeBatch(
      await encodeBatch(
        "cursor-0",
        "cursor-1",
        [{ cursor: "cursor-1", operations: [at(at(TAIL, 1).operations, 4)] }],
        { acknowledgedMutationIds: ["another-device"] },
      ),
      binding,
      text.encode("cursor-0"),
    );
    const applied = engine.applyCanonical(foreign.batch);
    expect(applied.acknowledgedMutationIds).toEqual(["another-device"]);
    expect(engine.pendingMutationIds()).toEqual(["m-1"]);
    const reference = cloneState(source);
    applyToSource(reference, at(OPTIMISTIC, 0));
    applyToSource(reference, at(at(TAIL, 1).operations, 4));
    const fresh = await encodeAt(reference, "cursor-1");
    expect(
      diff(renderOverlay(engine), renderWorkspace(fresh.workspace)).join("\n"),
    ).toBe("");
  });

  test("a truncate, an unbound table, a non-canonical encoding and an empty mutation are refused", () => {
    const binding = compileColumnarChangeBinding(WORKSPACE_CONTRACT, CONTRACT);
    const truncate = toBinary(
      change.OperationSchema,
      create(change.OperationSchema, {
        tableId: "public.issues",
        kind: change.OperationKind.TRUNCATE_AUTHORIZED_TABLE,
        payload: { case: "truncate", value: create(change.TruncateSchema, {}) },
      }),
    );
    expect(() => decodeOptimisticMutation("m-1", [truncate], binding)).toThrow(
      /no optimistic form/u,
    );
    const foreign = toBinary(change.OperationSchema, {
      ...wireOperation(at(OPTIMISTIC, 0)),
      tableId: "public.nope",
    });
    expect(() => decodeOptimisticMutation("m-1", [foreign], binding)).toThrow(
      /unsubscribed table/u,
    );
    // Field 2047 as a varint: unknown to the schema, so the bytes have a
    // second identity the server would refuse.
    const canonical = operationBytes(at(OPTIMISTIC, 0));
    const padded = new Uint8Array(canonical.byteLength + 3);
    padded.set(canonical);
    padded.set([0xf8, 0x7f, 0x01], canonical.byteLength);
    expect(() => decodeOptimisticMutation("m-1", [padded], binding)).toThrow(
      /not canonical/u,
    );
    expect(() => decodeOptimisticMutation("m-1", [], binding)).toThrow(
      /at least one operation/u,
    );
  });
});

describe("a renewed plan names every later request", () => {
  test("the request in flight keeps its terms, and each batch answers its own request's plan", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const mount = liveMount(overlayOver(base.workspace), "cursor-0");
    const first = await encodeBatch("cursor-0", "cursor-1", [
      { cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] },
    ]);
    const second = await encodeBatch(
      "cursor-1",
      "cursor-2",
      [{ cursor: "cursor-2", operations: TAIL[1]?.operations ?? [] }],
      { planId: "plan-renewed" },
    );
    const requests: {
      readonly planId: string;
      readonly token: string;
      readonly from: string;
    }[] = [];
    const answers: ((response: Response) => void)[] = [];
    const failures: unknown[] = [];
    const session = liveSession(
      mount,
      (_input, init) => {
        const request = fromBinary(
          change.ChangeRequestSchema,
          init?.body as Uint8Array,
        );
        requests.push({
          planId: request.planId,
          token: utf8.decode(request.authorizationScopeToken),
          from: utf8.decode(
            request.fromCursorExclusive?.token ?? new Uint8Array(),
          ),
        });
        if (requests.length > 2) return heldOpen(init);
        return new Promise((resolve) => answers.push(resolve));
      },
      { onFailed: (error) => failures.push(error) },
    );
    session.start();
    await vi.waitFor(() => expect(requests).toHaveLength(1));
    session.renew({
      planId: "plan-renewed",
      authorizationScopeToken: "scope-renewed",
    });
    answers[0]?.(new Response(first.slice() as unknown as BodyInit));
    await vi.waitFor(() => expect(requests).toHaveLength(2));
    answers[1]?.(new Response(second.slice() as unknown as BodyInit));
    await vi.waitFor(() => expect(requests).toHaveLength(3));
    await session.stopped();
    expect(failures).toEqual([]);
    expect(mount.applied).toEqual(["cursor-1", "cursor-2"]);
    expect(requests).toEqual([
      { planId: PLAN_ID, token: "scope-token", from: "cursor-0" },
      { planId: "plan-renewed", token: "scope-renewed", from: "cursor-1" },
      { planId: "plan-renewed", token: "scope-renewed", from: "cursor-2" },
    ]);
  });
});

describe("the live poll carries the pending mutation ids", () => {
  test("a nudge restarts the in-flight poll with the current ids without stopping the loop", async () => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    const served = await encodeBatch("cursor-0", "cursor-1", [
      { cursor: "cursor-1", operations: TAIL[0]?.operations ?? [] },
    ]);
    let pending: readonly string[] = [];
    const requests: {
      readonly from: string;
      readonly pendingMutationIds: readonly string[];
      aborted: boolean;
    }[] = [];
    const session = new ColumnarLiveSession(mount, {
      endpoint: "https://change.test/v1/change",
      planId: PLAN_ID,
      authorizationScopeToken: "scope-token",
      tableIds: CONTRACT.tables.map((table) => table.tableId),
      clientInstanceId: "instance-1",
      deviceId: "device-1",
      cursor: text.encode("cursor-0"),
      maxBatchBytes: 1_048_576,
      longPollTimeoutMs: 10,
      pendingMutationIds: () => pending,
      fetch: (_input, init) =>
        new Promise((resolve, reject) => {
          const body = init?.body;
          if (!(body instanceof Uint8Array)) throw new Error("unexpected body");
          const request = fromBinary(change.ChangeRequestSchema, body);
          const from = utf8.decode(
            request.fromCursorExclusive?.token ?? new Uint8Array(),
          );
          const entry = {
            from,
            pendingMutationIds: [...request.pendingMutationIds],
            aborted: false,
          };
          requests.push(entry);
          // Only the poll that names the id has anything to say. Every other
          // poll is held open the way a real long poll is, and reports its
          // abort on a later macrotask the way a real fetch does.
          if (from === "cursor-0" && request.pendingMutationIds.length > 0) {
            resolve(new Response(served.slice() as unknown as BodyInit));
            return;
          }
          init?.signal?.addEventListener(
            "abort",
            () => {
              entry.aborted = true;
              setTimeout(
                () => reject(new DOMException("aborted", "AbortError")),
                0,
              );
            },
            { once: true },
          );
        }),
    });
    session.start();
    await vi.waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0]?.pendingMutationIds).toEqual([]);

    pending = ["m-1"];
    session.nudge();
    await vi.waitFor(() => expect(mount.applied).toEqual(["cursor-1"]));
    expect(requests[0]?.aborted).toBe(true);
    expect(
      requests.slice(0, 2).map((request) => request.pendingMutationIds),
    ).toEqual([[], ["m-1"]]);
    // The loop went on to the next poll from the published cursor.
    await vi.waitFor(() => expect(requests).toHaveLength(3));
    expect(requests[2]).toMatchObject({
      from: "cursor-1",
      pendingMutationIds: ["m-1"],
    });
    expect(session.status).toMatchObject({
      running: true,
      failure: null,
      publishedBatches: 1,
    });
    await session.stopped();
  });
});

/**
 * The seam between a decoded barrier and any client behaviour is one throw in
 * `decodeColumnarChangeBatch`. The session tests above inject the error code
 * directly; these two decode the real Rust-origin barrier vector, so they fail
 * without that throw.
 */
describe("a decoded revocation barrier refuses the batch it stands in", () => {
  const BARRIER_VECTOR = new URL(
    "../../../../spec/fixtures/protocol-v1/rust/revocation-barrier.frame.hex",
    import.meta.url,
  );

  const hex = (value: string): Uint8Array =>
    Uint8Array.from(value.match(/.{2}/gu) ?? [], (pair) =>
      Number.parseInt(pair, 16),
    );

  /** The public half of the fixed pair the fixture generators sign with. */
  const FIXTURE_BARRIER_KEY = {
    keyId: "revocation-key-1",
    publicKey: hex(
      "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8",
    ),
  } as const;

  const users = schemaOf("users");
  // The vector's only operation is a truncate of `public.tasks`, so the
  // collection behind that wire table only has to exist and be fully bound.
  const FIXTURE_CONTRACT: ColumnarChangeContract = {
    planId: "plan-1",
    generation: {
      sourceEpoch: "epoch-1",
      publicationGeneration: 2n,
      schemaGeneration: 3n,
      authorizationGeneration: 4n,
      authorizationScopeHash: new Uint8Array(32).fill(5),
    },
    tables: [
      {
        tableId: "public.tasks",
        collection: users.name,
        columns: users.columns.map((column, ordinal) => ({
          columnId: ordinal + 1,
          name: column.name,
        })),
        primaryKey: [
          users.columns.findIndex((column) => column.name === "id") + 1,
        ],
      },
    ],
    revocationBarrierKey: FIXTURE_BARRIER_KEY,
  };

  const vector = (): Uint8Array =>
    hex(readFileSync(BARRIER_VECTOR, "utf8").trim());
  const FENCE = {
    cursor: text.encode("cursor-h"),
    tableStarts: [
      { tableId: "public.tasks", snapshotCursor: text.encode("cursor-p") },
    ],
  };

  test("the whole batch is refused as AUTHORIZATION_REVOKED", async () => {
    const binding = compileColumnarChangeBinding(
      WORKSPACE_CONTRACT,
      FIXTURE_CONTRACT,
    );
    const decode = decodeColumnarChangeBatch(
      vector(),
      binding,
      text.encode("cursor-0"),
      FENCE,
    );
    // Not a rejection of any shape: the code is what `session.ts` switches on
    // to suspend, cancel and rebuild, and the message names the generations.
    await expect(decode).rejects.toMatchObject({
      code: "AUTHORIZATION_REVOKED",
      message: /generation 4 was revoked by barrier barrier-1/u,
    });
    // The batch validated whole before the refusal, so the transaction it
    // carried is dropped rather than handed back with the barrier ignored.
    await expect(decode).rejects.not.toMatchObject({
      batch: expect.anything(),
    });
  });

  test("a mount with no configured key refuses the barrier outright", async () => {
    const withoutKey: ColumnarChangeContract = {
      planId: FIXTURE_CONTRACT.planId,
      generation: FIXTURE_CONTRACT.generation,
      tables: FIXTURE_CONTRACT.tables,
    };
    const binding = compileColumnarChangeBinding(
      WORKSPACE_CONTRACT,
      withoutKey,
    );
    await expect(
      decodeColumnarChangeBatch(
        vector(),
        binding,
        text.encode("cursor-0"),
        FENCE,
      ),
    ).rejects.toMatchObject({ code: "CHANGE_STREAM_INVALID" });
  });
});

/**
 * The server sends a barrier body only to a mount that asks for one, so the
 * flag has to reach the wire and has to be false when this mount could not
 * verify a barrier anyway.
 */
describe("the live poll negotiates the revocation barrier body", () => {
  const negotiatedFlag = async (
    extra: Partial<ColumnarLiveSessionOptions>,
  ): Promise<boolean | undefined> => {
    const source = seedState();
    const base = await encodeAt(source, "cursor-0");
    const engine = overlayOver(base.workspace);
    const mount = liveMount(engine, "cursor-0");
    let seen: boolean | undefined;
    let asked!: () => void;
    const polled = new Promise<void>((resolve) => {
      asked = resolve;
    });
    const session = new ColumnarLiveSession(mount, {
      endpoint: "https://change.test/v1/change",
      planId: PLAN_ID,
      authorizationScopeToken: "scope-token",
      tableIds: CONTRACT.tables.map((table) => table.tableId),
      clientInstanceId: "instance-1",
      deviceId: "device-1",
      cursor: text.encode("cursor-0"),
      maxBatchBytes: 1_048_576,
      longPollTimeoutMs: 10,
      ...extra,
      fetch: (_input, init) => {
        const body = init?.body;
        if (!(body instanceof Uint8Array)) throw new Error("unexpected body");
        seen = fromBinary(
          change.ChangeRequestSchema,
          body,
        ).acceptRevocationBarrier;
        asked();
        return Promise.resolve(new Response(null, { status: 503 }));
      },
    });
    session.start();
    await polled;
    session.stop();
    await session.stopped();
    return seen;
  };

  test("a mount that can verify a barrier asks for one, and one that cannot does not", async () => {
    expect(await negotiatedFlag({ acceptRevocationBarrier: true })).toBe(true);
    expect(await negotiatedFlag({})).toBe(false);
  });
});
