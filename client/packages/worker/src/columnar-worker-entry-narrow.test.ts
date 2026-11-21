/**
 * The engine worker for a mount that reads some tables of an artifact that
 * serves more: the others are verified with the artifact and unreachable
 * afterwards. Driven through the `WorkerScope` like
 * `columnar-worker-entry.test.ts`, over a real artifact and real wire bytes.
 */
import { create, toBinary } from "@bufbuild/protobuf";
import {
  FIXTURE_ARTIFACT_IDENTITY,
  WORKSPACE_CONTRACT,
  artifactChunks,
  encodeWorkspace,
  hashedArtifactChunks,
} from "../../core/src/artifact.fixture.js";
import { change } from "@calque/protocol";
import { beforeAll, describe, expect, test } from "vitest";

import { changeBatch } from "./columnar-change.fixture.js";
import type { ColumnarChangeContract } from "./columnar-change.js";
import type {
  WorkerArtifactEnd,
  WorkerRequest,
  WorkerResponse,
  WorkerSuccess,
} from "./columnar-worker-protocol.js";

const READ = ["users"];

const NARROW: ColumnarChangeContract = {
  planId: "plan-columnar-worker-narrow",
  generation: {
    sourceEpoch: "fixture-source-epoch-1",
    publicationGeneration: 1n,
    schemaGeneration: 1n,
    authorizationGeneration: 1n,
    authorizationScopeHash: new Uint8Array(32).fill(0x33),
  },
  tables: WORKSPACE_CONTRACT.tables
    .filter((table) => READ.includes(table.name))
    .map((table) => ({
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
  ignoredTableIds: WORKSPACE_CONTRACT.tables
    .filter((table) => !READ.includes(table.name))
    .map((table) => `public.${table.name}`),
};

const USERS = Array.from({ length: 3 }, (_unused, index) => ({
  id: `user-${String(index)}`,
  display_name: `User ${String(index)}`,
  email: `user${String(index)}@example.test`,
  active: index % 2 === 0,
}));
const USER_FIELDS = ["id", "display_name", "email", "active"] as const;

interface Scope {
  onmessage:
    ((event: { data: WorkerRequest | WorkerArtifactEnd }) => void) | null;
  postMessage(message: WorkerResponse): void;
}

const scope = globalThis as unknown as Scope;
const answers = new Map<number, (response: WorkerResponse) => void>();
let nextId = 1;

function ask<Type extends WorkerSuccess["type"]>(
  request: Omit<Extract<WorkerRequest, { type: Type }>, "id">,
): Promise<Extract<WorkerSuccess, { type: Type }>> {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    answers.set(id, (response) => {
      if (!response.ok) {
        reject(new Error(`${response.error.name}: ${response.error.message}`));
        return;
      }
      resolve(response as Extract<WorkerSuccess, { type: Type }>);
    });
    scope.onmessage?.({ data: { ...request, id } as WorkerRequest });
  });
}

function key(id: string): Uint8Array {
  return toBinary(
    change.PrimaryKeySchema,
    create(change.PrimaryKeySchema, {
      codecVersion: 1,
      parts: [
        create(change.ColumnValueSchema, {
          columnId: 1,
          value: create(change.CellValueSchema, {
            kind: { case: "textValue", value: id },
          }),
        }),
      ],
    }),
  );
}

function deleteOf(tableId: string, id: string): change.Operation {
  return create(change.OperationSchema, {
    tableId,
    kind: change.OperationKind.DELETE_BY_KEY,
    primaryKey: key(id),
    payload: { case: "delete", value: create(change.DeleteSchema, {}) },
  });
}

describe("the engine worker opens a mount that reads some tables of the artifact", () => {
  beforeAll(async () => {
    scope.postMessage = (response: WorkerResponse) => {
      const answer = answers.get(response.id);
      answers.delete(response.id);
      answer?.(response);
    };
    await import("./columnar-worker-entry.js");
    const tables = new Map<string, Record<string, unknown>[]>(
      WORKSPACE_CONTRACT.tables.map((table) => [table.name, []]),
    );
    tables.set(
      "users",
      USERS.map((user) => ({ ...user })),
    );
    const encoded = encodeWorkspace(tables, {
      identity: { ...FIXTURE_ARTIFACT_IDENTITY, cursor: "cursor-0" },
    });
    const hasher = new MessageChannel();
    const opening = ask<"open">({
      type: "open",
      trusted: {
        byteLength: encoded.bytes.byteLength,
        digest: encoded.digest,
        chunks: artifactChunks(encoded.bytes),
        identity: { ...FIXTURE_ARTIFACT_IDENTITY, cursor: "cursor-0" },
        schema: WORKSPACE_CONTRACT,
      },
      hashers: [hasher.port2],
      changeStream: NARROW,
    });
    const chunks = hashedArtifactChunks(encoded.bytes);
    for (const chunk of chunks) hasher.port1.postMessage(chunk, [chunk.bytes]);
    scope.onmessage?.({
      data: { type: "artifact-end", chunks: chunks.length },
    });
    const opened = await opening;
    hasher.port1.close();
    expect(opened.state.live).toBe(true);
    expect(opened.tableRows).toEqual({ users: USERS.length });
    expect(opened.rowCount).toBe(USERS.length);
  });

  test("answers rows of the tables it reads and refuses the others", async () => {
    const rows = await ask<"rows">({
      type: "rows",
      table: "users",
      start: 0,
      count: USERS.length,
      fields: [...USER_FIELDS],
    });
    expect(rows.rows).toHaveLength(USERS.length);
    await expect(
      ask<"rows">({
        type: "rows",
        table: "issues",
        start: 0,
        count: 1,
        fields: ["id"],
      }),
    ).rejects.toThrow(/unknown collection issues/u);
  });

  test("applies the bound part of a batch that also carries ignored tables", async () => {
    const applied = await ask<"change">({
      type: "change",
      batches: [
        await changeBatch(
          NARROW,
          "cursor-0",
          "cursor-1",
          [
            deleteOf("public.issues", "issue-1"),
            deleteOf("public.users", "user-0"),
            deleteOf("public.comments", "comment-1"),
          ],
          [],
        ),
      ],
    });
    expect(applied.changedCollections).toEqual(["users"]);
    expect(applied.operationCount).toBe(1);
    const rows = await ask<"rows">({
      type: "rows",
      table: "users",
      start: 0,
      count: USERS.length,
      fields: ["id"],
    });
    expect(rows.rows.map((row) => String(row[0])).sort()).toEqual([
      "user-1",
      "user-2",
    ]);
  });

  test("refuses a mutation on a table it does not read", async () => {
    await expect(
      ask<"mutate">({
        type: "mutate",
        mutationId: "m-narrow-1",
        operations: [
          toBinary(
            change.OperationSchema,
            deleteOf("public.issues", "issue-1"),
          ),
        ],
      }),
    ).rejects.toThrow(/unsubscribed table/u);
  });
});
