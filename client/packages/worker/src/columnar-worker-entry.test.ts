/**
 * The engine worker's request routing for `mutate`, `drop` and `rows`: the
 * worker a mount spawns turns the protocol requests into overlay calls and
 * answers with the mount state the session republishes on. The module under
 * test is `columnar-worker-entry.ts`, driven through the `WorkerScope` a
 * `DedicatedWorker` gives it, over a real artifact and real wire bytes.
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

const utf8 = new TextDecoder();

const GENERATION = {
  sourceEpoch: "fixture-source-epoch-1",
  publicationGeneration: 1n,
  schemaGeneration: 1n,
  authorizationGeneration: 1n,
  authorizationScopeHash: new Uint8Array(32).fill(0x33),
} as const;

const CONTRACT: ColumnarChangeContract = {
  planId: "plan-columnar-worker-entry",
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

const USERS = Array.from({ length: 3 }, (_unused, index) => ({
  id: `user-${String(index)}`,
  display_name: `User ${String(index)}`,
  email: `user${String(index)}@example.test`,
  active: index % 2 === 0,
}));
const USER_FIELDS = ["id", "display_name", "email", "active"] as const;
const MUTATED_NAME = "User 1, optimistically";

/**
 * The worker under test, addressed the way a `DedicatedWorker` addresses it.
 */
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

function userRow(id: string): Promise<readonly unknown[] | undefined> {
  return ask<"rows">({
    type: "rows",
    table: "users",
    start: 0,
    count: USERS.length,
    fields: [...USER_FIELDS],
  }).then((response) => response.rows.find((row) => String(row[0]) === id));
}

/** One `UPDATE users SET display_name = …` as the change stream spells it. */
function renameOperation(id: string, name: string): change.Operation {
  const table = WORKSPACE_CONTRACT.tables.find(
    (candidate) => candidate.name === "users",
  );
  if (!table) throw new Error("the fixture has no users table");
  const keyOrdinal =
    table.columns.findIndex((column) => column.name === "id") + 1;
  const nameOrdinal =
    table.columns.findIndex((column) => column.name === "display_name") + 1;
  return create(change.OperationSchema, {
    tableId: "public.users",
    kind: change.OperationKind.UPDATE_PATCH,
    primaryKey: toBinary(
      change.PrimaryKeySchema,
      create(change.PrimaryKeySchema, {
        codecVersion: 1,
        parts: [
          // A text key part is a text cell.
          create(change.ColumnValueSchema, {
            columnId: keyOrdinal,
            value: create(change.CellValueSchema, {
              kind: { case: "textValue", value: id },
            }),
          }),
        ],
      }),
    ),
    payload: {
      case: "update",
      value: create(change.RowPatchSchema, {
        changedColumns: [
          create(change.ColumnValueSchema, {
            columnId: nameOrdinal,
            value: create(change.CellValueSchema, {
              kind: { case: "textValue", value: name },
            }),
          }),
        ],
      }),
    },
  });
}

/** One `DELETE FROM users WHERE id = …` as the change stream spells it. */
function deleteOperation(id: string): change.Operation {
  const rename = renameOperation(id, "unused");
  return create(change.OperationSchema, {
    tableId: rename.tableId,
    kind: change.OperationKind.DELETE_BY_KEY,
    primaryKey: rename.primaryKey,
    payload: { case: "delete", value: create(change.DeleteSchema, {}) },
  });
}

/** The `mutate` request's bytes: one canonical encoded `Operation`. */
function operationBytes(operation: change.Operation): Uint8Array {
  return toBinary(change.OperationSchema, operation);
}

describe("the engine worker routes mutate and drop through the mount", () => {
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
    // One hasher, as the page's would, passing the artifact's one chunk on.
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
      changeStream: CONTRACT,
    });
    const chunks = hashedArtifactChunks(encoded.bytes);
    for (const chunk of chunks) hasher.port1.postMessage(chunk, [chunk.bytes]);
    scope.onmessage?.({
      data: { type: "artifact-end", chunks: chunks.length },
    });
    const opened = await opening;
    hasher.port1.close();
    expect(opened.state.live).toBe(true);
    expect(opened.tableRows.users).toBe(USERS.length);
  });

  test("mutate applies the delta, drop removes it, and the echo supersedes it", async () => {
    const operation = renameOperation("user-1", MUTATED_NAME);
    const before = await userRow("user-1");
    expect(before?.[1]).toBe("User 1");

    const mutated = await ask<"mutate">({
      type: "mutate",
      mutationId: "m-entry-1",
      operations: [operationBytes(operation)],
    });
    expect(mutated.state.pendingMutationIds).toEqual(["m-entry-1"]);
    // The mount stays where it is: an optimistic delta is not a cursor move.
    expect(utf8.decode(mutated.state.cursor ?? new Uint8Array())).toBe(
      "cursor-0",
    );
    expect((await userRow("user-1"))?.[1]).toBe(MUTATED_NAME);

    const dropped = await ask<"drop">({
      type: "drop",
      mutationId: "m-entry-1",
    });
    expect(dropped.state.pendingMutationIds).toEqual([]);
    expect((await userRow("user-1"))?.[1]).toBe("User 1");

    // Resubmitted, then echoed: the canonical values land in the same step
    // that retires the pending delta.
    const resubmitted = await ask<"mutate">({
      type: "mutate",
      mutationId: "m-entry-1",
      operations: [operationBytes(operation)],
    });
    expect(resubmitted.state.pendingMutationIds).toEqual(["m-entry-1"]);
    const applied = await ask<"change">({
      type: "change",
      batches: [
        await changeBatch(
          CONTRACT,
          "cursor-0",
          "cursor-1",
          [operation],
          ["m-entry-1"],
        ),
      ],
    });
    expect(applied.acknowledgedMutationIds).toEqual(["m-entry-1"]);
    expect(applied.state.pendingMutationIds).toEqual([]);
    expect(utf8.decode(applied.state.cursor ?? new Uint8Array())).toBe(
      "cursor-1",
    );
    expect((await userRow("user-1"))?.[1]).toBe(MUTATED_NAME);
  });

  test("a window bounded by a count a delete has since shortened reads to the table's end", async () => {
    const window = { type: "rows", table: "users", fields: ["id"] } as const;
    const deleted = await ask<"mutate">({
      type: "mutate",
      mutationId: "m-entry-delete",
      operations: [operationBytes(deleteOperation("user-0"))],
    });
    expect(deleted.state.pendingMutationIds).toEqual(["m-entry-delete"]);
    try {
      const whole = await ask<"rows">({
        ...window,
        start: 0,
        count: USERS.length,
      });
      expect(whole.rows.map((row) => String(row[0])).sort()).toEqual(
        USERS.slice(1).map((user) => user.id),
      );
      const past = await ask<"rows">({
        ...window,
        start: USERS.length - 1,
        count: 10,
      });
      expect(past.rows).toEqual([]);
      await expect(
        ask<"rows">({ ...window, start: -1, count: 1 }),
      ).rejects.toThrow("invalid materialized row window");
    } finally {
      await ask<"drop">({ type: "drop", mutationId: "m-entry-delete" });
    }
    const restored = await ask<"rows">({
      ...window,
      start: 0,
      count: USERS.length,
    });
    expect(restored.rows).toHaveLength(USERS.length);
  });

  test("dropping an unknown mutation is refused, and the mount does not move", async () => {
    await expect(
      ask<"drop">({ type: "drop", mutationId: "m-never-applied" }),
    ).rejects.toThrow();
    const state = await ask<"state">({ type: "state" });
    expect(state.state.pendingMutationIds).toEqual([]);
    expect(utf8.decode(state.state.cursor ?? new Uint8Array())).toBe(
      "cursor-1",
    );
  });

  test("batches a stream carried apply as one publication, or not at all", async () => {
    const first = await changeBatch(
      CONTRACT,
      "cursor-1",
      "cursor-2",
      [renameOperation("user-0", "User 0, streamed")],
      [],
    );
    // The second batch starts after a cursor the first never reached.
    const gapped = await changeBatch(
      CONTRACT,
      "cursor-3",
      "cursor-4",
      [renameOperation("user-2", "User 2, streamed")],
      [],
    );
    await expect(
      ask<"change">({ type: "change", batches: [first, gapped] }),
    ).rejects.toThrow(/batchStart\.fromCursorExclusive/u);
    const refused = await ask<"state">({ type: "state" });
    expect(utf8.decode(refused.state.cursor ?? new Uint8Array())).toBe(
      "cursor-1",
    );
    expect((await userRow("user-0"))?.[1]).toBe("User 0");

    const chained = await changeBatch(
      CONTRACT,
      "cursor-2",
      "cursor-3",
      [renameOperation("user-2", "User 2, streamed")],
      [],
    );
    const applied = await ask<"change">({
      type: "change",
      batches: [first, chained],
    });
    expect(utf8.decode(applied.state.cursor ?? new Uint8Array())).toBe(
      "cursor-3",
    );
    expect(applied.transactionCount).toBe(2);
    expect(applied.operationCount).toBe(2);
    expect(applied.changedCollections).toEqual(["users"]);
    expect((await userRow("user-0"))?.[1]).toBe("User 0, streamed");
    expect((await userRow("user-2"))?.[1]).toBe("User 2, streamed");
  });
});
