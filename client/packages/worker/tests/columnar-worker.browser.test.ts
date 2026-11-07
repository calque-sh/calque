/**
 * The direct-columnar engine worker, run as a dedicated worker. Nothing else
 * in the workspace runs `columnar-worker-entry.ts` as a worker; the other
 * tests call the decoder directly. A `change` request answering a fenced
 * catch-up carries the fence the request was made under; the entry passes it
 * to the decoder unchanged, so the response is held to that fence, and one
 * the mount did not ask for under that fence leaves the mount where it was. A
 * mount opened without a change contract refuses every change, mutation and
 * drop.
 *
 * The artifact is minted on the Node side of the session
 * (`columnar-fixture.ts`) because the fixture encoder needs Node; the tail is
 * real wire, framed and sealed here.
 */
import { create, toBinary } from "@bufbuild/protobuf";
import type { DirectColumnarSchema, TrustedArtifact } from "@calque/core";
import {
  BatchFramesDigest,
  change,
  changeFrameBodySha256,
  encodeFrame,
  type ExpectedSourceGeneration,
} from "@calque/protocol";
import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";

import {
  ArtifactHashers,
  EngineWorkerClient,
  type ColumnarChangeContract,
  type ColumnarChangeFence,
} from "../src/columnar-worker-client.js";
import type { FixtureUser } from "./columnar-fixture.js";

const text = new TextEncoder();
const utf8 = new TextDecoder();
const PLAN_ID = "plan-columnar-worker";
const GENERATION: ExpectedSourceGeneration = {
  sourceEpoch: "fixture-source-epoch-1",
  publicationGeneration: 1n,
  schemaGeneration: 1n,
  authorizationGeneration: 1n,
  authorizationScopeHash: new Uint8Array(32).fill(0x33),
};
const USER_FIELDS = ["id", "display_name", "email", "active"] as const;

const clients: EngineWorkerClient[] = [];

afterEach(() => {
  for (const client of clients.splice(0)) client.close();
});

function bytesOf(base64: string): ArrayBuffer {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1)
    bytes[index] = binary.charCodeAt(index);
  return bytes.buffer;
}

/**
 * Wire ids assigned from the artifact schema, as `@calque/client` binds them.
 */
function contractFor(schema: DirectColumnarSchema): ColumnarChangeContract {
  return {
    planId: PLAN_ID,
    generation: GENERATION,
    tables: schema.tables.map((table) => ({
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
}

/** A catch-up fenced at `cursor`, every table starting at `tableStart`. */
function fenceAt(
  contract: ColumnarChangeContract,
  cursor: string,
  tableStart: string,
): ColumnarChangeFence {
  return {
    cursor: text.encode(cursor),
    tableStarts: contract.tables.map((table) => ({
      tableId: table.tableId,
      snapshotCursor: text.encode(tableStart),
    })),
  };
}

// ------------------------------------------------------------------ the wire

type UserOperation =
  | { readonly kind: "insert"; readonly row: FixtureUser }
  | {
      readonly kind: "update";
      readonly id: string;
      readonly changed: Readonly<Partial<FixtureUser>>;
    };

interface Batch {
  readonly from: string;
  readonly through: string;
  /** Echoes a fenced request: `batchStart.requestedFence` and the end flag. */
  readonly requestedFence?: string;
  /** The per-table start a fenced request named, echoed in `tableStarts`. */
  readonly tableStart?: string;
  readonly operations: readonly UserOperation[];
}

/** `users` carries text and bool columns only. */
function cell(value: string | boolean) {
  return create(change.CellValueSchema, {
    kind:
      typeof value === "boolean"
        ? { case: "boolValue", value }
        : { case: "textValue", value },
  });
}

function wireOperation(
  contract: ColumnarChangeContract,
  operation: UserOperation,
) {
  const users = contract.tables.find((table) => table.collection === "users");
  if (!users) throw new Error("the contract does not bind users");
  const columnId = (name: string) => {
    const column = users.columns.find((candidate) => candidate.name === name);
    if (!column) throw new Error(`users has no column ${name}`);
    return column.columnId;
  };
  // The v1 key encoding: a text key part travels as `text_value`.
  const key = (id: string) =>
    toBinary(
      change.PrimaryKeySchema,
      create(change.PrimaryKeySchema, {
        codecVersion: 1,
        parts: [
          create(change.ColumnValueSchema, {
            columnId: columnId("id"),
            value: create(change.CellValueSchema, {
              kind: { case: "textValue", value: id },
            }),
          }),
        ],
      }),
    );
  if (operation.kind === "insert")
    return create(change.OperationSchema, {
      tableId: users.tableId,
      kind: change.OperationKind.INSERT_FULL_ROW,
      primaryKey: key(operation.row.id),
      payload: {
        case: "insert",
        value: create(change.FullRowSchema, {
          columns: USER_FIELDS.map((name) =>
            create(change.ColumnValueSchema, {
              columnId: columnId(name),
              value: cell(operation.row[name]),
            }),
          ),
        }),
      },
    });
  return create(change.OperationSchema, {
    tableId: users.tableId,
    kind: change.OperationKind.UPDATE_PATCH,
    primaryKey: key(operation.id),
    payload: {
      case: "update",
      value: create(change.RowPatchSchema, {
        changedColumns: USER_FIELDS.flatMap((name) => {
          const value = operation.changed[name];
          return value === undefined
            ? []
            : [
                create(change.ColumnValueSchema, {
                  columnId: columnId(name),
                  value: cell(value),
                }),
              ];
        }),
      }),
    },
  });
}

/** One transaction at `batch.through`, framed and sealed as a server would. */
async function encodeBatch(
  contract: ColumnarChangeContract,
  batch: Batch,
): Promise<Uint8Array> {
  const cursor = (token: string) =>
    create(change.CursorSchema, { token: text.encode(token) });
  const tableIds = contract.tables.map((table) => table.tableId).sort();
  const operations = batch.operations.map((operation) =>
    wireOperation(contract, operation),
  );
  const frames = [
    create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: {
        case: "batchStart",
        value: create(change.BatchStartSchema, {
          generation: create(change.SourceGenerationSchema, GENERATION),
          fromCursorExclusive: cursor(batch.from),
          ...(batch.requestedFence === undefined
            ? {}
            : { requestedFence: cursor(batch.requestedFence) }),
          tableIds,
          serverTimeUnixMicros: 1n,
          planId: PLAN_ID,
          tableStarts: tableIds.map((tableId) => ({
            tableId,
            ...(batch.tableStart === undefined
              ? {}
              : { shardSnapshotCursor: cursor(batch.tableStart) }),
          })),
          streamId: text.encode(`stream-${batch.from}`),
        }),
      },
    }),
    create(change.ChangeStreamFrameSchema, {
      protocolVersion: 1,
      body: {
        case: "transactionFragment",
        value: create(change.TransactionFragmentSchema, {
          transactionId: text.encode(batch.through),
          transactionCursor: cursor(batch.through),
          fragmentIndex: 0,
          firstFragment: true,
          finalFragment: true,
          postgresXid: 100n,
          beginLsn: "0/10",
          commitLsn: "0/20",
          commitTimeUnixMicros: 2n,
          operations,
        }),
      },
    }),
  ];
  const digest = new BatchFramesDigest();
  for (const frame of frames) {
    frame.bodySha256 = await changeFrameBodySha256(frame);
    digest.push(frame.bodySha256);
  }
  const end = create(change.ChangeStreamFrameSchema, {
    protocolVersion: 1,
    body: {
      case: "batchEnd",
      value: create(change.BatchEndSchema, {
        throughCursorInclusive: cursor(batch.through),
        reachedRequestedFence: batch.requestedFence === batch.through,
        currentlyUpToDate: true,
        relevantTransactionCount: 1n,
        relevantOperationCount: BigInt(operations.length),
        serverTimeUnixMicros: 3n,
        frameCount: BigInt(digest.frames),
        framesSha256: await digest.finish(),
      }),
    },
  });
  end.bodySha256 = await changeFrameBodySha256(end);
  frames.push(end);
  const encoded = frames.map((frame) =>
    encodeFrame(change.ChangeStreamFrameSchema, frame),
  );
  const bytes = new Uint8Array(
    encoded.reduce((total, part) => total + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of encoded) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return bytes;
}

// ----------------------------------------------------------------- the mount

interface MountOptions {
  /** The artifact's snapshot cursor. */
  readonly cursor?: string;
  /** Whether the open carries a change contract, which makes the mount live. */
  readonly live?: boolean;
  readonly queryCapacityBytes?: number | undefined;
}

async function openMount({
  cursor = "cursor-0",
  live = true,
  queryCapacityBytes,
}: MountOptions = {}) {
  const fixture = await commands.encodeColumnarFixture(cursor);
  const contract = contractFor(fixture.schema);
  const trusted: TrustedArtifact = {
    byteLength: fixture.byteLength,
    digest: fixture.digest,
    chunks: fixture.chunks,
    identity: fixture.identity,
    schema: fixture.schema,
  };
  const client = new EngineWorkerClient(
    new Worker(new URL("../src/columnar-worker-entry.ts", import.meta.url), {
      type: "module",
    }),
  );
  clients.push(client);
  // One hasher, as a page on a four-thread machine spawns: the artifact is
  // one chunk.
  const hashers = new ArtifactHashers(
    () =>
      new Worker(new URL("../src/columnar-hasher-entry.ts", import.meta.url), {
        type: "module",
        name: "calque-hasher",
      }),
    1,
  );
  try {
    const receiving = client.receive(
      trusted,
      hashers,
      undefined,
      queryCapacityBytes,
      undefined,
      live ? contract : undefined,
    );
    await hashers.send(0, bytesOf(fixture.bytes));
    receiving.end(1);
    const opened = await Promise.race([receiving.opened, hashers.failed]);
    return { fixture, contract, client, opened };
  } finally {
    hashers.close();
  }
}

const rowOf = (user: FixtureUser): readonly unknown[] =>
  USER_FIELDS.map((name) => user[name]);

/** Rows keyed by id: physical order is not part of the mount's contract. */
function byId(
  rows: readonly (readonly unknown[])[],
): Record<string, readonly unknown[]> {
  return Object.fromEntries(rows.map(([id, ...rest]) => [String(id), rest]));
}

describe("dedicated direct-columnar worker", () => {
  it("applies a fenced catch-up held to the fence the request named", async () => {
    const { fixture, contract, client, opened } = await openMount();
    expect(opened.state.live).toBe(true);
    expect(utf8.decode(opened.state.cursor)).toBe("cursor-0");
    expect(opened.tableRows.users).toBe(fixture.users.length);

    const target = fixture.users[1];
    if (!target) throw new Error("the fixture has fewer than two users");
    const renamed: FixtureUser = {
      ...target,
      display_name: "Renamed",
      active: !target.active,
    };
    const inserted: FixtureUser = {
      id: "user-9",
      display_name: "User 9",
      email: "user9@example.test",
      active: false,
    };
    const bytes = await encodeBatch(contract, {
      from: "cursor-0",
      through: "cursor-1",
      requestedFence: "cursor-1",
      tableStart: "cursor-0",
      operations: [
        {
          kind: "update",
          id: renamed.id,
          changed: {
            display_name: renamed.display_name,
            active: renamed.active,
          },
        },
        { kind: "insert", row: inserted },
      ],
    });

    const applied = await client.applyChangeBatches([bytes], {
      fence: fenceAt(contract, "cursor-1", "cursor-0"),
    });
    expect(utf8.decode(applied.state.cursor)).toBe("cursor-1");
    expect(applied.changedCollections).toEqual(["users"]);
    expect(applied.transactionCount).toBe(1);
    expect(applied.operationCount).toBe(2);
    expect(applied.currentlyUpToDate).toBe(true);
    expect(applied.state.collectionRows.users).toBe(fixture.users.length + 1);

    const after = await client.rows("users", 0, fixture.users.length + 1, [
      ...USER_FIELDS,
    ]);
    expect(byId(after.rows)).toEqual(
      byId(
        [
          ...fixture.users.map((user) =>
            user.id === renamed.id ? renamed : user,
          ),
          inserted,
        ].map(rowOf),
      ),
    );
    expect(utf8.decode((await client.state()).cursor)).toBe("cursor-1");
  });

  it("refuses a fenced response under any other terms, and the mount does not move", async () => {
    const { fixture, contract, client } = await openMount();
    const before = await client.rows("users", 0, fixture.users.length, [
      ...USER_FIELDS,
    ]);
    const bytes = await encodeBatch(contract, {
      from: "cursor-0",
      through: "cursor-1",
      requestedFence: "cursor-1",
      tableStart: "cursor-0",
      operations: [
        { kind: "update", id: "user-0", changed: { display_name: "Moved" } },
      ],
    });

    // A live poll names no fence, so a response that echoes one is not the
    // answer to it.
    const unfenced = await client
      .applyChangeBatches([bytes])
      .catch((error: unknown) => error);
    expect(unfenced).toMatchObject({ code: "CHANGE_STREAM_INVALID" });
    expect(String(unfenced)).toMatch(/requestedFence/u);
    // Nor is it the answer to a request fenced somewhere else.
    await expect(
      client.applyChangeBatches([bytes], {
        fence: fenceAt(contract, "cursor-2", "cursor-0"),
      }),
    ).rejects.toMatchObject({ code: "CHANGE_STREAM_INVALID" });

    const state = await client.state();
    expect(utf8.decode(state.cursor)).toBe("cursor-0");
    expect(state.collectionRows.users).toBe(fixture.users.length);
    expect(
      await client.rows("users", 0, fixture.users.length, [...USER_FIELDS]),
    ).toEqual(before);
  });

  it("refuses to open an artifact that names no snapshot cursor", async () => {
    // A live mount starts its first catch-up from the artifact's cursor, so a
    // mount without one would have nowhere to resume from.
    await expect(openMount({ cursor: "" })).rejects.toThrow(
      "invalid direct-columnar artifact identity",
    );
  });

  it.each([
    { mount: "frozen", queryCapacityBytes: undefined },
    { mount: "queryable but not live", queryCapacityBytes: 1 << 20 },
  ])(
    "refuses a change, a mutation and a drop on a $mount mount",
    async ({ queryCapacityBytes }) => {
      const { fixture, contract, client } = await openMount({
        live: false,
        queryCapacityBytes,
      });
      const bytes = await encodeBatch(contract, {
        from: "cursor-0",
        through: "cursor-1",
        operations: [
          { kind: "update", id: "user-0", changed: { display_name: "Moved" } },
        ],
      });
      const refusal = {
        code: "INVALID_LIFECYCLE",
        message: "this mount was opened without a live change stream",
      };
      await expect(client.applyChangeBatches([bytes])).rejects.toMatchObject(
        refusal,
      );
      await expect(client.mutate("m-1", [bytes])).rejects.toMatchObject(
        refusal,
      );
      await expect(client.drop("m-1")).rejects.toMatchObject(refusal);
      expect(
        byId(
          (
            await client.rows("users", 0, fixture.users.length, [
              ...USER_FIELDS,
            ])
          ).rows,
        ),
      ).toEqual(byId(fixture.users.map(rowOf)));
    },
  );
});
