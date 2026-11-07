/**
 * The engine worker runs queries in slices beside its live requests. Driven
 * through the `WorkerScope` a `DedicatedWorker` gives
 * `columnar-worker-entry.ts`, over a real artifact and real wire bytes: an
 * optimistic write sent while a sort over every row is running is answered
 * before the sort is, and the sort's answer is computed at the state the
 * write left, never at a mixture. The echo of a write no frame has answered
 * since waits for the frame running, so the page shows the write before it
 * shows it confirmed.
 */
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  ColumnarQueryEngine,
  OverlayEngine,
  artifactBase,
  openDirectColumnarArtifact,
  type QuerySpecification,
} from "@calque/core";
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
  WorkerCancel,
  WorkerRequest,
  WorkerResponse,
  WorkerSuccess,
} from "./columnar-worker-protocol.js";

const CONTRACT: ColumnarChangeContract = {
  planId: "plan-columnar-worker-queries",
  generation: {
    sourceEpoch: "fixture-source-epoch-1",
    publicationGeneration: 1n,
    schemaGeneration: 1n,
    authorizationGeneration: 1n,
    authorizationScopeHash: new Uint8Array(32).fill(0x33),
  },
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

/** Enough rows that a text sort over all of them takes many slices. */
const USER_COUNT = 150_000;
const USERS = Array.from({ length: USER_COUNT }, (_unused, index) => ({
  id: `user-${String(index)}`,
  display_name: `User ${String((index * 7_919) % USER_COUNT)}`,
  email: `user${String(index)}@example.test`,
  active: index % 2 === 0,
}));

const BY_NAME: QuerySpecification = {
  version: 1,
  from: { collection: "users", as: "user" },
  select: [
    { kind: "field", field: { source: "user", column: "id" }, as: "id" },
    {
      kind: "field",
      field: { source: "user", column: "display_name" },
      as: "display_name",
    },
  ],
  orderBy: [{ by: "display_name", direction: "asc" }],
};
const COUNT_USERS: QuerySpecification = {
  version: 1,
  from: { collection: "users", as: "user" },
  select: [{ kind: "aggregate", operator: "count", as: "count" }],
};
/** A query far shorter than one slice. */
const PROJECT_NAMES: QuerySpecification = {
  version: 1,
  from: { collection: "projects", as: "project" },
  select: [
    { kind: "field", field: { source: "project", column: "name" }, as: "name" },
  ],
  orderBy: [{ by: "name", direction: "asc" }],
};

interface Scope {
  onmessage:
    | ((event: {
        data: WorkerRequest | WorkerCancel | WorkerArtifactEnd;
      }) => void)
    | null;
  postMessage(message: WorkerResponse): void;
}

const scope = globalThis as unknown as Scope;
const answers = new Map<number, (response: WorkerResponse) => void>();
/** Request ids in the order their answers arrived. */
const answered: number[] = [];
let nextId = 1;

function ask<Type extends WorkerSuccess["type"]>(
  request: Omit<Extract<WorkerRequest, { type: Type }>, "id">,
): { id: number; answer: Promise<Extract<WorkerSuccess, { type: Type }>> } {
  const id = nextId++;
  const answer = new Promise<Extract<WorkerSuccess, { type: Type }>>(
    (resolve, reject) => {
      answers.set(id, (response) => {
        if (!response.ok) {
          reject(
            new Error(`${response.error.name}: ${response.error.message}`),
          );
          return;
        }
        resolve(response as Extract<WorkerSuccess, { type: Type }>);
      });
    },
  );
  scope.onmessage?.({ data: { ...request, id } as WorkerRequest });
  return { id, answer };
}

const macrotask = () => new Promise((resolve) => setTimeout(resolve, 0));

const utf8 = new TextDecoder();

/** A change from the mount's cursor carrying `operations`. */
async function nextBatch(
  operations: readonly Uint8Array[],
  mutationIds: readonly string[],
): Promise<Uint8Array> {
  const { state } = await ask<"state">({ type: "state" }).answer;
  const from = utf8.decode(state.cursor ?? new Uint8Array());
  return changeBatch(
    CONTRACT,
    from,
    `${from}+`,
    operations.map((bytes) => fromBinary(change.OperationSchema, bytes)),
    mutationIds,
  );
}

const sendChange = (batch: Uint8Array) =>
  ask<"change">({ type: "change", batches: [batch] });

/** A frame whose sort over every user runs for many slices. */
const sortingFrame = () =>
  ask<"frame">({
    type: "frame",
    members: [
      { type: "query", specification: PROJECT_NAMES },
      {
        type: "window",
        specification: BY_NAME,
        window: { offset: 0, count: 1 },
      },
    ],
  });

/** `UPDATE <table> SET <column> = …` as the change stream spells it. */
function renameOperation(
  id: string,
  name: string,
  table = "users",
  column = "display_name",
): Uint8Array {
  const columns = WORKSPACE_CONTRACT.tables.find(
    (candidate) => candidate.name === table,
  )?.columns;
  if (!columns) throw new Error(`the fixture has no ${table} table`);
  const ordinal = (name: string) =>
    columns.findIndex((column) => column.name === name) + 1;
  const text = (columnId: number, value: string) =>
    create(change.ColumnValueSchema, {
      columnId,
      value: create(change.CellValueSchema, {
        kind: { case: "textValue", value },
      }),
    });
  return toBinary(
    change.OperationSchema,
    create(change.OperationSchema, {
      tableId: `public.${table}`,
      kind: change.OperationKind.UPDATE_PATCH,
      primaryKey: toBinary(
        change.PrimaryKeySchema,
        create(change.PrimaryKeySchema, {
          codecVersion: 1,
          parts: [text(ordinal("id"), id)],
        }),
      ),
      payload: {
        case: "update",
        value: create(change.RowPatchSchema, {
          changedColumns: [text(ordinal(column), name)],
        }),
      },
    }),
  );
}

let poolBytes = 0;

describe("the engine worker runs queries beside its live requests", () => {
  beforeAll(async () => {
    scope.postMessage = (response: WorkerResponse) => {
      answered.push(response.id);
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
    tables.set(
      "projects",
      ["Alpha", "Beta", "Gamma"].map((name, index) => ({
        id: `project-${String(index)}`,
        organization_id: "organization-0",
        slug: name.toLowerCase(),
        name,
      })),
    );
    const identity = { ...FIXTURE_ARTIFACT_IDENTITY, cursor: "cursor-0" };
    const encoded = encodeWorkspace(tables, { identity });
    const trusted = {
      byteLength: encoded.bytes.byteLength,
      digest: encoded.digest,
      chunks: artifactChunks(encoded.bytes),
      identity,
      schema: WORKSPACE_CONTRACT,
    };
    // What a sort and a count over every user reserve, measured on a mount
    // of its own. The worker's pool fits a sort and a count at once, and not
    // two sorts.
    const workspace = await openDirectColumnarArtifact(
      encoded.bytes.slice().buffer,
      trusted,
    );
    const sizing = new ColumnarQueryEngine(
      new OverlayEngine(
        WORKSPACE_CONTRACT.tables.map((schema) => {
          const columns = schema.columns.map((column) => column.name);
          return {
            name: schema.name,
            columns,
            base: artifactBase(workspace.table(schema.name), columns),
            primaryKey: schema.primaryKey,
          };
        }),
      ),
      { capacityBytes: 1 << 30 },
    );
    const sort = sizing.estimateReservationBytes(BY_NAME, { maxRows: 5 });
    const count = sizing.estimateReservationBytes(COUNT_USERS);
    poolBytes = sort + count + Math.floor((sort - count) / 2);
    expect(poolBytes).toBeLessThan(2 * sort);
    // One hasher, as the page's would, passing the artifact's one chunk on.
    const hasher = new MessageChannel();
    const opening = ask<"open">({
      type: "open",
      trusted,
      hashers: [hasher.port2],
      queryCapacityBytes: poolBytes,
      changeStream: CONTRACT,
    }).answer;
    const chunks = hashedArtifactChunks(encoded.bytes);
    for (const chunk of chunks) hasher.port1.postMessage(chunk, [chunk.bytes]);
    scope.onmessage?.({
      data: { type: "artifact-end", chunks: chunks.length },
    });
    const opened = await opening;
    hasher.port1.close();
    expect(opened.tableRows.users).toBe(USER_COUNT);
  }, 60_000);

  test("a write sent during a long sort is answered first, and the sort's result includes it", async () => {
    const sort = ask<"window">({
      type: "window",
      specification: BY_NAME,
      window: { offset: 0, count: 5 },
    });
    // Let the sort start on its first slices before the write arrives.
    await macrotask();
    await macrotask();
    expect(answered).not.toContain(sort.id);
    const write = ask<"mutate">({
      type: "mutate",
      mutationId: "m-during-sort",
      operations: [renameOperation("user-1", "A first")],
    });
    const written = await write.answer;
    expect(answered).not.toContain(sort.id);
    const sorted = await sort.answer;
    expect(answered.indexOf(write.id)).toBeLessThan(answered.indexOf(sort.id));
    // The sort that began before the write restarted at the state it left.
    expect(sorted.result.rows[0]).toEqual(["user-1", "A first"]);
    expect(sorted.result.total).toBe(USER_COUNT);
    expect(sorted.generations).toEqual({
      users: written.state.collectionGenerations.users,
    });
    await ask<"drop">({ type: "drop", mutationId: "m-during-sort" }).answer;
  }, 60_000);

  test("a short query admitted behind a long one is answered first", async () => {
    const sort = ask<"window">({
      type: "window",
      specification: {
        ...BY_NAME,
        orderBy: [{ by: "display_name", direction: "desc" }],
      },
      window: { offset: 10, count: 5 },
    });
    await macrotask();
    const count = ask<"query">({ type: "query", specification: COUNT_USERS });
    expect((await count.answer).result.rows).toEqual([[USER_COUNT]]);
    expect(answered).not.toContain(sort.id);
    expect((await sort.answer).result.rows).toHaveLength(5);
  }, 60_000);

  test("a query that fits in one slice is answered at the state it was admitted to, before the write sent after it", async () => {
    const query = ask<"query">({ type: "query", specification: PROJECT_NAMES });
    const write = ask<"mutate">({
      type: "mutate",
      mutationId: "m-after-query",
      operations: [
        renameOperation("project-2", "Aardvark", "projects", "name"),
      ],
    });
    const [read, written] = await Promise.all([query.answer, write.answer]);
    expect(answered.indexOf(query.id)).toBeLessThan(answered.indexOf(write.id));
    expect(read.result.rows).toEqual([["Alpha"], ["Beta"], ["Gamma"]]);
    expect(read.generations.projects).toBeLessThan(
      written.state.collectionGenerations.projects ?? 0,
    );
    const after = await ask<"query">({
      type: "query",
      specification: PROJECT_NAMES,
    }).answer;
    expect(after.result.rows).toEqual([["Aardvark"], ["Alpha"], ["Beta"]]);
    await ask<"drop">({ type: "drop", mutationId: "m-after-query" }).answer;
  });

  test("a write sent behind a burst of queries waits for their first slices, not for the sort among them", async () => {
    const sort = ask<"window">({
      type: "window",
      specification: {
        version: 1,
        from: { collection: "users", as: "user" },
        select: [
          { kind: "field", field: { source: "user", column: "id" }, as: "id" },
          {
            kind: "field",
            field: { source: "user", column: "email" },
            as: "email",
          },
        ],
        orderBy: [{ by: "email", direction: "desc" }],
      },
      window: { offset: 0, count: 1 },
    });
    const names = [1, 2].map(() =>
      ask<"query">({ type: "query", specification: PROJECT_NAMES }),
    );
    const write = ask<"mutate">({
      type: "mutate",
      mutationId: "m-after-burst",
      operations: [
        renameOperation("user-1", "zz@example.test", "users", "email"),
      ],
    });
    const written = await write.answer;
    expect(answered).not.toContain(sort.id);
    for (const name of names) {
      expect(answered.indexOf(name.id)).toBeLessThan(
        answered.indexOf(write.id),
      );
      expect((await name.answer).result.rows).toEqual([
        ["Alpha"],
        ["Beta"],
        ["Gamma"],
      ]);
    }
    const sorted = await sort.answer;
    expect(sorted.result.rows).toEqual([["user-1", "zz@example.test"]]);
    expect(sorted.generations).toEqual({
      users: written.state.collectionGenerations.users,
    });
    await ask<"drop">({ type: "drop", mutationId: "m-after-burst" }).answer;
  }, 60_000);

  test("a sort over a join restarts when the joined collection moves between its slices", async () => {
    const sort = ask<"window">({
      type: "window",
      specification: {
        ...BY_NAME,
        joins: [
          {
            kind: "left",
            collection: "projects",
            as: "project",
            on: {
              left: { source: "user", column: "display_name" },
              right: { source: "project", column: "name" },
            },
          },
        ],
        select: [
          ...BY_NAME.select,
          {
            kind: "field",
            field: { source: "project", column: "id" },
            as: "project",
          },
        ],
      },
      window: { offset: 0, count: 1 },
    });
    await macrotask();
    expect(answered).not.toContain(sort.id);
    // Only the joined collection moves: the sorted one stays where it was.
    const write = ask<"mutate">({
      type: "mutate",
      mutationId: "m-joined",
      operations: [renameOperation("project-0", "User 0", "projects", "name")],
    });
    const written = await write.answer;
    const sorted = await sort.answer;
    expect(answered.indexOf(write.id)).toBeLessThan(answered.indexOf(sort.id));
    expect(sorted.result.rows).toEqual([["user-0", "User 0", "project-0"]]);
    expect(sorted.generations).toEqual({
      users: written.state.collectionGenerations.users,
      projects: written.state.collectionGenerations.projects,
    });
    await ask<"drop">({ type: "drop", mutationId: "m-joined" }).answer;
  }, 60_000);

  test("a cancelled query is never answered and gives back its share of the pool", async () => {
    const specification: QuerySpecification = {
      ...BY_NAME,
      orderBy: [{ by: "display_name", direction: "desc", nulls: "first" }],
    };
    const first = ask<"window">({
      type: "window",
      specification,
      window: { offset: 0, count: 5 },
    });
    await macrotask();
    scope.onmessage?.({ data: { type: "cancel", target: first.id } });
    // Had the first kept its reservation, the pool could not hold this one.
    const second = ask<"window">({
      type: "window",
      specification: {
        ...specification,
        orderBy: [{ by: "display_name", direction: "desc", nulls: "last" }],
      },
      window: { offset: 0, count: 5 },
    });
    expect((await second.answer).result.total).toBe(USER_COUNT);
    await macrotask();
    expect(answered).not.toContain(first.id);
  }, 60_000);

  test("a cancel for a query still waiting behind live requests withdraws it", async () => {
    const waiting = ask<"query">({ type: "query", specification: COUNT_USERS });
    scope.onmessage?.({ data: { type: "cancel", target: waiting.id } });
    const state = await ask<"state">({ type: "state" }).answer;
    expect(state.state.live).toBe(true);
    await macrotask();
    await macrotask();
    expect(answered).not.toContain(waiting.id);
  });

  test("a query whose admission throws fails alone, and the requests behind it are still answered", async () => {
    const deliver = scope.postMessage;
    const query = ask<"query">({ type: "query", specification: PROJECT_NAMES });
    // Its answer and then the failure its slice reports are both refused, so
    // the throw leaves the admission itself.
    let refused = 0;
    scope.postMessage = (response) => {
      if (response.id === query.id && refused < 2) {
        refused += 1;
        throw new Error("the answer could not be cloned");
      }
      deliver(response);
    };
    const state = ask<"state">({ type: "state" });
    const stalled = new Promise<"stalled">((resolve) =>
      setTimeout(() => resolve("stalled"), 2_000),
    );
    try {
      expect(
        await Promise.race([
          query.answer.then(
            () => "answered",
            (error: unknown) => String(error),
          ),
          stalled,
        ]),
      ).toBe("Error: Error: the answer could not be cloned");
      expect(
        await Promise.race([
          state.answer.then(({ state }) => state.live),
          stalled,
        ]),
      ).toBe(true);
    } finally {
      scope.postMessage = deliver;
    }
    expect(refused).toBe(2);
  });

  test("a frame answers every member at one state, reading again one a write moved after it finished", async () => {
    const frame = ask<"frame">({
      type: "frame",
      members: [
        { type: "query", specification: PROJECT_NAMES },
        {
          type: "window",
          specification: BY_NAME,
          window: { offset: 0, count: 1 },
        },
      ],
    });
    await macrotask();
    expect(answered).not.toContain(frame.id);
    // The project names finished in the frame's first slice; the sort is
    // still running when the write moves them.
    const write = ask<"mutate">({
      type: "mutate",
      mutationId: "m-during-frame",
      operations: [
        renameOperation("project-2", "Aardvark", "projects", "name"),
      ],
    });
    const written = await write.answer;
    const { results, generations } = await frame.answer;
    expect(answered.indexOf(write.id)).toBeLessThan(answered.indexOf(frame.id));
    expect(results).toEqual([
      {
        ok: true,
        result: {
          columns: ["name"],
          rows: [["Aardvark"], ["Alpha"], ["Beta"]],
        },
      },
      {
        ok: true,
        result: expect.objectContaining({ total: USER_COUNT }) as unknown,
      },
    ]);
    expect(generations).toEqual(written.state.collectionGenerations);
    await ask<"drop">({ type: "drop", mutationId: "m-during-frame" }).answer;
  }, 60_000);

  test("a frame of two sorts answers under a write between every slice", async () => {
    const frame = ask<"frame">({
      type: "frame",
      members: [
        {
          type: "window",
          specification: BY_NAME,
          window: { offset: 0, count: 1 },
        },
        {
          type: "window",
          specification: {
            ...BY_NAME,
            orderBy: [{ by: "display_name", direction: "desc" }],
          },
          window: { offset: 0, count: 1 },
        },
      ],
    });
    // Each write moves both sorts' collection, so without a bound one sort
    // would finish only for the other's write to send it back.
    const writes: string[] = [];
    let last: Awaited<ReturnType<typeof ask<"mutate">>["answer"]> | undefined;
    while (!answered.includes(frame.id) && writes.length < 200) {
      const mutationId = `m-steady-${String(writes.length)}`;
      writes.push(mutationId);
      last = await ask<"mutate">({
        type: "mutate",
        mutationId,
        operations: [renameOperation("user-1", `W${String(writes.length)}`)],
      }).answer;
      await macrotask();
    }
    const { results, generations } = await frame.answer;
    expect(writes.length).toBeLessThan(200);
    expect(generations.users).toBe(last?.state.collectionGenerations.users);
    expect(results).toEqual([
      {
        ok: true,
        result: expect.objectContaining({ total: USER_COUNT }) as unknown,
      },
      {
        ok: true,
        result: expect.objectContaining({ total: USER_COUNT }) as unknown,
      },
    ]);
    for (const mutationId of writes)
      await ask<"drop">({ type: "drop", mutationId }).answer;
  }, 120_000);

  test("a frame reads a member sent with `seen` only once a collection it names has moved past it", async () => {
    const { state } = await ask<"state">({ type: "state" }).answer;
    const projects = state.collectionGenerations.projects ?? -1;
    const held = await ask<"frame">({
      type: "frame",
      members: [
        {
          type: "query",
          specification: PROJECT_NAMES,
          seen: { projects },
        },
        {
          type: "query",
          specification: PROJECT_NAMES,
          seen: { projects: projects - 1 },
        },
      ],
    }).answer;
    expect(held.results).toEqual([
      null,
      {
        ok: true,
        result: { columns: ["name"], rows: [["Alpha"], ["Beta"], ["Gamma"]] },
      },
    ]);
    expect(held.generations).toEqual(state.collectionGenerations);
  });

  test("a frame member that fails fails alone", async () => {
    const { results } = await ask<"frame">({
      type: "frame",
      members: [
        {
          type: "query",
          specification: {
            ...PROJECT_NAMES,
            orderBy: [{ by: "missing", direction: "asc" }],
          },
        },
        { type: "query", specification: COUNT_USERS },
      ],
    }).answer;
    expect(results).toEqual([
      {
        ok: false,
        error: expect.objectContaining({ code: "INVALID_QUERY" }) as unknown,
      },
      { ok: true, result: { columns: ["count"], rows: [[USER_COUNT]] } },
    ]);
  });

  test("the echo of a write no frame has answered waits for the frame running, which answers at the optimistic state", async () => {
    // Moved first, so the frame's sort cannot be answered from cache.
    await sendChange(
      await nextBatch([renameOperation("user-3", "User 3 again")], []),
    ).answer;
    const rename = renameOperation("user-1", "A first");
    const echoed = await nextBatch([rename], ["m-echoed"]);
    const frame = sortingFrame();
    await macrotask();
    const written = await ask<"mutate">({
      type: "mutate",
      mutationId: "m-echoed",
      operations: [rename],
    }).answer;
    const echo = sendChange(echoed);
    await macrotask();
    expect(answered).not.toContain(frame.id);
    expect(answered).not.toContain(echo.id);
    const { results, generations } = await frame.answer;
    const confirmed = await echo.answer;
    expect(answered.indexOf(frame.id)).toBeLessThan(answered.indexOf(echo.id));
    expect(generations).toEqual(written.state.collectionGenerations);
    expect(results[1]).toEqual({
      ok: true,
      result: expect.objectContaining({
        rows: [["user-1", "A first"]],
      }) as unknown,
    });
    expect(confirmed.acknowledgedMutationIds).toEqual(["m-echoed"]);
    expect(confirmed.state.pendingMutationIds).toEqual([]);
  }, 60_000);

  test("a batch that echoes no unanswered write sends a running frame back to read it", async () => {
    await sendChange(await nextBatch([renameOperation("user-3", "User 3")], []))
      .answer;
    const remote = await nextBatch([renameOperation("user-2", "A second")], []);
    const frame = sortingFrame();
    await macrotask();
    const moved = await sendChange(remote).answer;
    expect(answered).not.toContain(frame.id);
    const { generations } = await frame.answer;
    expect(generations).toEqual(moved.state.collectionGenerations);
  }, 60_000);

  test("the echo of a write a later frame has answered does not wait for the next frame", async () => {
    const rename = renameOperation("user-1", "A first, again");
    const echoed = await nextBatch([rename], ["m-painted"]);
    await ask<"mutate">({
      type: "mutate",
      mutationId: "m-painted",
      operations: [rename],
    }).answer;
    await ask<"frame">({
      type: "frame",
      members: [{ type: "query", specification: PROJECT_NAMES }],
    }).answer;
    const frame = sortingFrame();
    await macrotask();
    const confirmed = await sendChange(echoed).answer;
    expect(answered).not.toContain(frame.id);
    const { generations } = await frame.answer;
    expect(generations).toEqual(confirmed.state.collectionGenerations);
  }, 60_000);

  test("the echo of a dropped write does not wait for the frame running", async () => {
    const rename = renameOperation("user-1", "A first, dropped");
    const echoed = await nextBatch([rename], ["m-dropped"]);
    await ask<"mutate">({
      type: "mutate",
      mutationId: "m-dropped",
      operations: [rename],
    }).answer;
    await ask<"drop">({ type: "drop", mutationId: "m-dropped" }).answer;
    const frame = sortingFrame();
    await macrotask();
    const applied = await sendChange(echoed).answer;
    expect(answered).not.toContain(frame.id);
    const { generations } = await frame.answer;
    expect(generations).toEqual(applied.state.collectionGenerations);
  }, 60_000);

  test("a held echo applies once the frame it waits for is withdrawn", async () => {
    const rename = renameOperation("user-1", "A first, withdrawn");
    const echoed = await nextBatch([rename], ["m-withdrawn"]);
    await ask<"mutate">({
      type: "mutate",
      mutationId: "m-withdrawn",
      operations: [rename],
    }).answer;
    const frame = sortingFrame();
    await macrotask();
    const echo = sendChange(echoed);
    // Each turn runs one of the sort's slices: enough for the echo to
    // decode, far from enough for the sort to finish.
    for (let turn = 0; turn < 6; turn++) await macrotask();
    expect(answered).not.toContain(frame.id);
    expect(answered).not.toContain(echo.id);
    scope.onmessage?.({ data: { type: "cancel", target: frame.id } });
    const confirmed = await echo.answer;
    expect(confirmed.acknowledgedMutationIds).toEqual(["m-withdrawn"]);
    await macrotask();
    expect(answered).not.toContain(frame.id);
  }, 60_000);
});
