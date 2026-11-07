import { fromBinary } from "@bufbuild/protobuf";
import { change, decodePrimaryKey } from "@calque/protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createGeneratedClientSchema } from "../client-options.js";
import { eq } from "../facade/predicates.js";
import type { CollectionFacade, CollectionShape } from "../facade/types.js";
import type { GeneratedClientSchemaDescriptor } from "../generated-schema.js";
import {
  descriptor,
  makeClient,
  plan,
  settled,
  until,
  type WorkerRequest,
} from "./session.fixture.js";

interface TaskRow {
  readonly id: string;
  readonly owner: string | null;
  readonly level: string;
}
type Tasks = CollectionShape<
  TaskRow,
  { readonly id: string },
  TaskRow,
  { readonly owner?: string | null; readonly level?: string },
  true
>;

const TASK = "f0e1d2c3-b4a5-4697-8879-6a5b4c3d2e1f";
const OWNER = "0a1b2c3d-4e5f-4061-8172-839495a6b7c8";
const NEW_TASK = "c0ffee00-dead-4bee-8f00-0123456789ab";

function bytesOf(text: string): Uint8Array {
  const hex = text.replaceAll("-", "");
  return Uint8Array.from({ length: 16 }, (_unused, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

/** The fixture schema, with `events` keyed by a uuid and owned by another. */
const uuidDescriptor: GeneratedClientSchemaDescriptor = {
  ...descriptor,
  collections: [
    {
      id: "events",
      tableId: "public.events",
      columns: [
        { name: "id", kind: "uuid" },
        { name: "owner", kind: "uuid" },
        { name: "level", kind: "text" },
      ],
      primaryKey: ["id"],
      writes: {
        operations: ["insert", "update", "delete"],
        writableColumns: ["owner", "level"],
      },
    },
    ...descriptor.collections.slice(1),
  ],
};

function uuidPlan(): Record<string, unknown> {
  const value = plan();
  const runtime = (value.artifacts as Record<string, unknown>[])[0]
    ?.runtime as {
    schema: { tables: Record<string, unknown>[] };
    tables: Record<string, unknown>[];
  };
  Object.assign(runtime.schema.tables[0] ?? {}, {
    columns: [
      { name: "id", type: "uuid", nullable: false },
      { name: "owner", type: "uuid", nullable: true },
      { name: "level", type: "dict", nullable: false },
    ],
  });
  Object.assign(runtime.tables[0] ?? {}, {
    columns: [
      { columnId: 1, name: "id" },
      { columnId: 2, name: "owner" },
      { columnId: 3, name: "level" },
    ],
  });
  return value;
}

function uuidClient() {
  const made = makeClient(uuidPlan(), undefined, undefined, undefined, {
    schema: createGeneratedClientSchema(uuidDescriptor),
  });
  const rows: (readonly unknown[])[] = [
    [bytesOf(TASK), bytesOf(OWNER), "error"],
  ];
  const answers = () =>
    made.worker.queryAnswers.set("events", {
      columns: ["id", "owner", "level"],
      rows: [...rows],
    });
  answers();
  made.worker.sliceRows = rows;
  const operations: change.Operation[] = [];
  // The fake overlay applies an insert the way the real one does: the row
  // it holds afterwards is the operation's cells, still bytes.
  const answer = made.worker.answer.bind(made.worker);
  made.worker.answer = (request: WorkerRequest) => {
    if (request.type === "mutate") {
      const operation = fromBinary(
        change.OperationSchema,
        (request.operations as Uint8Array[])[0] ?? new Uint8Array(),
      );
      operations.push(operation);
      if (operation.payload.case === "insert") {
        rows.push(
          operation.payload.value.columns.map(
            ({ value }) => value?.kind.value ?? null,
          ),
        );
        answers();
      }
    }
    answer(request);
  };
  return {
    ...made,
    operations,
    tasks: made.client.db.events as unknown as CollectionFacade<Tasks>,
  };
}

describe("uuid columns through a session", () => {
  beforeEach(() => {
    vi.stubGlobal("navigator", {
      userAgentData: { brands: [{ brand: "Chromium" }] },
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("read back as text from a live query and a slice", async () => {
    const { client, tasks } = uuidClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const live = tasks
      .query()
      .where(eq("owner", OWNER.toUpperCase()))
      .subscribe();
    const unsubscribe = live.subscribe(() => undefined);
    await live.promise;
    expect(live.getSnapshot()).toMatchObject({
      status: "ready",
      result: [{ id: TASK, owner: OWNER, level: "error" }],
    });
    expect(await tasks.slice(0, 1)).toEqual([
      { id: TASK, owner: OWNER, level: "error" },
    ]);
    unsubscribe();
    await client.close();
  });

  it("read back as text from a live window", async () => {
    const { client, tasks, worker } = uuidClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.queryRows = [[bytesOf(TASK), "error"]];
    const live = tasks.query().subscribeWindow(0, 1);
    const unsubscribe = live.subscribe(() => undefined);
    await live.promise;
    expect(live.getSnapshot()).toMatchObject({
      status: "ready",
      result: { offset: 0, total: 1, rows: [{ id: TASK, level: "error" }] },
    });
    unsubscribe();
    await client.close();
  });

  it("writes uuid text as 16 bytes, and reads the optimistic row back as text", async () => {
    const { client, tasks, operations } = uuidClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const live = tasks.query().orderBy("level").subscribe();
    const unsubscribe = live.subscribe(() => undefined);
    await live.promise;

    const { mutationId } = await tasks.insert({
      id: NEW_TASK.toUpperCase(),
      owner: OWNER,
      level: "warn",
    });
    // The status names the row the way reads do, as lowercase text.
    expect(client.mutationStatus(mutationId)).toMatchObject({
      collection: "events",
      key: { id: NEW_TASK },
    });
    const rows = () => {
      const snapshot = live.getSnapshot();
      return snapshot.status === "ready" ? snapshot.result : [];
    };
    await until(() => rows().length === 2, "the optimistic insert");
    expect(rows()).toEqual([
      { id: TASK, owner: OWNER, level: "error" },
      { id: NEW_TASK, owner: OWNER, level: "warn" },
    ]);

    await tasks.update({ id: TASK }, { owner: null });
    await tasks.delete({ id: NEW_TASK });
    const [insert, update, remove] = operations;
    expect(insert?.payload).toMatchObject({ case: "insert" });
    expect(
      insert?.payload.case === "insert" ? insert.payload.value.columns : [],
    ).toMatchObject([
      {
        columnId: 1,
        value: { kind: { case: "uuid16", value: bytesOf(NEW_TASK) } },
      },
      {
        columnId: 2,
        value: { kind: { case: "uuid16", value: bytesOf(OWNER) } },
      },
      { columnId: 3, value: { kind: { case: "textValue", value: "warn" } } },
    ]);
    expect(update?.kind).toBe(change.OperationKind.UPDATE_PATCH);
    expect(remove?.kind).toBe(change.OperationKind.DELETE_BY_KEY);
    expect(
      [insert, update, remove].map(
        (operation) =>
          decodePrimaryKey(operation?.primaryKey ?? new Uint8Array()).parts[0]
            ?.value?.kind,
      ),
    ).toEqual([
      { case: "uuid16", value: bytesOf(NEW_TASK) },
      { case: "uuid16", value: bytesOf(TASK) },
      { case: "uuid16", value: bytesOf(NEW_TASK) },
    ]);
    unsubscribe();
    await client.close();
  });

  it("refuses a key or value that is not uuid text before anything is applied", async () => {
    const { client, tasks, operations } = uuidClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(tasks.delete({ id: "task-1" })).rejects.toThrow(
      new TypeError(
        "events.id must be a uuid string, xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      ),
    );
    await expect(
      tasks.update({ id: bytesOf(TASK) as never }, { level: "warn" }),
    ).rejects.toThrow(TypeError);
    await expect(
      tasks.update({ id: TASK }, { owner: `${OWNER}0` }),
    ).rejects.toThrow("events.owner must be a uuid string");
    expect(operations).toEqual([]);
    await client.close();
  });
});
