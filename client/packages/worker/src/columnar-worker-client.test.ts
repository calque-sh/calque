import { afterEach, describe, expect, test } from "vitest";
import {
  EngineWorkerClient,
  type ArtifactChunkSource,
} from "./columnar-worker-client.js";
import type {
  WorkerArtifactCredit,
  WorkerArtifactEnd,
  WorkerRequest,
  WorkerResponse,
} from "./columnar-worker-protocol.js";

class FakeWorker extends EventTarget {
  terminated = false;
  requests: WorkerRequest[] = [];
  ends: WorkerArtifactEnd[] = [];
  /** How many ports each request transferred. */
  transferredPorts: number[] = [];

  postMessage(
    message: WorkerRequest | WorkerArtifactEnd,
    transfer: Transferable[] = [],
  ) {
    const cloned = structuredClone(message, { transfer });
    if (cloned.type === "artifact-end") this.ends.push(cloned);
    else {
      this.requests.push(cloned);
      this.transferredPorts.push(
        transfer.filter((item) => item instanceof MessagePort).length,
      );
    }
  }

  terminate() {
    this.terminated = true;
  }

  respond(response: WorkerResponse | WorkerArtifactCredit) {
    this.dispatchEvent(new MessageEvent("message", { data: response }));
  }
}

const channels: MessageChannel[] = [];

afterEach(() => {
  for (const { port1, port2 } of channels.splice(0)) {
    port1.close();
    port2.close();
  }
});

/** Hashers' ports, as `ArtifactHashers` hands them over, and the credits. */
function chunkSource(count = 3): ArtifactChunkSource & {
  readonly credited: number[];
  taken: number;
} {
  const ports = Array.from({ length: count }, () => {
    const channel = new MessageChannel();
    channels.push(channel);
    return channel.port2;
  });
  const source = {
    credited: [] as number[],
    taken: 0,
    takePorts: () => {
      source.taken += 1;
      return ports.splice(0);
    },
    credit: (index: number) => {
      source.credited.push(index);
    },
  };
  return source;
}

const trusted = {
  byteLength: 8,
  digest: `sha256:${"0".repeat(64)}` as const,
  identity: {
    sourceEpoch: "source-1",
    manifestVersion: "manifest-1",
    collectionSetHash: `sha256:${"1".repeat(64)}` as const,
    schemaHash: `sha256:${"2".repeat(64)}` as const,
    authorizationScopeVersion: "scope-1",
    authorizationScopeHash: `sha256:${"3".repeat(64)}` as const,
    cursor: "cursor-1",
  },
  schema: {
    format: "calque-direct-columnar" as const,
    version: 4 as const,
    tables: [
      {
        name: "events",
        primaryKey: ["id"],
        columns: [{ name: "id", type: "int64" as const, nullable: false }],
      },
    ],
  },
};

/**
 * A mount opened without a change contract: frozen at the artifact's cursor.
 */
const FROZEN_STATE = {
  cursor: undefined,
  live: false,
  collectionRows: { events: 1 },
  collectionGenerations: { events: 0 },
  pendingMutationIds: [],
} as const;

describe("engine worker client", () => {
  test("sends the open with the hashers' ports, credits the source for each chunk, ends with the count, and resolves only a matching open response", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const source = chunkSource(3);
    const receiving = client.receive(trusted, source);
    expect(source.taken).toBe(1);
    const request = worker.requests[0];
    expect(request).toMatchObject({ id: request?.id, type: "open", trusted });
    const hashers =
      request?.type === "open" ? request.hashers : ([] as MessagePort[]);
    expect(hashers).toHaveLength(3);
    for (const port of hashers) expect(port).toBeInstanceOf(MessagePort);
    // The ports are transferred with the open, not copied.
    expect(worker.transferredPorts).toEqual([3]);
    worker.respond({ type: "artifact-credit", index: 1 });
    worker.respond({ type: "artifact-credit", index: 0 });
    expect(source.credited).toEqual([1, 0]);
    receiving.end(2);
    expect(worker.ends).toEqual([{ type: "artifact-end", chunks: 2 }]);
    worker.respond({
      id: request?.id ?? 0,
      ok: true,
      type: "open",
      rowCount: 7,
      artifactByteLength: 8,
      tableRows: { events: 1 },
      state: FROZEN_STATE,
    });
    await expect(receiving.opened).resolves.toEqual({
      rowCount: 7,
      artifactByteLength: 8,
      tableRows: { events: 1 },
      state: FROZEN_STATE,
    });
    // Once the open has answered, no credit reaches the source.
    worker.respond({ type: "artifact-credit", index: 2 });
    expect(source.credited).toEqual([1, 0]);
  });

  test("mutate and drop carry the id and operations and resolve to the mount state", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const operation = Uint8Array.from([0x0a, 0x01, 0x78]);
    const mutating = client.mutate("m-1", [operation]);
    const mutate = worker.requests[0];
    expect(mutate).toMatchObject({ type: "mutate", mutationId: "m-1" });
    expect(
      (mutate as unknown as { operations: Uint8Array[] }).operations,
    ).toEqual([operation]);
    const pendingState = { ...FROZEN_STATE, pendingMutationIds: ["m-1"] };
    worker.respond({
      id: mutate?.id ?? 0,
      ok: true,
      type: "mutate",
      state: pendingState,
    });
    await expect(mutating).resolves.toEqual({ state: pendingState });

    const dropping = client.drop("m-1");
    const drop = worker.requests[1];
    expect(drop).toMatchObject({ type: "drop", mutationId: "m-1" });
    worker.respond({
      id: drop?.id ?? 0,
      ok: true,
      type: "drop",
      state: FROZEN_STATE,
    });
    await expect(dropping).resolves.toEqual({ state: FROZEN_STATE });
  });

  test("a refused mutation rejects with the worker's error code", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const mutating = client.mutate("m-1", [Uint8Array.from([1])]);
    worker.respond({
      id: worker.requests[0]?.id ?? 0,
      ok: false,
      error: {
        name: "RuntimeError",
        message: "this mount was opened without a live change stream",
        code: "INVALID_LIFECYCLE",
      },
    });
    await expect(mutating).rejects.toMatchObject({ code: "INVALID_LIFECYCLE" });
  });

  test("an already aborted open is not sent", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const controller = new AbortController();
    controller.abort();
    await expect(
      client.receive(
        trusted,
        chunkSource(),
        undefined,
        undefined,
        controller.signal,
      ).opened,
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(worker.requests).toHaveLength(0);
  });

  test("aborting an in-flight open destroys the worker and rejects", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const controller = new AbortController();
    const pending = client.receive(
      trusted,
      chunkSource(),
      undefined,
      undefined,
      controller.signal,
    ).opened;
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(worker.terminated).toBe(true);
  });

  test("worker errors reject every pending bounded row request", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const pending = client.rows("events", 0, 32, ["id"]);
    worker.dispatchEvent(
      Object.assign(new Event("error"), { message: "worker crashed" }),
    );
    await expect(pending).rejects.toThrow("worker crashed");
  });

  // Without the `messageerror` handler every pending request hangs forever, so
  // losing it shows as a timeout rather than a failed assertion.
  test("undecodable worker messages reject every pending bounded row request", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const pending = client.rows("events", 0, 32, ["id"]);
    worker.dispatchEvent(new MessageEvent("messageerror"));
    await expect(pending).rejects.toThrow(
      "engine worker message could not be decoded",
    );
  });

  test("returns sealed data query results without row indexes", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const pending = client.query({
      version: 1,
      from: { collection: "events", as: "e" },
      select: [
        {
          kind: "field",
          field: { source: "e", column: "id" },
          as: "id",
        },
      ],
    });
    const request = worker.requests[0];
    worker.respond({
      id: request?.id ?? 0,
      ok: true,
      type: "query",
      result: { columns: ["id"], rows: [[1], [2]] },
      generations: { events: 4 },
    });
    const { result, generations } = await pending;
    expect(result).toEqual({
      columns: ["id"],
      rows: [[1], [2]],
    });
    expect(generations).toEqual({ events: 4 });
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.columns)).toBe(true);
    expect(Object.isFrozen(result.rows[0])).toBe(true);
  });

  test("carries a binary column across the boundary as the same Uint8Array shape", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const pending = client.query({
      version: 1,
      from: { collection: "events", as: "e" },
      select: [
        { kind: "field", field: { source: "e", column: "id" }, as: "id" },
      ],
    });
    const request = worker.requests[0];
    const bytes = Uint8Array.from({ length: 16 }, (_, index) => index);
    worker.respond({
      id: request?.id ?? 0,
      ok: true,
      type: "query",
      result: { columns: ["id"], rows: [[bytes]] },
      generations: { events: 0 },
    });
    const value = (await pending).result.rows[0]?.[0];
    // Sealing must leave the array intact rather than walking it as a record.
    expect(value).toBeInstanceOf(Uint8Array);
    expect(value).toEqual(bytes);
  });

  test("a window carries its bounds and answers with the whole result's total", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const specification = {
      version: 1 as const,
      from: { collection: "events", as: "e" },
      select: [
        {
          kind: "field" as const,
          field: { source: "e", column: "id" },
          as: "id",
        },
      ],
    };
    const pending = client.window(
      specification,
      { offset: 700_000, count: 2 },
      { maxBytes: 1024 },
    );
    const request = worker.requests[0];
    expect(request).toEqual({
      id: request?.id,
      type: "window",
      specification,
      window: { offset: 700_000, count: 2 },
      options: { maxBytes: 1024 },
    });
    worker.respond({
      id: request?.id ?? 0,
      ok: true,
      type: "window",
      result: {
        columns: ["id"],
        rows: [[7n], [8n]],
        offset: 700_000,
        total: 800_000,
      },
      generations: { events: 2 },
    });
    const { result, generations } = await pending;
    expect(result).toEqual({
      columns: ["id"],
      rows: [[7n], [8n]],
      offset: 700_000,
      total: 800_000,
    });
    expect(Object.isFrozen(result)).toBe(true);
    expect(generations).toEqual({ events: 2 });
  });

  test("aborting a query withdraws it from the worker; aborting rows does not", async () => {
    const worker = new FakeWorker();
    const client = new EngineWorkerClient(worker as unknown as Worker);
    const controller = new AbortController();
    const pending = client.window(
      {
        version: 1,
        from: { collection: "events", as: "e" },
        select: [
          { kind: "field", field: { source: "e", column: "id" }, as: "id" },
        ],
      },
      { offset: 0, count: 1 },
      undefined,
      controller.signal,
    );
    const rows = new AbortController();
    const reading = client.rows("events", 0, 1, ["id"], rows.signal);
    const [request, read] = worker.requests;
    controller.abort();
    rows.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    await expect(reading).rejects.toMatchObject({ name: "AbortError" });
    expect(worker.requests.slice(2)).toEqual([
      { type: "cancel", target: request?.id },
    ]);
    expect(read?.type).toBe("rows");
    expect(worker.terminated).toBe(false);
  });
});
