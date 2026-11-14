import { afterEach, describe, expect, it, vi } from "vitest";

import { missingBrowserFeature } from "../src/bootstrap-start.js";
import type { SyncClient, SyncStatus } from "../src/index.js";
import {
  createGeneratedClientSchema,
  createInternalColumnarClient,
} from "../src/internal.js";
import type { GeneratedClientSchemaDescriptor } from "../src/generated-schema.js";
import { exactResponseBuffers } from "../src/transport.js";

const clients: SyncClient[] = [];

const descriptor: GeneratedClientSchemaDescriptor = {
  collections: [
    {
      id: "tasks",
      tableId: "public.tasks",
      columns: [
        { name: "id", kind: "int64" },
        { name: "title", kind: "text" },
      ],
      primaryKey: ["id"],
    },
  ],
};

afterEach(async () => {
  vi.unstubAllGlobals();
  for (const client of clients.splice(0)) await client.close();
});

async function settled(client: SyncClient): Promise<SyncStatus> {
  return new Promise((resolve) => {
    const check = (): void => {
      const status = client.status;
      if (status.state === "ready" || status.state === "error") {
        unsubscribe();
        resolve(status);
      }
    };
    const unsubscribe = client.subscribe(check);
    check();
  });
}

/**
 * A client whose workers cannot start and whose fetch answers 500: it ends in
 * an error, and the error says whether the browser was refused before the
 * client got as far as a worker.
 */
function startedClient(counts: { fetches: number }): SyncClient {
  const client = createInternalColumnarClient(
    {
      endpoint: "https://sync.test",
      auth: () => ({ bearerToken: "app-token" }),
      schema: createGeneratedClientSchema(descriptor),
      memoryBudgetBytes: 32 * 1024 * 1024,
      clientHeapReserveBytes: 4 * 1024 * 1024,
      deviceId: "browser-support-test",
      fetch: async () => {
        counts.fetches += 1;
        return new Response(null, { status: 500 });
      },
    },
    () => {
      throw new Error("the test starts no worker");
    },
    () => {
      throw new Error("the test starts no worker");
    },
  );
  clients.push(client);
  return client;
}

describe("the APIs the client needs, in this browser", () => {
  it("are all here, so the client starts its workers instead of refusing the browser", async () => {
    expect(missingBrowserFeature()).toBeUndefined();
    const counts = { fetches: 0 };
    const status = await settled(startedClient(counts));
    expect(status.state).toBe("error");
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.code).not.toBe("UNSUPPORTED_BROWSER");
    expect(status.error.message).toBe("the test starts no worker");
  });

  it("detect module workers without starting a worker", () => {
    const NativeWorker = globalThis.Worker;
    let started = 0;
    vi.stubGlobal(
      "Worker",
      new Proxy(NativeWorker, {
        construct(target, args, newTarget) {
          const worker: unknown = Reflect.construct(target, args, newTarget);
          started += 1;
          return worker;
        },
      }),
    );
    expect(missingBrowserFeature()).toBeUndefined();
    expect(started).toBe(0);
  });

  it("refuse a browser that lacks one before any request, naming it", async () => {
    vi.stubGlobal("ReadableStreamBYOBReader", undefined);
    const counts = { fetches: 0 };
    const status = await settled(startedClient(counts));
    expect(status.state).toBe("error");
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.code).toBe("UNSUPPORTED_BROWSER");
    expect(status.error.recovery).toBe("none");
    expect(status.error.message).toBe(
      "this browser lacks ReadableStreamBYOBReader, which Calque needs",
    );
    expect(counts.fetches).toBe(0);
  });
});

describe("an artifact body read through BYOB readers, in this browser", () => {
  it("arrives in order, in the buffers the reads filled, and a body that ends short is refused", async () => {
    const bytes = new Uint8Array(3 * 1024 * 1024 + 17);
    for (let index = 0; index < bytes.length; index += 1)
      bytes[index] = (index * 31 + 7) % 251;
    const url = URL.createObjectURL(new Blob([bytes]));
    try {
      const joined = new Uint8Array(bytes.length);
      let at = 0;
      for await (const buffer of exactResponseBuffers(
        await fetch(url),
        bytes.length,
        1024 * 1024,
      )) {
        joined.set(new Uint8Array(buffer), at);
        at += buffer.byteLength;
      }
      expect(at).toBe(bytes.length);
      expect(joined).toEqual(bytes);

      await expect(async () => {
        for await (const buffer of exactResponseBuffers(
          await fetch(url),
          bytes.length + 1,
          1024 * 1024,
        ))
          void buffer;
      }).rejects.toMatchObject({ code: "INVALID_RESPONSE" });
    } finally {
      URL.revokeObjectURL(url);
    }
  });
});
