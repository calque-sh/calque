import { afterEach, describe, expect, it } from "vitest";

import type { SyncClient, SyncStatus } from "../src/index.js";
import {
  createGeneratedClientSchema,
  createInternalColumnarClient,
} from "../src/internal.js";
import type { GeneratedClientSchemaDescriptor } from "../src/generated-schema.js";

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

describe("unsupported artifact transport profile", () => {
  it("rejects Firefox before bootstrap, download, or any worker's creation", async () => {
    let fetchCalls = 0;
    let workerCreations = 0;
    let hasherCreations = 0;
    const client = createInternalColumnarClient(
      {
        endpoint: "https://sync.test",
        auth: () => ({ bearerToken: "app-token" }),
        schema: createGeneratedClientSchema(descriptor),
        memoryBudgetBytes: 32 * 1024 * 1024,
        clientHeapReserveBytes: 4 * 1024 * 1024,
        deviceId: "firefox-profile-test",
        fetch: async () => {
          fetchCalls += 1;
          return new Response(null, { status: 500 });
        },
      },
      () => {
        workerCreations += 1;
        throw new Error("the engine worker must not be created");
      },
      () => {
        hasherCreations += 1;
        throw new Error("no hasher worker may be created");
      },
    );
    clients.push(client);

    const status = await settled(client);
    expect(status.state).toBe("error");
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.code).toBe("UNSUPPORTED_BROWSER");
    expect(status.error.recovery).toBe("none");
    expect(fetchCalls).toBe(0);
    expect(workerCreations).toBe(0);
    expect(hasherCreations).toBe(0);
  });
});
