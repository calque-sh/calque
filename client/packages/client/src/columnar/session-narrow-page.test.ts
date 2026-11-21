import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { stubBrowserFeatures } from "../browser.fixture.js";
import {
  ARTIFACT_BYTES,
  FakeRowStore,
  STORED_ROWS_KEY,
  controllableLongPoll,
  makeClient,
  narrowSchema,
  narrowed,
  patternBytes,
  plan,
  settled,
} from "./session.fixture.js";

function keyedPlan(): Record<string, unknown> {
  return narrowed({ ...plan(), storedRowsKey: STORED_ROWS_KEY });
}

describe("a page whose bindings read fewer tables than the server serves", () => {
  beforeEach(() => {
    stubBrowserFeatures();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("asks for its own tables, installs only those, and answers queries", async () => {
    const { client, worker, fetcher } = makeClient(
      narrowed(plan()),
      undefined,
      undefined,
      undefined,
      { schema: narrowSchema },
    );
    expect(await settled(client)).toEqual({ state: "ready" });

    const bootstrap = fetcher.mock.calls[0];
    expect(
      (
        JSON.parse(String(bootstrap?.[1]?.body)) as {
          requiredTables: string[];
        }
      ).requiredTables,
    ).toEqual(["public.events"]);

    const open = worker.requests.find(({ type }) => type === "open");
    expect(open?.changeStream).toMatchObject({
      tables: [
        { tableId: "public.events", collection: "events", primaryKey: [1] },
      ],
      ignoredTableIds: ["public.issues"],
    });
    expect(
      (
        open?.trusted as { schema: { tables: { name: string }[] } }
      ).schema.tables.map(({ name }) => name),
    ).toEqual(["events", "issues"]);

    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "error" },
    ]);
    await client.close();
  });

  it("names every served table to the change stream, the ignored ones too", async () => {
    const poll = controllableLongPoll();
    const { client } = makeClient(
      narrowed(plan()),
      poll.fetcher,
      undefined,
      undefined,
      { schema: narrowSchema },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await vi.waitFor(() => expect(poll.requests.length).toBeGreaterThan(0));
    expect(poll.requests[0]?.tables.map(({ tableId }) => tableId)).toEqual([
      "public.events",
      "public.issues",
    ]);
    await client.close();
  });

  it("reads the copy a wider page kept, downloading no artifact byte", async () => {
    const store = new FakeRowStore();
    const artifact = (keyedPlan().artifacts as Record<string, unknown>[])[0];
    store.stored = {
      keyId: STORED_ROWS_KEY,
      artifact: {
        artifactId: String(artifact?.artifactId),
        decodedSha256: String(artifact?.decodedSha256),
        decodedBytes: ARTIFACT_BYTES,
        chunkBytes: 4 * 1024 * 1024,
        chunkSha256: [],
      },
      chunks: [patternBytes(ARTIFACT_BYTES).slice().buffer],
    };
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      { schema: narrowSchema },
      store,
    );
    expect(await settled(made.client)).toEqual({ state: "ready" });
    expect(
      made.fetcher.mock.calls.filter(([input]) =>
        String(input).endsWith("/artifact-1"),
      ),
    ).toHaveLength(0);
    expect(made.worker.received).toEqual([patternBytes(ARTIFACT_BYTES)]);
    expect(store.erasures).toBe(0);
    await made.client.close();
  });
});
