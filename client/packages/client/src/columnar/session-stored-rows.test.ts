import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { stubBrowserFeatures } from "../browser.fixture.js";
import {
  ARTIFACT_BYTES,
  FakeRowStore,
  STORED_ROWS_KEY,
  makeClient,
  makeRebuildableClient,
  patternBytes,
  plan,
  settled,
  until,
} from "./session.fixture.js";

function keyedPlan(): Record<string, unknown> {
  return { ...plan(), storedRowsKey: STORED_ROWS_KEY };
}

function artifactRequests(fetcher: { mock: { calls: unknown[][] } }): number {
  return fetcher.mock.calls.filter(([input]) =>
    String(input instanceof Request ? input.url : input).endsWith(
      "/artifact-1",
    ),
  ).length;
}

/** The copy a load of `keyedPlan()` keeps, as the page's cookie names it. */
const COPY = {
  keyId: STORED_ROWS_KEY,
  decodedSha256: String(
    (keyedPlan().artifacts as Record<string, unknown>[])[0]?.decodedSha256,
  ),
};

/** What each bootstrap request the page posted named as held copies. */
function hintsSent(fetcher: { mock: { calls: unknown[][] } }): unknown[] {
  return fetcher.mock.calls
    .filter(([input]) => String(input).endsWith("/v1/bootstrap"))
    .map(
      ([, init]) =>
        (
          JSON.parse(String((init as RequestInit).body)) as {
            cachedArtifactHints: unknown;
          }
        ).cachedArtifactHints,
    );
}

/** The store as a load that kept the plan's artifact left it. */
function storeHolding(bytes: Uint8Array): FakeRowStore {
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
    chunks: [bytes.slice().buffer],
  };
  return store;
}

describe("the rows a page keeps between visits", () => {
  beforeEach(() => {
    stubBrowserFeatures();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("are kept once the load has activated, from the chunks the engine accepted", async () => {
    const store = new FakeRowStore();
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    expect(await settled(made.client)).toEqual({ state: "ready" });

    const [writer] = store.writers;
    expect(store.writers).toHaveLength(1);
    expect(writer?.committed).toBe(true);
    expect(writer?.abandoned).toBe(false);
    expect(writer?.keyId).toBe(STORED_ROWS_KEY);
    const open = made.worker.requests.find(({ type }) => type === "open");
    expect(open?.store).toBeDefined();
    await until(() => store.stored !== undefined, "the stored copy");
    expect(store.stored?.chunks.map((chunk) => new Uint8Array(chunk))).toEqual([
      new Uint8Array(ARTIFACT_BYTES),
    ]);
    expect(artifactRequests(made.fetcher)).toBe(1);
    // The page's cookie names the copy once it has landed.
    await until(() => store.hinted !== undefined, "the hint");
    expect(store.hinted).toEqual(COPY);
    await made.client.close();
  });

  it("are read by the next load, which downloads no artifact byte", async () => {
    const bytes = patternBytes(ARTIFACT_BYTES);
    const store = storeHolding(bytes);
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    expect(await settled(made.client)).toEqual({ state: "ready" });

    expect(artifactRequests(made.fetcher)).toBe(0);
    expect(made.worker.received).toEqual([bytes]);
    expect(store.writers).toEqual([]);
    expect(store.held).toBe(0);
    expect(store.erasures).toBe(0);
    expect(store.hinted).toEqual(COPY);
    expect(await made.client.db.events.query().run()).toEqual([
      { id: 1n, level: "error" },
    ]);
    await made.client.close();
  });

  it("are neither read nor kept without a key in the plan or with storeRows off", async () => {
    const unkeyed = new FakeRowStore();
    unkeyed.hinted = COPY;
    const withoutKey = makeClient(
      plan(),
      undefined,
      undefined,
      undefined,
      {},
      unkeyed,
    );
    expect(await settled(withoutKey.client)).toEqual({ state: "ready" });
    expect(unkeyed.opens).toBe(0);
    expect(unkeyed.writers).toEqual([]);
    expect(unkeyed.hinted).toBeUndefined();
    const open = withoutKey.worker.requests.find(({ type }) => type === "open");
    expect(open && "store" in open).toBe(false);
    await withoutKey.client.close();

    const off = storeHolding(patternBytes(ARTIFACT_BYTES));
    off.hinted = COPY;
    const storeRowsOff = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      { storeRows: false },
      off,
    );
    expect(await settled(storeRowsOff.client)).toEqual({ state: "ready" });
    expect(off.opens).toBe(0);
    expect(off.writers).toEqual([]);
    // The copy stays, but no hint keeps the page's backend from preloading.
    expect(off.hinted).toBeUndefined();
    expect(off.stored).toBeDefined();
    expect(artifactRequests(storeRowsOff.fetcher)).toBe(1);
    // An application that turned storing off can still erase what an
    // earlier page kept.
    await storeRowsOff.client.clearStoredRows();
    expect(off.erasures).toBe(1);
    expect(off.stored).toBeUndefined();
    await storeRowsOff.client.close();
  });

  it("are named by the plan request, so the server plans the cut they hold, unless storeRows is off", async () => {
    const store = storeHolding(patternBytes(ARTIFACT_BYTES));
    store.hinted = COPY;
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    expect(await settled(made.client)).toEqual({ state: "ready" });
    expect(hintsSent(made.fetcher)).toEqual([[COPY.decodedSha256]]);
    expect(artifactRequests(made.fetcher)).toBe(0);
    await made.client.close();

    const off = storeHolding(patternBytes(ARTIFACT_BYTES));
    off.hinted = COPY;
    const storeRowsOff = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      { storeRows: false },
      off,
    );
    expect(await settled(storeRowsOff.client)).toEqual({ state: "ready" });
    expect(hintsSent(storeRowsOff.fetcher)).toEqual([[]]);
    await storeRowsOff.client.close();
  });

  it("refuses storeRows that is not a boolean", () => {
    expect(() =>
      makeClient(keyedPlan(), undefined, undefined, undefined, {
        storeRows: "yes",
      }),
    ).toThrow("SyncClientOptions storeRows must be a boolean");
  });

  it("are erased when the copy is broken, and the load downloads the artifact and keeps it again", async () => {
    const store = storeHolding(patternBytes(ARTIFACT_BYTES));
    store.breakCopy = true;
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    expect(await settled(made.client)).toEqual({ state: "ready" });

    expect(store.erasures).toBe(1);
    expect(artifactRequests(made.fetcher)).toBe(1);
    expect(made.worker.received).toEqual([new Uint8Array(ARTIFACT_BYTES)]);
    expect(store.writers[0]?.committed).toBe(true);
    await made.client.close();
  });

  it("are erased when a chunk does not decrypt, and the load downloads the rest", async () => {
    const store = storeHolding(patternBytes(ARTIFACT_BYTES));
    store.breakChunk = 0;
    store.hinted = COPY;
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    expect(await settled(made.client)).toEqual({ state: "ready" });

    expect(store.erasures).toBe(1);
    expect(store.held).toBe(0);
    expect(store.hinted).toBeUndefined();
    expect(artifactRequests(made.fetcher)).toBe(1);
    expect(made.worker.received).toEqual([new Uint8Array(ARTIFACT_BYTES)]);
    await made.client.close();
  });

  it("are let go of by a load whose epoch ended while it looked them up", async () => {
    let release!: () => void;
    const store = storeHolding(patternBytes(ARTIFACT_BYTES));
    store.holdOpen = new Promise<void>((resolve) => {
      release = resolve;
    });
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    await until(() => store.opens === 1, "the first load's lookup");
    const reset = made.client.reset();
    release();
    await reset;
    expect(await settled(made.client)).toEqual({ state: "ready" });

    expect(store.opens).toBe(2);
    expect(store.held).toBe(0);
    await made.client.close();
  });

  it("are not kept from a load that did not activate", async () => {
    const store = new FakeRowStore();
    store.hinted = { keyId: "another-key", decodedSha256: "0".repeat(64) };
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    // The mount opens away from the artifact's cut, so the load fails after
    // every chunk reached the engine and the store.
    made.worker.openCursor = "cursor-elsewhere";
    const status = await settled(made.client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INTEGRITY_FAILED" },
    });

    expect(store.writers.length).toBeGreaterThan(0);
    for (const writer of store.writers) {
      expect(writer.committed).toBe(false);
      expect(writer.abandoned).toBe(true);
    }
    expect(store.stored).toBeUndefined();
    // A miss leaves no hint of a copy this plan would not read.
    expect(store.hinted).toBeUndefined();
    await made.client.close();
  });

  it("are erased, with the load in flight kept from storing, by clearStoredRows()", async () => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const store = new FakeRowStore();
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (String(input).endsWith("/artifact-1")) await held;
        return inner(input, init);
      },
      {},
      store,
    );
    await until(() => store.writers.length === 1, "the load's writer");
    await made.client.clearStoredRows();
    expect(store.erasures).toBe(1);
    release();
    expect(await settled(made.client)).toEqual({ state: "ready" });

    expect(store.writers[0]?.abandoned).toBe(true);
    expect(store.writers[0]?.committed).toBe(false);
    expect(store.stored).toBeUndefined();
    expect(store.hinted).toBeUndefined();
    await made.client.clearStoredRows();
    expect(store.erasures).toBe(2);
    await made.client.close();
  });

  it("are not kept by a load whose lookup clearStoredRows() overlapped", async () => {
    let release!: () => void;
    const store = new FakeRowStore();
    store.holdOpen = new Promise<void>((resolve) => {
      release = resolve;
    });
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    await until(() => store.opens === 1, "the load's lookup");
    await made.client.clearStoredRows();
    release();
    expect(await settled(made.client)).toEqual({ state: "ready" });

    expect(store.stored).toBeUndefined();
    expect(store.hinted).toBeUndefined();
    expect(store.writers).toHaveLength(0);
    await made.client.close();
  });

  it("are not named by the cookie when clearStoredRows() overlapped the read of them", async () => {
    let release!: () => void;
    const store = storeHolding(patternBytes(ARTIFACT_BYTES));
    store.holdOpen = new Promise<void>((resolve) => {
      release = resolve;
    });
    const made = makeClient(
      keyedPlan(),
      undefined,
      undefined,
      undefined,
      {},
      store,
    );
    await until(() => store.opens === 1, "the load's lookup");
    const held = store.stored;
    await made.client.clearStoredRows();
    // The copy open in this page is read before an erasure deletes it.
    store.stored = held;
    release();
    expect(await settled(made.client)).toEqual({ state: "ready" });

    expect(artifactRequests(made.fetcher)).toBe(0);
    expect(store.hinted).toBeUndefined();
    await made.client.close();
  });

  it("are erased by a revocation barrier", async () => {
    const store = new FakeRowStore();
    const harness = makeRebuildableClient(
      [
        {
          cursor: "cursor-p",
          marker: "epoch-one",
          change: {
            error: "authorization generation 7 was revoked by barrier b-1",
            code: "AUTHORIZATION_REVOKED",
          },
        },
        {
          cursor: "cursor-q",
          marker: "epoch-two",
          change: { cursor: "cursor-q" },
        },
      ],
      (index) =>
        index === 1
          ? until(() => store.erasures > 0, "the erasure").then(() => undefined)
          : undefined,
      { store },
    );
    const { client, poll } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(store.writers[0]?.committed).toBe(true);
    expect(store.erasures).toBe(0);

    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(() => harness.bootstraps === 2, "the rebuild's bootstrap");
    expect(store.erasures).toBe(1);
    await client.close();
  });

  it("are erased when the session locks", async () => {
    const store = new FakeRowStore();
    const harness = makeRebuildableClient(
      [
        {
          cursor: "cursor-p",
          marker: "epoch-one",
          change: {
            error: "change request failed with HTTP 401: AUTHORIZATION_CHANGED",
            code: "AUTHORIZATION_EXPIRED",
          },
        },
      ],
      undefined,
      {
        store,
        bootstrap: (index) =>
          index > 0 ? new Response("expired", { status: 401 }) : undefined,
      },
    );
    const { client, poll } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    await until(() => store.stored !== undefined, "the stored copy");

    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(() => client.status.state === "locked", "the lock");
    await until(() => store.erasures > 0, "the erasure");
    expect(store.stored).toBeUndefined();
    await client.close();
  });
});
