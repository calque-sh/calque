import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { stubBrowserFeatures } from "../browser.fixture.js";
import { eq } from "../facade/predicates.js";
import { UNLISTENED_GRACE_MS as GRACE_MS } from "./query-subscription.js";
import { makeClient, settled } from "./session.fixture.js";

describe("subscriptions no component listens to", () => {
  beforeEach(() => {
    stubBrowserFeatures();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("releases the resources of renders React discarded", async () => {
    const { client } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const discarded = [];
    for (let keystroke = 0; keystroke < 50; keystroke += 1) {
      const query = client.db.events
        .query()
        .where(eq("level", `k${keystroke}`));
      const resource = query.subscribe();
      await resource.promise;
      discarded.push({ query, resource });
    }
    vi.advanceTimersByTime(GRACE_MS);
    for (const { query, resource } of discarded)
      expect(query.subscribe()).not.toBe(resource);
    vi.useRealTimers();
    await client.close();
  });

  it("serves a component that commits after the promise settled without a second read", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const query = client.db.events.query().where(eq("level", "error"));
    const resource = query.subscribe();
    await resource.promise;
    const reads = worker.requests.filter(({ type }) => type === "frame").length;
    vi.advanceTimersByTime(GRACE_MS - 1);
    expect(query.subscribe()).toBe(resource);
    const stop = resource.subscribe(() => undefined);
    vi.advanceTimersByTime(GRACE_MS * 2);
    expect(query.subscribe()).toBe(resource);
    expect(resource.getSnapshot()).toMatchObject({ status: "ready" });
    for (let turn = 0; turn < 20; turn += 1) await Promise.resolve();
    expect(worker.requests.filter(({ type }) => type === "frame")).toHaveLength(
      reads,
    );
    stop();
    vi.useRealTimers();
    await client.close();
  });

  it("keeps a resource that renders keep asking for, and one whose read is in flight", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const retried = client.db.events.query().where(eq("level", "retried"));
    const first = retried.subscribe();
    await first.promise;
    for (let render = 0; render < 3; render += 1) {
      vi.advanceTimersByTime(GRACE_MS - 1);
      expect(retried.subscribe()).toBe(first);
    }
    worker.holdQueries = true;
    const slow = client.db.events.query().where(eq("level", "slow"));
    const reading = slow.subscribe();
    const read = reading.promise;
    vi.advanceTimersByTime(GRACE_MS * 2);
    expect(slow.subscribe()).toBe(reading);
    worker.holdQueries = false;
    worker.releaseHeld();
    await read;
    expect(slow.subscribe()).toBe(reading);
    vi.useRealTimers();
    await client.close();
  });
});
