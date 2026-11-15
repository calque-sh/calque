import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { stubBrowserFeatures } from "../browser.fixture.js";
import { eq } from "../facade/predicates.js";
import {
  RELEASED_SUBSCRIPTIONS,
  UNLISTENED_GRACE_MS as GRACE_MS,
} from "./query-subscription.js";
import {
  makeClient,
  makePublishingClient,
  settled,
  until,
} from "./session.fixture.js";

describe("subscriptions no component listens to", () => {
  beforeEach(() => {
    stubBrowserFeatures();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("keeps only the newest released resources of renders React discarded", async () => {
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
    const forgotten = discarded.length - RELEASED_SUBSCRIPTIONS;
    for (const { query, resource } of discarded.slice(0, forgotten))
      expect(query.subscribe()).not.toBe(resource);
    for (const { query, resource } of discarded.slice(forgotten))
      expect(query.subscribe()).toBe(resource);
    vi.useRealTimers();
    await client.close();
  });

  it("gives a released resource back with its rows and reads again only once they are stale", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const query = client.db.events.query().where(eq("level", "error"));
    const resource = query.subscribe();
    await resource.promise;
    const reads = () =>
      worker.requests.filter(({ type }) => type === "frame").length;
    const first = reads();
    vi.advanceTimersByTime(GRACE_MS * 2);
    expect(query.subscribe()).toBe(resource);
    expect(resource.getSnapshot()).toMatchObject({ status: "ready" });
    const stop = resource.subscribe(() => undefined);
    for (let turn = 0; turn < 20; turn += 1) await Promise.resolve();
    expect(reads()).toBe(first);
    stop();
    vi.useRealTimers();
    await client.close();
  });

  it("reads a released resource again when a publication moved its rows meanwhile", async () => {
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const query = client.db.events.query().where(eq("level", "error"));
    const resource = query.subscribe();
    await resource.promise;
    vi.advanceTimersByTime(GRACE_MS * 2);
    vi.useRealTimers();
    const reads = () =>
      worker.requests.filter(({ type }) => type === "frame").length;
    const first = reads();
    await publish();
    expect(reads()).toBe(first);
    expect(query.subscribe()).toBe(resource);
    const stop = resource.subscribe(() => undefined);
    await until(() => reads() === first + 1, "the read of the stale rows");
    stop();
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
