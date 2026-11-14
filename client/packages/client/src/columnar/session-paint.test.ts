import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { SyncPublication } from "../client.js";
import {
  controllableLongPoll,
  makeClient,
  plan,
  settled,
  until,
} from "./session.fixture.js";
import { stubBrowserFeatures } from "../browser.fixture.js";

/** A visible page whose animation frames run only when the test paints. */
function stubPage() {
  const frames: FrameRequestCallback[] = [];
  const visibilityListeners = new Set<() => void>();
  const page = {
    visibilityState: "visible",
    addEventListener: (_type: string, listener: () => void) =>
      visibilityListeners.add(listener),
    removeEventListener: (_type: string, listener: () => void) =>
      visibilityListeners.delete(listener),
  };
  vi.stubGlobal("document", page);
  vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) =>
    frames.push(callback),
  );
  return {
    get frames() {
      return frames.length;
    },
    async paint() {
      for (const frame of frames.splice(0)) frame(performance.now());
      await new Promise((resolve) => setTimeout(resolve, 0));
    },
    hide() {
      page.visibilityState = "hidden";
      for (const listener of [...visibilityListeners]) listener();
    },
  };
}

/**
 * A client with one listened query, and a write the query's re-read has put
 * on the page, awaiting its echo on the open poll.
 */
async function shownWrite() {
  const poll = controllableLongPoll();
  const { client, worker } = makeClient(plan(), poll.fetcher);
  expect(await settled(client)).toEqual({ state: "ready" });
  const resource = client.db.events.query().subscribe();
  resource.subscribe(() => undefined);
  await resource.promise;
  const frames = () =>
    worker.requests.filter((request) => request.type === "frame").length;
  const { mutationId } = await client.db.events.update(
    { id: 1n },
    { level: "warn" },
  );
  await until(() => frames() === 2, "the optimistic re-read");
  await until(
    () =>
      poll.open === 1 &&
      (poll.requests.at(-1)?.pendingMutationIds.includes(mutationId) ?? false),
    "the poll naming the write",
  );
  const published: SyncPublication[] = [];
  client.subscribePublications((publication) => published.push(publication));
  /** Answers the open poll, and waits for the poll that follows its batch. */
  const echo = async () => {
    const sent = poll.requests.length;
    poll.answer();
    await until(() => poll.requests.length > sent, "the next poll");
  };
  return {
    client,
    worker,
    poll,
    frames,
    echo,
    published,
    state: () => client.mutationStatus(mutationId)?.state,
  };
}

describe("a write's confirmation and the paint that shows it", () => {
  beforeEach(() => {
    stubBrowserFeatures();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("confirms a write the page shows once the browser has painted it", async () => {
    const page = stubPage();
    const { client, echo, published, state } = await shownWrite();
    expect(page.frames).toBe(1);
    await echo();
    expect(state()).toBe("awaiting_canonical_echo");
    expect(published).toEqual([]);
    await page.paint();
    expect(state()).toBe("canonical");
    expect(published).toHaveLength(1);
    await client.close();
  });

  it("holds a publication behind a held one, and adopts both in order", async () => {
    const page = stubPage();
    const { client, echo, published, state } = await shownWrite();
    await echo();
    await echo();
    expect(published).toEqual([]);
    await page.paint();
    expect(state()).toBe("canonical");
    expect(published.map(({ cursor }) => cursor)).toEqual([
      "cursor-p",
      "cursor-p",
    ]);
    await client.close();
  });

  it("adopts at once a batch that confirms no write the page is painting", async () => {
    stubPage();
    const { client, worker, echo, published, state } = await shownWrite();
    worker.echo = [];
    await echo();
    expect(published).toHaveLength(1);
    expect(state()).toBe("awaiting_canonical_echo");
    await client.close();
  });

  it("holds the echo of a write a second re-read put on the page before the paint", async () => {
    const page = stubPage();
    const { client, worker, poll, frames, echo, published } =
      await shownWrite();
    const { mutationId } = await client.db.events.update(
      { id: 1n },
      { level: "error" },
    );
    await until(() => frames() === 3, "the second write's re-read");
    await until(
      () =>
        poll.open === 1 &&
        (poll.requests.at(-1)?.pendingMutationIds.includes(mutationId) ??
          false),
      "the poll naming the second write",
    );
    expect(page.frames).toBe(1);
    worker.echo = [mutationId];
    await echo();
    expect(client.mutationStatus(mutationId)?.state).toBe(
      "awaiting_canonical_echo",
    );
    expect(published).toEqual([]);
    await page.paint();
    expect(client.mutationStatus(mutationId)?.state).toBe("canonical");
    await client.close();
  });

  it("does not hold an echo that arrives after the paint", async () => {
    const page = stubPage();
    const { client, echo, state } = await shownWrite();
    await page.paint();
    await echo();
    expect(state()).toBe("canonical");
    await client.close();
  });

  it("does not hold an echo on a hidden page", async () => {
    const page = stubPage();
    page.hide();
    const { client, echo, state } = await shownWrite();
    expect(page.frames).toBe(0);
    await echo();
    expect(state()).toBe("canonical");
    await client.close();
  });

  it("confirms a held write once the page is hidden", async () => {
    const page = stubPage();
    const { client, echo, state } = await shownWrite();
    await echo();
    expect(state()).toBe("awaiting_canonical_echo");
    page.hide();
    expect(state()).toBe("canonical");
    await client.close();
  });

  it("confirms a held write after a second without a paint", async () => {
    stubPage();
    const { client, echo, state } = await shownWrite();
    await echo();
    expect(state()).toBe("awaiting_canonical_echo");
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect(state()).toBe("canonical");
    await client.close();
  });

  it("confirms a held write when the client closes", async () => {
    stubPage();
    const { client, echo, state } = await shownWrite();
    await echo();
    expect(state()).toBe("awaiting_canonical_echo");
    await client.close();
    expect(state()).toBe("canonical");
  });

  it("confirms a held write when the mount is torn down, rather than abandoning it", async () => {
    stubPage();
    const { client, echo, state } = await shownWrite();
    await echo();
    const reset = client.reset();
    expect(state()).toBe("canonical");
    await reset;
    await client.close();
  });
});
