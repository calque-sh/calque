import type { LiveQueryResource } from "@calque/client";
import { act, cleanup, render, screen } from "@testing-library/react";
import { Suspense } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { stubBrowserFeatures } from "../../client/src/browser.fixture.js";
import { UNLISTENED_GRACE_MS } from "../../client/src/columnar/query-subscription.js";
import {
  makeClient,
  settled,
  type EventRow,
} from "../../client/src/columnar/session.fixture.js";
import { eq } from "../../client/src/facade/predicates.js";
import { SyncProvider, useSuspenseLiveQuery } from "../src/index.js";

/** Polls without timers, which these tests have faked. */
async function until(done: () => boolean): Promise<void> {
  for (let turn = 0; turn < 500 && !done(); turn += 1)
    await act(
      () =>
        new Promise<void>((resolve) => {
          const channel = new MessageChannel();
          channel.port1.onmessage = () => resolve();
          channel.port2.postMessage(null);
        }),
    );
  expect(done()).toBe(true);
}

describe("useSuspenseLiveQuery against a real client", () => {
  beforeEach(() => {
    stubBrowserFeatures();
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("takes its settled rows back when React retries after the grace period", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });

    const query = client.db.events.query().where(eq("level", "error"));
    const resources = new Set<LiveQueryResource<readonly EventRow[]>>();
    const source = {
      subscribe: () => {
        const resource = query.subscribe();
        resources.add(resource);
        return resource;
      },
    };
    const slow = Promise.withResolvers<undefined>();
    let slowDone = false;
    slow.promise.then(() => {
      slowDone = true;
    });

    function Rows() {
      return <div>{`rows:${useSuspenseLiveQuery(source).result.length}`}</div>;
    }
    function Sibling() {
      if (!slowDone) throw slow.promise;
      return <div>sibling</div>;
    }

    render(
      <SyncProvider client={client}>
        <Suspense fallback={<div>pending</div>}>
          <Rows />
          <Sibling />
        </Suspense>
      </SyncProvider>,
    );
    expect(screen.getByText("pending")).toBeTruthy();
    const reads = () =>
      worker.requests.filter(({ type }) => type === "frame").length;
    await until(() => screen.queryByText("pending") !== null && reads() === 1);
    await until(() => query.subscribe().getSnapshot().status === "ready");

    vi.advanceTimersByTime(UNLISTENED_GRACE_MS * 2);
    await act(async () => {
      slow.resolve(undefined);
      await slow.promise;
    });

    await until(() => screen.queryByText("sibling") !== null);
    expect(screen.getByText("rows:1")).toBeTruthy();
    expect(resources.size).toBe(1);
    expect(reads()).toBe(1);
    cleanup();
    vi.useRealTimers();
    await client.close();
  });
});
