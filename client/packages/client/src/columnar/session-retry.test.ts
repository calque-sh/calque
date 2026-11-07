import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { eq } from "../facade/predicates.js";
import { plan, until, makeClient, settled } from "./session.fixture.js";

/** The bootstrap's answer while the artifact it would plan is being published. */
function preparing(retryAfterSeconds: number): Response {
  return new Response(
    JSON.stringify({
      code: "ARTIFACT_PREPARING",
      message:
        "this authorization generation's artifact is still being published",
      resnapshotRequired: false,
    }),
    {
      status: 503,
      headers: {
        "content-type": "application/json",
        "retry-after": String(retryAfterSeconds),
      },
    },
  );
}

describe("direct-columnar session", () => {
  beforeEach(() => {
    vi.stubGlobal("navigator", {
      userAgentData: { brands: [{ brand: "Chromium" }] },
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    // The rebuild-window tests stub `Date.now`; a leaked stub would silently
    // decide the budget in every test after them.
    vi.restoreAllMocks();
  });

  it("retries a bootstrap that got no answer, and never one the server answered", async () => {
    // A service that is not listening yet, or is between restarts behind a
    // proxy, makes `fetch` reject, which the transport reports as
    // NETWORK_ERROR and the bootstrap retries. Anything the server actually
    // said is final.
    let refusals = 0;
    const { client, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (String(input).endsWith("/v1/bootstrap") && refusals < 2) {
          refusals += 1;
          throw new TypeError("Failed to fetch");
        }
        return inner(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(refusals).toBe(2);
    expect(
      fetcher.mock.calls.filter(([input]) =>
        String(input).endsWith("/v1/bootstrap"),
      ),
    ).toHaveLength(3);
    await client.close();

    // A server that answered 500 is a decision, not a missing listener: the
    // client reports it without a second request.
    const refused = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) =>
        String(input).endsWith("/v1/bootstrap")
          ? new Response("no", { status: 500 })
          : inner(input, init),
    );
    expect(await settled(refused.client)).toMatchObject({ state: "error" });
    expect(
      refused.fetcher.mock.calls.filter(([input]) =>
        String(input).endsWith("/v1/bootstrap"),
      ),
    ).toHaveLength(1);
    await refused.client.close();
  });

  it("client_reports_preparing_and_activates_no_query_until_ready", async () => {
    // After a revocation the server answers 503 ARTIFACT_PREPARING with a
    // bounded Retry-After until the new generation's artifact is published.
    // While the client waits, the application sees that it is waiting and
    // nothing else: no engine is opened, no query resolves, and no collection
    // is partially visible.
    let preparingAnswers = 0;
    const { client, worker, workerFactory, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (String(input).endsWith("/v1/bootstrap") && preparingAnswers < 2) {
          preparingAnswers += 1;
          return preparing(1);
        }
        return inner(input, init);
      },
    );
    const observed: string[] = [];
    const stop = client.subscribe(() => observed.push(client.status.state));

    await until(
      () => client.status.state === "preparing",
      "the preparing status",
    );

    // A query asked for while preparing does not resolve, does not fail, and
    // does not reach an engine: there is no engine.
    let settledQuery = false;
    const rows = client.db.events
      .query()
      .where(eq("level", "error"))
      .run()
      .then((value) => {
        settledQuery = true;
        return value;
      });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(settledQuery).toBe(false);
    expect(client.status).toEqual({ state: "preparing" });
    expect(workerFactory).not.toHaveBeenCalled();
    expect(worker.requests).toEqual([]);

    // The same query resolves when the artifact is published, against the
    // artifact -- one activation, one set of rows, no partial answer before it.
    expect(await rows).toEqual([{ id: 1n, level: "error" }]);
    expect(client.status).toEqual({ state: "ready" });
    expect(preparingAnswers).toBe(2);
    expect(
      fetcher.mock.calls.filter(([input]) =>
        String(input).endsWith("/v1/bootstrap"),
      ),
    ).toHaveLength(3);
    // The application saw the wait, and saw it before readiness.
    expect(observed.indexOf("preparing")).toBeGreaterThanOrEqual(0);
    expect(observed.indexOf("preparing")).toBeLessThan(
      observed.lastIndexOf("ready"),
    );
    stop();
    await client.close();
  }, 20_000);

  it("bounds a preparing wait by wall time when the server holds each request open", async () => {
    // A server building a tenant's first working set holds the request open
    // before answering preparing, so the Retry-After values alone undercount
    // the wait.
    const now = Date.now.bind(Date);
    let held = 0;
    vi.spyOn(Date, "now").mockImplementation(() => now() + held);
    let bootstraps = 0;
    const { client } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (!String(input).endsWith("/v1/bootstrap")) return inner(input, init);
        bootstraps += 1;
        held += 45_000;
        return preparing(1);
      },
    );
    for (let waited = 0; client.status.state !== "error"; waited += 50) {
      if (waited > 10_000) throw new Error("the client never gave up");
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    expect(bootstraps).toBe(4);
    await client.close();
  }, 20_000);

  it("reports no preparing status for a bootstrap answered after the client closed", async () => {
    const answer: PromiseWithResolvers<void> = Promise.withResolvers();
    let bootstraps = 0;
    const { client } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (!String(input).endsWith("/v1/bootstrap")) return inner(input, init);
        bootstraps += 1;
        await answer.promise;
        return preparing(1);
      },
    );
    await until(() => bootstraps === 1, "the bootstrap request");
    const observed: string[] = [];
    client.subscribe(() => observed.push(client.status.state));
    await client.close();
    answer.resolve();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(client.status).toEqual({ state: "closed" });
    expect(observed).not.toContain("preparing");
    expect(bootstraps).toBe(1);
  });

  it("sends no further bootstrap once the client closes during a preparing wait", async () => {
    let bootstraps = 0;
    const { client } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (!String(input).endsWith("/v1/bootstrap")) return inner(input, init);
        bootstraps += 1;
        return preparing(1);
      },
    );
    await until(
      () => client.status.state === "preparing",
      "the preparing status",
    );
    await client.close();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(bootstraps).toBe(1);
    expect(client.status).toEqual({ state: "closed" });
  });

  it("abandons the bootstrap retry schedule when the client closed during a delay", async () => {
    // The schedule spans up to 9.75 s. A client closed inside one of its
    // delays must not send the request the delay was waiting to send: the
    // authorization snapshot it would carry belongs to a session that is
    // gone, and the answer has no mount to open.
    let attempts = 0;
    const { client } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (String(input).endsWith("/v1/bootstrap")) {
          attempts += 1;
          throw new TypeError("Failed to fetch");
        }
        return inner(input, init);
      },
    );
    for (let attempt = 0; attempts === 0 && attempt < 200; attempt += 1)
      await new Promise((resolve) => setTimeout(resolve, 5));
    expect(attempts).toBe(1);
    await client.close();
    // Well past the schedule's first delay (250 ms).
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(attempts).toBe(1);
    expect(client.status).toEqual({ state: "closed" });
  });

  it("does not open a retry delay for a bootstrap whose client already closed", async () => {
    // The other half of the same guard: a refusal that lands after the client
    // closed must not start the schedule's timer at all, so a `close()` is not
    // held behind up to 3 s of sleeping before the request is abandoned.
    const delays: number[] = [];
    const realSetTimeout = globalThis.setTimeout;
    vi.stubGlobal("setTimeout", ((
      handler: TimerHandler,
      timeout?: number,
      ...rest: unknown[]
    ) => {
      if (timeout !== undefined) delays.push(timeout);
      return realSetTimeout(handler as () => void, timeout, ...rest);
    }) as typeof globalThis.setTimeout);
    let attempts = 0;
    let refuse!: (error: unknown) => void;
    const { client } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (String(input).endsWith("/v1/bootstrap")) {
          attempts += 1;
          if (attempts === 1)
            return await new Promise<Response>((_resolve, reject) => {
              refuse = reject;
            });
          throw new TypeError("Failed to fetch");
        }
        return inner(input, init);
      },
    );
    for (let attempt = 0; attempts === 0 && attempt < 200; attempt += 1)
      await new Promise((resolve) => realSetTimeout(resolve, 5));
    expect(attempts).toBe(1);
    await client.close();
    refuse(new TypeError("Failed to fetch"));
    await new Promise((resolve) => realSetTimeout(resolve, 50));
    expect(attempts).toBe(1);
    expect(delays).not.toContain(250);
  });
});
