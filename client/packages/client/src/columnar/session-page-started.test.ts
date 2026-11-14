import { fromBinary } from "@bufbuild/protobuf";
import { mutation } from "@calque/protocol";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  type MockInstance,
  vi,
} from "vitest";

import { renderBootstrap } from "../bootstrap-render.js";
import {
  type BootstrapParameters,
  type BootstrapStart,
  startBootstrap,
  startedAuth,
  takeStartedBootstrap,
} from "../bootstrap-start.js";
import { type SyncClient } from "../client.js";
import {
  type Collections,
  encoder,
  plan,
  controllableLongPoll,
  until,
  makeClient,
  planAt,
  makeRebuildableClient,
  settled,
} from "./session.fixture.js";
import {
  stubBrowserFeatures,
  stubWebAssemblyPolicy,
} from "../browser.fixture.js";

describe("a bootstrap request the page started", () => {
  /** What `makeClient` would send itself, but for the device. */
  const ANY_DEVICE = {
    endpoint: "https://sync.test/root",
    bearerToken: "token",
    requiredTables: ["public.issues", "public.events"],
    memoryBudgetBytes: 32 * 1024 * 1024,
    clientHeapReserveBytes: 4 * 1024 * 1024,
  } satisfies BootstrapStart;
  /** What `makeClient` would send itself. */
  const MATCHING = {
    ...ANY_DEVICE,
    deviceId: "device-a",
  } satisfies BootstrapStart;

  function jsonResponse(value: unknown, status = 200): Response {
    return new Response(JSON.stringify(value), {
      status,
      headers: { "content-type": "application/json" },
    });
  }

  /** The page's global fetch, which only the started request goes through. */
  function stubStarterFetch(answer: () => Response | Promise<Response>) {
    const starter = vi.fn<typeof fetch>(async () => answer());
    vi.stubGlobal("fetch", starter);
    return starter;
  }

  /**
   * A plan whose artifact no plan of the session's own names, answered with a
   * body that records being cancelled. A session that adopted it would read
   * it whole and fail on the artifact, so a refusal is the only way to ready.
   */
  function refusedStartedResponse() {
    const value = plan();
    value.planId = "plan-started";
    const artifact = (value.artifacts as Record<string, unknown>[])[0];
    if (!artifact) throw new Error("fixture artifact missing");
    (artifact.selectedRepresentation as Record<string, unknown>).downloadUrl =
      "https://sync.test/artifact-started";
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(JSON.stringify(value)));
        controller.close();
      },
      cancel() {
        cancelled = true;
      },
    });
    return {
      response: new Response(body, {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
      get cancelled() {
        return cancelled;
      },
    };
  }

  function sentInstanceId(call: Parameters<typeof fetch> | undefined): string {
    const body = JSON.parse(String(call?.[1]?.body)) as {
      clientInstanceId: string;
    };
    return body.clientInstanceId;
  }

  function ownBootstraps(fetcher: ReturnType<typeof makeClient>["fetcher"]) {
    return fetcher.mock.calls.filter(([input]) =>
      String(input).endsWith("/v1/bootstrap"),
    );
  }

  function adoptedMarks(): number {
    return performance.getEntriesByName("calque:bootstrap:adopted", "mark")
      .length;
  }

  function refusedMarks(): number {
    return performance.getEntriesByName("calque:bootstrap:refused", "mark")
      .length;
  }

  let warn: MockInstance<typeof console.warn>;

  beforeEach(() => {
    stubBrowserFeatures();
    performance.clearMarks();
    warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  });

  afterEach(() => {
    // A test that left a request in the slot would hand it to the next one.
    takeStartedBootstrap();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("adopts a matching request, binds its plan, and carries its clientInstanceId on change requests and mutations", async () => {
    const starter = stubStarterFetch(() =>
      jsonResponse({ ...plan(), planId: "plan-started" }),
    );
    startBootstrap(MATCHING);
    expect(starter).toHaveBeenCalledTimes(1);
    const poll = controllableLongPoll();
    const { client, worker, fetcher } = makeClient(plan(), poll.fetcher);
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(takeStartedBootstrap()).toBeUndefined();

    expect(String(starter.mock.calls[0]?.[0])).toBe(
      "https://sync.test/root/v1/bootstrap",
    );
    const startedId = sentInstanceId(starter.mock.calls[0]);
    expect(ownBootstraps(fetcher)).toHaveLength(0);
    expect(worker.requests[0]?.changeStream).toMatchObject({
      planId: "plan-started",
    });
    expect(adoptedMarks()).toBe(1);
    expect(warn).not.toHaveBeenCalled();

    await until(() => poll.open === 1, "the first poll");
    expect(poll.requests[0]?.clientInstanceId).toBe(startedId);
    await client.db.events.update({ id: 1n }, { level: "warn" });
    const submitted = fetcher.mock.calls.find(([input]) =>
      String(input).endsWith("/v1/mutations"),
    );
    expect(
      fromBinary(
        mutation.MutationBatchRequestSchema,
        new Uint8Array(submitted?.[1]?.body as ArrayBuffer),
      ).clientInstanceId,
    ).toBe(startedId);
    await client.close();
  });

  /** The `calque-plan` element's text in the head `renderBootstrap` wrote. */
  function renderedPlanText(head: string): string {
    const element =
      /<script type="application\/json" id="calque-plan">(.*?)<\/script>/su.exec(
        head,
      )?.[1];
    if (element === undefined) throw new Error("no calque-plan element");
    return element;
  }

  /**
   * `plan()` with each artifact's zstd object selected, as the server
   * answers a request whose `Accept-Encoding` names zstd: `renderBootstrap`
   * renders no plan that selects an identity one.
   */
  function renderablePlan(): Record<string, unknown> {
    const rendered = plan();
    for (const artifact of rendered.artifacts as Record<string, unknown>[])
      artifact.selectedRepresentation = {
        ...(artifact.selectedRepresentation as Record<string, unknown>),
        encoding: "zstd",
      };
    return { ...rendered, planId: "plan-rendered" };
  }

  /** `inner`, with the artifact sent as the browser hands over a zstd one. */
  function zstdArtifact(inner: typeof fetch): typeof fetch {
    return async (input, init) => {
      const response = await inner(input, init);
      if (!String(input).endsWith("/artifact-1")) return response;
      const headers = new Headers(response.headers);
      headers.set("content-encoding", "zstd");
      return new Response(response.body, { status: response.status, headers });
    };
  }

  it("adopts a plan the backend rendered, posts nothing, and carries the backend's clientInstanceId on change requests and mutations", async () => {
    // The backend names the decoding list, which a page where WebAssembly
    // compiles names.
    const backend = vi.fn<typeof fetch>(async () =>
      jsonResponse(renderablePlan()),
    );
    const head = await renderBootstrap(MATCHING, {
      acceptEncoding: "gzip, deflate, br, zstd",
      fetch: backend,
    });
    expect(backend).toHaveBeenCalledTimes(1);
    const renderedId = sentInstanceId(backend.mock.calls[0]);
    const starter = stubStarterFetch(() => {
      throw new Error("the page posted a bootstrap request");
    });
    startBootstrap({ ...MATCHING, renderedPlan: renderedPlanText(head) });
    expect(starter).not.toHaveBeenCalled();
    const poll = controllableLongPoll();
    const { client, worker, fetcher } = makeClient(
      plan(),
      poll.fetcher,
      undefined,
      zstdArtifact,
    );
    expect(await settled(client)).toEqual({ state: "ready" });

    expect(ownBootstraps(fetcher)).toHaveLength(0);
    expect(worker.requests[0]?.changeStream).toMatchObject({
      planId: "plan-rendered",
    });
    expect(adoptedMarks()).toBe(1);
    expect(warn).not.toHaveBeenCalled();
    await until(() => poll.open === 1, "the first poll");
    expect(poll.requests[0]?.clientInstanceId).toBe(renderedId);
    await client.db.events.update({ id: 1n }, { level: "warn" });
    const submitted = fetcher.mock.calls.find(([input]) =>
      String(input).endsWith("/v1/mutations"),
    );
    expect(
      fromBinary(
        mutation.MutationBatchRequestSchema,
        new Uint8Array(submitted?.[1]?.body as ArrayBuffer),
      ).clientInstanceId,
    ).toBe(renderedId);
    await client.close();
  });

  it("binds a client created without a deviceId to the device of the plan the backend rendered", async () => {
    const backend = vi.fn<typeof fetch>(async () =>
      jsonResponse(renderablePlan()),
    );
    const head = await renderBootstrap(ANY_DEVICE, {
      acceptEncoding: "gzip, deflate, br, zstd",
      fetch: backend,
    });
    const renderedDevice = (
      JSON.parse(String(backend.mock.calls[0]?.[1]?.body)) as {
        deviceId: string;
      }
    ).deviceId;
    expect(renderedDevice).not.toBe("device-a");
    stubStarterFetch(() => {
      throw new Error("the page posted a bootstrap request");
    });
    startBootstrap({ ...ANY_DEVICE, renderedPlan: renderedPlanText(head) });
    const poll = controllableLongPoll();
    const { client, fetcher } = makeClient(
      plan(),
      poll.fetcher,
      undefined,
      zstdArtifact,
      { deviceId: undefined },
    );
    expect(await settled(client)).toEqual({ state: "ready" });

    expect(ownBootstraps(fetcher)).toHaveLength(0);
    expect(adoptedMarks()).toBe(1);
    await until(() => poll.open === 1, "the first poll");
    expect(poll.requests[0]?.deviceId).toBe(renderedDevice);
    await client.db.events.update({ id: 1n }, { level: "warn" });
    const submitted = fetcher.mock.calls.find(([input]) =>
      String(input).endsWith("/v1/mutations"),
    );
    expect(
      fromBinary(
        mutation.MutationBatchRequestSchema,
        new Uint8Array(submitted?.[1]?.body as ArrayBuffer),
      ).deviceId,
    ).toBe(renderedDevice);
    await client.close();
  });

  it("does not adopt a plan the backend rendered where the page cannot compile WebAssembly, and sends its own", async () => {
    // The session names the shorter list.
    stubWebAssemblyPolicy();
    const backend = vi.fn<typeof fetch>(async () =>
      jsonResponse(renderablePlan()),
    );
    const head = await renderBootstrap(MATCHING, {
      acceptEncoding: "gzip, deflate, br, zstd",
      fetch: backend,
    });
    const renderedId = sentInstanceId(backend.mock.calls[0]);
    stubStarterFetch(() => {
      throw new Error("the page posted a bootstrap request");
    });
    startBootstrap({ ...MATCHING, renderedPlan: renderedPlanText(head) });
    const poll = controllableLongPoll();
    const { client, worker, fetcher } = makeClient(plan(), poll.fetcher);
    expect(await settled(client)).toEqual({ state: "ready" });

    const own = ownBootstraps(fetcher);
    expect(own).toHaveLength(1);
    expect(sentInstanceId(own[0])).not.toBe(renderedId);
    expect(adoptedMarks()).toBe(0);
    expect(worker.requests[0]?.changeStream).toMatchObject({
      planId: "plan-1",
    });
    await client.close();
  });

  it("does not adopt a plan the backend rendered for another budget, and sends its own", async () => {
    // The lists agree, so the budget is what differs.
    const backend = vi.fn<typeof fetch>(async () =>
      jsonResponse(renderablePlan()),
    );
    const head = await renderBootstrap(
      { ...MATCHING, memoryBudgetBytes: 64 * 1024 * 1024 },
      { acceptEncoding: "gzip, deflate, br, zstd", fetch: backend },
    );
    const renderedId = sentInstanceId(backend.mock.calls[0]);
    stubStarterFetch(() => {
      throw new Error("the page posted a bootstrap request");
    });
    startBootstrap({ ...MATCHING, renderedPlan: renderedPlanText(head) });
    const poll = controllableLongPoll();
    const { client, worker, fetcher } = makeClient(plan(), poll.fetcher);
    expect(await settled(client)).toEqual({ state: "ready" });

    const own = ownBootstraps(fetcher);
    expect(own).toHaveLength(1);
    expect(sentInstanceId(own[0])).not.toBe(renderedId);
    expect(adoptedMarks()).toBe(0);
    expect(worker.requests[0]?.changeStream).toMatchObject({
      planId: "plan-1",
    });
    await until(() => poll.open === 1, "the first poll");
    expect(poll.requests[0]?.clientInstanceId).toBe(sentInstanceId(own[0]));
    await client.close();
  });

  /** The warning a refusal logs, which names the parameter that differs. */
  function refusal(parameter: keyof BootstrapParameters): string {
    return `@calque/client: the page started a bootstrap request whose ${parameter} differs from the client's, so the client sends its own`;
  }

  const postedDiffering: readonly (readonly [
    keyof BootstrapParameters,
    BootstrapStart,
  ])[] = [
    ["url", { ...MATCHING, endpoint: "https://sync.test/elsewhere" }],
    ["bearerToken", { ...MATCHING, bearerToken: "second-principal" }],
    ["requiredTables", { ...MATCHING, requiredTables: ["public.events"] }],
    ["memoryBudgetBytes", { ...MATCHING, memoryBudgetBytes: 64 * 1024 * 1024 }],
    [
      "clientHeapReserveBytes",
      { ...MATCHING, clientHeapReserveBytes: 8 * 1024 * 1024 },
    ],
    ["deviceId", { ...MATCHING, deviceId: "device-b" }],
  ];

  it.each(postedDiffering)(
    "does not adopt a request whose %s differs: it warns, cancels the body, never binds the plan, and sends its own",
    async (parameter, start) => {
      const started = refusedStartedResponse();
      const starter = stubStarterFetch(() => started.response);
      startBootstrap(start);
      expect(starter).toHaveBeenCalledTimes(1);
      const poll = controllableLongPoll();
      const { client, worker, fetcher } = makeClient(plan(), poll.fetcher);
      expect(await settled(client)).toEqual({ state: "ready" });

      expect(warn).toHaveBeenCalledExactlyOnceWith(refusal(parameter));
      expect(refusedMarks()).toBe(1);
      const own = ownBootstraps(fetcher);
      expect(own).toHaveLength(1);
      const startedId = sentInstanceId(starter.mock.calls[0]);
      const ownId = sentInstanceId(own[0]);
      expect(ownId).not.toBe(startedId);
      expect(adoptedMarks()).toBe(0);
      expect(worker.requests[0]?.changeStream).toMatchObject({
        planId: "plan-1",
      });
      expect(
        fetcher.mock.calls.some(([input]) =>
          String(input).endsWith("/artifact-started"),
        ),
      ).toBe(false);
      await vi.waitFor(() => expect(started.cancelled).toBe(true));
      await until(() => poll.open === 1, "the first poll");
      expect(poll.requests[0]?.clientInstanceId).toBe(ownId);
      await client.close();
    },
  );

  const renderedDiffering: readonly (readonly [
    keyof BootstrapParameters,
    Record<string, unknown>,
  ])[] = [
    ["transportProfile", { transportProfile: "second-profile" }],
    ["protocolVersion", { protocolVersion: 2 }],
    ["acceptedEncodings", { acceptedEncodings: ["identity"] }],
  ];

  it.each(renderedDiffering)(
    "does not adopt a rendered plan whose %s differs: it warns and sends its own",
    async (parameter, change) => {
      const head = await renderBootstrap(MATCHING, {
        acceptEncoding: "gzip, deflate, br, zstd",
        fetch: async () => jsonResponse(renderablePlan()),
      });
      const rendered = JSON.parse(renderedPlanText(head)) as object;
      stubStarterFetch(() => {
        throw new Error("the page posted a bootstrap request");
      });
      startBootstrap({
        ...MATCHING,
        renderedPlan: JSON.stringify({ ...rendered, ...change }),
      });
      const poll = controllableLongPoll();
      const { client, worker, fetcher } = makeClient(plan(), poll.fetcher);
      expect(await settled(client)).toEqual({ state: "ready" });

      expect(warn).toHaveBeenCalledExactlyOnceWith(refusal(parameter));
      expect(refusedMarks()).toBe(1);
      expect(ownBootstraps(fetcher)).toHaveLength(1);
      expect(adoptedMarks()).toBe(0);
      expect(worker.requests[0]?.changeStream).toMatchObject({
        planId: "plan-1",
      });
      await client.close();
    },
  );

  it("is adopted by the first client only; a second client sends its own", async () => {
    stubStarterFetch(() => jsonResponse(plan()));
    startBootstrap(MATCHING);
    const first = makeClient();
    const second = makeClient();
    expect(await settled(first.client)).toEqual({ state: "ready" });
    expect(await settled(second.client)).toEqual({ state: "ready" });
    expect(ownBootstraps(first.fetcher)).toHaveLength(0);
    expect(ownBootstraps(second.fetcher)).toHaveLength(1);
    expect(adoptedMarks()).toBe(1);
    await first.client.close();
    await second.client.close();
  });

  it("is refused for good by a first client that does not match; a second client that would match sends its own", async () => {
    const started = refusedStartedResponse();
    stubStarterFetch(() => started.response);
    startBootstrap({ ...MATCHING, bearerToken: "second-principal" });
    const first = makeClient();
    const second = makeClient();
    expect(await settled(first.client)).toEqual({ state: "ready" });
    expect(await settled(second.client)).toEqual({ state: "ready" });
    expect(ownBootstraps(first.fetcher)).toHaveLength(1);
    expect(ownBootstraps(second.fetcher)).toHaveLength(1);
    expect(adoptedMarks()).toBe(0);
    await vi.waitFor(() => expect(started.cancelled).toBe(true));
    await first.client.close();
    await second.client.close();
  });

  it("does not adopt a request started with a bearer the client's own auth() does not return, and never logs either bearer", async () => {
    const started = refusedStartedResponse();
    const starter = stubStarterFetch(() => started.response);
    startBootstrap({ ...MATCHING, bearerToken: "started-grant" });
    const { client, worker, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      undefined,
      {
        auth: async () => ({ bearerToken: "rotated-grant" }),
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });

    expect(starter.mock.calls[0]?.[1]?.headers).toMatchObject({
      authorization: "Bearer started-grant",
    });
    const own = ownBootstraps(fetcher);
    expect(own).toHaveLength(1);
    expect(own[0]?.[1]?.headers).toMatchObject({
      authorization: "Bearer rotated-grant",
    });
    expect(sentInstanceId(own[0])).not.toBe(
      sentInstanceId(starter.mock.calls[0]),
    );
    expect(warn).toHaveBeenCalledExactlyOnceWith(refusal("bearerToken"));
    expect(JSON.stringify(warn.mock.calls)).not.toContain("grant-secret");
    expect(adoptedMarks()).toBe(0);
    expect(worker.requests[0]?.changeStream).toMatchObject({
      planId: "plan-1",
    });
    await vi.waitFor(() => expect(started.cancelled).toBe(true));
    await client.close();
  });

  it("adopts a request started from the rendered connection under startedAuth, which hands the next activation the refreshed bearer", async () => {
    vi.stubGlobal("document", {
      getElementById: (id: string) =>
        id === "calque-connection"
          ? {
              textContent: JSON.stringify({
                endpoint: "https://sync.test/root",
                bearerToken: "rendered-grant",
              }),
            }
          : null,
    });
    const starter = stubStarterFetch(() => jsonResponse(plan()));
    startBootstrap({
      requiredTables: MATCHING.requiredTables,
      memoryBudgetBytes: MATCHING.memoryBudgetBytes,
      clientHeapReserveBytes: MATCHING.clientHeapReserveBytes,
      deviceId: MATCHING.deviceId,
    });
    expect(starter.mock.calls[0]?.[1]?.headers).toMatchObject({
      authorization: "Bearer rendered-grant",
    });
    const refresh = vi.fn(() => ({ bearerToken: "refreshed-grant" }));
    const { client, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      undefined,
      { auth: startedAuth(refresh) },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(adoptedMarks()).toBe(1);
    expect(warn).not.toHaveBeenCalled();
    expect(ownBootstraps(fetcher)).toHaveLength(0);
    expect(refresh).not.toHaveBeenCalled();

    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    const own = ownBootstraps(fetcher);
    expect(own).toHaveLength(1);
    expect(own[0]?.[1]?.headers).toMatchObject({
      authorization: "Bearer refreshed-grant",
    });
    expect(refresh).toHaveBeenCalled();
    await client.close();
  });

  it("never binds an adopted response that answers after a reset replaced its activation", async () => {
    const stale = plan();
    stale.planId = "plan-started";
    const artifact = (stale.artifacts as Record<string, unknown>[])[0];
    if (!artifact) throw new Error("fixture artifact missing");
    (artifact.selectedRepresentation as Record<string, unknown>).downloadUrl =
      "https://sync.test/artifact-started";
    let answer!: (response: Response) => void;
    stubStarterFetch(
      () =>
        new Promise<Response>((resolve) => {
          answer = resolve;
        }),
    );
    startBootstrap(MATCHING);
    const { client, worker, fetcher } = makeClient();
    await until(() => adoptedMarks() === 1, "the adoption");
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(ownBootstraps(fetcher)).toHaveLength(1);

    // Read or cancelled, the replaced activation is done with it. With no
    // queue the body is pulled only when something reads it.
    let handled = false;
    answer(
      new Response(
        new ReadableStream<Uint8Array>(
          {
            pull(controller) {
              handled = true;
              controller.enqueue(encoder.encode(JSON.stringify(stale)));
              controller.close();
            },
            cancel() {
              handled = true;
            },
          },
          { highWaterMark: 0 },
        ),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );
    await until(() => handled, "the replaced activation to take its answer");
    for (let turn = 0; turn < 5; turn += 1)
      await new Promise((resolve) => setTimeout(resolve, 0));

    expect(client.status).toEqual({ state: "ready" });
    expect(
      worker.requests
        .filter((request) => request.changeStream !== undefined)
        .map((request) => request.changeStream),
    ).toEqual([expect.objectContaining({ planId: "plan-1" })]);
    expect(
      fetcher.mock.calls.some(([input]) =>
        String(input).endsWith("/artifact-started"),
      ),
    ).toBe(false);
    expect(adoptedMarks()).toBe(1);
    await client.close();
  });

  it("is adopted by the first activation only; a reset sends the session's own request", async () => {
    const starter = stubStarterFetch(() => jsonResponse(plan()));
    startBootstrap(MATCHING);
    const { client, fetcher } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(ownBootstraps(fetcher)).toHaveLength(0);
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    const own = ownBootstraps(fetcher);
    expect(own).toHaveLength(1);
    // The session keeps the id it adopted, as it keeps its own across a reset.
    expect(sentInstanceId(own[0])).toBe(sentInstanceId(starter.mock.calls[0]));
    expect(starter).toHaveBeenCalledTimes(1);
    expect(adoptedMarks()).toBe(1);
    await client.close();
  });

  it("is adopted by the first activation only; the rebuild after a revocation sends the session's own request", async () => {
    stubStarterFetch(() => jsonResponse(planAt("cursor-p")));
    startBootstrap(MATCHING);
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
      undefined,
      { bootstrap: () => jsonResponse(planAt("cursor-q")) },
    );
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(harness.bootstraps).toBe(0);
    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(() => workers.length === 2, "the replacement mount");
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(harness.bootstraps).toBe(1);
    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "epoch-two" },
      { id: 2n, level: "epoch-two" },
    ]);
    await client.close();
  });

  const refusals: readonly (readonly [string, () => Response])[] = [
    ["401", () => jsonResponse({ code: "UNAUTHORIZED" }, 401)],
    ["403", () => jsonResponse({ code: "FORBIDDEN" }, 403)],
    [
      "a body over the bootstrap response bound",
      () =>
        new Response("x".repeat(2 * 1024 * 1024 + 1), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    ],
    [
      "invalid JSON",
      () =>
        new Response("{", {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    ],
    ["a plan the protocol refuses", () => jsonResponse({ protocolVersion: 1 })],
    [
      "an expired plan",
      () => jsonResponse({ ...plan(), expiresAt: "2000-01-01T00:00:00Z" }),
    ],
    [
      "a plan whose tables do not bind",
      () => {
        const value = plan();
        const artifact = (value.artifacts as Record<string, unknown>[])[0];
        if (!artifact) throw new Error("fixture artifact missing");
        for (const table of (
          artifact.runtime as { tables: Record<string, unknown>[] }
        ).tables)
          delete table.columns;
        return jsonResponse(value);
      },
    ],
  ];

  it.each(refusals)(
    "refuses an adopted %s exactly as its own, and requests no artifact",
    async (_answer, answer) => {
      const answering =
        (inner: typeof fetch): typeof fetch =>
        async (input, init) =>
          String(input).endsWith("/v1/bootstrap")
            ? answer()
            : inner(input, init);
      const reported = (status: SyncClient<Collections>["status"]) =>
        status.state === "error"
          ? {
              state: status.state,
              code: status.error.code,
              recovery: status.error.recovery,
              message: status.error.message,
            }
          : status;

      const own = makeClient(plan(), undefined, undefined, answering);
      const ownStatus = reported(await settled(own.client));
      expect(ownStatus).toMatchObject({ state: "error" });

      stubStarterFetch(answer);
      startBootstrap(MATCHING);
      const adopted = makeClient(plan(), undefined, undefined, answering);
      expect(reported(await settled(adopted.client))).toEqual(ownStatus);
      expect(adoptedMarks()).toBe(1);
      expect(ownBootstraps(adopted.fetcher)).toHaveLength(
        ownBootstraps(own.fetcher).length - 1,
      );
      for (const { fetcher } of [own, adopted])
        expect(
          fetcher.mock.calls.some(([input]) =>
            String(input).includes("/artifact"),
          ),
        ).toBe(false);
      await own.client.close();
      await adopted.client.close();
    },
  );

  it("sends the session's own request for the retry an adopted expired plan asks for", async () => {
    stubStarterFetch(() =>
      jsonResponse({ ...plan(), expiresAt: "2000-01-01T00:00:00Z" }),
    );
    startBootstrap(MATCHING);
    const { client, fetcher } = makeClient();
    const status = await settled(client);
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.message).toBe("bootstrap plan expired");
    expect(status.error.recovery).toBe("retry");
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(ownBootstraps(fetcher)).toHaveLength(1);
    await client.close();
  });

  it("waits out an adopted preparing answer, then sends the session's own request", async () => {
    const starter = stubStarterFetch(
      () =>
        new Response(
          JSON.stringify({
            code: "ARTIFACT_PREPARING",
            message: "the registered projection has no published artifact yet",
            resnapshotRequired: false,
          }),
          {
            status: 503,
            headers: {
              "content-type": "application/json",
              "retry-after": "1",
            },
          },
        ),
    );
    startBootstrap(MATCHING);
    const { client, fetcher } = makeClient();
    await until(() => client.status.state === "preparing", "preparing");
    expect(ownBootstraps(fetcher)).toHaveLength(0);
    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "error" },
    ]);
    expect(client.status).toEqual({ state: "ready" });
    expect(ownBootstraps(fetcher)).toHaveLength(1);
    expect(starter).toHaveBeenCalledTimes(1);
    await client.close();
  }, 20_000);

  it("retries an adopted request that failed to send with the session's own, on the existing schedule", async () => {
    let failedAt = 0;
    const starter = stubStarterFetch(() => {
      failedAt = performance.now();
      return Promise.reject(new TypeError("fetch failed"));
    });
    startBootstrap(MATCHING);
    let sentAt = 0;
    const { client, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => (input, init) => {
        if (String(input).endsWith("/v1/bootstrap")) sentAt = performance.now();
        return inner(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const own = ownBootstraps(fetcher);
    expect(own).toHaveLength(1);
    // The first delay of BOOTSTRAP_RETRY_DELAYS_MS, 250 ms, less timer slack.
    expect(sentAt - failedAt).toBeGreaterThanOrEqual(240);
    // Adoption took the started id before the request failed.
    expect(sentInstanceId(own[0])).toBe(sentInstanceId(starter.mock.calls[0]));
    expect(adoptedMarks()).toBe(1);
    await client.close();
  });
});
