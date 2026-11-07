import { createHash } from "node:crypto";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  acceptedArtifactEncodings,
  type BootstrapParameters,
  type BootstrapRequest,
  type BootstrapStart,
  bootstrapUrl,
  brotliDecoderRuns,
  differingBootstrapParameter,
  discardStartedBootstrap,
  renderedConnection,
  startBootstrap,
  startedAuth,
  takeStartedBootstrap,
} from "./bootstrap-start.js";
import {
  BOOTSTRAP_START_SCRIPT,
  BOOTSTRAP_START_SCRIPT_SHA256,
} from "./bootstrap-start-script.js";
import {
  createGeneratedClientSchema,
  sealSyncClientOptions,
} from "./client-options.js";
import { artifactTransportProfile, requestBootstrapPlan } from "./transport.js";

const MEMORY_BUDGET_BYTES = 32 * 1024 * 1024;

/**
 * The page the tests run on, so an endpoint that is a path has a URL to be
 * read against.
 */
const PAGE = { href: "https://app.test/issues?id=7" };

/** Endpoints `createSyncClient` refuses: no token may go to any of them. */
const REFUSED_ENDPOINTS = [
  "",
  "not a url",
  "http://sync.test/root",
  "ftp://sync.test/root",
  "https://user:secret@sync.test/root",
  "https://user@sync.test/root",
  "https://:secret@sync.test/root",
  "http://127.0.0.2:55602",
  "https://sync.test/root#fragment",
  "https://sync.test/ro\u0007ot",
  "https://sync.test/ro\not",
  `https://sync.test/${"a".repeat(8_192)}`,
  "_calque/",
  // Each names another host, which a path read against the page would not.
  "//sync.test/root",
  "/\\sync.test/root",
];

/** Browsers the client refuses, each made from the Chromium profile. */
const OUTSIDE_PROFILE: readonly (readonly [string, () => void])[] = [
  ["without Chromium's brand", () => vi.stubGlobal("navigator", {})],
  [
    "without BYOB stream readers",
    () => vi.stubGlobal("ReadableStreamBYOBReader", undefined),
  ],
  ["without Response", () => vi.stubGlobal("Response", undefined)],
];

/** Endpoints `createSyncClient` accepts, each read as a directory. */
const ACCEPTED_ENDPOINTS = [
  "https://sync.test/root",
  "https://sync.test/root/",
  "https://sync.test/root?region=eu",
  "http://127.0.0.1:55602",
  "http://localhost:5173/_calque/",
  "/_calque/",
];

/**
 * A `WebAssembly` whose compile throws, as Chrome's does under a
 * Content-Security-Policy without `wasm-unsafe-eval`.
 */
const REFUSING_WEBASSEMBLY = {
  Module: function Module(): never {
    throw new Error("Refused to compile or instantiate WebAssembly module");
  },
};

/** Pages by whether the brotli decoder runs in them, and what each names. */
const DECODER_PAGES = [
  [
    "with workers and WebAssembly",
    () => vi.stubGlobal("Worker", vi.fn()),
    ["packed-brotli-large-window", "brotli-large-window", "zstd", "identity"],
  ],
  [
    "without workers",
    () => vi.stubGlobal("Worker", undefined),
    ["zstd", "identity"],
  ],
  [
    "whose policy forbids compiling WebAssembly",
    () => {
      vi.stubGlobal("Worker", vi.fn());
      vi.stubGlobal("WebAssembly", REFUSING_WEBASSEMBLY);
    },
    ["zstd", "identity"],
  ],
  [
    "without WebAssembly",
    () => {
      vi.stubGlobal("Worker", vi.fn());
      vi.stubGlobal("WebAssembly", undefined);
    },
    ["zstd", "identity"],
  ],
] as const;

const STORED_DEVICE = "22222222-2222-4222-8222-222222222222";

/** `localStorage`, holding `stored` as the device id if given. */
function stubStorage(stored?: string) {
  const items = new Map<string, string>(
    stored === undefined ? [] : [["calque-device-id", stored]],
  );
  const storage = {
    getItem: vi.fn((key: string) => items.get(key) ?? null),
    setItem: vi.fn((key: string, value: string) => {
      items.set(key, value);
    }),
  };
  vi.stubGlobal("localStorage", storage);
  return storage;
}

function sealed(endpoint: string) {
  return sealSyncClientOptions({
    endpoint,
    auth: () => ({ bearerToken: "token" }),
    schema: createGeneratedClientSchema({
      collections: [],
    }),
    memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    deviceId: "device-a",
  });
}

function stubFetch(
  answer: () => Promise<Response> = () => new Promise(() => undefined),
) {
  const fetcher = vi.fn<typeof fetch>(answer);
  vi.stubGlobal("fetch", fetcher);
  return fetcher;
}

function parameters(): BootstrapParameters {
  return {
    url: "https://sync.test/root/v1/bootstrap",
    bearerToken: "token",
    transportProfile: "chromium-native-artifact-v1",
    protocolVersion: 1,
    requiredTables: ["public.events", "public.issues"],
    acceptedEncodings: ["zstd", "identity"],
    memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    clientHeapReserveBytes: 4 * 1024 * 1024,
    deviceId: "device-a",
  };
}

describe("a bootstrap request the page starts", () => {
  beforeEach(() => {
    vi.stubGlobal("navigator", {
      userAgentData: { brands: [{ brand: "Chromium" }] },
    });
    vi.stubGlobal("location", PAGE);
  });

  afterEach(() => {
    takeStartedBootstrap();
    vi.unstubAllGlobals();
  });

  it("posts byte for byte the request the session would post, and leaves it in the slot once", async () => {
    stubStorage(STORED_DEVICE);
    const fetcher = stubFetch();
    startBootstrap({
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: ["public.issues", "public.events"],
      memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    });
    expect(fetcher).toHaveBeenCalledTimes(1);
    const started = takeStartedBootstrap();
    expect(takeStartedBootstrap()).toBeUndefined();
    if (!started) throw new Error("expected a started request");
    expect(Object.isFrozen(started)).toBe(true);
    expect(Object.isFrozen(started.parameters)).toBe(true);
    expect(Object.isFrozen(started.parameters.requiredTables)).toBe(true);

    const options = sealed("https://sync.test/root");
    const own = vi.fn<typeof fetch>(() => new Promise(() => undefined));
    void requestBootstrapPlan({
      fetch: own,
      endpoint: options.endpointUrl,
      bearerToken: "token",
      transport: artifactTransportProfile(),
      requiredTables: ["public.events", "public.issues"],
      memoryBudgetBytes: options.memoryBudgetBytes,
      clientHeapReserveBytes: options.clientHeapReserveBytes,
      clientInstanceId: started.clientInstanceId,
      deviceId: STORED_DEVICE,
    });
    const [startedUrl, startedInit] = fetcher.mock.calls[0] ?? [];
    const [ownUrl, ownInit] = own.mock.calls[0] ?? [];
    expect(String(startedUrl)).toBe(String(ownUrl));
    expect(startedInit).toEqual(ownInit);
    expect(started.parameters).toEqual({
      ...parameters(),
      clientHeapReserveBytes: options.clientHeapReserveBytes,
      deviceId: STORED_DEVICE,
    });
  });

  it("names the device the page names, else the one the browser keeps, which it creates once", () => {
    const storage = stubStorage();
    stubFetch();
    const start = {
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: ["public.events"],
    };
    startBootstrap({ ...start, deviceId: "device-z" });
    expect(takeStartedBootstrap()?.parameters.deviceId).toBe("device-z");
    expect(storage.setItem).not.toHaveBeenCalled();

    startBootstrap(start);
    const created = takeStartedBootstrap()?.parameters.deviceId;
    expect(created).toMatch(/^[0-9a-f-]{36}$/u);
    expect(storage.setItem).toHaveBeenCalledExactlyOnceWith(
      "calque-device-id",
      created,
    );
    startBootstrap(start);
    expect(takeStartedBootstrap()?.parameters.deviceId).toBe(created);
    expect(storage.setItem).toHaveBeenCalledOnce();
  });

  it("defaults the memory budget as the client does", () => {
    stubFetch();
    startBootstrap({
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: ["public.events"],
    });
    const started = takeStartedBootstrap();
    const client = sealSyncClientOptions({
      endpoint: "https://sync.test/root",
      auth: () => ({ bearerToken: "token" }),
      schema: createGeneratedClientSchema({
        collections: [],
      }),
      deviceId: "device-a",
    });
    expect(started?.parameters).toMatchObject({
      memoryBudgetBytes: client.memoryBudgetBytes,
      clientHeapReserveBytes: client.clientHeapReserveBytes,
    });
  });

  it("starts nothing while the slot is full", () => {
    const fetcher = stubFetch();
    for (const bearerToken of ["token", "second-token"])
      startBootstrap({
        endpoint: "https://sync.test/root",
        bearerToken,
        requiredTables: [],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(takeStartedBootstrap()?.parameters.bearerToken).toBe("token");
  });

  it.each(OUTSIDE_PROFILE)("starts nothing in a browser %s", (_, outside) => {
    const fetcher = stubFetch();
    outside();
    startBootstrap({
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: [],
      memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    });
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()).toBeUndefined();
  });

  it.each(REFUSED_ENDPOINTS)(
    "sends no token to %j, an endpoint createSyncClient refuses",
    (endpoint) => {
      expect(() => sealed(endpoint)).toThrow(TypeError);
      const fetcher = stubFetch();
      startBootstrap({
        endpoint,
        bearerToken: "token",
        requiredTables: [],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      });
      expect(fetcher).not.toHaveBeenCalled();
      expect(takeStartedBootstrap()).toBeUndefined();
    },
  );

  it.each(ACCEPTED_ENDPOINTS)(
    "posts to the URL the session would post to for %j",
    (endpoint) => {
      const fetcher = stubFetch();
      startBootstrap({
        endpoint,
        bearerToken: "token",
        requiredTables: [],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      });
      const expected = bootstrapUrl(sealed(endpoint).endpointUrl).href;
      expect(String(fetcher.mock.calls[0]?.[0])).toBe(expected);
      expect(takeStartedBootstrap()?.parameters.url).toBe(expected);
    },
  );

  it("reads an endpoint that is a path against the page's URL", () => {
    stubFetch();
    startBootstrap({
      endpoint: "/_calque/",
      bearerToken: "token",
      requiredTables: [],
    });
    expect(takeStartedBootstrap()?.parameters.url).toBe(
      "https://app.test/_calque/v1/bootstrap",
    );
  });

  it("sends nothing to an endpoint that is a path outside a page", () => {
    vi.stubGlobal("location", undefined);
    const fetcher = stubFetch();
    startBootstrap({
      endpoint: "/_calque/",
      bearerToken: "token",
      requiredTables: [],
    });
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()).toBeUndefined();
  });

  it("leaves a fetch that throws as a rejected response, and no unhandled rejection", async () => {
    const failure = new TypeError("invalid header value");
    stubFetch(() => {
      throw failure;
    });
    startBootstrap({
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: [],
      memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    });
    const started = takeStartedBootstrap();
    await expect(started?.response).rejects.toBe(failure);
  });

  it("leaves the request in a slot a script the bundle shares no module with can fill", () => {
    stubFetch();
    startBootstrap({
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: [],
      memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    });
    const slot = Symbol.for("@calque/client:started-bootstrap");
    const value = (globalThis as Record<symbol, unknown>)[slot];
    expect(takeStartedBootstrap()).toBe(value);
    expect(Object.getOwnPropertySymbols(globalThis)).not.toContain(slot);
  });

  it.each([
    ["a string", "started"],
    ["null", null],
    ["no response", { parameters: parameters(), clientInstanceId: "id" }],
    [
      "a response that is not a promise",
      {
        parameters: parameters(),
        clientInstanceId: "id",
        response: new Response("{}"),
      },
    ],
    [
      "a required table that is not a string",
      {
        parameters: { ...parameters(), requiredTables: [1] },
        clientInstanceId: "id",
        response: Promise.resolve(new Response("{}")),
      },
    ],
    [
      "no heap reserve",
      {
        parameters: { ...parameters(), clientHeapReserveBytes: undefined },
        clientInstanceId: "id",
        response: Promise.resolve(new Response("{}")),
      },
    ],
  ])("takes and drops a slot that holds %s", (_shape, value) => {
    const slot = Symbol.for("@calque/client:started-bootstrap");
    (globalThis as Record<symbol, unknown>)[slot] = value;
    expect(takeStartedBootstrap()).toBeUndefined();
    expect(Object.getOwnPropertySymbols(globalThis)).not.toContain(slot);
  });

  it("names the parameter in which a request differs from the session's, and none for an equal one", () => {
    const own = parameters();
    expect(differingBootstrapParameter(parameters(), own)).toBeUndefined();
    const differing: [
      keyof BootstrapParameters,
      Partial<BootstrapParameters>,
    ][] = [
      ["url", { url: "https://sync.test/elsewhere/v1/bootstrap" }],
      ["bearerToken", { bearerToken: "second-principal" }],
      ["transportProfile", { transportProfile: "second-profile" }],
      ["protocolVersion", { protocolVersion: 2 }],
      ["requiredTables", { requiredTables: ["public.events"] }],
      [
        "requiredTables",
        {
          requiredTables: ["public.events", "public.issues", "public.users"],
        },
      ],
      [
        "requiredTables",
        { requiredTables: ["public.issues", "public.events"] },
      ],
      ["acceptedEncodings", { acceptedEncodings: ["identity"] }],
      ["acceptedEncodings", { acceptedEncodings: ["identity", "zstd"] }],
      ["memoryBudgetBytes", { memoryBudgetBytes: MEMORY_BUDGET_BYTES * 2 }],
      ["clientHeapReserveBytes", { clientHeapReserveBytes: 8 * 1024 * 1024 }],
    ];
    for (const [field, change] of differing)
      expect(
        differingBootstrapParameter({ ...parameters(), ...change }, own),
      ).toBe(field);
  });

  it("cancels the body of a request it discards, and tolerates one that failed", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
      },
    });
    discardStartedBootstrap({
      parameters: parameters(),
      clientInstanceId: "instance",
      response: Promise.resolve(new Response(body)),
    });
    await vi.waitFor(() => expect(cancelled).toBe(true));
    discardStartedBootstrap({
      parameters: parameters(),
      clientInstanceId: "instance",
      response: Promise.reject(new TypeError("fetch failed")),
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
});

describe("the encodings a bootstrap request names", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("are zstd and identity in a runtime without workers, which is this one", () => {
    expect(brotliDecoderRuns()).toBe(false);
    expect(acceptedArtifactEncodings()).toEqual(["zstd", "identity"]);
  });

  it.each(DECODER_PAGES)(
    "name the two brotli encodings first only where the decoder worker can run: a page %s",
    (_page, stub, encodings) => {
      stub();
      expect(brotliDecoderRuns()).toBe(encodings.length === 4);
      expect(acceptedArtifactEncodings()).toEqual(encodings);
    },
  );
});

const RENDERED_ID = "11111111-1111-4111-8111-111111111111";
const PLAN_TEXT = JSON.stringify({
  planId: "plan-rendered",
  expiresAt: "2099-01-01T00:00:00Z",
});

/** A `calque-plan` element's JSON, as `renderBootstrap` writes it. */
function renderedPlan(change: Record<string, unknown> = {}): string {
  const {
    transportProfile,
    protocolVersion,
    requiredTables,
    acceptedEncodings,
    memoryBudgetBytes,
    clientHeapReserveBytes,
    deviceId,
  } = parameters();
  return JSON.stringify({
    transportProfile,
    protocolVersion,
    requiredTables,
    acceptedEncodings,
    memoryBudgetBytes,
    clientHeapReserveBytes,
    deviceId,
    clientInstanceId: RENDERED_ID,
    plan: PLAN_TEXT,
    ...change,
  });
}

/** Rendered plans the starter does not take, so the page posts its own. */
const UNUSABLE_PLANS: readonly (readonly [string, string])[] = [
  ["text that is not JSON", "{"],
  ["null", "null"],
  ["no plan", renderedPlan({ plan: undefined })],
  ["a plan that is not text", renderedPlan({ plan: { expiresAt: "2099" } })],
  ["a plan that is not JSON", renderedPlan({ plan: "{" })],
  ["a plan that is null", renderedPlan({ plan: "null" })],
  [
    "a plan without an expiry",
    renderedPlan({ plan: JSON.stringify({ planId: "plan-rendered" }) }),
  ],
  [
    "a plan whose expiry is not text",
    renderedPlan({ plan: JSON.stringify({ expiresAt: 4_102_444_800_000 }) }),
  ],
  [
    "an expired plan",
    renderedPlan({
      plan: JSON.stringify({ expiresAt: "2001-01-01T00:00:00Z" }),
    }),
  ],
  ["an empty clientInstanceId", renderedPlan({ clientInstanceId: "" })],
  ["no clientInstanceId", renderedPlan({ clientInstanceId: undefined })],
  ["no transport profile", renderedPlan({ transportProfile: undefined })],
  ["a protocol version as text", renderedPlan({ protocolVersion: "1" })],
  ["required tables as text", renderedPlan({ requiredTables: "public.x" })],
  ["an encoding that is not text", renderedPlan({ acceptedEncodings: [1] })],
  ["no budget", renderedPlan({ memoryBudgetBytes: undefined })],
  ["no heap reserve", renderedPlan({ clientHeapReserveBytes: undefined })],
  ["no device", renderedPlan({ deviceId: undefined })],
  ["an empty device", renderedPlan({ deviceId: "" })],
];

describe("a plan the backend rendered into the page", () => {
  const start = {
    endpoint: "https://sync.test/root",
    bearerToken: "token",
    requiredTables: ["public.issues", "public.events"],
    memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    clientHeapReserveBytes: 4 * 1024 * 1024,
  } satisfies BootstrapStart;

  beforeEach(() => {
    vi.stubGlobal("navigator", {
      userAgentData: { brands: [{ brand: "Chromium" }] },
    });
    vi.stubGlobal("location", PAGE);
    performance.clearMarks();
  });

  afterEach(() => {
    takeStartedBootstrap();
    vi.unstubAllGlobals();
  });

  it("is left in the slot as the response the backend got, and nothing is posted", async () => {
    const fetcher = stubFetch();
    startBootstrap({ ...start, renderedPlan: renderedPlan() });
    expect(fetcher).not.toHaveBeenCalled();
    const started = takeStartedBootstrap();
    if (!started) throw new Error("expected the rendered plan in the slot");
    expect(started.parameters).toEqual(parameters());
    expect(started.clientInstanceId).toBe(RENDERED_ID);
    expect(Object.isFrozen(started)).toBe(true);
    expect(Object.isFrozen(started.parameters)).toBe(true);
    expect(Object.isFrozen(started.parameters.acceptedEncodings)).toBe(true);
    const response = await started.response;
    expect(response.status).toBe(200);
    expect(await response.text()).toBe(PLAN_TEXT);
    expect(
      performance.getEntriesByName("calque:bootstrap:rendered", "mark"),
    ).toHaveLength(1);
    expect(
      performance.getEntriesByName("calque:bootstrap:started", "mark"),
    ).toHaveLength(0);
  });

  it("carries the parameters the backend sent, not the page's, so the session judges them", () => {
    stubFetch();
    startBootstrap({
      ...start,
      memoryBudgetBytes: 2 * MEMORY_BUDGET_BYTES,
      renderedPlan: renderedPlan(),
    });
    expect(takeStartedBootstrap()?.parameters).toEqual(parameters());
  });

  it.each(UNUSABLE_PLANS)(
    "is refused for %s, and the page posts its own request",
    (_case, text) => {
      const fetcher = stubFetch();
      startBootstrap({ ...start, renderedPlan: text });
      expect(fetcher).toHaveBeenCalledTimes(1);
      const started = takeStartedBootstrap();
      expect(started?.clientInstanceId).not.toBe(RENDERED_ID);
      expect(
        performance.getEntriesByName("calque:bootstrap:rendered", "mark"),
      ).toHaveLength(0);
    },
  );

  it("is not taken while the slot is full", () => {
    const fetcher = stubFetch();
    startBootstrap(start);
    startBootstrap({ ...start, renderedPlan: renderedPlan() });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(takeStartedBootstrap()?.clientInstanceId).not.toBe(RENDERED_ID);
  });

  it.each(REFUSED_ENDPOINTS)(
    "is not taken for %j, an endpoint createSyncClient refuses",
    (endpoint) => {
      stubFetch();
      startBootstrap({ ...start, endpoint, renderedPlan: renderedPlan() });
      expect(takeStartedBootstrap()).toBeUndefined();
    },
  );
});

describe("the connection the backend rendered into the page", () => {
  const CONNECTION = JSON.stringify({
    endpoint: "https://sync.test/root",
    bearerToken: "rendered-grant",
  });

  /** A document holding these element texts by id. */
  function documentWith(
    elements: Readonly<Record<string, string>>,
  ): Pick<Document, "getElementById"> {
    return {
      getElementById: (id: string) =>
        (id in elements
          ? { textContent: elements[id] }
          : null) as HTMLElement | null,
    };
  }

  beforeEach(() => {
    vi.stubGlobal("navigator", {
      userAgentData: { brands: [{ brand: "Chromium" }] },
    });
  });

  afterEach(() => {
    takeStartedBootstrap();
    vi.unstubAllGlobals();
  });

  it("gives startBootstrap the endpoint and bearer the page omits", () => {
    vi.stubGlobal(
      "document",
      documentWith({ "calque-connection": CONNECTION }),
    );
    const fetcher = stubFetch();
    startBootstrap({
      requiredTables: [],
      memoryBudgetBytes: MEMORY_BUDGET_BYTES,
    });
    expect(String(fetcher.mock.calls[0]?.[0])).toBe(
      "https://sync.test/root/v1/bootstrap",
    );
    expect(fetcher.mock.calls[0]?.[1]?.headers).toMatchObject({
      authorization: "Bearer rendered-grant",
    });
  });

  it("gives way to the endpoint and bearer the page passes", () => {
    vi.stubGlobal(
      "document",
      documentWith({ "calque-connection": CONNECTION }),
    );
    stubFetch();
    startBootstrap({
      endpoint: "https://sync.test/other",
      bearerToken: "own-grant",
      requiredTables: [],
    });
    expect(takeStartedBootstrap()?.parameters).toMatchObject({
      url: "https://sync.test/other/v1/bootstrap",
      bearerToken: "own-grant",
    });
  });

  it("gives startBootstrap the rendered plan the page omits", () => {
    vi.stubGlobal(
      "document",
      documentWith({
        "calque-connection": JSON.stringify({
          endpoint: "https://sync.test/root",
          bearerToken: "token",
        }),
        "calque-plan": renderedPlan(),
      }),
    );
    const fetcher = stubFetch();
    startBootstrap({ requiredTables: ["public.events"] });
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()?.clientInstanceId).toBe(RENDERED_ID);
  });

  it.each([
    ["no document", undefined],
    ["no connection", documentWith({})],
    [
      "a connection without a bearer",
      documentWith({
        "calque-connection": '{"endpoint":"https://sync.test/root"}',
      }),
    ],
  ])("leaves startBootstrap nothing to send with %s", (_case, page) => {
    vi.stubGlobal("document", page);
    const fetcher = stubFetch();
    startBootstrap({ requiredTables: [] });
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()).toBeUndefined();
  });

  it("is read by renderedConnection, from the page or the document it is given", () => {
    expect(
      renderedConnection(documentWith({ "calque-connection": CONNECTION })),
    ).toEqual({
      endpoint: "https://sync.test/root",
      bearerToken: "rendered-grant",
    });
    vi.stubGlobal(
      "document",
      documentWith({ "calque-connection": CONNECTION }),
    );
    expect(renderedConnection()).toEqual({
      endpoint: "https://sync.test/root",
      bearerToken: "rendered-grant",
    });
  });

  it.each([
    ["no element", {}],
    ["an empty element", { "calque-connection": "" }],
    ["text that is not JSON", { "calque-connection": "{" }],
    ["null", { "calque-connection": "null" }],
    ["a string", { "calque-connection": '"grant"' }],
    ["no bearer", { "calque-connection": '{"endpoint":"https://sync.test/"}' }],
    [
      "a bearer that is not a string",
      {
        "calque-connection":
          '{"endpoint":"https://sync.test/","bearerToken":7}',
      },
    ],
  ])("makes renderedConnection throw for %s", (_case, elements) => {
    expect(() => renderedConnection(documentWith(elements))).toThrowError(
      'the page has no calque-connection element holding a string "endpoint" and "bearerToken"',
    );
  });

  it("makes renderedConnection throw outside a page", () => {
    expect(() => renderedConnection()).toThrowError(/no calque-connection/u);
  });

  it("is the bearer startedAuth returns first; every later call returns refresh()'s", async () => {
    vi.stubGlobal(
      "document",
      documentWith({ "calque-connection": CONNECTION }),
    );
    let issued = 0;
    const refresh = vi.fn(async () => ({
      bearerToken: `refreshed-${(issued += 1)}`,
    }));
    const auth = startedAuth(refresh);
    expect(auth()).toEqual({ bearerToken: "rendered-grant" });
    expect(refresh).not.toHaveBeenCalled();
    await expect(auth()).resolves.toEqual({ bearerToken: "refreshed-1" });
    await expect(auth()).resolves.toEqual({ bearerToken: "refreshed-2" });
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("leaves startedAuth to refresh() from its first call on a page without one", () => {
    vi.stubGlobal("document", documentWith({}));
    const refresh = vi.fn(() => ({ bearerToken: "refreshed" }));
    const auth = startedAuth(refresh);
    expect(auth()).toEqual({ bearerToken: "refreshed" });
    expect(auth()).toEqual({ bearerToken: "refreshed" });
    expect(refresh).toHaveBeenCalledTimes(2);
  });
});

describe("the starter a page inlines in its head", () => {
  const INSTANCE_ID = "00000000-0000-4000-8000-000000000000";

  beforeEach(() => {
    vi.stubGlobal("navigator", {
      userAgentData: { brands: [{ brand: "Chromium" }] },
    });
    vi.spyOn(globalThis.crypto, "randomUUID").mockReturnValue(INSTANCE_ID);
    vi.stubGlobal("location", PAGE);
    stubStorage(STORED_DEVICE);
    performance.clearMarks();
  });

  afterEach(() => {
    takeStartedBootstrap();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  /** A document holding these element texts by id. */
  function page(elements: Readonly<Record<string, string>>) {
    vi.stubGlobal("document", {
      getElementById: (id: string) =>
        id in elements ? { textContent: elements[id] } : null,
    });
  }

  /** Runs the script as the page's head would, in strict mode. */
  function runScript(): void {
    new Function(`"use strict";\n${BOOTSTRAP_START_SCRIPT}`)();
  }

  function rendered(start: BootstrapStart): Record<string, string> {
    const { endpoint, bearerToken, renderedPlan: plan, ...request } = start;
    return {
      "calque-connection": JSON.stringify({ endpoint, bearerToken }),
      "calque-bootstrap": JSON.stringify(request),
      ...(typeof plan === "string" ? { "calque-plan": plan } : {}),
    };
  }

  /**
   * What each path sent and left in the slot on a page rendered for `start`,
   * with `startBootstrap` reading the connection and the plan from the page.
   */
  function bothPaths(start: BootstrapStart) {
    const elements = rendered(start);
    page(elements);
    const own = stubFetch();
    startBootstrap(
      JSON.parse(elements["calque-bootstrap"] ?? "") as BootstrapRequest,
    );
    const fromCall = { sent: own.mock.calls, slot: takeStartedBootstrap() };
    const inline = stubFetch();
    runScript();
    const fromScript = {
      sent: inline.mock.calls,
      slot: takeStartedBootstrap(),
    };
    return { fromCall, fromScript };
  }

  it("is the text its SHA-256 names", () => {
    expect(BOOTSTRAP_START_SCRIPT_SHA256).toBe(
      `sha256-${createHash("sha256").update(BOOTSTRAP_START_SCRIPT, "utf8").digest("base64")}`,
    );
  });

  it("can sit between a script element's tags as it is", () => {
    // Either sequence would end the element, or hide its end, early.
    expect(BOOTSTRAP_START_SCRIPT).not.toMatch(/<\/script|<!--/iu);
  });

  it.each([1, 4 * 1024 * 1024, MEMORY_BUDGET_BYTES, 2 * 1024 ** 3, 1024 ** 4])(
    "sends byte for byte what startBootstrap sends, and leaves the same request in the slot, for a budget of %d bytes",
    (memoryBudgetBytes) => {
      for (const clientHeapReserveBytes of [undefined, 6 * 1024 * 1024]) {
        const { fromCall, fromScript } = bothPaths({
          endpoint: "https://sync.test/root",
          bearerToken: "token",
          requiredTables: ["public.issues", "public.events"],
          memoryBudgetBytes,
          ...(clientHeapReserveBytes === undefined
            ? {}
            : { clientHeapReserveBytes }),
        });
        expect(fromCall.sent).toHaveLength(1);
        expect(fromScript.sent).toEqual(fromCall.sent);
        expect(fromScript.slot?.parameters).toEqual(fromCall.slot?.parameters);
        expect(fromScript.slot?.parameters.deviceId).toBe(STORED_DEVICE);
        expect(fromScript.slot?.clientInstanceId).toBe(INSTANCE_ID);
        expect(Object.isFrozen(fromScript.slot)).toBe(true);
        expect(Object.isFrozen(fromScript.slot?.parameters)).toBe(true);
        expect(
          Object.isFrozen(fromScript.slot?.parameters.requiredTables),
        ).toBe(true);
      }
    },
  );

  it.each(DECODER_PAGES)(
    "names the encodings startBootstrap names on a page %s",
    (_page, stub, encodings) => {
      stub();
      const { fromCall, fromScript } = bothPaths({
        endpoint: "https://sync.test/root",
        bearerToken: "token",
        requiredTables: ["public.issues"],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      });
      expect(fromCall.sent).toHaveLength(1);
      expect(fromScript.sent).toEqual(fromCall.sent);
      expect(fromScript.slot?.parameters).toEqual(fromCall.slot?.parameters);
      const body = JSON.parse(String(fromCall.sent[0]?.[1]?.body)) as {
        acceptedEncodings: unknown;
      };
      expect(body.acceptedEncodings).toEqual(encodings);
      expect(fromCall.slot?.parameters.acceptedEncodings).toEqual(encodings);
    },
  );

  it.each([
    ["the device the page names", "device-z", STORED_DEVICE, "device-z"],
    ["a device it creates and keeps", undefined, undefined, INSTANCE_ID],
    [
      "a new device over a stored id that is not one",
      undefined,
      "x",
      INSTANCE_ID,
    ],
  ])("names %s, as startBootstrap does", (_case, deviceId, stored, sent) => {
    const storage = stubStorage(stored);
    const { fromCall, fromScript } = bothPaths({
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: ["public.issues"],
      memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      ...(deviceId === undefined ? {} : { deviceId }),
    });
    expect(fromScript.sent).toEqual(fromCall.sent);
    const body = JSON.parse(String(fromScript.sent[0]?.[1]?.body)) as {
      deviceId: unknown;
    };
    expect(body.deviceId).toBe(sent);
    if (sent === INSTANCE_ID)
      expect(storage.setItem).toHaveBeenCalledWith("calque-device-id", sent);
  });

  it.each(ACCEPTED_ENDPOINTS)(
    "posts to the URL startBootstrap posts to for %j",
    (endpoint) => {
      const { fromCall, fromScript } = bothPaths({
        endpoint,
        bearerToken: "token",
        requiredTables: [],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      });
      expect(String(fromScript.sent[0]?.[0])).toBe(
        String(fromCall.sent[0]?.[0]),
      );
    },
  );

  it.each(REFUSED_ENDPOINTS)("sends no token to %j", (endpoint) => {
    const fetcher = stubFetch();
    page(
      rendered({
        endpoint,
        bearerToken: "token",
        requiredTables: [],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      }),
    );
    runScript();
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()).toBeUndefined();
  });

  const connection = JSON.stringify({
    endpoint: "https://sync.test/root",
    bearerToken: "token",
  });
  const request = JSON.stringify({
    requiredTables: [],
    memoryBudgetBytes: MEMORY_BUDGET_BYTES,
  });
  it.each([
    ["no connection", { "calque-bootstrap": request }],
    ["no request parameters", { "calque-connection": connection }],
    [
      "a connection that is not JSON",
      { "calque-connection": "{", "calque-bootstrap": request },
    ],
    [
      "an empty connection",
      { "calque-connection": "", "calque-bootstrap": request },
    ],
    [
      "a bearer that is not a string",
      {
        "calque-connection":
          '{"endpoint":"https://sync.test/root","bearerToken":7}',
        "calque-bootstrap": request,
      },
    ],
    [
      "required tables that are not strings",
      {
        "calque-connection": connection,
        "calque-bootstrap": '{"requiredTables":[1],"memoryBudgetBytes":1}',
      },
    ],
    [
      "a budget that is not a number",
      {
        "calque-connection": connection,
        "calque-bootstrap": '{"requiredTables":[],"memoryBudgetBytes":"1"}',
      },
    ],
    [
      "a heap reserve that is not a number",
      {
        "calque-connection": connection,
        "calque-bootstrap":
          '{"requiredTables":[],"memoryBudgetBytes":1,"clientHeapReserveBytes":"1"}',
      },
    ],
    [
      "a device that is not a string",
      {
        "calque-connection": connection,
        "calque-bootstrap":
          '{"requiredTables":[],"memoryBudgetBytes":1,"deviceId":7}',
      },
    ],
  ])("sends nothing for %s", (_case, elements) => {
    const fetcher = stubFetch();
    page(elements);
    runScript();
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()).toBeUndefined();
  });

  const start: BootstrapStart = {
    endpoint: "https://sync.test/root",
    bearerToken: "token",
    requiredTables: [],
    memoryBudgetBytes: MEMORY_BUDGET_BYTES,
  };

  it.each(OUTSIDE_PROFILE)("sends nothing in a browser %s", (_, outside) => {
    page(rendered(start));
    const fetcher = stubFetch();
    outside();
    runScript();
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()).toBeUndefined();
  });

  it("sends nothing while the slot is full", () => {
    page(rendered(start));
    stubFetch();
    startBootstrap(start);
    const full = stubFetch();
    runScript();
    expect(full).not.toHaveBeenCalled();
  });

  it("takes a rendered plan as startBootstrap does, and posts nothing", async () => {
    const { fromCall, fromScript } = bothPaths({
      endpoint: "https://sync.test/root",
      bearerToken: "token",
      requiredTables: ["public.issues", "public.events"],
      memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      renderedPlan: renderedPlan(),
    });
    expect(fromCall.sent).toHaveLength(0);
    expect(fromScript.sent).toHaveLength(0);
    expect(fromScript.slot?.parameters).toEqual(fromCall.slot?.parameters);
    expect(fromScript.slot?.clientInstanceId).toBe(RENDERED_ID);
    expect(fromCall.slot?.clientInstanceId).toBe(RENDERED_ID);
    expect(Object.isFrozen(fromScript.slot)).toBe(true);
    expect(Object.isFrozen(fromScript.slot?.parameters)).toBe(true);
    expect(Object.isFrozen(fromScript.slot?.parameters.requiredTables)).toBe(
      true,
    );
    const response = await fromScript.slot?.response;
    expect(response?.status).toBe(200);
    expect(response?.headers.get("content-type")).toBe("application/json");
    expect(await response?.text()).toBe(PLAN_TEXT);
    expect(
      performance.getEntriesByName("calque:bootstrap:rendered", "mark"),
    ).toHaveLength(2);
  });

  it.each(UNUSABLE_PLANS)(
    "posts what startBootstrap posts for %s",
    (_case, renderedPlan) => {
      const { fromCall, fromScript } = bothPaths({
        endpoint: "https://sync.test/root",
        bearerToken: "token",
        requiredTables: ["public.issues", "public.events"],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
        renderedPlan,
      });
      expect(fromCall.sent).toHaveLength(1);
      expect(fromScript.sent).toEqual(fromCall.sent);
      expect(fromScript.slot?.parameters).toEqual(fromCall.slot?.parameters);
      expect(fromScript.slot?.clientInstanceId).toBe(INSTANCE_ID);
    },
  );

  it("takes a rendered plan without a calque-bootstrap element", () => {
    const fetcher = stubFetch();
    page({
      "calque-connection": connection,
      "calque-plan": renderedPlan(),
    });
    runScript();
    expect(fetcher).not.toHaveBeenCalled();
    expect(takeStartedBootstrap()?.clientInstanceId).toBe(RENDERED_ID);
  });

  it.each(REFUSED_ENDPOINTS)(
    "does not take a rendered plan for %j",
    (endpoint) => {
      stubFetch();
      page({
        "calque-connection": JSON.stringify({ endpoint, bearerToken: "token" }),
        "calque-plan": renderedPlan(),
      });
      runScript();
      expect(takeStartedBootstrap()).toBeUndefined();
    },
  );

  it("marks the start, and leaves a fetch that throws as a rejected response", async () => {
    const failure = new TypeError("invalid header value");
    stubFetch(() => {
      throw failure;
    });
    page(
      rendered({
        endpoint: "https://sync.test/root",
        bearerToken: "token",
        requiredTables: [],
        memoryBudgetBytes: MEMORY_BUDGET_BYTES,
      }),
    );
    runScript();
    expect(
      performance.getEntriesByName("calque:bootstrap:started", "mark"),
    ).toHaveLength(1);
    await expect(takeStartedBootstrap()?.response).rejects.toBe(failure);
  });
});
