import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { renderBootstrap } from "./bootstrap-render.js";
import {
  type BootstrapStart,
  startBootstrap,
  takeStartedBootstrap,
} from "./bootstrap-start.js";

const INSTANCE_ID = "22222222-2222-4222-8222-222222222222";
const ARTIFACT_URL =
  "https://sync.test/root/v1/artifact-objects/sha256:ab?cap=capability";

/** Chrome's `Accept-Encoding` for a secure page, as the backend receives it. */
const BROWSER = "gzip, deflate, br, zstd";

const START = {
  endpoint: "https://sync.test/root",
  bearerToken: "token",
  requiredTables: ["public.issues", "public.events"],
  memoryBudgetBytes: 32 * 1024 * 1024,
} satisfies BootstrapStart;

function planWith(urls: readonly unknown[]): Record<string, unknown> {
  return {
    planId: "plan-rendered",
    expiresAt: "2099-01-01T00:00:00Z",
    artifacts: urls.map((downloadUrl) => ({
      selectedRepresentation: { encoding: "zstd", downloadUrl },
    })),
  };
}

function answering(body: string, status = 200) {
  return vi.fn<typeof fetch>(
    async () =>
      new Response(body, {
        status,
        headers: { "content-type": "application/json" },
      }),
  );
}

/** The `calque-plan` element's text in `head`, and the preloads before it. */
function parts(head: string): { preloads: string[]; element: string } {
  const at = head.indexOf('<script type="application/json" id="calque-plan">');
  if (at === -1 || !head.endsWith("</script>"))
    throw new Error(`no calque-plan element at the end of ${head}`);
  const preloads = head.slice(0, at).match(/<link [^>]*>/gu) ?? [];
  expect(preloads.join("")).toBe(head.slice(0, at));
  return {
    preloads,
    element: head.slice(
      at + '<script type="application/json" id="calque-plan">'.length,
      -"</script>".length,
    ),
  };
}

describe("a bootstrap the backend renders into the page", () => {
  beforeEach(() => {
    vi.stubGlobal("navigator", {
      userAgentData: { brands: [{ brand: "Chromium" }] },
    });
    // A page that can run the decoder worker, so it names what the backend
    // names for it.
    vi.stubGlobal("Worker", vi.fn());
    vi.spyOn(globalThis.crypto, "randomUUID").mockReturnValue(INSTANCE_ID);
  });

  afterEach(() => {
    takeStartedBootstrap();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("posts the request the page's starter would post, with the browser's Accept-Encoding", async () => {
    const backend = answering(JSON.stringify(planWith([ARTIFACT_URL])));
    await renderBootstrap(START, { acceptEncoding: BROWSER, fetch: backend });
    const page = vi.fn<typeof fetch>(() => new Promise(() => undefined));
    vi.stubGlobal("fetch", page);
    startBootstrap(START);
    expect(backend).toHaveBeenCalledTimes(1);
    const [url, init] = page.mock.calls[0] ?? [];
    expect(backend.mock.calls[0]).toEqual([
      url,
      {
        ...init,
        headers: {
          ...(init?.headers as Record<string, string>),
          "accept-encoding": BROWSER,
        },
      },
    ]);
  });

  it("names the two brotli encodings first in the request, whatever runtime the backend renders in", async () => {
    // The backend has no workers to probe, and the page that takes the plan
    // is a browser's.
    vi.stubGlobal("Worker", undefined);
    const backend = answering(JSON.stringify(planWith([ARTIFACT_URL])));
    await renderBootstrap(START, { acceptEncoding: BROWSER, fetch: backend });
    const body = JSON.parse(String(backend.mock.calls[0]?.[1]?.body)) as {
      acceptedEncodings: unknown;
    };
    expect(body.acceptedEncodings).toEqual([
      "packed-brotli-large-window",
      "brotli-large-window",
      "zstd",
      "identity",
    ]);
  });

  it("posts and renders nothing without the browser's Accept-Encoding", async () => {
    const backend = answering(JSON.stringify(planWith([ARTIFACT_URL])));
    expect(
      await renderBootstrap(START, {
        acceptEncoding: undefined,
        fetch: backend,
      }),
    ).toBe("");
    expect(backend).not.toHaveBeenCalled();
  });

  it("preloads each artifact, then renders the plan the page's starter takes as the server sent it", async () => {
    const second = `${ARTIFACT_URL}-2`;
    const body = JSON.stringify(planWith([ARTIFACT_URL, second]));
    const head = await renderBootstrap(START, {
      acceptEncoding: BROWSER,
      fetch: answering(body),
    });
    const { preloads, element } = parts(head);
    expect(preloads).toEqual(
      [ARTIFACT_URL, second].map(
        (url) =>
          `<link rel="preload" as="fetch" crossorigin="anonymous" href="${url}">`,
      ),
    );

    vi.stubGlobal(
      "fetch",
      vi.fn<typeof fetch>(() => new Promise(() => undefined)),
    );
    startBootstrap(START);
    const posted = takeStartedBootstrap();
    const page = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", page);
    startBootstrap({ ...START, renderedPlan: element });
    expect(page).not.toHaveBeenCalled();
    const adopted = takeStartedBootstrap();
    expect(adopted?.parameters).toEqual(posted?.parameters);
    expect(adopted?.clientInstanceId).toBe(INSTANCE_ID);
    expect(await (await adopted?.response)?.text()).toBe(body);
  });

  it("writes no character that ends the element or the attribute early", async () => {
    const url = `${ARTIFACT_URL}&x="<b>"`;
    const body = JSON.stringify({
      ...planWith([url]),
      planId: "</script><script>alert(1)</script><!--",
    });
    const head = await renderBootstrap(START, {
      acceptEncoding: BROWSER,
      fetch: answering(body),
    });
    const { preloads, element } = parts(head);
    expect(preloads).toEqual([
      `<link rel="preload" as="fetch" crossorigin="anonymous" href="${ARTIFACT_URL}&amp;x=&quot;&lt;b&gt;&quot;">`,
    ]);
    expect(element).not.toContain("<");
    expect((JSON.parse(element) as { plan: string }).plan).toBe(body);
  });

  it.each(["brotli-large-window", "packed-brotli-large-window"])(
    "preloads and renders an artifact in the %s representation",
    async (encoding) => {
      const body = JSON.stringify({
        artifacts: [
          { selectedRepresentation: { encoding, downloadUrl: ARTIFACT_URL } },
        ],
      });
      const head = await renderBootstrap(START, {
        acceptEncoding: BROWSER,
        fetch: answering(body),
      });
      const { preloads, element } = parts(head);
      expect(preloads).toEqual([
        `<link rel="preload" as="fetch" crossorigin="anonymous" href="${ARTIFACT_URL}">`,
      ]);
      expect((JSON.parse(element) as { plan: string }).plan).toBe(body);
    },
  );

  it("renders only the plan for a plan that names no artifact", async () => {
    const head = await renderBootstrap(START, {
      acceptEncoding: BROWSER,
      fetch: answering(JSON.stringify(planWith([]))),
    });
    expect(parts(head).preloads).toEqual([]);
  });

  it("hands the request the signal it is given", async () => {
    const backend = answering(JSON.stringify(planWith([ARTIFACT_URL])));
    const signal = new AbortController().signal;
    await renderBootstrap(START, {
      acceptEncoding: BROWSER,
      fetch: backend,
      signal,
    });
    expect(backend.mock.calls[0]?.[1]?.signal).toBe(signal);
  });

  it.each([
    "http://sync.test/root",
    "https://user:secret@sync.test/root",
    "https://sync.test/root#fragment",
    "not a url",
    // A path is read against the page's URL, and a backend has no page.
    "/_calque/",
  ])(
    "posts nothing to %j, an endpoint createSyncClient refuses",
    async (endpoint) => {
      const backend = answering(JSON.stringify(planWith([ARTIFACT_URL])));
      expect(
        await renderBootstrap(
          { ...START, endpoint },
          { acceptEncoding: BROWSER, fetch: backend },
        ),
      ).toBe("");
      expect(backend).not.toHaveBeenCalled();
    },
  );

  it.each([
    ["a 503 while the artifact is prepared", "{}", 503],
    ["a 401", "{}", 401],
    ["a body over 2 MiB", `{"pad":"${"a".repeat(2 * 1024 * 1024)}"}`, 200],
    ["a body that is not JSON", "{", 200],
    ["a plan without artifacts", JSON.stringify({ planId: "p" }), 200],
    [
      "an artifact without a URL",
      JSON.stringify({ artifacts: [{ selectedRepresentation: {} }] }),
      200,
    ],
    [
      "an artifact in the identity representation",
      JSON.stringify({
        artifacts: [
          {
            selectedRepresentation: {
              encoding: "identity",
              downloadUrl: ARTIFACT_URL,
            },
          },
        ],
      }),
      200,
    ],
    ["a relative URL", JSON.stringify(planWith(["/v1/artifact"])), 200],
    ["a URL that is not HTTP", JSON.stringify(planWith(["javascript:x"])), 200],
    ["a URL that is not text", JSON.stringify(planWith([7])), 200],
  ] as const)("renders nothing for %s", async (_case, body, status) => {
    expect(
      await renderBootstrap(START, {
        acceptEncoding: BROWSER,
        fetch: answering(body, status),
      }),
    ).toBe("");
  });

  it("renders nothing when the request fails", async () => {
    const backend = vi.fn<typeof fetch>(() =>
      Promise.reject(new TypeError("fetch failed")),
    );
    expect(
      await renderBootstrap(START, { acceptEncoding: BROWSER, fetch: backend }),
    ).toBe("");
  });

  it("cancels the body of an answer it does not render", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
      },
    });
    const backend = vi.fn<typeof fetch>(
      async () => new Response(body, { status: 503 }),
    );
    expect(
      await renderBootstrap(START, { acceptEncoding: BROWSER, fetch: backend }),
    ).toBe("");
    expect(cancelled).toBe(true);
  });
});
