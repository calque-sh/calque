/**
 * The HTTP seam, driven directly. The parser admits only the encodings the
 * supported profile reads, so the encoding gate cannot fire through the
 * session and is pinned here.
 */
import { readFileSync } from "node:fs";

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { mutation } from "@calque/protocol";
import { describe, expect, it, vi } from "vitest";

import { MAX_ARTIFACT_CHUNK_BYTES } from "./limits.js";
import { parseBootstrapPlan } from "./plan.js";
import {
  artifactTransportProfile,
  exactResponseBuffers,
  exactResponsePieces,
  fetchArtifact,
  mutationOutcomeIsDefinitive,
  mutationOutcomeResult,
  requestBootstrapPlan,
  submitMutationBatch,
  type ArtifactTransportProfile,
} from "./transport.js";
import {
  stubBrowserFeatures,
  stubWebAssemblyPolicy,
} from "./browser.fixture.js";

const COLUMNAR_VECTOR = new URL(
  "../../../../spec/bootstrap-plan.direct-columnar.example.json",
  import.meta.url,
);
const transport: ArtifactTransportProfile = {
  name: "chromium-native-artifact-v1",
  acceptedEncodings: ["zstd", "identity"],
  readableEncodings: [
    "packed-brotli-large-window",
    "brotli-large-window",
    "zstd",
    "identity",
  ],
};

/** The checked-in vector, dated so the expiry gate admits it. */
function planValue(): Record<string, unknown> {
  return {
    ...(JSON.parse(readFileSync(COLUMNAR_VECTOR, "utf8")) as Record<
      string,
      unknown
    >),
    expiresAt: "2099-01-01T00:00:00Z",
  };
}

function planFetch() {
  return vi.fn<typeof fetch>(async () =>
    Response.json(planValue(), { status: 200 }),
  );
}

function bootstrapBody(fetcher: ReturnType<typeof planFetch>) {
  return JSON.parse(String(fetcher.mock.calls[0]?.[1]?.body)) as Record<
    string,
    unknown
  >;
}

function request(fetcher: typeof fetch, signal?: AbortSignal) {
  return {
    fetch: fetcher,
    endpoint: new URL("https://sync.test/root/"),
    bearerToken: "token",
    transport,
    requiredTables: ["table/tasks"],
    memoryBudgetBytes: 256 * 1024 * 1024,
    clientHeapReserveBytes: 16 * 1024 * 1024,
    clientInstanceId: "instance-1",
    deviceId: "device-1",
    ...(signal ? { signal } : {}),
  };
}

describe("requestBootstrapPlan", () => {
  it("writes the negotiated request body", async () => {
    const fetcher = planFetch();
    await requestBootstrapPlan(request(fetcher));
    expect(bootstrapBody(fetcher)).toMatchObject({
      protocolVersion: 1,
      requiredTables: ["table/tasks"],
      acceptedEncodings: ["zstd", "identity"],
      clientInstanceId: "instance-1",
    });
  });

  it("waits out ARTIFACT_PREPARING on the server's own clamped schedule", async () => {
    // 503 is how an unavailable server answers too, so only the refusal body
    // marks the answer the client waits out. The delay is the server's
    // `Retry-After`, clamped.
    const preparing = (retryAfter?: string) =>
      vi.fn<typeof fetch>(
        async () =>
          new Response(
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
                ...(retryAfter === undefined
                  ? {}
                  : { "retry-after": retryAfter }),
              },
            },
          ),
      );
    for (const [header, expected] of [
      ["5", 5_000],
      ["0", 1_000],
      ["-4", 1_000],
      ["not-a-number", 1_000],
      ["100000", 300_000],
      [undefined, 1_000],
    ] as const) {
      await expect(
        requestBootstrapPlan(request(preparing(header))),
      ).rejects.toMatchObject({
        code: "ARTIFACT_PREPARING",
        recovery: "retry",
        retryAfterMs: expected,
      });
    }

    // A 503 that is not that refusal stays the generic retryable server
    // error, and one that only looks like it in its status is not enough.
    await expect(
      requestBootstrapPlan(
        request(
          vi.fn<typeof fetch>(
            async () => new Response("upstream down", { status: 503 }),
          ),
        ),
      ),
    ).rejects.toMatchObject({ code: "SERVER_ERROR", recovery: "retry" });
    await expect(
      requestBootstrapPlan(
        request(
          vi.fn<typeof fetch>(
            async () =>
              new Response(JSON.stringify({ code: "INTERNAL_RETRYABLE" }), {
                status: 503,
                headers: { "content-type": "application/json" },
              }),
          ),
        ),
      ),
    ).rejects.toMatchObject({ code: "SERVER_ERROR", recovery: "retry" });
  });

  it("maps a refusal by its status and the code its body names", async () => {
    const refusal = (status: number, code?: string) =>
      requestBootstrapPlan(
        request(
          vi.fn<typeof fetch>(async () =>
            code === undefined
              ? new Response("refused", { status })
              : Response.json(
                  { code, message: "refused", resnapshotRequired: false },
                  { status },
                ),
          ),
        ),
      );
    await expect(refusal(422, "CAPACITY_EXCEEDED")).rejects.toMatchObject({
      code: "CAPACITY_EXCEEDED",
      scope: "sync",
      recovery: "none",
    });
    // The server refused the request itself, so sending it again cannot help.
    await expect(refusal(400, "REQUEST_INVALID")).rejects.toMatchObject({
      code: "SERVER_ERROR",
      scope: "sync",
      recovery: "none",
      message: "bootstrap failed with HTTP 400 REQUEST_INVALID",
    });
    await expect(refusal(413)).rejects.toMatchObject({
      code: "SERVER_ERROR",
      recovery: "none",
      message: "bootstrap failed with HTTP 413",
    });
    await expect(refusal(422, "OTHER_REFUSAL")).rejects.toMatchObject({
      code: "SERVER_ERROR",
      recovery: "none",
    });
    await expect(refusal(409, "RESNAPSHOT_REQUIRED")).rejects.toMatchObject({
      code: "SERVER_ERROR",
      recovery: "retry",
      message: "bootstrap failed with HTTP 409 RESNAPSHOT_REQUIRED",
    });
    await expect(refusal(401, "AUTHORIZATION_REQUIRED")).rejects.toMatchObject({
      code: "AUTH_FAILED",
      scope: "auth",
      recovery: "reset",
    });
    // Only a code token reaches the message, never other text from the body.
    await expect(refusal(400, "<b>refused</b>")).rejects.toMatchObject({
      message: "bootstrap failed with HTTP 400",
    });
  });

  it("refuses a body that is not JSON, a plan of another protocol version, and an expired plan", async () => {
    const answering = (body: string) =>
      requestBootstrapPlan(
        request(vi.fn<typeof fetch>(async () => new Response(body))),
      );
    await expect(answering("{not json")).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      scope: "sync",
      recovery: "reset",
      message: "bootstrap response is not valid JSON",
    });
    await expect(
      answering(JSON.stringify({ ...planValue(), protocolVersion: 2 })),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      recovery: "reset",
      message: "bootstrap response does not match the protocol",
    });
    // An expired plan is one the next request replaces, so it is retryable.
    await expect(
      answering(
        JSON.stringify({ ...planValue(), expiresAt: "2000-01-01T00:00:00Z" }),
      ),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      recovery: "retry",
      message: "bootstrap plan expired",
    });
  });

  it("hands the caller's signal to the request", async () => {
    const fetcher = planFetch();
    const controller = new AbortController();
    await requestBootstrapPlan(request(fetcher, controller.signal));
    expect(fetcher.mock.calls[0]?.[1]?.signal).toBe(controller.signal);
  });

  it("posts exactly the method, headers, body and signal, with no cache mode", async () => {
    // Chromium skips its preflight cache for a no-store request, so a
    // `cache` mode here would preflight every bootstrap. The response's own
    // Cache-Control keeps it out of the HTTP cache.
    const headers = {
      authorization: "Bearer token",
      "content-type": "application/json",
      accept: "application/json",
      "x-calque-artifact-profile": "chromium-native-artifact-v1",
    };
    const fetcher = planFetch();
    await requestBootstrapPlan(request(fetcher));
    const [url, init] = fetcher.mock.calls[0] ?? [];
    expect(String(url)).toBe("https://sync.test/root/v1/bootstrap");
    expect(init).toStrictEqual({
      method: "POST",
      headers,
      body: expect.any(String),
    });

    const signalled = planFetch();
    const controller = new AbortController();
    await requestBootstrapPlan(request(signalled, controller.signal));
    expect(signalled.mock.calls[0]?.[1]).toStrictEqual({
      method: "POST",
      headers,
      body: expect.any(String),
      signal: controller.signal,
    });
  });

  it("names the origin it could not reach", async () => {
    const unreachable = vi.fn<typeof fetch>(async () => {
      throw new TypeError("Failed to fetch");
    });
    await expect(
      requestBootstrapPlan(request(unreachable)),
    ).rejects.toMatchObject({
      code: "NETWORK_ERROR",
      scope: "sync",
      message:
        "could not reach https://sync.test: check that it is running, that this browser trusts its certificate, and that it allows this page's origin",
    });
  });
});

describe("fetchArtifact", () => {
  const artifact = () => {
    const parsed = parseBootstrapPlan(planValue()).artifacts[0];
    if (!parsed) throw new Error("the vector carries no artifact");
    return parsed;
  };

  it("refuses an encoding the profile does not read before any request", async () => {
    const fetcher = vi.fn<typeof fetch>();
    // The parser admits only the encodings the profile reads, so the profile
    // is narrowed here.
    const narrowed: ArtifactTransportProfile = {
      ...transport,
      readableEncodings: ["identity"],
    };
    await expect(
      fetchArtifact(fetcher, narrowed, artifact(), "application/x-test"),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "bootstrap selected an unsupported artifact encoding",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it.each<readonly [string, () => void, readonly string[]]>([
    [
      "names them in the request where WebAssembly compiles",
      () => undefined,
      ["packed-brotli-large-window", "brotli-large-window", "zstd", "identity"],
    ],
    [
      "names neither where a policy forbids compiling it",
      stubWebAssemblyPolicy,
      ["zstd", "identity"],
    ],
  ])(
    "reads both brotli encodings in a plan and %s",
    (_case, stub, accepted) => {
      stubBrowserFeatures();
      stub();
      try {
        const profile = artifactTransportProfile();
        expect(profile.acceptedEncodings).toEqual(accepted);
        // Either is read in a plan whether or not this page's request
        // named it.
        expect(profile.readableEncodings).toEqual([
          "packed-brotli-large-window",
          "brotli-large-window",
          "zstd",
          "identity",
        ]);
      } finally {
        vi.unstubAllGlobals();
      }
    },
  );

  describe.each(["brotli-large-window", "packed-brotli-large-window"] as const)(
    "a %s representation",
    (encoding) => {
      const selected = () => {
        const parsed = artifact();
        return {
          ...parsed,
          selectedRepresentation: {
            ...parsed.selectedRepresentation,
            encoding,
          },
        };
      };
      const answer = (headers: Record<string, string>) =>
        vi.fn<typeof fetch>(
          async () =>
            new Response(new Uint8Array(4), {
              status: 200,
              headers: { "content-type": "application/x-test", ...headers },
            }),
        );

      it("is taken from a response of its encoded length under no Content-Encoding", async () => {
        const artifact = selected();
        const response = await fetchArtifact(
          answer({
            "content-length": String(
              artifact.selectedRepresentation.encodedBytes,
            ),
          }),
          transport,
          artifact,
          "application/x-test",
        );
        expect(response.status).toBe(200);
      });

      // A coding the response names is one the browser removes before the page
      // reads the body, and no browser decodes a large-window stream.
      for (const coding of [
        "br",
        "zstd",
        "brotli-large-window",
        "packed-brotli-large-window",
      ])
        it(`is refused under Content-Encoding: ${coding}`, async () => {
          const artifact = selected();
          await expect(
            fetchArtifact(
              answer({
                "content-encoding": coding,
                "content-length": String(
                  artifact.selectedRepresentation.encodedBytes,
                ),
              }),
              transport,
              artifact,
              "application/x-test",
            ),
          ).rejects.toMatchObject({
            code: "INVALID_RESPONSE",
            message: `artifact Content-Encoding does not match selected ${encoding} representation`,
          });
        });

      it("is refused under another Content-Length, or none", async () => {
        const artifact = selected();
        await expect(
          fetchArtifact(
            answer({
              "content-length": String(
                artifact.selectedRepresentation.encodedBytes + 1,
              ),
            }),
            transport,
            artifact,
            "application/x-test",
          ),
        ).rejects.toMatchObject({
          code: "INVALID_RESPONSE",
          message: "artifact encoded Content-Length does not match the plan",
        });
        await expect(
          fetchArtifact(answer({}), transport, artifact, "application/x-test"),
        ).rejects.toMatchObject({
          code: "INVALID_RESPONSE",
          message: "artifact response has no exact encoded Content-Length",
        });
      });
    },
  );

  it("refuses a partial response and one of another media type", async () => {
    const selected = artifact();
    const answer = (status: number, headers: Record<string, string>) =>
      vi.fn<typeof fetch>(
        async () =>
          new Response(new Uint8Array(4), {
            status,
            headers: {
              "content-encoding": "zstd",
              "content-length": String(
                selected.selectedRepresentation.encodedBytes,
              ),
              "content-type": "application/x-test",
              ...headers,
            },
          }),
      );
    const fetched = (fetcher: typeof fetch) =>
      fetchArtifact(fetcher, transport, selected, "application/x-test");
    for (const [status, range] of [
      [206, "bytes 0-3/8"],
      [200, "bytes 0-3/4"],
    ] as const)
      await expect(
        fetched(answer(status, { "content-range": range })),
      ).rejects.toMatchObject({
        code: "INVALID_RESPONSE",
        recovery: "reset",
        message: "artifact response must be a complete HTTP 200 response",
      });
    await expect(
      fetched(answer(200, { "content-type": "application/octet-stream" })),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "artifact response has an unexpected Content-Type",
    });
    // The media type is compared without its parameters or its case.
    await expect(
      fetched(answer(200, { "content-type": "Application/X-Test; q=1" })),
    ).resolves.toBeInstanceOf(Response);
  });

  it("requests the selected representation with the caller's signal", async () => {
    const selected = artifact();
    const controller = new AbortController();
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(new Uint8Array(4), {
          status: 200,
          headers: {
            "content-encoding": "zstd",
            "content-length": String(
              selected.selectedRepresentation.encodedBytes,
            ),
            "content-type": "application/x-test",
          },
        }),
    );
    await fetchArtifact(
      fetcher,
      transport,
      selected,
      "application/x-test",
      controller.signal,
    );
    expect(fetcher).toHaveBeenCalledWith(
      selected.selectedRepresentation.downloadUrl,
      expect.objectContaining({ method: "GET", signal: controller.signal }),
    );
  });
});

describe("submitMutationBatch", () => {
  function batch(mutationId = "m-1") {
    return create(mutation.MutationBatchRequestSchema, {
      protocolVersion: 1,
      clientInstanceId: "instance-1",
      deviceId: "device-a",
      authorizationScopeToken: new TextEncoder().encode("scope"),
      mutations: [
        create(mutation.MutationEnvelopeSchema, {
          mutationId,
          commandName: "crud:public.tasks",
          commandVersion: 1,
          canonicalInput: Uint8Array.of(1),
          baseCursor: create(mutation.CursorSchema, {
            token: Uint8Array.of(9),
          }),
          createdAtUnixMicros: 1n,
          declaredFootprint: create(mutation.MutationFootprintSchema, {
            rows: [],
            tableIds: ["public.tasks"],
          }),
        }),
      ],
    });
  }

  function outcomeResponse(
    mutationId: string,
    code: mutation.MutationOutcomeCode,
  ): Response {
    const accepted =
      code === mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO;
    const bytes = toBinary(
      mutation.MutationBatchResponseSchema,
      create(mutation.MutationBatchResponseSchema, {
        protocolVersion: 1,
        serverTimeUnixMicros: 1n,
        outcomes: [
          create(mutation.MutationOutcomeSchema, {
            mutationId,
            code,
            message: "ok",
            serverTransactionId: accepted
              ? new Uint8Array(16)
              : new Uint8Array(),
          }),
        ],
      }),
    );
    return new Response(bytes, { status: 200 });
  }

  it("posts the protobuf batch with the bearer and returns the matching outcome", async () => {
    const fetcher = vi.fn<typeof fetch>(async () =>
      outcomeResponse(
        "m-1",
        mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
      ),
    );
    const outcome = await submitMutationBatch({
      fetch: fetcher,
      endpoint: new URL("https://sync.test/root/"),
      bearerToken: "token",
      request: batch(),
    });
    expect(outcome.code).toBe(
      mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
    );
    const [url, init] = fetcher.mock.calls[0] ?? [];
    expect(String(url)).toBe("https://sync.test/root/v1/mutations");
    // The whole init: no `cache` mode, since Chromium skips its preflight
    // cache for a no-store request. The response's own Cache-Control keeps it
    // out of the HTTP cache.
    expect(init).toStrictEqual({
      method: "POST",
      headers: {
        authorization: "Bearer token",
        "content-type": "application/vnd.calque.mutation.v1+protobuf",
        accept: "application/vnd.calque.mutation.v1+protobuf",
      },
      body: expect.any(Uint8Array),
      signal: expect.any(AbortSignal),
    });
    const sent = fromBinary(
      mutation.MutationBatchRequestSchema,
      new Uint8Array(init?.body as ArrayBuffer),
    );
    expect(sent.mutations[0]?.mutationId).toBe("m-1");
    expect(init?.signal).toBeInstanceOf(AbortSignal);
  });

  it("classifies HTTP refusals: 401/403 auth, 400/404/422 definitive, others ambiguous", async () => {
    const submit = (status: number) =>
      submitMutationBatch({
        fetch: async () => new Response("no", { status }),
        endpoint: new URL("https://sync.test/root/"),
        bearerToken: "token",
        request: batch(),
      });
    await expect(submit(401)).rejects.toMatchObject({
      code: "AUTH_FAILED",
      mutationId: "m-1",
    });
    await expect(submit(403)).rejects.toMatchObject({ code: "AUTH_FAILED" });
    await expect(submit(400)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    await expect(submit(422)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    // 404 is what a deployment answers a caller holding no write capability,
    // and what one with no route at all answers; neither ran a command, so a
    // retry can never succeed and the delta must be dropped.
    await expect(submit(404)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
      message: "this deployment serves no mutation route for these credentials",
    });
    await expect(submit(500)).rejects.toMatchObject({
      code: "MUTATION_AMBIGUOUS",
    });
    await expect(
      submitMutationBatch({
        fetch: async () => {
          throw new TypeError("network down");
        },
        endpoint: new URL("https://sync.test/root/"),
        bearerToken: "token",
        request: batch(),
      }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
  });

  it("refuses a batch of other than one mutation before any request", async () => {
    const fetcher = vi.fn<typeof fetch>();
    const empty = batch();
    empty.mutations = [];
    const two = batch();
    two.mutations = [...two.mutations, ...batch("m-2").mutations];
    for (const request of [empty, two])
      await expect(
        submitMutationBatch({
          fetch: fetcher,
          endpoint: new URL("https://sync.test/root/"),
          bearerToken: "token",
          request,
        }),
      ).rejects.toMatchObject({
        code: "INTERNAL_ERROR",
        scope: "mutation",
        recovery: "none",
        message: "a mutation batch submits exactly one mutation",
      });
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("abandons a request unanswered after 30 s as ambiguous", async () => {
    vi.useFakeTimers();
    try {
      let signal: AbortSignal | undefined;
      const submitted = submitMutationBatch({
        fetch: (_input, init) =>
          new Promise((_resolve, reject) => {
            signal = init?.signal ?? undefined;
            signal?.addEventListener("abort", () => reject(signal?.reason), {
              once: true,
            });
          }),
        endpoint: new URL("https://sync.test/root/"),
        bearerToken: "token",
        request: batch(),
      });
      const settled = expect(submitted).rejects.toMatchObject({
        code: "MUTATION_AMBIGUOUS",
        recovery: "retry",
        mutationId: "m-1",
        cause: { code: "CANCELLED" },
      });
      await vi.advanceTimersByTimeAsync(29_999);
      expect(signal?.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(signal?.aborted).toBe(true);
      await settled;
    } finally {
      vi.useRealTimers();
    }
  });

  it("treats a response it cannot read or that breaks the protocol as ambiguous", async () => {
    const submit = (response: Response) =>
      submitMutationBatch({
        fetch: async () => response,
        endpoint: new URL("https://sync.test/root/"),
        bearerToken: "token",
        request: batch(),
      });
    const unreadable = {
      code: "MUTATION_AMBIGUOUS",
      recovery: "retry",
      mutationId: "m-1",
      message: "mutation response could not be read; retryMutation resends it",
    };
    const cut = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.error(new TypeError("network error"));
      },
    });
    await expect(submit(new Response(cut))).rejects.toMatchObject(unreadable);
    await expect(
      submit(new Response(new Uint8Array(1024 * 1024 + 1))),
    ).rejects.toMatchObject(unreadable);
    const invalid = {
      code: "MUTATION_AMBIGUOUS",
      recovery: "retry",
      mutationId: "m-1",
      message: "mutation response was invalid; retryMutation resends it",
    };
    await expect(
      submit(new Response(Uint8Array.of(0xff, 0xff, 0xff))),
    ).rejects.toMatchObject(invalid);
    const otherVersion = toBinary(
      mutation.MutationBatchResponseSchema,
      create(mutation.MutationBatchResponseSchema, {
        protocolVersion: 2,
        serverTimeUnixMicros: 1n,
        outcomes: [
          create(mutation.MutationOutcomeSchema, {
            mutationId: "m-1",
            code: mutation.MutationOutcomeCode.REJECTED_AUTHORIZATION,
            message: "no",
          }),
        ],
      }),
    );
    await expect(submit(new Response(otherVersion))).rejects.toMatchObject(
      invalid,
    );
  });

  it("treats an outcome for another mutation id as ambiguous", async () => {
    await expect(
      submitMutationBatch({
        fetch: async () =>
          outcomeResponse(
            "m-2",
            mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
          ),
        endpoint: new URL("https://sync.test/root/"),
        bearerToken: "token",
        request: batch("m-1"),
      }),
    ).rejects.toMatchObject({
      code: "MUTATION_AMBIGUOUS",
      message: expect.stringContaining("did not match"),
    });
  });

  it("maps every outcome code to its application result", () => {
    const outcome = (code: mutation.MutationOutcomeCode) =>
      create(mutation.MutationOutcomeSchema, {
        mutationId: "m-1",
        code,
        message: "why",
      });
    expect(
      mutationOutcomeResult(
        outcome(mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO),
      ),
    ).toEqual({
      mutationId: "m-1",
      state: "awaiting_canonical_echo",
      message: "why",
    });
    expect(
      mutationOutcomeResult(
        outcome(mutation.MutationOutcomeCode.REJECTED_AUTHORIZATION),
      ).state,
    ).toBe("rejected");
    expect(
      mutationOutcomeResult(
        outcome(mutation.MutationOutcomeCode.COMMAND_VERSION_UNSUPPORTED),
      ).state,
    ).toBe("rejected");
    // A malformed outcome is a response the client could not read: the
    // mutation may have committed, so it is ambiguous and `retryMutation`
    // resends it.
    expect(() =>
      mutationOutcomeResult(outcome(mutation.MutationOutcomeCode.CONFLICT)),
    ).toThrow(
      expect.objectContaining({
        code: "MUTATION_AMBIGUOUS",
        recovery: "retry",
        mutationId: "m-1",
        message: expect.stringContaining("omitted conflict details"),
      }),
    );
    expect(() =>
      mutationOutcomeResult(
        outcome(mutation.MutationOutcomeCode.RETRYABLE_FAILURE),
      ),
    ).toThrow(
      expect.objectContaining({
        code: "MUTATION_AMBIGUOUS",
        recovery: "retry",
        mutationId: "m-1",
        message: expect.stringContaining("retry delay"),
      }),
    );
    expect(
      mutationOutcomeIsDefinitive(
        mutation.MutationOutcomeCode.RETRYABLE_FAILURE,
      ),
    ).toBe(false);
    expect(
      mutationOutcomeIsDefinitive(mutation.MutationOutcomeCode.CONFLICT),
    ).toBe(true);
  });
});

/**
 * The body of a representation the page passes on rather than keeps, as the
 * session reads a `brotli-large-window` one for its decoder worker.
 */
describe("exactResponsePieces", () => {
  /** A body delivered as `pieces`, and whether its reader was cancelled. */
  function piecedResponse(pieces: readonly Uint8Array[], fail?: unknown) {
    let next = 0;
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        const piece = pieces[next];
        next += 1;
        if (piece) controller.enqueue(piece);
        else if (fail === undefined) controller.close();
        else controller.error(fail);
      },
      cancel() {
        cancelled = true;
      },
    });
    return { response: new Response(stream), cancelled: () => cancelled };
  }

  it("passes each read's bytes on in its own buffer and marks the last", async () => {
    const first = Uint8Array.of(1, 2, 3);
    const { response } = piecedResponse([
      first,
      new Uint8Array(0),
      Uint8Array.of(4, 5),
    ]);
    const body = exactResponsePieces(response, 5);
    const one = await body.read();
    const two = await body.read();
    expect(one?.end).toBe(false);
    // The buffer the stream delivered, not a copy of it.
    expect(one?.bytes).toBe(first.buffer);
    expect([...new Uint8Array(two?.bytes ?? new ArrayBuffer(0))]).toEqual([
      4, 5,
    ]);
    expect(two?.end).toBe(true);
    expect(await body.read()).toBeUndefined();
  });

  it("copies a read that is a view of part of a larger buffer", async () => {
    const backing = Uint8Array.of(9, 1, 2, 9);
    // A `Buffer`'s own `slice` is another view, so the copy cannot be one.
    for (const view of [
      backing.subarray(1, 3),
      Buffer.from(backing.buffer, 1, 2),
    ]) {
      const { response } = piecedResponse([view]);
      const piece = await exactResponsePieces(response, 2).read();
      expect(piece?.bytes.byteLength).toBe(2);
      expect([...new Uint8Array(piece?.bytes ?? new ArrayBuffer(0))]).toEqual([
        1, 2,
      ]);
    }
    expect([...backing]).toEqual([9, 1, 2, 9]);
  });

  it("fails closed when the body ends short of the declared length", async () => {
    const { response } = piecedResponse([Uint8Array.of(1, 2, 3)]);
    const body = exactResponsePieces(response, 4);
    expect(await body.read()).toMatchObject({ end: false });
    await expect(body.read()).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "artifact response contained 3 of 4 bytes",
    });
  });

  it("cancels the reader and fails closed when the body runs past the declared length", async () => {
    // A third piece keeps the source open, so the cancel reaches it.
    const source = piecedResponse([
      Uint8Array.of(1, 2, 3),
      Uint8Array.of(4),
      Uint8Array.of(5),
    ]);
    const body = exactResponsePieces(source.response, 3);
    expect(await body.read()).toMatchObject({ end: true });
    await expect(body.read()).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "artifact response exceeds encoded length",
    });
    expect(source.cancelled()).toBe(true);
  });

  it("reports a body the network cut as a network error", async () => {
    const { response } = piecedResponse(
      [Uint8Array.of(1)],
      new TypeError("network error"),
    );
    const body = exactResponsePieces(response, 4);
    await body.read();
    await expect(body.read()).rejects.toMatchObject({
      code: "NETWORK_ERROR",
      scope: "sync",
      message: "artifact response stream failed",
    });
  });

  it("ends without a verdict on its length once cancelled", async () => {
    const source = piecedResponse([
      Uint8Array.of(1),
      Uint8Array.of(2),
      Uint8Array.of(3),
    ]);
    const body = exactResponsePieces(source.response, 4);
    await body.read();
    await body.cancel();
    expect(source.cancelled()).toBe(true);
    expect(await body.read()).toBeUndefined();
  });

  it("refuses a response with no body", () => {
    expect(() => exactResponsePieces(new Response(null), 4)).toThrow(
      "artifact response has no body",
    );
  });
});

/**
 * The bounded-chunk, tamper-cleanup and cancellation branches of the artifact
 * stream the session reads every artifact through, driven on Node's byte
 * streams.
 */
describe("exactResponseBuffers", () => {
  /** A conforming byte stream, plus a record of what the loop asked it for. */
  function byteResponse(body: Uint8Array) {
    const requested: number[] = [];
    const offsets: number[] = [];
    let cancelled: unknown = null;
    let sent = 0;
    const stream = new ReadableStream({
      type: "bytes",
      pull(controller) {
        const byob = controller.byobRequest;
        if (!byob?.view) throw new Error("the loop must read in BYOB mode");
        if (sent >= body.byteLength) {
          controller.close();
          byob.respond(0);
          return;
        }
        requested.push(byob.view.byteLength);
        offsets.push(byob.view.byteOffset);
        const take = Math.min(byob.view.byteLength, body.byteLength - sent);
        new Uint8Array(byob.view.buffer, byob.view.byteOffset, take).set(
          body.subarray(sent, sent + take),
        );
        sent += take;
        byob.respond(take);
      },
      cancel(reason) {
        cancelled = reason ?? "cancelled";
      },
    });
    return {
      response: new Response(stream),
      requested,
      offsets,
      cancelled: () => cancelled,
    };
  }

  /** A pattern the assembled buffer can be compared against byte for byte. */
  function pattern(length: number) {
    const bytes = new Uint8Array(length);
    for (let index = 0; index < length; index += 1) bytes[index] = index % 251;
    return bytes;
  }

  /** Reads `response` to its end in the session's 1 MiB buffers. */
  async function readAll(response: Response, expectedBytes: number) {
    const buffers: Uint8Array[] = [];
    for await (const buffer of exactResponseBuffers(
      response,
      expectedBytes,
      1 << 20,
    ))
      buffers.push(new Uint8Array(buffer));
    return buffers;
  }

  it("reads a multi-chunk body in requests bounded by MAX_ARTIFACT_CHUNK_BYTES", async () => {
    const body = pattern(MAX_ARTIFACT_CHUNK_BYTES * 2 + 1024);
    const source = byteResponse(body);
    const buffers: ArrayBuffer[] = [];
    for await (const buffer of exactResponseBuffers(
      source.response,
      body.byteLength,
      body.byteLength,
    ))
      buffers.push(buffer);
    // Three reads, not one: the loop never asks for the whole body, and the
    // last request is the remainder rather than another full chunk. Each read
    // lands where its bytes belong in the one buffer, so none is copied after.
    expect(source.requested).toEqual([
      MAX_ARTIFACT_CHUNK_BYTES,
      MAX_ARTIFACT_CHUNK_BYTES,
      1024,
    ]);
    expect(source.offsets).toEqual([
      0,
      MAX_ARTIFACT_CHUNK_BYTES,
      MAX_ARTIFACT_CHUNK_BYTES * 2,
    ]);
    expect(buffers.map((buffer) => new Uint8Array(buffer))).toEqual([body]);
  });

  it("fills buffers of the size asked for, the last holding the rest", async () => {
    const body = pattern(MAX_ARTIFACT_CHUNK_BYTES * 2 + 1000);
    const source = byteResponse(body);
    const buffers: Uint8Array[] = [];
    for await (const buffer of exactResponseBuffers(
      source.response,
      body.byteLength,
      MAX_ARTIFACT_CHUNK_BYTES + 5,
    ))
      buffers.push(new Uint8Array(buffer));
    expect(buffers.map(({ byteLength }) => byteLength)).toEqual([
      MAX_ARTIFACT_CHUNK_BYTES + 5,
      MAX_ARTIFACT_CHUNK_BYTES + 5,
      990,
    ]);
    // No read crosses from one buffer into the next.
    expect(source.requested).toEqual([
      MAX_ARTIFACT_CHUNK_BYTES,
      5,
      MAX_ARTIFACT_CHUNK_BYTES,
      5,
      990,
    ]);
    expect(source.offsets).toEqual([
      0,
      MAX_ARTIFACT_CHUNK_BYTES,
      0,
      MAX_ARTIFACT_CHUNK_BYTES,
      0,
    ]);
    const joined = new Uint8Array(body.byteLength);
    let offset = 0;
    for (const buffer of buffers) {
      joined.set(buffer, offset);
      offset += buffer.byteLength;
    }
    expect(joined).toEqual(body);
    await expect(
      exactResponseBuffers(byteResponse(body).response, 1, 0).next(),
    ).rejects.toThrow("at least one byte");
  });

  it("cancels the reader and fails closed when the body runs past the declared length", async () => {
    const body = pattern(MAX_ARTIFACT_CHUNK_BYTES + 64);
    const source = byteResponse(body);
    // The plan declares fewer bytes than the origin serves, as for a tampered
    // or swapped representation. The loop must stop rather than truncate, and
    // its `finally` must cancel the body.
    await expect(
      readAll(source.response, MAX_ARTIFACT_CHUNK_BYTES),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "artifact response exceeds decoded length",
    });
    expect(source.cancelled()).not.toBeNull();
  });

  it("fails closed when the body ends short of the declared length", async () => {
    const body = pattern(1024);
    const source = byteResponse(body);
    await expect(readAll(source.response, 4096)).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "artifact response contained 1024 of 4096 bytes",
    });
  });

  it("cancels the reader when the consumer abandons the stream mid-body", async () => {
    const body = pattern(MAX_ARTIFACT_CHUNK_BYTES * 3);
    const source = byteResponse(body);
    const chunks = exactResponseBuffers(
      source.response,
      body.byteLength,
      MAX_ARTIFACT_CHUNK_BYTES,
    );
    const first = await chunks.next();
    expect(first.done).toBe(false);
    expect(source.cancelled()).toBeNull();
    // What a caller's abort does to the generator: the body is still open, so
    // the `finally` owes the stream a cancel rather than a leaked reader.
    await chunks.return(undefined);
    expect(source.cancelled()).not.toBeNull();
  });

  it("cancels the reader and fails closed on a chunk larger than the bound", async () => {
    // A conforming byte stream cannot overfill a BYOB view, so the violation
    // has to come from a reader that ignores the contract.
    let cancelled = false;
    const oversized = {
      body: {
        getReader: () => ({
          read: async () => ({
            done: false,
            value: new Uint8Array(MAX_ARTIFACT_CHUNK_BYTES + 1),
          }),
          cancel: async () => {
            cancelled = true;
          },
          releaseLock: () => undefined,
        }),
      },
    } as unknown as Response;
    await expect(
      readAll(oversized, MAX_ARTIFACT_CHUNK_BYTES * 4),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "artifact response violated the bounded BYOB contract",
    });
    expect(cancelled).toBe(true);
  });

  it("cancels the reader and fails closed on bytes outside the buffer it read into", async () => {
    // A reader that answers with bytes at the wrong place in a buffer, or in
    // a buffer of another size, hands back bytes the loop never placed. It
    // ends the body after that one answer, so a loop that accepted it would
    // fail on the length instead.
    for (const value of [
      new Uint8Array(new ArrayBuffer(4096), 8, 16),
      new Uint8Array(new ArrayBuffer(8192), 0, 16),
    ]) {
      let cancelled = false;
      let answered = false;
      const misplaced = {
        body: {
          getReader: () => ({
            read: async () => {
              if (answered) return { done: true, value: undefined };
              answered = true;
              return { done: false, value };
            },
            cancel: async () => {
              cancelled = true;
            },
            releaseLock: () => undefined,
          }),
        },
      } as unknown as Response;
      await expect(readAll(misplaced, 4096)).rejects.toMatchObject({
        code: "INVALID_RESPONSE",
        message: "artifact response violated the bounded BYOB contract",
      });
      expect(cancelled).toBe(true);
    }
  });

  it("refuses a response whose body cannot be read in BYOB mode", async () => {
    const refuses = {
      body: {
        getReader: () => {
          throw new TypeError("byob unsupported");
        },
      },
    } as unknown as Response;
    await expect(readAll(refuses, 1024)).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      message: "artifact response does not support bounded BYOB reads",
    });
  });
});
