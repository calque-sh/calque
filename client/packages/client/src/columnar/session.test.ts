import { create, fromBinary } from "@bufbuild/protobuf";
import { MAX_QUERY_SPEC_BYTES, type QuerySpecification } from "@calque/core";
import { change, decodePrimaryKey, mutation } from "@calque/protocol";
import { HASHER_CREDIT_MS } from "@calque/worker/columnar";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  createInternalColumnarClient,
  type SyncPublication,
} from "../client.js";
import { createGeneratedClientSchema } from "../client-options.js";
import { SyncError } from "../error.js";
import { eq } from "../facade/predicates.js";
import { MutationSubmissionError, type MutationStatus } from "../mutation.js";
import type { ColumnarEngineWorker, ColumnarHasherWorker } from "./worker.js";
import {
  type EventRow,
  type Collections,
  type WorkerRequest,
  decoder,
  MOUNT_CURSOR,
  FakeWorker,
  FakeHasher,
  fakeHashers,
  descriptor,
  ARTIFACT_BYTES,
  plan,
  byteStream,
  opaqueBatch,
  abortingLongPoll,
  releasableChangePolls,
  controllableLongPoll,
  decodeChangeRequest,
  validatedFetch,
  fencedPlan,
  outcomeResponder,
  acceptedResponder,
  until,
  fakeFetch,
  CHUNK_BYTES,
  LONG_ARTIFACT_BYTES,
  chunkDigests,
  longPlan,
  patternBytes,
  stubThreads,
  PACED_BYTES,
  servePaced,
  serveArtifact,
  makeAttemptedClient,
  makeClient,
  makePublishingClient,
  makeRebuildableClient,
  settled,
  type Frame,
  type PaintedFrame,
  renderTwoCollections,
} from "./session.fixture.js";

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

  it("activates the served artifact and answers typed facade queries", async () => {
    const { client, worker, fetcher, workerFactory } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(workerFactory).toHaveBeenCalledTimes(1);
    const open = worker.requests[0];
    expect(open?.type).toBe("open");
    expect(open?.trusted).toMatchObject({
      byteLength: ARTIFACT_BYTES,
      digest: `sha256:${"b".repeat(64)}`,
      identity: {
        sourceEpoch: "epoch-1",
        manifestVersion: "manifest-1",
        authorizationScopeVersion: "scope-1",
        authorizationScopeHash: `sha256:${"a".repeat(64)}`,
        cursor: "cursor-p",
      },
    });
    expect(open?.changeStream).toMatchObject({
      planId: "plan-1",
      tables: [
        { tableId: "public.events", collection: "events", primaryKey: [1] },
        { tableId: "public.issues", collection: "issues", primaryKey: [1] },
      ],
    });
    expect(
      fetcher.mock.calls.filter(([input]) => String(input).endsWith("/change")),
    ).toHaveLength(1);
    const bootstrap = fetcher.mock.calls[0];
    expect(String(bootstrap?.[0])).toBe("https://sync.test/root/v1/bootstrap");
    const body = JSON.parse(String(bootstrap?.[1]?.body)) as {
      requiredTables: string[];
    };
    expect(body.requiredTables).toEqual(["public.events", "public.issues"]);

    const rows = await client.db.events
      .query()
      .where(eq("level", "error"))
      .run();
    expect(rows).toEqual([{ id: 1n, level: "error" }]);
    const level: string = rows[0]?.level ?? "";
    expect(level).toBe("error");
    const query = worker.requests.find((request) => request.type === "query");
    const specification = query?.specification as QuerySpecification;
    expect(specification.from).toEqual({ collection: "events", as: "events" });
    expect(specification.where).toMatchObject({
      kind: "compare",
      operator: "eq",
    });

    const window = await client.db.issues.slice(0, 10);
    expect(window).toEqual([{ id: 2n, title: "warn" }]);
    const rowsRequest = worker.requests.find(
      (request) => request.type === "rows",
    );
    expect(rowsRequest).toMatchObject({
      table: "issues",
      start: 0,
      count: 1,
      fields: ["id", "title"],
    });
    await client.close();
    expect(worker.terminated).toBe(true);
    expect(client.status).toEqual({ state: "closed" });
  });

  it("publishes CLOSED to open live queries on close, and refuses every later call with it", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const resource = client.db.events.query().subscribe();
    const stop = resource.subscribe(() => undefined);
    await resource.promise;
    expect(resource.getSnapshot().status).toBe("ready");
    await client.close();
    const closed = { code: "CLOSED", scope: "client", recovery: "none" };
    expect(resource.getSnapshot()).toMatchObject({
      status: "error",
      error: closed,
    });
    expect(client.status).toEqual({ state: "closed" });
    expect(worker.terminated).toBe(true);
    await expect(client.db.events.query().run()).rejects.toMatchObject(closed);
    expect(() => client.db.events.query().subscribe()).toThrow(
      expect.objectContaining(closed),
    );
    await expect(client.db.events.slice(0, 1)).rejects.toMatchObject(closed);
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject(closed);
    await expect(client.refresh()).rejects.toMatchObject(closed);
    await expect(client.reset()).rejects.toMatchObject(closed);
    await client.close();
    expect(client.status).toEqual({ state: "closed" });
    stop();
  });

  it.each([
    {
      code: "RESULT_TOO_LARGE",
      name: "QueryError",
      expected: "RESULT_TOO_LARGE",
    },
    { code: "INVALID_QUERY", name: "QueryError", expected: "INVALID_QUERY" },
    {
      code: "CAPACITY_EXCEEDED",
      name: "QueryError",
      expected: "CAPACITY_EXCEEDED",
    },
    { code: undefined, name: "TypeError", expected: "QUERY_FAILED" },
  ] as const)(
    "reports a query the engine refuses as $expected",
    async ({ code, name, expected }) => {
      const { client, worker } = makeClient();
      expect(await settled(client)).toEqual({ state: "ready" });
      worker.queryError = {
        name,
        message: "the engine's reason",
        ...(code === undefined ? {} : { code }),
      };
      const error = { code: expected, scope: "query", recovery: "none" };
      await expect(client.db.events.query().run()).rejects.toMatchObject({
        ...error,
        message: "the engine's reason",
      });
      const resource = client.db.events.query().subscribe();
      const stop = resource.subscribe(() => undefined);
      await resource.promise.catch(() => undefined);
      expect(resource.getSnapshot()).toMatchObject({ status: "error", error });
      // A refused query leaves the mount serving.
      expect(client.status).toEqual({ state: "ready" });
      stop();
      await client.close();
    },
  );

  it.each([
    {
      returned: "a string",
      value: "token",
      message:
        "auth() must return an object with an own bearerToken data property",
    },
    {
      returned: "an object that cannot be inspected",
      value: new Proxy(
        {},
        {
          getOwnPropertyDescriptor() {
            throw new Error("trap");
          },
        },
      ),
      message:
        "auth() must return an object whose own bearerToken property can be inspected",
    },
    {
      returned: "an inherited bearerToken",
      value: Object.create({ bearerToken: "token" }) as object,
      message: "auth() bearerToken must be an own data property",
    },
    {
      returned: "a bearerToken getter",
      value: {
        get bearerToken() {
          return "token";
        },
      },
      message: "auth() bearerToken must be an own data property",
    },
    {
      returned: "an empty bearerToken",
      value: { bearerToken: "" },
      message: "auth() bearerToken must be a non-empty string",
    },
    {
      returned: "an oversized bearerToken",
      value: { bearerToken: "t".repeat(16_385) },
      message: "auth() bearerToken exceeds its 16384 code-unit limit",
    },
    {
      returned: "a bearerToken with a control character",
      value: { bearerToken: "token\n" },
      message: "auth() bearerToken must not contain control characters",
    },
  ])(
    "refuses $returned from auth() as AUTH_FAILED before any request",
    async ({ value, message }) => {
      const { client, fetcher, workerFactory } = makeClient(
        plan(),
        undefined,
        undefined,
        undefined,
        { auth: () => value },
      );
      expect(await settled(client)).toMatchObject({
        state: "error",
        error: {
          code: "AUTH_FAILED",
          scope: "auth",
          recovery: "none",
          message,
        },
      });
      expect(fetcher).not.toHaveBeenCalled();
      expect(workerFactory).not.toHaveBeenCalled();
      await client.close();
    },
  );

  it("sends a bearerToken of exactly 16384 code units", async () => {
    const bearerToken = "t".repeat(16_384);
    const { client, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      undefined,
      { auth: () => ({ bearerToken }) },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(fetcher.mock.calls[0]?.[1]?.headers).toMatchObject({
      authorization: `Bearer ${bearerToken}`,
    });
    await client.close();
  });

  it("refuses bad mutations without reaching the engine", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    // A row the command cannot write is refused by the facade itself.
    await expect(
      client.db.events.insert({ level: "error" } as unknown as EventRow),
    ).rejects.toThrow("insert row must carry id");
    await expect(
      client.db.events.update({ id: 1n }, { id: 2n } as unknown as {
        level: string;
      }),
    ).rejects.toThrow("not writable");
    expect(
      worker.requests
        .map((request) => request.type)
        .filter((type) => !["open", "artifact-end"].includes(type)),
    ).toEqual([]);
    await client.close();
  });

  it("applies an update optimistically, submits it, and confirms it only on the echo", async () => {
    const poll = controllableLongPoll();
    const { client, worker, fetcher } = makeClient(plan(), poll.fetcher);
    expect(await settled(client)).toEqual({ state: "ready" });
    await until(() => poll.open === 1, "the first poll");
    expect(poll.requests[0]?.pendingMutationIds).toEqual([]);

    const resource = client.db.events.query().subscribe();
    const unsubscribeQuery = resource.subscribe(() => undefined);
    await resource.promise;
    const frames = () =>
      worker.requests.filter((request) => request.type === "frame");
    expect(frames()).toHaveLength(1);

    const seen: MutationStatus[] = [];
    const stopWatching = client.subscribeMutations((status) => {
      seen.push(status);
    });
    const pending = client.db.events.update({ id: 1n }, { level: "warn" });
    await until(
      () => worker.requests.some((request) => request.type === "mutate"),
      "the optimistic apply",
    );
    const mutate = worker.requests.find((request) => request.type === "mutate");
    const mutationId = mutate?.mutationId as string;
    expect(typeof mutationId).toBe("string");
    // The overlay is written first, with the wire operation the echo replays.
    const operations = mutate?.operations as Uint8Array[];
    expect(operations).toHaveLength(1);
    const operation = fromBinary(
      change.OperationSchema,
      operations[0] ?? new Uint8Array(),
    );
    expect(operation.tableId).toBe("public.events");
    expect(operation.kind).toBe(change.OperationKind.UPDATE_PATCH);
    expect(decodePrimaryKey(operation.primaryKey).parts).toMatchObject([
      { columnId: 1, value: { kind: { case: "int64Value", value: 1n } } },
    ]);
    expect(operation.payload).toMatchObject({
      case: "update",
      value: {
        changedColumns: [
          {
            columnId: 2,
            value: { kind: { case: "textValue", value: "warn" } },
          },
        ],
      },
    });
    await until(
      () => client.mutationStatus(mutationId) !== undefined,
      "the optimistic status",
    );
    expect(seen[0]).toEqual({
      mutationId,
      collection: "events",
      key: { id: 1n },
      state: "submitting",
    });
    // The subscribed query re-ran against the overlay before any POST.
    await until(() => frames().length === 2, "the optimistic re-run");

    // Then the POST, carrying the same bytes as its canonical input.
    await until(
      () =>
        fetcher.mock.calls.some(([input]) =>
          String(input).endsWith("/v1/mutations"),
        ),
      "the submission",
    );
    const post = fetcher.mock.calls.find(([input]) =>
      String(input).endsWith("/v1/mutations"),
    );
    expect(post?.[1]?.headers).toMatchObject({ authorization: "Bearer token" });
    const request = fromBinary(
      mutation.MutationBatchRequestSchema,
      new Uint8Array(post?.[1]?.body as ArrayBuffer),
    );
    expect(request.deviceId).toBe("device-a");
    expect(new TextDecoder().decode(request.authorizationScopeToken)).toBe(
      "scope-secret",
    );
    const envelope = request.mutations[0];
    expect(envelope).toMatchObject({
      mutationId,
      commandName: "crud:public.events",
      commandVersion: 1,
    });
    expect(envelope?.canonicalInput).toEqual(operations[0]);
    expect(envelope?.baseCursor?.token).toEqual(MOUNT_CURSOR);
    expect(envelope?.declaredFootprint).toMatchObject({
      tableIds: ["public.events"],
      rows: [{ tableId: "public.events", primaryKey: operation.primaryKey }],
    });

    // Accepted is not confirmed: the status waits for the echo and the delta
    // stays in the overlay.
    const result = await pending;
    expect(result).toMatchObject({
      mutationId,
      state: "awaiting_canonical_echo",
    });
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(client.mutationStatus(mutationId)).toMatchObject({
      state: "awaiting_canonical_echo",
    });
    expect(worker.pendingIds).toEqual([mutationId]);
    // The nudge restarted the poll so the pending id rides the request.
    await until(() => poll.open === 1, "the restarted poll");
    expect(poll.requests.at(-1)?.pendingMutationIds).toEqual([mutationId]);

    let notifications = 0;
    const unsubscribe = client.subscribeMutation(mutationId, () => {
      notifications += 1;
    });
    poll.answer();
    await until(
      () => client.mutationStatus(mutationId)?.state === "canonical",
      "the canonical echo",
    );
    expect(notifications).toBe(1);
    expect(seen.map(({ state }) => state)).toEqual([
      "submitting",
      "awaiting_canonical_echo",
      "canonical",
    ]);
    expect(
      seen.every(
        ({ collection, key }) => collection === "events" && key.id === 1n,
      ),
    ).toBe(true);
    stopWatching();
    expect(worker.pendingIds).toEqual([]);
    expect(
      worker.requests.filter((request) => request.type === "drop"),
    ).toEqual([]);
    unsubscribe();
    unsubscribeQuery();
    await client.close();
  });

  it("drops a definitively refused mutation and refolds the survivors", async () => {
    const { client, worker } = makeClient(
      plan(),
      undefined,
      outcomeResponder(mutation.MutationOutcomeCode.REJECTED_VALIDATION),
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const result = await client.db.events.delete({ id: 2n });
    expect(result).toMatchObject({ state: "rejected" });
    expect(worker.requests.map((request) => request.type)).toEqual([
      "open",
      "artifact-end",
      "mutate",
      "drop",
    ]);
    expect(worker.pendingIds).toEqual([]);
    expect(client.mutationStatus(result.mutationId)).toMatchObject({
      collection: "events",
      key: { id: 2n },
      state: "rejected",
      result,
    });
    await client.close();
  });

  it("keeps a conflicting mutation's status with the conflict the server named", async () => {
    const { client, worker } = makeClient(
      plan(),
      undefined,
      outcomeResponder(mutation.MutationOutcomeCode.CONFLICT, {
        conflict: create(mutation.ConflictSchema, {
          kind: "row-missing",
          applicationPayload: new Uint8Array(),
        }),
      }),
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const result = await client.db.events.update({ id: 9n }, { level: "x" });
    expect(result).toMatchObject({
      state: "conflict",
      conflict: { kind: "row-missing" },
    });
    expect(worker.pendingIds).toEqual([]);
    expect(client.mutationStatus(result.mutationId)).toMatchObject({
      collection: "events",
      key: { id: 9n },
      state: "conflict",
    });
    await client.close();
  });

  it("applies an echo that names a foreign mutation canonically and leaves ours pending", async () => {
    const poll = controllableLongPoll();
    const { client, worker } = makeClient(plan(), poll.fetcher);
    expect(await settled(client)).toEqual({ state: "ready" });
    const result = await client.db.events.update({ id: 1n }, { level: "warn" });
    await until(
      () =>
        poll.open === 1 &&
        poll.requests.at(-1)?.pendingMutationIds.length === 1,
      "the poll carrying the pending id",
    );
    worker.echo = ["someone-elses-mutation"];
    poll.answer();
    await until(() => worker.generation >= 3, "the echo to apply");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.mutationStatus(result.mutationId)?.state).toBe(
      "awaiting_canonical_echo",
    );
    expect(client.mutationStatus("someone-elses-mutation")).toBeUndefined();
    expect(worker.pendingIds).toEqual([result.mutationId]);
    await client.close();
  });

  it("fails closed when the mutation endpoint refuses the credentials", async () => {
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async () => new Response("nope", { status: 401 }),
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "AUTH_FAILED", scope: "auth" });
    expect(client.status).toMatchObject({
      state: "error",
      error: { code: "AUTH_FAILED" },
    });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;
    expect(client.mutationStatus(mutationId)?.state).toBe("ambiguous");
    await client.close();
  });

  it("resubmits the identical envelope under the same id after a lost response", async () => {
    // A lost response may hide a commit, so the retry resends the same
    // envelope under the same id for the server to answer from its receipt.
    // This asserts the bytes, not just that a POST happened.
    const submitted: Uint8Array[] = [];
    let dropTheResponse = true;
    const replaying = outcomeResponder(
      mutation.MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
    );
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submitted.push(new Uint8Array(init?.body as ArrayBuffer));
        if (dropTheResponse) {
          dropTheResponse = false;
          throw new TypeError("fetch failed");
        }
        return replaying(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;
    expect(client.mutationStatus(mutationId)?.state).toBe("ambiguous");
    // The delta survives the ambiguity: dropping it would lose a write that
    // may already be canonical.
    expect(worker.pendingIds).toEqual([mutationId]);

    const retried = await client.retryMutation(mutationId);
    expect(retried).toMatchObject({
      mutationId,
      state: "awaiting_canonical_echo",
    });
    expect(submitted).toHaveLength(2);
    expect(submitted[1]).toEqual(submitted[0]);
    expect(worker.pendingIds).toEqual([mutationId]);
    expect(client.mutationStatus(mutationId)?.state).toBe(
      "awaiting_canonical_echo",
    );

    // Nothing is left to resubmit once the envelope has been answered.
    await expect(client.retryMutation(mutationId)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    await client.close();
  });

  it("keeps a mutation answered with a malformed outcome for retryMutation", async () => {
    // A conflict outcome without its conflict details breaks the protocol.
    // The answer may stand for a commit the client cannot read, so the error
    // is ambiguous with recovery `retry`, and `retryMutation` must honour
    // that by resending the envelope instead of refusing it.
    const submitted: Uint8Array[] = [];
    const malformed = outcomeResponder(mutation.MutationOutcomeCode.CONFLICT);
    const replaying = outcomeResponder(
      mutation.MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
    );
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submitted.push(new Uint8Array(init?.body as ArrayBuffer));
        return (submitted.length === 1 ? malformed : replaying)(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({
      code: "MUTATION_AMBIGUOUS",
      recovery: "retry",
      message: expect.stringContaining("invalid"),
    });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;
    expect(client.mutationStatus(mutationId)?.state).toBe("ambiguous");
    expect(worker.pendingIds).toEqual([mutationId]);

    await expect(client.retryMutation(mutationId)).resolves.toMatchObject({
      mutationId,
      state: "awaiting_canonical_echo",
    });
    expect(submitted).toHaveLength(2);
    expect(submitted[1]).toEqual(submitted[0]);
    await client.close();
  });

  /**
   * A mutation whose response was lost, with its delta still riding `poll`,
   * and a mutation endpoint that answers the resend ALREADY_CANONICAL only
   * when the test says so. The first submission is the lost response.
   */
  async function lostThenAlreadyCanonical() {
    const poll = controllableLongPoll();
    const alreadyCanonical = outcomeResponder(
      mutation.MutationOutcomeCode.ALREADY_CANONICAL,
    );
    let submissions = 0;
    let answerResend: (() => void) | undefined;
    const { client, worker } = makeClient(
      plan(),
      poll.fetcher,
      async (input, init) => {
        submissions += 1;
        if (submissions === 1) throw new TypeError("fetch failed");
        await new Promise<void>((resolve) => {
          answerResend = resolve;
        });
        return alreadyCanonical(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;
    await until(
      () =>
        poll.open === 1 &&
        poll.requests.at(-1)?.pendingMutationIds.includes(mutationId) === true,
      "the poll carrying the pending id",
    );
    return {
      client,
      worker,
      poll,
      mutationId,
      submissions: () => submissions,
      answerResend: async () => {
        await until(() => answerResend !== undefined, "the resend");
        answerResend?.();
      },
    };
  }

  it("stays ready when a resend is answered already_canonical after its echo", async () => {
    // The first attempt committed and only its response was lost, so its echo
    // and the resend race. When the echo lands first it removes the delta,
    // and the overlay refuses to drop a mutation it no longer holds: dropping
    // on the ALREADY_CANONICAL answer would turn a won race into a failed
    // session.
    const { client, worker, poll, mutationId, answerResend } =
      await lostThenAlreadyCanonical();
    const retrying = client.retryMutation(mutationId);
    await until(
      () => client.mutationStatus(mutationId)?.state === "submitting",
      "the resend",
    );
    poll.answer();
    await until(
      () => client.mutationStatus(mutationId)?.state === "canonical",
      "the echo",
    );
    expect(worker.pendingIds).toEqual([]);

    await answerResend();
    expect(await retrying).toMatchObject({
      mutationId,
      state: "already_canonical",
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.status).toEqual({ state: "ready" });
    expect(client.mutationStatus(mutationId)?.state).toBe("canonical");
    expect(
      worker.requests.filter((request) => request.type === "drop"),
    ).toEqual([]);
    await client.close();
  });

  it("keeps the delta of a resend answered already_canonical until its echo", async () => {
    // The receipt says the command committed, not that this page has the
    // row: the echo carrying it may still be on the way. Dropping the delta
    // on the answer would hide the row until then; keeping it lets the echo
    // replace it in one step, as it does after an accepted outcome.
    const { client, worker, poll, mutationId, answerResend } =
      await lostThenAlreadyCanonical();
    const retrying = client.retryMutation(mutationId);
    await answerResend();
    expect(await retrying).toMatchObject({
      mutationId,
      state: "already_canonical",
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(worker.pendingIds).toEqual([mutationId]);
    expect(client.mutationStatus(mutationId)?.state).toBe(
      "awaiting_canonical_echo",
    );

    poll.answer();
    await until(
      () => client.mutationStatus(mutationId)?.state === "canonical",
      "the echo",
    );
    expect(worker.pendingIds).toEqual([]);
    expect(client.status).toEqual({ state: "ready" });
    expect(
      worker.requests.filter((request) => request.type === "drop"),
    ).toEqual([]);
    await client.close();
  });

  it("does not resend a mutation the echo confirmed while the retry waited", async () => {
    // `retryMutation` waits for the mount and the credentials before it
    // resends. An echo that lands in that wait has settled the mutation, and
    // resending would put its status back to awaiting an echo already
    // applied, where nothing would ever move it again.
    let authHeld = Promise.resolve();
    let releaseAuth = (): void => undefined;
    const poll = controllableLongPoll();
    let submissions = 0;
    const { client, worker } = makeClient(
      plan(),
      poll.fetcher,
      async (input, init) => {
        submissions += 1;
        if (submissions === 1) throw new TypeError("fetch failed");
        return acceptedResponder(input, init);
      },
      undefined,
      {
        auth: async () => {
          await authHeld;
          return { bearerToken: "token" };
        },
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;
    await until(
      () =>
        poll.open === 1 &&
        poll.requests.at(-1)?.pendingMutationIds.includes(mutationId) === true,
      "the poll carrying the pending id",
    );

    authHeld = new Promise((resolve) => {
      releaseAuth = () => resolve();
    });
    const retrying = client.retryMutation(mutationId);
    poll.answer();
    await until(
      () => client.mutationStatus(mutationId)?.state === "canonical",
      "the echo",
    );
    releaseAuth();
    expect(await retrying).toMatchObject({
      mutationId,
      state: "already_canonical",
    });
    expect(submissions).toBe(1);
    expect(client.mutationStatus(mutationId)?.state).toBe("canonical");
    await client.close();
  });

  it("keeps the envelope of a retryable failure, which settles nothing", async () => {
    // RETRYABLE_FAILURE decides nothing: the delta stays pending and no echo
    // will confirm it, so discarding the envelope would strand the optimistic
    // row with nothing for `retryMutation` to resend.
    const submitted: Uint8Array[] = [];
    let refuseOnce = true;
    const refusing = outcomeResponder(
      mutation.MutationOutcomeCode.RETRYABLE_FAILURE,
      { retryAfterMs: 250n },
    );
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submitted.push(new Uint8Array(init?.body as ArrayBuffer));
        if (refuseOnce) {
          refuseOnce = false;
          return refusing(input, init);
        }
        return acceptedResponder(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const result = await client.db.events.update({ id: 1n }, { level: "warn" });
    expect(result).toMatchObject({ state: "retryable", retryAfterMs: 250n });
    expect(client.mutationStatus(result.mutationId)?.state).toBe("retryable");
    const states: MutationStatus[] = [];
    const stopWatching = client.subscribeMutations((status) => {
      states.push(status);
    });
    // The delta was not dropped: the command may still run on the retry.
    expect(worker.pendingIds).toEqual([result.mutationId]);
    expect(
      worker.requests.filter((request) => request.type === "drop"),
    ).toEqual([]);

    const retried = await client.retryMutation(result.mutationId);
    expect(retried).toMatchObject({
      mutationId: result.mutationId,
      state: "awaiting_canonical_echo",
    });
    // The resend is in flight again, for the same row, until it is answered.
    stopWatching();
    expect(states).toMatchObject([
      { collection: "events", key: { id: 1n }, state: "submitting" },
      {
        collection: "events",
        key: { id: 1n },
        state: "awaiting_canonical_echo",
      },
    ]);
    // The same identifier and the same bytes, so the server judges a replay.
    expect(submitted).toHaveLength(2);
    expect(submitted[1]).toEqual(submitted[0]);
    expect(worker.pendingIds).toEqual([result.mutationId]);
    // Answered now, so there is nothing left to resend.
    await expect(client.retryMutation(result.mutationId)).rejects.toMatchObject(
      { code: "MUTATION_REJECTED" },
    );
    await client.close();
  });

  it("drops the oldest retained envelope once the bound is reached", async () => {
    // The retained envelopes are bounded and evict oldest-first, leaving every
    // younger envelope resendable.
    let dropTheResponse = true;
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        if (dropTheResponse) throw new TypeError("fetch failed");
        return acceptedResponder(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const ambiguous: string[] = [];
    for (let index = 0; index < 65; index += 1) {
      await expect(
        client.db.events.update({ id: 1n }, { level: `warn-${index}` }),
      ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
      ambiguous.push(
        worker.requests.filter((request) => request.type === "mutate").at(-1)
          ?.mutationId as string,
      );
    }
    expect(new Set(ambiguous).size).toBe(65);
    dropTheResponse = false;

    // The 65th submission evicted the 1st, and only the 1st.
    await expect(
      client.retryMutation(ambiguous[0] as string),
    ).rejects.toMatchObject({ code: "MUTATION_REJECTED" });
    await expect(
      client.retryMutation(ambiguous[1] as string),
    ).resolves.toMatchObject({
      mutationId: ambiguous[1],
      state: "awaiting_canonical_echo",
    });
    await expect(
      client.retryMutation(ambiguous.at(-1) as string),
    ).resolves.toMatchObject({
      mutationId: ambiguous.at(-1),
      state: "awaiting_canonical_echo",
    });
    // The evicted mutation keeps its delta and keeps riding the poll: the
    // bound costs the retry, not the write.
    expect(worker.pendingIds).toContain(ambiguous[0]);
    await client.close();
  });

  it("refuses a retry that comes back on a different activation", async () => {
    // `reset` notifies status listeners before it clears the retained
    // envelopes, so a listener that retries on "initializing" reads an
    // envelope of the mount being torn down. The retry wakes on a different
    // activation and refuses to submit a dead epoch's cursor and scope token.
    let dropTheResponse = true;
    let submissions = 0;
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submissions += 1;
        if (dropTheResponse) {
          dropTheResponse = false;
          throw new TypeError("fetch failed");
        }
        return acceptedResponder(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;

    let retrying: Promise<unknown> | undefined;
    const unsubscribe = client.subscribe(() => {
      if (retrying === undefined && client.status.state === "initializing")
        retrying = client.retryMutation(mutationId).then(
          () => undefined as unknown,
          (error: unknown) => error,
        );
    });
    await client.reset();
    unsubscribe();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(await retrying).toMatchObject({
      code: "MUTATION_AMBIGUOUS",
      scope: "mutation",
      recovery: "none",
      message: expect.stringContaining("before the current activation"),
    });
    // Nothing was resubmitted: the fresh mount's rows are the answer.
    expect(submissions).toBe(1);
    await client.close();
  });

  it("refuses a schema with no collections before it spawns a worker or sends a request", () => {
    const fetcher = vi.fn<typeof fetch>();
    const workerFactory = vi.fn(
      () => new FakeWorker() as unknown as ColumnarEngineWorker,
    );
    const hashers = fakeHashers();
    expect(() =>
      createInternalColumnarClient(
        {
          endpoint: "https://sync.test/root",
          auth: () => ({ bearerToken: "token" }),
          schema: createGeneratedClientSchema({
            collections: [],
          }),
          memoryBudgetBytes: 32 * 1024 * 1024,
          deviceId: "device-a",
          fetch: fetcher,
          clientHeapReserveBytes: 4 * 1024 * 1024,
        },
        workerFactory,
        hashers.create,
      ),
    ).toThrow(new TypeError("the schema carries no generated collections"));
    expect(fetcher).not.toHaveBeenCalled();
    expect(workerFactory).not.toHaveBeenCalled();
    expect(hashers.create).not.toHaveBeenCalled();
  });

  it("refuses a mutation ID that is not a bounded identifier wherever it takes one", async () => {
    const { client } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    for (const mutationId of ["", " leading-space", "x".repeat(513)]) {
      expect(() => client.mutationStatus(mutationId)).toThrow(TypeError);
      expect(() =>
        client.subscribeMutation(mutationId, () => undefined),
      ).toThrow(TypeError);
      await expect(client.retryMutation(mutationId)).rejects.toThrow(TypeError);
    }
    await client.close();
  });

  it("reports a publication listener that throws, and keeps publishing to the others", async () => {
    const { client, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const reportError = vi.fn();
    vi.stubGlobal("reportError", reportError);
    const failure = new Error("the page's listener failed");
    const stopThrowing = client.subscribePublications(() => {
      throw failure;
    });
    let publications = 0;
    const stopCounting = client.subscribePublications(() => {
      publications += 1;
    });

    await publish();
    await publish();
    expect(publications).toBe(2);
    expect(reportError).toHaveBeenCalledTimes(2);
    expect(reportError).toHaveBeenCalledWith(failure);
    expect(client.status).toEqual({ state: "ready" });
    stopThrowing();
    stopCounting();
    await client.close();
  });

  it("ignores an unsubscribe called again after the listener subscribed anew", async () => {
    const { client, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    let publications = 0;
    const countPublication = () => {
      publications += 1;
    };
    const stalePublications = client.subscribePublications(countPublication);
    stalePublications();
    const stopPublications = client.subscribePublications(countPublication);
    stalePublications();
    let statuses = 0;
    const countStatus = () => {
      statuses += 1;
    };
    const staleMutations = client.subscribeMutations(countStatus);
    staleMutations();
    const stopMutations = client.subscribeMutations(countStatus);
    staleMutations();

    await publish();
    await client.db.events.update({ id: 1n }, { level: "warn" });
    expect(publications).toBe(1);
    expect(statuses).toBeGreaterThan(0);
    stopPublications();
    stopMutations();
    await client.close();
  });

  it("refuses to resubmit a mutation the session has no envelope for", async () => {
    const { client } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    // Never submitted at all.
    await expect(
      client.retryMutation("11111111-2222-3333-4444-555555555555"),
    ).rejects.toMatchObject({ code: "MUTATION_REJECTED" });
    // Answered, so the receipt is the server's business and not a retry.
    const result = await client.db.events.update({ id: 1n }, { level: "warn" });
    await expect(client.retryMutation(result.mutationId)).rejects.toMatchObject(
      { code: "MUTATION_REJECTED" },
    );
    await expect(client.retryMutation(7 as unknown as string)).rejects.toThrow(
      "mutation ID must be a valid bounded identifier",
    );
    await client.close();
  });

  it("will not resubmit across a reset, because the mount it was based on is gone", async () => {
    // The envelope carries the base cursor and scope token of the activation
    // it was built against. Replaying it onto a fresh mount would submit a
    // cursor from an epoch that no longer exists, so the retry refuses and
    // the rebuilt mount's rows are the answer instead.
    let dropTheResponse = true;
    let submissions = 0;
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submissions += 1;
        if (dropTheResponse) {
          dropTheResponse = false;
          throw new TypeError("fetch failed");
        }
        return acceptedResponder(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(client.retryMutation(mutationId)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    expect(submissions).toBe(1);
    await client.close();
  });

  it("marks every unconfirmed mutation ambiguous when the session resets", async () => {
    const { client } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const result = await client.db.events.update({ id: 1n }, { level: "warn" });
    expect(client.mutationStatus(result.mutationId)?.state).toBe(
      "awaiting_canonical_echo",
    );
    await client.reset();
    const status = client.mutationStatus(result.mutationId);
    expect(status?.state).toBe("ambiguous");
    const error = status?.state === "ambiguous" ? status.error : undefined;
    expect(error).toBeInstanceOf(MutationSubmissionError);
    // The reset took the envelope with the mount, so the error must not
    // promise a retry that `retryMutation` refuses.
    expect({ ...error }).toEqual({
      name: "MutationSubmissionError",
      code: "MUTATION_AMBIGUOUS",
      scope: "mutation",
      recovery: "none",
      mutationId: result.mutationId,
    });
    expect(error?.message).toBe(
      "the session reset before the canonical echo; the mutation may have committed",
    );
    await expect(client.retryMutation(result.mutationId)).rejects.toMatchObject(
      { code: "MUTATION_REJECTED" },
    );
    await client.close();
  });

  it("does not tell the caller to retry a mutation whose mount a reset tore down mid-flight", async () => {
    // The response is lost after the reset, so the submission settles on a
    // mount that is gone and retains nothing for `retryMutation`.
    let submitted!: () => void;
    const reached = new Promise<void>((resolve) => {
      submitted = resolve;
    });
    let loseTheResponse!: (error: Error) => void;
    const { client, worker } = makeClient(plan(), undefined, () => {
      submitted();
      return new Promise<Response>((_, reject) => {
        loseTheResponse = reject;
      });
    });
    expect(await settled(client)).toEqual({ state: "ready" });
    const submission = client.db.events
      .update({ id: 1n }, { level: "warn" })
      .then(
        () => undefined as unknown,
        (error: unknown) => error,
      );
    await reached;
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    loseTheResponse(new TypeError("fetch failed"));
    const error = await submission;
    expect(error).toBeInstanceOf(MutationSubmissionError);
    expect({ ...(error as MutationSubmissionError) }).toEqual({
      name: "MutationSubmissionError",
      code: "MUTATION_AMBIGUOUS",
      scope: "mutation",
      recovery: "none",
      mutationId,
    });
    await expect(client.retryMutation(mutationId)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    await client.close();
  });

  it("cancels a mutation whose auth() a reset overtook before it reaches an engine", async () => {
    const held = Promise.withResolvers<{ bearerToken: string }>();
    let auths = 0;
    const { client, worker, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      undefined,
      {
        auth: () =>
          (auths += 1) === 2 ? held.promise : { bearerToken: "token" },
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const write = client.db.events.update({ id: 1n }, { level: "warn" });
    await until(() => auths === 2, "the mutation's auth()");
    await client.reset();
    held.resolve({ bearerToken: "token" });
    await expect(write).rejects.toMatchObject({ code: "CANCELLED" });
    expect(worker.requests.some(({ type }) => type === "mutate")).toBe(false);
    expect(
      fetcher.mock.calls.some(([input]) =>
        String(input).endsWith("/v1/mutations"),
      ),
    ).toBe(false);
    await client.close();
  });

  it("cancels a mutation whose optimistic apply a reset interrupted", async () => {
    const { client, worker, fetcher } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    // The engine never answers the apply; only the reset's teardown ends it.
    const post = worker.postMessage.bind(worker);
    let applying = false;
    worker.postMessage = (message, transfer) => {
      if (message.type === "mutate") applying = true;
      else post(message, transfer);
    };
    const write = client.db.events.update({ id: 1n }, { level: "warn" });
    await until(() => applying, "the optimistic apply");
    const reset = client.reset();
    await expect(write).rejects.toMatchObject({ code: "CANCELLED" });
    await reset;
    expect(
      fetcher.mock.calls.some(([input]) =>
        String(input).endsWith("/v1/mutations"),
      ),
    ).toBe(false);
    await client.close();
  });

  it("keeps the status a reset gave a mutation whose outcome arrives after it", async () => {
    const answer: PromiseWithResolvers<void> = Promise.withResolvers();
    const rejected = outcomeResponder(
      mutation.MutationOutcomeCode.REJECTED_VALIDATION,
    );
    let submitted = false;
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submitted = true;
        await answer.promise;
        return rejected(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const write = client.db.events.update({ id: 1n }, { level: "warn" });
    await until(() => submitted, "the submission");
    const mutationId = worker.requests.find(({ type }) => type === "mutate")
      ?.mutationId as string;
    expect(client.mutationStatus(mutationId)?.state).toBe("submitting");
    await client.reset();
    const abandoned = client.mutationStatus(mutationId);
    expect(abandoned?.state).toBe("ambiguous");
    answer.resolve();
    await write;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.mutationStatus(mutationId)).toEqual(abandoned);
    expect(worker.requests.some(({ type }) => type === "drop")).toBe(false);
    await client.close();
  });

  it("settles a submission the server refused after a reset without asking the closed engine to drop it", async () => {
    const answer: PromiseWithResolvers<void> = Promise.withResolvers();
    const { client, worker } = makeClient(plan(), undefined, async () => {
      await answer.promise;
      return new Response("no such route", { status: 404 });
    });
    expect(await settled(client)).toEqual({ state: "ready" });
    // A closed engine answers nothing.
    const post = worker.postMessage.bind(worker);
    let drops = 0;
    worker.postMessage = (message, transfer) => {
      if (message.type === "drop") drops += 1;
      else post(message, transfer);
    };
    let outcome: unknown;
    const write = client.db.events.update({ id: 1n }, { level: "warn" }).then(
      () => (outcome = "resolved"),
      (error: unknown) => (outcome = error),
    );
    await until(
      () => worker.requests.some(({ type }) => type === "mutate"),
      "the optimistic apply",
    );
    await client.reset();
    answer.resolve();
    await until(() => outcome !== undefined, "the submission to settle");
    await write;
    expect(outcome).toMatchObject({ code: "MUTATION_REJECTED" });
    expect(drops).toBe(0);
    await client.close();
  });

  it("a reset while a refused mutation is being dropped leaves the session ready", async () => {
    const { client, worker } = makeClient(
      plan(),
      undefined,
      outcomeResponder(mutation.MutationOutcomeCode.REJECTED_VALIDATION),
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    // The drop is never answered; the reset's teardown fails it.
    const post = worker.postMessage.bind(worker);
    let dropping = false;
    worker.postMessage = (message, transfer) => {
      if (message.type === "drop") dropping = true;
      else post(message, transfer);
    };
    const write = client.db.events
      .update({ id: 1n }, { level: "warn" })
      .catch(() => undefined);
    await until(() => dropping, "the drop of the refused delta");
    await client.reset();
    await write;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.status).toEqual({ state: "ready" });
    await client.close();
  });

  it("does not tell a retry that a reset cancelled while it waited for auth() to retry again", async () => {
    // The reset clears the envelope while the retry waits for credentials, so
    // the retry wakes on a different mount with nothing left to resend.
    let holdNextAuth = false;
    let authHeld = (): void => undefined;
    let releaseAuth = (): void => undefined;
    let submissions = 0;
    const { client, worker } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submissions += 1;
        if (submissions === 1) throw new TypeError("fetch failed");
        return acceptedResponder(input, init);
      },
      undefined,
      {
        auth: async () => {
          if (holdNextAuth) {
            holdNextAuth = false;
            authHeld();
            await new Promise<void>((resolve) => {
              releaseAuth = resolve;
            });
          }
          return { bearerToken: "token" };
        },
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;

    const retryWaitsForAuth = new Promise<void>((resolve) => {
      authHeld = resolve;
    });
    holdNextAuth = true;
    const retrying = client.retryMutation(mutationId).then(
      () => undefined as unknown,
      (error: unknown) => error,
    );
    await retryWaitsForAuth;
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    releaseAuth();
    const error = await retrying;
    expect(error).toBeInstanceOf(SyncError);
    expect({ ...(error as SyncError) }).toEqual({
      name: "SyncError",
      code: "CANCELLED",
      scope: "mutation",
      recovery: "none",
    });
    expect((error as SyncError).message).toBe(
      "the session reset before this operation completed",
    );
    await expect(client.retryMutation(mutationId)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    expect(submissions).toBe(1);
    await client.close();
  });

  it("tells a mutation a reset cancelled before submission that it can run again", async () => {
    let holdNextAuth = false;
    let authHeld = (): void => undefined;
    let releaseAuth = (): void => undefined;
    let submissions = 0;
    const { client } = makeClient(
      plan(),
      undefined,
      async (input, init) => {
        submissions += 1;
        return acceptedResponder(input, init);
      },
      undefined,
      {
        auth: async () => {
          if (holdNextAuth) {
            holdNextAuth = false;
            authHeld();
            await new Promise<void>((resolve) => {
              releaseAuth = resolve;
            });
          }
          return { bearerToken: "token" };
        },
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const mutationWaitsForAuth = new Promise<void>((resolve) => {
      authHeld = resolve;
    });
    holdNextAuth = true;
    const mutating = client.db.events
      .update({ id: 1n }, { level: "warn" })
      .then(
        () => undefined as unknown,
        (error: unknown) => error,
      );
    await mutationWaitsForAuth;
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    releaseAuth();
    const error = await mutating;
    expect(error).toBeInstanceOf(SyncError);
    expect({ ...(error as SyncError) }).toEqual({
      name: "SyncError",
      code: "CANCELLED",
      scope: "mutation",
      recovery: "retry",
    });
    expect(submissions).toBe(0);
    // Nothing was submitted, so issuing the mutation again is the retry.
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).resolves.toMatchObject({ state: "awaiting_canonical_echo" });
    expect(submissions).toBe(1);
    await client.close();
  });

  it("rejects a plan with two artifacts before any worker exists", async () => {
    const value = plan();
    const artifacts = value.artifacts as Record<string, unknown>[];
    const first = artifacts[0];
    if (!first) throw new Error("fixture artifact missing");
    const second = structuredClone(first);
    const firstRuntime = first.runtime as {
      tables: unknown[];
      schema: { tables: unknown[] };
    };
    const secondRuntime = second.runtime as {
      tables: unknown[];
      schema: { tables: unknown[] };
    };
    second.artifactId = "artifact-2";
    second.attachAs = "stage_0001";
    second.installOrder = 1;
    second.tables = ["public.issues"];
    secondRuntime.tables = [firstRuntime.tables[1]];
    secondRuntime.schema.tables = [firstRuntime.schema.tables[1]];
    first.tables = ["public.events"];
    firstRuntime.tables.pop();
    firstRuntime.schema.tables.pop();
    artifacts.push(second);
    const { client, workerFactory } = makeClient(value);
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INVALID_RESPONSE" },
    });
    expect(workerFactory).not.toHaveBeenCalled();
    await expect(client.db.events.query().run()).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
    });
  });

  it("reports a budget the server refuses as CAPACITY_EXCEEDED, once, before any worker exists", async () => {
    const { client, fetcher, workerFactory } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) =>
        String(input).endsWith("/v1/bootstrap")
          ? Response.json(
              {
                code: "CAPACITY_EXCEEDED",
                message:
                  "bootstrap cannot fit the declared client memory budget",
                resnapshotRequired: false,
              },
              { status: 422 },
            )
          : inner(input, init),
    );
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "CAPACITY_EXCEEDED", scope: "sync", recovery: "none" },
    });
    expect(workerFactory).not.toHaveBeenCalled();
    expect(
      fetcher.mock.calls.filter(([input]) =>
        String(input).endsWith("/v1/bootstrap"),
      ),
    ).toHaveLength(1);
    await expect(client.db.events.query().run()).rejects.toMatchObject({
      code: "CAPACITY_EXCEEDED",
    });
  });

  it("refuses a plan whose minimum safe budget exceeds the configured one", async () => {
    const value = plan();
    (value.memory as Record<string, unknown>).minimumSafeBudgetBytes =
      64 * 1024 * 1024;
    const { client, workerFactory } = makeClient(value);
    expect(await settled(client)).toMatchObject({
      state: "error",
      error: {
        code: "CAPACITY_EXCEEDED",
        scope: "sync",
        recovery: "none",
        message:
          "the working set needs a memoryBudgetBytes of at least 67108864; this client has 33554432",
      },
    });
    expect(workerFactory).not.toHaveBeenCalled();
  });

  it("rejects a generated schema that drifted from the served artifact before open", async () => {
    const value = plan();
    const runtime = (
      value.artifacts as { runtime: Record<string, unknown> }[]
    )[0]?.runtime;
    const schema = runtime?.schema as {
      tables: { columns: { type: string }[] }[];
    };
    const level = schema.tables[0]?.columns[1];
    if (!level) throw new Error("fixture column missing");
    level.type = "f64";
    const { client, workerFactory, hashers } = makeClient(value);
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INVALID_RESPONSE" },
    });
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.message).toBe(
      "the generated bindings do not match what the server serves: events.level is served as f64, not text; regenerate them with calque-bindings from the calque.config.json the server runs",
    );
    expect(workerFactory).not.toHaveBeenCalled();
    // Spawned before the plan was asked for, and terminated with its refusal.
    expect(hashers.map(({ terminated }) => terminated)).toEqual([true]);
  });

  it("drops a result whose epoch moved during the query", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.holdQueries = true;
    const pending = client.db.events.query().run();
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(worker.held).toHaveLength(1);
    const reset = client.reset();
    await expect(pending).rejects.toMatchObject({ code: "CANCELLED" });
    await reset;
    expect(client.status).toEqual({ state: "ready" });
    await client.close();
  });

  it("keeps a live window current and shares it per range", async () => {
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.queryRows = [
      [1n, "error"],
      [2n, "warn"],
      [3n, "info"],
    ];
    const window = client.db.events.query().subscribeWindow(1, 1);
    expect(client.db.events.query().subscribeWindow(1, 1)).toBe(window);
    expect(client.db.events.query().subscribeWindow(0, 1)).not.toBe(window);
    const unsubscribe = window.subscribe(() => undefined);
    await window.promise;
    expect(window.getSnapshot()).toEqual({
      status: "ready",
      result: { offset: 1, total: 3, rows: [{ id: 2n, level: "warn" }] },
    });
    const frames = () =>
      worker.requests.filter((request) => request.type === "frame");
    expect(frames()).toHaveLength(1);

    worker.queryRows = [...worker.queryRows, [4n, "info"]];
    await publish();
    await window.promise;
    expect(frames()).toHaveLength(2);
    expect(window.getSnapshot()).toMatchObject({
      status: "ready",
      result: { offset: 1, total: 4 },
    });
    unsubscribe();
    await client.close();
  });

  it("re-runs a subscription only when one of its collections moved", async () => {
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const resource = client.db.events.query().subscribe();
    let notifications = 0;
    const unsubscribe = resource.subscribe(() => {
      notifications += 1;
    });
    await resource.promise;
    expect(resource.getSnapshot()).toMatchObject({ status: "ready" });
    expect(notifications).toBe(1);
    // A publication listener runs after the stale subscriptions were asked to
    // re-read, so the promise it observes is that read, not the previous one.
    const publications: SyncPublication[] = [];
    const inFlight: Promise<void>[] = [];
    const stopPublications = client.subscribePublications((publication) => {
      publications.push(publication);
      inFlight.push(resource.promise);
    });

    worker.changedCollections = ["issues"];
    await publish();
    await resource.promise;
    expect(notifications).toBe(1);

    worker.changedCollections = ["events"];
    await publish();
    await resource.promise;
    expect(notifications).toBe(2);
    expect(publications).toEqual([
      {
        cursor: "cursor-p",
        latestCommitTimeUnixMicros: 1_700_000_000_000_000n,
        changedCollections: ["issues"],
        transactionCount: 1,
      },
      {
        cursor: "cursor-p",
        latestCommitTimeUnixMicros: 1_700_000_000_000_000n,
        changedCollections: ["events"],
        transactionCount: 1,
      },
    ]);
    expect(Object.isFrozen(publications[0])).toBe(true);
    // The second listener observed the re-read the events move started.
    expect(inFlight).toHaveLength(2);
    await inFlight[1];
    stopPublications();
    unsubscribe();
    await client.close();
  });

  it("reads a window of an ordered query through the mount, and withdraws one aborted", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.queryRows = [
      [3n, "fatal"],
      [2n, "warn"],
      [1n, "error"],
    ];
    const view = client.db.events.query().orderBy("id", "desc");
    expect(await view.window(1, 5)).toEqual({
      offset: 1,
      total: 3,
      rows: [
        { id: 2n, level: "warn" },
        { id: 1n, level: "error" },
      ],
    });
    const request = worker.requests.find(
      (candidate) => candidate.type === "window",
    );
    expect(request?.window).toEqual({ offset: 1, count: 5 });
    worker.holdQueries = true;
    worker.postMessage = ((original) =>
      function (this: FakeWorker, message: WorkerRequest) {
        if (message.type === "window") {
          this.requests.push(structuredClone(message));
          return;
        }
        original.call(this, message);
      })(worker.postMessage);
    const controller = new AbortController();
    const pending = view.window(0, 1, { signal: controller.signal });
    await new Promise((resolve) => setTimeout(resolve, 5));
    controller.abort();
    await expect(pending).rejects.toMatchObject({ code: "CANCELLED" });
    const posted = worker.requests.filter(
      (candidate) => candidate.type === "window",
    );
    expect(worker.requests.at(-1)).toEqual({
      type: "cancel",
      target: posted.at(-1)?.id,
    });
    await client.close();
  });

  it("publishes a frame the worker answered after a publication at that publication's state, without reading again", async () => {
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const frames = () =>
      worker.requests.filter((request) => request.type === "frame");
    worker.holdQueries = true;
    const resource = client.db.events.query().subscribe();
    let notifications = 0;
    const unsubscribe = resource.subscribe(() => {
      notifications += 1;
    });
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(worker.held).toHaveLength(1);
    // The batch lands while the frame is in the worker, which reads at the
    // state the batch left.
    worker.queryRows = [[1n, "warn"]];
    await publish();
    worker.holdQueries = false;
    worker.releaseHeld();
    await resource.promise;
    expect(frames()).toHaveLength(1);
    expect(resource.getSnapshot()).toEqual({
      status: "ready",
      result: [{ id: 1n, level: "warn" }],
    });
    expect(notifications).toBe(1);
    worker.changedCollections = [];
    await publish();
    await resource.promise;
    expect(frames()).toHaveLength(1);
    unsubscribe();
    await client.close();
  });

  it("reads again the rows a publication moved while nobody listened", async () => {
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const frames = () =>
      worker.requests.filter((request) => request.type === "frame");
    worker.holdQueries = true;
    const resource = client.db.events.query().subscribe();
    const read = resource.promise;
    await new Promise((resolve) => setTimeout(resolve, 5));
    // The batch lands while the frame is in the worker and nobody listens, so
    // no publication would re-read it: the worker reads at the state the
    // batch left.
    worker.queryRows = [[2n, "warn"]];
    await publish();
    worker.holdQueries = false;
    worker.releaseHeld();
    await read;
    expect(frames()).toHaveLength(1);
    expect(resource.getSnapshot()).toEqual({
      status: "ready",
      result: [{ id: 2n, level: "warn" }],
    });
    // One that lands after the answer re-reads nothing until a listener comes.
    worker.queryRows = [[3n, "fatal"]];
    await publish();
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(frames()).toHaveLength(1);
    const unsubscribe = resource.subscribe(() => undefined);
    await resource.promise;
    expect(frames()).toHaveLength(2);
    expect(resource.getSnapshot()).toEqual({
      status: "ready",
      result: [{ id: 3n, level: "fatal" }],
    });
    unsubscribe();
    await client.close();
  });

  it("withdraws a frame from the worker when the last listener of its members leaves", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.holdQueries = true;
    const resource = client.db.events.query().subscribe();
    const unsubscribe = resource.subscribe(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 5));
    const first = worker.held[0];
    if (!first) throw new Error("held frame missing");
    unsubscribe();
    expect(worker.requests.at(-1)).toEqual({
      type: "cancel",
      target: first.id,
    });
    // The worker's answer to a withdrawn frame is never published, and a
    // listener that comes back reads again.
    worker.holdQueries = false;
    worker.releaseHeld();
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(resource.getSnapshot()).toEqual({ status: "loading" });
    const again = resource.subscribe(() => undefined);
    await resource.promise;
    expect(
      worker.requests.filter((request) => request.type === "frame"),
    ).toHaveLength(2);
    expect(resource.getSnapshot()).toMatchObject({ status: "ready" });
    again();
    await client.close();
  });

  it("keeps a subscription's query in the worker while a caller awaits its rows", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.holdQueries = true;
    const resource = client.db.events.query().subscribe();
    const unsubscribe = resource.subscribe(() => undefined);
    const read = resource.promise;
    await new Promise((resolve) => setTimeout(resolve, 5));
    unsubscribe();
    expect(worker.requests.map((request) => request.type)).not.toContain(
      "cancel",
    );
    worker.releaseHeld();
    await read;
    expect(resource.getSnapshot()).toMatchObject({ status: "ready" });
    await client.close();
  });

  it("settles a query issued while reset tears down the change stream", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(worker.requests[0]?.changeStream).toBeDefined();
    const reset = client.reset();
    // Issued while the old change stream's aborted long-poll has not reported
    // back: the session has no activation, and must park this caller rather
    // than poll for one.
    const rows = await client.db.events.query().run();
    expect(rows).toEqual([{ id: 1n, level: "error" }]);
    await reset;
    expect(client.status).toEqual({ state: "ready" });
    await client.close();
  });

  it("refuses a plan whose tables carry no wire column ids before any worker exists", async () => {
    const value = plan();
    const artifact = (value.artifacts as Record<string, unknown>[])[0];
    if (!artifact) throw new Error("fixture artifact missing");
    for (const table of (
      artifact.runtime as { tables: Record<string, unknown>[] }
    ).tables)
      delete table.columns;
    const { client, workerFactory, fetcher, hashers } = makeClient(value);
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INVALID_RESPONSE" },
    });
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.message).toContain("carries no wire column ids");
    expect(workerFactory).not.toHaveBeenCalled();
    expect(hashers.map(({ terminated }) => terminated)).toEqual([true]);
    expect(
      fetcher.mock.calls.filter(([input]) =>
        String(input).endsWith("/artifact-1"),
      ),
    ).toHaveLength(0);
  });

  it("closes the worker it spawned for a download that fails", async () => {
    // The worker boots while the artifact downloads, so a download that
    // fails has a worker to close.
    const { client, worker, workerFactory, hashers } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) =>
        String(input).endsWith("/artifact-1")
          ? new Response("unavailable", { status: 503 })
          : inner(input, init),
    );
    const status = await settled(client);
    expect(status.state).toBe("error");
    expect(workerFactory).toHaveBeenCalledTimes(1);
    expect(worker.terminated).toBe(true);
    expect(hashers.map(({ terminated }) => terminated)).toEqual([true]);
    // The open went out before the download; nothing followed it.
    expect(worker.requests.map((request) => request.type)).toEqual(["open"]);
  });

  it("closes the worker it spawned when the client closes during the download", async () => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    let requested = false;
    const { client, worker, hashers } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (!String(input).endsWith("/artifact-1")) return inner(input, init);
        requested = true;
        await held;
        return inner(input, init);
      },
    );
    await until(() => requested, "the artifact request");
    await client.close();
    // The hashers end with the client, not when the download next looks.
    expect(hashers.map(({ terminated }) => terminated)).toEqual([true]);
    release();
    await until(() => worker.terminated, "the worker to close");
    // Once closed, the session sends the worker no byte of the artifact.
    expect(worker.requests.map((request) => request.type)).toEqual(["open"]);
    expect(hashers[0]?.chunks).toEqual([]);
  });

  it("spawns its hashers before it asks for the plan, and terminates them once the artifact opens", async () => {
    stubThreads(8);
    let atBootstrap: number | undefined;
    let spawned: readonly FakeHasher[] = [];
    const { client, worker, hashers, workerFactory } = makeClient(
      plan(),
      undefined,
      undefined,
      (inner) => async (input, init) => {
        if (String(input).endsWith("/v1/bootstrap"))
          atBootstrap = spawned.length;
        return inner(input, init);
      },
    );
    // The activation asks for the plan after `auth()` has answered, by
    // which time this has run.
    spawned = hashers;
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(atBootstrap).toBe(3);
    expect(workerFactory).toHaveBeenCalledTimes(1);
    expect(hashers.map(({ terminated }) => terminated)).toEqual([
      true,
      true,
      true,
    ]);
    expect(worker.terminated).toBe(false);
    await client.close();
  });

  it("streams the artifact through the hashers as it downloads, and opens after the last chunk lands", async () => {
    stubThreads(8);
    const body = patternBytes(LONG_ARTIFACT_BYTES);
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { client, worker, hashers } = makeClient(
      longPlan(LONG_ARTIFACT_BYTES),
      undefined,
      undefined,
      serveArtifact(body, held),
    );
    // The open goes to the worker before a byte of the artifact has, and
    // takes one port per hasher.
    await until(
      () => worker.requests.some((request) => request.type === "open"),
      "the open",
    );
    const [open] = worker.requests;
    expect(open).toMatchObject({
      type: "open",
      trusted: {
        byteLength: LONG_ARTIFACT_BYTES,
        chunks: { bytes: CHUNK_BYTES, sha256: chunkDigests(3) },
      },
    });
    expect(open?.hashers).toHaveLength(3);
    expect(open && "buffer" in open).toBe(false);
    expect(worker.received).toEqual([]);
    release();
    const status = await settled(client);
    expect(status.state).toBe("ready");
    expect(worker.received.map((bytes) => bytes.byteLength)).toEqual([
      CHUNK_BYTES,
      CHUNK_BYTES,
      LONG_ARTIFACT_BYTES - 2 * CHUNK_BYTES,
    ]);
    const joined = new Uint8Array(LONG_ARTIFACT_BYTES);
    let offset = 0;
    for (const bytes of worker.received) {
      joined.set(bytes, offset);
      offset += bytes.byteLength;
    }
    // Compared as buffers: an element-wise deep equality of 10 MiB is slow.
    expect(Buffer.compare(joined, body)).toBe(0);
    // Each chunk went through exactly one hasher.
    expect(
      hashers.flatMap(({ chunks }) => chunks).sort((a, b) => a - b),
    ).toEqual([0, 1, 2]);
    expect(worker.requests.map((request) => request.type).slice(0, 2)).toEqual([
      "open",
      "artifact-end",
    ]);
    expect(worker.requests[1]).toMatchObject({ chunks: 3 });
  });

  it("reads a complete artifact to the end of its body, and neither cancels the body nor aborts the request", async () => {
    stubThreads(8);
    const body = patternBytes(LONG_ARTIFACT_BYTES);
    const seen = {
      signal: undefined as AbortSignal | null | undefined,
      ended: false,
      cancelled: false,
    };
    const serve =
      (inner: typeof fetch): typeof fetch =>
      async (input, init) => {
        if (!String(input).endsWith("/artifact-1")) return inner(input, init);
        seen.signal = init?.signal;
        let sent = 0;
        const stream = new ReadableStream({
          type: "bytes",
          pull(controller) {
            if (sent >= body.byteLength) {
              seen.ended = true;
              controller.close();
              controller.byobRequest?.respond(0);
              return;
            }
            const next = body.slice(sent, sent + PACED_BYTES);
            sent += next.byteLength;
            controller.enqueue(next);
          },
          cancel() {
            seen.cancelled = true;
          },
        });
        return new Response(stream, {
          status: 200,
          headers: {
            "content-type": "application/vnd.calque.direct-columnar.v4",
            "content-length": String(body.byteLength),
          },
        });
      };
    const { client, hashers } = makeClient(
      longPlan(LONG_ARTIFACT_BYTES),
      undefined,
      undefined,
      serve,
    );
    expect((await settled(client)).state).toBe("ready");
    // After the last expected byte the page reads once more and finds the
    // end of the body. Terminating the hashers once the artifact opened is
    // not a failure, so nothing aborts the request.
    expect(seen).toMatchObject({ ended: true, cancelled: false });
    expect(seen.signal?.aborted).toBe(false);
    expect(hashers.map(({ terminated }) => terminated)).toEqual([
      true,
      true,
      true,
    ]);
  });

  it("reads no further than two chunks a hasher while the engine worker credits none", async () => {
    const bytes = 4 * CHUNK_BYTES + 3;
    const body = patternBytes(bytes);
    const given = { bytes: 0 };
    const { client, worker, hashers } = makeClient(
      longPlan(bytes),
      undefined,
      undefined,
      servePaced(body, given),
    );
    worker.holdCredits = true;
    await until(() => worker.accepted === 2, "two chunks to land");
    // Time for a page that did not wait to read on.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(hashers.map(({ chunks }) => chunks)).toEqual([[0, 1]]);
    // The third chunk is read and waits for room; nothing past it is.
    expect(given.bytes).toBeLessThanOrEqual(3 * CHUNK_BYTES + PACED_BYTES);
    expect(client.status.state).toBe("initializing");
    expect(worker.requests.map((request) => request.type)).toEqual(["open"]);
    worker.releaseCredits();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(hashers[0]?.chunks).toEqual([0, 1, 2, 3, 4]);
    expect(given.bytes).toBe(bytes);
    await client.close();
  });

  it("marks the download done before the last chunk waits for a hasher", async () => {
    performance.clearMarks();
    const { client, worker, hashers } = makeClient(
      longPlan(LONG_ARTIFACT_BYTES),
      undefined,
      undefined,
      serveArtifact(patternBytes(LONG_ARTIFACT_BYTES)),
    );
    worker.holdCredits = true;
    await until(
      () =>
        performance.getEntriesByName("calque:artifact:downloaded").length === 1,
      "the downloaded mark",
    );
    // The last chunk is read, and waits: the one hasher holds two.
    expect(hashers[0]?.chunks).toEqual([0, 1]);
    expect(worker.requests.map((request) => request.type)).toEqual(["open"]);
    worker.releaseCredits();
    expect(await settled(client)).toEqual({ state: "ready" });
    await client.close();
  });

  it("refuses a chunk the worker refuses at once, ends the download and terminates every worker", async () => {
    stubThreads(6);
    // Seven chunks: with the refused one and those behind it never
    // credited, the page cannot reach the end, whenever the refusal lands.
    const bytes = 6 * CHUNK_BYTES + 3;
    let signal: AbortSignal | undefined;
    const { client, worker, hashers } = makeClient(
      longPlan(bytes),
      undefined,
      undefined,
      (inner) => (input, init) => {
        if (String(input).endsWith("/artifact-1"))
          signal = init?.signal ?? undefined;
        return servePaced(patternBytes(bytes), { bytes: 0 })(inner)(
          input,
          init,
        );
      },
    );
    worker.refuseChunk = 2;
    worker.openError = "artifact digest binding mismatch";
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { message: "artifact digest binding mismatch" },
    });
    expect(signal?.aborted).toBe(true);
    expect(worker.terminated).toBe(true);
    expect(hashers.map(({ terminated }) => terminated)).toEqual([true, true]);
    expect(worker.received.flatMap((_bytes, index) => [index])).not.toContain(
      2,
    );
    expect(
      worker.requests.some((request) => request.type === "artifact-end"),
    ).toBe(false);
  });

  it("reports the chunk the worker refused, not the download that refusal cancelled", async () => {
    const bytes = 4 * CHUNK_BYTES + 3;
    let signal: AbortSignal | undefined;
    const { client, worker } = makeClient(
      longPlan(bytes),
      undefined,
      undefined,
      (inner) => (input, init) => {
        if (String(input).endsWith("/artifact-1"))
          signal = init?.signal ?? undefined;
        // The page is still reading the third chunk when the refusal lands.
        return servePaced(
          patternBytes(bytes),
          { bytes: 0 },
          {
            at: 2 * CHUNK_BYTES + PACED_BYTES,
            held: new Promise(() => undefined),
          },
        )(inner)(input, init);
      },
    );
    worker.refuseChunk = 0;
    worker.openError = "artifact digest binding mismatch";
    const status = await settled(client);
    expect(signal?.aborted).toBe(true);
    expect(status).toMatchObject({
      state: "error",
      error: { message: "artifact digest binding mismatch" },
    });
    expect(worker.terminated).toBe(true);
  });

  it("never ends an artifact whose body is short or long", async () => {
    for (const length of [LONG_ARTIFACT_BYTES - 1, LONG_ARTIFACT_BYTES + 1]) {
      const { client, worker, hashers } = makeClient(
        longPlan(LONG_ARTIFACT_BYTES),
        undefined,
        undefined,
        serveArtifact(patternBytes(length), undefined, LONG_ARTIFACT_BYTES),
      );
      const status = await settled(client);
      expect(status, String(length)).toMatchObject({
        state: "error",
        error: {
          message:
            length < LONG_ARTIFACT_BYTES
              ? `artifact response contained ${length} of ${LONG_ARTIFACT_BYTES} bytes`
              : "artifact response exceeds decoded length",
        },
      });
      expect(worker.terminated).toBe(true);
      expect(hashers.map(({ terminated }) => terminated)).toEqual([true]);
      expect(
        worker.requests.some((request) => request.type === "artifact-end"),
      ).toBe(false);
    }
  });

  describe("a load that ends before its artifact opens", () => {
    const failures: ReadonlyArray<{
      readonly name: string;
      readonly fail: (hasher: FakeHasher) => void;
      readonly error: string;
    }> = [
      {
        name: "an error event",
        fail: (hasher) => {
          hasher.crash("hasher crashed");
        },
        error: "hasher crashed",
      },
      {
        name: "a messageerror event",
        fail: (hasher) => {
          hasher.dispatchEvent(new MessageEvent("messageerror"));
        },
        error: "a hasher worker message could not be decoded",
      },
      {
        name: "a digest it could not take",
        fail: (hasher) => {
          hasher.dispatchEvent(
            new MessageEvent("message", {
              data: { type: "failed", index: 1, message: "digest failed" },
            }),
          );
        },
        error: "a hasher could not digest chunk 1: digest failed",
      },
    ];

    for (const { name, fail, error } of failures)
      it(`${name} refuses the load mid-download, ends the download and terminates every worker`, async () => {
        stubThreads(6);
        const bytes = 6 * CHUNK_BYTES + 3;
        let signal: AbortSignal | undefined;
        const { client, worker, hashers } = makeClient(
          longPlan(bytes),
          undefined,
          undefined,
          (inner) => (input, init) => {
            if (String(input).endsWith("/artifact-1"))
              signal = init?.signal ?? undefined;
            return servePaced(patternBytes(bytes), { bytes: 0 })(inner)(
              input,
              init,
            );
          },
        );
        worker.holdCredits = true;
        await until(() => worker.accepted === 4, "four chunks to land");
        const hasher = hashers[1];
        if (!hasher) throw new Error("no hasher");
        fail(hasher);
        const status = await settled(client);
        expect(status).toMatchObject({
          state: "error",
          error: { message: error },
        });
        expect(signal?.aborted).toBe(true);
        expect(worker.terminated).toBe(true);
        expect(hashers.map(({ terminated }) => terminated)).toEqual([
          true,
          true,
        ]);
        expect(
          worker.requests.some((request) => request.type === "artifact-end"),
        ).toBe(false);
      });

    it("a hasher that fails while the page waits on the network ends the download at once", async () => {
      const bytes = 3 * CHUNK_BYTES;
      let signal: AbortSignal | undefined;
      const { client, worker, hashers } = makeClient(
        longPlan(bytes),
        undefined,
        undefined,
        (inner) => (input, init) => {
          if (String(input).endsWith("/artifact-1"))
            signal = init?.signal ?? undefined;
          // The body stalls partway through the second chunk.
          return servePaced(
            patternBytes(bytes),
            { bytes: 0 },
            {
              at: CHUNK_BYTES + PACED_BYTES,
              held: new Promise(() => undefined),
            },
          )(inner)(input, init);
        },
      );
      await until(() => worker.accepted === 1, "the first chunk to land");
      const [hasher] = hashers;
      if (!hasher) throw new Error("no hasher");
      hasher.crash("hasher crashed");
      expect(await settled(client)).toMatchObject({
        state: "error",
        error: { message: "hasher crashed" },
      });
      expect(signal?.aborted).toBe(true);
      expect(worker.terminated).toBe(true);
    });

    it("a hasher that fails holding a chunk after the page has ended the artifact refuses the load", async () => {
      let release!: () => void;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const { client, worker, hashers } = makeClient(
        longPlan(2 * CHUNK_BYTES),
        undefined,
        undefined,
        serveArtifact(patternBytes(2 * CHUNK_BYTES), held),
      );
      await until(() => hashers.length === 1, "the hasher");
      const [hasher] = hashers;
      if (!hasher) throw new Error("no hasher");
      hasher.keepChunks = true;
      release();
      await until(
        () =>
          worker.requests.some((request) => request.type === "artifact-end"),
        "the end",
      );
      expect(hasher.chunks).toEqual([0, 1]);
      hasher.crash("hasher crashed");
      expect(await settled(client)).toMatchObject({
        state: "error",
        error: { message: "hasher crashed" },
      });
      expect(worker.terminated).toBe(true);
      expect(hasher.terminated).toBe(true);
    });

    it("closing the client while the engine worker waits on chunks a hasher still holds terminates it at once", async () => {
      let release!: () => void;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const { client, worker, hashers } = makeClient(
        longPlan(2 * CHUNK_BYTES),
        undefined,
        undefined,
        serveArtifact(patternBytes(2 * CHUNK_BYTES), held),
      );
      await until(() => hashers.length === 1, "the hasher");
      const [hasher] = hashers;
      if (!hasher) throw new Error("no hasher");
      hasher.keepChunks = true;
      release();
      await until(
        () =>
          worker.requests.some((request) => request.type === "artifact-end"),
        "the end",
      );
      // The page has handed over both chunks and ended the artifact. The
      // engine worker waits for the two the hasher holds, which a closed
      // hasher will never pass on.
      expect(hasher.chunks).toEqual([0, 1]);
      await client.close();
      expect(hasher.terminated).toBe(true);
      expect(worker.terminated).toBe(true);
      expect(client.status).toEqual({ state: "closed" });
    });

    it("a hasher that cannot be constructed refuses the load before the plan is asked for", async () => {
      stubThreads(8);
      const worker = new FakeWorker();
      const fetcher = vi.fn(validatedFetch(fakeFetch(plan())));
      const workerFactory = vi.fn(
        () => worker as unknown as ColumnarEngineWorker,
      );
      const spawned: FakeHasher[] = [];
      const client = createInternalColumnarClient<Collections>(
        {
          endpoint: "https://sync.test/root",
          auth: () => ({ bearerToken: "token" }),
          schema: createGeneratedClientSchema<Collections>(descriptor),
          memoryBudgetBytes: 32 * 1024 * 1024,
          deviceId: "device-a",
          fetch: fetcher,
          clientHeapReserveBytes: 4 * 1024 * 1024,
        },
        workerFactory,
        () => {
          if (spawned.length === 2)
            throw new TypeError(
              "this runtime has no Worker constructor; Calque runs only in Chromium-based browsers",
            );
          const hasher = new FakeHasher();
          spawned.push(hasher);
          return hasher as unknown as ColumnarHasherWorker;
        },
      );
      const status = await settled(client);
      expect(status).toMatchObject({
        state: "error",
        error: {
          message:
            "this runtime has no Worker constructor; Calque runs only in Chromium-based browsers",
        },
      });
      expect(spawned.map(({ terminated }) => terminated)).toEqual([true, true]);
      expect(fetcher).not.toHaveBeenCalled();
      expect(workerFactory).not.toHaveBeenCalled();
    });

    it("a chunk the engine worker has not credited within the deadline refuses the load", async () => {
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      try {
        const { client, worker, hashers } = makeClient(
          longPlan(LONG_ARTIFACT_BYTES),
          undefined,
          undefined,
          serveArtifact(patternBytes(LONG_ARTIFACT_BYTES)),
        );
        worker.holdCredits = true;
        const untilFaked = async (condition: () => boolean, what: string) => {
          for (let turn = 0; turn < 200; turn += 1) {
            if (condition()) return;
            await vi.advanceTimersByTimeAsync(1);
            await new Promise((resolve) => setImmediate(resolve));
          }
          throw new Error(`timed out waiting for ${what}`);
        };
        await untilFaked(() => worker.accepted === 2, "two chunks to land");
        await vi.advanceTimersByTimeAsync(HASHER_CREDIT_MS - 1_000);
        expect(client.status.state).toBe("initializing");
        expect(hashers.map(({ terminated }) => terminated)).toEqual([false]);
        await vi.advanceTimersByTimeAsync(1_000);
        await untilFaked(() => client.status.state === "error", "the refusal");
        expect(client.status).toMatchObject({
          state: "error",
          error: {
            message: `chunk 0 was not accepted within ${String(HASHER_CREDIT_MS)} ms`,
          },
        });
        expect(worker.terminated).toBe(true);
        expect(hashers.map(({ terminated }) => terminated)).toEqual([true]);
      } finally {
        vi.useRealTimers();
      }
    });

    it("closing the client mid-download terminates every worker at once", async () => {
      stubThreads(6);
      const bytes = 6 * CHUNK_BYTES + 3;
      const { client, worker, hashers } = makeClient(
        longPlan(bytes),
        undefined,
        undefined,
        servePaced(patternBytes(bytes), { bytes: 0 }),
      );
      worker.holdCredits = true;
      await until(() => worker.accepted === 4, "four chunks to land");
      await client.close();
      expect(hashers.map(({ terminated }) => terminated)).toEqual([true, true]);
      expect(worker.terminated).toBe(true);
      expect(client.status).toEqual({ state: "closed" });
      expect(
        worker.requests.some((request) => request.type === "artifact-end"),
      ).toBe(false);
    });
  });

  it("runs the fenced catch-up to the activation cursor before reporting ready", async () => {
    const polls = releasableChangePolls(1);
    const { client, worker, fetcher, hashers } = makeClient(
      fencedPlan(),
      polls.changeFetch,
    );
    worker.openCursor = "cursor-a";
    const statuses: string[] = [];
    client.subscribe(() => statuses.push(client.status.state));
    // The mount opened at the artifact's cut. Until the change stream lands it
    // on the activation cursor the client is not ready and a facade query
    // parks.
    let queryDone = false;
    const parked = client.db.events
      .query()
      .run()
      .finally(() => {
        queryDone = true;
      });
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(polls.polls).toBe(1);
    expect(client.status).toEqual({ state: "initializing" });
    expect(queryDone).toBe(false);
    // The artifact is open, so the hashers are gone before the catch-up ends.
    expect(hashers.map(({ terminated }) => terminated)).toEqual([true]);
    expect(
      worker.requests.filter((request) => request.type === "query"),
    ).toHaveLength(0);
    const first = polls.bodies[0];
    if (!first) throw new Error("no change request was sent");
    // The catch-up request is fenced: through the activation cursor, every
    // table starting at the artifact's snapshot cursor.
    expect(decodeChangeRequest(first)).toEqual({
      from: "cursor-a",
      through: "cursor-p",
      tables: [
        { tableId: "public.events", snapshotCursor: "cursor-a" },
        { tableId: "public.issues", snapshotCursor: "cursor-a" },
      ],
    });

    polls.release(0);
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(await parked).toEqual([{ id: 1n, level: "error" }]);
    expect(statuses).toEqual(["ready"]);
    // The next poll is live: no fence, no table starts.
    await new Promise((resolve) => setTimeout(resolve, 10));
    const second = polls.bodies[1];
    if (!second) throw new Error("no live poll followed the catch-up");
    expect(decodeChangeRequest(second)).toEqual({
      from: "cursor-p",
      through: undefined,
      tables: [
        { tableId: "public.events", snapshotCursor: undefined },
        { tableId: "public.issues", snapshotCursor: undefined },
      ],
    });
    expect(
      fetcher.mock.calls.filter(([input]) => String(input).endsWith("/change")),
    ).toHaveLength(2);
    await client.close();
  });

  it("refuses a mount that opened away from the artifact cursor", async () => {
    const { client, worker, fetcher } = makeClient();
    worker.openCursor = "cursor-x";
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INTEGRITY_FAILED", recovery: "reset" },
    });
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.message).toContain("opened away from the artifact");
    expect(worker.terminated).toBe(true);
    expect(
      fetcher.mock.calls.filter(([input]) => String(input).endsWith("/change")),
    ).toHaveLength(0);
  });

  it("rejects activation when the catch-up fails", async () => {
    const polls = releasableChangePolls(1);
    const { client, worker } = makeClient(fencedPlan(), polls.changeFetch);
    worker.openCursor = "cursor-a";
    worker.changeOutcome = { error: "torn batch" };
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.status).toEqual({ state: "initializing" });
    polls.release(0);
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INTEGRITY_FAILED" },
    });
    expect(worker.terminated).toBe(true);
    await expect(client.db.events.query().run()).rejects.toMatchObject({
      code: "INTEGRITY_FAILED",
    });
  });

  it("a reset during the fenced catch-up stops the old change stream and terminates its worker at once", async () => {
    // The catch-up poll never answers, so the old epoch never reaches its
    // fence. Nothing but the reset can end it.
    const workers: FakeWorker[] = [];
    const polls: AbortSignal[] = [];
    const client = createInternalColumnarClient<Collections>(
      {
        endpoint: "https://sync.test/root",
        auth: () => ({ bearerToken: "token" }),
        schema: createGeneratedClientSchema<Collections>(descriptor),
        memoryBudgetBytes: 32 * 1024 * 1024,
        deviceId: "device-a",
        fetch: validatedFetch(
          fakeFetch(fencedPlan(), (input, init) => {
            if (init?.signal) polls.push(init.signal);
            return abortingLongPoll(input, init);
          }),
        ),
        clientHeapReserveBytes: 4 * 1024 * 1024,
      },
      () => {
        const worker = new FakeWorker();
        worker.openCursor = "cursor-a";
        workers.push(worker);
        return worker as unknown as ColumnarEngineWorker;
      },
      fakeHashers().create,
    );
    await until(() => polls.length === 1, "the first catch-up poll");
    const reset = client.reset();
    await until(() => workers[0]?.terminated === true, "the old worker to end");
    expect(polls[0]?.aborted).toBe(true);
    // The next epoch catches up in its turn, on its own worker.
    await until(() => polls.length === 2, "the second catch-up poll");
    expect(workers).toHaveLength(2);
    expect(workers[1]?.terminated).toBe(false);
    expect(client.status).toEqual({ state: "initializing" });
    await client.close();
    await reset;
    expect(workers[1]?.terminated).toBe(true);
    expect(polls[1]?.aborted).toBe(true);
  });

  it("bases a mutation after a fenced catch-up on the cursor the catch-up reached", async () => {
    const polls = releasableChangePolls(1);
    const { client, worker, fetcher } = makeClient(
      fencedPlan(),
      polls.changeFetch,
    );
    worker.openCursor = "cursor-a";
    await until(() => polls.polls === 1, "the catch-up poll");
    polls.release(0);
    expect(await settled(client)).toEqual({ state: "ready" });
    await client.db.events.update({ id: 1n }, { level: "warn" });
    const post = fetcher.mock.calls.find(([input]) =>
      String(input).endsWith("/v1/mutations"),
    );
    const request = fromBinary(
      mutation.MutationBatchRequestSchema,
      new Uint8Array(post?.[1]?.body as ArrayBuffer),
    );
    expect(request.mutations[0]?.baseCursor?.token).toEqual(MOUNT_CURSOR);
    await client.close();
  });

  it("a reset that a second reset replaced loads nothing of its own", async () => {
    const plans: PromiseWithResolvers<void> = Promise.withResolvers();
    let bootstraps = 0;
    const { client, auth, workers, requests } = makeAttemptedClient(
      (inner) => async (input, init) => {
        if (String(input).endsWith("/v1/bootstrap")) {
          bootstraps += 1;
          if (bootstraps > 1) await plans.promise;
        }
        return inner(input, init);
      },
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    const first = client.reset();
    const second = client.reset();
    await until(() => bootstraps === 2, "the second reset's plan request");
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(auth).toHaveBeenCalledTimes(2);
    plans.resolve();
    await Promise.all([first, second]);
    expect(client.status).toEqual({ state: "ready" });
    expect(auth).toHaveBeenCalledTimes(2);
    expect(requests("/v1/bootstrap")).toBe(2);
    expect(workers).toHaveLength(2);
    expect(workers[0]?.terminated).toBe(true);
    await client.close();
  });

  it("a reset while auth() is pending asks for no plan under the grant it replaced", async () => {
    const stale = Promise.withResolvers<{ bearerToken: string }>();
    let auths = 0;
    const { client, fetcher } = makeClient(
      plan(),
      undefined,
      undefined,
      undefined,
      {
        auth: () =>
          (auths += 1) === 1 ? stale.promise : { bearerToken: "token" },
      },
    );
    const reset = client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    await reset;
    stale.resolve({ bearerToken: "stale" });
    await new Promise((resolve) => setTimeout(resolve, 20));
    const bearers = fetcher.mock.calls
      .filter(([input]) => String(input).endsWith("/v1/bootstrap"))
      .map(([, init]) => new Headers(init?.headers).get("authorization"));
    expect(bearers).toEqual(["Bearer token"]);
    expect(client.status).toEqual({ state: "ready" });
    await client.close();
  });

  it("reports an integrity failure without the engine's own words", async () => {
    // An engine refusal can carry internal names, offsets and page numbers,
    // and a hostile artifact could put anything in the string. None of that is
    // application state, so the classifier replaces the message rather than
    // forwarding it.
    const polls = releasableChangePolls(1);
    const { client, worker } = makeClient(fencedPlan(), polls.changeFetch);
    worker.openCursor = "cursor-a";
    worker.changeOutcome = {
      error: "staging schema rm_stage_17 exposed 0xdeadbeef from page 9",
      code: "ARTIFACT_INVALID",
    };
    await new Promise((resolve) => setTimeout(resolve, 20));
    polls.release(0);
    const status = await settled(client);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INTEGRITY_FAILED", recovery: "reset" },
    });
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.message).toBe("artifact integrity verification failed");
    expect(String(status.error)).not.toMatch(/rm_stage|deadbeef|page 9/iu);
  });

  it("refuses a catch-up that lands short of the activation cursor", async () => {
    const polls = releasableChangePolls(1);
    const { client, worker } = makeClient(fencedPlan(), polls.changeFetch);
    worker.openCursor = "cursor-a";
    // The mount moved, but not onto the fence: the change stream must keep
    // catching up with a further fenced request rather than report live.
    worker.changeOutcome = { cursor: "cursor-m" };
    for (let attempt = 0; polls.polls === 0 && attempt < 200; attempt += 1)
      await new Promise((resolve) => setTimeout(resolve, 5));
    polls.release(0);
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(client.status).toEqual({ state: "initializing" });
    expect(polls.polls).toBe(1);
    const second = polls.bodies[1];
    if (!second) throw new Error("no second fenced request followed");
    expect(decodeChangeRequest(second)).toMatchObject({
      from: "cursor-m",
      through: "cursor-p",
    });
    await client.close();
    expect(client.status).toEqual({ state: "closed" });
  });

  it("requests relations and subscribes the change stream with the plan's wire ids", async () => {
    const value = plan();
    const artifact = (value.artifacts as Record<string, unknown>[])[0];
    if (!artifact) throw new Error("fixture artifact missing");
    const wire = { events: "table:aaaa:g1", issues: "table:bbbb:g1" } as const;
    const runtime = artifact.runtime as { tables: Record<string, unknown>[] };
    for (const table of runtime.tables)
      table.tableId = wire[table.localTable as keyof typeof wire];
    value.requiredTables = [wire.events, wire.issues];
    artifact.tables = [wire.events, wire.issues];
    const polls = releasableChangePolls(0);
    const { client, worker, fetcher } = makeClient(value, polls.changeFetch);
    expect(await settled(client)).toEqual({ state: "ready" });
    for (
      let attempt = 0;
      polls.bodies.length === 0 && attempt < 200;
      attempt += 1
    )
      await new Promise((resolve) => setTimeout(resolve, 5));
    const poll = polls.bodies[0];
    if (!poll) throw new Error("no change poll was sent");
    expect(
      decodeChangeRequest(poll).tables.map(({ tableId }) => tableId),
    ).toEqual([wire.events, wire.issues]);
    const body = JSON.parse(String(fetcher.mock.calls[0]?.[1]?.body)) as {
      requiredTables: string[];
    };
    expect(body.requiredTables).toEqual(["public.events", "public.issues"]);
    expect(worker.requests[0]?.changeStream).toMatchObject({
      tables: [
        { tableId: wire.events, collection: "events" },
        { tableId: wire.issues, collection: "issues" },
      ],
    });
    await client.close();
  });

  it("switching the principal replaces the worker and leaves no row of the old projection", async () => {
    // The server decides the projection from the grant alone, so when the
    // application switches principals no row of the projection it left may
    // survive into the one it joined: not in a mount, a cached query
    // resource, or a facade that outlived the swap.
    const workers: FakeWorker[] = [];
    const principals = {
      a: { bearerToken: "grant-a" },
      b: { bearerToken: "grant-b" },
    };
    const rows = { a: [[1n, "a-only"]], b: [[2n, "b-only"]] } as const;
    let principal: "a" | "b" = "a";
    const planFor = (which: "a" | "b") => {
      const value = plan();
      const [artifact] = value.artifacts as Record<string, unknown>[];
      if (!artifact) throw new Error("the fixture plan has no artifact");
      return {
        ...value,
        planId: `plan-${which}`,
        authorization: {
          ...(value.authorization as Record<string, unknown>),
          // Two projections: distinct scope identities and distinct tokens.
          scopeHash: `sha256:${(which === "a" ? "a" : "b").repeat(64)}`,
          scopeToken: `scope-${which}`,
        },
        artifacts: [
          {
            ...artifact,
            artifactId: `artifact-${which}`,
            selectedRepresentation: {
              ...(artifact.selectedRepresentation as Record<string, unknown>),
              downloadUrl: `https://sync.test/artifact-${which}`,
            },
          },
        ],
      };
    };
    const bearerTokens: string[] = [];
    const fetcher = vi.fn<typeof fetch>(
      validatedFetch(async (input, init) => {
        const url = String(input instanceof Request ? input.url : input);
        if (url.endsWith("/change")) return abortingLongPoll(input, init);
        if (url.endsWith("/v1/bootstrap")) {
          const authorization = String(
            (init?.headers as Record<string, string>).authorization,
          );
          bearerTokens.push(authorization);
          return new Response(JSON.stringify(planFor(principal)), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        }
        if (url.endsWith("/artifact-a") || url.endsWith("/artifact-b"))
          return new Response(byteStream(new Uint8Array(ARTIFACT_BYTES)), {
            status: 200,
            headers: {
              "content-type": "application/vnd.calque.direct-columnar.v4",
              "content-length": String(ARTIFACT_BYTES),
            },
          });
        return new Response("not found", { status: 404 });
      }),
    );
    const client = createInternalColumnarClient<Collections>(
      {
        endpoint: "https://sync.test/root",
        auth: () => principals[principal],
        schema: createGeneratedClientSchema<Collections>(descriptor),
        memoryBudgetBytes: 32 * 1024 * 1024,
        deviceId: "device-a",
        fetch: fetcher,
        clientHeapReserveBytes: 4 * 1024 * 1024,
      },
      () => {
        const worker = new FakeWorker();
        worker.queryRows = rows[principal];
        workers.push(worker);
        return worker as unknown as ColumnarEngineWorker;
      },
      fakeHashers().create,
    );

    expect(await settled(client)).toEqual({ state: "ready" });
    const events = client.db.events;
    expect(await events.query().run()).toEqual([{ id: 1n, level: "a-only" }]);
    expect(workers).toHaveLength(1);
    expect(bearerTokens).toEqual(["Bearer grant-a"]);

    // The application hands the client its other principal and resets.
    principal = "b";
    await client.reset();

    // A new worker, and the one that held A's mount destroyed with it: an
    // engine cannot be carried across the boundary.
    expect(workers).toHaveLength(2);
    expect(workers[0]?.terminated).toBe(true);
    expect(workers[1]?.terminated).toBe(false);
    expect(bearerTokens).toEqual(["Bearer grant-a", "Bearer grant-b"]);
    expect(
      fetcher.mock.calls.filter(([input]) =>
        String(input).endsWith("/artifact-b"),
      ),
    ).toHaveLength(1);
    // B's plan, B's artifact, B's rows -- through the facade the application
    // was already holding, which must not answer out of the old mount.
    expect(client.status).toEqual({ state: "ready" });
    expect(await events.query().run()).toEqual([{ id: 2n, level: "b-only" }]);
    expect(await client.db.events.query().run()).toEqual([
      { id: 2n, level: "b-only" },
    ]);
    expect(
      workers[1]?.requests.filter((request) => request.type === "query"),
    ).not.toHaveLength(0);
    // Nothing was asked of A's worker after the swap.
    const askedOfA = workers[0]?.requests.length ?? 0;
    await client.db.events.query().run();
    expect(workers[0]?.requests).toHaveLength(askedOfA);

    await client.close();
  });

  it("fails the live queries of a frame the worker fails, and reads them again at the next publication", async () => {
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.frameError = { name: "Error", message: "the worker's reason" };
    const resource = client.db.events.query().subscribe();
    const stop = resource.subscribe(() => undefined);
    await expect(resource.promise).rejects.toMatchObject({
      code: "QUERY_FAILED",
      message: "the worker's reason",
    });
    expect(resource.getSnapshot()).toMatchObject({ status: "error" });

    worker.frameError = undefined;
    await publish();
    await resource.promise;
    expect(resource.getSnapshot()).toMatchObject({ status: "ready" });
    stop();
    await client.close();
  });

  it("keeps one subscription per distinct query specification", async () => {
    // Two live queries over two collections must be two subscriptions. The
    // session's reuse cache decides that, and a cache that cannot tell them
    // apart hands the second caller the first one's resource: the page then
    // renders one collection's rows under both facades, and a publication
    // that moved only the second collection re-runs nothing at all.
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const events = client.db.events.query().subscribe();
    const issues = client.db.issues.query().subscribe();
    expect(issues).not.toBe(events);
    // The same query twice is still one subscription: the reuse the cache
    // exists for has to keep working.
    expect(client.db.events.query().subscribe()).toBe(events);

    let eventsReads = 0;
    let issuesReads = 0;
    const stopEvents = events.subscribe(() => {
      eventsReads += 1;
    });
    const stopIssues = issues.subscribe(() => {
      issuesReads += 1;
    });
    await Promise.all([events.promise, issues.promise]);
    expect(eventsReads).toBe(1);
    expect(issuesReads).toBe(1);
    const frames = worker.requests.filter(
      (request) => request.type === "frame",
    );
    expect(frames).toHaveLength(1);
    expect(
      (frames[0]?.members as { specification: QuerySpecification }[]).map(
        (member) => member.specification.from,
      ),
    ).toEqual([
      { collection: "events", as: "events" },
      { collection: "issues", as: "issues" },
    ]);

    // A publication that moved only `issues` re-reads only the issues query.
    worker.changedCollections = ["issues"];
    await publish();
    await Promise.all([events.promise, issues.promise]);
    expect(eventsReads).toBe(1);
    expect(issuesReads).toBe(2);

    stopEvents();
    stopIssues();
    await client.close();
  });

  it("hands the worker the reads asked for before ready in the order they were asked", async () => {
    // A page asks for its first view's reads as it loads, and the worker
    // answers in the order it is asked, so the page asks first for what it
    // shows first. That holds only if the session sends the reads that waited
    // for the mount in the order they were asked, whatever their kind.
    const { client, worker } = makeClient();
    const counted = client.db.events.query().run();
    const window = client.db.issues.slice(0, 10);
    const listed = client.db.issues.query().subscribe().promise;
    const filtered = client.db.events.query().where(eq("level", "error")).run();
    expect(client.status.state).not.toBe("ready");
    await Promise.all([counted, window, listed, filtered]);
    const reads = worker.requests.flatMap((request) => {
      if (request.type === "rows") return [`rows ${String(request.table)}`];
      const queries =
        request.type === "frame"
          ? (request.members as { specification: QuerySpecification }[])
          : request.type === "query"
            ? [request as unknown as { specification: QuerySpecification }]
            : [];
      return queries.map(
        ({ specification: { from, where } }) =>
          `query ${from.collection}${where ? " where" : ""}`,
      );
    });
    expect(reads).toEqual([
      "query events",
      "rows issues",
      "query issues",
      "query events where",
    ]);
    await client.close();
  });

  it("drops a result that succeeded after the epoch moved", async () => {
    const { client, worker } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    worker.holdQueries = true;
    const pending = client.db.events.query().run();
    await new Promise((resolve) => setTimeout(resolve, 5));
    const held = worker.held[0];
    if (!held) throw new Error("held query missing");
    // The worker answers, then the epoch moves before that answer is read:
    // the rows are from a mount the session no longer serves.
    worker.holdQueries = false;
    worker.answer(held);
    const reset = client.reset();
    await expect(pending).rejects.toMatchObject({ code: "CANCELLED" });
    await reset;
    await client.close();
  });

  it("publishes the cursor the mount moved to", async () => {
    // `SyncPublication.cursor` is the only place a consumer learns where the
    // collections now stand. A publication that reported the artifact's
    // opening cursor after the mount had moved would have the page label
    // fresh rows with a stale cut.
    const { client, worker, publish } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const cursors: string[] = [];
    const stop = client.subscribePublications((publication) => {
      cursors.push(publication.cursor);
    });
    worker.changeOutcome = { cursor: "cursor-q" };
    await publish();
    worker.changeOutcome = { cursor: "cursor-r" };
    await publish();
    expect(cursors).toEqual(["cursor-q", "cursor-r"]);
    stop();
    await client.close();
  });

  it("stops notifying publication listeners once the activation they read is gone", async () => {
    // The listeners run after the re-reads, so one of them can close or reset
    // the client. Every listener after that would be handed a publication of
    // a mount the session no longer serves.
    const { client, poll } = makePublishingClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const seen: string[] = [];
    let closing: Promise<void> | undefined;
    const stopFirst = client.subscribePublications(() => {
      seen.push("first");
      closing = client.close();
    });
    const stopSecond = client.subscribePublications(() => {
      seen.push("second");
    });
    await until(() => poll.open > 0, "an open change poll");
    poll.answer();
    await until(() => seen.length > 0, "the publication");
    expect(seen).toEqual(["first"]);
    stopFirst();
    stopSecond();
    await closing;
    expect(client.status).toEqual({ state: "closed" });
  });

  it("keeps one subscription per specification and result bound", async () => {
    // Reuse is keyed on the specification *and* the bounds the caller asked
    // for. Collapsing two different bounds into one subscription hands the
    // second caller rows it never asked for, and silently at that: the rows
    // are well formed, just cut to somebody else's limit.
    const { client } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const oneRow = client.db.events.query().subscribe({ maxRows: 1 });
    const twoRows = client.db.events.query().subscribe({ maxRows: 2 });
    const unbounded = client.db.events.query().subscribe();
    const smallBytes = client.db.events.query().subscribe({ maxBytes: 1024 });
    const largeBytes = client.db.events.query().subscribe({ maxBytes: 2048 });
    expect(twoRows).not.toBe(oneRow);
    expect(unbounded).not.toBe(oneRow);
    expect(unbounded).not.toBe(smallBytes);
    expect(largeBytes).not.toBe(smallBytes);
    // The reuse the scan exists for still holds for an identical request.
    expect(client.db.events.query().subscribe({ maxRows: 1 })).toBe(oneRow);
    expect(client.db.events.query().subscribe({ maxBytes: 1024 })).toBe(
      smallBytes,
    );
    await client.close();
  });

  it("refuses an oversized specification whether or not a query is live", async () => {
    // Reuse compares canonical text, which validation produces, so a
    // specification is validated on every subscribe, even with no other query
    // live to compare it against.
    const { client } = makeClient();
    expect(await settled(client)).toEqual({ state: "ready" });
    const oversized = () =>
      client.db.events
        .query()
        .where(eq("level", "x".repeat(MAX_QUERY_SPEC_BYTES)))
        .subscribe();
    expect(oversized).toThrow(
      `query specification exceeds ${MAX_QUERY_SPEC_BYTES} bytes`,
    );
    client.db.events.query().subscribe();
    expect(oversized).toThrow(
      `query specification exceeds ${MAX_QUERY_SPEC_BYTES} bytes`,
    );
    await client.close();
  });

  it("reports a change-stream failure raised after the fence but before the activation was adopted", async () => {
    // The fence resolves inside the change stream's own turn, and it polls
    // again in that same turn. A poll that fails there raises the failure
    // while the session still has no activation to fail, so the activation
    // carries it and the session reports it instead of ready.
    let changePolls = 0;
    const { client, worker } = makeClient(
      fencedPlan(),
      () => Promise.resolve(new Response(opaqueBatch())),
      undefined,
      (inner) =>
        ((input: RequestInfo | URL, init?: RequestInit) => {
          if (String(input).endsWith("/change")) {
            changePolls += 1;
            if (changePolls > 1)
              throw new Error("the change endpoint vanished");
          }
          return inner(input, init);
        }) as typeof fetch,
    );
    worker.openCursor = "cursor-a";
    const status = await settled(client);
    expect(changePolls).toBe(2);
    expect(status).toMatchObject({
      state: "error",
      error: { code: "INTERNAL_ERROR", recovery: "reset" },
    });
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.message).toContain("the change endpoint vanished");
    expect(worker.terminated).toBe(true);
  });

  it("paints two collections one source transaction moved at one source state, through the public surface alone", async () => {
    // `renderTwoCollections` is written the way an application writes it:
    // `client.db.<collection>` and `client.subscribePublications`, nothing
    // else. Its per-collection callbacks read both snapshots the way a
    // `useSyncExternalStore` render does.
    const poll = controllableLongPoll();
    const { client, worker } = makeClient(plan(), poll.fetcher);
    // Three source states. Generation 0 is the artifact's cut; each batch
    // replaces both collections' rows at once, the way one committed
    // transaction moves two tables.
    const generations: readonly Frame[] = [
      { events: "1:error", issues: "2:open" },
      { events: "1:warn", issues: "2:closed" },
      { events: "1:fatal", issues: "2:archived" },
    ];
    const commit = (generation: number) => {
      const frame = generations[generation];
      if (!frame) throw new Error(`no source generation ${generation}`);
      worker.queryAnswers.set("events", {
        columns: ["id", "level"],
        rows: [[1n, frame.events.slice(2)]],
      });
      worker.queryAnswers.set("issues", {
        columns: ["id", "title"],
        rows: [[2n, frame.issues.slice(2)]],
      });
    };
    commit(0);
    worker.changedCollections = ["events", "issues"];

    expect(await settled(client)).toEqual({ state: "ready" });
    const painted: PaintedFrame[] = [];
    const perCollection: Frame[] = [];
    const application = renderTwoCollections(client, painted, perCollection);
    await application.firstRead;
    expect(painted).toEqual([]);
    expect(
      worker.requests.filter((request) => request.type === "frame"),
    ).toHaveLength(1);

    // The first batch, with the read held in the worker.
    worker.changeOutcome = { cursor: "cursor-q" };
    worker.onChange = () => commit(1);
    worker.holdQueriesFor = "issues";
    await until(() => poll.open === 1, "the poll before the first batch");
    poll.answer();
    await until(() => worker.held.length === 1, "the first batch's frame");
    expect(painted).toEqual([]);
    worker.holdQueriesFor = undefined;
    worker.releaseHeld();
    await until(() => painted.length === 1, "the paint of cursor-q");
    await application.drained();

    // The second batch, unheld, so the ordinary path is exercised too.
    worker.changeOutcome = { cursor: "cursor-r" };
    worker.onChange = () => commit(2);
    await until(() => poll.open === 1, "the poll before the second batch");
    poll.answer();
    await until(() => painted.length === 2, "the paint of cursor-r");
    await application.drained();

    expect(painted).toEqual([
      { cursor: "cursor-q", ...generations[1] },
      { cursor: "cursor-r", ...generations[2] },
    ]);
    // Every frame a per-collection callback saw is a pair the source
    // committed, and each callback saw each batch.
    expect(perCollection).toEqual([
      generations[0],
      generations[0],
      generations[1],
      generations[1],
      generations[2],
      generations[2],
    ]);
    expect(application.publications).toEqual([
      {
        cursor: "cursor-q",
        latestCommitTimeUnixMicros: 1_700_000_000_000_000n,
        changedCollections: ["events", "issues"],
        transactionCount: 1,
      },
      {
        cursor: "cursor-r",
        latestCommitTimeUnixMicros: 1_700_000_000_000_000n,
        changedCollections: ["events", "issues"],
        transactionCount: 1,
      },
    ]);

    application.stop();
    await client.close();
  });

  it("paints batches that later batches overtook at one source state, the newest", async () => {
    const poll = controllableLongPoll();
    const { client, worker } = makeClient(plan(), poll.fetcher);
    const generations: readonly Frame[] = [
      { events: "1:error", issues: "2:open" },
      { events: "1:warn", issues: "2:closed" },
      { events: "1:fatal", issues: "2:archived" },
      { events: "1:trace", issues: "2:reopened" },
    ];
    const commit = (generation: number) => {
      const frame = generations[generation];
      if (!frame) throw new Error(`no source generation ${generation}`);
      worker.queryAnswers.set("events", {
        columns: ["id", "level"],
        rows: [[1n, frame.events.slice(2)]],
      });
      worker.queryAnswers.set("issues", {
        columns: ["id", "title"],
        rows: [[2n, frame.issues.slice(2)]],
      });
    };
    commit(0);
    worker.changedCollections = ["events", "issues"];

    expect(await settled(client)).toEqual({ state: "ready" });
    const painted: PaintedFrame[] = [];
    const perCollection: Frame[] = [];
    const application = renderTwoCollections(client, painted, perCollection);
    await application.firstRead;

    // Three batches land while the first batch's frame is in the worker.
    worker.holdQueries = true;
    for (const [generation, cursor] of [
      [1, "cursor-q"],
      [2, "cursor-r"],
      [3, "cursor-s"],
    ] as const) {
      worker.changeOutcome = { cursor };
      worker.onChange = () => commit(generation);
      await until(() => poll.open === 1, `the poll before ${cursor}`);
      poll.answer();
      await until(
        () => application.publications.length === generation,
        `the publication of ${cursor}`,
      );
    }
    expect(worker.held).toHaveLength(1);
    expect(painted).toEqual([]);

    // The worker answers the frame at the state it then holds, the third
    // batch's, so all three paints show that state, and nothing reads again.
    worker.holdQueries = false;
    worker.releaseHeld();
    await until(() => painted.length === 3, "the three paints");
    await application.drained();
    expect(painted).toEqual([
      { cursor: "cursor-q", ...generations[3] },
      { cursor: "cursor-r", ...generations[3] },
      { cursor: "cursor-s", ...generations[3] },
    ]);
    expect(perCollection).toEqual([
      generations[0],
      generations[0],
      generations[3],
      generations[3],
    ]);
    expect(
      worker.requests.filter((request) => request.type === "frame"),
    ).toHaveLength(2);

    application.stop();
    await client.close();
  });

  /**
   * A client that falls behind rebuilds from a fresh snapshot, since nothing
   * between the mount and the source can throttle a commit. These three tests
   * show the rebuild happens, never shows two epochs at once, and stops.
   */
  it("client_rebuilds_from_a_resnapshot_required_without_a_reload", async () => {
    const harness = makeRebuildableClient([
      {
        cursor: "cursor-p",
        marker: "epoch-one",
        change: {
          error: "the source no longer retains cursor-p",
          code: "RESNAPSHOT_REQUIRED",
        },
      },
      {
        cursor: "cursor-q",
        marker: "epoch-two",
        change: { cursor: "cursor-q" },
      },
    ]);
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "epoch-one" },
      { id: 2n, level: "epoch-one" },
    ]);

    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();

    // Nothing reloads the page and nothing calls `reset()`: the session
    // acquires a fresh artifact, swaps the engine behind the same facades, and
    // reports ready again on its own.
    await until(
      () => workers.length === 2,
      "the rebuild to mint a second mount",
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(harness.bootstraps).toBe(2);
    expect(workers[0]?.terminated).toBe(true);
    expect(workers[1]?.terminated).toBe(false);

    // The facades read the new mount, and the stream resumed at the new
    // artifact's cursor rather than at the one the source refused.
    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "epoch-two" },
      { id: 2n, level: "epoch-two" },
    ]);
    expect(await client.db.issues.slice(0, 10)).toEqual([
      { id: 3n, title: "epoch-two" },
    ]);
    const resumed = poll.requests.at(-1);
    expect(decoder.decode(resumed?.fromCursorExclusive?.token)).toBe(
      "cursor-q",
    );
    expect(resumed?.throughCursorInclusive).toBeUndefined();
    await client.close();
  });

  it("facades_never_observe_a_mixed_epoch_during_rebuild", async () => {
    // The replacement plan is held so the rebuild stays in flight long enough
    // to watch.
    let releaseRebuild!: () => void;
    const held = new Promise<void>((resolve) => {
      releaseRebuild = resolve;
    });
    const harness = makeRebuildableClient(
      [
        {
          cursor: "cursor-p",
          marker: "epoch-one",
          change: {
            error: "the source no longer retains cursor-p",
            code: "RESNAPSHOT_REQUIRED",
          },
        },
        {
          cursor: "cursor-q",
          marker: "epoch-two",
          change: { cursor: "cursor-q" },
        },
      ],
      (index) => (index === 1 ? held : undefined),
    );
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });

    // Two facades, read together, all the way through the rebuild. One
    // collection served by the outgoing mount while the other is served by the
    // incoming one would show up here as one result naming both markers.
    const observed: string[] = [];
    const refusals: (string | undefined)[] = [];
    let attempts = 0;
    let reading = true;
    const reader = (async () => {
      while (reading) {
        attempts += 1;
        try {
          const [events, window] = await Promise.all([
            client.db.events.query().run(),
            client.db.issues.slice(0, 10),
          ]);
          const markers = new Set([
            ...events.map((row) => row.level),
            ...window.map((row) => row.title ?? ""),
          ]);
          observed.push([...markers].sort().join("+"));
        } catch (error) {
          refusals.push((error as { readonly code?: string }).code);
        }
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    })();

    await until(() => observed.length > 0, "a read before the rebuild");
    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(
      () => client.status.state === "initializing",
      "the rebuild to start",
    );

    // The rebuild is in flight and the outgoing mount is gone. Every facade
    // call made in this window parks: it does not read the epoch that is being
    // replaced, and it does not read a half-built one either.
    const settledBeforeHold = observed.length;
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(observed.length).toBe(settledBeforeHold);
    expect(client.status).toEqual({ state: "initializing" });

    releaseRebuild();
    await until(
      () => workers.length === 2,
      "the rebuild to mint a second mount",
    );
    expect(await settled(client)).toEqual({ state: "ready" });
    await until(
      () => observed.includes("epoch-two"),
      "a read after the rebuild",
    );
    reading = false;
    await reader;

    expect(observed.filter((entry) => entry.includes("+"))).toEqual([]);
    expect([...new Set(observed)].sort()).toEqual(["epoch-one", "epoch-two"]);
    // The epochs did not interleave either: once the new mount answered, the
    // old one never answered again.
    expect(observed.lastIndexOf("epoch-one")).toBeLessThan(
      observed.indexOf("epoch-two"),
    );
    // The mechanism behind that: the outgoing engine is closed before the
    // incoming one is opened, so no facade call can be served by two mounts.
    expect(harness.predecessorClosedAtOpen).toEqual([undefined, true]);
    // Every read the loop issued is in one of the two arrays, so neither can
    // be quietly empty. The only refusal permitted is `CANCELLED`, for a read
    // already in flight when the epoch moved.
    expect(observed.length + refusals.length).toBe(attempts);
    expect(refusals.filter((code) => code !== "CANCELLED")).toEqual([]);
    await client.close();
  });

  it("a_failed_rebuild_ends_terminal_and_says_so", async () => {
    // Every mount this source serves is refused by the stream on its first
    // poll, so no rebuild ever recovers anything. The clock is frozen, so all
    // four refusals fall inside one window and it is the count that ends the
    // session — the elapsed-time half of the bound is the next test's.
    vi.spyOn(Date, "now").mockReturnValue(Date.UTC(2026, 0, 1));
    const harness = makeRebuildableClient([
      {
        cursor: "cursor-p",
        marker: "epoch-one",
        change: {
          error: "the source no longer retains cursor-p",
          code: "RESNAPSHOT_REQUIRED",
        },
      },
    ]);
    const { client, poll, workers } = harness;
    // The first activation and the three rebuilds the budget allows. Each
    // mount opens complete and reports ready; each is then refused by the
    // stream, and the next mount appearing is the rebuild that refusal started.
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      await until(() => workers.length === attempt, `mount ${attempt}`);
      expect(await settled(client)).toEqual({ state: "ready" });
      await until(() => poll.open > 0, `the poll of mount ${attempt}`);
      poll.answer();
    }
    await until(
      () => client.status.state === "error",
      "the exhausted rebuild budget to report",
    );

    const status = client.status;
    expect(status).toMatchObject({
      state: "error",
      error: { code: "RESNAPSHOT_REQUIRED", recovery: "reset" },
    });
    if (status.state !== "error") throw new Error("expected an error");
    expect(status.error.message).toContain("did not reach a live cursor");
    expect(status.error.message).toContain("the source no longer retains");

    // Terminal means stopped: no fifth plan is requested, no fifth mount is
    // opened, and every mount the run built is closed.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(harness.bootstraps).toBe(4);
    expect(workers).toHaveLength(4);
    expect(workers.every((worker) => worker.terminated)).toBe(true);
    // And the facades say so rather than parking on a rebuild that will not
    // come.
    await expect(client.db.events.query().run()).rejects.toMatchObject({
      code: "RESNAPSHOT_REQUIRED",
      recovery: "reset",
    });

    // `recovery: "reset"` holds: calling it builds a fifth mount that opens
    // complete and answers the facades again.
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(workers).toHaveLength(5);
    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "epoch-one" },
      { id: 2n, level: "epoch-one" },
    ]);
    await client.close();
  });

  it("a_rebuild_window_that_elapsed_reopens_rather_than_ending_the_tab", async () => {
    // The same source as the test above, but an hour passes between
    // refusals, as when a quiet tab's grant expires and the stream answers 401
    // `resnapshot_required`. Each refusal is one recovered rebuild, so the
    // budget is a rate rather than a lifetime allowance.
    let clock = Date.UTC(2026, 0, 1);
    vi.spyOn(Date, "now").mockImplementation(() => clock);
    const harness = makeRebuildableClient([
      {
        cursor: "cursor-p",
        marker: "epoch-one",
        change: {
          error: "the grant this stream opened under expired",
          code: "RESNAPSHOT_REQUIRED",
        },
      },
    ]);
    const { client, poll, workers } = harness;
    // Twice the count one window allows, so a lifetime budget would have
    // reported terminal at the fourth mount instead of building a seventh.
    for (let hour = 1; hour <= 6; hour += 1) {
      await until(() => workers.length === hour, `mount ${hour}`);
      expect(await settled(client)).toEqual({ state: "ready" });
      expect(await client.db.events.query().run()).toEqual([
        { id: 1n, level: "epoch-one" },
        { id: 2n, level: "epoch-one" },
      ]);
      await until(() => poll.open > 0, `the poll of mount ${hour}`);
      clock += 60 * 60 * 1000;
      poll.answer();
    }
    await until(() => workers.length === 7, "the seventh mount");
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(client.status).toEqual({ state: "ready" });
    await client.close();
  });

  it("an_adopted_publication_refills_the_rebuild_budget", async () => {
    // One window for the whole test, and six refusals in it, twice what the
    // window allows. The batch the stream serves each mount before refusing
    // the next poll keeps the session alive: only a publication clears the
    // count.
    vi.spyOn(Date, "now").mockReturnValue(Date.UTC(2026, 0, 1));
    const refusal = {
      error: "the source no longer retains cursor-p",
      code: "RESNAPSHOT_REQUIRED",
    } as const;
    const harness = makeRebuildableClient([
      { cursor: "cursor-p", marker: "epoch-one", change: { ...refusal } },
    ]);
    const { client, poll, workers } = harness;
    const publications: SyncPublication[] = [];
    const stop = client.subscribePublications((publication) => {
      publications.push(publication);
    });
    for (let pass = 1; pass <= 6; pass += 1) {
      await until(() => workers.length === pass, `mount ${pass}`);
      expect(await settled(client)).toEqual({ state: "ready" });
      const mount = workers.at(-1);
      if (!mount) throw new Error("the harness minted no mount");
      // Served, then refused: the first poll of this mount moves it and
      // publishes, the second is the fall-behind that starts the next
      // rebuild.
      mount.changeOutcome = { cursor: `cursor-live-${pass}` };
      await until(() => poll.open > 0, `the served poll of mount ${pass}`);
      poll.answer();
      await until(
        () => publications.length === pass,
        `the publication of mount ${pass}`,
      );
      mount.changeOutcome = { ...refusal };
      await until(() => poll.open > 0, `the refused poll of mount ${pass}`);
      poll.answer();
    }
    await until(() => workers.length === 7, "the seventh mount");
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(publications).toHaveLength(6);
    expect(publications.at(-1)?.cursor).toBe("cursor-live-6");
    stop();
    await client.close();
  });

  /**
   * A revocation barrier suspends affected queries, cancels pending
   * mutations, discards resident state and rebuilds from a fresh grant. These
   * three tests cover that order and the bound that stops it looping. The
   * barrier arrives as the worker refusal a decoded barrier raises, the seam
   * the session sees.
   */
  it("client_suspends_cancels_discards_and_rebuilds_on_a_revocation_barrier", async () => {
    let releaseRebuild!: () => void;
    const held = new Promise<void>((resolve) => {
      releaseRebuild = resolve;
    });
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
      (index) => (index === 1 ? held : undefined),
    );
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });

    // A subscription holding the revoked scope's rows, and a write in flight.
    const rows = client.db.events.query().subscribe();
    rows.subscribe(() => undefined);
    await rows.promise;
    expect(rows.getSnapshot()).toMatchObject({
      status: "ready",
      result: [
        { id: 1n, level: "epoch-one" },
        { id: 2n, level: "epoch-one" },
      ],
    });
    const write = await client.db.events.update({ id: 1n }, { level: "warn" });
    expect(client.mutationStatus(write.mutationId)?.state).toBe(
      "awaiting_canonical_echo",
    );

    // The order probe: what was already true when the outgoing engine was
    // closed. Teardown is step three, so a mutation still awaiting its echo
    // here, or a snapshot still holding revoked rows here, would be the two
    // steps before it not having happened.
    const first = workers[0];
    if (!first) throw new Error("the harness minted no mount");
    let atTeardown:
      | { status: string; mutation: string | undefined; snapshot: string }
      | undefined;
    const terminate = first.terminate.bind(first);
    first.terminate = () => {
      atTeardown ??= {
        status: client.status.state,
        mutation: client.mutationStatus(write.mutationId)?.state,
        snapshot: rows.getSnapshot().status,
      };
      terminate();
    };

    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(() => atTeardown !== undefined, "the outgoing engine to close");
    expect(atTeardown).toEqual({
      // 1. suspended
      status: "initializing",
      // 2. cancelled
      mutation: "ambiguous",
      // 3a. the revoked scope's rows are off the facade surface before 3b,
      // the engine that holds them, is closed.
      snapshot: "loading",
    });
    expect(client.mutationStatus(write.mutationId)).toMatchObject({
      state: "ambiguous",
      error: { code: "MUTATION_AMBIGUOUS" },
    });

    // No facade is served while the session is suspended. This read is issued
    // after the barrier and stays pending for as long as the rebuild is held.
    let settledQuery = false;
    const parked = client.db.events
      .query()
      .run()
      .then((result) => {
        settledQuery = true;
        return result;
      });
    for (let turn = 0; turn < 20; turn += 1)
      await new Promise((resolve) => setTimeout(resolve, 1));
    expect(settledQuery).toBe(false);
    expect(rows.getSnapshot().status).not.toBe("ready");

    // 4. rebuilt from a fresh grant: a second bootstrap, a second mount, and
    // the facades answering the new epoch.
    releaseRebuild();
    await until(() => workers.length === 2, "the replacement mount");
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(harness.bootstraps).toBe(2);
    expect(await parked).toEqual([
      { id: 1n, level: "epoch-two" },
      { id: 2n, level: "epoch-two" },
    ]);
    await rows.promise;
    expect(rows.getSnapshot()).toMatchObject({
      status: "ready",
      result: [
        { id: 1n, level: "epoch-two" },
        { id: 2n, level: "epoch-two" },
      ],
    });
    // The retained envelope is gone with the scope it was signed under, so
    // there is nothing to resubmit against the retired grant.
    await expect(client.retryMutation(write.mutationId)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    await client.close();
  });

  /**
   * A mutation already executing when the application fences commits first,
   * and its echo lands past the barrier, so only the rebuilt snapshot shows
   * it. The write stays ambiguous, and its message sends the application to
   * that snapshot rather than telling it to reissue, which would apply it
   * twice.
   */
  it("a_write_committed_before_the_barrier_is_in_the_rebuilt_snapshot_and_stays_ambiguous", async () => {
    const harness = makeRebuildableClient([
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
        queryRows: [
          [1n, "warn"],
          [2n, "epoch-two"],
        ],
        change: { cursor: "cursor-q" },
      },
    ]);
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    // The update resolves only once the server has answered, and the harness
    // answers every submission ACCEPTED.
    const write = await client.db.events.update({ id: 1n }, { level: "warn" });

    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(() => workers.length === 2, "the replacement mount");
    expect(await settled(client)).toEqual({ state: "ready" });
    expect(harness.bootstraps).toBe(2);
    expect(await client.db.events.query().run()).toEqual([
      { id: 1n, level: "warn" },
      { id: 2n, level: "epoch-two" },
    ]);

    const status = client.mutationStatus(write.mutationId);
    if (status?.state !== "ambiguous")
      throw new Error(`expected an ambiguous write, got ${status?.state}`);
    expect(status.error.code).toBe("MUTATION_AMBIGUOUS");
    expect(status.error.message).toBe(
      "an authenticated revocation barrier retired this mutation's grant before the canonical echo; the mutation may have committed, so read the rebuilt snapshot before reissuing it",
    );
    await expect(client.retryMutation(write.mutationId)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    await client.close();
  });

  it("no_row_of_the_revoked_scope_reaches_a_facade_after_the_barrier", async () => {
    let releaseRebuild!: () => void;
    const held = new Promise<void>((resolve) => {
      releaseRebuild = resolve;
    });
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
      (index) => (index === 1 ? held : undefined),
    );
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });

    // Everything the application could see, read as fast as it can, from
    // before the barrier until after the rebuild. A revoked-scope row observed
    // after the barrier's turn is the failure this test exists for.
    const observed: string[] = [];
    const refusals: (string | undefined)[] = [];
    let attempts = 0;
    let reading = true;
    const reader = (async () => {
      while (reading) {
        attempts += 1;
        try {
          const result = await client.db.events.query().run();
          observed.push(
            `${String(attempts)}:${result.map((row) => row.level).join(",")}`,
          );
        } catch (error) {
          refusals.push(
            error instanceof Error && "code" in error
              ? String(error.code)
              : undefined,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    })();

    await until(() => observed.length > 0, "a read before the barrier");
    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(
      () => workers[0]?.terminated === true,
      "the revoked mount to be discarded",
    );
    const barrierAt = attempts;
    releaseRebuild();
    await until(() => workers.length === 2, "the replacement mount");
    expect(await settled(client)).toEqual({ state: "ready" });
    await until(
      () => observed.some((entry) => entry.includes("epoch-two")),
      "a read from the rebuilt mount",
    );
    reading = false;
    await reader;

    // Nothing after the barrier named the revoked epoch, and nothing ever
    // named both at once.
    const after = observed.filter(
      (entry) => Number(entry.split(":")[0]) > barrierAt,
    );
    expect(after.length).toBeGreaterThan(0);
    expect(after.filter((entry) => entry.includes("epoch-one"))).toEqual([]);
    expect(
      observed.filter(
        (entry) => entry.includes("epoch-one") && entry.includes("epoch-two"),
      ),
    ).toEqual([]);
    // Every attempt is accounted for, so the filter above cannot be vacuous
    // because reads were quietly failing.
    expect(observed.length + refusals.length).toBe(attempts);
    expect(refusals.filter((code) => code !== "CANCELLED")).toEqual([]);
    await client.close();
  });

  it("a_revocation_that_never_recovers_locks_under_the_shared_budget", async () => {
    // Every mount this source serves is revoked on its first poll. The clock
    // is frozen, so all four barriers fall in one window and the count ends
    // the session -- the same budget the fall-behind branch spends, because a
    // stream that keeps producing barriers costs one mount per pass either
    // way.
    vi.spyOn(Date, "now").mockReturnValue(Date.UTC(2026, 0, 1));
    const harness = makeRebuildableClient([
      {
        cursor: "cursor-p",
        marker: "epoch-one",
        change: {
          error: "authorization generation 7 was revoked by barrier b-1",
          code: "AUTHORIZATION_REVOKED",
        },
      },
    ]);
    const { client, poll, workers } = harness;
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      await until(() => workers.length === attempt, `mount ${attempt}`);
      expect(await settled(client)).toEqual({ state: "ready" });
      await until(() => poll.open > 0, `the poll of mount ${attempt}`);
      poll.answer();
    }
    await until(
      () => client.status.state === "locked",
      "the exhausted rebuild budget to lock",
    );
    const status = client.status;
    expect(status).toMatchObject({
      state: "locked",
      reason: "revoked",
      error: { code: "AUTHORIZATION_REVOKED", recovery: "reset" },
    });
    if (status.state !== "locked") throw new Error("expected a lock");
    expect(status.error.message).toContain("revocation rebuild");
    // Locked means no facade is served, ever, rather than served from the
    // last mount that worked.
    await expect(client.db.events.query().run()).rejects.toMatchObject({
      code: "AUTHORIZATION_REVOKED",
    });
    expect(workers.every((worker) => worker.terminated)).toBe(true);
    await client.close();
  });

  /**
   * After a barrier the session calls `auth()` once. A grant the server accepts
   * rebuilds it (the tests above); an `auth()` that throws, or a grant the
   * server refuses, locks it. Both are the application saying this principal
   * may no longer read, and neither is retried.
   */
  for (const refusal of ["auth() throws", "the server refuses the grant"]) {
    it(`a_revocation_whose_rebuild_fails_locks_the_session_when_${refusal.replaceAll(/\W+/gu, "_")}`, async () => {
      let authCalls = 0;
      let signedOut = false;
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
        ],
        undefined,
        {
          auth: () => {
            authCalls += 1;
            if (signedOut && refusal === "auth() throws")
              throw new Error("the application signed this user out");
            return { bearerToken: `token-${authCalls}` };
          },
          bootstrap: (index) =>
            index > 0 && refusal === "the server refuses the grant"
              ? new Response("unauthorized", { status: 401 })
              : undefined,
        },
      );
      const { client, poll, workers } = harness;
      expect(await settled(client)).toEqual({ state: "ready" });
      const rows = client.db.events.query().subscribe();
      rows.subscribe(() => undefined);
      await rows.promise;
      expect(rows.getSnapshot().status).toBe("ready");
      const write = await client.db.events.update(
        { id: 1n },
        { level: "warn" },
      );

      await until(() => poll.open > 0, "the change stream to poll");
      signedOut = true;
      poll.answer();
      await until(
        () => client.status.state === "locked",
        "the failed rebuild to lock the session",
      );
      const status = client.status;
      expect(status).toMatchObject({
        state: "locked",
        reason: "revoked",
        error: { code: "AUTHORIZATION_REVOKED", recovery: "reset" },
      });
      // The subscription publishes the lock rather than the revoked rows, the
      // write was cancelled with the documented message, and queries reject.
      expect(rows.getSnapshot()).toMatchObject({
        status: "error",
        error: { code: "AUTHORIZATION_REVOKED" },
      });
      const cancelledWrite = client.mutationStatus(write.mutationId);
      expect(cancelledWrite).toMatchObject({
        state: "ambiguous",
        error: { code: "MUTATION_AMBIGUOUS" },
      });
      if (cancelledWrite?.state !== "ambiguous")
        throw new Error("expected an ambiguous write");
      expect(cancelledWrite.error.message).toContain(
        "an authenticated revocation barrier retired this mutation's grant",
      );
      await expect(client.db.events.query().run()).rejects.toMatchObject({
        code: "AUTHORIZATION_REVOKED",
      });
      await expect(client.refresh()).rejects.toMatchObject({
        code: "AUTHORIZATION_REVOKED",
      });
      // Nothing retries: one more bootstrap or auth() call is a loop.
      const bootstraps = harness.bootstraps;
      const calls = authCalls;
      for (let turn = 0; turn < 20; turn += 1)
        await new Promise((resolve) => setTimeout(resolve, 1));
      expect(harness.bootstraps).toBe(bootstraps);
      expect(authCalls).toBe(calls);
      expect(client.status.state).toBe("locked");
      expect(workers.every((worker) => worker.terminated)).toBe(true);
      await client.close();
    });
  }

  /**
   * A write whose POST is still on the wire when the barrier lands reaches
   * the server under the retired grant, which refuses it with 401. That
   * answer arrives after the session already cancelled the write, and must
   * not replace the cancellation with a retryable credential failure: the
   * envelope names a scope the barrier retired.
   */
  it("a_submission_refused_after_a_revocation_keeps_the_revocations_cancellation", async () => {
    let signedOut = false;
    let answerSubmission!: () => void;
    const submissionHeld = new Promise<void>((resolve) => {
      answerSubmission = resolve;
    });
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
      ],
      undefined,
      {
        auth: () => {
          if (signedOut)
            throw new Error("the application signed this user out");
          return { bearerToken: "token" };
        },
        mutation: async () => {
          await submissionHeld;
          return new Response("unauthorized", { status: 401 });
        },
      },
    );
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    const write = client.db.events.update({ id: 1n }, { level: "warn" }).then(
      () => undefined,
      (error: unknown) => error,
    );
    const mutating = () =>
      workers[0]?.requests.find((request) => request.type === "mutate");
    await until(() => mutating() !== undefined, "the optimistic delta");
    const mutationId = mutating()?.mutationId as string;
    await until(
      () => client.mutationStatus(mutationId)?.state === "submitting",
      "the write to be in flight",
    );

    await until(() => poll.open > 0, "the change stream to poll");
    signedOut = true;
    poll.answer();
    await until(
      () => client.status.state === "locked",
      "the failed rebuild to lock the session",
    );
    answerSubmission();
    expect(await write).toMatchObject({ code: "AUTH_FAILED" });
    for (let turn = 0; turn < 20; turn += 1)
      await new Promise((resolve) => setTimeout(resolve, 1));

    const status = client.mutationStatus(mutationId);
    if (status?.state !== "ambiguous")
      throw new Error(`expected an ambiguous write, got ${status?.state}`);
    expect(status.error.message).toContain(
      "an authenticated revocation barrier retired this mutation's grant",
    );
    await expect(client.retryMutation(mutationId)).rejects.toMatchObject({
      code: "MUTATION_REJECTED",
    });
    expect(client.status).toMatchObject({ state: "locked", reason: "revoked" });
    await client.close();
  });

  it("a_locked_session_is_reopened_by_reset_alone", async () => {
    let signedIn = true;
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
      {
        auth: () => {
          if (!signedIn) throw new Error("signed out");
          signedIn = false;
          return { bearerToken: "token" };
        },
      },
    );
    const { client, poll } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(() => client.status.state === "locked", "the lock");
    signedIn = true;
    await client.reset();
    expect(await settled(client)).toEqual({ state: "ready" });
    await expect(client.db.events.query().run()).resolves.toEqual([
      { id: 1n, level: "epoch-two" },
      { id: 2n, level: "epoch-two" },
    ]);
    await client.close();
  });

  /**
   * An expired grant is not a revocation: the rows were authorized when they
   * arrived. The session rebuilds from `auth()` without blanking, so a tab
   * whose application renews its grant keeps painting until the rebuilt mount
   * replaces them; one whose application cannot renew it locks and discards.
   */
  it("an_expired_grant_that_the_application_renews_rebuilds_without_blanking", async () => {
    let releaseRebuild!: () => void;
    const held = new Promise<void>((resolve) => {
      releaseRebuild = resolve;
    });
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
        {
          cursor: "cursor-q",
          marker: "epoch-two",
          change: { cursor: "cursor-q" },
        },
      ],
      (index) => (index === 1 ? held : undefined),
    );
    const { client, poll, workers } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    const rows = client.db.events.query().subscribe();
    rows.subscribe(() => undefined);
    await rows.promise;

    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(
      () => workers[0]?.terminated === true,
      "the expired mount to be torn down",
    );
    // Rebuilding, and still painting the rows the expired grant authorized.
    expect(client.status.state).toBe("initializing");
    expect(rows.getSnapshot()).toMatchObject({
      status: "ready",
      result: [
        { id: 1n, level: "epoch-one" },
        { id: 2n, level: "epoch-one" },
      ],
    });
    releaseRebuild();
    await until(() => workers.length === 2, "the replacement mount");
    expect(await settled(client)).toEqual({ state: "ready" });
    await rows.promise;
    expect(rows.getSnapshot()).toMatchObject({
      status: "ready",
      result: [
        { id: 1n, level: "epoch-two" },
        { id: 2n, level: "epoch-two" },
      ],
    });
    await client.close();
  });

  it("an_expired_grant_that_the_application_cannot_renew_locks_and_discards", async () => {
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
        bootstrap: (index) =>
          index > 0 ? new Response("expired", { status: 401 }) : undefined,
      },
    );
    const { client, poll } = harness;
    expect(await settled(client)).toEqual({ state: "ready" });
    const rows = client.db.events.query().subscribe();
    rows.subscribe(() => undefined);
    await rows.promise;
    expect(rows.getSnapshot().status).toBe("ready");

    await until(() => poll.open > 0, "the change stream to poll");
    poll.answer();
    await until(() => client.status.state === "locked", "the lock");
    expect(client.status).toMatchObject({
      state: "locked",
      reason: "expired",
      error: { code: "AUTHORIZATION_EXPIRED", recovery: "reset" },
    });
    expect(rows.getSnapshot()).toMatchObject({
      status: "error",
      error: { code: "AUTHORIZATION_EXPIRED" },
    });
    await expect(client.db.events.query().run()).rejects.toMatchObject({
      code: "AUTHORIZATION_EXPIRED",
    });
    expect(harness.bootstraps).toBe(2);
    await client.close();
  });

  it("carries the grant verification key into the change contract, and nothing when there is none", async () => {
    const configured = makeClient(plan(), undefined, undefined, undefined, {
      grantVerificationKey: {
        keyId: "revocation-key-1",
        publicKey: "A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg",
      },
    });
    expect(await settled(configured.client)).toEqual({ state: "ready" });
    const open = configured.worker.requests.find(
      (request) => request.type === "open",
    );
    const contract = open?.changeStream as
      | { revocationBarrierKey?: { keyId: string; publicKey: Uint8Array } }
      | undefined;
    expect(contract?.revocationBarrierKey?.keyId).toBe("revocation-key-1");
    expect(contract?.revocationBarrierKey?.publicKey).toEqual(
      new Uint8Array(
        Buffer.from(
          "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8",
          "hex",
        ),
      ),
    );
    await configured.client.close();

    // Unconfigured, the mount is handed no key at all, so `ChangeStreamState`
    // refuses every barrier it is sent rather than trusting one.
    const bare = makeClient();
    expect(await settled(bare.client)).toEqual({ state: "ready" });
    const bareOpen = bare.worker.requests.find(
      (request) => request.type === "open",
    );
    expect(
      (bareOpen?.changeStream as Record<string, unknown> | undefined)
        ?.revocationBarrierKey,
    ).toBeUndefined();
    await bare.client.close();
  });

  it("negotiates the barrier body on the wire only when it can verify one", async () => {
    // The server sends a barrier only to a poll that asks for one, and asking
    // is honest only with a key to authenticate it, so the flag follows the
    // key.
    const flagOf = async (
      extraOptions: Record<string, unknown>,
    ): Promise<boolean> => {
      const polls = releasableChangePolls(0);
      const { client } = makeClient(
        plan(),
        polls.changeFetch,
        undefined,
        undefined,
        extraOptions,
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      for (
        let attempt = 0;
        polls.bodies.length === 0 && attempt < 200;
        attempt += 1
      )
        await new Promise((resolve) => setTimeout(resolve, 5));
      const body = polls.bodies[0];
      if (!body) throw new Error("no change poll was sent");
      const asked = fromBinary(
        change.ChangeRequestSchema,
        body,
      ).acceptRevocationBarrier;
      await client.close();
      return asked;
    };

    expect(
      await flagOf({
        grantVerificationKey: {
          keyId: "revocation-key-1",
          publicKey: "A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg",
        },
      }),
    ).toBe(true);
    expect(await flagOf({})).toBe(false);
  });

  it("refuses a malformed grant verification key rather than ignoring it", () => {
    const key32 = "A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg";
    for (const key of [
      { keyId: "", publicKey: key32 },
      // 31 bytes.
      { keyId: "k", publicKey: "A".repeat(42) },
      // Padded, and the standard alphabet: not what `calque serve` reads.
      { keyId: "k", publicKey: `${key32}=` },
      { keyId: "k", publicKey: "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg" },
      // The last character's two unused bits are set.
      { keyId: "k", publicKey: `${key32.slice(0, 42)}h` },
      // A hex key under another field name.
      { keyId: "k", publicKeyHex: "00".repeat(32) },
      { keyId: "k" },
      "not-an-object",
    ]) {
      expect(() =>
        makeClient(plan(), undefined, undefined, undefined, {
          grantVerificationKey: key,
        }),
      ).toThrow(TypeError);
    }
  });
});
