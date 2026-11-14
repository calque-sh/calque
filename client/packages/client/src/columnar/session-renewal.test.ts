import { setTimeout as realDelay } from "node:timers/promises";

import { fromBinary } from "@bufbuild/protobuf";
import { mutation } from "@calque/protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  acceptedResponder,
  NO_AUTOMATIC_RESENDS,
  controllableLongPoll,
  decoder,
  makeClient,
  outcomeResponder,
  plan,
  type FakeWorker,
} from "./session.fixture.js";
import { stubBrowserFeatures } from "../browser.fixture.js";

const MINUTE = 60_000;
const LIFE_MS = 15 * MINUTE;
const TABLES = ["public.events", "public.issues"];

/** What a renewal request names beside the fields every bootstrap sends. */
interface RenewalRequest {
  readonly requiredTables: readonly string[];
  readonly residentStateProof: string;
  readonly residentTables: readonly string[];
  readonly residentCursor: string;
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/** The fixture plan, expiring `LIFE_MS` from now. */
function initialPlan(): Record<string, unknown> {
  const value = plan();
  value.expiresAt = new Date(Date.now() + LIFE_MS).toISOString();
  return value;
}

/**
 * The plan the server answers the `count`th renewal with: no artifact, the
 * same source, scope and tables, and a change stream that continues from
 * `cursor`.
 */
function renewedPlan(
  count: number,
  cursor: string,
  expiresAt = Date.now() + LIFE_MS,
): Record<string, unknown> {
  const value = plan();
  value.planId = `plan-${String(count + 1)}`;
  value.expiresAt = new Date(expiresAt).toISOString();
  (value.authorization as Record<string, unknown>).scopeToken =
    `scope-secret-${String(count + 1)}`;
  value.residentTables = TABLES;
  value.residentCursor = cursor;
  value.activationCursor = cursor;
  value.nextResidentStateProof = `proof-${String(count + 1)}`;
  value.artifacts = [];
  Object.assign(value.changeStream as Record<string, unknown>, {
    fromCursorExclusive: cursor,
    throughCursorInclusive: cursor,
  });
  return value;
}

type RenewalAnswer = (
  request: RenewalRequest,
  count: number,
) => Response | Promise<Response>;

const renewing: RenewalAnswer = (request, count) =>
  json(renewedPlan(count, request.residentCursor));

/**
 * A client whose plan expires in 15 minutes and whose bootstrap endpoint
 * answers each renewal through `answer`. Each renewal's body is kept, and so
 * is the scope token of each write.
 */
function makeRenewingClient(
  answer: RenewalAnswer = renewing,
  mutationFetch: typeof fetch = acceptedResponder,
  options: Record<string, unknown> = {},
) {
  const renewals: RenewalRequest[] = [];
  const writes: string[] = [];
  const poll = controllableLongPoll();
  const made = makeClient(
    initialPlan(),
    poll.fetcher,
    (input, init) => {
      const request = fromBinary(
        mutation.MutationBatchRequestSchema,
        new Uint8Array(init?.body as ArrayBuffer),
      );
      writes.push(decoder.decode(request.authorizationScopeToken));
      return mutationFetch(input, init);
    },
    (inner) => async (input, init) => {
      if (String(input).endsWith("/v1/bootstrap")) {
        const body = JSON.parse(String(init?.body)) as Partial<RenewalRequest>;
        if (body.residentStateProof !== undefined) {
          renewals.push(body as RenewalRequest);
          return answer(body as RenewalRequest, renewals.length);
        }
      }
      return inner(input, init);
    },
    options,
  );
  const requests = (suffix: string): number =>
    made.fetcher.mock.calls.filter(([input]) => String(input).endsWith(suffix))
      .length;
  return { ...made, poll, renewals, writes, requests };
}

/**
 * Lets the timers due by now, the microtasks and the real macrotasks run
 * until `condition` holds, moving the clock a millisecond a turn for 500
 * turns. Past those it holds the clock and waits up to 2 s of real time for
 * work the fake clock does not drive, such as the first import of the live
 * module on a loaded host.
 */
async function until(condition: () => boolean, what: string): Promise<void> {
  for (let turn = 0; turn < 500; turn += 1) {
    if (condition()) return;
    await vi.advanceTimersByTimeAsync(1);
    await new Promise((resolve) => setImmediate(resolve));
  }
  const deadline = performance.now() + 2_000;
  while (performance.now() < deadline) {
    if (condition()) return;
    await vi.advanceTimersByTimeAsync(0);
    await realDelay(1);
  }
  if (condition()) return;
  throw new Error(`timed out waiting for ${what}`);
}

/** Runs real macrotasks for a while, for a test that expects nothing. */
async function quiet(): Promise<void> {
  for (let turn = 0; turn < 20; turn += 1)
    await new Promise((resolve) => setImmediate(resolve));
}

function marks(name: string): number {
  return performance.getEntriesByName(`calque:${name}`).length;
}

/** The plan id and scope token of each change request, in order. */
function polled(
  poll: ReturnType<typeof controllableLongPoll>,
): (readonly [string, string])[] {
  return poll.requests.map(
    (request) =>
      [
        request.planId,
        decoder.decode(request.authorizationScopeToken),
      ] as const,
  );
}

/** The plan id each `change` request to the worker named, if any. */
function appliedUnder(worker: FakeWorker): unknown[] {
  return worker.requests
    .filter((request) => request.type === "change")
    .map((request) => request.planId);
}

/**
 * Closes the client while the clock moves, since an aborted poll reports its
 * abort on a timer.
 */
async function close(made: ReturnType<typeof makeRenewingClient>) {
  let closed = false;
  const closing = made.client.close().then(() => {
    closed = true;
  });
  await until(() => closed, "the close");
  await closing;
}

async function ready(made: ReturnType<typeof makeRenewingClient>) {
  await until(() => made.client.status.state === "ready", "the activation");
  await until(() => made.poll.open === 1, "the first poll");
}

describe("a session renews its plan before it expires", () => {
  beforeEach(() => {
    stubBrowserFeatures();
    vi.useFakeTimers({
      toFake: ["setTimeout", "clearTimeout", "Date"],
      now: Date.UTC(2026, 10, 7, 12),
    });
    performance.clearMarks();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("swaps the plan in place, and every later request and write carries it", async () => {
    const made = makeRenewingClient();
    const { client, worker, workerFactory, poll, renewals, writes } = made;
    const statuses: string[] = [];
    client.subscribe(() => statuses.push(client.status.state));
    await ready(made);

    // Nothing goes out before two thirds of the plan's life.
    await vi.advanceTimersByTimeAsync(9 * MINUTE);
    expect(renewals).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(MINUTE);
    await until(() => marks("plan:renewed") === 1, "the renewal");
    expect(renewals).toEqual([
      expect.objectContaining({
        requiredTables: TABLES,
        residentStateProof: "proof-1",
        residentTables: TABLES,
        residentCursor: "cursor-p",
      }),
    ]);

    // The poll in flight keeps the plan it was sent under; the next one, and
    // the batches that answer it, carry the renewed plan.
    worker.changeOutcome = { cursor: "cursor-q" };
    poll.answer();
    await until(() => poll.requests.length === 2, "the next poll");
    poll.answer();
    await until(() => poll.requests.length === 3, "a third poll");
    expect(polled(poll)).toEqual([
      ["plan-1", "scope-secret"],
      ["plan-2", "scope-secret-2"],
      ["plan-2", "scope-secret-2"],
    ]);
    expect(appliedUnder(worker)).toEqual([undefined, "plan-2"]);

    await client.db.events.update({ id: 1n }, { level: "warn" });
    expect(writes).toEqual(["scope-secret-2"]);

    // The next renewal presents the renewed plan's proof and the cursor the
    // mount has reached since.
    await vi.advanceTimersByTimeAsync(10 * MINUTE);
    await until(() => marks("plan:renewed") === 2, "the second renewal");
    expect(renewals[1]).toMatchObject({
      residentStateProof: "proof-2",
      residentCursor: "cursor-q",
    });
    expect(statuses).toEqual(["ready"]);
    expect(client.status).toEqual({ state: "ready" });
    expect(workerFactory).toHaveBeenCalledTimes(1);
    expect(made.requests("/artifact-1")).toBe(1);
    await close(made);
  });

  it("keeps a pending write across a renewal, and its retry carries the renewed token", async () => {
    let dropTheResponse = true;
    const replaying = outcomeResponder(
      mutation.MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
    );
    const made = makeRenewingClient(
      renewing,
      async (input, init) => {
        if (dropTheResponse) {
          dropTheResponse = false;
          throw new TypeError("fetch failed");
        }
        return replaying(input, init);
      },
      NO_AUTOMATIC_RESENDS,
    );
    const { client, worker, writes } = made;
    await ready(made);
    await expect(
      client.db.events.update({ id: 1n }, { level: "warn" }),
    ).rejects.toMatchObject({ code: "MUTATION_AMBIGUOUS" });
    const mutationId = worker.requests.find(
      (request) => request.type === "mutate",
    )?.mutationId as string;

    await vi.advanceTimersByTimeAsync(10 * MINUTE);
    await until(() => marks("plan:renewed") === 1, "the renewal");
    expect(client.mutationStatus(mutationId)?.state).toBe("ambiguous");
    expect(worker.pendingIds).toEqual([mutationId]);

    await expect(client.retryMutation(mutationId)).resolves.toMatchObject({
      mutationId,
      state: "awaiting_canonical_echo",
    });
    expect(writes).toEqual(["scope-secret", "scope-secret-2"]);
    expect(worker.pendingIds).toEqual([mutationId]);
    await close(made);
  });

  it("asks again after a network failure, and stops before the plan's last seconds", async () => {
    const made = makeRenewingClient(() => {
      throw new TypeError("Failed to fetch");
    });
    const { renewals, poll } = made;
    await ready(made);
    await vi.advanceTimersByTimeAsync(10 * MINUTE);
    await until(() => renewals.length === 1, "the first renewal");
    // One, five, fifteen and then every thirty seconds, while the answer
    // could still arrive before the plan's last five.
    for (const [delayMs, count] of [
      [1_000, 2],
      [5_000, 3],
      [15_000, 4],
      [30_000, 5],
    ] as const) {
      await vi.advanceTimersByTimeAsync(delayMs);
      await until(() => renewals.length === count, `renewal ${count}`);
    }
    await vi.advanceTimersByTimeAsync(5 * MINUTE);
    await quiet();
    const sent = renewals.length;
    expect(sent).toBeLessThanOrEqual(14);
    await vi.advanceTimersByTimeAsync(MINUTE);
    await quiet();
    expect(renewals).toHaveLength(sent);
    expect(marks("plan:renewed")).toBe(0);
    expect(polled(poll)).toEqual([["plan-1", "scope-secret"]]);
    await close(made);
  });

  it("takes a retry that succeeds", async () => {
    const made = makeRenewingClient((request, count) => {
      if (count === 1) throw new TypeError("Failed to fetch");
      return renewing(request, count - 1);
    });
    await ready(made);
    await vi.advanceTimersByTimeAsync(10 * MINUTE);
    await until(() => made.renewals.length === 1, "the first renewal");
    await vi.advanceTimersByTimeAsync(1_000);
    await until(() => marks("plan:renewed") === 1, "the retried renewal");
    made.poll.answer();
    await until(() => made.poll.requests.length === 2, "the next poll");
    expect(polled(made.poll)[1]).toEqual(["plan-2", "scope-secret-2"]);
    await close(made);
  });

  it.each([
    [400, "REQUEST_INVALID", false],
    [409, "RESNAPSHOT_REQUIRED", true],
  ] as const)(
    "does not ask again once the server refused a renewal with %i, and leaves the plan to expire",
    async (status, code, resnapshotRequired) => {
      const made = makeRenewingClient(() =>
        json(
          { code, message: "bootstrap refused", resnapshotRequired },
          status,
        ),
      );
      await ready(made);
      await vi.advanceTimersByTimeAsync(10 * MINUTE);
      await until(() => marks("plan:renewal-refused") === 1, "the refusal");
      await vi.advanceTimersByTimeAsync(4 * MINUTE);
      await quiet();
      expect(made.renewals).toHaveLength(1);
      expect(made.client.status).toEqual({ state: "ready" });
      made.poll.answer();
      await until(() => made.poll.requests.length === 2, "the next poll");
      expect(polled(made.poll)[1]).toEqual(["plan-1", "scope-secret"]);
      await close(made);
    },
  );

  it("asks again when auth() fails, since the application may answer next time", async () => {
    let calls = 0;
    const auth = vi.fn(() => {
      calls += 1;
      if (calls === 2) throw new Error("the grant endpoint is down");
      return { bearerToken: "token" };
    });
    const made = makeRenewingClient(renewing, acceptedResponder, { auth });
    await ready(made);
    await vi.advanceTimersByTimeAsync(10 * MINUTE);
    await until(() => auth.mock.calls.length === 2, "the failed auth()");
    await quiet();
    expect(made.renewals).toHaveLength(0);
    expect(marks("plan:renewal-refused")).toBe(0);
    await vi.advanceTimersByTimeAsync(1_000);
    await until(() => marks("plan:renewed") === 1, "the retried renewal");
    expect(made.renewals).toHaveLength(1);
    expect(made.client.status).toEqual({ state: "ready" });
    await close(made);
  });

  it("refuses a renewal plan that does not continue the mount", async () => {
    const made = makeRenewingClient((request, count) =>
      json(renewedPlan(count, `${request.residentCursor}-elsewhere`)),
    );
    await ready(made);
    await vi.advanceTimersByTimeAsync(10 * MINUTE);
    await until(() => marks("plan:renewal-refused") === 1, "the refusal");
    expect(marks("plan:renewed")).toBe(0);
    made.poll.answer();
    await until(() => made.poll.requests.length === 2, "the next poll");
    expect(polled(made.poll)[1]).toEqual(["plan-1", "scope-secret"]);
    expect(made.workerFactory).toHaveBeenCalledTimes(1);
    await close(made);
  });

  it("keeps the plan when auth() hands back a grant that ends no later, and asks again", async () => {
    const expiry = Date.now() + LIFE_MS;
    const made = makeRenewingClient((request, count) =>
      json(
        count === 1
          ? renewedPlan(count, request.residentCursor, expiry)
          : renewedPlan(count - 1, request.residentCursor),
      ),
    );
    await ready(made);
    await vi.advanceTimersByTimeAsync(10 * MINUTE);
    await until(() => made.renewals.length === 1, "the first renewal");
    await quiet();
    expect(marks("plan:renewed")).toBe(0);
    await vi.advanceTimersByTimeAsync(1_000);
    await until(() => marks("plan:renewed") === 1, "the second renewal");
    made.poll.answer();
    await until(() => made.poll.requests.length === 2, "the next poll");
    expect(polled(made.poll)[1]).toEqual(["plan-2", "scope-secret-2"]);
    await close(made);
  });

  it("sends no renewal once the client closed", async () => {
    const made = makeRenewingClient();
    await ready(made);
    await close(made);
    await vi.advanceTimersByTimeAsync(LIFE_MS);
    await quiet();
    expect(made.renewals).toHaveLength(0);
  });
});
