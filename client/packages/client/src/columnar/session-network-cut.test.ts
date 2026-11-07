import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { type SyncClient } from "../client.js";
import { SyncError } from "../error.js";
import {
  type Collections,
  ARTIFACT_BYTES,
  plan,
  byteStream,
  until,
  CHUNK_BYTES,
  LONG_ARTIFACT_BYTES,
  longPlan,
  patternBytes,
  stubThreads,
  PACED_BYTES,
  makeAttemptedClient,
  makeRebuildableClient,
  settled,
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

  describe("an artifact download the network cut", () => {
    // A request the network cut reaches the page as a `fetch` or body read
    // that rejects with a TypeError, as when a proxy restarts or Chrome
    // reports `net::ERR_NETWORK_CHANGED`. The download asks again on the
    // bootstrap's schedule, against the same plan while that plan is valid.

    const artifactHeaders = {
      "content-type": "application/vnd.calque.direct-columnar.v4",
      "content-length": String(ARTIFACT_BYTES),
    };

    /** Records the delay of every timer set from here on, and still runs it. */
    function recordDelays(): number[] {
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
      return delays;
    }

    /**
     * The status once the client left `initializing`, or after about two
     * seconds, so a retry the client should not have made shows as an
     * assertion on what it reports rather than as the test's timeout.
     */
    async function concluded(
      client: SyncClient<Collections>,
    ): Promise<SyncClient<Collections>["status"]> {
      for (
        let turn = 0;
        client.status.state === "initializing" && turn < 400;
        turn += 1
      )
        await new Promise((resolve) => setTimeout(resolve, 5));
      return client.status;
    }

    /** A bootstrap answer carrying `value` as the plan. */
    function planResponse(value: Record<string, unknown>): Response {
      return new Response(JSON.stringify(value), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    /**
     * The marks the session left, since the last `performance.clearMarks()`,
     * for a download it asked for again or a plan it went back for.
     */
    function retryMarks(): string[] {
      return performance
        .getEntriesByType("mark")
        .map(({ name }) => name)
        .filter((name) => /^calque:artifact:(?:retry:\d+|replan)$/u.test(name));
    }

    it("asks again for an artifact request that got no answer, against the same plan", async () => {
      performance.clearMarks();
      let cut = false;
      const { client, workers, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          if (String(input).endsWith("/artifact-1") && !cut) {
            cut = true;
            throw new TypeError("Failed to fetch");
          }
          return inner(input, init);
        },
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      // One grant, one plan, one engine worker: only the download repeated.
      expect(auth).toHaveBeenCalledTimes(1);
      expect(requests("/v1/bootstrap")).toBe(1);
      expect(requests("/artifact-1")).toBe(2);
      expect(workers).toHaveLength(1);
      expect(await client.db.events.query().run()).toEqual([
        { id: 1n, level: "error" },
      ]);
      // What lets a harness label the sample: its phases mix two requests.
      expect(retryMarks()).toEqual(["calque:artifact:retry:1"]);
      await client.close();
    });

    it("resumes a body the network cut mid-read at the first chunk it had not handed over", async () => {
      stubThreads(8);
      const body = patternBytes(LONG_ARTIFACT_BYTES);
      let responses = 0;
      const { client, workers, hashers, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          if (!String(input).endsWith("/artifact-1")) return inner(input, init);
          responses += 1;
          // The first response ends a mebibyte into the second chunk.
          const cutAt = responses === 1 ? CHUNK_BYTES + PACED_BYTES : undefined;
          let given = 0;
          const stream = new ReadableStream({
            type: "bytes",
            pull(controller) {
              if (cutAt !== undefined && given >= cutAt) {
                controller.error(new TypeError("network error"));
                return;
              }
              if (given >= body.byteLength) {
                controller.close();
                controller.byobRequest?.respond(0);
                return;
              }
              const next = body.slice(given, given + PACED_BYTES);
              given += next.byteLength;
              controller.enqueue(next);
            },
          });
          return new Response(stream, {
            status: 200,
            headers: {
              ...artifactHeaders,
              "content-length": String(body.byteLength),
            },
          });
        },
        longPlan(LONG_ARTIFACT_BYTES),
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      expect(requests("/v1/bootstrap")).toBe(1);
      expect(requests("/artifact-1")).toBe(2);
      expect(workers).toHaveLength(1);
      // Chunk 0 reached a hasher once, from the first response; the second
      // response's copy of it was read and dropped.
      expect(
        hashers.flatMap(({ chunks }) => chunks).sort((a, b) => a - b),
      ).toEqual([0, 1, 2]);
      const received = workers[0]?.received ?? [];
      const joined = new Uint8Array(LONG_ARTIFACT_BYTES);
      let offset = 0;
      for (const bytes of received) {
        joined.set(bytes, offset);
        offset += bytes.byteLength;
      }
      expect(offset).toBe(LONG_ARTIFACT_BYTES);
      expect(Buffer.compare(joined, body)).toBe(0);
      expect(
        workers[0]?.requests.filter(({ type }) => type === "artifact-end"),
      ).toEqual([expect.objectContaining({ chunks: 3 })]);
      await client.close();
    });

    it("reports the download's own error once the schedule is spent", async () => {
      performance.clearMarks();
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      try {
        const { client, auth, requests } = makeAttemptedClient(
          (inner) => async (input, init) => {
            if (String(input).endsWith("/artifact-1"))
              throw new TypeError("Failed to fetch");
            return inner(input, init);
          },
        );
        const turns = async (): Promise<void> => {
          for (let turn = 0; turn < 20; turn += 1)
            await new Promise((resolve) => setImmediate(resolve));
        };
        for (
          let turn = 0;
          requests("/artifact-1") === 0 && turn < 200;
          turn += 1
        )
          await new Promise((resolve) => setImmediate(resolve));
        expect(requests("/artifact-1")).toBe(1);
        // The six delays add up to 9,750 ms. Short of that the download is
        // still being asked for.
        await vi.advanceTimersByTimeAsync(9_749);
        await turns();
        expect(client.status).toEqual({ state: "initializing" });
        expect(requests("/artifact-1")).toBe(6);
        await vi.advanceTimersByTimeAsync(1);
        await turns();
        expect(client.status).toMatchObject({
          state: "error",
          error: {
            code: "NETWORK_ERROR",
            scope: "sync",
            recovery: "retry",
            message: "artifact download failed",
          },
        });
        expect(requests("/artifact-1")).toBe(7);
        expect(requests("/v1/bootstrap")).toBe(1);
        expect(auth).toHaveBeenCalledTimes(1);
        expect(retryMarks()).toEqual(
          [1, 2, 3, 4, 5, 6].map((retry) => `calque:artifact:retry:${retry}`),
        );
        await client.close();
      } finally {
        vi.useRealTimers();
      }
    });

    it("never asks again for a download answered with another status, an invalid one, a cancelled one, or another scope's network error", async () => {
      const cases: readonly (readonly [
        string,
        () => Response,
        Record<string, unknown>,
      ])[] = [
        [
          "403",
          () => new Response("no", { status: 403 }),
          { code: "AUTH_FAILED" },
        ],
        [
          "500",
          () => new Response("broken", { status: 500 }),
          { code: "SERVER_ERROR" },
        ],
        [
          "a Content-Length the plan does not name",
          () =>
            new Response(byteStream(new Uint8Array(ARTIFACT_BYTES)), {
              status: 200,
              headers: {
                ...artifactHeaders,
                "content-length": String(ARTIFACT_BYTES + 1),
              },
            }),
          { code: "INVALID_RESPONSE" },
        ],
        [
          "a cancelled request",
          () => {
            throw new DOMException("the request was aborted", "AbortError");
          },
          { code: "CANCELLED" },
        ],
        [
          "a network error from another scope",
          () => {
            throw new SyncError(
              "NETWORK_ERROR",
              "auth",
              "retry",
              "the grant service could not be reached",
            );
          },
          { code: "NETWORK_ERROR", scope: "auth" },
        ],
      ];
      for (const [name, answer, error] of cases) {
        performance.clearMarks();
        const { client, requests } = makeAttemptedClient(
          (inner) => async (input, init) =>
            String(input).endsWith("/artifact-1")
              ? answer()
              : inner(input, init),
        );
        expect(await concluded(client), name).toMatchObject({
          state: "error",
          error,
        });
        expect(requests("/artifact-1"), name).toBe(1);
        expect(requests("/v1/bootstrap"), name).toBe(1);
        expect(retryMarks(), name).toEqual([]);
        await client.close();
      }
    });

    it("ends the load at once when the client closes during a delay", async () => {
      performance.clearMarks();
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      try {
        const { client, workers, hashers, auth, requests } =
          makeAttemptedClient((inner) => async (input, init) => {
            if (String(input).endsWith("/artifact-1"))
              throw new TypeError("Failed to fetch");
            return inner(input, init);
          });
        for (let turn = 0; retryMarks().length === 0 && turn < 200; turn += 1)
          await new Promise((resolve) => setImmediate(resolve));
        expect(retryMarks()).toEqual(["calque:artifact:retry:1"]);
        expect(requests("/artifact-1")).toBe(1);
        // The delay is the only timer the load has set.
        expect(vi.getTimerCount()).toBe(1);
        await client.close();
        // Its engine worker is gone once close() resolves, not when the
        // delay would have ended, and nothing is left scheduled.
        expect(workers.map(({ terminated }) => terminated)).toEqual([true]);
        expect(hashers.length).toBeGreaterThan(0);
        expect(hashers.every(({ terminated }) => terminated)).toBe(true);
        expect(vi.getTimerCount()).toBe(0);
        await vi.advanceTimersByTimeAsync(10_000);
        expect(requests("/artifact-1")).toBe(1);
        expect(requests("/v1/bootstrap")).toBe(1);
        expect(auth).toHaveBeenCalledTimes(1);
        expect(client.status).toEqual({ state: "closed" });
      } finally {
        vi.useRealTimers();
      }
    });

    it("reports a refusal that arrives during a delay at once, without asking again", async () => {
      performance.clearMarks();
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
      try {
        const { client, workers, hashers, requests } = makeAttemptedClient(
          (inner) => async (input, init) => {
            if (String(input).endsWith("/artifact-1"))
              throw new TypeError("Failed to fetch");
            return inner(input, init);
          },
        );
        for (let turn = 0; retryMarks().length === 0 && turn < 200; turn += 1)
          await new Promise((resolve) => setImmediate(resolve));
        expect(retryMarks()).toEqual(["calque:artifact:retry:1"]);
        hashers[0]?.crash("hasher crashed");
        // No timer runs from here: the delay is not what ends the wait.
        for (
          let turn = 0;
          client.status.state === "initializing" && turn < 200;
          turn += 1
        )
          await new Promise((resolve) => setImmediate(resolve));
        expect(client.status).toMatchObject({
          state: "error",
          error: { message: "hasher crashed" },
        });
        expect(requests("/artifact-1")).toBe(1);
        expect(workers.map(({ terminated }) => terminated)).toEqual([true]);
        expect(vi.getTimerCount()).toBe(0);
        await client.close();
      } finally {
        vi.useRealTimers();
      }
    });

    it("ends a load a reset replaced during a delay before the reset's own starts, and loads again", async () => {
      const delays = recordDelays();
      let cutting = true;
      /** Which engine workers were terminated when the reset asked for its plan. */
      const atResetPlan: boolean[][] = [];
      const { client, workers, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          const url = String(input);
          if (url.endsWith("/artifact-1") && cutting)
            throw new TypeError("Failed to fetch");
          if (url.endsWith("/v1/bootstrap") && !cutting)
            atResetPlan.push(workers.map(({ terminated }) => terminated));
          return inner(input, init);
        },
      );
      await until(() => delays.includes(250), "the first delay");
      expect(requests("/artifact-1")).toBe(1);
      cutting = false;
      await client.reset();
      expect(client.status).toEqual({ state: "ready" });
      // The replaced load's engine worker was gone before the reset's
      // activation asked for a plan, so the two never lived at once.
      expect(atResetPlan).toEqual([[true]]);
      // Well past the first delay: the replaced load asked for nothing more.
      await new Promise((resolve) => setTimeout(resolve, 400));
      expect(requests("/artifact-1")).toBe(2);
      expect(requests("/v1/bootstrap")).toBe(2);
      expect(auth).toHaveBeenCalledTimes(2);
      expect(workers.map(({ terminated }) => terminated)).toEqual([
        true,
        false,
      ]);
      await client.close();
    });

    it("opens no delay for a download cut after the client closed", async () => {
      const delays = recordDelays();
      let cut: ((error: unknown) => void) | undefined;
      const { client, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          if (!String(input).endsWith("/artifact-1")) return inner(input, init);
          return await new Promise<Response>((_resolve, reject) => {
            cut = reject;
          });
        },
      );
      await until(() => cut !== undefined, "the artifact request");
      await client.close();
      cut?.(new TypeError("Failed to fetch"));
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(requests("/artifact-1")).toBe(1);
      expect(delays).not.toContain(250);
    });

    it("reports a refusal that crossed a cut request, and does not ask again", async () => {
      performance.clearMarks();
      const delays = recordDelays();
      const { client, hashers, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          if (!String(input).endsWith("/artifact-1")) return inner(input, init);
          // A hasher fails while the request is out, and the request then
          // fails as a cut would rather than as the abort that followed.
          const [hasher] = hashers;
          if (!hasher) throw new Error("no hasher");
          hasher.crash("hasher crashed");
          await new Promise((resolve) => setTimeout(resolve, 0));
          throw new TypeError("Failed to fetch");
        },
      );
      expect(await concluded(client)).toMatchObject({
        state: "error",
        error: { message: "hasher crashed" },
      });
      expect(requests("/artifact-1")).toBe(1);
      expect(delays).not.toContain(250);
      expect(retryMarks()).toEqual([]);
      await client.close();
    });

    it("goes back to the bootstrap when a retry would go out after the plan expired", async () => {
      performance.clearMarks();
      const delays = recordDelays();
      const start = Date.now();
      let now = start;
      vi.spyOn(Date, "now").mockImplementation(() => now);
      let bootstraps = 0;
      let downloads = 0;
      const { client, workers, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          const url = String(input);
          if (url.endsWith("/v1/bootstrap")) {
            bootstraps += 1;
            // The first plan expires a second after it was issued; the
            // second is the fixture's, good until 2099.
            return bootstraps === 1
              ? planResponse({
                  ...plan(),
                  expiresAt: new Date(start + 1_000).toISOString(),
                })
              : inner(input, init);
          }
          if (url.endsWith("/artifact-1") && (downloads += 1) === 1) {
            // Cut with 100 ms of the plan left, less than the first delay.
            now = start + 900;
            throw new TypeError("Failed to fetch");
          }
          return inner(input, init);
        },
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      // A fresh grant and plan, straight away: no retry waited against the
      // plan that was about to expire.
      expect(auth).toHaveBeenCalledTimes(2);
      expect(requests("/v1/bootstrap")).toBe(2);
      expect(requests("/artifact-1")).toBe(2);
      expect(delays).not.toContain(250);
      expect(workers.map(({ terminated }) => terminated)).toEqual([
        true,
        false,
      ]);
      expect(retryMarks()).toEqual(["calque:artifact:replan"]);
      await client.close();
    });

    it("goes back to the bootstrap once, and reports a second plan the download outlived", async () => {
      performance.clearMarks();
      const start = Date.now();
      let now = start;
      vi.spyOn(Date, "now").mockImplementation(() => now);
      let expiresAt = start;
      let bootstraps = 0;
      const { client, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          const url = String(input);
          // Past the fifth plan the fixture's own is served, so a client
          // that kept going back ends up ready rather than looping.
          if (url.endsWith("/v1/bootstrap") && (bootstraps += 1) <= 5) {
            expiresAt = now + 1_000;
            return planResponse({
              ...plan(),
              expiresAt: new Date(expiresAt).toISOString(),
            });
          }
          if (url.endsWith("/artifact-1") && bootstraps <= 5) {
            now = expiresAt - 100;
            throw new TypeError("Failed to fetch");
          }
          return inner(input, init);
        },
      );
      expect(await concluded(client)).toMatchObject({
        state: "error",
        error: { code: "NETWORK_ERROR", message: "artifact download failed" },
      });
      expect(auth).toHaveBeenCalledTimes(2);
      expect(requests("/v1/bootstrap")).toBe(2);
      expect(requests("/artifact-1")).toBe(2);
      // The mark says a plan was asked for, not that one was outlived.
      expect(retryMarks()).toEqual(["calque:artifact:replan"]);
      await client.close();
    });

    it("asks for a new plan once when the server refuses the download's capability, and loads it", async () => {
      // The refusal served no byte, and a new plan names a capability the
      // server holds.
      const cases = [
        // The request reached the server once the plan had expired.
        ["an expired capability, 404", 404, true],
        // The allowlist lives in the serving process, so a restart retires
        // every capability it issued, however long its plan had left.
        ["a server that restarted, 404", 404, false],
        ["a missing or malformed capability, 401", 401, false],
      ] as const;
      for (const [name, status, expiring] of cases) {
        performance.clearMarks();
        const start = Date.now();
        let now = start;
        const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
        let bootstraps = 0;
        let refused = false;
        const { client, workers, auth, requests } = makeAttemptedClient(
          (inner) => async (input, init) => {
            const url = String(input);
            if (url.endsWith("/v1/bootstrap") && (bootstraps += 1) === 1)
              return expiring
                ? planResponse({
                    ...plan(),
                    expiresAt: new Date(start + 1_000).toISOString(),
                  })
                : inner(input, init);
            if (url.endsWith("/artifact-1") && !refused) {
              refused = true;
              if (expiring) now = start + 1_000;
              return new Response("gone", { status });
            }
            return inner(input, init);
          },
        );
        expect(await settled(client), name).toEqual({ state: "ready" });
        expect(auth, name).toHaveBeenCalledTimes(2);
        expect(requests("/v1/bootstrap"), name).toBe(2);
        expect(requests("/artifact-1"), name).toBe(2);
        expect(
          workers.map(({ terminated }) => terminated),
          name,
        ).toEqual([true, false]);
        expect(retryMarks(), name).toEqual(["calque:artifact:replan"]);
        expect(await client.db.events.query().run(), name).toEqual([
          { id: 1n, level: "error" },
        ]);
        await client.close();
        clock.mockRestore();
      }
    });

    it("reports the new plan's refused capability as that download's own error", async () => {
      for (const [status, error] of [
        [404, { code: "SERVER_ERROR", scope: "sync", recovery: "retry" }],
        [401, { code: "AUTH_FAILED", scope: "auth", recovery: "reset" }],
      ] as const) {
        performance.clearMarks();
        let plans = 0;
        const { client, workers, auth, requests } = makeAttemptedClient(
          (inner) => async (input, init) => {
            const url = String(input);
            if (url.endsWith("/v1/bootstrap")) plans += 1;
            // Past the fourth plan the fixture's own artifact is served, so a
            // client that kept going back ends up ready, and fails the
            // assertions below, rather than looping.
            return url.endsWith("/artifact-1") && plans <= 4
              ? new Response("gone", { status })
              : inner(input, init);
          },
        );
        const name = String(status);
        expect(await concluded(client), name).toMatchObject({
          state: "error",
          error: {
            ...error,
            name: "SyncError",
            message: `artifact download failed with HTTP ${status}`,
          },
        });
        // One new plan, not one per refusal.
        expect(auth, name).toHaveBeenCalledTimes(2);
        expect(requests("/v1/bootstrap"), name).toBe(2);
        expect(requests("/artifact-1"), name).toBe(2);
        expect(
          workers.map(({ terminated }) => terminated),
          name,
        ).toEqual([true, true]);
        expect(retryMarks(), name).toEqual(["calque:artifact:replan"]);
        await client.close();
      }
    });

    it("reports the bootstrap's refusal, and no row, when the grant behind a refused capability was revoked", async () => {
      // A revoked grant's artifact URLs answer 404 and its bootstrap 401. The
      // new plan is where the revocation is enforced, so the load must end at
      // that refusal.
      let bootstraps = 0;
      const { client, workers, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          const url = String(input);
          if (url.endsWith("/v1/bootstrap") && (bootstraps += 1) > 1)
            return new Response("revoked", { status: 401 });
          if (url.endsWith("/artifact-1"))
            return new Response("gone", { status: 404 });
          return inner(input, init);
        },
      );
      expect(await concluded(client)).toMatchObject({
        state: "error",
        error: {
          code: "AUTH_FAILED",
          scope: "auth",
          message: "bootstrap failed with HTTP 401",
        },
      });
      expect(auth).toHaveBeenCalledTimes(2);
      expect(requests("/v1/bootstrap")).toBe(2);
      expect(requests("/artifact-1")).toBe(1);
      // The one engine worker was sent no byte and is gone, and the refused
      // bootstrap spawned none.
      expect(workers.map(({ terminated }) => terminated)).toEqual([true]);
      expect(workers[0]?.received).toEqual([]);
      await expect(client.db.events.query().run()).rejects.toMatchObject({
        code: "AUTH_FAILED",
      });
      await client.close();
    });

    it("reports a refusal that crossed a refused capability, and asks for no new plan", async () => {
      performance.clearMarks();
      const { client, hashers, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          if (!String(input).endsWith("/artifact-1")) return inner(input, init);
          const [hasher] = hashers;
          if (!hasher) throw new Error("no hasher");
          hasher.crash("hasher crashed");
          await new Promise((resolve) => setTimeout(resolve, 0));
          return new Response("gone", { status: 404 });
        },
      );
      expect(await concluded(client)).toMatchObject({
        state: "error",
        error: { message: "hasher crashed" },
      });
      expect(auth).toHaveBeenCalledTimes(1);
      expect(requests("/v1/bootstrap")).toBe(1);
      expect(retryMarks()).toEqual([]);
      await client.close();
    });

    it("starts a new plan's download from its first chunk, in a new engine worker, after chunks were handed", async () => {
      stubThreads(8);
      const body = patternBytes(LONG_ARTIFACT_BYTES);
      /** `body`, cut by the network at `cutAt` when one is given. */
      const served = (cutAt?: number, onCut?: () => void): Response => {
        let given = 0;
        const stream = new ReadableStream({
          type: "bytes",
          pull(controller) {
            if (cutAt !== undefined && given >= cutAt) {
              onCut?.();
              controller.error(new TypeError("network error"));
              return;
            }
            if (given >= body.byteLength) {
              controller.close();
              controller.byobRequest?.respond(0);
              return;
            }
            const next = body.slice(given, given + PACED_BYTES);
            given += next.byteLength;
            controller.enqueue(next);
          },
        });
        return new Response(stream, {
          status: 200,
          headers: {
            ...artifactHeaders,
            "content-length": String(body.byteLength),
          },
        });
      };
      for (const trigger of ["a 404", "the plan's expiry"] as const) {
        performance.clearMarks();
        const start = Date.now();
        let now = start;
        const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
        let bootstraps = 0;
        let downloads = 0;
        /**
         * What the first plan's load had handed over when the second was
         * asked for.
         */
        const atSecondPlan: { terminated: boolean[]; hashed: number[] }[] = [];
        const { client, workers, hashers, requests } = makeAttemptedClient(
          (inner) => async (input, init) => {
            const url = String(input);
            if (url.endsWith("/v1/bootstrap")) {
              bootstraps += 1;
              if (bootstraps === 2)
                atSecondPlan.push({
                  terminated: workers.map(({ terminated }) => terminated),
                  hashed: hashers.flatMap(({ chunks }) => chunks),
                });
              const value = longPlan(LONG_ARTIFACT_BYTES);
              // Under the expiry trigger the first plan is good for a second.
              if (bootstraps === 1 && trigger === "the plan's expiry")
                value.expiresAt = new Date(start + 1_000).toISOString();
              return planResponse(value);
            }
            if (!url.endsWith("/artifact-1")) return inner(input, init);
            downloads += 1;
            // The first response ends a mebibyte into the second chunk, with
            // 100 ms of the plan left under the expiry trigger. Under the 404
            // trigger the retry finds the capability retired.
            if (downloads === 1)
              return served(CHUNK_BYTES + PACED_BYTES, () => {
                if (trigger === "the plan's expiry") now = start + 900;
              });
            if (downloads === 2 && trigger === "a 404")
              return new Response("gone", { status: 404 });
            return served();
          },
          longPlan(LONG_ARTIFACT_BYTES),
        );
        expect(await settled(client), trigger).toEqual({ state: "ready" });
        expect(requests("/v1/bootstrap"), trigger).toBe(2);
        expect(requests("/artifact-1"), trigger).toBe(
          trigger === "a 404" ? 3 : 2,
        );
        expect(retryMarks(), trigger).toEqual(
          trigger === "a 404"
            ? ["calque:artifact:retry:1", "calque:artifact:replan"]
            : ["calque:artifact:replan"],
        );
        // The first plan's load had handed chunk 0 over, and its engine
        // worker was gone before the second plan was asked for.
        expect(atSecondPlan, trigger).toEqual([
          { terminated: [true], hashed: [0] },
        ]);
        // The second plan's worker took every chunk from the first, the
        // first one hashed again rather than carried over.
        expect(workers, trigger).toHaveLength(2);
        expect(
          workers.map(({ terminated }) => terminated),
          trigger,
        ).toEqual([true, false]);
        expect(
          hashers.flatMap(({ chunks }) => chunks).sort((a, b) => a - b),
          trigger,
        ).toEqual([0, 0, 1, 2]);
        const received = workers[1]?.received ?? [];
        const joined = new Uint8Array(LONG_ARTIFACT_BYTES);
        let offset = 0;
        for (const bytes of received) {
          joined.set(bytes, offset);
          offset += bytes.byteLength;
        }
        expect(offset, trigger).toBe(LONG_ARTIFACT_BYTES);
        expect(Buffer.compare(joined, body), trigger).toBe(0);
        expect(
          workers[1]?.requests.filter(({ type }) => type === "artifact-end"),
          trigger,
        ).toEqual([expect.objectContaining({ chunks: 3 })]);
        await client.close();
        clock.mockRestore();
      }
    });

    it("resolves a reset whose download outlived its plan once the fresh plan's activation settles", async () => {
      const start = Date.now();
      let now = start;
      vi.spyOn(Date, "now").mockImplementation(() => now);
      let resetting = false;
      let bootstraps = 0;
      let expiresAt = start;
      const { client, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          const url = String(input);
          // The reset's plan expires a second after it was issued; the plan
          // before it and the one after are the fixture's.
          if (url.endsWith("/v1/bootstrap") && resetting) {
            bootstraps += 1;
            if (bootstraps === 1) {
              expiresAt = now + 1_000;
              return planResponse({
                ...plan(),
                expiresAt: new Date(expiresAt).toISOString(),
              });
            }
          }
          if (url.endsWith("/artifact-1") && resetting && bootstraps === 1) {
            now = expiresAt - 100;
            throw new TypeError("Failed to fetch");
          }
          return inner(input, init);
        },
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      resetting = true;
      await client.reset();
      // No wait for the status: the reset resolved after the activation that
      // replaced the outlived plan's, not when that replacement began.
      expect(client.status).toEqual({ state: "ready" });
      expect(auth).toHaveBeenCalledTimes(3);
      expect(requests("/v1/bootstrap")).toBe(3);
      expect(requests("/artifact-1")).toBe(3);
      await client.close();
    });

    it("asks again for a reset's download the network cut, against the reset's plan", async () => {
      let resetting = false;
      let cut = false;
      const { client, workers, auth, requests } = makeAttemptedClient(
        (inner) => async (input, init) => {
          if (String(input).endsWith("/artifact-1") && resetting && !cut) {
            cut = true;
            throw new TypeError("Failed to fetch");
          }
          return inner(input, init);
        },
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      resetting = true;
      await client.reset();
      expect(client.status).toEqual({ state: "ready" });
      expect(auth).toHaveBeenCalledTimes(2);
      expect(requests("/v1/bootstrap")).toBe(2);
      expect(requests("/artifact-1")).toBe(3);
      // The reset's one worker took both of its requests.
      expect(workers.map(({ terminated }) => terminated)).toEqual([
        true,
        false,
      ]);
      await client.close();
    });

    it("asks again for a rebuild's download the network cut, against the rebuild's plan", async () => {
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
        undefined,
        {
          artifact: (index) => {
            // The rebuild's first request.
            if (index === 1) throw new TypeError("Failed to fetch");
          },
        },
      );
      const { client, poll, workers } = harness;
      expect(await settled(client)).toEqual({ state: "ready" });
      await until(() => poll.open > 0, "the change stream to poll");
      poll.answer();
      await until(
        () => harness.artifacts === 3,
        "the rebuild to ask for its artifact again",
      );
      expect(await settled(client)).toEqual({ state: "ready" });
      expect(harness.bootstraps).toBe(2);
      expect(harness.artifacts).toBe(3);
      expect(workers.map(({ terminated }) => terminated)).toEqual([
        true,
        false,
      ]);
      expect(await client.db.events.query().run()).toEqual([
        { id: 1n, level: "epoch-two" },
        { id: 2n, level: "epoch-two" },
      ]);
      await client.close();
    });
  });
});
