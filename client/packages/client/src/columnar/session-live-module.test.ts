import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { createInternalColumnarClient } from "../client.js";
import { createGeneratedClientSchema } from "../client-options.js";
import {
  type Collections,
  FakeWorker,
  abortingLongPoll,
  descriptor,
  fakeFetch,
  fakeHashers,
  fencedPlan,
  until,
  validatedFetch,
} from "./session.fixture.js";
import type { ColumnarEngineWorker } from "./worker.js";
import { stubBrowserFeatures } from "../browser.fixture.js";

// The change stream's module loads while the artifact downloads. Holding it
// back lets a reset land after the artifact opened and before any change
// stream exists.
const liveModule = vi.hoisted((): PromiseWithResolvers<void> =>
  Promise.withResolvers(),
);
vi.mock("@calque/worker/columnar-live", async (importOriginal) => {
  await liveModule.promise;
  return importOriginal();
});

beforeEach(() => {
  stubBrowserFeatures();
  performance.clearMarks();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

it("a reset while the change stream's module loads starts no change stream for the load it replaced", async () => {
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
  const opens = (): number =>
    performance.getEntriesByName("calque:open:done", "mark").length;
  await until(() => opens() === 1, "the first load's artifact to open");
  const reset = client.reset();
  await until(() => opens() === 2, "the second load's artifact to open");
  liveModule.resolve();
  await until(() => polls.length === 1, "the second load's catch-up poll");
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(polls).toHaveLength(1);
  expect(workers).toHaveLength(2);
  expect(workers[0]?.terminated).toBe(true);
  expect(workers[1]?.terminated).toBe(false);
  await client.close();
  await reset;
  expect(polls[0]?.aborted).toBe(true);
});
