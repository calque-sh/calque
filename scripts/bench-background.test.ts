import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";

import {
  type BackgroundQos,
  type ChildReport,
  CLIENTS_PER_PROCESS,
  histogramQuantile,
  joinReports,
  lastFramePayload,
  latency,
  observerIndices,
  onOrigin,
  placed,
  processShares,
  renderedPlan,
  schedulingPriorities,
} from "./bench-background.ts";

function frame(payload: readonly number[]): Uint8Array {
  const bytes = new Uint8Array(4 + payload.length);
  new DataView(bytes.buffer).setUint32(0, payload.length, false);
  bytes.set(payload, 4);
  return bytes;
}

test("a rendered plan yields the client instance and what a follower reads", () => {
  const plan = {
    planId: "plan-1",
    authorization: { scopeToken: "scope-abc" },
    artifacts: [{ tables: ["t2", "t1"] }, { tables: ["t1", "t3"] }],
    changeStream: {
      endpoint: "http://127.0.0.1:9/_calque/v1/changes",
      throughCursorInclusive: "cursor-7",
      longPollTimeoutMs: 1_000,
      maxBatchBytes: 8_388_608,
    },
  };
  // As renderBootstrap writes it: JSON with every `<` escaped.
  const element = JSON.stringify({
    clientInstanceId: "client-1",
    plan: JSON.stringify(plan),
  }).replaceAll("<", "\\u003c");
  const html = `<link rel="preload" as="fetch" href="x"><script type="application/json" id="calque-plan">${element}</script>`;
  assert.deepEqual(renderedPlan(html), {
    clientInstanceId: "client-1",
    plan: {
      planId: "plan-1",
      scopeToken: "scope-abc",
      tableIds: ["t1", "t2", "t3"],
      endpoint: "http://127.0.0.1:9/_calque/v1/changes",
      activationCursor: "cursor-7",
      longPollTimeoutMs: 1_000,
      maxBatchBytes: 8_388_608,
    },
  });
  assert.throws(() => renderedPlan(""), /rendered no plan/u);
});

test("the last frame of a batch is found by its length prefixes alone", () => {
  const batch = new Uint8Array([
    ...frame([1, 2, 3]),
    ...frame([9]),
    ...frame([4, 5]),
  ]);
  assert.deepEqual([...lastFramePayload(batch)], [4, 5]);
  // A batch that ends inside a frame, or holds none, is refused.
  assert.throws(
    () => lastFramePayload(batch.subarray(0, batch.length - 1)),
    /whole sequence/u,
  );
  assert.throws(() => lastFramePayload(new Uint8Array(0)), /whole sequence/u);
  assert.throws(() => lastFramePayload(new Uint8Array(4)), /whole sequence/u);
});

test("a plan's change URL is asked of calque's loopback origin on the same path", () => {
  assert.equal(
    onOrigin(
      "https://127.0.0.1:8443/_calque/v1/changes?x=1",
      "http://127.0.0.1:8787/",
    ),
    "http://127.0.0.1:8787/_calque/v1/changes?x=1",
  );
});

test("observers are spread across the clients and never outnumber them", () => {
  assert.deepEqual(
    observerIndices(1000, 8),
    [0, 125, 250, 375, 500, 625, 750, 875],
  );
  assert.deepEqual(observerIndices(3, 8), [0, 1, 2]);
  assert.deepEqual(observerIndices(0, 8), []);
});

test("histogram quantiles are nearest-rank over 1 ms bins and the overflow", () => {
  const bins = [0, 2, 0, 1, 1];
  assert.equal(histogramQuantile(bins, 0, 0.5), 1);
  assert.equal(histogramQuantile(bins, 0, 0.75), 3);
  assert.equal(histogramQuantile(bins, 0, 1), 4);
  assert.equal(histogramQuantile(bins, 1, 1), Number.POSITIVE_INFINITY);
  assert.equal(histogramQuantile([0, 0], 0, 0.5), null);
});

test("a latency summary is nearest-rank, and there is none of nothing", () => {
  assert.equal(latency([]), null);
  assert.deepEqual(latency([30, 10, 20, 40]), { p50: 20, p95: 40, max: 40 });
});

test("clients are shared out in whole processes, the last taking the rest", () => {
  assert.deepEqual(processShares(1), [1]);
  assert.deepEqual(processShares(CLIENTS_PER_PROCESS), [CLIENTS_PER_PROCESS]);
  assert.deepEqual(processShares(CLIENTS_PER_PROCESS + 1), [
    CLIENTS_PER_PROCESS,
    1,
  ]);
  const shares = processShares(3_000);
  assert.equal(
    shares.reduce((sum, share) => sum + share, 0),
    3_000,
  );
  assert.ok(shares.every((share) => share <= CLIENTS_PER_PROCESS));
});

test("the children's reports join into one: counts added, the slowest times, the first child's observers", () => {
  const part = (overrides: Partial<ChildReport>): ChildReport => ({
    bootstrapped: 2,
    bootstrapFailures: { "503": 1 },
    bootstrapMs: 100,
    bootstrapLatenciesMs: [10, 20],
    allLiveMs: 50,
    requests: 10,
    refusals: { "503": 2 },
    transportErrors: 0,
    firstTransportError: null,
    batches: 20,
    bytes: 2_000,
    observers: {
      count: 0,
      transactions: 0,
      commitToArrivalMs: { p50: null, p95: null, p99: null, max: null },
      spreadMs: { p50: null, p95: null, max: null },
    },
    loadClient: {
      cpuSeconds: 1,
      wallSeconds: 10,
      eventLoopDelayMs: { p50: 10, p99: 12, max: 15 },
    },
    ...overrides,
  });
  const observed = {
    count: 8,
    transactions: 40,
    commitToArrivalMs: { p50: 5, p95: 9, p99: 11, max: 12 },
    spreadMs: { p50: 1, p95: 2, max: 3 },
  };
  const joined = joinReports(
    6,
    [
      part({ observers: observed }),
      part({
        bootstrapMs: 300,
        allLiveMs: 80,
        transportErrors: 1,
        firstTransportError: "reset",
        loadClient: {
          cpuSeconds: 2,
          wallSeconds: 11,
          eventLoopDelayMs: { p50: 11, p99: 40, max: 90 },
        },
      }),
      part({ bootstrapFailures: { "500": 1 }, bootstrapLatenciesMs: [90] }),
    ],
    { settled: true, settleMs: 7, distinctFinalCursors: 1 },
  );
  assert.equal(joined.clients, 6);
  assert.equal(joined.processes, 3);
  assert.equal(joined.bootstrapped, 6);
  assert.deepEqual(joined.bootstrapFailures, { "503": 2, "500": 1 });
  assert.equal(joined.bootstrapMs, 300);
  assert.deepEqual(joined.bootstrapLatencyMs, { p50: 20, p95: 90, max: 90 });
  assert.equal(joined.allLiveMs, 80);
  assert.equal(joined.requests, 30);
  assert.deepEqual(joined.refusals, { "503": 6 });
  assert.equal(joined.transportErrors, 1);
  assert.equal(joined.firstTransportError, "reset");
  assert.equal(joined.batches, 60);
  assert.equal(joined.bytes, 6_000);
  assert.equal(joined.settled, true);
  assert.deepEqual(joined.observers, observed);
  assert.deepEqual(joined.loadClient, {
    cpuSeconds: 4,
    wallSeconds: 11,
    eventLoopDelayMs: { p50: 11, p99: 40, max: 90 },
  });
  // One child whose clients never all got a batch leaves no all-live time.
  assert.equal(
    joinReports(4, [part({}), part({ allLiveMs: null })], {
      settled: false,
      settleMs: 60_000,
      distinctFinalCursors: 2,
    }).allLiveMs,
    null,
  );
  assert.throws(
    () =>
      joinReports(0, [], {
        settled: false,
        settleMs: 0,
        distinctFinalCursors: 0,
      }),
    /no background client reported/u,
  );
});

test(
  "a placed process runs at its clamp's priority, below an unplaced one",
  { skip: process.platform !== "darwin" && "taskpolicy is macOS's" },
  async () => {
    const priorityUnder = async (qos: BackgroundQos | undefined) => {
      const [command, ...args] = placed(qos, [
        process.execPath,
        "-e",
        "console.log('up'); setInterval(() => {}, 1_000)",
      ]);
      const child = spawn(command!, args, {
        stdio: ["ignore", "pipe", "ignore"],
      });
      try {
        await once(child.stdout, "data");
        return schedulingPriorities([child.pid!])[0]!;
      } finally {
        child.kill();
      }
    };
    const unplaced = await priorityUnder(undefined);
    const utility = await priorityUnder("utility");
    assert.ok(utility <= 20, `utility ${String(utility)}`);
    assert.ok(unplaced > 20, `unplaced ${String(unplaced)}`);
  },
);
