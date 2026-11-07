import assert from "node:assert/strict";
import test from "node:test";

import { hasherCount } from "../../client/packages/worker/src/columnar-hashers.ts";
import {
  consumerEvidenceFault,
  type ConsumerEvidence,
  type CreatedWorkerRecord,
} from "./browser-consumer-evidence.ts";

const THREADS = 8;
const engine: CreatedWorkerRecord = {
  name: "calque-columnar",
  type: "module",
  url: "http://127.0.0.1/assets/columnar-browser-worker-entry-1.js",
};
const hasher: CreatedWorkerRecord = {
  name: "calque-hasher",
  type: "module",
  url: "http://127.0.0.1/assets/columnar-browser-hasher-entry-1.js",
};

/** Two activations, each one engine and its hashers, all terminated. */
function good(): ConsumerEvidence {
  const activation = [
    engine,
    ...Array.from({ length: hasherCount(THREADS) }, () => hasher),
  ];
  const created = [...activation, ...activation];
  return {
    result: {
      ok: true,
      states: ["ready", "ready", "closed"],
      refusal: "the schema carries no generated collections",
    },
    trace: { created, terminated: created.length },
    threads: THREADS,
  };
}

test("a load that ran the whole lifecycle and closed every worker passes", () => {
  assert.equal(consumerEvidenceFault("zstd", good(), []), undefined);
});

test("each planted failure is refused with what failed", () => {
  const evidence = good();
  const { result } = evidence;
  if (!result) throw new Error("the good evidence has a result");
  const created = evidence.trace?.created ?? [];
  const cases: [string, ConsumerEvidence, readonly string[], RegExp][] = [
    ["a page error", evidence, ["TypeError: boom"], /page errors \(zstd\)/u],
    [
      "a lifecycle that never reset",
      {
        ...evidence,
        result: { ...result, states: ["ready", "closed"] },
      },
      [],
      /lifecycle failed/u,
    ],
    [
      "no result at all",
      { ...evidence, result: undefined },
      [],
      /lifecycle failed/u,
    ],
    [
      "a collection-less schema accepted",
      { ...evidence, result: { ...result, refusal: "" } },
      [],
      /collection-less schema was not refused/u,
    ],
    [
      "a worker left running",
      { ...evidence, trace: { created, terminated: created.length - 1 } },
      [],
      /worker lifecycle drifted/u,
    ],
    [
      "a worker the client does not own",
      {
        ...evidence,
        trace: {
          created: [...created, { name: null, type: null, url: "x.js" }],
          terminated: created.length + 1,
        },
      },
      [],
      /worker lifecycle drifted/u,
    ],
    [
      "one hasher fewer than the thread count gives",
      {
        ...evidence,
        trace: {
          created: created.filter((_worker, index) => index !== 1),
          terminated: created.length - 1,
        },
      },
      [],
      /worker lifecycle drifted/u,
    ],
  ];
  for (const [name, planted, pageErrors, expected] of cases)
    assert.match(
      consumerEvidenceFault("zstd", planted, pageErrors) ?? "passed",
      expected,
      name,
    );
});
