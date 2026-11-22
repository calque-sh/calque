import assert from "node:assert/strict";
import { test } from "node:test";
import { underLoadSummary, type UnderLoadResult } from "./bench-under-load.ts";

function sample(
  verdict: UnderLoadResult["verdict"],
  stablePaintMs: number | null,
  artifact: number,
  contentEncoding: string | null,
): UnderLoadResult {
  return {
    name: "under-load-0",
    verdict,
    invalid: [],
    stablePaintMs,
    reachedMs: stablePaintMs,
    failure: null,
    wire: { plan: 1, artifact, catchUp: 10, page: 1, total: artifact + 12 },
    objects: [{ contentEncoding, contentLength: artifact }],
    record: {},
  };
}

test("a summary under the load ranks a failed visit last and counts bytes and objects over the passed ones", () => {
  const summary = underLoadSummary([
    sample("PASS", 900, 1_000, null),
    sample("PASS", 1_100, 3_000, "zstd"),
    sample("DNC", null, 5_000, "zstd"),
  ]);
  assert.deepEqual(summary.verdicts, ["PASS", "PASS", "DNC"]);
  assert.equal(summary.stable_paint_ms.failures, 1);
  assert.equal(summary.stable_paint_ms.max, null);
  assert.deepEqual(summary.objects, { brotli: 1, zstd: 1 });
  assert.equal(summary.bytes.artifact.max, 3_000);
  assert.equal(summary.bytes.artifact.min, 1_000);
});
