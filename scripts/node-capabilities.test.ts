import assert from "node:assert/strict";
import test from "node:test";

import {
  currentNodeRuntime,
  nodeCapabilityRefusals,
  requireNodeCapabilities,
  type NodeRuntime,
} from "./lib/node-capabilities.ts";

/** A runtime with every capability, to take one away from at a time. */
const capable: NodeRuntime = {
  zstdCompressSync: () => Buffer.alloc(0),
};

test("the running Node has every capability the workspace needs", () => {
  assert.deepEqual(nodeCapabilityRefusals(currentNodeRuntime()), []);
  requireNodeCapabilities();
});

test("a Node with every capability is not refused", () => {
  assert.deepEqual(nodeCapabilityRefusals(capable), []);
});

test("a Node without built-in zstd is refused, naming 22.15 and 23.8", () => {
  const refusals = nodeCapabilityRefusals({
    ...capable,
    zstdCompressSync: undefined,
  });
  assert.equal(refusals.length, 1, refusals.join("\n"));
  assert.match(
    refusals[0] ?? "",
    /^`zlib\.zstdCompressSync` is not a function: .* 22\.15 and 23\.8,/,
  );
});
