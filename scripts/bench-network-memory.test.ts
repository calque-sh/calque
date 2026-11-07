import assert from "node:assert/strict";
import test from "node:test";

import {
  networkMemoryProblem,
  parseNetstatMemory,
  SAFE_FRACTION,
} from "./bench-network-memory.ts";

const NETSTAT = `188/1617 mbufs in use:
	118 mbufs allocated to data
	25 mbufs allocated to packet headers
	45 mbufs allocated to socket names and addresses
	1429 mbufs allocated to caches
2/4064 mbuf 2KB clusters in use
0/2196 mbuf 4KB clusters in use
3/964 mbuf 16KB clusters in use
37093 KB allocated to network (2.2% in use)
825709 requests for memory denied
`;

const POOL = 262_144 * 2_048;

test("netstat -m reads as the mbufs and clusters in use and the denied requests", () => {
  assert.deepEqual(parseNetstatMemory(NETSTAT), {
    inUseBytes: 188 * 256 + 2 * 2_048 + 3 * 16_384,
    deniedRequests: 825_709,
  });
  assert.throws(
    () => parseNetstatMemory("2/4064 mbuf 2KB clusters in use\n"),
    /no mbufs in use or no denied requests/u,
  );
});

test("a denied request for network memory stops the clients", () => {
  const before = { inUseBytes: 1_000_000, deniedRequests: 825_709 };
  assert.equal(networkMemoryProblem(before, before, POOL), null);
  assert.equal(
    networkMemoryProblem(
      before,
      { inUseBytes: 1_000_000, deniedRequests: 825_712 },
      POOL,
    ),
    "the host denied 3 requests for network memory",
  );
});

test("network memory in use past the safe fraction of the pool stops the clients", () => {
  const before = { inUseBytes: 0, deniedRequests: 0 };
  const at = (inUseBytes: number) =>
    networkMemoryProblem(before, { inUseBytes, deniedRequests: 0 }, POOL);
  assert.equal(at(SAFE_FRACTION * POOL - 1), null);
  assert.equal(
    at(SAFE_FRACTION * POOL),
    "the host had 256 MiB of network memory in use, 50% of its 512 MiB pool and past the 50% the background clients may take",
  );
});
