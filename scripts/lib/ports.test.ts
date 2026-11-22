import assert from "node:assert/strict";
import test from "node:test";

import { RESERVED_PORTS, reservedPort } from "./ports.ts";

test("reserved ports are distinct and under the ephemeral range", async () => {
  const ports = await Promise.all(
    Array.from({ length: 50 }, () => reservedPort()),
  );
  assert.equal(new Set(ports).size, ports.length);
  for (const port of ports) {
    assert.ok(port >= RESERVED_PORTS.first, String(port));
    assert.ok(port <= RESERVED_PORTS.last, String(port));
  }
});
