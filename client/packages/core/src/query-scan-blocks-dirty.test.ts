/**
 * The block filter against the row path and the naive oracle over a dirty
 * overlay, as `expectBlocksDecideAsRows` describes. It has a file of its own
 * so that vitest runs it beside the other overlay states.
 */
import { describe, test } from "vitest";

import { expectBlocksDecideAsRows } from "./query-scan.fixture.js";

describe("query scan kernels", () => {
  test("a block decides every predicate as the row path does, counters included, over a dirty overlay", async () => {
    await expectBlocksDecideAsRows(["dirty"]);
  }, 120_000);
});
