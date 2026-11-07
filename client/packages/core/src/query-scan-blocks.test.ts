/**
 * The block filter against the row path and the naive oracle over a clean and
 * a truncated overlay, as `expectBlocksDecideAsRows` describes. The dirty and
 * odd overlays have files of their own, so that vitest runs the three side by
 * side.
 */
import { describe, test } from "vitest";

import { expectBlocksDecideAsRows } from "./query-scan.fixture.js";

describe("query scan kernels", () => {
  test("a block decides every predicate as the row path does, counters included, over a clean and a truncated overlay", async () => {
    await expectBlocksDecideAsRows(["clean", "truncated"]);
  }, 120_000);
});
