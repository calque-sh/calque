/**
 * The second half of the NULL predicate tests' random predicates, as
 * `expectPathsSelectAsSqlite` describes. It has a file of its own so that
 * vitest runs it beside the first half, in query-null.test.ts.
 */
import { describe, test } from "vitest";

import { expectPathsSelectAsSqlite } from "./query-null.fixture.js";

describe("predicates over NULL", () => {
  test("select the rows SQLite's WHERE selects, on every filter path, for seeds 150 to 299", async () => {
    await expectPathsSelectAsSqlite(150, 300);
  }, 120_000);
});
