import { describe, expect, test } from "vitest";

import {
  ORACLE_COLLECTIONS,
  buildPair,
  emptyCoverage,
  generateStream,
  renderSteps,
  runSteps,
  shrink,
  tallyCoverage,
  type Coverage,
  type CoverageKey,
} from "./overlay-oracle.fixture.js";

const SEEDS = Array.from(
  { length: 40 },
  (_unused, index) => 0x5eed_0000 + index,
);
const STEPS = 60;

describe("overlay/sqlite differential equivalence", () => {
  test.each(SEEDS)(
    "seed 0x%s agrees with the SQLite implementation",
    async (seed) => {
      const { base, reference } = await buildPair(seed);
      reference.close();
      const stream = generateStream(seed, STEPS, base);
      const divergence = await runSteps(seed, stream.steps);
      if (divergence) {
        const minimal = await shrink(seed, stream.steps, divergence.index);
        throw new Error(
          [
            `DIVERGENCE seed=0x${seed.toString(16)} step=${divergence.index}`,
            divergence.detail,
            `minimal failing prefix (${minimal.length} steps):`,
            renderSteps(minimal),
          ].join("\n"),
        );
      }
      expect(divergence).toBeNull();
    },
    60_000,
  );

  /**
   * Floors over what the generator emitted across the whole seed set, so a
   * silent drop in coverage cannot leave every seed green. The stream is
   * seeded, so the counts are stable; the floors sit near a quarter of the
   * observed values, so a weight going to zero trips while a harmless retuning
   * does not.
   */
  test("the generated stream exercises every delta kind the overlay implements", async () => {
    const coverage = emptyCoverage();
    for (const seed of SEEDS) {
      const { base, reference } = await buildPair(seed);
      reference.close();
      tallyCoverage(generateStream(seed, STEPS, base).steps, coverage);
    }

    const floors: Record<CoverageKey, number> = {
      canonicalBatches: 200,
      canonicalInsert: 200,
      canonicalUpdate: 150,
      canonicalFullTuple: 100,
      canonicalDelete: 100,
      canonicalTruncate: 5,
      acknowledgements: 50,
      optimisticMutations: 200,
      optimisticInsert: 100,
      optimisticPatch: 200,
      optimisticDelete: 50,
      patchSet: 300,
      // `increment` is the mode whose result depends on the canonical value
      // beneath it, so a partial loss of its coverage matters most.
      patchIncrement: 80,
      rejections: 50,
    };

    const shortfall = (Object.keys(floors) as CoverageKey[])
      .filter((key) => coverage[key] < floors[key])
      .map((key) => `${key}: ${coverage[key]} < ${floors[key]}`);
    expect(
      shortfall,
      `generator coverage: ${JSON.stringify(coverage)}`,
    ).toEqual([]);

    // Every counter must also be non-zero, so a kind added to `CoverageKey`
    // without a floor still cannot pass unexercised.
    const unexercised = (Object.keys(coverage) as CoverageKey[]).filter(
      (key) => coverage[key] === 0,
    );
    expect(unexercised).toEqual([]);
  }, 60_000);
});

describe("the coverage tally itself", () => {
  test("counts each delta kind exactly once", () => {
    const issueColumns =
      ORACLE_COLLECTIONS.find(({ name }) => name === "issues")?.columns ?? [];
    const key = [0];
    const primaryKey = new Uint8Array([0]);
    const coverage: Coverage = tallyCoverage(
      [
        {
          kind: "canonical",
          batch: {
            transactions: [
              {
                operations: [
                  {
                    kind: "insert",
                    collection: "issues",
                    primaryKey,
                    key,
                    values: [],
                  },
                  {
                    kind: "update",
                    collection: "issues",
                    primaryKey,
                    key,
                    changed: issueColumns.map((_unused, column) => ({
                      column,
                      value: null,
                    })),
                  },
                  { kind: "delete", collection: "issues", primaryKey, key },
                  { kind: "truncate", collection: "issues" },
                ],
                mutationIds: ["m1", "m2"],
              },
            ],
          },
        },
        {
          kind: "optimistic",
          mutation: {
            mutationId: "m3",
            actions: [
              {
                kind: "insert",
                collection: "issues",
                primaryKey,
                key,
                values: [],
              },
              { kind: "delete", collection: "issues", primaryKey, key },
              {
                kind: "patch",
                collection: "issues",
                primaryKey,
                key,
                changes: [
                  { column: 1, value: 1, mode: "set" },
                  { column: 2, value: 1, mode: "increment" },
                ],
              },
            ],
          },
        },
        { kind: "reject", mutationId: "m3" },
      ],
      emptyCoverage(),
    );
    expect(coverage).toEqual({
      canonicalBatches: 1,
      canonicalInsert: 1,
      canonicalUpdate: 1,
      canonicalFullTuple: 1,
      canonicalDelete: 1,
      canonicalTruncate: 1,
      acknowledgements: 2,
      optimisticMutations: 1,
      optimisticInsert: 1,
      optimisticPatch: 1,
      optimisticDelete: 1,
      patchSet: 1,
      patchIncrement: 1,
      rejections: 1,
    });
  });
});
