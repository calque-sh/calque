import { createHash } from "node:crypto";
import { describe, expect, test } from "vitest";

import { openDirectColumnarArtifact } from "./artifact.js";
import {
  encodeWorkspace,
  WORKSPACE_CONTRACT,
  type EncodedArtifact,
} from "./artifact.fixture.js";

/**
 * `NUMERIC_BLOCK_ROWS` in the fixture encoder: one FOR block per 1,024 rows.
 */
const BLOCK_ROWS = 1_024;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

type FixtureRow = Readonly<Record<string, unknown>>;

function tables(
  events: readonly FixtureRow[],
): ReadonlyMap<string, readonly FixtureRow[]> {
  return new Map([
    ["events", events],
    ["issues", []],
    ["comments", []],
    ["issue_tags", []],
    ["users", []],
    ["projects", []],
    ["memberships", []],
  ]);
}

function events(
  ids: readonly bigint[],
  occurredAt: readonly bigint[],
): readonly FixtureRow[] {
  return ids.map((id, index) => ({
    id,
    organization_id: "organization-1",
    project_id: "project-1",
    issue_id: null,
    level: "info",
    message: "event",
    occurred_at: occurredAt[index % occurredAt.length],
    payload: { index },
  }));
}

async function open(encoded: EncodedArtifact, bytes = encoded.bytes) {
  return openDirectColumnarArtifact(Uint8Array.from(bytes).buffer, {
    byteLength: bytes.byteLength,
    digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
    identity: encoded.identity,
    schema: WORKSPACE_CONTRACT,
  });
}

/**
 * Reference timestamp text: bigint floor division, then `toISOString` on the
 * whole milliseconds, independent of the day-prefix cache and the
 * safe-integer arithmetic it checks.
 */
function referenceText(micros: bigint): string {
  let millis = micros / 1_000n;
  let remainder = micros % 1_000n;
  if (remainder < 0n) {
    millis -= 1n;
    remainder += 1_000n;
  }
  const base = new Date(Number(millis)).toISOString();
  return remainder === 0n
    ? base
    : `${base.slice(0, -1)}${remainder.toString().padStart(3, "0")}Z`;
}

/** Pads `values` to exactly one FOR block by repeating its last value. */
function block(values: readonly bigint[]): bigint[] {
  if (values.length > BLOCK_ROWS) throw new Error("block overflows");
  const last = values[values.length - 1] ?? 0n;
  return [
    ...values,
    ...Array.from({ length: BLOCK_ROWS - values.length }, () => last),
  ];
}

function sequence(start: bigint, step: bigint): bigint[] {
  return Array.from(
    { length: BLOCK_ROWS },
    (_unused, index) => start + BigInt(index) * step,
  );
}

/**
 * One block per decode path, each built so the encoder picks the width that
 * path needs: a width of at most 45 bits inside the safe range (the number
 * path), the same width far outside it, a width above 45, and full 64-bit
 * values, plus microsecond and millisecond scales.
 */
const TIMESTAMP_BLOCKS: readonly (readonly bigint[])[] = [
  // Microseconds, width 45 exactly: the widest offset the number path reads.
  sequence(1_786_000_000_000_123n, 30_000_000_123n),
  // Before 1970 with sub-millisecond remainders, still safe.
  sequence(-1_000_000_000_000_123n, -3_600_000_001n),
  // Narrow offsets on a base far above 2^53: exact only through bigint.
  sequence(1n << 60n, 1_000_001n),
  sequence(-(1n << 60n), 999_999n),
  // Offsets wider than 45 bits.
  sequence(-(1n << 52n), 1n << 43n),
  // Whole milliseconds (scale 1,000), one new day per row across five blocks:
  // more distinct days than the day-prefix cache holds before it clears.
  ...Array.from({ length: 5 }, (_unused, index) =>
    sequence(
      1_700_000_000_000_000n + BigInt(index * BLOCK_ROWS) * 86_400_000_000n,
      86_400_000_000n + 1_000n,
    ),
  ),
  block([
    0n,
    1n,
    -1n,
    999n,
    1_000n,
    1_001n,
    -999n,
    -1_000n,
    -1_001n,
    MAX_SAFE,
    MAX_SAFE - 1n,
    MAX_SAFE + 1n,
    -MAX_SAFE,
    -MAX_SAFE + 1n,
    -MAX_SAFE - 1n,
    // Quotients by 1,000 at or above 2^43, where doubles are 2^-9 apart, with
    // the largest and smallest fractions a remainder can leave.
    8_796_093_022_208_999n,
    8_796_093_022_208_001n,
    -8_796_093_022_208_999n,
    -8_796_093_022_208_001n,
    // The first instant of year 10000 and the last of year -1.
    253_402_300_800_000_000n,
    -62_167_219_200_000_001n,
    // The ends of the `Date` range.
    8_640_000_000_000_000_000n,
    -8_640_000_000_000_000_000n,
    8_639_999_999_999_999_999n,
  ]),
  // Narrow blocks whose base is a safe integer but whose range is not: the
  // offset carries past 2^53, or the scale does.
  sequence(MAX_SAFE - 1_000n, 1n),
  sequence(-MAX_SAFE - 23n, 1n),
  sequence(8_000_000_000_000_000_000n, 1_000n),
  sequence(-8_000_000_000_000_000_000n, 1_000n),
  // Width 0: one value repeated, so the block holds no packed bits.
  block([1_786_000_000_000_123n]),
  // A final block shorter than `BLOCK_ROWS`; it must stay last.
  [1_786_000_000_000_001n, 1_786_000_000_000_999n, 1_786_000_000_004_095n],
];

describe("direct-columnar exact numeric decode", () => {
  test("renders every timestamp decode path as the bigint reference text", async () => {
    const values = TIMESTAMP_BLOCKS.flat();
    const encoded = encodeWorkspace(
      tables(
        events(
          values.map((_value, index) => BigInt(index)),
          values,
        ),
      ),
    );
    const column = (await open(encoded)).table("events").column("occurred_at");
    const mismatches = values.flatMap((micros, index) => {
      const actual = column.valueAt(index);
      const expected = referenceText(micros);
      return actual === expected ? [] : [{ index, micros, actual, expected }];
    });
    expect(mismatches).toEqual([]);
  });

  test("reads packed offsets of every width exactly, as bigint and as text", async () => {
    // One block per width from 1 to 63, each holding its widest offset so the
    // encoder cannot choose a narrower one, and one raw 64-bit block. The odd
    // base keeps every block at microsecond scale, and all of it stays inside
    // the `Date` range so the text can be checked too.
    const values = [
      ...Array.from({ length: 63 }, (_unused, index) => {
        const width = BigInt(index + 1);
        const base = -(1n << (width - 1n)) - 1n;
        return block([
          base,
          ...Array.from(
            { length: 64 },
            (_u, step) =>
              base + ((BigInt(step) * 0x9e37_79b9_7f4a_7c15n) % (1n << width)),
          ),
          base + (1n << width) - 1n,
        ]);
      }).flat(),
      ...block([-8_600_000_000_000_000_000n, 8_600_000_000_000_000_000n]),
    ];
    const encoded = encodeWorkspace(
      tables(
        events(
          values.map((_value, index) => BigInt(index)),
          values,
        ),
      ),
    );
    const column = (await open(encoded)).table("events").column("occurred_at");
    const mismatches = values.flatMap((micros, index) => {
      const ordered = column.orderedAt?.(index);
      const text = column.valueAt(index);
      return ordered === micros && text === referenceText(micros)
        ? []
        : [{ index, micros, ordered, text }];
    });
    expect(mismatches).toEqual([]);
  });

  test("refuses a FOR block whose values leave the int64 domain", async () => {
    // Distinctive bases, so each block header is found by its base bytes.
    const micros = 81_985_529_216_486_895n;
    const millis = 81_985_529_216_486n;
    const values = [
      ...sequence(micros, 1n),
      ...sequence(millis * 1_000n, 1_000n),
    ];
    const encoded = encodeWorkspace(
      tables(
        events(
          values.map((_value, index) => BigInt(index)),
          values,
        ),
      ),
    );
    const withBase = (from: bigint, to: bigint): Uint8Array => {
      const bytes = Uint8Array.from(encoded.bytes);
      const view = new DataView(bytes.buffer);
      const offsets: number[] = [];
      for (let offset = 0; offset + 8 <= bytes.length; offset++)
        if (view.getBigInt64(offset, true) === from) offsets.push(offset);
      expect(offsets).toHaveLength(1);
      view.setBigInt64(offsets[0] ?? 0, to, true);
      return bytes;
    };
    const max = (1n << 63n) - 1n;
    const min = -(1n << 63n);
    // Each block is 10 bits wide, so its largest value is its base plus 1,023
    // before the scale applies.
    const opens = async (bytes: Uint8Array) =>
      (await open(encoded, bytes)).table("events").column("occurred_at");
    const top = await opens(withBase(micros, max - 1_023n));
    expect(top.orderedAt?.(1_023)).toBe(max);
    await expect(opens(withBase(micros, max - 1_022n))).rejects.toThrow(
      "FOR numeric value overflows its exact domain",
    );
    const bottom = await opens(withBase(millis, min / 1_000n));
    expect(bottom.orderedAt?.(BLOCK_ROWS)).toBe((min / 1_000n) * 1_000n);
    await expect(opens(withBase(millis, min / 1_000n - 1n))).rejects.toThrow(
      "FOR numeric value overflows its exact domain",
    );
  });
});
