import { describe, expect, test } from "vitest";

import {
  MAX_PACKED_OFFSET_BITS,
  packedBigIntAt,
  packedNumberAt,
  packedNumberReader,
  packedOffsetAt,
} from "./packed.js";

/** Deterministic bytes with every bit pattern in play, high bits included. */
function noise(length: number, seed: number): Uint8Array {
  let state = seed >>> 0;
  return Uint8Array.from({ length }, () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state >>> 24;
  });
}

/**
 * Sixteen consecutive values cover every bit alignment for any width, since
 * `index * width` modulo 8 cycles within eight indexes. All-ones and all-zeros
 * bytes put a set or clear bit on each side of every value.
 */
const SOURCES = [
  noise(512, 1),
  noise(512, 0x9e37_79b9),
  new Uint8Array(512).fill(0xff),
  new Uint8Array(512),
];
const INDEXES = 16;

/** A reader's value, or the class of error it threw. */
function outcome(read: () => number | bigint): unknown {
  try {
    return Number(read());
  } catch (error) {
    return error instanceof Error ? error.constructor.name : error;
  }
}

describe("bit-packed readers", () => {
  test("read every width up to 32 as packedBigIntAt does", () => {
    const mismatches: unknown[] = [];
    for (const bytes of SOURCES)
      for (let width = 0; width <= 32; width++)
        for (let index = 0; index < INDEXES; index++) {
          const expected = Number(packedBigIntAt(bytes, width, index));
          const actual = packedNumberAt(bytes, width, index);
          if (actual !== expected)
            mismatches.push({ width, index, actual, expected });
        }
    expect(mismatches).toEqual([]);
  });

  test("read every offset width up to 45 at any block start as packedBigIntAt does", () => {
    const mismatches: unknown[] = [];
    for (const bytes of SOURCES)
      for (const start of [0, 3, 8])
        for (let width = 0; width <= MAX_PACKED_OFFSET_BITS; width++)
          for (let index = 0; index < INDEXES; index++) {
            const expected = Number(
              packedBigIntAt(bytes.subarray(start), width, index),
            );
            const actual = packedOffsetAt(bytes, width, index, start);
            if (actual !== expected)
              mismatches.push({ start, width, index, actual, expected });
          }
    expect(mismatches).toEqual([]);
  });

  test("read every width up to 32 through a width's reader as packedNumberAt does", () => {
    const mismatches: unknown[] = [];
    for (const bytes of SOURCES)
      for (let width = 0; width <= 32; width++) {
        const read = packedNumberReader(bytes, width);
        // The last values the bytes hold, whose word read runs past the end.
        const count = width === 0 ? INDEXES : Math.floor((512 * 8) / width);
        const indexes = [
          ...Array.from({ length: INDEXES }, (_, index) => index),
          ...Array.from({ length: 8 }, (_, back) => count - 1 - back),
        ];
        for (const index of indexes) {
          const expected = packedNumberAt(bytes, width, index);
          const actual = read(index);
          if (actual !== expected)
            mismatches.push({ width, index, actual, expected });
        }
      }
    expect(mismatches).toEqual([]);
  });

  test("read the last offsets the bytes hold and refuse the next ones as packedBigIntAt does", () => {
    const mismatches: unknown[] = [];
    for (const bytes of [noise(24, 11), new Uint8Array(24).fill(0xff)])
      for (const start of [0, 3, 8])
        for (let width = 1; width <= MAX_PACKED_OFFSET_BITS; width++) {
          const count = Math.floor(((bytes.length - start) * 8) / width);
          for (let index = Math.max(0, count - 8); index < count + 4; index++) {
            const expected = outcome(() =>
              packedBigIntAt(bytes.subarray(start), width, index),
            );
            const actual = outcome(() =>
              packedOffsetAt(bytes, width, index, start),
            );
            if (actual !== expected)
              mismatches.push({ start, width, index, actual, expected });
          }
        }
    expect(mismatches).toEqual([]);
  });

  test("refuse widths their number result cannot hold and reads past the bytes", () => {
    const bytes = noise(16, 7);
    expect(() => packedNumberAt(bytes, 33, 0)).toThrow(/exceeds 32 bits/u);
    expect(() => packedOffsetAt(bytes, 46, 0, 0)).toThrow(/exceeds 45 bits/u);
    expect(() => packedNumberAt(bytes, 8, 16)).toThrow(RangeError);
    expect(() => packedNumberAt(bytes, 31, 4)).toThrow(RangeError);
    expect(() => packedOffsetAt(bytes, 45, 3, 0)).toThrow(RangeError);
    expect(() => packedOffsetAt(bytes, 12, -1, 0)).toThrow(RangeError);
    expect(() => packedBigIntAt(bytes, 64, 2)).toThrow(RangeError);
  });
});
