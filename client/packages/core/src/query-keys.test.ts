import { describe, expect, test } from "vitest";

import { ByteKeyTable, ByteStringTable, GroupKeyTable } from "./query-keys.js";

/** Deterministic 32-bit values from `seed`. */
function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state;
  };
}

describe("query key tables", () => {
  test("number 16-byte values by first addition, equal exactly when the bytes are", () => {
    const next = generator(3);
    // Values that differ in one byte at every position, repeats, and values
    // past a doubling of the table, each read at an odd offset.
    const values: Uint8Array[] = [];
    for (let position = 0; position < 16; position++) {
      const value = new Uint8Array(16);
      value[position] = 1;
      values.push(
        value,
        new Uint8Array(16).fill(0xff).fill(0, position, position + 1),
      );
    }
    values.push(new Uint8Array(16), new Uint8Array(16).fill(0xff));
    for (let value = 0; value < 3_000; value++)
      values.push(
        Uint8Array.from({ length: 16 }, (_, byte) =>
          byte < 12 ? 0 : next() >>> 29,
        ),
      );
    const bytes = new Uint8Array(values.length * 16 + 3);
    values.forEach((value, index) => bytes.set(value, index * 16 + 3));

    const table = new ByteKeyTable();
    const expected = new Map<string, number>();
    const mismatches: unknown[] = [];
    values.forEach((value, index) => {
      const text = value.join(",");
      const known = expected.get(text) ?? -1;
      if (table.find(bytes, index * 16 + 3) !== known)
        mismatches.push({ index, find: known });
      if (known === -1) expected.set(text, expected.size);
      const id = table.add(bytes, index * 16 + 3);
      if (id !== expected.get(text)) mismatches.push({ index, add: id });
    });
    expect(mismatches).toEqual([]);
    expect(table.size).toBe(expected.size);
    expect(expected.size).toBeGreaterThan(64);
    values.forEach((value, index) => {
      if (table.find(bytes, index * 16 + 3) !== expected.get(value.join(",")))
        mismatches.push({ index, again: true });
    });
    expect(mismatches).toEqual([]);
  });

  test("number byte strings by first addition, equal exactly when the bytes are", () => {
    const next = generator(5);
    // The empty string, strings one a prefix of the next, ones that differ
    // only past a whole word or only in length by a trailing zero, two pairs
    // whose hashes are equal, and enough short random ones to grow the table
    // past several doublings.
    const values: Uint8Array[] = [
      ...["yydjaa", "ssaaba", "ebgba", "tahbaa"].map((text) =>
        new TextEncoder().encode(text),
      ),
      new Uint8Array(0),
      Uint8Array.of(0),
      Uint8Array.of(0, 0),
      Uint8Array.of(0x61),
      Uint8Array.of(0x61, 0),
      Uint8Array.of(1, 2, 3, 4),
      Uint8Array.of(1, 2, 3, 4, 5),
      Uint8Array.of(1, 2, 3, 4, 6),
      new Uint8Array(300).fill(7),
    ];
    for (let value = 0; value < 3_000; value++)
      values.push(
        Uint8Array.from({ length: next() >>> 29 }, () => next() >>> 30),
      );
    // Each read out of one shared buffer, after a byte that is not its own.
    const offsets = [0];
    for (const value of values)
      offsets.push((offsets.at(-1) ?? 0) + 1 + value.length);
    const bytes = new Uint8Array(offsets.at(-1) ?? 0).fill(0xee);
    values.forEach((value, index) =>
      bytes.set(value, (offsets[index] ?? 0) + 1),
    );

    const table = new ByteStringTable();
    const expected = new Map<string, number>();
    const mismatches: unknown[] = [];
    for (const round of [0, 1])
      values.forEach((value, index) => {
        const text = value.join(",");
        if (!expected.has(text)) expected.set(text, expected.size);
        const start = (offsets[index] ?? 0) + 1;
        const id = table.add(bytes, start, start + value.length);
        if (id !== expected.get(text)) mismatches.push({ round, index, id });
      });
    expect(mismatches).toEqual([]);
    expect(table.size).toBe(expected.size);
    expect(expected.size).toBeGreaterThan(64);
  });

  test("number id tuples by first addition, equal exactly when every id is", () => {
    for (const width of [0, 1, 2, 5]) {
      const next = generator(width + 17);
      const table = new GroupKeyTable(width);
      const expected = new Map<string, number>();
      const ids = new Int32Array(width + 2);
      const mismatches: unknown[] = [];
      for (let row = 0; row < 4_000; row++) {
        // Small ids, the null id, and ids spilled below it; the two slots past
        // the width change freely and must not take part.
        for (let field = 0; field < ids.length; field++)
          ids[field] = (next() >>> 28) - 4;
        const text = Array.from(ids.subarray(0, width)).join(",");
        const known = expected.get(text) ?? -1;
        const found = table.find(ids);
        if (found !== known) mismatches.push({ width, row, found, known });
        if (found !== -1) continue;
        expected.set(text, expected.size);
        const added = table.add(ids);
        if (added !== expected.get(text))
          mismatches.push({ width, row, added });
      }
      expect(mismatches).toEqual([]);
      expect(table.size).toBe(expected.size);
      if (width >= 2) expect(expected.size).toBeGreaterThan(64);
    }
  });
});
