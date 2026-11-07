/**
 * Open-addressing tables the query evaluator groups and joins through, keyed
 * by encoded cells instead of the `valueKey` text of every row.
 */

/** Little-endian 32-bit word at `offset`; any bytes past the end read as 0. */
function wordAt(bytes: Uint8Array, offset: number): number {
  return (
    (bytes[offset] ?? 0) |
    ((bytes[offset + 1] ?? 0) << 8) |
    ((bytes[offset + 2] ?? 0) << 16) |
    ((bytes[offset + 3] ?? 0) << 24)
  );
}

/** One murmur3 round: folds `word` into `hash`. */
function mixWord(hash: number, word: number): number {
  let value = Math.imul(word, 0xcc9e2d51);
  value = (value << 15) | (value >>> 17);
  value = Math.imul(value, 0x1b873593);
  const mixed = hash ^ value;
  return (Math.imul((mixed << 13) | (mixed >>> 19), 5) + 0xe6546b64) | 0;
}

/** murmur3's finalizer, so that low bits depend on every input bit. */
function finishHash(hash: number): number {
  let value = hash ^ (hash >>> 16);
  value = Math.imul(value, 0x85ebca6b);
  value ^= value >>> 13;
  value = Math.imul(value, 0xc2b2ae35);
  return value ^ (value >>> 16);
}

const INITIAL_SLOTS = 64;

/**
 * Distinct 16-byte values, numbered from 0 in the order they were first added.
 * Two values get the same number exactly when their bytes are equal, so a
 * number stands in for the `valueKey` of a 16-byte `Uint8Array`.
 *
 * Holds 16 bytes a value and at most four 4-byte slots a value, so it never
 * holds more than 32 bytes a value past the doubling slack.
 */
export class ByteKeyTable {
  #words = new Int32Array(INITIAL_SLOTS * 2);
  #slots = new Int32Array(INITIAL_SLOTS);
  #size = 0;

  get size(): number {
    return this.#size;
  }

  /** The number of the 16 bytes from `bytes[offset]`, or -1 when absent. */
  find(bytes: Uint8Array, offset: number): number {
    return this.#lookup(bytes, offset, false);
  }

  /** The number of the 16 bytes from `bytes[offset]`, added when absent. */
  add(bytes: Uint8Array, offset: number): number {
    return this.#lookup(bytes, offset, true);
  }

  #lookup(bytes: Uint8Array, offset: number, add: boolean): number {
    const w0 = wordAt(bytes, offset);
    const w1 = wordAt(bytes, offset + 4);
    const w2 = wordAt(bytes, offset + 8);
    const w3 = wordAt(bytes, offset + 12);
    const slots = this.#slots;
    const words = this.#words;
    const mask = slots.length - 1;
    let slot =
      finishHash(mixWord(mixWord(mixWord(mixWord(0, w0), w1), w2), w3)) & mask;
    for (;;) {
      const entry = slots[slot] ?? 0;
      if (entry === 0) break;
      const base = (entry - 1) * 4;
      if (
        words[base] === w0 &&
        words[base + 1] === w1 &&
        words[base + 2] === w2 &&
        words[base + 3] === w3
      )
        return entry - 1;
      slot = (slot + 1) & mask;
    }
    if (!add) return -1;
    const key = this.#size;
    if ((key + 1) * 4 > words.length) {
      const grown = new Int32Array(words.length * 2);
      grown.set(words);
      this.#words = grown;
    }
    const base = key * 4;
    this.#words[base] = w0;
    this.#words[base + 1] = w1;
    this.#words[base + 2] = w2;
    this.#words[base + 3] = w3;
    this.#size = key + 1;
    slots[slot] = key + 1;
    if (this.#size * 2 > slots.length) this.#rehash(slots.length * 2);
    return key;
  }

  #rehash(capacity: number): void {
    const slots = new Int32Array(capacity);
    const mask = capacity - 1;
    const words = this.#words;
    for (let key = 0; key < this.#size; key++) {
      const base = key * 4;
      let slot =
        finishHash(
          mixWord(
            mixWord(
              mixWord(mixWord(0, words[base] ?? 0), words[base + 1] ?? 0),
              words[base + 2] ?? 0,
            ),
            words[base + 3] ?? 0,
          ),
        ) & mask;
      while ((slots[slot] ?? 0) !== 0) slot = (slot + 1) & mask;
      slots[slot] = key + 1;
    }
    this.#slots = slots;
  }
}

/** murmur3 over `bytes[start..end)`, seeded with its length. */
function hashRange(bytes: Uint8Array, start: number, end: number): number {
  let hash = end - start;
  let offset = start;
  for (; offset + 4 <= end; offset += 4)
    hash = mixWord(
      hash,
      (bytes[offset] ?? 0) |
        ((bytes[offset + 1] ?? 0) << 8) |
        ((bytes[offset + 2] ?? 0) << 16) |
        ((bytes[offset + 3] ?? 0) << 24),
    );
  let tail = 0;
  for (let shift = 0; offset < end; offset++, shift += 8)
    tail |= (bytes[offset] ?? 0) << shift;
  return finishHash(mixWord(hash, tail));
}

/**
 * Distinct byte strings of any length, numbered from 0 in the order they were
 * first added. Two byte strings get the same number exactly when they are
 * equal, so a number stands in for the `valueKey` of a string wherever each
 * string has exactly one byte form.
 *
 * Holds a copy of each distinct string's bytes, 8 bytes of bookkeeping a
 * string and at most four 4-byte slots a string, past the doubling slack.
 */
export class ByteStringTable {
  #data = new Uint8Array(INITIAL_SLOTS * 16);
  /** Where each string ends in `#data`; each starts where the last ended. */
  #ends = new Int32Array(INITIAL_SLOTS);
  #hashes = new Int32Array(INITIAL_SLOTS);
  #slots = new Int32Array(INITIAL_SLOTS);
  #size = 0;

  get size(): number {
    return this.#size;
  }

  /** The number of `bytes[start..end)`, added when absent. */
  add(bytes: Uint8Array, start: number, end: number): number {
    const hash = hashRange(bytes, start, end);
    const length = end - start;
    const slots = this.#slots;
    const mask = slots.length - 1;
    let slot = hash & mask;
    for (;;) {
      const entry = slots[slot] ?? 0;
      if (entry === 0) break;
      const key = entry - 1;
      if (this.#hashes[key] === hash) {
        const from = key === 0 ? 0 : (this.#ends[key - 1] ?? 0);
        if ((this.#ends[key] ?? 0) - from === length) {
          const data = this.#data;
          let index = 0;
          while (index < length && data[from + index] === bytes[start + index])
            index++;
          if (index === length) return key;
        }
      }
      slot = (slot + 1) & mask;
    }
    const key = this.#size;
    const from = key === 0 ? 0 : (this.#ends[key - 1] ?? 0);
    if (from + length > this.#data.length) {
      const grown = new Uint8Array(
        Math.max(this.#data.length * 2, from + length),
      );
      grown.set(this.#data);
      this.#data = grown;
    }
    if (key === this.#ends.length) {
      const ends = new Int32Array(key * 2);
      ends.set(this.#ends);
      this.#ends = ends;
      const hashes = new Int32Array(key * 2);
      hashes.set(this.#hashes);
      this.#hashes = hashes;
    }
    this.#data.set(bytes.subarray(start, end), from);
    this.#ends[key] = from + length;
    this.#hashes[key] = hash;
    this.#size = key + 1;
    slots[slot] = key + 1;
    if (this.#size * 2 > slots.length) this.#rehash(slots.length * 2);
    return key;
  }

  #rehash(capacity: number): void {
    const slots = new Int32Array(capacity);
    const mask = capacity - 1;
    for (let key = 0; key < this.#size; key++) {
      let slot = (this.#hashes[key] ?? 0) & mask;
      while ((slots[slot] ?? 0) !== 0) slot = (slot + 1) & mask;
      slots[slot] = key + 1;
    }
    this.#slots = slots;
  }
}

/**
 * Groups keyed by a tuple of `width` 32-bit field ids, numbered from 0 in the
 * order they were added. Two tuples find the same group exactly when every id
 * is equal, so the ids must be equal exactly when the fields' `valueKey`s are.
 *
 * Holds `4 * width` bytes a group and at most four 4-byte slots a group, past
 * the doubling slack.
 */
export class GroupKeyTable {
  readonly #width: number;
  #ids: Int32Array;
  #slots = new Int32Array(INITIAL_SLOTS);
  #size = 0;

  constructor(width: number) {
    this.#width = width;
    this.#ids = new Int32Array(Math.max(1, width) * INITIAL_SLOTS);
  }

  get size(): number {
    return this.#size;
  }

  #hash(ids: Int32Array, base: number): number {
    let hash = 0;
    for (let field = 0; field < this.#width; field++)
      hash = mixWord(hash, ids[base + field] ?? 0);
    return finishHash(hash);
  }

  /** The group of the tuple `ids[0..width)`, or -1 when there is none. */
  find(ids: Int32Array): number {
    const width = this.#width;
    const slots = this.#slots;
    const stored = this.#ids;
    const mask = slots.length - 1;
    let slot = this.#hash(ids, 0) & mask;
    for (;;) {
      const entry = slots[slot] ?? 0;
      if (entry === 0) return -1;
      const base = (entry - 1) * width;
      let field = 0;
      while (field < width && stored[base + field] === ids[field]) field++;
      if (field === width) return entry - 1;
      slot = (slot + 1) & mask;
    }
  }

  /**
   * Adds the tuple `ids[0..width)`, which `find` did not find, and returns its
   * group.
   */
  add(ids: Int32Array): number {
    const width = this.#width;
    const group = this.#size;
    if ((group + 1) * width > this.#ids.length) {
      const grown = new Int32Array(this.#ids.length * 2);
      grown.set(this.#ids);
      this.#ids = grown;
    }
    this.#ids.set(ids.subarray(0, width), group * width);
    this.#size = group + 1;
    if (this.#size * 2 > this.#slots.length)
      this.#slots = new Int32Array(this.#slots.length * 2);
    else {
      this.#place(group);
      return group;
    }
    for (let placed = 0; placed <= group; placed++) this.#place(placed);
    return group;
  }

  #place(group: number): void {
    const slots = this.#slots;
    const mask = slots.length - 1;
    let slot = this.#hash(this.#ids, group * this.#width) & mask;
    while ((slots[slot] ?? 0) !== 0) slot = (slot + 1) & mask;
    slots[slot] = group + 1;
  }
}
