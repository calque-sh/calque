/**
 * Readers for little-endian bit-packed integers, as the direct-columnar format
 * lays them out: value `index` of width `width` starts at bit `index * width`,
 * least significant bit first. The decoder validates a packed part's length
 * before it reads one, so a read outside the bytes is a decoder defect and
 * throws rather than returning a guess.
 *
 * A module of its own so tests can check each fast path against
 * `packedBigIntAt` at widths no fixture dictionary reaches.
 */

export function packedNumberAt(
  bytes: Uint8Array,
  width: number,
  index: number,
): number {
  if (width === 0) return 0;
  if (width > 32) throw new Error("packed number width exceeds 32 bits");
  const bitOffset = index * width;
  const byteOffset = Math.floor(bitOffset / 8);
  const shift = bitOffset % 8;
  const byteCount = Math.ceil((shift + width) / 8);
  // Dictionary codes are read once per row. When the bits read fit in 31,
  // int32 shifts give the same value without a power per byte.
  if (shift + width <= 31) {
    let bits = 0;
    for (let byte = 0; byte < byteCount; byte++)
      bits |= byteAt(bytes, byteOffset + byte) << (byte * 8);
    return (bits >>> shift) & ((1 << width) - 1);
  }
  let accumulator = 0;
  for (let byte = 0; byte < byteCount; byte++)
    accumulator += byteAt(bytes, byteOffset + byte) * 2 ** (byte * 8);
  return Math.floor(accumulator / 2 ** shift) % 2 ** width;
}

/**
 * `packedNumberAt` for one width up to 24 bits, as a reader for a loop that
 * reads a value a row. The four bytes from a value's first byte hold all of
 * its bits at any shift, so they are read whole and masked; a byte past the
 * end reads as 0, and only masked-off bits ever see one. Unlike
 * `packedNumberAt` it does not throw for an index past the data, so a caller
 * reads only indexes the validated part holds. Wider widths read through
 * `packedNumberAt`.
 */
export function packedNumberReader(
  bytes: Uint8Array,
  width: number,
): (index: number) => number {
  if (width > MAX_WORD_READ_BITS)
    return (index) => packedNumberAt(bytes, width, index);
  const mask = (1 << width) - 1;
  return (index) => {
    const bit = index * width;
    // `bit` can pass 2^31; `& 7` still takes its low bits, which is all the
    // shift needs, and the division is exact.
    const shift = bit & 7;
    const byte = (bit - shift) / 8;
    return (
      (((bytes[byte] ?? 0) |
        ((bytes[byte + 1] ?? 0) << 8) |
        ((bytes[byte + 2] ?? 0) << 16) |
        ((bytes[byte + 3] ?? 0) << 24)) >>>
        shift) &
      mask
    );
  };
}

/** The widest value a shift of up to 7 bits leaves inside 31 bits of a word. */
const MAX_WORD_READ_BITS = 24;

/**
 * `packedBigIntAt` for widths up to 45 bits, as a number. The value's bits
 * start `shift` (0 to 7) bits into a byte, so they span at most seven bytes:
 * the first four are read as one unsigned word and any remaining three as a
 * second, masked to the value's `shift + width - 32` high bits. Each part is
 * below 2^32 and the sum below 2^45, so it is exact.
 *
 * A value with a byte outside the data throws the `RangeError` a read of
 * that byte always has.
 */
export function packedOffsetAt(
  bytes: Uint8Array,
  width: number,
  index: number,
  byteStart: number,
): number {
  if (width === 0) return 0;
  if (width > MAX_PACKED_OFFSET_BITS)
    throw new Error("packed offset width exceeds 45 bits");
  const bitOffset = index * width;
  const byteOffset = byteStart + Math.floor(bitOffset / 8);
  const shift = bitOffset % 8;
  const end = shift + width;
  if (byteOffset < 0 || byteOffset + Math.ceil(end / 8) > bytes.length)
    throw new RangeError(OUTSIDE_DATA);
  const low =
    ((bytes[byteOffset] ?? 0) |
      ((bytes[byteOffset + 1] ?? 0) << 8) |
      ((bytes[byteOffset + 2] ?? 0) << 16) |
      ((bytes[byteOffset + 3] ?? 0) << 24)) >>>
    shift;
  if (end <= 32) return width === 32 ? low : low & ((1 << width) - 1);
  const high =
    ((bytes[byteOffset + 4] ?? 0) |
      ((bytes[byteOffset + 5] ?? 0) << 8) |
      ((bytes[byteOffset + 6] ?? 0) << 16)) &
    ((1 << (end - 32)) - 1);
  return low + high * (WORD / (1 << shift));
}

const WORD = 2 ** 32;

export const MAX_PACKED_OFFSET_BITS = 45;

export function packedBigIntAt(
  bytes: Uint8Array,
  width: number,
  index: number,
): bigint {
  if (width === 0) return 0n;
  const bitOffset = index * width;
  const byteOffset = Math.floor(bitOffset / 8);
  const shift = bitOffset % 8;
  const byteCount = Math.ceil((shift + width) / 8);
  let accumulator = 0n;
  for (let byte = 0; byte < byteCount; byte++)
    accumulator |= BigInt(byteAt(bytes, byteOffset + byte)) << BigInt(byte * 8);
  return (accumulator >> BigInt(shift)) & ((1n << BigInt(width)) - 1n);
}

const OUTSIDE_DATA = "index is outside the validated data";

function byteAt(bytes: Uint8Array, index: number): number {
  const value = bytes[index];
  if (value === undefined) throw new RangeError(OUTSIDE_DATA);
  return value;
}
