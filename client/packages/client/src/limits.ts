/** A control character, which no name, endpoint or credential may contain. */
export const CONTROL_CHARACTER = /\p{Cc}/u;

/** The most bytes one read of an artifact response may deliver. */
export const MAX_ARTIFACT_CHUNK_BYTES = 256 * 1024;

/**
 * The fixed main-thread pool a client charges its own bookkeeping against,
 * and the floor `clientHeapReserveBytes` may not go under. Derived as 1,536
 * tracked entries at a conservative 512 bytes each.
 */
export const CLIENT_METADATA_RESERVE_BYTES = 786_432;

const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/;

/**
 * Whether `value` is spelled as a plan or mutation ID: at most 512
 * characters, an ASCII letter or digit, then letters, digits and `._:/-`.
 */
export function isBoundedIdentifier(value: string): boolean {
  return value.length <= 512 && IDENTIFIER.test(value);
}

const STRING_CHAR_CODE_AT = String.prototype.charCodeAt;
const STRING_CHAR_CODE_AT_VALUE = Function.prototype.call.bind(
  STRING_CHAR_CODE_AT,
) as (value: string, index: number) => number;

/**
 * The UTF-8 length of `value`, stopping at `limit`. It counts rather than
 * encodes, so a hostile string cannot make the measurement itself allocate,
 * and it reads code units through a captured intrinsic so a redefined
 * `charCodeAt` cannot change the answer. Returns `limit + 1` as soon as the
 * limit is passed.
 */
export function utf8ByteLengthAtMost(value: string, limit: number): number {
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const code = STRING_CHAR_CODE_AT_VALUE(value, index);
    if (code <= 0x7f) {
      bytes += 1;
    } else if (code <= 0x7ff) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length) {
      const next = STRING_CHAR_CODE_AT_VALUE(value, index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
    if (bytes > limit) return limit + 1;
  }
  return bytes;
}
